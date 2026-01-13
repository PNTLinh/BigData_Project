import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, avg, sum as spark_sum, count,
    current_timestamp, when, lit, hour, dayofweek, 
    unix_timestamp, broadcast, round as spark_round, year, month
)
from pyspark.sql.types import (
    StructType, StructField, IntegerType, LongType,
    DoubleType, TimestampType, StringType
)


# ---------- Logging Setup ----------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ---------- Config ----------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "taxi-trips")
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "taxi_streaming")

# Checkpoint paths - CRITICAL: Must be unique per query
CHECKPOINT_ROOT = os.getenv("CHECKPOINT_ROOT", "/tmp/spark_checkpoints")
# Ensure the dictionary keys match the dictionary access later
CHECKPOINT_PATH_DICT = {
    "demand": os.path.join(CHECKPOINT_ROOT, "demand"),
    "ops": os.path.join(CHECKPOINT_ROOT, "ops"),
    "rider": os.path.join(CHECKPOINT_ROOT, "rider")
}

# 7ax1 z0n3 100kup from: https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
SCRIPT_DIR = os.path.dirname(__file__)
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, '..'))
ZONE_LOOKUP_PATH = os.path.join(PROJECT_ROOT, "spark", "taxi_zone_lookup.csv")


# ---------- Schemas ----------
taxi_schema = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("tpep_pickup_datetime", TimestampType(), True),
    StructField("tpep_dropoff_datetime", TimestampType(), True),
    StructField("passenger_count", LongType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("RatecodeID", LongType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
    StructField("payment_type", LongType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True),
    StructField("Airport_fee", DoubleType(), True),
    StructField("cbd_congestion_fee", DoubleType(), True)
])

zone_schema = StructType([
    StructField("LocationID", IntegerType(), True),
    StructField("Borough", StringType(), True),
    StructField("Zone", StringType(), True),
    StructField("service_zone", StringType(), True)
])


# ---------- Spark Session ----------
spark = (
    SparkSession.builder
    .appName("NYC Taxi Trip Streaming Pipeline")
    .config("spark.cassandra.connection.host", CASSANDRA_HOST)
    .config("spark.cassandra.connection.port", "9042")
    .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_ROOT)
    .config("spark.scheduler.mode", "FAIR") 
    .config("spark.sql.shuffle.partitions", "4") 
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


# ---------- Read from Kafka ----------
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest") 
    .load()
)

# Parse JSON payload
parsed_df = kafka_df.select(
    from_json(col("value").cast("string"), taxi_schema).alias("data"),
    col("timestamp").alias("kafka_timestamp")
).select("data.*", "kafka_timestamp")


# ---------- Data Cleaning & Pre-processing ----------
df_with_duration = parsed_df.withColumn(
    "trip_duration_sec", 
    (unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime"))
)

cleaned_df = df_with_duration.filter(
    (col("trip_distance") > 0) &            # some are negative, can take abs if we really want to?
    (col("trip_distance") < 1000) &         # RANDOMLY SET
    (col("fare_amount") >= 0) &
    (col("passenger_count") > 0) &
    (col("passenger_count") < 10) &         # max 9 ppl = 5 adults + 4 children on lap (but >5 is probably outlier anyway)
    (col("trip_duration_sec") > 60) &       # <1m trips are often cancelled/errors
    (col("trip_duration_sec") < 14400) &    # >4h trips are likely unrealistic
    (col("PULocationID").isNotNull()) &
    (col("PULocationID") < 264)             # 264=Unknown, 265=N/A in NYC taxonomy
)


# ---------- Feature Engineering ----------
# 1. Event time
df_with_time = cleaned_df.withColumn(
    "event_time",
    when(col("tpep_pickup_datetime").isNotNull(), col("tpep_pickup_datetime"))
    .otherwise(col("kafka_timestamp"))
)

# Watermark
watermarked = df_with_time.withWatermark("event_time", "10 minutes")

# 2. Derived Features
enriched_df = watermarked.withColumn(
    "pickup_hour", hour("event_time")
).withColumn(
    "pickup_weekday", dayofweek("event_time") 
).withColumn(
    "is_weekend", 
    col("pickup_weekday").isin([1, 2]) # 1=Sunday, 7=Saturday (Spark default). 
    # NOTE: Spark dayofweek: 1=Sunday, 2=Monday... 7=Saturday. 
    # If you want Sat/Sun as weekend, check logic: isin([1, 7]). 
    # Assuming standard business logic (Sat=7, Sun=1).
).withColumn(
    "speed_mph",
    when(col("trip_duration_sec") > 0, 
         (col("trip_distance") / (col("trip_duration_sec") / 3600)))
    .otherwise(lit(0.0))
).withColumn(
    "tip_ratio",
    when(col("total_amount") > 0, 
         (col("tip_amount") / col("total_amount")))
    .otherwise(lit(0.0))
).withColumn(
    "fare_per_mile",
    when(col("trip_distance") > 0,
         (col("fare_amount") / col("trip_distance")))
    .otherwise(lit(0.0))
)

enriched_df = enriched_df.withColumn(
    "distance_category",
    when(col("trip_distance") < 2.0, lit("Short (<2m)"))
    .when(col("trip_distance") < 10.0, lit("Medium (2-10m)"))
    .otherwise(lit("Long (>10m)"))
).withColumn(
    "peak_category",
    when((~col("is_weekend")) & (col("pickup_hour").between(16, 20)), lit("PM Rush"))
    .when((~col("is_weekend")) & (col("pickup_hour").between(7, 10)) , lit("AM Rush"))
    .otherwise(lit("Off-Peak"))
)


# ---------- Load Static Data (Broadcast Join) ----------
try:
    # Use the defined schema
    zone_df = spark.read.schema(zone_schema).option("header", "true").csv(ZONE_LOOKUP_PATH)
    zone_lookup = zone_df.select(
        col("LocationID").alias("lookup_PULocationID"),
        col("Zone").alias("pickup_zone")
    )
except Exception as e:
    logger.warning(f"Could not load zone lookup file: {e}. Zone enrichment will be skipped.")
    zone_lookup = None

if zone_lookup:
    final_stream = enriched_df.join(
        broadcast(zone_lookup),
        enriched_df.PULocationID == zone_lookup.lookup_PULocationID,
        "left"
    ).drop("lookup_PULocationID")
else:
    # Fallback if CSV missing
    final_stream = enriched_df.withColumn("pickup_zone", lit("Unknown"))


# ---------- Aggregations ----------

# --- Query A: Demand & Revenue Dynamics ---
demand_agg = final_stream \
    .groupBy(
        window(col("event_time"), "1 hour"),
        col("pickup_zone"),
        col("peak_category")
    ) \
    .agg(
        count("*").alias("total_trips"),
        spark_sum("total_amount").alias("total_revenue"),
        avg("trip_distance").alias("avg_distance"),
        avg("fare_per_mile").alias("avg_fare_per_mile"),
        avg("tip_ratio").alias("avg_tip_ratio")
    ) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "pickup_zone", "peak_category",
        "total_trips", "total_revenue", "avg_distance", "avg_fare_per_mile", "avg_tip_ratio"
    )

# --- Query B: Operational Efficiency ---
ops_agg = final_stream \
    .groupBy(
        window(col("event_time"), "30 minutes"),
        col("pickup_zone")
    ) \
    .agg(
        avg("speed_mph").alias("avg_speed"),
        avg("trip_duration_sec").alias("avg_duration_sec"),
        avg("passenger_count").alias("avg_occupancy")
    ) \
    .select(
        col("window.start").alias("window_start"),
        "pickup_zone",
        "avg_speed", "avg_duration_sec", "avg_occupancy"
    )

# --- Query C: Rider Behavior ---
rider_agg = final_stream \
    .groupBy(
        window(col("event_time"), "1 hour"),
        col("payment_type"),
        col("distance_category")
    ) \
    .agg(
        count("*").alias("total_trips"),
        avg("tip_ratio").alias("avg_tip_ratio")
        # Note: Percentiles are expensive in streaming but useful for behavior
        # We comment it out for stability unless spark.sql.streaming.forceDeleteTempCheckpointLocation is handled
        # expr("percentile_approx(tip_ratio, 0.5)").alias("median_tip_ratio")
    ) \
    .select(
        col("window.start").alias("window_start"),
        "payment_type", "distance_category",
        "total_trips", "avg_tip_ratio"
    )


# ---------- Sinks ----------
def write_to_cassandra(batch_df, batch_id, table_name):
    if batch_df.rdd.isEmpty():
        return
    
    logger.info(f"Writing batch {batch_id} to {table_name}")
    (batch_df.write
        .format("org.apache.spark.sql.cassandra")
        .mode("append")
        .options(table=table_name, keyspace=CASSANDRA_KEYSPACE)
        .save())

print("Starting Demand Stream...")
query_demand = (
    demand_agg.writeStream
    .outputMode("update")
    .foreachBatch(lambda df, id: write_to_cassandra(df, id, "demand_dynamics"))
    .option("checkpointLocation", CHECKPOINT_PATH_DICT['demand'])
    .start()
)

print("Starting Ops Stream...")
query_ops = (
    ops_agg.writeStream
    .outputMode("update")
    .foreachBatch(lambda df, id: write_to_cassandra(df, id, "operational_efficiency"))
    .option("checkpointLocation", CHECKPOINT_PATH_DICT['ops'])
    .start()
)

print("Starting Rider Stream...")
query_rider = (
    rider_agg.writeStream
    .outputMode("update")
    .foreachBatch(lambda df, id: write_to_cassandra(df, id, "rider_behavior"))
    .option("checkpointLocation", CHECKPOINT_PATH_DICT['rider'])
    .start()
)

# ---------- Execution ----------
print("Streaming pipeline started. Press Ctrl+C to stop.")
spark.streams.awaitAnyTermination()