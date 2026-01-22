import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window, count
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:29092")
TOPIC = os.getenv("KAFKA_TOPIC", "mta-ace-stream")

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "mta_data")

def main():
    spark = (SparkSession.builder
        .appName("MTA_Streaming_Processor")
        .config("spark.cassandra.connection.host", CASSANDRA_HOST)
        .getOrCreate())

    # Schema message Kafka (phải khớp với producer/spark hiện tại của bạn)
    schema = StructType([
        StructField("route_id", StringType(), True),
        StructField("trip_id", StringType(), True),
        StructField("stop_id", StringType(), True),
        StructField("arrival_timestamp", LongType(), True),
        StructField("processed_at", StringType(), True),  # thường là string
    ])

    raw = (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .load())

    parsed = (raw.selectExpr("CAST(value AS STRING) AS value")
        .select(from_json(col("value"), schema).alias("j"))
        .select("j.*")
        .withColumn("processed_ts", to_timestamp(col("processed_at")))  # parse timestamp
        .filter(col("route_id").isNotNull() & col("processed_ts").isNotNull())
    )

    # Aggregate theo phút
    per_min = (parsed
        .groupBy(col("route_id"), window(col("processed_ts"), "1 minute").alias("w"))
        .agg(count("*").alias("total"))
        .select(
            col("route_id"),
            col("w").getField("start").alias("bucket_minute"),
            col("total")
        )
    )

    def write_to_cassandra(batch_df, batch_id):
        # Ghi aggregate
        (batch_df.write
            .format("org.apache.spark.sql.cassandra")
            .mode("append")
            .options(table="route_minute_stats", keyspace=KEYSPACE)
            .save())

    (per_min.writeStream
        .foreachBatch(write_to_cassandra)
        .outputMode("update")
        .option("checkpointLocation", "/tmp/spark_checkpoints/route_minute_stats")
        .start()
        .awaitTermination())

if __name__ == "__main__":
    main()
