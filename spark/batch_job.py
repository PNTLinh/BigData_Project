from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, window, from_unixtime, to_date

def main():
    # Khởi tạo Spark Session cho xử lý Batch
    spark = SparkSession.builder \
        .appName("MTA_Batch_Processor") \
        .config("spark.cassandra.connection.host", "cassandra") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print("--- Đang bắt đầu tiến trình Batch Layer: Đọc dữ liệu từ HDFS ---")

    # 1. Đọc dữ liệu lịch sử từ HDFS (Dữ liệu đã được Streaming lưu lại dưới dạng Parquet)
    # Đường dẫn này phải khớp với path trong file streaming.py
    history_df = spark.read.parquet("hdfs://namenode:8020/mta/history")

    # 2. Xử lý dữ liệu: Chuyển đổi timestamp và làm sạch
    # Giả sử chúng ta muốn tính toán thống kê theo ngày và theo tuyến tàu (route_id)
    enriched_df = history_df.withColumn(
        "arrival_date", 
        to_date(from_unixtime(col("arrival_timestamp")))
    )

    # 3. Thực hiện Aggregation (Tổng hợp dữ liệu)
    # Ví dụ: Tính tổng số chuyến tàu và thời gian đến trung bình mỗi ngày cho từng tuyến
    daily_stats = enriched_df.groupBy("arrival_date", "route_id").agg(
        count("trip_id").alias("total_trips"),
        avg("arrival_timestamp").alias("avg_arrival_ts")
    )

    print("--- Kết quả tổng hợp dữ liệu lịch sử ---")
    daily_stats.show()

    # 4. Ghi kết quả vào Cassandra (Serving Layer)
    # Kết quả này sẽ phục vụ cho các biểu đồ dạng Historical Trend trên Grafana
    try:
        daily_stats.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="daily_route_stats", keyspace="mta_data") \
            .mode("append") \
            .save()
        print("--- Ghi dữ liệu vào Cassandra thành công! ---")
    except Exception as e:
        print(f"Lỗi khi ghi vào Cassandra: {e}")

    spark.stop()

if __name__ == "__main__":
    main()