#!/bin/bash

echo "=================================================="
echo "   BIG DATA PIPELINE - AUTOMATED STARTUP SCRIPT   "
echo "=================================================="

# 1. Dọn dẹp sạch sẽ môi trường cũ
echo "[1/6] Cleaning up old environment..."
docker compose down -v
# docker system prune -f  # Uncomment dòng này nếu muốn xóa cache triệt để

# 2. Khởi động Hạ tầng (TRỪ SPARK)
echo "[2/6] Starting Infrastructure..."
# Đã bỏ zookeeper, dùng đúng tên service trong compose.yaml
docker compose up -d namenode datanode cassandra broker grafana producer

# 3. Chờ Cassandra sẵn sàng
echo "[3/6] Waiting for Cassandra to be ready..."
# Vòng lặp kiểm tra: Chạy thử lệnh cqlsh, nếu lỗi thì đợi 5s rồi thử lại
while ! docker exec cassandra cqlsh -e "DESCRIBE KEYSPACES" > /dev/null 2>&1; do
    echo "  -> Cassandra is warming up... (sleeping 5s)"
    sleep 5
done
echo "  -> Cassandra is UP!"

# Nạp Schema
echo "  -> Initializing Keyspace & Tables..."
docker exec -i cassandra cqlsh < cassandra/init.cql

# 4. Chờ HDFS NameNode sẵn sàng
echo "[4/6] Waiting for HDFS NameNode..."
sleep 15 # Đợi Java process của Hadoop khởi động hẳn

# Ép tắt Safe Mode (Quan trọng với máy ít RAM)
echo "  -> Forcing Safe Mode OFF..."
docker exec namenode hdfs dfsadmin -safemode leave

# 5. Tạo thư mục và Cấp quyền HDFS
echo "[5/6] Setting up HDFS permissions..."
# Tạo thư mục
docker exec namenode hdfs dfs -mkdir -p /taxi_data/raw
# Cấp quyền 777
docker exec namenode hdfs dfs -chmod -R 777 /taxi_data
echo "  -> HDFS is ready for Spark!"

# 6. Bây giờ mới bật Spark
echo "[6/6] STARTING SPARK STREAMING..."
docker compose up -d spark

echo "=================================================="
echo "   DEPLOYMENT SUCCESSFUL! SYSTEM IS RUNNING.      "
echo "=================================================="
echo "Check HDFS files (after 2 mins): docker exec -it namenode hdfs dfs -ls -R /taxi_data/raw"
echo "Check Spark logs: docker logs -f spark"