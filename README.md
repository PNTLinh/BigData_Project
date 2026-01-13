# Dự Án Phân Tích Dữ Liệu Taxi NYC

Dự án này xây dựng một hệ thống xử lý dữ liệu tự động: Dữ liệu từ xe taxi chảy qua Kafka, được Spark tính toán ngay lập tức, lưu vào Cassandra và hiển thị biểu đồ lên Grafana.

## 1. Sơ đồ hoạt động
Dữ liệu di chuyển theo luồng: Dữ liệu gốc (Parquet) → Kafka (Trạm trung chuyển) → Spark (Bộ não tính toán) → Cassandra (Kho lưu trữ) → Grafana (Biểu đồ hiển thị).

## 2. Các công cụ chính
Kafka: Tiếp nhận dữ liệu khổng lồ theo thời gian thực.

Spark: Tính toán các con số (tổng chuyến xe, doanh thu) theo từng khung giờ.

Cassandra: Cơ sở dữ liệu chuyên dùng để lưu trữ dữ liệu lớn và nhanh.

Grafana: Nơi vẽ các biểu đồ xanh đỏ giúp chúng ta dễ dàng theo dõi.

HDFS: Kho lưu trữ dự phòng lâu dài.

## 3. Cách chạy hệ thống

### Bước 1: Khởi động 

```
docker compose up -d
```
### Bước 2: Kiểm tra log file

```
docker logs -f spark
docker logs -f producer
```

### Bước 3: Xem kết quả
Biểu đồ Grafana: Truy cập http://localhost:3000 (Tài khoản: admin / Mật khẩu: admin).

Dữ liệu thô trong Cassandra: docker exec -it cassandra cqlsh

## 4. Các lệnh xử lý lỗi

Xóa hết làm lại: docker compose down -v

Xóa bộ nhớ tạm của Spark: rm -rf /tmp/spark_checkpoints

Tạo lại bảng dữ liệu: docker exec -i cassandra cqlsh < cassandra/init.cql

## 5. Kết quả đạt được

Tổng số chuyến xe theo thời gian thực.

Doanh thu của các khu vực trong thành phố.

Loại hình thanh toán (tiền mặt hay thẻ) phổ biến nhất.