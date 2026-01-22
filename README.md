# MTA Real-time Data Pipeline (Lambda Architecture)

Dự án này xây dựng một hệ thống xử lý dữ liệu lớn (Big Data) theo kiến trúc **Lambda**, cho phép thu thập, xử lý và phân tích dữ liệu giao thông công cộng (tuyến tàu ACE của New York) theo thời gian thực (Real-time) và theo lô (Batch).

## 🚀 Kiến trúc hệ thống

Hệ thống bao gồm 4 lớp chính:

1. **Source & Ingestion Layer**: Sử dụng **Python Producer** để fetch dữ liệu từ MTA API và đẩy vào **Apache Kafka**.
2. **Speed Layer**: **Spark Streaming** tiêu thụ dữ liệu từ Kafka, xử lý tức thì và ghi vào **Cassandra**.
3. **Batch Layer (Cold Storage)**: Dữ liệu thô được lưu trữ tại **Hadoop HDFS**. Định kỳ, một **Spark Batch Job** sẽ tổng hợp dữ liệu lịch sử và ghi kết quả vào Cassandra.
4. **Serving & Visualization Layer**: **Cassandra** cung cấp dữ liệu cho **Grafana** để hiển thị Dashboard thời gian thực.

## 📂 Cấu trúc thư mục

```text
FINAL/
├── cassandra/          # File khởi tạo database (.cql)
├── grafana/            # Cấu hình tự động cho Dashboard & Datasource
├── kafka/              # Mã nguồn Producer và Dockerfile cho Kafka App
├── spark/              # Mã nguồn xử lý Streaming và Batch
├── kubernetes/         # Các file YAML để triển khai lên Cluster K8s
└── docker-compose.yml  # Triển khai nhanh môi trường phát triển (Local)

```

## 🛠️ Hướng dẫn cài đặt

### Yêu cầu hệ thống

* Docker & Docker Desktop
* Kubernetes (đã được enable trong Docker Desktop)
* Python 3.9+

### Triển khai trên Kubernetes

Triển khai theo thứ tự các file cấu hình để đảm bảo các phụ thuộc được đáp ứng:

1. **Khởi tạo Core & Storage:**
```bash
kubectl apply -f kubernetes/00-core.yaml
kubectl apply -f kubernetes/01-data-layer.yaml
kubectl apply -f kubernetes/05-hdfs.yaml

```


2. **Khởi tạo Database (Chờ Cassandra sẵn sàng):**
```bash
kubectl apply -f kubernetes/02-init-job.yaml

```


3. **Triển khai Ứng dụng & Dashboard:**
```bash
kubectl apply -f kubernetes/03-apps.yaml
kubectl apply -f kubernetes/04-ui.yaml
kubectl apply -f kubernetes/06-batch-cronjob.yaml

```



## 📊 Theo dõi kết quả

* **Grafana Dashboard**: Truy cập `http://localhost:3000` (admin/admin) để xem biểu đồ tàu chạy thực tế.
* **Kafka UI**: Truy cập `http://localhost:8080` để giám sát các luồng tin nhắn trong Kafka.
* **HDFS Web UI**: Truy cập `http://localhost:9870` để kiểm tra các file lưu trữ lịch sử.


