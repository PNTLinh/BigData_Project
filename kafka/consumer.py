import json
from kafka import KafkaConsumer
from datetime import datetime

# Cấu hình
KAFKA_BROKER = 'my-cluster-kafka-bootstrap.default.svc.cluster.local:9092'
TOPIC = 'mta-ace-stream'
GROUP_ID = 'mta-monitoring-group'

def start_consumer():
    # Khởi tạo Consumer
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        group_id=GROUP_ID,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='latest'  # Chỉ đọc các tin nhắn mới nhất
    )

    print(f"Listening for MTA updates on topic: {TOPIC}...")
    print("-" * 50)

    try:
        for message in consumer:
            data = message.value
            trip_id = data.get('trip_id')
            route_id = data.get('route_id')
            stops = data.get('stops', [])

            if stops:
                # Lấy điểm dừng tiếp theo (stop đầu tiên trong danh sách)
                next_stop = stops[0]
                arrival_time = next_stop.get('arrival')

                if arrival_time:
                    # Tính toán số phút còn lại
                    now = datetime.now().timestamp()
                    wait_seconds = arrival_time - now
                    wait_minutes = round(wait_seconds / 60)

                    if wait_minutes >= 0:
                        print(f"Tuyến {route_id} | Chuyến {trip_id}")
                        print(f"  -> Sắp đến ga: {next_stop['stop_id']}")
                        print(f"  -> Dự kiến trong: {wait_minutes} phút")
                        print("-" * 30)
                    
    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        consumer.close()

if __name__ == "__main__":
    start_consumer()
    