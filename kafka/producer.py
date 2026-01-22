import os
import time
import json
import requests
from kafka import KafkaProducer
from google.transit import gtfs_realtime_pb2

# Cấu hình tối giản
MTA_URL = "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-ace"
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "broker:29092")
TOPIC = 'mta-ace-stream'

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
)


def stream_mta():
    print(f"Streaming MTA ACE data to Kafka topic: {TOPIC}...")
    while True:
        try:
            # Gọi thẳng API không cần headers
            response = requests.get(MTA_URL, timeout=10)
            
            if response.status_code == 200:
                feed = gtfs_realtime_pb2.FeedMessage()
                feed.ParseFromString(response.content)
                
                count = 0
                for entity in feed.entity:
                    # Chỉ lấy trip_update để theo dõi lịch trình
                    if entity.HasField('trip_update'):
                        data = {
                            "trip_id": entity.trip_update.trip.trip_id,
                            "route_id": entity.trip_update.trip.route_id,
                            "stops": [
                                {
                                    "stop_id": st.stop_id,
                                    "arrival": st.arrival.time if st.HasField('arrival') else None
                                } for st in entity.trip_update.stop_time_update
                            ]
                        }
                        producer.send(TOPIC, value=data)
                        count += 1
                
                producer.flush()
                print(f"Sent {count} updates to Kafka.")
            else:
                print(f"Failed to fetch data. Status: {response.status_code}")

        except Exception as e:
            print(f"Error: {e}")

        time.sleep(2) # Nghỉ 30s giữa các lần fetch

if __name__ == "__main__":
    stream_mta()