from confluent_kafka import Consumer

consumer_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'taxi-trip-consumer-group-1',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(consumer_config)
consumer.subscribe(['taxi-trips'])

print("Consumer started. Waiting for messages...")

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        print(f"Received message: key='{msg.key().decode('utf-8')}', value='{msg.value().decode('utf-8')}'")
except KeyboardInterrupt:
    print("Stopping consumer.")
finally:
    consumer.close()