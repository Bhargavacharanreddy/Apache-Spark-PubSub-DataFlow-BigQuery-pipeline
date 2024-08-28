from kafka import KafkaProducer
import json

# Kafka configuration
kafka_topic = 'quickstart-events'
kafka_bootstrap_servers = ['localhost:9092']

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=kafka_bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Sample messages to send to Kafka
messages = [
    {"type": "category1", "timestamp": "2024-08-25T00:00:00Z", "user_id": "123", "event_name": "event1", "value": 100.5, "details": {"sub_field1": "detail1", "sub_field2": 42}},
    {"type": "category2", "timestamp": "2024-08-25T00:01:00Z", "user_id": "124", "event_name": "event2", "value": 200.5, "details": {"sub_field1": "detail2", "sub_field2": 43}},
    {"type": "category3", "timestamp": "2024-08-25T00:02:00Z", "user_id": "125", "event_name": "event3", "value": 300.5, "details": {"sub_field1": "detail3", "sub_field2": 44}}
]

# Send messages to Kafka topic
for message in messages:
    producer.send(kafka_topic, message)
    print(f"Sent message: {message}")

# Ensure all messages are sent
producer.flush()
