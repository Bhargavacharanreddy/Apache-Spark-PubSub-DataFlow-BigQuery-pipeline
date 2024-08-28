from kafka import KafkaConsumer
from google.cloud import pubsub_v1
import json
import os
import time

# Set environment variable for Google Cloud credentials (if not already set)
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/path/to/your-service-account-key.json"

# Kafka Configuration
kafka_topic = 'quickstart-events'
kafka_bootstrap_servers = ['localhost:9092']
kafka_group_id = 'my-group'

# Pub/Sub Configuration
pubsub_project_id = 'kafka-pubsub-dataflow-bigquery'
pubsub_topic_name = 'streaming-data-topic-01'

# Initialize Pub/Sub Publisher
publisher = pubsub_v1.PublisherClient()
pubsub_topic_path = publisher.topic_path(pubsub_project_id, pubsub_topic_name)

def consume_kafka_and_publish_to_pubsub():
    # Initialize Kafka Consumer
    consumer = KafkaConsumer(
        kafka_topic,
        bootstrap_servers=kafka_bootstrap_servers,
        group_id=kafka_group_id,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda x: x.decode('utf-8')
    )

    print(f"Subscribed to Kafka topic: {kafka_topic}")
    try:
        for message in consumer:
            # Read message from Kafka
            message_value = message.value
            print(f"Received message from Kafka: {message_value}")

            # Publish to Pub/Sub
            try:
                future = publisher.publish(pubsub_topic_path, message_value.encode('utf-8'))
                print(f"Published message to Pub/Sub: {message_value}")
            except Exception as e:
                print(f"Failed to publish message to Pub/Sub: {e}")

    except KeyboardInterrupt:
        print("Interrupted by user. Closing...")
    except Exception as e:
        print(f"Error occurred: {e}")
        time.sleep(5)  # Wait before retrying
        consume_kafka_and_publish_to_pubsub()  # Reconnect and retry
    finally:
        consumer.close()

if __name__ == "__main__":
    while True:
        try:
            consume_kafka_and_publish_to_pubsub()
        except Exception as e:
            print(f"An error occurred, restarting the consumer loop: {e}")
            time.sleep(10)  # Wait before restarting the loop
