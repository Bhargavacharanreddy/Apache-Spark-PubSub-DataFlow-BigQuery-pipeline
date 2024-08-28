# Apache-Spark-PubSub-DataFlow-BigQuery-pipeline
A real-time ( streaming ) data pipeline using Apache Spark, Google Pub/Sub, DataFlow, BigQuery Services


# Steps to be followed

### Step1: Start the zookeeper service
> Command: bin/zookeeper-server-start.sh config/zookeeper.properties


### Step 2: Start the Kafka service
> Command: bin/kafka-server-start.sh config/server.properties

### Step 3: Run pubsub_to_bigquery.py script to create dataflow streaming job
> Command: python pubsub_to_bigquery.py \
    --runner=DataflowRunner \
    --project=kafka-pubsub-dataflow-bigquery \
    --temp_location=gs://kafka-pubsub-dataflow-bigquery-bucket/temp \
    --staging_location=gs://kafka-pubsub-dataflow-bigquery-bucket/staging \
    --region=us-east1


### Step 4: Run the kafka_to_pubsub.py script in the background
> Command: nohup python3 kafka_to_pubsub.py > kafka_to_pubsub.log 2>&1 &



### Step 5: Write some events/messages into the Kafka topic 

> Command: bin/kafka-console-producer.sh --topic quickstart-events --bootstrap-server localhost:9092
Sample messages:
> #### {"type": "category1", "timestamp": "2024-08-25T00:00:00Z", "user_id": "123", "event_name": "event1", "value": 100.5, "details": {"sub_field1": "detail1", "sub_field2": 42}}
> ####  {"type": "category2", "timestamp": "2024-08-25T00:01:00Z", "user_id": "124", "event_name": "event2", "value": 200.5, "details": {"sub_field1": "detail2", "sub_field2": 43}}
> ####    {"type": "category3", "timestamp": "2024-08-25T00:02:00Z", "user_id": "125", "event_name": "event3", "value": 300.5, "details": {"sub_field1": "detail3", "sub_field2": 54}}




### Step 6: Read and check the events/messages of the Kafka topic

> Command: bin/kafka-console-consumer.sh --topic quickstart-events --from-beginning --bootstrap-server localhost:9092


### Step 7: Query the BigQuery Tables to see the processed data

> Command: 
> #### select * from kafka-pubsub-dataflow-bigquery.processed_streaming_data.use-case-1
> #### select * from kafka-pubsub-dataflow-bigquery.processed_streaming_data.use-case-2
> #### select * from kafka-pubsub-dataflow-bigquery.processed_streaming_data.use-case-3



### Step 8: Stop the Dataflow service in the UI

### Step 9: Kill the kafka_to_pubsub.py script which is running in the background

