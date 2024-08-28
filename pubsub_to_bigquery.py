import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from google.cloud import bigquery

class TransformForTableFn(beam.DoFn):
    def __init__(self, schema_fields):
        self.schema_fields = schema_fields

    def process(self, element):
        message_data = json.loads(element)  # Parse JSON message from Pub/Sub
        row = {}
        for field in self.schema_fields:
            if field in message_data:
                row[field] = message_data.get(field)
            elif 'details' in message_data and field.startswith('details_'):
                row[field] = message_data['details'].get(field.split('_', 1)[1])
            else:
                row[field] = None
        yield row

def get_bigquery_schema(project_id, dataset_id, table_id):
    client = bigquery.Client(project=project_id)
    table_ref = client.dataset(dataset_id).table(table_id)
    table = client.get_table(table_ref)
    return table.schema

def convert_schema_to_string(schema):
    return ', '.join(f"{field.name}:{field.field_type}" for field in schema)

def run():
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True  # Enable streaming mode

    # Define project and dataset
    project_id = 'kafka-pubsub-dataflow-bigquery'
    dataset_id = 'processed_streaming_data'

    # Retrieve schema from BigQuery tables
    schema1_fields = get_bigquery_schema(project_id, dataset_id, 'use-case-1')
    schema2_fields = get_bigquery_schema(project_id, dataset_id, 'use-case-2')
    schema3_fields = get_bigquery_schema(project_id, dataset_id, 'use-case-3')

    # Convert the schemas to string format required by WriteToBigQuery
    schema1 = convert_schema_to_string(schema1_fields)
    schema2 = convert_schema_to_string(schema2_fields)
    schema3 = convert_schema_to_string(schema3_fields)

    # Define schema fields dynamically based on the retrieved schema
    fields1 = [field.name for field in schema1_fields]
    fields2 = [field.name for field in schema2_fields]
    fields3 = [field.name for field in schema3_fields]

    # Pub/Sub topic to read from
    pubsub_topic = 'projects/kafka-pubsub-dataflow-bigquery/topics/streaming-data-topic-01'

    with beam.Pipeline(options=options) as p:
        # Read messages from Pub/Sub
        messages = p | 'ReadFromPubSub' >> ReadFromPubSub(topic=pubsub_topic)

        # Transform data for each table schema
        table1_data = messages | 'TransformForTable1' >> beam.ParDo(TransformForTableFn(fields1))
        table2_data = messages | 'TransformForTable2' >> beam.ParDo(TransformForTableFn(fields2))
        table3_data = messages | 'TransformForTable3' >> beam.ParDo(TransformForTableFn(fields3))

        # Write to BigQuery
        table1_data | 'WriteToBigQueryTable1' >> WriteToBigQuery(
            table=f'{project_id}.{dataset_id}.use-case-1',
            schema=schema1,
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )

        table2_data | 'WriteToBigQueryTable2' >> WriteToBigQuery(
            table=f'{project_id}.{dataset_id}.use-case-2',
            schema=schema2,
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )

        table3_data | 'WriteToBigQueryTable3' >> WriteToBigQuery(
            table=f'{project_id}.{dataset_id}.use-case-3',
            schema=schema3,
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )

if __name__ == '__main__':
    run()
