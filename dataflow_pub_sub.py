import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import apache_beam.transforms.window as window

class MyOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        # Using add_argument instead of add_value_provider_argument 
        # to fix the 'RuntimeValueProvider' TypeError during template creation.
        parser.add_argument("--input_topic", help="PubSub topic to read from")
        parser.add_argument("--output_table", help="BigQuery table: project:dataset.table")

def parse_event(event):
    import json
    return json.loads(event)

def run():
    options = MyOptions()
    # Access the custom options specifically
    custom_options = options.view_as(MyOptions)
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as p:
        
        # 1. READ & PARSE
        raw_data = (
            p 
            | "Read from PubSub" >> beam.io.ReadFromPubSub(topic=custom_options.input_topic)
            | "Decode bytes" >> beam.Map(lambda x: x.decode("utf-8"))
            | "Parse JSON" >> beam.Map(parse_event)
        )

        # 2. TRANSFORM
        formatted = (
            raw_data
            | "Format for BQ" >> beam.Map(lambda e: {
                "order_id": e["order_id"],
                "amount": float(e["amount"]),
                "city": e["city"]
            })
        )

        # 3. WRITE TO BIGQUERY
        formatted | "Write to BigQuery" >> beam.io.WriteToBigQuery(
            table=custom_options.output_table,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )

        # 4. WRITE TO GCS (Requires Windowing for Streaming)
        (
            formatted 
            | "Window into Minutes" >> beam.WindowInto(window.FixedWindows(60)) 
            | "Write to GCS" >> beam.io.WriteToText(
                "gs://dataflow_practice_101/output/stream_res",
                file_name_suffix=".json"
            )
        )

if __name__ == "__main__":
    run()


"""

python3 dataflow_pub_sub.py   \
--runner DataflowRunner   \
--project fast-level-up-487010   \
--region us-central1   \
--temp_location gs://dataflow_practice_101/temp   \
--staging_location gs://dataflow_practice_101/staging   
--template_location gs://dataflow_practice_101/templates/pubsub_to_gcs_bq  \
--input_topic "projects/fast-level-up-487010/topics/orders-topic"  \
--output_table "fast-level-up-487010:test.orders_stream"



gcloud dataflow jobs run "pubsub-to-bq-job-$(date +%Y%m%d-%H%M%S)" \
    --gcs-location gs://dataflow_practice_101/templates/pubsub_to_gcs_bq \
    --region us-central1 \
    --worker-zone us-central1-a \
    --worker-machine-type e2-medium \
    --staging-location gs://dataflow_practice_101/staging \
    --parameters input_topic=projects/fast-level-up-487010/topics/orders-topic,output_table=fast-level-up-487010:test.orders_stream
"""