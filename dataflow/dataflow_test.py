import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

class MyOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument("--input_path")
        parser.add_value_provider_argument("--output_table")

def run():
    options = MyOptions()
    with beam.Pipeline(options=options) as p:
        formatted= (
            p
            | "Read CSV" >> beam.io.ReadFromText(options.input_path)
            | "Skip Header" >> beam.Filter(lambda row : not row.startswith("order_id"))
            | "Split columns" >> beam.Map(lambda row: row.split(","))
            | "Format for BQ" >> beam.Map(lambda cols: {
                "order_id": cols[0],
                "amount": float(cols[1]),
                "city": cols[2]
            })
            
        )

        formatted | "Write to GCS" >> beam.io.WriteToText(
            "gs://dataflow_practice_101/output/res"
        )

        formatted |"Write to BigQuery" >> beam.io.WriteToBigQuery(
                table=options.output_table,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )



if __name__ == "__main__":
    run()



"""
python3 your_pipeline_file.py \
  --runner DataflowRunner \
  --project YOUR_GCP_PROJECT \
  --region asia-south1 \
  --temp_location gs://dataflow_practice_101/temp \
  --staging_location gs://dataflow_practice_101/staging \
  --template_location gs://dataflow_practice_101/templates/csv_to_gcs_bq

"""