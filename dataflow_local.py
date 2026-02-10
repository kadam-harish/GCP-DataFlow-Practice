import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


class CleanClicksDoFn(beam.DoFn):
    def process(self, element):
        yield element.split(',')

def run():
    options = PipelineOptions()

    p= beam.Pipeline(options=options) 
    (
            p
            | "Read CSV" >> beam.io.ReadFromText("sales.csv")
            | "Skip header" >> beam.Filter(lambda row: not row.startswith("order_id"))
            | "split columns" >> beam.ParDo(CleanClicksDoFn())

            | "Format for BQ" >> beam.Map(lambda cols: {
                "order_id": cols[0],
                "amount": float(cols[1]),
                "city": cols[2]
            })
            | "Print Output" >> beam.Map(print)
            | "Write to file" >> beam.io.WriteToText("output/res.csv")
    )
    p.run().wait_until_finish()

if __name__ == "__main__":
    run()
