"""ShopFlow orders: raw CSV files in Cloud Storage -> clean rows in BigQuery (Silver),
and the lines it can't read -> back to Cloud Storage (rejected).

Test it in Cloud Shell (DirectRunner), then run the same file on Dataflow (DataflowRunner).
"""
import argparse
import csv
from datetime import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

STATUSES = {"placed", "shipped", "delivered", "cancelled"}
SCHEMA = ("order_id:INTEGER,customer_id:INTEGER,channel:STRING,order_ts:TIMESTAMP,"
          "status:STRING,currency:STRING,promo_id:INTEGER")


def clean(line):
    """One CSV line -> a clean row (main output), or the raw line (tagged 'rejected')."""
    try:
        order_id, customer_id, channel, order_ts, status, currency, promo_id = next(csv.reader([line]))
        row = {
            "order_id": int(order_id),
            "customer_id": int(customer_id),
            "channel": channel.strip().lower(),
            "order_ts": datetime.strptime(order_ts, "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S"),
            "status": status.strip().lower(),
            "currency": currency.strip().upper() or "USD",
            "promo_id": int(promo_id) if promo_id else None,
        }
        if row["status"] not in STATUSES:
            raise ValueError(f"unknown status {status!r}")
        yield row
    except ValueError:
        yield beam.pvalue.TaggedOutput("rejected", line)


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)         # gs://<bucket>/shopflow/orders/*.csv
    parser.add_argument("--output_table", required=True)  # <project>:shopflow_silver.orders
    parser.add_argument("--rejected", required=True)      # gs://<bucket>/rejected/orders
    args, beam_args = parser.parse_known_args()

    with beam.Pipeline(options=PipelineOptions(beam_args)) as p:
        rows = (
            p
            | "Read CSV" >> beam.io.ReadFromText(args.input, skip_header_lines=1)
            | "Drop duplicate lines" >> beam.Distinct()
            | "Clean" >> beam.FlatMap(clean).with_outputs("rejected", main="clean")
        )
        rows.clean | "Write Silver" >> beam.io.WriteToBigQuery(
            args.output_table,
            schema=SCHEMA,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
        )
        rows.rejected | "Write rejected" >> beam.io.WriteToText(args.rejected, file_name_suffix=".csv")


if __name__ == "__main__":
    run()
