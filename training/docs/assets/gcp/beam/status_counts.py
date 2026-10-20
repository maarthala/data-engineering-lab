import argparse
import csv

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


def status_of(line):
    """The 5th CSV value, trimmed and lower-cased: ' Delivered ' -> 'delivered'."""
    return next(csv.reader([line]))[4].strip().lower()


parser = argparse.ArgumentParser()
parser.add_argument("--input", required=True)
parser.add_argument("--output", required=True)
args, beam_args = parser.parse_known_args()

with beam.Pipeline(options=PipelineOptions(beam_args)) as p:
    (
        p
        | "Read" >> beam.io.ReadFromText(args.input, skip_header_lines=1)
        | "Status" >> beam.Map(status_of)
        | "Count" >> beam.combiners.Count.PerElement()
        | "Format" >> beam.MapTuple(lambda status, n: f"{status},{n}")
        | "Write" >> beam.io.WriteToText(args.output, file_name_suffix=".csv",
                                         header="status,lines", num_shards=1)
    )
