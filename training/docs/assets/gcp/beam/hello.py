import apache_beam as beam

with beam.Pipeline() as p:
    (
        p
        | "Channels" >> beam.Create(["web", "app", "web", "marketplace"])
        | "Upper case" >> beam.Map(str.upper)
        | "Print" >> beam.Map(print)
    )
