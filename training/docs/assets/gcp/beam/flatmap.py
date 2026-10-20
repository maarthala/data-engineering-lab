import apache_beam as beam

BASKETS = ["laptop,mouse", "desk", "pen,paper,stapler"]

with beam.Pipeline() as p:
    (
        p
        | "Baskets" >> beam.Create(BASKETS)
        | "One per product" >> beam.FlatMap(lambda basket: basket.split(","))
        | "Print" >> beam.Map(print)
    )
