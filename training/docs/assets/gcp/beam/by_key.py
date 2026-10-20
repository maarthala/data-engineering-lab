import apache_beam as beam

ORDERS = [
    (1, "web", "delivered", 120.0),
    (2, "app", "cancelled", 35.5),
    (3, "web", "delivered", 80.0),
    (4, "marketplace", "shipped", 15.0),
    (5, "app", "delivered", 42.5),
]

with beam.Pipeline() as p:
    orders = p | "Orders" >> beam.Create(ORDERS)

    (
        orders
        | "Channel, amount" >> beam.Map(lambda o: (o[1], o[3]))
        | "Sum per channel" >> beam.CombinePerKey(sum)
        | "Print revenue" >> beam.Map(print)
    )
    (
        orders
        | "Channel, id" >> beam.Map(lambda o: (o[1], o[0]))
        | "Group per channel" >> beam.GroupByKey()
        | "Sorted ids" >> beam.MapTuple(lambda channel, ids: (channel, sorted(ids)))
        | "Print groups" >> beam.Map(print)
    )
