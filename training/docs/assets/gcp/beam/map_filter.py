import apache_beam as beam

ORDERS = [
    # (order_id, channel, status, amount)
    (1, "web", "delivered", 120.0),
    (2, "app", "cancelled", 35.5),
    (3, "web", "delivered", 80.0),
    (4, "marketplace", "shipped", 15.0),
    (5, "app", "delivered", 42.5),
]

with beam.Pipeline() as p:
    (
        p
        | "Orders" >> beam.Create(ORDERS)
        | "Only delivered" >> beam.Filter(lambda o: o[2] == "delivered")
        | "Id and amount" >> beam.Map(lambda o: f"order {o[0]}: {o[3]:.2f}")
        | "Print" >> beam.Map(print)
    )
