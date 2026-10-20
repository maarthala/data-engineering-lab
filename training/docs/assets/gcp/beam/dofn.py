import apache_beam as beam

LINES = ["1,web,delivered", "2,app,DELIVERED", "x,web,shipped", "4,app,lost"]
STATUSES = {"placed", "shipped", "delivered", "cancelled"}


class ParseOrder(beam.DoFn):
    def process(self, line):
        order_id, channel, status = line.split(",")
        status = status.lower()
        if order_id.isdigit() and status in STATUSES:
            yield {"order_id": int(order_id), "channel": channel, "status": status}
        else:
            yield beam.pvalue.TaggedOutput("bad", line)


with beam.Pipeline() as p:
    parsed = (
        p
        | "Lines" >> beam.Create(LINES)
        | "Parse" >> beam.ParDo(ParseOrder()).with_outputs("bad", main="good")
    )
    parsed.good | "Print good" >> beam.Map(lambda row: print("good:", row))
    parsed.bad | "Print bad" >> beam.Map(lambda line: print("bad: ", line))
