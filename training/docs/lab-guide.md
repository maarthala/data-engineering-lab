# Lab guide

How to use the lab's portal features: what each one does, and the steps. The lessons use them; this
page is the reference to come back to.

## 📨 Pub/Sub streams

A **stream** is a Kafka topic of your own, `<you>.<name>`, that the lab's simulator fills with
business events: a few every second, some of them late if you ask for it. Lessons:
[Events, topics and partitions](pubsub/events.md) and the ones after it.

### Make a stream
**📨 Pub/Sub → + New stream**, then:

| Choice | What it means |
|---|---|
| **Carries** | *Click events*, *Orders*, *Payments*, *Stock moves*, or a **Custom schema** you build yourself |
| **Name** | lowercase letters, digits and `_`; the topic is `<you>.<name>` |
| **Events a second** | 1–20 (the most a lab manager allows) |
| **Late events** | off, 5 % or 20 %; how late (1 min … 1 day); *random delays* or *offline bursts* |
| **Create & start** / **Create** | start publishing now, or make the stream and press **▶ Start** later |

A stream pauses itself after 8 hours (**▶ Resume** starts it again), and its topic keeps events for 24 hours.

### Custom schema
Build the event from a library of about 150 ready-made fields (customer, location, device, product,
order, payment, IoT …):

- **Drag** a field from the library onto an object (it goes inside), onto a field (it goes before it),
  or onto empty space (the top). **⠿** moves a field. Or click a field to add it; **+ add here** on an
  object makes clicks go into it.
- **+ Object** groups fields (`{…}`); **list** makes a field or an object an array (from … to elements).
- Per field: its name, how often it's **empty**, your **own values** (text fields), **send as text**,
  and the message **key**.
- The sample event on the right changes as you edit.

Every event also has `event_id`, `event_time`, `publish_time` and `schema_revision`.

**🧬 Schema → ✎ Change schema** saves a new **revision**: all events at once, or 50 % / 10 % of them for
a while (old and new shapes on one topic, like apps that update one by one). The revisions list shows
what changed. A field keeps its type in every revision; a new shape needs a new name, as a schema
registry would insist.

### Look at the events
**👀 Peek** shows the last events sent: partition · offset, key, event time, and how late each one was
(🕓). Click one for its JSON.

### Land them in your lakehouse
Kafka keeps events 24 hours. To keep them, land them in an Iceberg table with a **📡 Ingestion** job:

1. Make the stream a source: **📡 Ingestion → Sources → + New source**, **Source: 📨 Pub/Sub stream**,
   pick it in **Stream** → **Save source**.
   Or, on the stream's page, **🏞 Lakehouse → + New Ingestion job**: it adds the source and opens the
   new job with it chosen. (Removing the source later leaves the stream as it is.)
2. **+ New job**: the source is your stream.
3. Pick a destination: a catalog and namespace. The table is named after the stream.
4. Pick how it runs: **Manual**, **Cron** (an expression) or **Datastream** (all the time, within seconds).

Every event becomes a row (Bronze), typed, with `_kafka` (topic, partition, offset, timestamp). A custom
stream's table starts with its current revision; a field a later revision adds gets its own column. Build
Silver from it with SQL, dbt or Spark: [Silver and Gold from late events (batch)](pubsub/late-batch.md).

### Use it in a notebook
Your stream's Kafka login is in 🔑 **My secrets** (`kafka`); `lab_kafka()` puts it into the settings, so
the password never sits in your notebook. You may read your own topics, with consumer groups named
`<you>.…`.

**Spark Structured Streaming**: a watermark, a tumbling window, closed windows into an Iceberg table
([Stream processing in a notebook](pubsub/stream-processing.md)):

```python
from pyspark.sql.functions import from_json, col, window, count

spark.conf.set("spark.sql.shuffle.partitions", "3")   # one per Kafka partition
schema = "<the stream's Spark schema — below>"

raw = (spark.readStream.format("kafka").options(**lab_kafka("<stream>"))
       .option("startingOffsets", "earliest").load())        # key, value, partition, offset, timestamp
events = (raw.select(from_json(col("value").cast("string"), schema).alias("e")).select("e.*")
             .withWatermark("event_time", "10 minutes"))     # wait 10 min for late events

per_minute = (events.groupBy(window("event_time", "1 minute"), "event_type")   # a tumbling window
                    .agg(count("*").alias("events")))

spark.sql("CREATE NAMESPACE IF NOT EXISTS my.silver")
q = (per_minute.writeStream.format("iceberg").outputMode("append")   # a window is written once it closes
       .option("checkpointLocation", "s3a://<you>-lake/checkpoints/<stream>_per_minute")
       .trigger(processingTime="30 seconds")
       .toTable("my.silver.<stream>_per_minute"))
# q.status · q.lastProgress · q.stop()
```

The Spark schema of each kind:

| Carries | `schema` |
|---|---|
| Click events | `event_id STRING, event_type STRING, event_time TIMESTAMP, publish_time TIMESTAMP, session_id STRING, customer_id BIGINT, device STRING, page STRING, product_id BIGINT, search_term STRING, referrer STRING` |
| Orders | `event_id STRING, event_type STRING, event_time TIMESTAMP, publish_time TIMESTAMP, order_id STRING, customer_id BIGINT, items ARRAY<STRUCT<product_id: BIGINT, quantity: INT, price: DOUBLE>>, total DOUBLE, currency STRING, country STRING` |
| Payments | `event_id STRING, event_type STRING, event_time TIMESTAMP, publish_time TIMESTAMP, payment_id STRING, order_id STRING, customer_id BIGINT, amount DOUBLE, currency STRING, method STRING, reason STRING` |
| Stock moves | `event_id STRING, event_type STRING, event_time TIMESTAMP, publish_time TIMESTAMP, sku STRING, store STRING, qty_change INT, reason STRING` |
| Custom schema | its own: **🧬 Schema** shows it, with a Copy button. Without an `event_type` field, group by `window(…)` alone |

**Plain Python**, a few events at a time ([Stream processing in a notebook](pubsub/stream-processing.md) explains each line):

```python
import json
from confluent_kafka import Consumer

c = Consumer({**lab_kafka(python=True), "group.id": "<you>.reader",
              "auto.offset.reset": "earliest", "enable.auto.commit": False})
c.subscribe(["<you>.<stream>"])
got = 0
while got < 5:
    m = c.poll(5.0)                        # the next event, or None after 5 s without one
    if m is None or m.error():
        continue
    print(m.partition(), m.offset(), m.key().decode(), json.loads(m.value())["event_time"])
    got += 1
c.commit(asynchronous=False)               # the group's bookmark: "read up to here"
c.close()
```

**Apache Beam** on the Flink unit: `ReadFromKafka(**lab_kafka("<stream>", beam=True))`, as a batch read
or a streaming job with windows and late data:
[A streaming pipeline: Pub/Sub topics on workers](beam/kafka.md).
