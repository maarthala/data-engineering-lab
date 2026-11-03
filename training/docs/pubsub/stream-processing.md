# Stream processing in a notebook: watermarks and windows

## Concept
The last lesson waited for late events by re-computing a lookback window in batch. A **stream
processor** works the other way round: it reads the topic continuously and keeps **state** in
memory (e.g. a count for each minute), updating it as events arrive. Two questions follow:

- **When is a minute finished?** Never quite: a late event can always come. So you choose how long
  to wait. That's the **watermark**: *the latest event time seen, minus a delay you allow*, e.g.
  10 minutes. Once the watermark has passed a minute's end, that minute is **closed**. Its result
  is written, its state dropped, and events for it that still come are **dropped as too late**.
- **What's a "minute"?** That's the **window**:

| Window | Groups events into | Example |
|---|---|---|
| **tumbling** | fixed, back-to-back intervals | orders per minute: 14:25–14:26, 14:26–14:27 … |
| **sliding** | fixed intervals that overlap | orders in the last 5 minutes, every minute |
| **session** | runs of activity, ended by a gap | one visitor's clicks until 30 s of nothing |

The longer the delay, the more late events count, and the longer you wait for each result.
That's the trade-off to set. **Spark Structured Streaming** does all of this in a notebook. It runs
on the lab's Spark cluster, so the **Spark** and **Streaming** units must both be on.

## Lab
Open a notebook. You need the streams from [Events, topics and partitions](events.md).

### 1 · Your Kafka login
Your first stream gave you a Kafka login, kept in 🔑 **My secrets** as the secret `kafka`:

```python
sorted(key_vault_dict("kafka"))
```
```text
['bootstrap', 'password', 'username']
```

It may **read** your own topics and use consumer groups whose name starts with `<you>.`. Other
members' topics and publishing are refused. **`lab_kafka()`** puts the login into the settings Kafka
needs, so the password never sits in your notebook.

### 2 · Subscribe from plain Python
`confluent_kafka` is a Kafka client for Python. A **`Consumer`** in group `<you>.first`, reading
from the **earliest** event:

```python
import json
from confluent_kafka import Consumer

c = Consumer({**lab_kafka(python=True), "group.id": "<you>.first",
              "auto.offset.reset": "earliest", "enable.auto.commit": False})
c.subscribe(["<you>.orders"])
got = 0
while got < 5:
    m = c.poll(5.0)                        # the next event, or None after 5 s without one
    if m is None or m.error():
        continue
    e = json.loads(m.value())              # value: the event's JSON, as bytes
    print(m.partition(), m.offset(), m.key().decode(), e["event_type"], e["event_time"])
    got += 1
c.commit(asynchronous=False)               # save the group's bookmark: "read up to here"
c.close()
```
```text
2 0 o_17b792cc0d66 order_placed 2026-11-03T14:25:33.416Z
2 1 o_7d54e7959028 order_placed 2026-11-03T14:25:34.188Z
2 2 o_41c2fa2eeaab order_placed 2026-11-03T14:25:34.743Z
2 3 o_ef8258b19003 order_placed 2026-11-03T14:25:36.100Z
2 4 o_56edd3bc4c16 order_placed 2026-11-03T14:25:36.904Z
```

- **`auto.offset.reset: earliest`**: where a **new** group starts. `latest` would mean only events
  from now on.
- **`commit()`**: saves the group's offsets. Run the same cell again and the first event is
  `2 5`: the group goes on where it left off. A new group name starts from the beginning again.

### 3 · The topic as a Spark table
Spark reads Kafka like any other source: `format("kafka")` with `lab_kafka("orders")`, which also
says which topic to subscribe to:

```python
raw = spark.read.format("kafka").options(**lab_kafka("orders")).load()
raw.printSchema()
```
```text
root
 |-- key: binary (nullable = true)
 |-- value: binary (nullable = true)
 |-- topic: string (nullable = true)
 |-- partition: integer (nullable = true)
 |-- offset: long (nullable = true)
 |-- timestamp: timestamp (nullable = true)
 |-- timestampType: integer (nullable = true)
```

Every message is a row; the event is in `value`, as bytes. `from_json` ([JSON](../unit2/json.md))
turns it into columns. It needs the event's schema, written like a table's columns (the stream's
page shows it under **🧪 Use it in a notebook**):

```python
from pyspark.sql.functions import from_json, col

schema = ("event_id STRING, event_type STRING, event_time TIMESTAMP, publish_time TIMESTAMP, order_id STRING, "
          "customer_id BIGINT, items ARRAY<STRUCT<product_id: BIGINT, quantity: INT, price: DOUBLE>>, "
          "total DOUBLE, currency STRING, country STRING")
events = raw.select(from_json(col("value").cast("string"), schema).alias("e")).select("e.*")
events.select("order_id", "event_type", "event_time", "total").show(3, truncate=False)
```
```text
+--------------+------------+-----------------------+------+
|order_id      |event_type  |event_time             |total |
+--------------+------------+-----------------------+------+
|o_d5c3b702b9bb|order_placed|2026-11-03 14:25:32.865|1302.6|
|o_18d41f27e2b9|order_placed|2026-11-03 14:25:33.437|814.16|
|o_0a4ee4499233|order_placed|2026-11-03 14:25:32.717|392.87|
+--------------+------------+-----------------------+------+
```

`spark.read` is a **batch** read: what the topic holds now, once.

### 4 · A stream: tumbling windows with a watermark
`spark.readStream` is the same read, **continuously**: a streaming DataFrame that grows as events
arrive. On it:

- **`withWatermark("event_time", "2 minutes")`**: how long to wait for late events.
- **`window("event_time", "1 minute")`**: the tumbling window, a column with each event's
  minute (`window.start`, `window.end`).
- **`writeStream … toTable(…)`** starts the query and writes to an Iceberg table:
  - `outputMode("append")`: each window is written **once**, when it closes;
  - `trigger(processingTime="30 seconds")`: a **micro-batch** every 30 s;
  - `checkpointLocation`: where Spark keeps its offsets and state (next step).

```python
from pyspark.sql.functions import window, count

spark.conf.set("spark.sql.shuffle.partitions", "3")    # one per Kafka partition (the default, 200, is slow)
spark.sql("CREATE NAMESPACE IF NOT EXISTS my.silver")

stream = (spark.readStream.format("kafka").options(**lab_kafka("orders"))
          .option("startingOffsets", "earliest").load()
          .select(from_json(col("value").cast("string"), schema).alias("e")).select("e.*")
          .withWatermark("event_time", "2 minutes"))
per_minute = stream.groupBy(window("event_time", "1 minute"), "event_type").agg(count("*").alias("events"))

q = (per_minute.writeStream.format("iceberg").outputMode("append")
     .option("checkpointLocation", "s3a://<you>-lake/checkpoints/orders_per_minute")
     .trigger(processingTime="30 seconds")
     .toTable("my.silver.orders_per_minute"))
```

The cell returns at once: `q` runs in the background on the cluster. **`q.lastProgress`** reports
the last micro-batch. Print the parts that matter every 30 seconds:

```python
import time
for _ in range(7):
    time.sleep(30)
    p = q.lastProgress
    s = p["stateOperators"][0]
    print(f"batch {p['batchId']}: watermark {p['eventTime']['watermark']} · "
          f"windows kept {s['numRowsTotal']} · dropped as too late {s['numRowsDroppedByWatermark']}")
```
```text
batch 1: watermark 2026-11-03T14:26:44.283Z · windows kept 13 · dropped as too late 0
batch 2: watermark 2026-11-03T14:27:00.545Z · windows kept 12 · dropped as too late 1
batch 3: watermark 2026-11-03T14:27:29.439Z · windows kept 12 · dropped as too late 4
batch 4: watermark 2026-11-03T14:27:59.431Z · windows kept 16 · dropped as too late 4
batch 5: watermark 2026-11-03T14:28:29.363Z · windows kept 12 · dropped as too late 3
batch 6: watermark 2026-11-03T14:28:59.660Z · windows kept 16 · dropped as too late 6
batch 7: watermark 2026-11-03T14:29:29.713Z · windows kept 12 · dropped as too late 6
```

- The **watermark** moves forward about 30 s per batch, always 2 minutes behind the newest event.
- **Windows kept** are the open ones (a minute × event type), still waiting.
- **Dropped as too late**: events that came after the watermark had passed their minute. This stream
  sends some up to 10 minutes late, and with 2 minutes of patience those are lost to this table.
  They're still in Bronze ([Land events in Bronze](bronze.md)).

Only closed minutes are in the table:

```python
q.stop()
spark.sql("""SELECT window.start, window.end, event_type, events FROM my.silver.orders_per_minute
             WHERE event_type = 'order_placed' ORDER BY window.start""").show(truncate=False)
```
```text
+-------------------+-------------------+------------+------+
|start              |end                |event_type  |events|
+-------------------+-------------------+------------+------+
|2026-11-03 14:25:00|2026-11-03 14:26:00|order_placed|216   |
|2026-11-03 14:26:00|2026-11-03 14:27:00|order_placed|292   |
|2026-11-03 14:27:00|2026-11-03 14:28:00|order_placed|220   |
|2026-11-03 14:28:00|2026-11-03 14:29:00|order_placed|245   |
+-------------------+-------------------+------------+------+
```

**`q.stop()`** ends the query. A query also stops when your notebook's session ends; stop yours
when you're done, because the cluster is shared.

### 5 · Restart: the checkpoint
Run the `q = (per_minute.writeStream …)` cell of step 4 again, with the same `checkpointLocation`.
Wait a batch or two, then look at its first batch, and check that no window was written twice:

```python
time.sleep(45)
print("first batch after restart:", q.recentProgress[0]["batchId"])
q.stop()
spark.sql("""SELECT window, event_type, count(*) AS n FROM my.silver.orders_per_minute
             GROUP BY window, event_type HAVING count(*) > 1""").count()
```
```text
first batch after restart: 8
0
```

`recentProgress` is the list of the last batches' reports. The query before stopped after batch 7.

The checkpoint (in your bucket) holds which offsets each batch read and the open windows' state.
So a restart goes on from batch 8, with nothing read twice and nothing missed. A new
`checkpointLocation` would start over from `startingOffsets`.

### 6 · Session windows: visits
**`session_window("event_time", "30 seconds")`** starts a session at a visitor's first click and
ends it 30 s after their last. That's sessionization, from the clicks stream:

```python
from pyspark.sql.functions import session_window

cs = ("event_id STRING, event_type STRING, event_time TIMESTAMP, publish_time TIMESTAMP, session_id STRING, "
      "customer_id BIGINT, device STRING, page STRING, product_id BIGINT, search_term STRING, referrer STRING")
clicks = (spark.readStream.format("kafka").options(**lab_kafka("clicks")).option("startingOffsets", "earliest").load()
          .select(from_json(col("value").cast("string"), cs).alias("e")).select("e.*")
          .withWatermark("event_time", "1 minute"))
sessions = clicks.groupBy(session_window("event_time", "30 seconds"), "session_id", "device").agg(count("*").alias("events"))
q2 = (sessions.writeStream.format("iceberg").outputMode("append")
      .option("checkpointLocation", "s3a://<you>-lake/checkpoints/click_sessions")
      .trigger(processingTime="30 seconds").toTable("my.silver.click_sessions"))
```

After a couple of minutes, `q2.stop()`, then:

```python
spark.sql("""SELECT device, count(*) AS sessions, round(avg(events), 1) AS avg_events
             FROM my.silver.click_sessions GROUP BY device ORDER BY device""").show()
```
```text
+-------+--------+----------+
| device|sessions|avg_events|
+-------+--------+----------+
|android|      55|       9.5|
|    ios|      60|      10.5|
|    web|      96|      11.6|
+-------+--------+----------+
```

The offline bursts don't break sessions: their events carry the time they happened, so they fall
into the right session, as long as they come within the watermark.

!!! tip "Sliding windows"
    `window("event_time", "5 minutes", "1 minute")`: 5-minute windows, a new one every minute, so
    each event counts in five of them. Everything else stays the same.

## Check yourself
1. A watermark of 2 minutes, and an event arrives 3 minutes late. What happens to it, and where can
   you still find it?
2. Why does `outputMode("append")` write a minute only once, and only after a delay?
3. You stop a query and start it again with the **same** checkpoint, and then with a **new** one.
   What's the difference?

??? success "Answers"
    1. Its minute closed before it came, so the query drops it (it's counted in
       `numRowsDroppedByWatermark`). It's still in Kafka for 24 h, and in Bronze if an Ingestion
       job lands the stream.
    2. Append never changes a row once written. So Spark writes a window only when the watermark has
       passed it and no event can change it any more.
    3. Same checkpoint: it goes on from where it stopped, with no duplicates. New checkpoint: it
       starts over from `startingOffsets`, and writes those windows again.

## You can now…
- subscribe to your topic from Python (consumer group, offsets, commit) and from Spark;
- write a streaming query with a watermark and tumbling or session windows into an Iceberg table;
- read a query's progress: its watermark, its open windows, the events it dropped;
- restart a query from its checkpoint.
