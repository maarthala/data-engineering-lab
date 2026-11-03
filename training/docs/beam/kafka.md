# A streaming pipeline: Pub/Sub topics on workers

## Concept
A **Dataflow streaming job** reads a Pub/Sub topic and never ends. It keeps reading, windows the
events by **event time**, and writes a result per window, corrected when late events come. Your
streams from [Events, topics and partitions](../pubsub/events.md) are the lab's Pub/Sub. They are
**Kafka** topics (`<you>.orders`), so Beam reads them with **`ReadFromKafka`** (on Dataflow it
would be `ReadFromPubSub`; the rest of the pipeline is the same).

| Dataflow | Here |
|---|---|
| `ReadFromPubSub(topic=…)` | `ReadFromKafka(**lab_kafka("orders", beam=True))` |
| a batch job | `max_num_records=…`: read that many events, then finish |
| a streaming job | `lab_beam("flink", streaming=True)`: runs until you stop it |
| **Stop** (drain / cancel) in the console | `lab_beam_stop("<name>")` |

`ReadFromKafka` is written in **Java**. Beam runs it next to your Python steps on the same workers:
a **cross-language** pipeline. Dataflow does the same for its Java connectors. You don't see it,
except that the first run takes a little longer.

!!! info "Needs the Flink and Streaming units, and a stream"
    The pipeline runs on the Flink cluster ([Run it on workers](workers.md)) and reads Kafka, so a
    lab manager must have both units on. It reads your **orders** stream: create it in
    **📡 Pub/Sub** if you haven't.

## Lab
### 1 · A batch read: the latest 300 events
**`lab_kafka("orders", beam=True)`** gives `ReadFromKafka` your topic and your Kafka login (the
same one as in [Stream processing in a notebook](../pubsub/stream-processing.md)). Each element is
a **`(key, value)`** pair of bytes; the value is the event's JSON.

```python
import apache_beam as beam, json, os, pandas as pd
from apache_beam.io.kafka import ReadFromKafka
OUT = "s3://<you>-lake/files/beam/"
so = {"key": os.environ["LAKE_S3_KEY"], "secret": os.environ["LAKE_S3_SECRET"],
      "client_kwargs": {"endpoint_url": os.environ["AWS_S3_ENDPOINT"]}}

with beam.Pipeline(options=lab_beam("flink", name="orders-batch", parallelism=2)) as p:
    (p | "Read the topic" >> ReadFromKafka(**lab_kafka("orders", beam=True), max_num_records=300)
       | "Event" >> beam.Map(lambda kv: json.loads(kv[1]))
       | "Type" >> beam.Map(lambda e: (e["event_type"], 1))
       | "Count" >> beam.CombinePerKey(sum)
       | "As text" >> beam.MapTuple(lambda t, n: f"{t},{n}")
       | "Write" >> beam.io.WriteToText(OUT + "order_types", file_name_suffix=".csv", num_shards=1))

pd.read_csv(OUT + "order_types-00000-of-00001.csv", names=["event_type", "events"], storage_options=so)
```
```text
     event_type  events
     order_paid      71
   order_placed     109
  order_shipped      91
order_cancelled      22
order_delivered       7
```

- **`ReadFromKafka(…, max_num_records=300)`** reads 300 events and stops, so the pipeline is a
  **batch** job and the `with` block ends. Your numbers differ: it's your own live stream.
- **`kv[1]`** is the value. **`json.loads`** turns its JSON into a dict.
- It reads as your consumer group **`<you>.beam`**; groups must start with `<you>.`.

### 2 · A streaming job: orders per minute, on time and late
Now without `max_num_records`. The pipeline never ends, so it isn't a `with` block:
**`p.run()`** starts it on the cluster and returns at once.

```python
from datetime import datetime, timezone
from apache_beam import window
from apache_beam.io import fileio
from apache_beam.transforms.trigger import AfterWatermark, AfterProcessingTime, AccumulationMode

def event_time(kv):
    e = json.loads(kv[1])
    t = datetime.fromisoformat(e["event_time"].replace("Z", "+00:00")).timestamp()
    return window.TimestampedValue((e["event_type"], 1), t)

class Line(beam.DoFn):                      # one CSV line per result: minute, type, count, pane, when
    def process(self, kv, w=beam.DoFn.WindowParam, pane=beam.DoFn.PaneInfoParam):
        minute = datetime.fromtimestamp(w.start.micros / 1e6, timezone.utc).strftime("%H:%M")
        timing = {0: "EARLY", 1: "ON_TIME", 2: "LATE"}.get(pane.timing, "UNKNOWN")
        yield f"{minute},{kv[0]},{kv[1]},{timing},{datetime.now(timezone.utc):%H:%M:%S}"

p = beam.Pipeline(options=lab_beam("flink", name="orders-per-minute", parallelism=2, streaming=True))
(p
 | "Read the topic" >> ReadFromKafka(**lab_kafka("orders", beam=True))
 | "Event time" >> beam.Map(event_time)
 | "Per minute" >> beam.WindowInto(window.FixedWindows(60),
                                   trigger=AfterWatermark(late=AfterProcessingTime(20)),
                                   allowed_lateness=120,
                                   accumulation_mode=AccumulationMode.ACCUMULATING)
 | "Count" >> beam.CombinePerKey(sum)
 | "As a line" >> beam.ParDo(Line())
 | "Write" >> fileio.WriteToFiles(OUT + "orders_per_minute/", shards=1,
                                  file_naming=fileio.default_file_naming("orders_per_minute", ".csv")))
p.run()
```

- **`e["event_time"]`** is when the order happened ([Events](../pubsub/events.md)); `fromisoformat`
  reads it (Python wants `+00:00` instead of `Z`). **`TimestampedValue`** makes it the element's
  event time, so windows go by when orders **happened**, not when they arrived.
- **`AfterWatermark(late=AfterProcessingTime(20))`**: a result when the minute is complete (on time),
  then, while late events come, a corrected one at most every 20 seconds.
- **`allowed_lateness=120`**: events up to 2 minutes late still correct their minute.
- **`fileio.WriteToFiles`** writes a stream: new files as results come. (`WriteToText` only works in
  batch.)
- **`streaming=True`** also turns on **checkpoints** every 10 s: Flink saves the job's state and
  Kafka offsets, so a failed worker resumes from the last checkpoint instead of starting over.

**Tools → Flink UI** shows `<you>-orders-per-minute` as **RUNNING**. So does the notebook:

```python
lab_beam_jobs()
```
```text
[('orders-per-minute', 'RUNNING', 23)]
```

**`lab_beam_jobs()`** lists your running jobs: name, state, seconds running
(`lab_beam_jobs(running=False)` lists the finished ones too).

### 3 · Read the results
Give it about five minutes. Every result is a line in a file under `files/beam/orders_per_minute/`.
**`s3fs`** lists them (`fs.glob`) and pandas reads each one:

```python
import s3fs
fs = s3fs.S3FileSystem(key=so["key"], secret=so["secret"], client_kwargs=so["client_kwargs"])
files = fs.glob("<you>-lake/files/beam/orders_per_minute/*.csv")
df = pd.concat(pd.read_csv(fs.open(f), names=["minute", "event_type", "orders", "pane", "written"]) for f in files)
placed = df[df.event_type == "order_placed"].sort_values(["minute", "written"])
placed[placed.minute.between("19:21", "19:22")]
```
```text
minute   event_type  orders    pane  written
 19:21 order_placed     213 ON_TIME 19:22:03
 19:21 order_placed     232    LATE 19:22:27
 19:21 order_placed     235    LATE 19:22:48
 19:21 order_placed     236    LATE 19:23:17
 19:21 order_placed     236    LATE 19:24:01
 19:22 order_placed     199 ON_TIME 19:23:03
 19:22 order_placed     222    LATE 19:23:23
 19:22 order_placed     235    LATE 19:23:45
 19:22 order_placed     236    LATE 19:24:15
 19:22 order_placed     236    LATE 19:25:01
```

The 19:21 minute, as it happened:

- **19:22:03**, just after the minute ended: **213** orders, *ON_TIME*.
- Then late orders kept coming in (phones that were offline, slow networks), and the count was
  **corrected**: 232, 235, 236, about every 20 seconds.
- After 2 minutes (`allowed_lateness`) the minute is closed. Later events for it are dropped.

The first minutes of the file look odd (counts of 0 or 1, *ON_TIME*). Those are the backlog the job
read when it started, from minutes that had already ended. Look at the minutes after the start.

### 4 · Stop the job
A streaming job runs, and holds its worker slots, until you stop it:

```python
lab_beam_stop("orders-per-minute")
lab_beam_jobs()
```
```text
✓ stopped <you>-orders-per-minute
[]
```

**`lab_beam_stop(name)`** stops your job of that name (only your own). In the Flink UI it is now
*CANCELED*. Stop your streaming jobs when you're done: the cluster's slots are shared by everyone.

!!! tip "Spark or Beam?"
    [Stream processing in a notebook](../pubsub/stream-processing.md) did this with Spark: one result
    per window, after the watermark, late events dropped. Beam (and Dataflow) give the on-time result
    **and correct it** for late events. Same topic, same events, different answers to "when is a
    minute done?".

## Check yourself
1. Why does the batch read use a `with` block, and the streaming job `p.run()`?
2. The 19:21 minute went 213 → 232 → 235 → 236. Which settings made that happen?
3. What happens to an order for 19:21 that arrives at 19:25?

??? success "Answers"
    1. A `with` block waits for the pipeline to finish. The batch read finishes after 300 events; a
       streaming job never does, so `p.run()` starts it and returns.
    2. `AfterWatermark(late=AfterProcessingTime(20))`: on time, then corrections at most every 20 s.
       `ACCUMULATING`: each correction counts all orders so far. `allowed_lateness=120` kept the
       minute open for them.
    3. It's dropped. 19:25 is more than 2 minutes (`allowed_lateness`) after the minute ended at 19:22.

## You can now…
- read a Pub/Sub (Kafka) topic in Beam, as a batch read or a streaming job;
- window a stream by event time, with on-time results corrected for late events;
- start, list and stop your streaming jobs on the workers.
