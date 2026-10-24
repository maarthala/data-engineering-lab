# Pub/Sub and Dataflow (Apache Beam) from the inside

> **Goal of this lesson:** understand how Google's streaming stack works inside, well enough to design a pipeline, justify your choices, and answer the "why" questions.
>
> **Quick reference:** [streaming and CDC deep dive](../deep-dives/streaming-cdc.md). **CDC (Datastream)** is in the next lesson: [Datastream and CDC](datastream-cdc.md).
>
> **Remember:** windows, watermarks and allowed lateness belong **here**, in event streaming. They do **not** belong in a CDC design.

---

## 1. What it is and why it exists

### Pub/Sub in plain language
**Pub/Sub** is Google's managed **message bus**. Producers ("publishers") send small messages to a named channel (a **topic**). Consumers ("subscribers") read them through a **subscription**. Pub/Sub stores each message durably until every subscription has confirmed it (**acknowledged** it), or until retention runs out.

It exists to **decouple** producers from consumers:

- The producer does not need to know who reads the data, or whether they are online.
- A slow consumer does not slow down the producer. Messages wait in the backlog.
- You can add a new consumer later (a new subscription) without touching the producer.

**Analogy: a post office with personal mailboxes.** A sender drops a letter at the post office (topic). The post office makes a copy for every mailbox that has registered for that sender (subscriptions). Each mailbox owner takes the letter and signs for it (ack). If they don't sign within a time limit, the post office assumes the letter was lost and delivers it again. That is why you sometimes get the same letter twice.

### Dataflow and Apache Beam in plain language
**Apache Beam** is an open-source **programming model** (a library) for data pipelines. You describe *what* to compute: read, transform, group, aggregate, write. The same code can run as **batch** (finite data) or **streaming** (infinite data).

**Dataflow** is Google's fully managed **runner** for Beam. It takes your pipeline, turns it into an optimized execution graph, starts workers (VMs), distributes the data, scales up and down, recovers from failures, and gives you exactly-once processing for streaming.

**Analogy: a recipe and a restaurant kitchen.** Beam is the recipe ("cut, then group by table, then cook for 5 minutes"). Dataflow is the kitchen manager who hires cooks, assigns work, replaces a cook who gets sick, and makes sure each dish is served exactly once.

### Why this pair exists
Kafka + self-managed Spark (what you used on AWS) gives you power, but you run brokers, partitions, clusters, checkpoints and upgrades yourself. Pub/Sub + Dataflow are **serverless**: no brokers, no partitions to size, no cluster to patch. You pay for usage and focus on the logic.

---

## 2. Where it fits in a modern GCP data platform

A typical enterprise data warehouse (EDW) modernization on GCP uses BigQuery, Cloud Composer, **Dataflow** and Datastream, with reusable ingestion patterns, CDC and **streaming**. A common goal is a **near real-time sales data model** in BigQuery, with Looker on top.

Where Pub/Sub + Dataflow show up in that work:

| Platform task | Role of Pub/Sub / Dataflow |
|---|---|
| Near real-time sales | POS (point-of-sale) events or web-shop orders arrive as **events** on Pub/Sub; Dataflow cleans and aggregates them into BigQuery |
| Reusable ingestion patterns | **Flex Templates** = a parameterized, containerized pipeline that every team can launch with its own topic/table |
| Shared ingestion projects | Topics, subscriptions and Dataflow jobs live in an ingestion project; BigQuery datasets in another; IAM via service accounts |
| Load metrics (duration, throughput, stability, cost) | Dataflow metrics: **system lag, data freshness, backlog, throughput**, plus Pub/Sub **oldest unacked message age** |
| CDC | Datastream does the CDC. Dataflow is used only when you need transformation in flight (Datastream → GCS → Dataflow → BigQuery) |

```text
                      SHARED INGESTION PROJECT                         EDW PROJECT
 POS / web shop  ──►  Pub/Sub topic ──► subscription ──► Dataflow  ──► BigQuery raw / staging
  (events)              (schema)          (DLQ)         (Flex Tmpl)          │
                                                                              ▼
 Operational DB ──►  Datastream (CDC) ─────────────────────────────►  BigQuery replica tables
                                                                              │
                                                       Dataform/dbt models ◄──┘
                                                              │
                                                              ▼
                                                     Sales mart  ──► Looker (conversational BI)
```

**Key principle:** *"I don't start with Dataflow. I start with the simplest managed option (BigQuery subscription, Datastream direct, BigQuery SQL) and only add Dataflow when there is real stream-processing logic: parsing, enrichment, windowed aggregation, routing, dead-lettering."*

---

## 3. Architecture: how it works under the hood

### 3.1 Pub/Sub internals

Pub/Sub is a **global** service. You don't create it in a region; topics and subscriptions are global resources. But **messages are stored in regions**.

```text
  Publisher (client library: batching, retries, flow control)
       │  publish(data, attributes, ordering_key)
       ▼
  ┌──────────────── Pub/Sub DATA PLANE ─────────────────────────────┐
  │                                                                  │
  │  Publishing forwarders (nearest region, or allowed regions by    │
  │  message storage policy)                                         │
  │       │ writes message to storage in several zones               │
  │       │ returns message_id to publisher only AFTER it is durable │
  │       ▼                                                          │
  │  Message storage  ── one logical copy per topic, tracked per     │
  │       │              subscription (acked / not acked)            │
  │       ▼                                                          │
  │  Subscribing forwarders ── deliver to subscribers, track         │
  │                            ack deadlines, redeliver              │
  └──────────────────────────────────────────────────────────────────┘
       │  CONTROL PLANE assigns publishers/subscribers to forwarders
       ▼
  Subscription A (Dataflow pull)   Subscription B (BigQuery sub)   Subscription C (push → Cloud Run)
```

Key points:

1. **Durability before ack to the publisher.** The publish call returns a `message_id` only after the message is stored in more than one zone. If the publisher gets an error or a timeout, it retries. A retry after a timeout can create a **duplicate** (the first write may have succeeded). This is one source of at-least-once.
2. **One copy per topic, state per subscription.** Pub/Sub does not copy the data N times for N subscriptions. Each subscription tracks which messages it has acknowledged.
3. **Message storage policy** can restrict which regions store the data (data residency, e.g. EU only).
4. **Limits to know (check current docs):** max message size 10 MB (data + attributes); attributes are key/value strings; topic retention up to 31 days; subscription retention 10 minutes to 7 days (default 7 days).

### 3.2 The delivery cycle: lease, ack, redelivery

```text
 time ──────────────────────────────────────────────────────────────────►

 Pub/Sub:  deliver m1 ─┐
                       │ ack deadline (default 10 s, max 600 s)
 Subscriber:           └─ processing ... modAckDeadline (extend) ... ack ✔   → m1 removed for this sub

 Pub/Sub:  deliver m2 ─┐
                       │ ack deadline
 Subscriber:           └─ processing ... worker crashes ✘
 Pub/Sub:                              deadline expires ─► redeliver m2 (to any subscriber)
```

- **Ack deadline**: how long the subscriber has to ack before Pub/Sub redelivers. Default 10 seconds, max 600 seconds.
- **Lease management**: client libraries (and Dataflow) automatically send `modifyAckDeadline` to extend the lease while processing.
- **Nack**: the subscriber can say "I can't process this now". It is redelivered quickly.
- **Retry policy**: immediate redelivery or **exponential backoff** (minimum and maximum delay).

### 3.3 Why at-least-once is the default
Pub/Sub is a distributed system. Any of these can cause a duplicate:

- The publisher retries after a timeout, but the first publish succeeded (**publish-side duplicate**, with a *different* `message_id`).
- The subscriber processed the message but the ack was lost or arrived after the deadline (**delivery-side duplicate**, with the *same* `message_id`).
- A forwarder fails over and redelivers unacked messages.

Guaranteeing "exactly once" across a network needs coordination, which costs latency. So the default is "**at least once, very rarely twice**", and **the consumer must be idempotent**.

### 3.4 Dataflow internals (overview)

```text
  Your Beam code (Python/Java)
        │  pipeline graph (PTransforms + PCollections)
        ▼
  Dataflow service (control plane, regional)
        │  1. validates graph, 2. optimizes it (FUSION, combiner lifting),
        │  3. creates a job, 4. starts worker VMs, 5. assigns work, 6. autoscales
        ▼
  ┌────────────── Workers (Compute Engine VMs, your project) ──────────────┐
  │  SDK harness (runs your DoFns in Python)  ◄──►  Runner harness (Java)   │
  │  fused stages: Read → Parse → Window → partial Combine                  │
  └──────────────────────────────┬──────────────────────────────────────────┘
                                 │ shuffle (grouping by key)
                                 ▼
  STREAMING: Streaming Engine (Google backend: shuffle + state + timers)
  BATCH:     Dataflow Shuffle service (Google backend)
                                 │
                                 ▼
  Workers: final Combine → Format → Write (BigQuery Storage Write API)
```

- The **workers** are VMs in your project. Python code runs in an **SDK harness** container. Dataflow talks to it through the Beam **Fn API** ("portable" architecture).
- **Shuffle and state** do not live on the workers when you use **Streaming Engine** (streaming) or **Dataflow Shuffle** (batch). This is what makes autoscaling smooth: a worker can be removed without moving large state.

---

## 4. Core concepts, one by one

Each concept follows the same pattern: **what → how it works inside → why it matters → example → common mistake.**

### 4.1 Topic and subscription

- **What:** a topic is the channel you publish to. A subscription is a named, durable cursor that receives **every** message published to the topic after it was created.
- **Inside:** fan-out is "one stored copy + per-subscription ack state". Within one subscription, messages are **load-balanced** across all connected subscribers.
- **Why it matters:** a new consumer = a new subscription, with zero impact on others. Two Dataflow jobs on the **same** subscription would **split** the messages, not duplicate them.
- **Example:**

  ```bash
  gcloud pubsub topics create pos-sales --message-retention-duration=7d
  gcloud pubsub subscriptions create pos-sales-dataflow \
      --topic=pos-sales --ack-deadline=60 \
      --dead-letter-topic=pos-sales-dlq --max-delivery-attempts=10
  ```
- **Common mistake:** creating the subscription *after* the producer started and expecting old messages. A subscription only sees messages published after it exists (unless you **seek** it back using topic retention).

### 4.2 Ack, ack deadline, redelivery

- **What:** the consumer confirms with an **ack**. If it doesn't ack in time, the message is redelivered.
- **Inside:** subscribing forwarders track a lease per outstanding message. Expired leases go back to the pool.
- **Why it matters:** if processing takes longer than the deadline and the lease isn't extended, you get **duplicates** and a growing backlog ("redelivery storm").
- **Example:** client libraries let you set `max_lease_duration` and flow control. In Dataflow you don't manage acks yourself: **Dataflow acks a message only after it has durably committed the result of the first fused stage** (into Streaming Engine state/shuffle). So a worker crash never loses data.
- **Common mistake:** acking *before* processing ("fire and forget"). If you crash after the ack, the message is lost.

### 4.3 Exactly-once delivery (the Pub/Sub feature)

- **What:** a subscription option. Pub/Sub guarantees no redelivery of a message once it was **successfully acked** and no redelivery while the ack deadline is still valid.
- **Inside:** the ack becomes a confirmed operation. The client gets a success/failure result for the ack, so it knows whether the ack really "stuck".
- **Limits (check current docs):**
    - Only for **pull** subscriptions (including StreamingPull). Not push, not BigQuery/Cloud Storage subscriptions.
    - The guarantee is **within a cloud region**.
    - It removes **delivery-side** duplicates only. **Publish-side duplicates** (producer retries) have a new `message_id` and are still possible.
    - Higher latency and lower throughput than the default.
- **Why it matters:** "exactly-once delivery" ≠ "exactly-once end to end". Don't confuse them.
- **Common mistake:** enabling it for a Dataflow subscription. Dataflow already deduplicates Pub/Sub deliveries itself, so it adds latency without benefit (Google's docs advise against combining them; check current docs).

### 4.4 Ordering keys

- **What:** messages published with the same **ordering key** are delivered **in publish order** to the subscription (subscription must have `enable_message_ordering`).
- **Inside:** all messages of one key are routed through the same path, delivered one "batch" at a time per key. If the publisher fails for a key, the client library **pauses that key** until you call `resume_publish`, so order can't be broken by a retry.
- **Why it matters:** order only exists **per key** (like a Kafka partition, but at key level). There is a throughput limit **per ordering key** (about 1 MB/s; check docs). The ordering is guaranteed **within a region**.
- **Example:** `publisher.publish(topic, data, ordering_key="store-042")`.
- **Common mistake:** thinking ordering keys make the whole topic ordered, or that Dataflow preserves this order. **Dataflow does not process in order** (it parallelizes and shuffles). In Beam you handle order with **event time + windows**, or with **state** and a sequence number.

### 4.5 Retention, seek and replay

- **What:**
    - **Subscription retention** (default 7 days): unacked messages are kept this long. Optionally also **retain acked messages**.
    - **Topic retention** (up to 31 days): the topic keeps all messages, even acked ones, for any subscription to seek into.
    - **Seek**: move a subscription to a **timestamp** (re-deliver everything after it) or to a **snapshot** (the exact ack state at a moment).
- **Why it matters:** this is your **replay** mechanism. Example: you deploy a bug that writes bad data for 2 hours. Fix the code, then seek the subscription back to before the deploy and reprocess (with an idempotent sink).
- **Example:**

  ```bash
  gcloud pubsub snapshots create before-deploy --subscription=pos-sales-dataflow
  # ... deploy goes wrong ...
  gcloud pubsub subscriptions seek pos-sales-dataflow --snapshot=before-deploy
  # or:  --time=2026-10-23T08:00:00Z
  ```
- **Common mistake:** treating Pub/Sub as long-term storage. It is not a log you can replay from months ago like a Kafka topic with infinite retention. Keep a raw archive in **GCS or BigQuery** for long replays.

### 4.6 Dead-letter topics (DLQ)

- **What:** after **N failed delivery attempts** (5–100), Pub/Sub forwards the message to a dead-letter topic.
- **Inside:** Pub/Sub counts delivery attempts (approximately, best effort). The Pub/Sub **service account** needs permission to publish to the DLQ topic and to subscribe to the source subscription.
- **Why it matters:** a "poison message" that always crashes the consumer would otherwise be redelivered forever and block progress (especially with ordering keys).
- **Common mistake:** relying on the Pub/Sub DLQ for Dataflow. **Dataflow acks messages after reading**, so a parse failure in your DoFn never counts as a failed delivery. In Dataflow you build the dead-letter path **inside the pipeline** with a side output (section 4.14).

### 4.7 Flow control and batching

- **Publisher batching**: the client groups messages by count, bytes and latency (e.g. 100 messages or 10 ms). Bigger batches = higher throughput, slightly higher latency.
- **Publisher flow control**: limit outstanding messages/bytes so a fast producer doesn't run out of memory; block or error when full.
- **Subscriber flow control**: `max_messages` / `max_bytes` outstanding per client. Prevents a consumer from pulling more than it can process before deadlines expire.
- **Why it matters:** this is the knob that prevents redelivery storms in custom consumers. Dataflow handles this automatically.

### 4.8 Subscription types

| Type | How it works | Use it for | Notes |
|---|---|---|---|
| **Pull / StreamingPull** | Subscriber opens a stream and receives messages | Dataflow, GKE/VM consumers | Highest throughput, client controls rate |
| **Push** | Pub/Sub sends an HTTPS POST to your endpoint; HTTP 2xx = ack | Cloud Run, Cloud Functions | Can attach an OIDC token for auth; Pub/Sub backs off on errors |
| **BigQuery subscription** | Pub/Sub writes each message as a row directly into a BigQuery table (via the Storage Write API) | Raw ingestion with **no transformation** | Can use topic schema or table schema; can write metadata (message_id, publish_time, attributes). Unwritable messages go to the DLQ |
| **Cloud Storage subscription** | Pub/Sub batches messages into files in a bucket | Raw archive, data lake landing | Batch by time/bytes/messages; text or Avro. ≈ **Kinesis Firehose → S3** |

**Single Message Transforms (SMTs)** exist for simple per-message changes (e.g. a JavaScript UDF) on topics/subscriptions. They are newer; check current docs before relying on them.

### 4.9 Schemas

- **What:** a topic can have an **Avro** or **Protocol Buffer** schema. Messages are **validated at publish time**; invalid messages are rejected. Encoding: JSON or binary.
- **Inside:** schemas have **revisions**. A topic can accept a range of revisions (first/last revision), which allows controlled evolution.
- **Why it matters:** it is a **data contract** at the edge. Bad data is rejected at the producer instead of being found in BigQuery later. A BigQuery subscription can map schema fields to table columns.
- **Common mistake:** no schema plus `SELECT *` downstream: one producer change silently breaks reports.

### 4.10 Pipeline and PCollection

- **What:** a `Pipeline` is the whole graph. A `PCollection` is an immutable, distributed collection of elements. It is **bounded** (batch: files, tables) or **unbounded** (stream: Pub/Sub).
- **Inside:** every element in a PCollection has: the **value**, an **event timestamp**, one or more **windows**, and **pane info** (which firing produced it). You never index a PCollection; you only transform it.
- **Why it matters:** because elements carry timestamps and windows, the **same** `GroupByKey` means "group all" in batch and "group per window" in streaming.
- **Common mistake:** trying to "loop over" a PCollection or collect it to the driver like `df.collect()` in Spark. Beam has no driver-side collect.

### 4.11 PTransforms, ParDo and the DoFn lifecycle

- **What:** a `PTransform` turns PCollection(s) into PCollection(s). `ParDo` is the general per-element transform. It runs a `DoFn` (your class). `Map`, `FlatMap`, `Filter` are shortcuts.
- **Inside:** Dataflow splits data into **bundles** (small groups of elements). The DoFn lifecycle:

```text
 setup()          once per DoFn instance (per worker thread). Open clients, load models.
   start_bundle() before each bundle. Reset buffers.
     process()    once per element. Yield 0..N outputs.
     process()
   finish_bundle() after each bundle. Flush buffered writes.
   start_bundle() ...
 teardown()       best effort when the instance is discarded. Close clients.
```

- **Why it matters:** if a bundle fails, **the whole bundle is retried** (maybe on another worker). So `process()` can run **more than once** for the same element. Side effects (an HTTP call, a direct DB insert) must be **idempotent**. Results that go through Beam outputs are deduplicated by Dataflow; external side effects are not.
- **Example:**

  ```python
  class EnrichWithStore(beam.DoFn):
      def setup(self):
          self.client = make_http_client()      # expensive: once per instance
      def process(self, sale):
          sale["region"] = self.client.lookup_region(sale["store_id"])
          yield sale
      def teardown(self):
          self.client.close()
  ```
- **Common mistake:** creating a client per element in `process()` (slow, may hit quotas), or relying on `teardown()` to commit data (it is not guaranteed to run).

### 4.12 GroupByKey and Combine

- **What:** `GroupByKey` collects all values for a key (per window). `Combine` reduces values with an associative, commutative function (sum, count, mean, top-N).
- **Inside:** `GroupByKey` requires a **shuffle**: all values for a key move to one worker. `Combine` allows **combiner lifting**: each worker computes a **partial** result *before* the shuffle (like a map-side combine in Spark), so only small accumulators are shuffled.

```text
  GroupByKey(sum):  worker1: (s1,5)(s1,3)  worker2: (s1,2)   ── shuffle 3 values ──► s1: [5,3,2] → 10
  CombinePerKey:    worker1: (s1, 8)       worker2: (s1, 2)  ── shuffle 2 partials ─► s1: 8+2 → 10
```

- **Why it matters:** prefer `CombinePerKey` over `GroupByKey + sum` for performance, and it helps with **hot keys** (`with_hot_key_fanout`).
- **Common mistake:** `GroupByKey` on an unbounded PCollection in the **global window** with the default trigger: it never fires, because the global window never ends. You need a window or a trigger.

### 4.13 Side inputs

- **What:** an extra, usually small, input that every worker can read in full inside a DoFn. Example: a store → region lookup table.
- **Inside:** the side input is materialized and broadcast to workers (like a Spark broadcast variable). In streaming, a side input can be **refreshed periodically** (the "slowly updating side input" pattern, e.g. with `PeriodicImpulse` and a global window).
- **Example:**

  ```python
  stores = p | "ReadStores" >> beam.io.ReadFromBigQuery(query="SELECT store_id, region FROM ref.stores",
                                                        use_standard_sql=True)
  store_map = beam.pvalue.AsDict(stores | beam.Map(lambda r: (r["store_id"], r["region"])))
  enriched = sales | beam.Map(lambda s, m: {**s, "region": m.get(s["store_id"])}, m=store_map)
  ```
- **Common mistake:** using a huge table as a side input (memory pressure). For big lookups, use a key-value store (Bigtable) or a join via `CoGroupByKey`.

### 4.14 Side outputs (tagged outputs) for dead letters

- **What:** one DoFn can emit to several output PCollections, using tags.
- **Why it matters:** this is how you implement a **dead-letter path** in Dataflow: good records go to the main output; bad records (with the error message) go to a `dead_letter` output, which you write to a BigQuery error table or a GCS bucket. The pipeline never crashes on one bad message.
- **Example:**

  ```python
  class Parse(beam.DoFn):
      def process(self, msg):
          try:
              yield parse(msg)
          except Exception as e:
              yield beam.pvalue.TaggedOutput("dead_letter", {"raw": msg.data.decode("utf-8", "replace"),
                                                             "error": str(e)})
  results = msgs | beam.ParDo(Parse()).with_outputs("dead_letter", main="ok")
  good, bad = results.ok, results.dead_letter
  ```
- **Common mistake:** letting exceptions escape in streaming. Dataflow **retries a failing bundle indefinitely** in streaming, so one poison message can **stall** the pipeline (the watermark stops, lag grows).

### 4.15 Coders

- **What:** a coder defines how elements are serialized to bytes (for shuffle, state, and between stages).
- **Why it matters:** wrong or slow coders cost CPU. Python falls back to **pickling** for unknown types, which is slow and not deterministic. Deterministic coders are required for **keys** (two equal keys must produce the same bytes, or grouping breaks).
- **Tip:** use simple types (tuples, dicts of primitives), `beam.Row`, or `NamedTuple` with a registered `RowCoder`. Use `with_output_types(...)` type hints.

### 4.16 Event time vs. processing time

- **Event time:** when the thing happened (the sale at the till, the sensor reading).
- **Processing time:** when the pipeline sees it.
- **Skew** = processing time − event time. It is never zero and it changes: network, mobile devices offline, a store's internet down for 20 minutes, a backlog after a deploy.

```text
 event time   08:00   08:01   08:02   08:03   08:04
                │       │       │        │       │
                ▼        ╲       ╲        ▼        ╲
 processing   08:00:02   08:02:30  08:03:10  08:03:01  08:09:40   ◄ store 17 was offline
                                                       (event 08:04 arrives ~6 min late)
```

- **Why it matters:** business questions are in event time ("sales between 08:00 and 08:05"). If you group by processing time, a network problem changes your numbers.
- **Example:** in Pub/Sub, set the event time as a **message attribute** and tell Beam to use it: `ReadFromPubSub(..., timestamp_attribute="event_ts")`. The attribute must be milliseconds since epoch or an RFC 3339 string.
- **Common mistake:** reading with the default timestamp (the Pub/Sub **publish time**) and then re-stamping elements with `TimestampedValue` from the payload. The **watermark is still based on publish time**, so events whose payload timestamp is older than the watermark are treated as **late** and may be dropped. Use `timestamp_attribute` so the source watermark follows event time.

### 4.17 Windows
A window splits an unbounded stream into finite chunks by **event time** so you can aggregate.

```text
 event time ─────────────────────────────────────────────────────────────►
            08:00     08:05     08:10     08:15     08:20

 FIXED (tumbling) 5 min:
            [────────)[────────)[────────)[────────)
            each event in exactly ONE window

 SLIDING 10 min, every 5 min:
            [──────────────────)
                      [──────────────────)
                                [──────────────────)
            each event in size/period = 2 windows (moving averages)

 SESSION, gap 10 min (per key, e.g. per customer):
   cust A:  x  x   x                       x x
            [────────────)                [───────)     a gap > 10 min closes the session
            windows are dynamic, they MERGE when events fall close together

 GLOBAL (default):
            [────────────────────────────────────────────────────────── ∞
            one window forever; needs a trigger to emit anything in streaming
```

- **Inside:** `WindowInto` only **assigns** windows to elements (it adds window metadata). The actual grouping happens at the next `GroupByKey`/`Combine`, which groups by **(key, window)**.
- **Session windows** are "merging windows": every event starts a window [t, t+gap) and overlapping windows for the same key merge.

### 4.18 Watermarks

- **What:** the watermark is the system's estimate: *"I believe all events with event time ≤ W have arrived."* It is a moving point in event time.
- **How it is estimated (Dataflow + Pub/Sub):** Dataflow looks at the **backlog** of the subscription: the oldest unacknowledged messages and their timestamps. When you use a custom `timestamp_attribute`, Dataflow tracks event timestamps in the backlog (historically with an extra "tracking" subscription; check current docs). Downstream stages have their own watermarks: a stage's **output watermark** can't move ahead of its input watermark or the oldest data it still holds.
- **Why it can be wrong:** it is a **heuristic**. Pub/Sub has no partitions with ordered offsets like Kafka, so Dataflow can only estimate from what is in the backlog. A store that was offline publishes old events *after* the watermark has moved past them. Those events are **late data**.
- **Why it matters:** the watermark decides **when a window is "complete"** and the default trigger fires. Too eager = incomplete results; holding back = higher latency.
- **What stops a watermark:** a stuck element (poison message retried forever), a very old message in the backlog, a slow stage, a hot key. A **stuck watermark** shows up as growing **data freshness** in the Dataflow UI.

```text
 event time of data
     ▲
08:10│                                     ● ● ●   ideal: watermark = processing time
     │                               ● ●  ╱
08:05│                         ● ●  ╱    ●(late!)   real watermark (heuristic) — lags and is not perfect
     │                   ● ●  ╱
08:00│             ● ●  ╱
     └──────────────────────────────────────────────► processing time
```

### 4.19 Triggers, allowed lateness and accumulation

- **Trigger:** decides **when** to emit results (a "pane") for a window.
    - **Event-time trigger:** `AfterWatermark()` fires when the watermark passes the end of the window (the default).
    - **Processing-time trigger:** `AfterProcessingTime(60)` fires 60 s (wall clock) after the first element in the pane. Used for **early** (speculative) results.
    - **Data-driven trigger:** `AfterCount(n)`.
    - Composite: `AfterWatermark(early=AfterProcessingTime(60), late=AfterCount(1))`.
    - `Repeatedly(...)` to fire again and again (for the global window).
- **Allowed lateness:** how long (in event time, measured by the watermark) a window stays open **after** the watermark has passed its end. Late events inside that time update the window and fire a **late pane**. After that, the window's state is **garbage-collected** and later events are **dropped** (Dataflow counts them as dropped due to lateness).
- **Accumulation mode:** for repeated firings of the same window:
    - **ACCUMULATING:** each pane contains the **full updated** result (e.g. total = 105, then 112). The sink must **overwrite** (upsert by window key).
    - **DISCARDING:** each pane contains only the **new** data since the last firing (e.g. 105, then +7). The sink must **add**.
- **Why it matters:** this is a correctness decision that depends on the sink. Accumulating panes + append-only table = double counting.

### 4.20 Worked example: sensor readings with a timeline
Example: each IoT sensor sends a power reading. We compute a **5-minute average per sensor**, fixed windows, early results every minute, allowed lateness 15 minutes, accumulating panes.

```text
 Window [08:00, 08:05) for sensor S1.

 processing time ──►  08:01      08:02      08:05:20         08:09          08:22          08:25
                        │          │          │                │              │              │
 arrivals:            r(08:00:30) r(08:03:10)                 r(08:04:50)    r(08:01:00)
                       val=10      val=14                     val=18 LATE    val=12 TOO LATE
                        │          │                           │              │
 watermark:                                   passes 08:05     │       passes 08:20 (=08:05+15m)
                                                │              │              │
 panes:          EARLY (08:02) avg=12  ON-TIME (08:05:20) avg=12   LATE (08:09) avg=14   ── window GC'd
                 [10,14]                [10,14]                    [10,14,18]           r(08:01) DROPPED
                 timing=EARLY           timing=ON_TIME             timing=LATE           (→ late side path)
```

What the sink sees (accumulating):

| Pane | Timing | sensor | window_start | avg | is_final |
|---|---|---|---|---|---|
| 1 | EARLY | S1 | 08:00 | 12.0 | no |
| 2 | ON_TIME | S1 | 08:00 | 12.0 | no |
| 3 | LATE | S1 | 08:00 | 14.0 | (last one before GC) |

The BigQuery table should be keyed by `(sensor_id, window_start)` and keep the **latest pane** (MERGE, or `QUALIFY ROW_NUMBER() OVER (PARTITION BY sensor_id, window_start ORDER BY pane_index DESC) = 1` on an append table).

The event at 08:01 arriving at 08:25 is **beyond allowed lateness**. Options: accept the loss (document it), or keep the **raw events** in BigQuery too and **recompute** that day in batch. This is the "late data table + batch correction" pattern.

### 4.21 State and timers (brief)

- **State:** per **key and window**, a DoFn can keep values (`ValueState`, `BagState`, `CombiningState`), stored durably by the runner (Streaming Engine).
- **Timers:** callbacks per key, in **event time** (fire when the watermark passes T) or **processing time** (fire after a wall-clock delay).
- **Use cases:** deduplication by `event_id` with expiry, "alert if no reading for 10 minutes", custom session logic, ordering by sequence number, batching RPC calls per key.
- **Why it matters:** this is how you go beyond windows. It is also a good way to dedupe beyond Pub/Sub's guarantees: a stateful DoFn keyed by `event_id` that remembers seen IDs for N hours, with an event-time timer to clear the state.

---

## 5. Dataflow runner internals

### 5.1 Graph optimization: fusion
Dataflow **fuses** consecutive steps into one **stage**, so an element flows through Read → Parse → Map in memory on the same worker without serialization.

```text
 Your graph:   Read → Parse → Filter → Enrich → GroupByKey → Format → Write
 Fused:       [Read+Parse+Filter+Enrich] ══shuffle══ [GroupByKey+Format+Write]
```

- **Good:** less I/O, much faster.
- **Bad:** parallelism of a fused stage is limited by its **input**. Classic problem: a `FlatMap` that turns 1 file name into 10 million rows; all rows stay on the worker that read the file name. **Fix:** insert `beam.Reshuffle()` to break fusion and redistribute.
- **Combiner lifting** is the other main optimization (partial combines before shuffle, section 4.12).

### 5.2 Workers and autoscaling

- **Horizontal autoscaling** (add/remove workers):
    - Batch: based on the remaining work and the throughput.
    - Streaming: based on **backlog** (how long it would take to process the waiting data) and **CPU utilization**. Set `max_num_workers` to cap cost. You can update the min/max range on a running job.
- **Vertical autoscaling** (**Dataflow Prime**): adjusts worker **memory** up or down, e.g. after out-of-memory errors.
- **Right fitting / resource hints:** different stages can get different resources (e.g. a GPU or high-memory stage) with Prime.
- **Dynamic work rebalancing** (batch): splits remaining work from slow workers (stragglers) and gives it to idle ones.
- **Why streaming autoscaling works with Streaming Engine:** state and shuffle are in the backend, so adding/removing workers means reassigning **key ranges**, not copying gigabytes of state.

### 5.3 Streaming Engine and Dataflow Shuffle

| | Without | With |
|---|---|---|
| **Streaming Engine** (streaming) | Shuffle and state stored on worker persistent disks; scaling moves disks | Shuffle, state and timers stored in Google's backend; workers are lighter, scale faster, updates are smoother |
| **Dataflow Shuffle** (batch) | Shuffle on worker disks | Shuffle in the backend service; faster, fewer disk problems |

Streaming Engine is the default/recommended for new streaming jobs in current SDKs (check current docs). Billing for Streaming Engine is separate (by data processed or by compute units; check docs).

### 5.4 Exactly-once processing in streaming
Dataflow streaming gives **exactly-once processing of records** by default. How:

1. **Source dedup:** Dataflow deduplicates Pub/Sub messages by `message_id` (redeliveries). With `id_label`, it can deduplicate on **your own** attribute (e.g. `event_id`), which catches **publish-side** duplicates too (within a limited time window; check docs).
2. **Durable commits per step:** each worker processes a work item (a key range) and **atomically commits** its outputs, state changes and timer changes to Streaming Engine. Only then are the inputs considered done (and Pub/Sub messages acked after the first stage commits).
3. **Shuffle dedup:** every record sent across a shuffle has a **unique ID**. If a worker retries and sends the same record again, the receiver ignores the duplicate.
4. **Sinks:** exactly-once into the sink needs **sink cooperation**: e.g. BigQuery **Storage Write API** in exactly-once mode (stream offsets), or idempotent writes (MERGE by key).

```text
 Pub/Sub ──(dedup by message_id / id_label)──► Stage 1 ──commit──► Streaming Engine ──(dedup by record id)──► Stage 2 ──► Sink (idempotent / offsets)
                                                   │
                                          retries are invisible downstream,
                                          BUT external side effects in DoFn can repeat
```

**At-least-once streaming mode:** an option (with Streaming Engine) that skips part of the deduplication. It is **cheaper and has lower latency**, and it is a good choice when the sink is idempotent anyway, or duplicates are acceptable (e.g. raw landing with downstream dedup). Pair it with `STORAGE_API_AT_LEAST_ONCE` for BigQuery.

### 5.5 Checkpointing (and how it differs from Spark/Flink)

- **Spark Structured Streaming:** micro-batches; offsets + state checkpointed to a checkpoint directory (S3/HDFS) per batch.
- **Flink:** periodic distributed snapshots (Chandy-Lamport barriers).
- **Dataflow:** **continuous, fine-grained commits** per work item to Streaming Engine. There is no global checkpoint interval to tune. Recovery = another worker picks up the key range from the last committed state.
- **Dataflow snapshots** (separate feature): save a running streaming job's state to create a new job from it later (for backup or migration; check current support for Python).

### 5.6 Update, drain, cancel

| Action | What happens | Data | When |
|---|---|---|---|
| **Update** (`--update --job_name=same`) | New code replaces the running job **in place**; state and in-flight data are kept | No loss | Code fix, compatible change. Dataflow runs a **compatibility check**; renamed steps need `--transform_name_mapping` |
| **Drain** | Stops reading new data, **finishes** in-flight work, **closes all open windows immediately** (fires them), then stops | No loss of read data, but windows fire **early with partial data** | Incompatible change: drain, then start a new job (the new job resumes from the subscription) |
| **Cancel** | Stops immediately | In-flight data and buffered state are **lost** (messages already acked by Dataflow won't come back unless you seek) | Emergency, test jobs, or when replaying from a seek/snapshot anyway |

Important detail: because Pub/Sub keeps **unacked** messages, after a **drain** the new job simply continues from the subscription backlog. After a **cancel**, use a Pub/Sub **snapshot/seek** to replay what was lost, and make the sink idempotent.

Also: you can update some options **in flight** without replacing the job (e.g. autoscaling min/max workers).

### 5.7 Templates: reusable ingestion

- **Classic templates:** the pipeline graph is pre-built and staged to GCS; limited parameters (`ValueProvider`). Older approach.
- **Flex Templates:** the pipeline is packaged as a **Docker image** (Artifact Registry) plus a **template spec file** (JSON in GCS) that lists the parameters. At launch, Dataflow starts a launcher VM that runs your code with the parameters and builds the graph. So the graph can **change based on parameters** (e.g. which sinks to create).
- **Google-provided templates** (open source, mostly Java): Pub/Sub → BigQuery, Pub/Sub Subscription → BigQuery (with a UDF), Pub/Sub → GCS (Avro/text), **Datastream → BigQuery**, JDBC → BigQuery, GCS text → BigQuery, BigQuery → Bigtable, and more. Many accept a **JavaScript or Python UDF** for light transformation and route failures to a dead-letter table.
- **Beam YAML** and the Dataflow **job builder** UI are newer ways to define pipelines without code (check current docs).

**Pattern for shared ingestion projects:**

```text
 platform team:  repo "ingestion-templates"  ──CI──► build image ─► Artifact Registry
                                                  └► template spec ─► gs://ingestion-templates/pubsub_to_bq.json
 Terraform module "streaming_ingestion":  topic + schema + subscription + DLQ + SA + IAM + Dataflow job (flex template)
 domain team:    module "pos_sales" { topic="pos-sales", table="raw.pos_sales", window_s=300, ... }
 Composer:       DataflowStartFlexTemplateOperator for batch runs
```

```bash
gcloud dataflow flex-template run "pos-sales-$(date +%Y%m%d-%H%M)" \
  --template-file-gcs-location=gs://ingestion-templates/pubsub_to_bq.json \
  --region=europe-north1 \
  --service-account-email=df-ingest@ingest-prj.iam.gserviceaccount.com \
  --parameters=input_subscription=projects/ingest-prj/subscriptions/pos-sales-dataflow,output_table=edw-prj:raw.pos_sales,dlq_table=edw-prj:raw.pos_sales_dlq
```

### 5.8 Hot keys

- **What:** one key with much more data than others (e.g. a flagship store, a "null" key, a sensor stuck in a loop). Because a key is processed by one worker at a time, that worker becomes the bottleneck. The watermark of the whole stage waits for it.
- **Detect:** Dataflow logs "hot key detected" messages (you can enable logging the key itself); one worker at 100% CPU while others idle; growing system lag in one stage.
- **Fix:**
    - `beam.CombinePerKey(fn).with_hot_key_fanout(16)`: spreads partial combines over 16 sub-keys, then merges.
    - **Salting**: key = `(store_id, random 0..N)` → aggregate → re-key by `store_id` → aggregate again.
    - Pre-aggregate before the shuffle; fix the source (why are all keys `null`?).

### 5.9 Monitoring metrics

| Metric | Meaning | What a bad value tells you |
|---|---|---|
| **System lag** | The longest time an item has been waiting to be processed in a stage | Workers can't keep up, or something is stuck/retrying |
| **Data freshness** | How far the output watermark is behind real time (now − watermark) | Watermark is held back: backlog, stuck element, hot key, very late data |
| **Backlog** (elements/bytes, Pub/Sub unacked) | How much data is waiting in the source | Under-provisioned or `max_num_workers` too low |
| **Pub/Sub oldest unacked message age** | Age of the oldest message waiting in the subscription | Consumer down or behind. Alert on this! |
| **Throughput** per step | Elements/s or bytes/s | Find the slow step |
| **CPU / memory per worker** | Utilization | Hot key (one worker hot), OOM risk |
| **Errors / dead-letter count** | Exceptions in logs, rows in DLQ table | Bad producer, schema change |
| **Dropped due to lateness** counter | Late data discarded | Allowed lateness too short or watermark too eager |

For the platform metrics "load duration, throughput, stability and cost": data freshness = latency SLA, throughput, DLQ rate = stability, and job cost (billing export labels per job).

### 5.10 Cost drivers

- **Worker resources:** vCPU-hours, memory GB-hours, persistent disk (streaming jobs run **24/7**, so a minimum of workers is a fixed monthly cost).
- **Streaming Engine** / **Dataflow Shuffle** data processed (or compute units).
- **Pub/Sub:** throughput (data published and delivered; each subscription is billed for delivery), retention storage, cross-region egress.
- **BigQuery writes:** Storage Write API ingestion; the streaming inserts legacy API is more expensive.
- **Savings levers:** batch instead of streaming when latency allows; **FlexRS** (cheaper, delayed-start batch); at-least-once streaming mode; right-sized machine types; `max_num_workers`; Combine before shuffle; avoid Dataflow entirely for simple cases (section 6). Prices change, so check current numbers in the docs.

---

## 6. When NOT to use Dataflow

| Need | Better choice | Why |
|---|---|---|
| Land raw Pub/Sub messages in BigQuery, no transformation | **BigQuery subscription** | No code, no workers, lower cost, Storage Write API underneath |
| Land raw messages as files for a lake | **Cloud Storage subscription** | Like Firehose → S3 |
| Replicate an operational database to BigQuery | **Datastream → BigQuery directly** | Built-in CDC apply (upsert/delete per primary key) |
| Transformations expressible in SQL on data already in BigQuery | **BigQuery SQL** via Dataform/dbt, scheduled by Composer | Simpler, cheaper, testable |
| Continuous SQL on a stream already in BigQuery | **BigQuery continuous queries** (newer; check docs and editions requirements) | No pipeline to run |
| Existing Spark jobs, Spark skills, lift-and-shift | **Dataproc / Dataproc Serverless** | Reuse code |
| Light per-message transform | Pub/Sub **SMTs** or a template with a UDF | Less to maintain |

**Use Dataflow when:** parsing/validation with dead-lettering, enrichment with side inputs or external calls, **event-time windowing with late data**, stateful logic (dedup, sessionization, alerting), routing to multiple sinks, or the same logic for batch and streaming.

---

## 7. A full Python Beam pipeline, line by line

Scenario: POS sale events on Pub/Sub. Each message is JSON: `{"event_id": "...", "store_id": "S042", "sku": "123", "amount": 19.90, "ts": "2026-10-23T08:01:12Z"}`. The producer also sets the attribute `event_ts` (RFC 3339) and `event_id`.

Goals: parse, dead-letter bad messages, compute **revenue per store per 5 minutes** with early results and 15 min allowed lateness, write raw rows and aggregates to BigQuery with the **Storage Write API**.

```python
import json
import logging
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms import window
from apache_beam.transforms.trigger import (
    AfterWatermark, AfterProcessingTime, AfterCount, AccumulationMode)
from apache_beam.utils.timestamp import Duration


class SalesOptions(PipelineOptions):                                  # (1)
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument("--input_subscription", required=True)
        parser.add_argument("--raw_table", required=True)
        parser.add_argument("--agg_table", required=True)
        parser.add_argument("--dlq_table", required=True)
        parser.add_argument("--window_seconds", type=int, default=300)
        parser.add_argument("--allowed_lateness_seconds", type=int, default=900)


class ParseSale(beam.DoFn):                                           # (2)
    DLQ = "dead_letter"

    def process(self, msg):                                           # msg: PubsubMessage
        raw = msg.data.decode("utf-8", errors="replace")
        try:
            e = json.loads(raw)
            sale = {
                "event_id": str(e["event_id"]),
                "store_id": str(e["store_id"]),
                "sku": str(e["sku"]),
                "amount": float(e["amount"]),
                "event_ts": e["ts"],                                  # keep as string for BQ TIMESTAMP
                "publish_ts": msg.publish_time.isoformat() if msg.publish_time else None,  # (3)
            }
            if sale["amount"] < 0:
                raise ValueError("negative amount")
            yield sale
        except Exception as err:                                      # (4)
            yield beam.pvalue.TaggedOutput(self.DLQ, {
                "raw_payload": raw[:100000],
                "attributes": json.dumps(dict(msg.attributes or {})),
                "error": f"{type(err).__name__}: {err}",
                "failed_at": datetime.now(timezone.utc).isoformat(),
            })


class AddWindowInfo(beam.DoFn):                                       # (5)
    def process(self, kv, win=beam.DoFn.WindowParam, pane=beam.DoFn.PaneInfoParam):
        store_id, revenue = kv
        yield {
            "store_id": store_id,
            "window_start": win.start.to_utc_datetime().isoformat(),
            "window_end": win.end.to_utc_datetime().isoformat(),
            "revenue": round(revenue, 2),
            "pane_index": pane.index,
            "pane_timing": {0: "EARLY", 1: "ON_TIME", 2: "LATE", 3: "UNKNOWN"}.get(pane.timing, "UNKNOWN"),
            "is_last": pane.is_last,
            "emitted_at": datetime.now(timezone.utc).isoformat(),
        }


RAW_SCHEMA = ("event_id:STRING,store_id:STRING,sku:STRING,amount:FLOAT64,"
              "event_ts:TIMESTAMP,publish_ts:TIMESTAMP")
AGG_SCHEMA = ("store_id:STRING,window_start:TIMESTAMP,window_end:TIMESTAMP,revenue:FLOAT64,"
              "pane_index:INT64,pane_timing:STRING,is_last:BOOL,emitted_at:TIMESTAMP")
DLQ_SCHEMA = "raw_payload:STRING,attributes:STRING,error:STRING,failed_at:TIMESTAMP"


def run(argv=None):
    opts = PipelineOptions(argv, save_main_session=True)
    opts.view_as(StandardOptions).streaming = True                    # (6)
    o = opts.view_as(SalesOptions)

    with beam.Pipeline(options=opts) as p:
        msgs = p | "ReadPubSub" >> beam.io.ReadFromPubSub(            # (7)
            subscription=o.input_subscription,
            with_attributes=True,                                     # gives PubsubMessage objects
            timestamp_attribute="event_ts",                           # event time + watermark
            id_label="event_id")                                      # dedup on producer id (Dataflow)

        parsed = msgs | "Parse" >> beam.ParDo(ParseSale()).with_outputs(
            ParseSale.DLQ, main="ok")                                 # (8)

        # Raw rows: one row per event (idempotency key = event_id)
        parsed.ok | "WriteRaw" >> beam.io.WriteToBigQuery(            # (9)
            o.raw_table, schema=RAW_SCHEMA,
            method=beam.io.WriteToBigQuery.Method.STORAGE_WRITE_API,
            triggering_frequency=5,                                   # seconds between commits
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)

        # Dead letters
        parsed.dead_letter | "WriteDLQ" >> beam.io.WriteToBigQuery(
            o.dlq_table, schema=DLQ_SCHEMA,
            method=beam.io.WriteToBigQuery.Method.STORAGE_API_AT_LEAST_ONCE,   # (10)
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER)

        # Windowed aggregation
        (parsed.ok
         | "KeyByStore" >> beam.Map(lambda s: (s["store_id"], s["amount"]))
         | "Window5m" >> beam.WindowInto(                             # (11)
               window.FixedWindows(o.window_seconds),
               trigger=AfterWatermark(
                   early=AfterProcessingTime(60),                     # speculative result every ~1 min
                   late=AfterCount(1)),                               # update for each late element
               accumulation_mode=AccumulationMode.ACCUMULATING,       # full total in each pane
               allowed_lateness=Duration(seconds=o.allowed_lateness_seconds))
         | "SumPerStore" >> beam.CombinePerKey(sum).with_hot_key_fanout(8)   # (12)
         | "AddWindowInfo" >> beam.ParDo(AddWindowInfo())
         | "WriteAgg" >> beam.io.WriteToBigQuery(
               o.agg_table, schema=AGG_SCHEMA,
               method=beam.io.WriteToBigQuery.Method.STORAGE_WRITE_API,
               triggering_frequency=5,
               create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER))


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
```

Line-by-line explanation:

1. **Custom options** make the pipeline **reusable**: subscription, tables, window size and lateness are parameters. This is exactly what a Flex Template exposes.
2. **ParseSale** is a DoFn with two outputs. It never raises an exception for bad input.
3. We keep **publish time** next to event time. `publish_ts − event_ts` = ingestion delay, a useful freshness metric.
4. **The dead-letter path**: on any error, emit the raw payload, attributes and error message to a tagged output. The pipeline keeps running; bad data is visible and replayable.
5. **AddWindowInfo** reads the window and **pane** metadata (`WindowParam`, `PaneInfoParam`), so the sink knows if a row is EARLY, ON_TIME or LATE, and which version (`pane_index`) it is.
6. `streaming = True`: an unbounded source requires streaming mode.
7. **ReadFromPubSub**:
    - `with_attributes=True`: we receive `PubsubMessage` (data + attributes + publish_time) instead of raw bytes.
    - `timestamp_attribute="event_ts"`: element timestamps **and the watermark** follow event time.
    - `id_label="event_id"`: Dataflow deduplicates on the producer's ID, catching publish retries (Dataflow runner only; dedup within a limited time window).
8. `.with_outputs(...)` returns an object with `.ok` and `.dead_letter` PCollections.
9. **Storage Write API** (`STORAGE_WRITE_API`): exactly-once semantics into BigQuery via stream offsets. `triggering_frequency` controls how often data is committed in streaming (latency vs. cost). In Python this is a cross-language (Java) transform under the hood, so the Dataflow job needs the Java expansion service (handled automatically by the SDK; check SDK version notes).
10. For the DLQ, `STORAGE_API_AT_LEAST_ONCE` is cheaper and good enough: a duplicate error row is harmless.
11. **WindowInto**: fixed 5-minute windows in event time. Early panes every ~60 s (processing time), the on-time pane when the watermark passes the window end, one late pane per late element, up to 15 min of lateness. **Accumulating** → each pane holds the full total.
12. **CombinePerKey(sum)** uses combiner lifting. `with_hot_key_fanout(8)` protects against one huge store.

**The sink must match the accumulation mode.** The aggregate table gets several rows per `(store_id, window_start)`. Expose a view that keeps the latest pane:

```sql
CREATE OR REPLACE VIEW mart.store_revenue_5m AS
SELECT * EXCEPT(rn)
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id, window_start
                               ORDER BY pane_index DESC, emitted_at DESC) AS rn
  FROM raw.store_revenue_5m_panes
)
WHERE rn = 1;
```

Run it locally first with the **DirectRunner** and `TestStream` (to simulate late data) in unit tests; then on Dataflow:

```bash
python sales_pipeline.py --runner=DataflowRunner --project=ingest-prj --region=europe-north1 \
  --temp_location=gs://ingest-prj-tmp/df --staging_location=gs://ingest-prj-tmp/staging \
  --service_account_email=df-ingest@ingest-prj.iam.gserviceaccount.com \
  --enable_streaming_engine --max_num_workers=10 --job_name=pos-sales \
  --input_subscription=projects/ingest-prj/subscriptions/pos-sales-dataflow \
  --raw_table=edw-prj:raw.pos_sales --agg_table=edw-prj:raw.store_revenue_5m_panes \
  --dlq_table=edw-prj:raw.pos_sales_dlq
```

---

## 8. Idempotency and deduplication end to end ("effectively-once")

The honest sentence: **"Exactly-once end to end is really effectively-once: at-least-once delivery somewhere, plus deduplication and idempotent writes."**

Where duplicates come from, and what removes each one:

```text
 PRODUCER ──► PUB/SUB ──► DATAFLOW ──► BIGQUERY ──► MODELS
    │            │            │             │           │
 retry after   redelivery   bundle       write       re-run of a
 timeout       after lost   retries      retries     batch model
 (new msg_id)  ack (same    (inside)                 (Composer retry)
               msg_id)
    │            │            │             │           │
 event_id     Dataflow     Dataflow      Storage     MERGE on key /
 in payload   dedups by    exactly-once  Write API   partition
 + id_label   message_id   commits +     offsets,    overwrite,
              (or exactly- shuffle IDs   or MERGE    QUALIFY dedup
              once sub for               on key
              non-Dataflow)
```

| Layer | Technique | Covers |
|---|---|---|
| Producer | Generate a stable **event_id** (UUID at the source, or natural key such as `store_id + receipt_no + line_no`) | Gives every later layer something to dedupe on |
| Pub/Sub | Default at-least-once; exactly-once delivery for non-Dataflow pull consumers | Delivery-side duplicates only |
| Dataflow source | Dedup by `message_id`; `id_label` for producer IDs | Redeliveries; publish retries in a time window |
| Dataflow processing | Exactly-once commits; dedup across shuffles | Worker retries |
| Dataflow custom | Stateful dedup DoFn keyed by `event_id` with an expiry timer | Duplicates over longer periods |
| BigQuery write | Storage Write API exactly-once; or MERGE on key | Write retries |
| Downstream | `QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY publish_ts) = 1`; idempotent incremental models | Anything left; replays after a seek |
| Side effects | External calls in DoFns must be idempotent (idempotency key in the API) | Bundle retries |

**Replay is safe only if everything is idempotent.** That's why seek/replay, drain-and-restart and backfills all depend on this table.

---

## 9. What matters most in practice

### 9.1 Choose the simplest ingestion path, and document why

The EDW goal is "leaner, faster, more stable". Each Dataflow job is code to test, deploy, monitor and pay for 24/7. A good platform design reduces moving parts: BigQuery subscription for raw landing; Datastream for CDC; BigQuery SQL for modelling; **Dataflow only for real stream logic**. Always document the alternative you rejected.

### 9.2 Event time, watermarks, lateness: get it right, and keep it in the right box
This topic is often confused with CDC. **Windows group immutable events by event time.** The watermark estimates completeness; allowed lateness keeps windows open for stragglers; triggers decide when to emit; accumulation mode decides what each emission contains. **None of this is needed for CDC**, which orders changes per primary key by log position (see [Datastream and CDC](datastream-cdc.md)).

### 9.3 At-least-once is the default; design idempotent sinks
Know the sources of duplicates (section 8) and the techniques per layer. Be precise: Pub/Sub exactly-once **delivery** is regional, pull-only, and doesn't remove publish duplicates; Dataflow exactly-once **processing** doesn't cover external side effects; the **Storage Write API** gives exactly-once into BigQuery.

### 9.4 Dead-letter handling inside the pipeline
In Dataflow, parse errors never reach the Pub/Sub DLQ, because Dataflow already acked. Use **tagged outputs** to a DLQ table with raw payload + error + timestamp; alert on DLQ rate; provide a **replay** path (fix, then re-publish from the DLQ table). Remember: DLQ is for **malformed** data, not for **late** or **out-of-order** data.

### 9.5 Reusable ingestion with Flex Templates + Terraform

Shared ingestion projects need reusable patterns. A platform template (parameterized pipeline, container image, spec file) + a Terraform module that creates topic, schema, subscription, DLQ, service account, IAM and the job. Domain teams fill in parameters. Versioned images, CI builds, and documentation are the "handover of patterns" part.

### 9.6 Operating streaming jobs: update vs. drain vs. cancel
Know what happens to state and in-flight data in each case, the compatibility check for update, and that drain **fires windows early**. Combine with Pub/Sub snapshots before risky deployments.

### 9.7 Monitoring and SLAs
Define freshness from the business view ("sales in Looker are at most 5 minutes old") and map it to metrics: Pub/Sub oldest unacked age, Dataflow data freshness and system lag, DLQ count, and a BigQuery check `CURRENT_TIMESTAMP() - MAX(event_ts)`. Alert on each in Cloud Monitoring.

### 9.8 Performance: fusion, hot keys, Combine
Understand fusion and `Reshuffle`, combiner lifting, hot-key fanout. These are the usual causes and fixes when "my pipeline is slow".

### 9.9 Cost awareness
Streaming jobs run all the time. Use autoscaling caps, Streaming Engine, at-least-once mode where safe, batch or FlexRS when latency allows, and the no-Dataflow options. Tag jobs with labels for cost reporting (cost is a key platform metric).

### 9.10 Pub/Sub vs. Kafka

If you know Kafka, the main question is "how is Pub/Sub different?". See section 10.

---

## 10. Mapping from Kafka, AWS and Spark

### 10.1 Pub/Sub vs. Kafka: the deep comparison

| Aspect | Kafka (incl. MSK) | Pub/Sub |
|---|---|---|
| Unit of scale | **Partitions**. You choose the count; it caps consumer parallelism | No partitions. Scales automatically; parallelism = number of subscriber clients/workers |
| Ordering | Total order **per partition** | Order **per ordering key** (opt-in), within a region; otherwise no order |
| Consumer position | **Offset** per partition, committed by the consumer group | **Per-message ack** with deadline; no offsets |
| Consumer group | Consumer group: partitions are assigned to members; rebalancing | **Subscription**: messages are load-balanced across clients; no rebalancing protocol for you to manage |
| Slow message | Blocks its partition (head-of-line) unless you skip it | Other messages continue; the slow one is redelivered |
| Replay | Reset offsets to any retained position | **Seek** to a timestamp or snapshot within retention |
| Retention | Configurable, can be very long or infinite; compacted topics | Subscription ≤ 7 days, topic ≤ 31 days; **no log compaction** |
| Delivery | At-least-once; exactly-once with idempotent producer + transactions (Kafka Streams) | At-least-once; exactly-once **delivery** option for pull (regional) |
| DLQ | Build it yourself (or Kafka Connect DLQ) | Built-in dead-letter topic |
| Operations | Brokers, ZooKeeper/KRaft, disks, partition rebalancing (managed MSK helps) | Serverless |
| Schema | Schema Registry (Confluent) | Built-in topic schemas (Avro/Protobuf) |
| Connectors | Kafka Connect ecosystem | Native BigQuery/Cloud Storage subscriptions; Dataflow templates |
| GCP Kafka option | **Google Cloud Managed Service for Apache Kafka** exists if you need Kafka semantics (check current docs). Pub/Sub Lite is deprecated |

**Summary:** *"The biggest mental shift from Kafka is offsets versus acks. In Kafka the consumer stores a position in an ordered partition, so a slow message blocks the partition and replay is an offset reset. In Pub/Sub, every message is acked individually with a deadline. There are no partitions to size, but no global order either; I get order only per ordering key, and in Dataflow I don't rely on order at all, I use event time."*

### 10.2 Other mappings

| Kafka / AWS / Spark | GCP equivalent | Notes |
|---|---|---|
| **Kinesis Data Streams** (shards, iterator, 24h–365d retention) | Pub/Sub | Shards ≈ partitions; Pub/Sub has none |
| **Kinesis Data Firehose → S3** | Pub/Sub **Cloud Storage subscription** | Buffering by size/time, Avro/text files |
| **Firehose → Redshift** | Pub/Sub **BigQuery subscription** | Direct table writes |
| **Spark Structured Streaming** | Beam on Dataflow | Micro-batch vs. continuous per-key commits; `withWatermark("ts","15 minutes")` ≈ Beam watermark + `allowed_lateness`; Spark output modes (append/update/complete) ≈ triggers + accumulation |
| Spark `checkpointLocation` | Streaming Engine state (no path to manage) | |
| Spark `groupBy(window(...))` | `WindowInto` + `CombinePerKey` | |
| Spark broadcast variable | Side input | |
| Spark `repartition()` | `beam.Reshuffle()` | Also breaks fusion |
| Spark on EMR (self-managed) | Dataproc (Serverless) | For existing Spark code |
| Kinesis Data Analytics / Flink | Dataflow | Flink checkpoints vs. Dataflow continuous commits |
| **AWS DMS** / **Debezium** | **Datastream** | CDC: see [Datastream and CDC](datastream-cdc.md). Debezium → Kafka → consumer ≈ Datastream → GCS → Dataflow |
| AWS Glue streaming jobs | Dataflow (or templates) | |
| CloudWatch metrics (IteratorAge) | Pub/Sub **oldest unacked message age** | Same purpose: consumer lag |

**Example translation:** *"A typical AWS streaming design sends high-volume events through Kafka or Kinesis and Firehose to S3, and Spark computes 5-minute aggregates with a watermark. On GCP you would use Pub/Sub with a Cloud Storage subscription for the raw archive, and Dataflow with event-time fixed windows, 15 minutes allowed lateness, accumulating panes, written with the Storage Write API to a BigQuery table partitioned by date and clustered by device. Data beyond allowed lateness stays in the raw archive and is corrected by a daily batch recompute."*

---

## 11. Hands-on exercise

**Time:** 2–3 hours. Use a personal GCP project with free credits. Delete resources afterwards (streaming jobs cost money while running).

### Part A: Pub/Sub only (no Dataflow) — 30 min

1. Create a topic `sales-raw` with an **Avro schema** (event_id, store_id, amount, ts).
2. Create a **BigQuery subscription** writing to `raw.sales_bqsub`, with "write metadata" on.
3. Publish 20 messages with `gcloud pubsub topics publish` (2 of them invalid). Observe: invalid messages are **rejected at publish** by the schema.
4. Query the table: see `message_id`, `publish_time`, `attributes` columns.

**Learn:** the no-code path, schema as a contract.

### Part B: Delivery semantics — 30 min

1. Create a pull subscription with ack deadline 10 s.
2. Pull without acking (`gcloud pubsub subscriptions pull` **without** the `--auto-ack` flag). Wait 15 s and pull again: the same message comes back (same `message_id`; if a DLQ is configured, the delivery attempt counter goes up).
3. Create a snapshot, ack everything, then **seek** to the snapshot: messages come back.

**Learn:** ack deadlines, redelivery, replay.

### Part C: Beam locally with late data — 60 min
Write a unit test with `TestStream`:

```python
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.transforms import window, trigger
from apache_beam.utils.timestamp import Duration

def test_late_data():
    stream = (TestStream()
        .advance_watermark_to(0)
        .add_elements([window.TimestampedValue(("S1", 10), 30),
                       window.TimestampedValue(("S1", 14), 190)])
        .advance_watermark_to(301)                      # window [0,300) fires ON_TIME → 24
        .add_elements([window.TimestampedValue(("S1", 18), 290)])   # late, within lateness → 42
        .advance_watermark_to(300 + 900 + 1)            # window expires
        .add_elements([window.TimestampedValue(("S1", 12), 60)])    # too late → dropped
        .advance_watermark_to_infinity())
    with TestPipeline(options=None) as p:
        out = (p | stream
                 | beam.WindowInto(window.FixedWindows(300),
                       trigger=trigger.AfterWatermark(late=trigger.AfterCount(1)),
                       accumulation_mode=trigger.AccumulationMode.ACCUMULATING,
                       allowed_lateness=Duration(seconds=900))
                 | beam.CombinePerKey(sum))
        assert_that(out, equal_to([("S1", 24), ("S1", 42)]))
```

Then change to `DISCARDING` and predict the output before running (`("S1", 24), ("S1", 18)`). (You need `streaming=True` in the test options for TestStream on some SDK versions; check.)

**Learn:** watermark, allowed lateness, accumulation, drop behaviour, testability.

### Part D: Dataflow — 60 min

1. Run the Google-provided **Pub/Sub Subscription to BigQuery** template from the console. Note the parameters, the job graph, the dead-letter table.
2. Run your own pipeline from section 7 on Dataflow with `max_num_workers=2`. Publish data with a small Python script, including late events (old `event_ts`).
3. Look at **data freshness**, **system lag**, the fused stages in the job graph, and the panes in BigQuery.
4. **Drain** the job and see the windows fire. Then delete everything.

### Design exercise (no code)

*"ShopFlow wants store revenue in Looker within 2 minutes, from 500 stores, peak 3,000 sales/s; stores sometimes go offline for up to 1 hour."*
Write your design covering: ingestion path, event time, window size, triggers (early panes for 2-minute freshness), allowed lateness (≥ 1 hour, or a batch correction), accumulation mode + sink design, dedup key, DLQ, monitoring, cost, and what you'd do without Dataflow (BigQuery subscription + incremental SQL every 2 minutes, if acceptable).

---

## 12. Glossary

| Term | Meaning |
|---|---|
| **Ack / nack** | Subscriber confirms (or rejects) a message; acked messages are not redelivered |
| **Ack deadline** | Time a subscriber has to ack before redelivery (default 10 s, max 600 s) |
| **Accumulating / discarding** | Each pane contains the full result so far / only new data since the last pane |
| **Allowed lateness** | How long after the watermark passes a window's end that late data is still accepted |
| **At-least-once** | Every message is delivered one or more times; duplicates are possible |
| **Backlog** | Messages waiting in a subscription, not yet acked |
| **BigQuery subscription** | Pub/Sub writes messages directly into a BigQuery table |
| **Bounded / unbounded** | Finite (batch) / infinite (stream) PCollection |
| **Bundle** | A small group of elements processed together by a DoFn; the unit of retry |
| **Coder** | Serializer for elements; must be deterministic for keys |
| **Combiner lifting** | Partial aggregation before the shuffle |
| **Data freshness** | Dataflow metric: how far the output watermark is behind now |
| **Dead-letter topic / DLQ** | Where undeliverable or unparseable messages go |
| **DoFn** | User class with `process()` used by `ParDo` |
| **Drain** | Stop a streaming job gracefully: stop reading, finish work, fire windows |
| **Effectively-once** | Exactly-once results from at-least-once delivery plus idempotent processing/writes |
| **Event time / processing time** | When it happened / when the pipeline saw it |
| **Flex Template** | Parameterized, containerized Dataflow pipeline launched without source code |
| **FlexRS** | Cheaper batch execution with delayed scheduling |
| **Fusion** | Dataflow merging steps into one stage |
| **GroupByKey** | Shuffle that collects all values per key (and window) |
| **Hot key** | One key with much more data than others; bottleneck |
| **`id_label`** | Pub/Sub attribute used by Dataflow to dedupe on producer IDs |
| **Message storage policy** | Restricts the regions where Pub/Sub stores messages |
| **Ordering key** | Pub/Sub key; messages with the same key are delivered in order |
| **Pane** | One firing (output) of a window: EARLY, ON_TIME or LATE |
| **ParDo** | General per-element transform |
| **PCollection** | Distributed, immutable dataset in Beam; elements carry timestamp, window, pane |
| **PTransform** | Any operation on PCollections |
| **Reshuffle** | Redistributes data; breaks fusion |
| **Seek** | Move a subscription's ack state to a timestamp or snapshot (replay) |
| **Side input** | Small extra dataset broadcast to all workers |
| **Side (tagged) output** | Additional output of a DoFn, e.g. for dead letters |
| **Snapshot (Pub/Sub)** | Saved ack state of a subscription for later seek |
| **State and timers** | Per-key, per-window durable storage and callbacks in a DoFn |
| **Storage Write API** | BigQuery's high-throughput write API; exactly-once or at-least-once modes |
| **Streaming Engine** | Dataflow backend for streaming shuffle, state and timers |
| **System lag** | Dataflow metric: longest time an item has waited in a stage |
| **Trigger** | Rule for when a window emits a pane |
| **Watermark** | Estimate that all data up to event time W has arrived |
| **Window (fixed/sliding/session/global)** | Event-time grouping of elements |
