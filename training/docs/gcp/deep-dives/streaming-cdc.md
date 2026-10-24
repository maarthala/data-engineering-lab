# Streaming and CDC: Pub/Sub, Dataflow, Datastream

> Pub/Sub, Dataflow and Datastream for near real-time sales data. A common mistake is mixing **event-stream windowing** with **CDC ordering**; this page keeps them clearly separate.

---

## 1. Two different problems

```
EVENT STREAMING                                   CHANGE DATA CAPTURE (CDC)
"things that happened"                            "rows that changed in a database"

sensor reading, click, POS scan                   INSERT/UPDATE/DELETE on orders table
immutable events, many per second                 mutable state, the latest version matters
order problem: event time vs. arrival time        order problem: several changes to the SAME ROW
solution: windows + watermarks + lateness         solution: source log position per primary key
GCP: Pub/Sub → Dataflow → BigQuery                GCP: Datastream → BigQuery (or GCS → Dataflow)
AWS: Kinesis/Kafka → Flink/Firehose               AWS: DMS / Debezium → Kafka
```

Sensor or clickstream data via Kafka/Kinesis/Pub/Sub = **event streaming**.
Sales data from an operational database = **CDC**.
Real systems often have **both**, for example POS events streamed plus ERP via CDC.

---

## 2. Pub/Sub

### Concepts

| Concept | Meaning | Kafka equivalent |
|---|---|---|
| **Topic** | Named channel that publishers write to | Topic |
| **Subscription** | Named cursor that delivers messages to a consumer; **each subscription gets every message** | Consumer group |
| **Ack** | Consumer confirms processing; un-acked messages are redelivered after the **ack deadline** | Offset commit |
| **Ordering key** | Messages with the same key are delivered in order (within a region) | Partition key |
| **Retention** | Topic retention up to 31 days; subscription retention up to 7 days; **seek** to replay | Retention + offset reset |
| **Dead-letter topic** | After N failed deliveries, the message goes to the DLQ | Manual DLQ pattern |

Important differences from Kafka:

- **No partitions to manage.** Pub/Sub scales automatically.
- Delivery is **at-least-once by default**, so **consumers must be idempotent** (deduplicate on a message or event ID). **Exactly-once delivery** exists for pull subscriptions in one region.
- **Push vs. pull** subscriptions: push calls an HTTPS endpoint (Cloud Run), pull is used by Dataflow and clients.

### Subscription types worth knowing

| Type | Use |
|---|---|
| Pull / streaming pull | Dataflow, custom consumers |
| Push | Cloud Run / Functions |
| **BigQuery subscription** | Writes **directly to a BigQuery table**, no code. Good for simple raw ingestion |
| **Cloud Storage subscription** | Writes batched files to GCS (like Firehose → S3) |

> AWS comparison: **Kinesis Firehose → S3** ≈ **Pub/Sub → Cloud Storage subscription** or **BigQuery subscription**.

---

## 3. Dataflow (Apache Beam)

### Beam model in one picture

```
Pipeline
  └─ Read (PubSub / GCS / BigQuery / JDBC / Kafka)
       └─ PCollection (distributed dataset, bounded = batch, unbounded = stream)
            └─ ParDo / Map (per element)  → GroupByKey / Combine (aggregate per key + window)
                 └─ Write (BigQuery / GCS / Pub/Sub / Bigtable)
```

- **The same code runs batch or streaming.** Dataflow is Google's **managed runner** (autoscaling, monitoring, no cluster management).
- **Python or Java SDKs** (Java is more complete; Python is common for data engineers).

### Time and windows (event streaming)

| Concept | Meaning |
|---|---|
| **Event time** | When it happened (sensor timestamp) |
| **Processing time** | When the pipeline saw it |
| **Window** | How events are grouped in time: **fixed** (tumbling, e.g. 5 min), **sliding** (e.g. 1h every 5 min), **session** (gap-based, per user) |
| **Watermark** | The pipeline's estimate that "all events up to time T have arrived" |
| **Trigger** | When to emit results: at the watermark, early (speculative), late (on late data) |
| **Allowed lateness** | How long after the watermark late events still update a window, e.g. 15 min |
| **Accumulation mode** | Late firings emit the **full updated** result (accumulating) or only the **delta** (discarding) |

```python
import apache_beam as beam
from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms.trigger import AfterWatermark, AfterProcessingTime, AccumulationMode

(p
 | "Read"   >> beam.io.ReadFromPubSub(subscription=SUB, timestamp_attribute="event_ts")
 | "Parse"  >> beam.Map(parse_json)                                  # -> dict
 | "Key"    >> beam.Map(lambda r: ((r["building_id"], r["sensor_id"]), r["value"]))
 | "Window" >> beam.WindowInto(
        FixedWindows(5 * 60),
        trigger=AfterWatermark(late=AfterProcessingTime(60)),        # re-fire for late data
        allowed_lateness=15 * 60,                                    # accept events up to 15 min late
        accumulation_mode=AccumulationMode.ACCUMULATING)
 | "Avg"    >> beam.combiners.Mean.PerKey()
 | "ToRow"  >> beam.Map(to_bq_row)
 | "Write"  >> beam.io.WriteToBigQuery(TABLE, method="STORAGE_WRITE_API"))
```

Typical setup for sensor streams: 5-minute event-time windows with a watermark and 15 minutes of allowed lateness. Results are emitted at the watermark and updated for late events. Anything later than that goes to a late-data table and is fixed by a batch backfill.

### Dataflow operations

| Topic | What to know |
|---|---|
| **Autoscaling** | Horizontal autoscaling (`max_num_workers` caps cost). **Streaming Engine** moves state and shuffle off the workers, so scaling is smoother |
| **Right-fitting / Dataflow Prime** | Vertical autoscaling and per-stage resources |
| **Templates** | **Flex Templates** = a container-packaged pipeline with parameters, giving **reusable ingestion patterns**. Google also ships ready templates (Pub/Sub → BigQuery, Datastream → BigQuery, JDBC → BigQuery) |
| **Update vs. drain vs. cancel** | **Update** replaces a streaming job in place, keeping state. **Drain** stops reading and finishes in-flight work, so nothing is lost. **Cancel** stops immediately and loses in-flight data |
| **Exactly-once** | Dataflow streaming gives exactly-once processing by default. An **at-least-once mode** is cheaper and lower latency if the sink deduplicates |
| **Dead letters** | Wrap parsing in try/except and route bad records to a side output → DLQ table/topic |
| **Hot keys** | One key with huge volume → one worker overloaded. Fix with `Combine` with fanout, or salting |
| **Monitoring** | System lag, data freshness, backlog (unacked messages), throughput, worker CPU |
| **Cost** | Worker vCPU/memory hours + Streaming Engine/Shuffle. Use batch when latency allows (much cheaper); FlexRS for delay-tolerant batch |

### When to use Dataflow vs. alternatives

| Need | Use |
|---|---|
| Raw stream into BigQuery with no transformation | **Pub/Sub BigQuery subscription**, no Dataflow |
| Database CDC into BigQuery | **Datastream direct** |
| Streaming transformation, enrichment, windowed aggregation | **Dataflow** |
| SQL-expressible batch transformation | **BigQuery SQL** (Dataform/dbt), not Dataflow |
| Existing Spark code / lift-and-shift Spark | **Dataproc** (Serverless) |
| Continuous SQL on streaming data in BigQuery | **BigQuery continuous queries** (newer option) |

Don't start with Dataflow. Start with the simplest managed option and only add Dataflow when there's real stream processing logic.

---

## 4. Datastream (CDC)

### What it is
A **serverless CDC and replication service**. It reads a database's **transaction log** and streams changes to **BigQuery** or **Cloud Storage**.

Sources: **MySQL, PostgreSQL, Oracle, SQL Server**, AlloyDB, Cloud SQL (others may be in preview, so check). Destinations: **BigQuery** (direct) or **GCS** (Avro/JSON files).

### Building blocks

```
Connection profile (source)  ─┐
                               ├─► Stream (which schemas/tables, backfill on/off, write mode) ──► BigQuery datasets
Connection profile (dest)    ─┘                                                                (one per source schema or a single dataset)
Private connectivity config (VPC peering / PSC) or IP allowlist or SSH tunnel to reach the DB
```

### Lifecycle

1. **Backfill**: an initial **snapshot** of each selected table (can be skipped or re-triggered per table).
2. **CDC**: continuous changes from the log (it actually starts capturing from the log position before the backfill, so no change is missed).
3. **Write to BigQuery** in one of two modes:

| Write mode | Result in BigQuery | Use |
|---|---|---|
| **Merge** (default) | Table mirrors the **current state**; updates and deletes applied using the PK and BigQuery CDC (`max_staleness`) | Operational reporting, a "current" view |
| **Append-only** | **Every change is a row**, with metadata: change type, source timestamp, log position | Audit, history, SCD2 building, debugging |

Each row carries `datastream_metadata` (UUID, source timestamp, and so on). Use the **source timestamp** for freshness and ordering logic downstream.

### Source prerequisites

| DB | Requirement |
|---|---|
| **PostgreSQL** | `wal_level = logical`; a **publication** for the tables; a **replication slot** (pgoutput); a user with replication privileges; tables need a **primary key** (or replica identity) for updates/deletes |
| **MySQL** | `binlog_format = ROW`, `binlog_row_image = FULL`, enough binlog retention, GTID recommended |
| **Oracle** | Archive log mode, supplemental logging, LogMiner (or binary reader) |
| **SQL Server** | CDC enabled on the DB/tables, or transaction log mode |

### ⚠️ Operational risks and how to handle them

| Risk | What happens | Mitigation |
|---|---|---|
| **Stream paused/failed (Postgres)** | The replication slot holds WAL → **source disk fills up** → production outage | Monitor slot lag and WAL size; alert; a runbook to resume or drop the slot |
| **Log retention too short (MySQL/Oracle)** | The stream falls behind and logs are purged → unrecoverable gap | Enough retention (days); after a gap, **re-backfill** the table |
| **Schema change** | New column → propagated automatically. Type change, rename or drop → may fail or need handling | Alert on stream errors; coordinate DDL with source teams; downstream contract tests |
| **Tables without a PK** | Merge mode can't identify rows | Add a PK or use append-only |
| **Large initial backfill** | Load on the source DB | Backfill from a **read replica** if supported, off-peak, with limited concurrency |
| **Freshness vs. cost** | Low `max_staleness` = more merge cost | Agree an SLA (e.g. 5–15 min) |

### Handling CDC edge cases

| Edge case | Handling |
|---|---|
| **Out-of-order changes** | Datastream/BigQuery order changes per PK using the **source log position / change sequence number**. The latest wins. Deterministic, no manual review |
| **Deletes** | Merge mode: **row removed**. Append-only: a row with change type `DELETE`, which staging turns into `is_deleted` in staging and filter out in marts |
| **Schema changes** | Additive changes are automatic. Breaking changes are alerted and handled. Downstream models use explicit columns |
| **Backfill history** | Datastream **initial backfill**, re-triggerable per table. For huge history, consider a bulk export/load first, then CDC from a known position |
| **Freshness** | `max_staleness` on the target tables + downstream model cadence. Promise an **SLA** and measure `now - max(source_timestamp)` |

### Direct to BigQuery vs. GCS + Dataflow

| | Datastream → BigQuery | Datastream → GCS → Dataflow → BigQuery |
|---|---|---|
| Complexity | Low (no code) | Higher |
| Latency | Lowest | Higher |
| Transformation in flight | ❌ | ✅ PII masking, enrichment, routing |
| Raw replayable change files | ❌ (append-only mode gives history in BigQuery) | ✅ Files in GCS |
| Multiple targets | ❌ | ✅ |
| **Default choice** | ✅ | Only with a clear reason |

---

## 5. Exactly-once, idempotency and deduplication (applies to both)

The end-to-end rule: **assume at-least-once somewhere, make the sink idempotent.**

| Layer | Technique |
|---|---|
| Producer | Include a unique **event_id** (or source PK + log position) |
| Transport | Pub/Sub at-least-once (or exactly-once subscription); Kafka idempotent producer |
| Processing | Dataflow exactly-once; deduplicate on event_id within a window |
| Sink | **MERGE / upsert on key**, partition overwrite, or Storage Write API with offsets (exactly-once) |
| Downstream | `QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ingest_ts) = 1` |

In short: exactly-once end-to-end is really effectively-once: at-least-once delivery plus idempotent writes.

---

## 6. Monitoring streaming and CDC pipelines

| Metric | Where | Alert when |
|---|---|---|
| Pub/Sub **oldest unacked message age** / backlog | Cloud Monitoring | > N minutes, meaning consumers are behind |
| Dataflow **system lag**, **data freshness** | Dataflow metrics | > SLA |
| Datastream **total latency / throughput / errors** | Datastream metrics | Latency > SLA, or any unsupported event |
| Postgres **replication slot lag** (bytes of WAL retained) | Source DB monitoring | > threshold |
| **End-to-end freshness** in BigQuery | `now - MAX(_source_ts)` in a scheduled query | > SLA |
| DLQ count | DLQ table/topic | > 0, or above the baseline |

---
