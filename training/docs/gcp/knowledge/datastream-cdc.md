# Datastream and Change Data Capture (CDC) from the inside

> **Goal of this lesson:** understand how CDC works at the database-log level and how Datastream and BigQuery apply changes, well enough to design a near real-time sales pipeline and to run it in production.
>
> **Quick reference:** [Streaming and CDC deep dive](../deep-dives/streaming-cdc.md) (section 4). **Event streaming (Pub/Sub, Dataflow, windows):** [Pub/Sub and Dataflow](pubsub-dataflow.md).
>
> **The one key idea:** CDC ordering is **per primary key, by source log position**. It is **not** windowing, watermarks or allowed lateness. Out-of-order changes are resolved **automatically and deterministically**, never by manual review.

---

## 1. What it is and why it exists

### CDC in plain language
**Change Data Capture (CDC)** means: capture every **insert, update and delete** in a source database and deliver those changes to another system, continuously, in the right order.

Why not just copy the tables every night?

- A full copy of a large table takes hours and loads the source.
- A nightly copy is a day old. The business wants **near real-time** sales.
- A copy shows only the final state; you lose the **intermediate changes** (order created → paid → refunded) and you can't see **deletes** at all unless you compare full snapshots.

### Datastream in plain language
**Datastream** is Google's **serverless CDC and replication service**. It connects to a database (MySQL, PostgreSQL, Oracle, SQL Server, and others; check current docs for the list and preview sources), reads its **transaction log**, and writes the changes to **BigQuery** (directly, as up-to-date tables) or to **Cloud Storage** (as files). It also does the **initial copy** (backfill) for you.

**Analogy: a bank account.** The balance is the table. The bank statement is the transaction log: every deposit and withdrawal, numbered in order. If you want a copy of the balance somewhere else, you don't photocopy the balance every hour. You read the statement lines after the last one you processed and apply them in their number order. If two lines arrive at your desk in the wrong order, you sort them by their **line number**. You never "wait 15 minutes for late lines"; the line number tells you the truth.

That is exactly the difference between CDC and event streaming:

- In **event streaming**, each event is a new fact and you group facts in **time windows**.
- In **CDC**, each change **replaces** the previous version of a **row**, and the **log position** says which version is the latest.

---

## 2. Where it fits in a modern GCP data platform

In a typical enterprise data warehouse (EDW) modernization on GCP, Datastream sits next to BigQuery, Cloud Composer and Dataflow. It is the standard way to build **reusable CDC ingestion patterns** from operational databases. A common goal is a **near real-time sales data model** in BigQuery, exposed in Looker for (conversational) BI.

```text
  SOURCE (on-prem / Cloud SQL / AWS)        SHARED INGESTION PROJECT            EDW PROJECT (BigQuery)
  ┌──────────────────────────┐            ┌──────────────────────────┐       ┌──────────────────────────────┐
  │ POS / ERP Postgres        │  WAL      │ Datastream               │       │ cdc_pos (replica tables,      │
  │  orders, order_lines,     │ ───────►  │  connection profiles     │ ────► │   merge mode: current state)  │
  │  payments, refunds        │ logical   │  private connectivity    │       │ cdc_pos_log (append-only:     │
  │ (read replica? see §9)    │ decoding  │  stream: backfill + CDC  │       │   full change history)        │
  └──────────────────────────┘            └──────────────────────────┘       │        │ Dataform/dbt (Composer)│
                                                                              │        ▼                       │
                                                                              │ stg → fct_sales (incremental)  │
                                                                              │     → dim_* (SCD2)             │
                                                                              │        ▼                       │
                                                                              │ Looker (datagroups, freshness) │
                                                                              └──────────────────────────────┘
```

Typical tasks for a data engineer working with Datastream:

- Design **reusable** CDC ingestion: one Terraform module for connection profiles, private connectivity, stream and target datasets per source system.
- Choose **write mode** (merge vs. append-only) and **staleness** per table to balance freshness and cost.
- Build current-state and history models on top.
- Define and track **freshness, throughput, stability and cost** metrics.
- Write the **runbooks**: paused stream, schema change, re-backfill.

---

## 3. CDC fundamentals: the three ways to capture changes

### 3.1 Query-based CDC (timestamp polling)
Run a query every few minutes: `SELECT * FROM orders WHERE updated_at > :last_watermark`.

| Pros | Cons |
|---|---|
| Simple; no special DB permissions | **Misses hard deletes** (the row is gone, nothing to select) |
| Works on any database | Needs a reliable `updated_at` on every table, set by **every** writer |
| | **Misses intermediate changes** between polls (only the last version) |
| | Load on the source (repeated queries, needs indexes) |
| | Clock and transaction issues: a long transaction that commits *after* your poll, with an `updated_at` *before* your watermark, is **missed** forever |

The last problem is subtle and good to mention: `updated_at` is set when the row is written, but the row becomes visible when the transaction **commits**. A poll can pass the timestamp before the commit happens. People "fix" it with an overlap window (`> last_watermark - 5 min`), which then needs deduplication.

### 3.2 Trigger-based CDC
Database triggers on each table write every change into a "shadow" change table, which you then read.

| Pros | Cons |
|---|---|
| Captures deletes and every change | **Adds write load to every transaction** (each write becomes two) |
| Works without log access | Triggers must be maintained per table; schema changes break them |
| | DBAs and application teams usually dislike them |

### 3.3 Log-based CDC (what Datastream, Debezium, AWS DMS do)
Read the database's own **transaction log**, the log the database already writes for crash recovery and replication.

| Pros | Cons |
|---|---|
| Captures **every** committed insert/update/delete, including hard deletes | Needs log configuration and special privileges (DBA work) |
| **Commit order** is preserved (the log is ordered) | Log retention must be managed; a stopped reader can fill disks (Postgres) or lose position (MySQL/Oracle) |
| Very low source impact (reads a log, no queries on tables) | Engine-specific details (each DB is different) |
| Low latency (seconds) | Tables need a primary key (or a replica identity) for correct updates/deletes |

**This is the default for a serious EDW.** Query-based polling remains a fallback for sources without log access (e.g. some SaaS APIs or a vendor DB where you can't get privileges).

---

## 4. How database transaction logs work

### 4.1 The general idea: write-ahead logging
Every serious OLTP database follows the **write-ahead log** rule: before a change is written to the data files, a description of the change is written (appended) to a sequential log on disk. On a crash, the database replays the log to recover. Replicas use the same log to stay in sync.

```text
  Transaction T1: UPDATE orders SET status='PAID' WHERE id=42; COMMIT;

  1. append to log:  [pos 1000] T1 UPDATE orders id=42 status: NEW → PAID
  2. append to log:  [pos 1001] T1 COMMIT
  3. later:          data page for orders id=42 updated on disk (checkpoint)

  The log is APPEND-ONLY and has a strictly increasing POSITION.
```

The **position** has different names:

| Database | Log | Position name | Example |
|---|---|---|---|
| PostgreSQL | WAL (write-ahead log) | **LSN** (Log Sequence Number), a byte offset in the WAL | `0/16B3748` |
| MySQL | **binlog** (binary log) | **binlog file + offset**, or **GTID** (global transaction ID) | `mysql-bin.000123:4567`, `3E11FA47-...:23` |
| Oracle | **redo log** (online) → **archive log** (when full) | **SCN** (System Change Number) | `9876543210` |
| SQL Server | **transaction log** | **LSN** | `0x00000027:00000158:0003` |

### 4.2 Why the position gives a total order
- The log is written **sequentially** by the database itself. Position *n+1* is written after position *n*.
- Each change belongs to a transaction, and the **commit record** has a position too. CDC readers emit changes **in commit order** (a transaction's changes are emitted when it commits).
- So for any two changes to the **same row**, the one with the **higher position** happened later **in the database's own view of history**. This is not an estimate; it is the source of truth that the database itself uses for recovery.
- Two changes to the same row can never "happen at the same time": the database row lock forces one transaction to wait, so they get different, ordered positions.

```text
   Position (LSN)   Txn   Change
   0/1000           T7    INSERT order 42 status=NEW
   0/1100           T8    INSERT order 43 status=NEW
   0/1200           T9    UPDATE order 42 status=PAID
   0/1300           T10   DELETE order 43
   0/1400           T11   UPDATE order 42 status=REFUNDED

   Final state for order 42 = the change with the highest LSN = REFUNDED.
   Final state for order 43 = DELETE at 0/1300 → row gone.
```

**Contrast with event streaming:** an event's timestamp comes from a device clock. Clocks drift, devices go offline, and there is no global sequence. That's why streaming needs a **watermark** (a guess about completeness). CDC has a **real sequence**, so no guess is needed.

### 4.3 PostgreSQL: WAL and logical decoding
The Postgres WAL is **physical** by default: it describes byte changes to data pages. To get **row-level** changes, Postgres has **logical decoding**:

```text
  Postgres primary
  ┌───────────────────────────────────────────────────────────────────┐
  │ backends write WAL ──► pg_wal/ (16 MB segment files)               │
  │                              │                                     │
  │   wal_level = logical  (adds the info needed to decode rows)       │
  │                              ▼                                     │
  │   walsender process ── logical decoding ── output plugin pgoutput  │
  │      │   uses REPLICATION SLOT "datastream_slot"                   │
  │      │   filters by PUBLICATION "datastream_pub" (which tables)    │
  └──────┼─────────────────────────────────────────────────────────────┘
         ▼
   Datastream reads row changes in commit order, confirms LSN back to the slot
```

- **`wal_level = logical`**: makes the WAL contain enough information to rebuild row changes. Needs a restart.
- **Publication** (`CREATE PUBLICATION ... FOR TABLE ...` or `FOR ALL TABLES`): the list of tables whose changes are published.
- **Replication slot** (`pg_create_logical_replication_slot('datastream_slot', 'pgoutput')`): a **server-side bookmark**. It remembers the LSN that the consumer has confirmed. **Postgres keeps all WAL from that LSN onwards**, even if the consumer is gone. This guarantees no change is lost, and it is also the biggest operational risk (section 9.6).
- **pgoutput**: the built-in output plugin that turns WAL into logical change messages (also used by native logical replication).
- **REPLICA IDENTITY**: what the WAL records about the **old** row on UPDATE/DELETE. `DEFAULT` = the primary key columns. Without a PK, updates/deletes can't identify the row unless you set `REPLICA IDENTITY FULL` (whole old row, more WAL) or use a unique index.
- **TOAST detail:** large column values stored out-of-line (TOAST) are not repeated in the WAL for an UPDATE that doesn't change them. Consumers may receive "unchanged TOAST" placeholders. `REPLICA IDENTITY FULL` avoids this at the cost of more WAL. (Check current Datastream docs for how it handles this.)

```sql
-- On the Postgres source (self-managed; on Cloud SQL use the cloudsql.logical_decoding flag)
ALTER SYSTEM SET wal_level = logical;                 -- restart required
CREATE USER datastream_user WITH REPLICATION LOGIN PASSWORD '***';
GRANT USAGE ON SCHEMA pos TO datastream_user;
GRANT SELECT ON ALL TABLES IN SCHEMA pos TO datastream_user;   -- needed for backfill
ALTER DEFAULT PRIVILEGES IN SCHEMA pos GRANT SELECT ON TABLES TO datastream_user;
CREATE PUBLICATION datastream_pub FOR TABLE pos.orders, pos.order_lines, pos.payments;
SELECT pg_create_logical_replication_slot('datastream_slot', 'pgoutput');
```

### 4.4 MySQL: the binlog
- The **binlog** records changes for replication. Three formats: `STATEMENT` (the SQL text, not usable for CDC because non-deterministic statements like `NOW()` give different results), `MIXED`, and **`ROW`** (the before/after image of each changed row). CDC needs **`binlog_format = ROW`** and **`binlog_row_image = FULL`** (all columns, not only changed ones).
- Binlog files rotate (`mysql-bin.000123`, `...124`) and are **purged** after the retention (`binlog_expire_logs_seconds`). If the CDC reader is behind the purge point, its position no longer exists → the stream fails and you must **re-backfill**. Unlike Postgres, MySQL does **not** keep logs for a slow reader.
- **GTID** (`gtid_mode = ON`): each transaction gets a global ID `server_uuid:sequence`. It makes the position **independent of the file name**, which helps with **failover**: after the primary fails over to a replica, the reader can continue by GTID. Datastream supports GTID-based replication for MySQL (check current docs for which versions).
- Reading from a **replica** is possible if the replica writes its own binlog (`log_replica_updates` / `log_slave_updates`).

### 4.5 Oracle: redo, archive logs, LogMiner
- Oracle writes changes to **online redo logs** (a small ring of files). When a redo file is full, it is copied to an **archive log** if the DB is in **ARCHIVELOG mode** (required for CDC).
- **Supplemental logging** must be enabled (database-level minimal, plus all or PK columns at table level), otherwise the redo doesn't contain enough to identify rows.
- **LogMiner** is Oracle's built-in API that reads redo/archive logs and returns SQL-level row changes. Datastream can use **LogMiner** or a **binary log reader** (faster, reads logs directly; check current docs for support and requirements).
- **SCN** is Oracle's global, increasing change number.
- Risk: archive logs are deleted by RMAN retention; if the reader falls behind, changes are lost.

### 4.6 SQL Server: CDC and transaction log
- SQL Server has built-in **CDC**: `sys.sp_cdc_enable_db` and `sys.sp_cdc_enable_table` create **change tables** populated by a log reader job (SQL Server Agent). Change tables are cleaned up after a retention period (default 3 days).
- Datastream supports reading from **change tables** or directly from the **transaction log** (check current docs; the transaction-log method has its own requirements).
- Position = **LSN**.

### 4.7 What a change record looks like
Regardless of the engine, a log-based CDC record contains:

```text
  {
    operation:  INSERT | UPDATE | DELETE  (BigQuery: UPSERT | DELETE)
    table:      pos.orders
    key:        {order_id: 42}
    after:      {order_id: 42, status: "PAID", amount: 19.90, ...}   (full row for insert/update)
    before:     {...}                                                (optional, depends on engine/settings)
    position:   LSN 0/1200   (or SCN, binlog file:offset, GTID)
    commit_ts:  2026-10-23T08:01:12.345Z   (source timestamp)
    txn_id:     T9
  }
```

---

## 5. Datastream architecture

### 5.1 Building blocks

```text
 ┌──────────────────────── Datastream (regional service) ─────────────────────────┐
 │                                                                                 │
 │  Private connectivity configuration ─── VPC peering (Datastream ↔ your VPC)     │
 │      or Private Service Connect interface (newer; check docs)                   │
 │                                                                                 │
 │  Source connection profile             Destination connection profile           │
 │   type: PostgreSQL                      type: BigQuery  (or Cloud Storage)       │
 │   host, port, db, user                                                          │
 │   password / Secret Manager ref                                                 │
 │   connectivity: IP allowlist | forward SSH tunnel | private connectivity        │
 │            │                                      │                             │
 │            └──────────────►  STREAM  ◄────────────┘                             │
 │                              - include/exclude schemas & tables                 │
 │                              - Postgres: publication + replication slot names    │
 │                              - backfill: all | none (+ per-table re-trigger)    │
 │                              - BigQuery: dataset per schema or single dataset,   │
 │                                write mode merge | append-only, staleness         │
 │                              - GCS: path prefix, Avro | JSON, file rotation      │
 │                              - state: NOT_STARTED → STARTING → RUNNING ⇄ PAUSED │
 │                                       → FAILED / FAILED_PERMANENTLY              │
 └─────────────────────────────────────────────────────────────────────────────────┘
```

- **Connection profile**: how to reach a source or destination (reusable across streams).
- **Connectivity options** to the source:
    - **IP allowlist**: the DB accepts connections from Datastream's public IPs (per region). Simple, but traffic goes over the internet (TLS required).
    - **Forward SSH tunnel**: via a bastion host.
    - **Private connectivity**: Datastream creates a private connection that **peers** with your VPC (or uses a Private Service Connect interface). Traffic stays private. Note: VPC peering is **not transitive**, so reaching a source behind another peering (e.g. Cloud SQL private IP, or on-prem via VPN/Interconnect) may need a **reverse proxy VM** or the correct network setup; check current docs.
- **Credentials**: store the DB password in **Secret Manager** and reference it from the connection profile (supported in current versions; check docs), instead of putting it in Terraform state.
- **Stream**: the actual replication job. One stream per source database is typical.

### 5.2 The two phases: backfill and CDC, and how they are stitched together

```text
 time ───────────────────────────────────────────────────────────────────────►

 log:    ... 0/900 │ 0/1000  0/1100  0/1200  0/1300  0/1400  0/1500 ...
                   │
 stream start:     ▼  Datastream records/creates the start position P0 = 0/1000
                      and starts reading CDC from P0

 backfill:            [ SELECT * FROM orders  (chunks, in parallel, takes hours) ]
                         reads row 42 when it is at version "PAID"  (committed at 0/1200)

 CDC:                 0/1000 ... 0/1200 (42 PAID) ... 0/1400 (42 REFUNDED) ...

 destination receives for row 42:  backfill(PAID), cdc(PAID @0/1200), cdc(REFUNDED @0/1400)
 applying "latest by order" → REFUNDED  ✔   no change missed, overlaps are harmless
```

The principle (Datastream's exact internals are not fully public, but this is the logic):

1. **CDC capture starts first** (or at the same time), from a known log position. So every change committed after that point is captured.
2. The **backfill** reads table snapshots in chunks, concurrently with CDC.
3. A row may appear **both** in the backfill and in CDC (overlap). That's fine, because:
    - Each change carries the **full row image** and is applied as an **upsert by primary key** (idempotent).
    - The destination orders events per key using **ordering metadata** (source position / sequence numbers / timestamps) so an older backfill image can't overwrite a newer CDC change.
4. Result: **no gap and no permanent duplicates**. This is the same "snapshot + log from a position before the snapshot" technique that Debezium and AWS DMS use.

**Backfill controls:**

- Backfill **all** objects, or **none** (CDC only, e.g. when you loaded history by another method).
- **Re-trigger backfill per table** (after a failure, a lost log position, or a breaking schema change).
- **Exclude** big tables from automatic backfill and backfill them later.
- Backfill concurrency limits (to protect the source; check current settings).

### 5.3 Destinations

#### BigQuery (direct)
Datastream creates datasets and tables (with the primary key declared) and streams changes in using the BigQuery **Storage Write API**. Two **write modes**:

| Write mode | What the table contains | How it works | Use it for |
|---|---|---|---|
| **Merge** (default) | **Current state**, one row per primary key; deletes removed | Changes arrive as `UPSERT`/`DELETE` CDC records; BigQuery applies them per PK (section 6) | Replica tables, current-state reporting, the base of the sales model |
| **Append-only** | **Every change is a row**: inserts, each update, deletes, with metadata | Plain appends; no CDC apply | History, audit, **SCD2** building, debugging, "what was the status at 10:00?" |

**Metadata** (column names vary by mode and version; check current docs):

- `datastream_metadata.uuid`: unique ID of the change event (dedup key).
- `datastream_metadata.source_timestamp`: when the change happened in the source (commit time). **Use it for freshness and ordering logic downstream.**
- In append-only mode also: `change_type` (INSERT/UPDATE-INSERT/UPDATE-DELETE/DELETE… check exact values), `is_deleted`, `change_sequence_number`, and **sort keys** that give the per-row order.

**Staleness (freshness) setting:** the stream sets the BigQuery table's **`max_staleness`** (Datastream calls it data freshness/staleness limit; the default is on the order of minutes, check docs). You can change it per table later with `ALTER TABLE`.

#### Cloud Storage
- Files per table in **Avro** or **JSON** (JSON optionally compressed), rotated by size or time (check limits), under a path like `gs://bucket/prefix/schema_table/yyyy/mm/dd/hh/mm/...`.
- Each record has a generic envelope:

```json
{
  "uuid": "b9f8...",
  "read_timestamp": "2026-10-23T08:01:13.001Z",
  "source_timestamp": "2026-10-23T08:01:12.345Z",
  "object": "pos_orders",
  "read_method": "postgresql-cdc-logical-decoding",
  "source_metadata": {
    "schema": "pos", "table": "orders",
    "is_deleted": false, "change_type": "UPDATE",
    "tx_id": 9123, "lsn": "0/1200"
  },
  "payload": { "order_id": 42, "status": "PAID", "amount": 19.90 }
}
```

`read_method` tells you whether a record came from the **backfill** or **CDC** (values are engine-specific, e.g. a "-backfill" suffix vs. "-cdc-..."; check docs). The position field depends on the engine: `lsn` (Postgres), `log_file` + `log_position` (MySQL), `scn` / `rs_id` / `ssn` (Oracle).

GCS output can send **Pub/Sub notifications** for new files, which is how the Datastream-to-BigQuery Dataflow template discovers them (section 10).

### 5.4 Schema drift handling
| Change in source | What Datastream/BigQuery does (verify in current docs) | Your handling |
|---|---|---|
| **Add column** | Detected; the column is added to the BigQuery table; new rows have values, old rows NULL | Usually nothing. Downstream models use explicit column lists, so new columns don't break them |
| **Add table** (matching include rules) | Included automatically if the include list uses a wildcard; needs backfill | Decide if it belongs in scope; publication must include it in Postgres |
| **Drop column** | Column stays in BigQuery (no data loss), becomes NULL for new rows | Communicate; deprecate downstream |
| **Rename column** | Typically seen as a new column (old stays) | Treat as breaking; map in staging |
| **Change type** (e.g. INT → STRING, precision change) | May be incompatible with the BigQuery column → events fail / "unsupported event" errors | Alert; create a new column or recreate the table and **re-backfill** |
| **Change primary key** | Breaks merge logic | Recreate table + re-backfill |

**Principle:** additive changes are automatic; breaking changes are **detected, alerted and handled deliberately**. Protect downstream with **explicit SELECT lists** (no `SELECT *`), Dataform/dbt **schema tests and contracts**, and a DDL agreement with the source team ("tell us before you change types").

### 5.5 Stream states and operations
- **NOT_STARTED → STARTING → RUNNING**: normal.
- **PAUSED**: you paused it. Datastream remembers its position. On resume, it continues from there **if the log still has it**.
- **FAILED**: a recoverable error (network, credentials, permissions). Datastream retries; fix the cause and it resumes.
- **FAILED_PERMANENTLY**: cannot continue (e.g. log position no longer available). Needs intervention: recreate/reset position and re-backfill.
- **DRAINING**: finishing in-flight data before pausing.
- You can also choose a **CDC start position** for some sources (e.g. start from a specific LSN/GTID/SCN), useful after a bulk history load (check current docs per source).

### 5.6 Monitoring metrics
Datastream exposes metrics in Cloud Monitoring (names may differ slightly; check docs):

| Metric | Meaning | Alert when |
|---|---|---|
| **Data freshness** | How far behind the source the stream is (now − source timestamp of the last processed event) | > SLA (e.g. 10 min) |
| **System latency / total latency** | Time from reading the change to writing it to the destination; total includes source → destination | Grows steadily |
| **Throughput** (events/s, bytes) | Volume processed | Drops to 0 during business hours |
| **Unsupported events** / errors | Events that could not be processed (e.g. type issues) | Any |
| **Stream state** | RUNNING/PAUSED/FAILED | Not RUNNING |
| Source side: **replication slot lag** (Postgres) | `pg_current_wal_lsn() - confirmed_flush_lsn` in bytes | > threshold (e.g. 10 GB or 20% of free disk) |
| Destination: **BigQuery freshness** | `CURRENT_TIMESTAMP() - MAX(datastream_metadata.source_timestamp)` | > SLA |

```sql
-- Postgres: how much WAL each slot is holding back
SELECT slot_name, active, wal_status,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS retained_wal
FROM pg_replication_slots;
```

---

## 6. How BigQuery applies the changes (merge mode)

### 6.1 BigQuery CDC tables
BigQuery has native **change data capture** support. A CDC-enabled table needs:

1. A **primary key** declared (`PRIMARY KEY (...) NOT ENFORCED`). BigQuery doesn't enforce it; it uses it to know **which row** a change applies to.
2. The **`max_staleness`** table option (e.g. `INTERVAL 15 MINUTE`).

Changes are written through the **Storage Write API** with two pseudo-columns:

- **`_CHANGE_TYPE`**: `UPSERT` (insert or update, the full row) or `DELETE` (only the key matters).
- **`_CHANGE_SEQUENCE_NUMBER`** (optional): a custom ordering value. If present, BigQuery keeps, per primary key, the change with the **highest** sequence number. If absent, BigQuery uses ingestion order. Datastream supplies ordering information derived from the **source log position** so the result follows the source's commit order (exact internals: check docs).

```sql
-- What a CDC-enabled table looks like (Datastream creates this for you)
CREATE TABLE cdc_pos.orders (
  order_id    INT64 NOT NULL,
  store_id    STRING,
  status      STRING,
  amount      NUMERIC,
  updated_at  TIMESTAMP,
  datastream_metadata STRUCT<uuid STRING, source_timestamp INT64>,
  PRIMARY KEY (order_id) NOT ENFORCED
)
CLUSTER BY order_id
OPTIONS (max_staleness = INTERVAL 15 MINUTE);
```

(BigQuery CDC tables are clustered by the primary key, which makes applying changes efficient. Partitioning CDC tables has restrictions; check current docs.)

### 6.2 Background apply vs. merge-on-read

```text
                      ┌───────────────────────────────┐
  Datastream ──UPSERT/DELETE──► change buffer (recent,  │
                      │         not yet applied)       │
                      │              │ background apply │  (BigQuery runs merge jobs
                      │              ▼                  │   periodically; billed to you)
                      │        base table (applied)     │
                      └───────────────────────────────┘

  Query at time Q:
   - if last apply ≥ Q − max_staleness  → read base table only      (cheap, fast, "stale up to max_staleness")
   - else                               → base table + MERGE buffered changes at query time
                                          (merge-on-read: correct but slower and more expensive)
```

- **Background apply**: BigQuery periodically merges buffered changes into the base table. These are **background jobs** that consume compute (with Editions you can assign a reservation for background jobs; check docs).
- **Query behaviour:** queries return data that is **at most `max_staleness` old**. If background apply has fallen behind more than that, the query does the merge **at read time** to keep the promise.

### 6.3 The cost/freshness trade-off
| `max_staleness` | Freshness | Cost | Use |
|---|---|---|---|
| Very low (0–1 min) | Near real-time | **Highest**: frequent applies and/or merge-on-read in every query | Only if the business really needs it |
| 5–15 min | Good near real-time | Moderate | **Typical** for "near real-time sales" |
| 1 hour+ | Hourly | Lowest | Most dimension/master data |

**Senior answer:** freshness is a **business SLA**, not a technical maximum. Agree "sales in Looker at most 15 minutes old", set `max_staleness` per table accordingly (fact tables lower, dimensions higher), then **measure** it.

```sql
ALTER TABLE cdc_pos.orders   SET OPTIONS (max_staleness = INTERVAL 5 MINUTE);
ALTER TABLE cdc_pos.products SET OPTIONS (max_staleness = INTERVAL 1 HOUR);
```

---

## 7. ⭐ The big distinction: CDC ordering vs. stream windowing

This is the section to know by heart.

### 7.1 Side by side

| | **Event streaming** (Pub/Sub → Dataflow) | **CDC** (Datastream → BigQuery) |
|---|---|---|
| What a record is | An immutable **fact** ("sale happened", "temperature was 21.3") | A **new version of a row** (it replaces the previous version) |
| What you want at the end | Aggregates over time (revenue per 5 min) | The **current state** of each row (and maybe its history) |
| Source of order | Event timestamp from a device/app clock: **no global sequence** | The database **log position** (LSN/SCN/binlog/GTID): **a total commit order** |
| What "out of order" means | An event arrives after later events; its time window may already be emitted | A change for row K arrives after a newer change for row K |
| How it's resolved | **Watermark** (estimate) + **allowed lateness** + triggers; late data may update a window or be dropped | **Compare positions per primary key**; the higher position wins; older change is ignored |
| Is there uncertainty? | Yes, the watermark is a heuristic | **No**, the position is exact |
| Can data be "too late"? | Yes (after allowed lateness it is dropped) | **No**. An older change is simply superseded; a newer change always applies, however late it arrives |
| Need windows? | Yes | **No** |
| Duplicates | Dedup by event ID | Idempotent upsert by PK (same change applied twice = same result); `uuid` for append tables |
| Bad records | DLQ for unparseable events | Unsupported events / errors in stream metrics; fix and re-backfill the table |

### 7.2 Example A: event streaming (windows needed)
Sensor S1 readings, 5-minute windows, allowed lateness 15 min:

```text
 arrives  event_ts  value
 08:01    08:00:30  10
 08:03    08:03:10  14
 08:09    08:04:50  18      ← late, but within allowed lateness → window [08:00,08:05) updated: avg 14
 08:25    08:01:00  12      ← beyond allowed lateness → DROPPED (or to a late-data path)
```
Every reading **counts**. The question is "**which time bucket**, and is the bucket still open?"

### 7.3 Example B: CDC (no windows; order by position)
Order 42 in the POS database:

```text
 source commit       LSN      change
 08:00:00            0/1000   INSERT 42 status=NEW      amount=50
 08:00:05            0/1200   UPDATE 42 status=PAID
 08:30:00            0/1400   UPDATE 42 status=REFUNDED

 Suppose they ARRIVE at the destination in this order (retry, parallel backfill/CDC):
   1) 0/1000 NEW   2) 0/1400 REFUNDED   3) 0/1200 PAID   (PAID arrives last!)

 Apply rule per key 42: keep the change with the HIGHEST position.
   after 1): NEW (0/1000)
   after 2): REFUNDED (0/1400)         0/1400 > 0/1000 → apply
   after 3): PAID (0/1200) is IGNORED  0/1200 < 0/1400 → older, superseded
   result:   REFUNDED  ✔  deterministic, no waiting, no manual review
```
The question is never "which bucket?" It is "**which version is newest?**", and the log position answers it exactly.

### 7.4 What about deletes arriving out of order?
```text
 0/2000 INSERT 77   0/2100 DELETE 77      arrival: DELETE first, INSERT second
 Naive apply: delete (nothing to delete), then insert → row 77 EXISTS  ✘ wrong
 Correct:     keep per key the highest position = DELETE at 0/2100 → row 77 is gone ✔
```
This is why systems keep the **position of the delete** (a "tombstone") for some time, and why BigQuery's CDC compares **sequence numbers** rather than applying blindly in arrival order.

### 7.5 Where do windows appear around CDC, then?
Only **downstream**, if you build event-style metrics **from** CDC data, e.g. "revenue per 5 minutes by `paid_at`". That's an analytical query over the replicated table, written in SQL (`TIMESTAMP_TRUNC(paid_at, MINUTE)`), and late corrections are handled by **re-computing recent partitions** in an incremental model, not by stream watermarks.

### 7.6 Summary in one paragraph
In CDC, out-of-order data isn't a windowing problem. Every change carries the source log position: LSN in Postgres, SCN in Oracle, binlog position or GTID in MySQL. Datastream and BigQuery apply changes per primary key and keep the one with the highest position, so the result is deterministic. Watermarks and allowed lateness are for event streams in Dataflow. The dead-letter path in CDC is only for changes that can't be applied, like an incompatible type; you fix those and re-backfill, you don't review them by hand.

---

## 8. Core concepts, one by one

### 8.1 Primary keys
- **What:** the column(s) that identify a row.
- **Inside:** merge-mode needs the PK to decide which BigQuery row an UPSERT or DELETE targets. Postgres needs it (or `REPLICA IDENTITY`) to put the old key in the WAL for updates/deletes.
- **Why it matters:** without a PK there's no "current state" per row.
- **Example:** `ALTER TABLE pos.payments ADD PRIMARY KEY (payment_id);`
- **Common mistake:** assuming every legacy table has a PK. Check first: `SELECT ... FROM information_schema.tables t LEFT JOIN information_schema.table_constraints c ... WHERE c.constraint_type IS NULL`.

### 8.2 Tables without primary keys
Options, in order of preference:

1. **Add a PK** in the source (best; talk to the owners).
2. Postgres: `REPLICA IDENTITY FULL` or `USING INDEX` on a unique index so updates/deletes are identifiable in the WAL.
3. Replicate the table in **append-only** mode (Datastream treats tables without a PK as append-only in BigQuery; verify in current docs) and build current state downstream with a **business key** if one exists.
4. If the table is small and rarely changes, a scheduled **full reload** may be simpler than CDC.

### 8.3 Deletes: hard vs. soft
- **Hard delete in source** (`DELETE FROM`):
    - Merge mode: the row is **removed** in BigQuery. History is lost (unless you also have append-only).
    - Append-only: a change row with `change_type = DELETE` / `is_deleted = true`. Downstream: `is_deleted` flag in staging; marts filter it out; SCD2 closes the version.
- **Soft delete in source** (`UPDATE ... SET deleted_at = now()`): just an update for CDC. Your models must interpret `deleted_at`.
- **Senior point:** for sales, **refunds and cancellations** are usually status updates, not deletes. Find out which; it changes the fact model.
- **Common mistake:** using merge-mode replica tables as the only copy, then the business asks "how many orders were deleted last month?" Keep an **append-only** stream (or a second destination) for history-critical tables.

### 8.4 Merge vs. append-only in practice
You often need **both**: merge for cheap current-state queries, append-only for history and SCD2. Options:

- Two streams from the same source (two slots/publications in Postgres → more source load and two WAL holders), or
- **Append-only only**, and build current state yourself in an incremental model (`QUALIFY ROW_NUMBER() OVER (PARTITION BY pk ORDER BY sort keys DESC) = 1`). Cheaper on the source, more SQL, and you control the apply.
- Or Datastream → **GCS** (full change log as files) + BigQuery for current state.
Choose by table importance; document the choice in the pattern.

### 8.5 Backfill strategies for big tables
- **Default:** Datastream backfill (parallel chunked reads). Fine for most tables up to hundreds of GB (depends on source and network; test).
- **Protect the source:** limit backfill concurrency; run during off-peak hours; backfill from a **read replica** where supported (MySQL replica with binlog; Oracle/SQL Server options vary; Postgres logical decoding on standby needs PG16+ and Datastream support; check docs).
- **Huge history (TBs, years):** split it:
    1. Start CDC first (or record the log position) so new changes are captured.
    2. Load history by bulk export (e.g. database dump → GCS → BigQuery load, or BigQuery federated query for Cloud SQL), for data **before** the position.
    3. Merge history + CDC in BigQuery by PK and position/timestamp. Overlaps are resolved by "latest wins".
- **Exclude** huge, cold tables from automatic backfill and handle them separately.
- **Partition the target** of your downstream models by business date so history loads don't rewrite everything.

### 8.6 Schema changes
See 5.4. Add: a **schema registry mindset**: keep a contract (expected columns and types) in Dataform/dbt tests; run them on every model build; alert on Datastream "unsupported event" metrics; agree a **DDL change process** with source owners (additive changes free; breaking changes announced, with a migration plan: new column → dual write → switch → drop).

### 8.7 Replication slot and WAL growth risk (Postgres)
- **Why:** the slot keeps WAL from the last confirmed LSN. If Datastream stops confirming (stream paused, failed, network/IAM issue), WAL **accumulates** on the primary's disk.
- **Impact:** disk full → **the production database stops accepting writes**. This is the #1 CDC incident.
- **Guardrails:**
    - Alert on retained WAL per slot (query in 5.6) and on stream state ≠ RUNNING.
    - **`max_slot_wal_keep_size`** (Postgres 13+): caps WAL kept for slots. If exceeded, the slot is **invalidated** (`wal_status = 'lost'`), the DB is safe, but CDC can't continue → **re-create the slot and re-backfill**. This is a deliberate choice: **protect production over CDC continuity**.
    - Size disks with headroom for the longest outage you accept (e.g. a weekend).
    - Drop **unused** slots (old tests); an inactive slot still holds WAL.
- **Heartbeats:** on a DB with **low write volume** in the replicated tables but high writes elsewhere, the slot's confirmed LSN may not advance, so WAL piles up. A periodic write to a small heartbeat table in the publication keeps it moving (check Datastream guidance).

### 8.8 Log retention risk (MySQL, Oracle, SQL Server)
The opposite failure mode: the DB **does not wait** for the reader. If the stream is down longer than log retention, the position is purged and the stream **fails permanently** for those changes. Guardrails: binlog/archive retention of **several days** (e.g. ≥ 7 days, per Datastream recommendations; check docs), alerts on stream state, and a tested **re-backfill** procedure.

### 8.9 Source impact
- Log reading is light, but the **backfill** does full table scans.
- Logical decoding uses CPU on the primary (Postgres decodes on the server).
- Long-running transactions on the source delay CDC output (changes are emitted at commit) and hold back the slot.
- Mitigate: off-peak backfill, concurrency limits, replicas where supported, and agree a monitoring view with the DBAs.

### 8.10 Security
- **Least-privilege DB user**: Postgres `REPLICATION` + `SELECT` on the replicated tables only (no superuser); MySQL `REPLICATION SLAVE`, `REPLICATION CLIENT`, `SELECT`; Oracle LogMiner-specific grants.
- **Secret Manager** for the password; rotate it; the Datastream service agent gets `secretAccessor` on that secret only.
- **Private connectivity** instead of public IP allowlists where possible; TLS for the DB connection.
- **IAM:** Terraform deploys Datastream with a CI service account; the Datastream service agent needs BigQuery write (dataset-level) or GCS write; humans get `datastream.viewer`.
- **PII:** Datastream writes data as is. If columns must be masked **before** they land, use the GCS + Dataflow path, exclude columns in the stream config (column-level include/exclude is supported; check docs), or apply BigQuery **column-level security / policy tags / data masking** on the landing tables.

---

## 9. What matters most in practice

### 9.1 CDC ≠ windowing
See section 7. CDC changes are ordered per primary key by the source log position; the latest wins; the result is deterministic. Watermarks and allowed lateness do not belong in a CDC design, except as a contrast with event streaming.

### 9.2 Default to Datastream → BigQuery direct
It is the lean, stable option with the fewest moving parts. Go through **GCS + Dataflow** only for a clear reason: in-flight transformation/masking, multiple targets, a replayable raw change archive, or a destination Datastream doesn't support. Make this trade-off explicit in every design.

### 9.3 Source prerequisites per engine
Knowing `wal_level=logical` + publication + slot + `pgoutput` (Postgres), `binlog_format=ROW` + `binlog_row_image=FULL` + retention + GTID (MySQL), ARCHIVELOG + supplemental logging + LogMiner (Oracle), CDC tables or log method (SQL Server). Most Datastream setup problems come from a missing prerequisite on the source.

### 9.4 Freshness: `max_staleness` as an SLA
Explain the change buffer, background apply and merge-on-read; set staleness per table; measure `now − MAX(source_timestamp)` end to end, including the downstream model schedule. "Near real-time" = Datastream seconds + staleness minutes + model interval minutes + Looker cache.

### 9.5 Merge vs. append-only, deletes and history
Know what each mode stores, how deletes appear, and how to build current state and SCD2. Sales use cases need history (refunds, status changes).

### 9.6 Operational risks: slot/WAL growth and log retention
The paused-stream scenario (section 13). Know both failure modes: Postgres fills the disk; MySQL/Oracle lose the position.

### 9.7 Schema evolution
Additive automatic; breaking alerted; downstream contracts. Have a re-backfill procedure.

### 9.8 Reusable, secure ingestion pattern
Terraform module per source: private connectivity, connection profiles (Secret Manager), stream with include lists, target datasets with IAM, staleness defaults, monitoring alerts, runbook. This is what a platform team typically owns in shared ingestion projects.

### 9.9 Backfill and history strategy
Automatic backfill for normal tables; bulk load + CDC from a position for very large history; re-backfill per table after failures.

### 9.10 Metrics
Freshness (Datastream + BigQuery), throughput, errors/unsupported events, slot lag, and cost (Datastream GB processed + BigQuery background apply). Together these cover the usual platform metrics: load duration, throughput, stability and cost.

---

## 10. Datastream → GCS → Dataflow → BigQuery: when and why

```text
  Source DB ─► Datastream ─► GCS (Avro/JSON change files) ─► Pub/Sub notification
                                     │                              │
                                     │                              ▼
                                     │                   Dataflow "Datastream to BigQuery"
                                     │                   template (or your own pipeline):
                                     │                    - read new files
                                     │                    - optional UDF (mask, enrich)
                                     │                    - write to STAGING table (append, every change)
                                     │                    - periodic MERGE into REPLICA table by PK,
                                     │                      ordered by source position/timestamp
                                     │                    - dead-letter folder/table for failures
                                     ▼
                         raw change archive (replayable, cheap, lifecycle rules)
```

| Reason | Example |
|---|---|
| **Transformation or masking in flight** | Hash customer emails before any BigQuery dataset sees them |
| **Multiple destinations** | Same changes to BigQuery and to Bigtable/Spanner/another system |
| **Replayable raw archive** | Re-process all changes after a bug, independent of BigQuery |
| **Custom apply logic** | Special conflict rules, soft-delete conversion, routing per tenant |
| Destination not supported directly | e.g. Postgres/Spanner targets (templates exist; check docs) |

Costs of this path: more components (GCS, Pub/Sub, Dataflow 24/7), more latency (file rotation + merge interval), more to monitor. **Default is direct.**

Note: in your own Dataflow pipeline on CDC files, you still **don't use windows for correctness**. You order by the position fields per key (or let a BigQuery MERGE do it). Windows in that pipeline would only be a technical batching mechanism.

**Mapping:** this path ≈ **Debezium → Kafka → consumer → warehouse**, or **AWS DMS → S3 (CDC files) → Glue/Spark MERGE**. Datastream direct ≈ DMS with a target that applies changes.

---

## 11. Mapping from other platforms

| Other platform | GCP | Notes |
|---|---|---|
| **AWS DMS** (replication instance, endpoints, task: full load + CDC) | **Datastream** (serverless; connection profiles; stream: backfill + CDC) | No instance to size in Datastream. DMS "full load + ongoing replication" = backfill + CDC |
| DMS endpoints | Connection profiles | |
| DMS task table mappings | Stream include/exclude lists | |
| DMS → S3 CDC files (`Op` column I/U/D) | Datastream → GCS (Avro/JSON, `change_type`, `is_deleted`) | |
| DMS → Redshift apply | Datastream → BigQuery merge mode | BigQuery applies with `_CHANGE_TYPE` / sequence numbers |
| **Debezium** (Kafka Connect source; snapshot + streaming; `op`, `source.lsn`, `ts_ms`) | Datastream | Same principles: snapshot + log from position; envelope with before/after, source position |
| Debezium **tombstones** (null value after delete for log compaction) | Not needed; delete is a change record | |
| Kafka topic per table, keyed by PK | GCS folder per table / BigQuery table per source table | Debezium uses the **PK as the Kafka key** so all changes of a row go to one partition → per-row order. Datastream carries positions instead |
| Debezium heartbeat (`heartbeat.interval.ms`) | Heartbeat table pattern for Postgres slots | |
| Spark/Delta `MERGE INTO` from CDC files | BigQuery CDC apply, or your own MERGE in Dataform | Same "latest by PK + position" logic |
| Microsoft Fabric mirroring | Datastream → BigQuery | Both: managed replication into the analytics store |
| Kinesis/Kafka windowing for event streams | **Not CDC**: Pub/Sub + Dataflow | Keep them separate! |

In short: if you know log-based CDC from DMS or Debezium (snapshot plus log from a known position, per-key ordering by log position, idempotent upserts), Datastream is the serverless version of the same idea, with direct apply into BigQuery CDC tables.

---

## 12. Worked design: near real-time sales from Postgres POS into BigQuery

### 12.1 Requirements (assume and state them)
- Source: Postgres POS database, schema `pos`: `orders`, `order_lines`, `payments`, `refunds`, `stores`, `products`. ~5M order lines/day, peaks at lunch.
- Freshness SLA: **sales in Looker ≤ 15 minutes** behind the till.
- History: needed for status changes and product/store attributes (SCD2).
- PII: customer email/phone in `orders` must not land unmasked.

### 12.2 Architecture

```text
 Postgres (primary, wal_level=logical)
   publication datastream_pub (pos.* except customers_pii table)
   slot datastream_slot (pgoutput), max_slot_wal_keep_size set, heartbeat table
        │  private connectivity (VPC peering / PSC) ; user datastream_user (REPLICATION + SELECT)
        ▼
 Datastream stream "pos-to-bq"  (ingestion project, Terraform module)
   - include: pos.orders, pos.order_lines, pos.payments, pos.refunds, pos.stores, pos.products
   - exclude columns: orders.customer_email, orders.customer_phone  (or mask downstream with policy tags)
   - backfill: all (off-peak), order_lines history: bulk load (see 8.5)
   - destination: BigQuery dataset cdc_pos, MERGE mode, staleness 5 min (facts) / 60 min (dims)
   - second stream or GCS archive: append-only for orders, stores, products (history) → dataset cdc_pos_log
        ▼
 BigQuery (EDW project)
   cdc_pos.*        current state          cdc_pos_log.*    every change (append-only)
        │                                         │
        └──────────── Dataform (scheduled every 5 min by Composer or Dataform workflows) ──┐
                                                                                         ▼
   stg_*  (explicit columns, types, is_deleted)                                          │
   fct_sales_line  (incremental MERGE, partitioned by order_date, clustered store_id, product_id)
   dim_store_scd2, dim_product_scd2 (from append-only log)                               │
   mon_freshness   (freshness per table)                                                 │
        ▼                                                                                │
 Looker: explores on fct_sales_line + dims; datagroup triggered by MAX(source_ts) ◄──────┘
```

**Freshness budget:** Datastream (seconds to ~1 min) + `max_staleness` 5 min + model every 5 min + Looker datagroup check ≈ **≤ 12 minutes worst case**, inside the 15-minute SLA. Always write this budget down explicitly in the design.

### 12.3 Current-state fact model (incremental)

```sql
-- Dataform/dbt-style incremental model: fct_sales_line
-- Reprocess only recently changed lines (by source timestamp), with a safety overlap.
MERGE mart.fct_sales_line T
USING (
  SELECT
    ol.order_line_id,
    o.order_id,
    DATE(o.order_ts)                         AS order_date,
    o.store_id,
    ol.product_id,
    ol.quantity,
    ol.unit_price * ol.quantity              AS gross_amount,
    o.status                                 AS order_status,
    GREATEST(TIMESTAMP_MILLIS(ol.datastream_metadata.source_timestamp),
             TIMESTAMP_MILLIS(o.datastream_metadata.source_timestamp)) AS source_ts
  FROM cdc_pos.order_lines ol
  JOIN cdc_pos.orders o USING (order_id)
  WHERE TIMESTAMP_MILLIS(ol.datastream_metadata.source_timestamp)
          > (SELECT TIMESTAMP_SUB(MAX(source_ts), INTERVAL 30 MINUTE) FROM mart.fct_sales_line)
     OR TIMESTAMP_MILLIS(o.datastream_metadata.source_timestamp)
          > (SELECT TIMESTAMP_SUB(MAX(source_ts), INTERVAL 30 MINUTE) FROM mart.fct_sales_line)
) S
ON T.order_line_id = S.order_line_id
WHEN MATCHED THEN UPDATE SET
  order_status = S.order_status, quantity = S.quantity, gross_amount = S.gross_amount, source_ts = S.source_ts
WHEN NOT MATCHED THEN INSERT ROW;
-- Deleted lines: merge mode removed them from cdc_pos.order_lines; handle with a
-- "WHEN NOT MATCHED BY SOURCE" over a bounded partition range, or use the append-only log's DELETE rows.
```

Notes:

- Field types of `datastream_metadata.source_timestamp` (INT64 millis vs. TIMESTAMP) depend on the Datastream version; check your table schema.
- The **30-minute overlap** handles changes that were applied to the CDC table late (staleness, retries). This is the "lookback" of an incremental model, **not** a stream watermark.
- In the target, **partition by `order_date`** and **cluster by `store_id, product_id`**; use the partition filter in the MERGE where possible to limit cost.

### 12.4 History: SCD2 from the append-only log

```sql
-- dim_product_scd2 from cdc_pos_log.products (append-only: one row per change)
WITH changes AS (
  SELECT
    product_id, product_name, category, list_price,
    TIMESTAMP_MILLIS(datastream_metadata.source_timestamp) AS valid_from,
    datastream_metadata.is_deleted                          AS is_deleted,
    datastream_metadata.uuid                                AS change_uuid,
    -- per-row order: use the change sequence / sort keys provided by Datastream if available,
    -- source_timestamp alone can tie within one transaction
    datastream_metadata.change_sequence_number              AS seq
  FROM cdc_pos_log.products
  QUALIFY ROW_NUMBER() OVER (PARTITION BY change_uuid ORDER BY seq) = 1     -- dedupe redelivered events
)
SELECT
  product_id, product_name, category, list_price, is_deleted,
  valid_from,
  LEAD(valid_from) OVER (PARTITION BY product_id ORDER BY valid_from, seq) AS valid_to,
  LEAD(valid_from) OVER (PARTITION BY product_id ORDER BY valid_from, seq) IS NULL AS is_current
FROM changes;
```

- Ordering uses the **source order** (timestamp + sequence/position), **never arrival time**.
- Optionally collapse consecutive rows where tracked attributes didn't change (updates that only touched an untracked column).
- Make it **incremental** in production (only products changed since the last run, then rebuild their versions).

### 12.5 Freshness measurement

```sql
-- mon_freshness: run every 5 minutes, alert if > SLA
SELECT 'cdc_pos.orders' AS table_name,
       TIMESTAMP_DIFF(CURRENT_TIMESTAMP(),
                      TIMESTAMP_MILLIS(MAX(datastream_metadata.source_timestamp)), SECOND) AS lag_s
FROM cdc_pos.orders
UNION ALL
SELECT 'mart.fct_sales_line', TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(source_ts), SECOND)
FROM mart.fct_sales_line;
```

Caveat: at night, when no sales happen, `MAX(source_ts)` gets old even though the pipeline is healthy. Use the **heartbeat table** (a row updated every minute in the source and replicated) as the true freshness probe. That's a strong senior detail.

### 12.6 Validation
- Row counts and sums per day: source (`SELECT COUNT(*), SUM(amount) FROM pos.orders WHERE order_ts::date = ...`) vs. BigQuery, for closed days.
- Spot checks for deleted keys.
- Alert on unsupported events and DLQ files.

---

## 13. Recovery runbook: the stream was paused for 2 days

> *"The source is Postgres. The Datastream stream was paused for 2 days over a weekend because of an IAM issue. What happens on the source, and how do you recover?"*

### 13.1 What happened on the source
- The replication slot `datastream_slot` stayed at the last confirmed LSN from Friday.
- Postgres **kept all WAL** generated since then (all tables, not only published ones, because WAL is shared).
- Retained WAL = 2 days of writes. On a busy POS DB that can be **tens to hundreds of GB**.
- Three possible outcomes:
    1. **Disk had room** → slot is fine, `wal_status` = `reserved`/`extended`. Recoverable without data loss.
    2. **`max_slot_wal_keep_size` exceeded** → slot **invalidated** (`wal_status = 'lost'`). DB is safe, but CDC position is gone.
    3. **No cap and disk filled** → **production outage** (DB can't write WAL). Worst case; the DBA may drop the slot to recover the DB.

### 13.2 Steps

```text
 1. ASSESS (5 min)
    - Datastream: stream state (PAUSED / FAILED / FAILED_PERMANENTLY), error message.
    - Postgres:  SELECT slot_name, active, wal_status, retained WAL (query in §5.6); disk free %.
    - Decide which outcome (1, 2 or 3) you are in.

 2. PROTECT THE SOURCE FIRST
    - If disk is near full: add disk / free space with the DBA. Only as a last resort drop the slot
      (accept a re-backfill). Production beats CDC continuity.

 3. FIX THE ROOT CAUSE
    - Restore the IAM permission (e.g. Datastream service agent lost BigQuery dataEditor on the dataset,
      or Secret Manager access). Do it in Terraform, not by hand, so it doesn't drift again.

 4a. OUTCOME 1 (slot healthy): RESUME
    - Resume the stream. Datastream continues from the slot's confirmed LSN. No data loss.
    - Expect a catch-up spike: throughput up, freshness high, then falling.
    - Watch retained WAL shrink, source CPU (decoding 2 days of WAL), BigQuery background apply.
    - Changes arrive late but are applied in position order per key → correct final state.
      (No "late data" problem: CDC has no allowed lateness.)

 4b. OUTCOME 2/3 (slot lost/dropped): RE-ESTABLISH + RE-BACKFILL
    - Recreate the slot (and publication if needed) — new start position.
    - Restart CDC from the new position and trigger BACKFILL for all affected tables
      (or the whole stream). Backfill + CDC overlap → latest wins → consistent state.
    - Merge mode target: backfill re-upserts every row; deletes that happened during the gap:
      rows deleted in source are NOT in the backfill, so they may remain in BigQuery as "ghost rows".
      Fix: recreate the target tables before backfill, or run a key comparison and delete keys not in
      the source.
    - Append-only target: the gap's intermediate changes are lost forever; history has a hole.
      Document it; mark the period in the SCD2 tables.

 5. VALIDATE
    - Freshness back under SLA; counts/sums per day vs. source; spot-check keys changed during the gap.
    - Rerun downstream incremental models with a lookback covering the 2 days
      (their normal 30-min overlap is too small for a 2-day gap!).
    - Invalidate Looker caches (datagroup) once caught up.

 6. PREVENT
    - Alert on stream state != RUNNING for > 15 min and on retained WAL > threshold (page someone at weekends).
    - Set max_slot_wal_keep_size to protect production; size disk for your accepted outage.
    - IAM via Terraform with policy tests; avoid manual grants.
    - Heartbeat table; postmortem; update the runbook.
```

### 13.3 The same incident on MySQL/Oracle
- **MySQL:** no slot, the binlog is purged by retention. If retention (e.g. 7 days) > 2 days → resume, no loss. If not → the position is gone → **re-backfill**.
- **Oracle:** depends on archive log retention (RMAN). Same logic.
- So the risk flips: **Postgres risks the source disk; MySQL/Oracle risk the CDC position.**

---

## 14. Hands-on exercise

**Time:** 2–3 hours. Personal GCP project. Delete everything afterwards (Cloud SQL and Datastream cost money).

1. **Source:** create a small **Cloud SQL for PostgreSQL** instance. Set the flag `cloudsql.logical_decoding = on`. Create schema `pos` with `orders(order_id PK, store_id, status, amount, updated_at)` and insert 1,000 rows.
2. **Prepare CDC:** create the user, grants, publication and slot (SQL in 4.3; on Cloud SQL give the user the `REPLICATION` attribute with `ALTER USER ... WITH REPLICATION`, check Cloud SQL docs).
3. **Datastream:** create a source connection profile (IP allowlist is fine for a lab; use private connectivity in real life), a BigQuery destination profile, and a stream in **merge** mode with staleness 0–5 min and backfill on. Start it.
4. **Observe backfill:** see 1,000 rows appear. Look at the table DDL in BigQuery: primary key, `max_staleness`, `datastream_metadata`.
5. **Observe CDC:** run `UPDATE pos.orders SET status='PAID' WHERE order_id < 100; DELETE FROM pos.orders WHERE order_id = 5;` Check BigQuery after the staleness interval. Run a query immediately and note bytes processed (merge-on-read vs. after apply).
6. **Schema drift:** `ALTER TABLE pos.orders ADD COLUMN channel text;` then update some rows. Check the new column in BigQuery. Then try a type change on a column and watch the stream's errors/unsupported events metric.
7. **Pause risk:** pause the stream, generate writes (`INSERT ... SELECT generate_series(...)`), and watch retained WAL grow with the slot query. Resume; watch it drain.
8. **Append-only:** create a second stream (or switch a lab stream) with append-only mode on another dataset. Update one row three times. Look at the rows, `change_type`, `is_deleted`, sort keys. Write the "current state" query with `QUALIFY ROW_NUMBER()` yourself and compare to the merge table.
9. **Write down** in 5 sentences what you saw in steps 5 and 8 and why the merge table and the append-only table differ.

---

## 16. Glossary

| Term | Meaning |
|---|---|
| **Append-only mode** | Datastream BigQuery write mode where every change is a new row with metadata |
| **ARCHIVELOG mode** | Oracle mode that keeps filled redo logs as archive logs (needed for CDC) |
| **Backfill** | The initial snapshot copy of a table, done by Datastream before/while streaming changes |
| **Background apply** | BigQuery jobs that merge buffered CDC changes into the base table |
| **Binlog** | MySQL's binary log of changes; needs `ROW` format for CDC |
| **CDC** | Change Data Capture: continuous capture of inserts, updates and deletes |
| **`_CHANGE_SEQUENCE_NUMBER`** | BigQuery CDC pseudo-column used to order changes per key |
| **`_CHANGE_TYPE`** | BigQuery CDC pseudo-column: `UPSERT` or `DELETE` |
| **Connection profile** | Datastream object describing how to connect to a source or destination |
| **`datastream_metadata`** | Metadata struct on Datastream tables (uuid, source_timestamp, ...) |
| **FAILED_PERMANENTLY** | Stream state that requires intervention (e.g. lost log position) |
| **GTID** | MySQL global transaction ID; position that survives failover |
| **Heartbeat table** | Small source table updated regularly to keep CDC moving and measure freshness |
| **Log-based CDC** | Reading changes from the database transaction log |
| **LogMiner** | Oracle API to read redo/archive logs as row changes |
| **LSN** | Log Sequence Number: position in the Postgres WAL or SQL Server log |
| **`max_slot_wal_keep_size`** | Postgres setting that caps WAL kept for slots; slot is invalidated if exceeded |
| **`max_staleness`** | BigQuery table option: maximum age of data returned for a CDC table |
| **Merge mode** | Datastream BigQuery write mode keeping current state per primary key |
| **Merge-on-read** | BigQuery applying pending changes at query time when background apply is behind |
| **pgoutput** | Built-in Postgres logical decoding output plugin |
| **Private connectivity** | Datastream's private network path to your VPC (peering or PSC) |
| **Publication** | Postgres object listing the tables whose changes are published |
| **Query-based CDC** | Polling with a timestamp/ID column; misses hard deletes |
| **Redo log** | Oracle's transaction log |
| **Replica identity** | Postgres setting for what old-row data goes into the WAL on update/delete |
| **Replication slot** | Postgres bookmark of the consumer's position; retains WAL until confirmed |
| **SCD2** | Slowly Changing Dimension type 2: one row per version with valid_from/valid_to |
| **SCN** | System Change Number: Oracle's log position |
| **Schema drift** | Source schema changes that must be propagated or handled |
| **Source timestamp** | Commit time of the change in the source database |
| **Stream** | Datastream object that replicates selected objects from source to destination |
| **Supplemental logging** | Oracle setting to log extra column data needed for CDC |
| **Tombstone** | Marker that a key was deleted, kept so older changes can't resurrect it |
| **Trigger-based CDC** | Capturing changes into shadow tables with database triggers |
| **Unsupported event** | A change Datastream could not process (e.g. incompatible type) |
| **WAL** | Write-Ahead Log: Postgres's transaction log |
| **`wal_level = logical`** | Postgres setting that enables logical decoding |
