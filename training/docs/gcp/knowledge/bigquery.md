# BigQuery from the inside out

> **Goal of this lesson:** understand BigQuery well enough to explain *how it works inside*, *why* the common optimizations work, and *what you would do* on a real data warehouse platform.
>
> **Quick reference (cheat sheet):** [BigQuery deep dive](../deep-dives/bigquery.md). This lesson explains the "why" behind it. It does not repeat every table there.
>
> **About numbers:** prices and some limits change. Where a number is marked *(approx.)*, check the current Google Cloud docs before you rely on it.

---

## 1. What BigQuery is and why it exists

### 1.1 The one-sentence summary

BigQuery is Google's **serverless, columnar, massively parallel SQL data warehouse**. Storage and compute are **separate services**, and you never manage servers, clusters, files, indexes or vacuum jobs.

In short:

> "BigQuery is a serverless data warehouse. Data is stored in a columnar format on Google's distributed file system, Colossus. Queries are run by a shared execution engine, Dremel, which splits each query into stages and runs them on thousands of slots. Because storage and compute are separate, I pay for them separately, and most of my optimization work is about reading less data and using fewer slot-seconds."

### 1.2 An analogy: the library and the reading room

Think of a huge library.

- **Storage (Colossus)** is the warehouse of books. It is enormous, cheap, and always there.
- **Compute (Dremel slots)** are the librarians. Google has a giant pool of them shared by all customers.
- **Your query** is a research request. A head librarian (the planner) splits it into tasks and gives each task to many librarians at once.
- **Columnar storage** means each book is cut into chapters, and each chapter is stored on a separate shelf. If you only need chapter 3 (one column), librarians only carry chapter 3. That is why `SELECT *` is expensive: you ask them to carry the whole book.
- **Partitions** are rooms sorted by date. **Clustering** is the order of books on the shelves inside each room. Both help the librarians skip rooms and shelves.

You never hire librarians or build shelves. You only decide *what you ask for* and *how you pay* for the librarians' time.

### 1.3 The problem it solves

| Traditional warehouse (Teradata, Oracle, on-prem SQL Server) | BigQuery |
|---|---|
| Storage and compute live on the same nodes. To get more disk you buy more CPU, and the other way round. | Storage and compute scale independently. |
| You size the cluster for the peak. It sits idle at night. | Compute is borrowed from a shared pool per query (on-demand) or per reservation (Editions, with autoscaling). |
| DBAs tune indexes, statistics, vacuum, distribution keys. | No indexes to tune (except special search/vector indexes). Data layout is managed. You choose partitioning and clustering. |
| Scaling = a migration project. | Scaling = nothing. A query on 10 TB just gets more slots. |

### 1.4 Compared with Spark and a lakehouse

If you know self-managed Spark or a Microsoft Fabric lakehouse, the ideas are very similar, but the **responsibility line** moves:

| Topic | Spark / Fabric lakehouse | BigQuery |
|---|---|---|
| Who owns the files? | You (Parquet/Delta files in S3/OneLake). | Google (Capacitor files in Colossus). You never see them. |
| Cluster sizing | You choose executors, memory, autoscale (or Fabric capacity units). | No cluster. Slots are assigned per query. |
| Small-files problem | You run `OPTIMIZE`, compaction, Z-order, `VACUUM`. | Background compaction and automatic reclustering, free. |
| Table format | Delta/Iceberg: open, readable by many engines. | Native storage is proprietary, but readable via the Storage Read API, and BigLake/Iceberg tables exist for open formats. |
| Cost unit | VM hours or capacity units. | Bytes scanned (on-demand) or slot-hours (Editions). |
| Shuffle | Executors write shuffle files to local disk. | A separate, in-memory, distributed shuffle tier. |

The big mental shift: **in Spark you tune the engine; in BigQuery you tune the data layout and the SQL, and you choose the pricing model.**

---

## 2. Where BigQuery fits in a modern GCP data platform

A typical enterprise data warehouse (EDW) modernization on GCP aims for "a leaner, faster, more stable data warehouse, plus a near real-time sales data model that feeds a conversational BI solution in Looker." Here is how each goal maps to BigQuery skills.

| Platform goal | BigQuery skills it needs | Section |
|---|---|---|
| Move the load layer into shared GCP **ingestion projects**, IAM, Terraform | Project/dataset structure, dataset locations, dataset-level IAM, service accounts, `jobUser` vs. `dataViewer`, reservations assigned to projects | 4.1, 4.15 |
| **Simplify load chains**, restructure Composer DAGs | Idempotent loads (partition overwrite, MERGE), batch load jobs, scripting/transactions, DML concurrency limits | 4.9, 4.10, 5.3 |
| **Optimize BigQuery queries for runtime and cost** | Pricing models, partitioning, clustering, query plan reading, joins, caching, materialized views, INFORMATION_SCHEMA | 4.2–4.8, 5.1 |
| **Near real-time sales data model** on BigQuery | Storage Write API, CDC tables (`_CHANGE_TYPE`, `max_staleness`), Datastream, incremental MERGE, nested data | 4.10–4.12, 5.4 |
| **Exposed in Looker** (conversational BI) | BI Engine, materialized views, aggregate tables, BI reservation, clean semantic models | 4.8, 5.5 |
| **Metrics: load duration, throughput, stability, cost** | `INFORMATION_SCHEMA.JOBS`, `JOBS_TIMELINE`, `TABLE_STORAGE`, job labels, billing export | 4.17, 5.7 |
| **Testing, documentation, handover** | Dry runs, table clones for tests, `maximum_bytes_billed`, standard patterns | 4.16, 5.8 |
| **Strong DWH modelling** | Star schema vs. nested/denormalized, partition/cluster design of facts, SCD handling with MERGE | 4.13, 5.2 |

---

## 3. Architecture, low level

### 3.1 The big picture

```text
                 ┌────────────────────────────────────────────┐
  bq / console / │  BigQuery API front end                    │
  Looker / Python│  auth (IAM), quotas, job management        │
  ──────────────▶│                                            │
                 └──────────────────┬─────────────────────────┘
                                    │ query job
                                    ▼
                 ┌────────────────────────────────────────────┐
                 │  Query planning                            │
                 │  parse → analyze → optimize → stage DAG    │
                 │  metadata lookup: partitions, block stats  │
                 └──────────────────┬─────────────────────────┘
                                    │ stages
                                    ▼
   ┌──────────────────────────────────────────────────────────────┐
   │  DREMEL execution engine (compute)                           │
   │                                                              │
   │   stage 1 workers (slots) ──▶ ┌───────────────┐ ──▶ stage 2  │
   │   stage 1 workers (slots) ──▶ │ SHUFFLE tier  │ ──▶ stage 2  │
   │   stage 1 workers (slots) ──▶ │ (in memory,   │ ──▶ stage 3  │
   │                               │ disaggregated)│              │
   │   Borg schedules the workers  └───────────────┘              │
   └───────────────┬──────────────────────────────────────────────┘
                   │  JUPITER network (petabit-scale, data center)
                   ▼
   ┌──────────────────────────────────────────────────────────────┐
   │  COLOSSUS distributed file system (storage)                  │
   │  table data in CAPACITOR columnar files, compressed,         │
   │  encrypted, replicated across zones in the location          │
   └──────────────────────────────────────────────────────────────┘
```

Each box is a separate Google system. Let us go through them.

### 3.2 Dremel: the execution engine

Dremel started at Google around 2006 to run interactive SQL on huge logs. The original design (2010 paper) was a **serving tree**:

```text
                     root server
                    /           \
              mixer             mixer          ← partial aggregation
             /     \           /     \
         leaf     leaf      leaf     leaf      ← read columns, filter, pre-aggregate
```

Leaves read data and compute partial results. Mixers combine them. The root returns the final answer. This works very well for `SELECT ... WHERE ... GROUP BY` on one big table.

Modern BigQuery generalized this. A query is now a **DAG of stages** (like Spark stages). Between stages, data goes through the **shuffle tier**. Each stage runs on many **workers**, and the unit of work capacity is the **slot**.

**What is a slot?** A slot is a *virtual unit of compute*: a share of CPU, memory and network on a Dremel server. It is not one CPU core exactly, but think of it like a Spark executor core. A stage is split into many small **work units**. The scheduler gives work units to free slots. If you have 2,000 slots, up to about 2,000 work units can run at the same time.

Important properties:

- **Fair scheduling.** Slots are shared fairly between the running queries in a project or reservation. A new query gets a fair share quickly. That is why one big query does not completely block small ones, but many big queries slow each other down.
- **Dynamic execution.** BigQuery can change the plan while the query runs. For example, it can change the number of workers for a stage, or re-partition data when one partition is too big. You don't control this, but it explains why the same query can show slightly different plans.
- **Preemptible work units.** If a worker fails, only its work unit is retried. Because shuffle data is outside the worker, a retry doesn't restart the whole stage. This is one reason BigQuery queries rarely fail on hardware problems.

### 3.3 The shuffle tier (in memory, disaggregated)

In Spark, the mappers write shuffle files on the *local disks of executors*, and reducers fetch them. If an executor dies, the shuffle data is lost and the stage reruns.

In BigQuery, shuffle is a **separate service**. Producers write their partitioned output into **remote memory** (the shuffle tier). Consumers read from it. This is called *disaggregated* shuffle.

```text
   stage 1 (read + filter + partial agg)          stage 2 (final agg / join)
   ┌────────┐                                      ┌────────┐
   │worker A│──┐  key hash 0..N   ┌───────────┐ ┌─▶│worker X│
   ├────────┤  ├────────────────▶ │  SHUFFLE  │─┤  ├────────┤
   │worker B│──┤                  │ (memory;  │ ├─▶│worker Y│
   ├────────┤  │                  │  spills   │ │  ├────────┤
   │worker C│──┘                  │  to disk) │ └─▶│worker Z│
   └────────┘                     └───────────┘    └────────┘
```

Why it matters for you:

- Shuffle is fast because it is in memory and the network (Jupiter) is very fast.
- But memory is limited. When the shuffle data is too big, it **spills to disk**. Spilling is much slower. In the query plan you will see "bytes spilled to disk". That is a sign to filter or aggregate earlier.
- **Big joins and big `GROUP BY`s create big shuffles.** Reducing shuffle is the main way to reduce slot time.

### 3.4 Colossus: the storage

Colossus is Google's distributed file system (the successor of GFS). BigQuery stores all table data there.

- Data is **replicated** (or erasure-coded) and spread across many disks, so reads are highly parallel.
- Data is **encrypted at rest** by default (Google-managed keys, or your own keys with CMEK).
- Data is stored redundantly across zones within the dataset's **location** (a region like `europe-north1`, or a multi-region like `EU`).

Because Colossus is shared and huge, a query can read from thousands of disks in parallel. You never think about disk throughput.

### 3.5 Capacitor: the columnar file format

Capacitor is BigQuery's internal columnar format (successor of the format in the Dremel paper). Think of it as "Parquet, but tuned and managed by Google".

What it does:

1. **Stores each column separately.** A query that reads 3 of 80 columns reads only those 3 columns. In on-demand pricing you pay only for those 3 columns.
2. **Encodes each column.** It uses **dictionary encoding** (store each distinct value once, then small integer codes), **run-length encoding** (store "value X repeated 5,000 times" instead of 5,000 copies), and other encodings, then compresses the result.
3. **Reorders rows to compress better.** Capacitor can choose a row order inside a file that makes long runs of equal values, so run-length encoding works better.
4. **Supports nested data natively.** STRUCT and ARRAY columns are stored column by column too, with extra information (repetition and definition levels, from the Dremel paper) to rebuild the nesting. So nested data is not stored as a JSON blob.
5. **Keeps statistics.** Each file/block stores metadata such as min/max per column. This enables pruning (see 3.8).
6. **Can filter on encoded data.** For example, with a dictionary, the engine can check the filter once per distinct value instead of once per row.

Important for billing: **on-demand bytes are counted on the logical (uncompressed) size of the columns you read**, not on the compressed size. Compression makes BigQuery fast, but it does not make on-demand queries cheaper. (Physical *storage* billing does use compressed size. See 4.3.)

### 3.6 Jupiter: the network

Jupiter is Google's data center network. It has petabit-scale bisection bandwidth. In simple words: any machine can talk to any other machine at very high speed.

Why it matters: **separating storage from compute only works if the network is fast enough** to read remote data as if it were local. Jupiter is what makes "compute here, storage there" practical. It also makes the remote shuffle tier fast.

### 3.7 Borg: resource management

Borg is Google's cluster manager (the ancestor of Kubernetes). It runs the Dremel servers, shuffle servers and other services on Google's machines. It decides *which machines* run *which work*, handles machine failures, and lets BigQuery share huge hardware pools across all customers.

You never touch Borg. But it explains two things:

- Why BigQuery can give a query thousands of slots within seconds (the hardware is already running).
- Why on-demand slots are "best effort": you share a pool with others.

### 3.8 Separation of storage and compute: what it means for you

| Consequence | Example |
|---|---|
| You pay storage and compute separately. | A 50 TB table that nobody queries costs only storage. |
| Many engines can read the same data. | Dataflow, Spark (Dataproc), and BI tools read via the Storage Read API without a copy. |
| Compute scales per query, not per cluster. | A nightly 5 TB rebuild and a 10 MB dashboard query run side by side. |
| Isolation is done with **reservations**, not clusters. | Give ETL and BI separate slot reservations so dashboards are not starved. |
| Copies are cheap. | Table clones and snapshots share storage; you pay only for the changed data. |

### 3.9 Lifecycle of a query, step by step

Let us follow one query:

```sql
SELECT s.region, SUM(l.net_amount) AS net_sales
FROM sales.fct_order_line AS l
JOIN sales.dim_store AS s USING (store_id)
WHERE l.order_date BETWEEN '2026-10-01' AND '2026-10-22'
  AND l.product_id = 'P-1001'
GROUP BY s.region;
```

```text
 1 SUBMIT     client → API: create query job (job_id, location, labels)
 2 AUTH/QUOTA check IAM: bigquery.jobs.create on the project,
              read permission on tables; check quotas, maximum_bytes_billed
 3 CACHE?     same SQL text, same tables unchanged, deterministic? → return cached
 4 PARSE      GoogleSQL text → syntax tree
 5 ANALYZE    resolve tables/columns/types against the catalog
 6 PRUNE      metadata: only partitions 2026-10-01..22;
              within them, only blocks where min(product_id) <= 'P-1001' <= max
 7 OPTIMIZE   push filters down, choose join type (broadcast: dim_store is small),
              decide stages
 8 EXECUTE    stage S00: read order_line columns (order_date, product_id,
              store_id, net_amount) → filter → partial SUM by store_id
              stage S01: read dim_store (store_id, region) → broadcast
              stage S02: join + partial SUM by region → shuffle
              stage S03: final SUM by region → output
 9 RESULT     write result to a temporary (anonymous) table
10 RETURN     client pages through the result (or reads it with the Read API)
11 CACHE      the result stays cached about 24h for identical queries
```

Some details worth knowing:

- **Step 2: two projects can be involved.** The job runs (and is billed) in the project where you *create the job*. The data can live in another project. You need `bigquery.jobs.create` (role `roles/bigquery.jobUser`) in the job project, and data read permission (`roles/bigquery.dataViewer`) on the dataset. This is the basis of the "shared ingestion projects" and "consumer projects" pattern.
- **Step 3: cache.** The cache is free. It does not apply if the query uses non-deterministic functions such as `CURRENT_TIMESTAMP()` or `RAND()`, if a referenced table changed, if you write to a destination table, or if a table has a streaming buffer. More in 4.8.
- **Step 6: pruning happens before execution.** For partitions, the dry-run estimate is exact. For clustering, the dry run shows an *upper bound*, because the exact block skipping is only known at run time.
- **Step 8: a leaf stage only reads the referenced columns.** `net_amount`, `store_id`, `product_id`, `order_date`. Not the other 40 columns.
- **Step 9: results.** Every query result is written to a table. If you don't name one, BigQuery creates an anonymous temporary table. Writing a big result is also work (slot time).

### 3.10 Storage internals: how data gets in and gets organized

There are two physical "shapes" of data inside a BigQuery table.

```text
   streaming writes                         batch load / query results / DML
   (Storage Write API,                      (load job, CTAS, INSERT...SELECT)
    legacy insertAll)                                │
        │                                            │
        ▼                                            │
 ┌───────────────────────┐                           │
 │ WRITE-OPTIMIZED store │  rows readable            │
 │ (append-friendly,     │  within seconds           │
 │  replicated)          │                           │
 └──────────┬────────────┘                           │
            │ background conversion                  │
            ▼                                        ▼
 ┌───────────────────────────────────────────────────────────┐
 │ READ-OPTIMIZED columnar storage (Capacitor files)         │
 │ grouped by partition, sorted by cluster columns,          │
 │ with per-block min/max metadata                           │
 └───────────────────────────────────────────────────────────┘
            ▲
            │ background compaction + automatic reclustering (free)
```

1. **Streaming data** lands in a **write-optimized** area first. It is queryable within seconds. In the legacy streaming API this area is the famous "streaming buffer". Background workers later convert it into columnar Capacitor files. During that time, some operations on these recent rows have restrictions (for example, legacy streamed rows can't be copied or exported until they leave the buffer; check the docs for DML on very recent rows).
2. **Batch loads, query results and DML** write columnar files directly.
3. **Background compaction** merges small files into bigger, better-compressed ones. This is why you don't have a "small files problem" like in Spark/Delta.
4. **Storage is immutable at file level.** A DML statement does not edit a file in place. It writes new files and marks old ones as replaced. The old versions are kept for **time travel**. This is a *copy-on-write* style design (see 4.9).

**Metadata.** BigQuery keeps a large metadata system next to the data (Google calls it "Big Metadata" in a research paper). For each storage block it keeps column statistics such as min and max values. The planner reads this metadata to skip blocks *without reading the data*. This is exactly how partition and cluster pruning work.

**How partitions are physically represented.** A partitioned table is one logical table, but each partition (for example `20261022`) is its own group of storage files with its own metadata. So:

- Pruning a partition = not opening its files at all.
- Overwriting a partition = replacing that group of files atomically.
- Expiring a partition = dropping that group.
- Storage pricing is tracked per partition (a partition untouched for 90 days becomes long-term storage, even if other partitions change).
- Special partitions: `__NULL__` (NULL partition key), `__UNPARTITIONED__` (values out of range, or for ingestion-time tables, rows still in the streaming buffer).

**How clustering is physically represented.** Inside each partition, rows are **sorted by the cluster columns** (up to 4) and cut into blocks. Each block has min/max metadata for the cluster columns. A filter `product_id = 'P-1001'` only reads blocks whose range can contain that value.

```text
 partition 2026-10-22 (clustered by store_id, product_id)
 ┌──────────────┬──────────────┬──────────────┬──────────────┐
 │ block 1      │ block 2      │ block 3      │ block 4      │
 │ store S001   │ store S001   │ store S002   │ store S003   │
 │ prod A..F    │ prod G..Z    │ prod A..Z    │ prod A..M    │
 └──────────────┴──────────────┴──────────────┴──────────────┘
   WHERE store_id = 'S002'  → read only block 3
```

New data appended to a clustered table is at first *not* perfectly sorted (new blocks overlap old ranges). **Automatic reclustering** rewrites blocks in the background so they become sorted again. It is free and uses no slots from your reservation. That is the difference from Delta, where you schedule `OPTIMIZE ... ZORDER BY` yourself.

---

## 4. Core concepts, one by one

For each concept: **what it is → how it works → why it matters → example → common mistake.**

### 4.1 Projects, datasets, tables, views, locations

**What.** The resource hierarchy is: Organization → Folders → **Projects** → **Datasets** → **Tables / Views / Routines / Models**. A table name is `project.dataset.table`.

**How.**

- A **project** is the billing and quota boundary. Jobs run *in* a project.
- A **dataset** is a container for tables, like a schema. It has a fixed **location** and is the most common IAM boundary.
- A **table** is native storage, an external table, or a materialized view. A **view** is saved SQL (no data). A **routine** is a UDF or stored procedure.
- **Location is set when the dataset is created and cannot be changed.** A query can only read datasets in one location. You cannot join an `EU` table with a `US` table in one query.

**Why it matters.** Many platforms use shared GCP ingestion projects. A typical layout:

```text
 org
 ├── folder: data-platform
 │   ├── prj-ingest-prod     datasets: raw_pos, raw_erp      (landing, owned by ingestion SA)
 │   ├── prj-edw-prod        datasets: stg, core, mart_sales (warehouse)
 │   └── prj-bi-prod         runs Looker queries (own reservation / billing)
 └── folder: sandbox ...
```

**Example.**

```sql
CREATE SCHEMA `prj-edw-prod.mart_sales`
OPTIONS (location = 'EU', default_partition_expiration_days = NULL,
         description = 'Sales mart for Looker');
```

**Common mistake.** Creating datasets in different locations (`EU` and `europe-north1`) and then discovering you can't join them. Decide the location once, as a platform standard, in Terraform.

### 4.2 Slots and compute pricing

**What.** Compute is measured in **slots**. You pay for compute in one of two ways:

| | On-demand | Capacity (Editions) |
|---|---|---|
| You pay for | **Bytes processed** per query (about $6.25 per TiB in US, *approx.*; first 1 TiB per month free) | **Slot-hours** of a reservation |
| Slots you get | Shared pool, best effort (roughly up to ~2,000 concurrent slots per project, *approx.*) | Your reservation: **baseline** + **autoscaling** up to a max |
| Optimize by | Reading fewer bytes | Using fewer slot-seconds, smoothing peaks |
| Good for | Small, spiky, unpredictable use | Large, steady workloads; predictable bills |

**Editions** are Standard, Enterprise and Enterprise Plus. Higher editions cost more per slot-hour and add features (for example more security and governance features, and multi-region disaster recovery in Enterprise Plus). Check the current feature table.

**How reservations work.**

- A **reservation** is a named pool of slots in an **admin project**.
- An **assignment** connects a project, folder or organization to a reservation, for a job type (for example `QUERY`, or `PIPELINE` for load jobs).
- **Baseline** slots are always allocated and always billed. **Autoscaling** adds slots when needed, up to the max you set, and you pay for them while they are allocated (with a short minimum period, *approx.* 1 minute, in steps of 50 slots, *approx.*).
- **Idle slots** from one reservation can be borrowed by other reservations in the same admin project (configurable).
- **Commitments** (1 or 3 years) give a discount on baseline slots.

**Why it matters.** The cost optimization goal is *different* in each model:

- On-demand: a query that scans 10 TB costs 10 TB, even if it takes 5 seconds.
- Editions: the same scan might be cheap if it uses few slot-seconds. But a badly written join that runs 1 hour with 2,000 slots is expensive, even if it reads only 100 GB.

**Example: check what a query cost in both views.**

```sql
SELECT job_id,
       total_bytes_billed / POW(1024, 4)       AS tib_billed,       -- on-demand view
       total_slot_ms / 1000 / 3600             AS slot_hours,        -- Editions view
       TIMESTAMP_DIFF(end_time, start_time, SECOND) AS runtime_s
FROM `region-eu`.INFORMATION_SCHEMA.JOBS
WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
ORDER BY slot_hours DESC
LIMIT 10;
```

**Common mistake.** Optimizing for the wrong metric. Under Editions, people sometimes add partition filters (good), but leave a huge shuffle-heavy join (bad), and the bill doesn't move. Always ask first: "Are we on on-demand or Editions?" Another mistake: setting a high baseline "to be safe". Baseline is billed 24/7 even when idle.

### 4.3 Storage pricing

**What.** You pay for storage per GiB per month, separately from compute.

| Concept | Meaning |
|---|---|
| **Active storage** | Table or partition modified in the last 90 days. |
| **Long-term storage** | Not modified for **90 consecutive days**. Price drops by about **50%** automatically. Same performance. Querying does *not* reset it; modifying does. |
| **Logical billing** | Billed on **uncompressed** size. Time travel and fail-safe bytes are *not* billed. |
| **Physical billing** | Billed on **compressed** size, at a higher unit price (*approx.* 2x), and time travel + fail-safe bytes **are** billed. Set per dataset. |
| **Time travel** | Old versions kept **2–7 days** (configurable per dataset, default 7). |
| **Fail-safe** | **7 more days** after time travel. Not queryable by you; recovery through Google support. |

**How.** Because of the immutable files (3.10), every update creates new files, and the old files are kept for time travel and fail-safe. Under physical billing you pay for those old files too.

**Why it matters.**

- Data that compresses well (repeated strings, codes, sensor IDs, timestamps) often compresses 5–10x or more. Physical billing can then be much cheaper, *unless* the table is rewritten very often.
- A daily `CREATE OR REPLACE` of a 5 TB table makes 5 TB of new files every day, plus old versions in time travel. Under physical billing, that can multiply storage cost. Incremental loads fix this.
- Partitioning helps long-term pricing: old partitions that are not touched become long-term, even if today's partition changes.

**Example.**

```sql
-- Compare logical vs. physical size before switching a dataset's billing model
SELECT table_schema,
       SUM(total_logical_bytes)  / POW(1024, 4) AS logical_tib,
       SUM(total_physical_bytes) / POW(1024, 4) AS physical_tib,
       SUM(time_travel_physical_bytes + fail_safe_physical_bytes) / POW(1024, 4) AS tt_fs_tib
FROM `region-eu`.INFORMATION_SCHEMA.TABLE_STORAGE
GROUP BY 1
ORDER BY logical_tib DESC;

ALTER SCHEMA mart_sales SET OPTIONS (storage_billing_model = 'PHYSICAL',
                                     max_time_travel_hours = 48);
```

**Common mistake.** Switching to physical billing without checking `time_travel_physical_bytes` on heavily rewritten tables. Also: thinking that reading old data resets long-term pricing. It doesn't.

### 4.4 Partitioning

**What.** A partitioned table is split into segments by one column: a **time-unit column** (`DATE`, `TIMESTAMP`, `DATETIME`; hourly/daily/monthly/yearly), **ingestion time** (`_PARTITIONTIME`/`_PARTITIONDATE`), or an **integer range**.

**How.** Each partition is a separate group of files with its own metadata (3.10). When the `WHERE` clause constrains the partition column with values the planner can evaluate *before* execution, unneeded partitions are never opened.

**Why it matters.**

- **Cost and speed:** a query for 7 days on a table with 6 years of daily partitions reads about 7/2,190 of the data.
- **Exact estimates:** dry runs show the exact bytes after partition pruning.
- **Operations:** you can overwrite, delete, or expire single partitions cheaply. This is the base of idempotent loading.
- **Storage:** untouched partitions become long-term storage.

**Limits.** Up to **10,000 partitions per table**. Daily partitions cover about 27 years. Hourly partitions reach 10,000 in about 14 months, so use hourly only with partition expiration. There are also limits on how many partitions one job can modify (*approx.* 4,000; check docs).

**Example.**

```sql
CREATE TABLE core.fct_order_line (
  order_id     STRING NOT NULL,
  line_no      INT64  NOT NULL,
  order_ts     TIMESTAMP NOT NULL,
  order_date   DATE   NOT NULL,          -- derived, stored, used for partitioning
  store_id     STRING,
  product_id   STRING,
  customer_id  STRING,
  qty          INT64,
  net_amount   NUMERIC,
  _loaded_at   TIMESTAMP
)
PARTITION BY order_date
CLUSTER BY store_id, product_id
OPTIONS (require_partition_filter = TRUE);
```

**Common mistakes.**

1. The filter is on another column than the partition column (`created_at` vs. `order_date`).
2. The partition column is wrapped in a function the planner can't invert, for example `FORMAT_DATE('%Y%m', order_date) = '202610'`.
3. The filter value comes from a subquery or a join, so it is not known at planning time. Fix: compute it first into a script variable (`DECLARE`).
4. Too fine partitions: thousands of tiny daily partitions of a few MB. Then partition by month and rely on clustering.
5. Forgetting `require_partition_filter`, so a Looker explore without a date filter scans 6 years.

See the pruning examples in the [BigQuery deep dive](../deep-dives/bigquery.md) (section 3).

### 4.5 Clustering

**What.** Clustering sorts the data inside each partition (or inside the whole table, if not partitioned) by **up to 4 columns**, and stores block-level min/max metadata.

**How.** Block pruning at run time (3.10). The **column order matters**: data is sorted by the first column, then by the second inside the first, and so on. A filter on only the 2nd column helps less, because its values are spread over many blocks. **Automatic reclustering** keeps the sort order as new data arrives, free.

**Why it matters.**

- Good for **high-cardinality** columns used in filters and joins (`store_id`, `product_id`, `customer_id`, `sensor_id`). Partitioning can't do that, because one partition per customer would break the 10,000 limit.
- Also helps aggregations and joins on the cluster key, because related rows are close together.
- Works on non-partitioned tables too (for example a big `dim_customer` clustered by `customer_id`).

**Example: effect.**

```sql
-- Dry run says: up to 120 GB (upper bound, the dry run can't predict block pruning)
-- Actual bytes billed after run: maybe 3 GB
SELECT SUM(net_amount)
FROM core.fct_order_line
WHERE order_date BETWEEN '2026-01-01' AND '2026-10-22'
  AND store_id = 'S042';
```

**Common mistakes.**

1. Expecting the dry run to show the clustering benefit. It shows an upper bound. Measure `total_bytes_billed` after the run.
2. Clustering a tiny table. Under about 1 GB (per partition) the benefit is small; there are few blocks to skip. Also, on-demand has a minimum of 10 MB billed per table referenced.
3. Wrong column order: putting a low-value column (like `channel` with 3 values) first, when most queries filter by `store_id`.
4. Clustering on a column that is never filtered or joined on.

### 4.6 Reading the query plan

**What.** Every query job has an **execution plan**: the stages, their steps, and timing. In the console it is the "Execution details" tab. Programmatically: `INFORMATION_SCHEMA.JOBS.job_stages` or the jobs API.

**How to read a stage.**

```text
 S02: Join+          (input: S00, S01)
   Wait    avg ▇       max ▇▇
   Read    avg ▇▇      max ▇▇▇
   Compute avg ▇▇▇     max ▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇  ← max ≫ avg = SKEW
   Write   avg ▇       max ▇
   Records read: 1.2B   Records written: 3.4B   ← row explosion
   Shuffle output bytes: 410 GB   Spilled to disk: 95 GB  ← memory pressure
   Steps: READ $1,$2 FROM __stage00_output
          JOIN ... ON $1 = $10  (HASH JOIN, EACH WITH EACH)
          WRITE ... TO __stage02_output BY HASH($3)
```

| Signal | Meaning | Typical fix |
|---|---|---|
| **Wait** high | Waiting for slots (contention) or for earlier stages | More slots / separate reservation / schedule differently |
| **Read** high | Reading a lot of input | Partition/cluster pruning, fewer columns |
| **Compute** max ≫ avg | **Skew**: one worker got a hot key (NULL, 'UNKNOWN', one huge store) | Filter/handle hot keys separately, pre-aggregate, salt |
| **Write** high | Big output to shuffle or to the result table | Aggregate earlier, write less |
| Records written ≫ read on a join | **Row explosion** (many-to-many join) | Check the grain, deduplicate first |
| **Bytes spilled to disk** | Shuffle didn't fit in memory | Reduce data before join/aggregate; avoid global `ORDER BY` |
| One stage with a single worker at the end | Global `ORDER BY`, global window without `PARTITION BY`, or huge `ARRAY_AGG` | Add `LIMIT`, partition the window, aggregate differently |

**Why it matters.** It turns "the query is slow" into a specific cause. For example: open the execution details and compare max vs. avg compute time per stage to detect skew.

**Example: find the heaviest stages across jobs.**

```sql
SELECT j.job_id, s.name AS stage, s.slot_ms,
       s.compute_ms_max, s.compute_ms_avg,
       SAFE_DIVIDE(s.compute_ms_max, s.compute_ms_avg) AS skew_ratio,
       s.shuffle_output_bytes_spilled
FROM `region-eu`.INFORMATION_SCHEMA.JOBS AS j, UNNEST(j.job_stages) AS s
WHERE j.creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
ORDER BY s.slot_ms DESC
LIMIT 20;
```

**Common mistake.** Only looking at total runtime. Runtime includes waiting for slots. A query can be slow because the reservation is busy, not because the SQL is bad. Check `Wait` and `JOBS_TIMELINE` for contention.

### 4.7 Joins: broadcast vs. hash (shuffle) join

**What.** BigQuery mostly uses two join strategies, like Spark:

```text
 BROADCAST JOIN (one side small)            HASH / SHUFFLE JOIN (both sides big)
 ┌─────────┐                                ┌─────────┐      ┌─────────┐
 │ dim (S) │──copy to every worker──┐       │ fact A  │      │ fact B  │
 └─────────┘                        ▼       └────┬────┘      └────┬────┘
 ┌──────┐ ┌──────┐ ┌──────┐   join locally       │ hash(key)      │ hash(key)
 │fact 1│ │fact 2│ │fact 3│   no shuffle of      ▼                ▼
 └──────┘ └──────┘ └──────┘   the big side    ┌──────── SHUFFLE ────────┐
                                              └─┬──────────┬──────────┬─┘
                                             worker 1   worker 2   worker 3
                                             (keys 0-x) (keys x-y) (keys y-z)
```

**How.** The optimizer chooses. If one input is small enough, it broadcasts it (in the plan: `EACH WITH ALL`, or similar). Otherwise both sides are repartitioned by the join key through shuffle (`EACH WITH EACH` / `HASH JOIN`). Dynamic execution can switch strategy at run time.

**Why it matters.** A star schema (big fact + small dimensions) is mostly broadcast joins: cheap. Fact-to-fact joins (orders × payments × shipments) are shuffle joins: expensive. Shuffle size = slot time.

**Example: filter and aggregate before a big join.**

```sql
-- Bad: join 2 years of lines with 2 years of returns, then filter
-- Good: reduce both sides first
WITH lines AS (
  SELECT order_id, line_no, net_amount
  FROM core.fct_order_line
  WHERE order_date >= DATE_SUB(@run_date, INTERVAL 7 DAY)
),
rets AS (
  SELECT order_id, line_no, SUM(return_amount) AS return_amount
  FROM core.fct_return_line
  WHERE return_date >= DATE_SUB(@run_date, INTERVAL 7 DAY)
  GROUP BY 1, 2
)
SELECT l.*, COALESCE(r.return_amount, 0) AS return_amount
FROM lines AS l
LEFT JOIN rets AS r USING (order_id, line_no);
```

**Common mistakes.**

1. Joining on keys with many NULLs or a default value ('UNKNOWN'): all those rows hash to one worker → skew.
2. Joining on expressions like `CAST(id AS STRING)`: the types don't match the stored type, and it may prevent other optimizations.
3. Many-to-many joins because the grain wasn't checked.
4. Using a fact-to-fact join where a nested/repeated design (4.13) would avoid it.

Tip: declaring **primary and foreign keys** (`NOT ENFORCED`) gives the optimizer information. It can, for example, remove an unneeded join. But BigQuery does not check them, so they must be true.

### 4.8 Caching, materialized views and BI Engine

There are three different "make repeated queries cheap" features. Know the difference.

| | Query result cache | Materialized view (MV) | BI Engine |
|---|---|---|---|
| What is stored | The final result of one exact query | A precomputed aggregate/join result, kept up to date | Table data in memory |
| Who maintains it | Automatic | BigQuery, incrementally | BigQuery, in a memory reservation (GB) |
| Freshness | Invalid when a table changes | Always correct; reads combine MV + new base data; `max_staleness` can relax this | Current |
| Cost | Free | Storage + refresh compute | Pay per GB of reserved memory |
| Typical use | Same dashboard tile, same day | Daily sales per store/product for Looker | Sub-second interactive dashboards |

**How each works.**

- **Cache:** the result of each query is stored in a temporary table for about 24 hours. A new query with *identical text* on *unchanged* tables reuses it. It is skipped with non-deterministic functions, destination tables, or tables with streaming changes. In practice it is per user/project.
- **Materialized view:** BigQuery stores the result, and on base table change it **refreshes incrementally** (only the new data), automatically in the background. When you query the MV and it is behind, BigQuery **combines** the stored result with the delta from the base table, so the answer is still correct. The optimizer can also **rewrite** a query on the base table to use the MV ("smart tuning"). MVs support a limited SQL subset (aggregations, some joins; check docs). Option `max_staleness` lets BigQuery return slightly stale data without reading the delta, which is cheaper.
- **BI Engine:** an in-memory, vectorized layer. You reserve memory (in GB) in a project and location. Queries from Looker, Looker Studio and others are accelerated when the data they need fits in that memory.

**Example: MV for the Looker sales tile.**

```sql
CREATE MATERIALIZED VIEW mart_sales.mv_daily_store_product
PARTITION BY order_date
CLUSTER BY store_id
OPTIONS (enable_refresh = TRUE, refresh_interval_minutes = 15,
         max_staleness = INTERVAL 30 MINUTE)
AS
SELECT order_date, store_id, product_id,
       SUM(net_amount) AS net_sales,
       SUM(qty)        AS units,
       COUNT(*)        AS line_count
FROM core.fct_order_line
GROUP BY order_date, store_id, product_id;
```

**Common mistakes.**

1. Using `CURRENT_DATE()` in every dashboard query: kills the cache. Let Looker pass a date literal, or rely on MVs.
2. Building an MV with unsupported SQL and being surprised. For complex logic use a scheduled aggregate table (Dataform/Composer `MERGE`).
3. Buying BI Engine before fixing the model. If the query scans 2 TB, BI Engine can't hold it. First aggregate, then accelerate.

### 4.9 DML and MERGE internals

**What.** BigQuery supports `INSERT`, `UPDATE`, `DELETE`, `MERGE`, and multi-statement transactions. But it is a column store optimized for **big batch changes**, not for single-row OLTP updates.

**How (copy-on-write style).**

```text
 UPDATE core.fct_order_line SET qty = 3 WHERE order_id = 'O-1' AND order_date = '2026-10-22'

 before:  partition 20261022 = [file A][file B][file C]
          find files that contain matching rows → file B
 after:   partition 20261022 = [file A][file B'][file C]     (B' = B with the change)
          file B kept for time travel (and billed under physical storage)
```

- BigQuery finds the storage files that contain affected rows (using partition and cluster pruning), **rewrites those files**, and swaps them in atomically. Other files are untouched.
- So the cost of a DML statement depends on **how many files it touches**, not on how many rows change. Changing 1 row in each of 1,000 files rewrites 1,000 files.
- Newer optimizations reduce rewrite cost for small changes (Google calls one "fine-grained DML"; check current docs), but the mental model "DML is a batch rewrite" is still correct.

**DML concurrency.** BigQuery uses snapshot isolation. Mutating statements (`UPDATE`, `DELETE`, `MERGE`) on the **same table** are limited: only a small number run at once (*approx.* 2) and the rest are **queued** (up to *approx.* 20); beyond that they fail. Concurrent mutations that conflict on the same partition can fail and need a retry. `INSERT`s have much higher concurrency.

**MERGE rules.**

- `MERGE` reads the target and the source, joins them on the `ON` condition, then rewrites affected files.
- If **more than one source row matches the same target row**, the MERGE **fails** ("UPDATE/MERGE must match at most one source row for each target row"). Always deduplicate the source.
- If the `ON` condition doesn't restrict the target partitions, MERGE scans the **whole target** every run.

**Why it matters.** Most "load chain is slow and expensive" problems in BigQuery are one of: full rebuilds, many small DML statements in a loop, or MERGEs that scan the whole target.

**Example.** See the full MERGE with a target partition filter in the [BigQuery deep dive](../deep-dives/bigquery.md) (section 7) and the worked example in 5.3 below.

**Common mistakes.**

1. Row-by-row updates from Python in a loop (each one is a job and a file rewrite). Batch them into one staging table + one MERGE.
2. Many DAG tasks writing to the same table in parallel → DML queueing and conflicts. Give each table **one writer**.
3. MERGE on a 2 TB table without a partition predicate, every 15 minutes. Cost math (on-demand, *approx.* price): 96 runs × 2 TB ≈ 192 TB/day ≈ $1,200/day. With a 3-day window predicate it is a tiny fraction.

### 4.10 Loading data: batch loads, Storage Write API, legacy streaming, Storage Read API

| Method | How it works | Cost (*approx.*) | Use for |
|---|---|---|---|
| **Batch load job** (`bq load`, `LOAD DATA`) | Reads files from GCS (Avro, Parquet, ORC, CSV, JSON), writes columnar files directly, atomic per job | **Free** on the shared slot pool (or uses your `PIPELINE` reservation) | Daily/hourly file drops |
| **Storage Write API** | gRPC streaming API. Stream types: **default** (at-least-once, simple), **committed** (with offsets → exactly-once), **pending** (buffer, then atomic commit), buffered | Per GB ingested, with a free monthly allowance; cheaper than legacy | Streaming, Dataflow, Pub/Sub subscriptions, Datastream |
| **Legacy streaming** (`tabledata.insertAll`) | REST, row-based JSON, best-effort dedup by `insertId` | Per GB, more expensive | Old code; migrate away |
| **Query results** (`INSERT ... SELECT`, CTAS) | Normal query job | Query cost | Transformations inside BigQuery |
| **Pub/Sub BigQuery subscription** | Pub/Sub writes directly to a table (uses Write API) | Pub/Sub + ingestion | Simple streaming without Dataflow |

**Storage Read API** (the other direction). A gRPC API to read table data in **parallel streams**, in Arrow or Avro format, with **column projection** and **row filters** pushed down. Spark (Dataproc), Dataflow and Python (`pandas`/`bigframes`) use it. It reads the storage directly; it is billed per data read, not as a query.

**Why it matters.**

- Load jobs are **atomic**: either the whole file set is loaded or nothing. Combined with `WRITE_TRUNCATE` to a partition, that gives idempotent batch loads.
- Committed-type Write API streams with offsets give **exactly-once** ingestion, even when a writer retries.
- Load jobs have quotas (for example *approx.* 1,500 load jobs per table per day). Micro-batching every 30 seconds with load jobs will hit them. For near real-time, use the Write API.

**Example (Python, load a partition idempotently).**

```python
from google.cloud import bigquery

client = bigquery.Client(project="prj-ingest-prod")
run_date = "20261022"

job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.PARQUET,
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # replace only this partition
    time_partitioning=bigquery.TimePartitioning(field="order_date"),
    labels={"pipeline": "pos_orders", "env": "prod"},
)
uri = f"gs://pos-landing/orders/dt={run_date}/*.parquet"
job = client.load_table_from_uri(uri, f"raw_pos.orders${run_date}", job_config=job_config)
job.result()  # raises on failure; rerun is safe
```

**Common mistake.** Using legacy `insertAll` for new pipelines, then fighting duplicates and the streaming buffer. Or using streaming when the business only needs data every hour (batch loads are free).

### 4.11 CDC tables: primary keys, `_CHANGE_TYPE`, `max_staleness`

**What.** BigQuery has built-in support for **change data capture**: a writer sends upserts and deletes, and BigQuery applies them to the table by primary key. Datastream uses this.

**How.**

1. The table has a **primary key** declared `NOT ENFORCED` (BigQuery uses it to apply changes, but it doesn't reject duplicate inserts from other writers).
2. The table has a **`max_staleness`** option.
3. A writer (Storage Write API) sends rows with the pseudo-column **`_CHANGE_TYPE`** = `'UPSERT'` or `'DELETE'`, and optionally **`_CHANGE_SEQUENCE_NUMBER`** to order changes for the same key (for example from the source log position).
4. Changes land in the write-optimized area. **Background apply jobs** merge them into the base table (these use your slots or on-demand compute; you can assign a `BACKGROUND` reservation).
5. At query time: if the last apply is newer than `now - max_staleness`, BigQuery reads the base table only (cheap). If it is older, BigQuery **merges the pending changes on the fly** during the query (correct, but more expensive).

```text
 source DB ──Datastream──▶ Write API: rows + _CHANGE_TYPE
                                  │
                           ┌──────▼──────┐   background apply   ┌──────────────┐
                           │ change log  │ ───────────────────▶ │ base table   │
                           └─────────────┘   every ~max_staleness└──────────────┘
                                  │                                   │
                       query: "is base fresh enough?" ── yes ─▶ read base only
                                                       └ no ──▶ base + merge changes at query time
```

**Why it matters.** It gives near-real-time replicas of operational tables (orders, order lines) **without writing MERGE jobs**. The trade-off is clear: **smaller `max_staleness` = fresher data = more apply cost**.

**Example.**

```sql
CREATE TABLE raw_pos.orders (
  order_id   STRING NOT NULL,
  store_id   STRING,
  status     STRING,
  order_ts   TIMESTAMP,
  updated_at TIMESTAMP,
  PRIMARY KEY (order_id) NOT ENFORCED
)
CLUSTER BY order_id
OPTIONS (max_staleness = INTERVAL 10 MINUTE);
```

**Common mistakes.**

1. Setting `max_staleness = 0` or 1 minute for a dashboard that refreshes every 15 minutes. Match the staleness to the business SLA.
2. Thinking `NOT ENFORCED` primary keys prevent duplicates for normal `INSERT`s. They don't.
3. Not monitoring how far behind the apply is. Check the table's last apply time and source timestamps (Datastream adds `datastream_metadata.source_timestamp`) and track freshness.

### 4.12 Nested and repeated data (STRUCT / ARRAY)

**What.** A column can be a `STRUCT` (a record with fields) or an `ARRAY` (a list). Combined, `ARRAY<STRUCT<...>>` stores child rows *inside* the parent row.

**How.** Capacitor stores each leaf field as its own column, with repetition/definition levels (3.5). So `lines.net_amount` is a real column you can read without reading the rest. And the order lines are stored next to their order, so no shuffle is needed to bring them together.

**Why it matters.** A join between `orders` and `order_lines` on two big tables is a shuffle join. If lines are nested inside orders, the "join" is free: the data is already together. Google recommends denormalizing with nested/repeated fields for parent-child data that is read together.

**Example.**

```sql
CREATE TABLE core.orders_nested
PARTITION BY order_date
CLUSTER BY store_id AS
SELECT o.order_id, o.order_date, o.store_id, o.customer_id,
       ARRAY_AGG(STRUCT(l.line_no, l.product_id, l.qty, l.net_amount)
                 ORDER BY l.line_no) AS lines
FROM core.orders o
JOIN core.fct_order_line l USING (order_id, order_date)
GROUP BY 1, 2, 3, 4;

-- Order-level metric: no UNNEST, no join
SELECT order_date, SUM((SELECT SUM(net_amount) FROM UNNEST(lines))) AS net_sales
FROM core.orders_nested
WHERE order_date = '2026-10-22'
GROUP BY 1;

-- Line-level metric: UNNEST inside the row (no shuffle)
SELECT l.product_id, SUM(l.qty) AS units
FROM core.orders_nested, UNNEST(lines) AS l
WHERE order_date = '2026-10-22'
GROUP BY 1;
```

**Common mistakes.**

1. Nesting things that change independently (e.g. putting a slowly changing product dimension inside every order). Nest *children of the same lifecycle*.
2. Looker/BI users need flat views. Offer a flat view or model the UNNEST in LookML.
3. Updating one line means rewriting the whole order row (fine for orders, bad for very hot rows).
4. Cluster columns must be top-level fields; you can't cluster on `lines.product_id`.

### 4.13 External tables and BigLake (brief)

- **External tables** query files in GCS (Parquet, ORC, Avro, CSV, JSON) or other sources without loading them. Slower than native, fewer optimizations, and no cache. Good for landing zones or one-off reads.
- **BigLake tables** are external tables with **fine-grained security** (row/column policies) via a connection service account, plus **metadata caching** for faster planning. They support open formats including **Apache Iceberg**. BigQuery can also manage Iceberg tables in your GCS bucket.
- **Why it matters:** if the organization has a lake in GCS (for example Delta or Iceberg files), BigLake is how BigQuery joins the lakehouse world. For the core warehouse, native tables are usually faster and simpler.

### 4.14 Security and governance

| Layer | Feature | How it works |
|---|---|---|
| Who can run jobs | `roles/bigquery.jobUser` on a project | Jobs (and cost) are in this project |
| Who can read data | `roles/bigquery.dataViewer` / `dataEditor` / `dataOwner` on **dataset** (or table) | Dataset is the usual boundary |
| Columns | **Policy tags** (taxonomy in Data Catalog / Dataplex) | Column tagged `PII.email`; only principals with *Fine-Grained Reader* on the tag can read it |
| Masking | **Dynamic data masking** rules on policy tags | Others see hashed (SHA-256), NULL, default value, or partial (last 4 chars) |
| Rows | **Row access policies** | `FILTER USING (country = 'FI')` per group |
| Share results not tables | **Authorized views / datasets / routines** | The view is authorized on the source dataset; users get access to the view only |
| Network perimeter | **VPC Service Controls** | Stop data exfiltration outside the perimeter |
| Keys | **CMEK** with Cloud KMS | Your key encrypts the table |

**Example.**

```sql
CREATE ROW ACCESS POLICY fi_only
ON mart_sales.fct_sales
GRANT TO ('group:sales-fi@example.com')
FILTER USING (country_code = 'FI');
```

**Why it matters.** A conversational BI tool answers *anything* a user asks. If the data model is not governed at the BigQuery layer, the chatbot can leak data. Security in BigQuery (not only in Looker) is the safe design.

**Common mistake.** Granting `roles/bigquery.admin` or project-level `dataViewer` to everyone "to make it work". Use groups for humans, service accounts for workloads, and **dataset-level** bindings.

### 4.15 Time travel, snapshots and clones

- **Time travel:** query any table as of a point in the last 2–7 days: `FOR SYSTEM_TIME AS OF ...`. Also undelete a dropped table within the window.
- **Fail-safe:** 7 more days, recovery only through support.
- **Table snapshot:** a read-only, point-in-time copy. You pay only for the data that differs from the base table. Useful before risky migrations; keeps longer than 7 days.
- **Table clone:** a *writable* copy that shares storage with the base. You pay for changed data. Perfect for **testing a new MERGE on production-like data** without copying TBs.

```sql
-- Recover yesterday's bad load
CREATE OR REPLACE TABLE core.fct_order_line_restore AS
SELECT * FROM core.fct_order_line
FOR SYSTEM_TIME AS OF TIMESTAMP '2026-10-22 03:00:00+00'
WHERE order_date = '2026-10-21';

-- Cheap test copy
CREATE TABLE test.fct_order_line_clone CLONE core.fct_order_line;
```

Common mistake: setting time travel to 2 days to save storage, then needing to recover a weekend incident on Tuesday.

### 4.16 INFORMATION_SCHEMA for monitoring

**What.** System views with metadata about jobs, tables, storage, reservations. Always **region-qualified** (`region-eu`, `region-us`).

| View | Use |
|---|---|
| `INFORMATION_SCHEMA.JOBS` (`_BY_PROJECT`, `_BY_USER`, `_BY_FOLDER`, `_BY_ORGANIZATION`) | Bytes billed, slot-ms, runtime, errors, labels, `cache_hit`, `job_stages` — about 180 days of history |
| `JOBS_TIMELINE` | Slot usage per second: find contention and peaks |
| `TABLE_STORAGE` | Logical/physical/long-term/time-travel bytes per table |
| `PARTITIONS` | Rows, size and last modified per partition: find tiny or skewed partitions |
| `TABLES`, `COLUMNS`, `TABLE_OPTIONS` | Schema audit (e.g. tables without partitioning or `require_partition_filter`) |
| `RESERVATIONS`, `ASSIGNMENTS` | Capacity configuration |

**Why it matters.** This is the "Measure" step of any optimization. It is also the source for the key platform metrics (duration, throughput, stability, cost). See the queries in the [BigQuery deep dive](../deep-dives/bigquery.md) (section 10).

**Example: slot usage per hour (contention view).**

```sql
SELECT TIMESTAMP_TRUNC(period_start, HOUR) AS hour,
       SUM(period_slot_ms) / (1000 * 3600)  AS avg_slots_used
FROM `region-eu`.INFORMATION_SCHEMA.JOBS_TIMELINE
WHERE job_creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY)
GROUP BY 1
ORDER BY 1;
```

**Common mistake.** Querying `INFORMATION_SCHEMA.JOBS` without the region qualifier, or in the wrong region, and seeing nothing.

### 4.17 Scripting and stored procedures (brief)

BigQuery supports **procedural SQL**: `DECLARE`, `SET`, `IF`, `LOOP`/`WHILE`/`FOR ... IN`, `BEGIN ... EXCEPTION ... END`, `EXECUTE IMMEDIATE` (dynamic SQL), `CALL` for **stored procedures**, and **transactions** (`BEGIN TRANSACTION ... COMMIT`).

```sql
CREATE OR REPLACE PROCEDURE core.load_order_line(run_date DATE)
BEGIN
  DECLARE from_date DATE DEFAULT DATE_SUB(run_date, INTERVAL 3 DAY);
  BEGIN TRANSACTION;
    DELETE FROM core.fct_order_line WHERE order_date BETWEEN from_date AND run_date;
    INSERT INTO core.fct_order_line
    SELECT * EXCEPT (_rn) FROM (
      SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id, line_no ORDER BY _source_ts DESC) AS _rn
      FROM stg.order_line WHERE order_date BETWEEN from_date AND run_date)
    WHERE _rn = 1;
  COMMIT TRANSACTION;
EXCEPTION WHEN ERROR THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = @@error.message;
END;
```

Why it matters: a variable computed with `DECLARE` is a constant at planning time, so it **enables partition pruning**, where a subquery might not. Common mistake: putting all orchestration logic inside giant procedures. Keep orchestration in Composer/Dataform and keep SQL units small and testable.

---

## 5. What matters most in practice

These are the topics that matter most when you modernize a BigQuery data warehouse.

### 5.1 The cost model and the optimization method: measure → fix → prove

Never start with a guess.

```text
 MEASURE                       FIX (top offenders first)            PROVE
 ───────                       ─────────────────────────            ─────
 INFORMATION_SCHEMA.JOBS  ──▶  partition / cluster / filters   ──▶  same metrics after
 billing export                incremental instead of full         dashboard: cost/day,
 JOBS_TIMELINE (contention)    fewer shuffles, pre-aggregate       runtime/DAG, failures
 Composer task durations       MVs / BI Engine for Looker          guardrails so it
 query plans of top 10         right pricing model / reservations  doesn't come back
```

**Step 1, measure.** Group jobs by **normalized query pattern** (`query_info.query_hashes.normalized_literals`), by service account, and by **job label** (`pipeline`, `dag_id`). Usually 10–20 patterns cause 80% of cost. Check the pricing model. Compare this quarter with last quarter in the billing export: did data grow, did a new dashboard arrive, did someone change a load to full refresh?

**Step 2, fix.** Apply the standard techniques. The internals explain why each works:

- Partition/cluster pruning → fewer files opened (3.10).
- Selecting fewer columns → fewer Capacitor columns read (3.5).
- Incremental loads → fewer files rewritten (4.9), less time travel storage (4.3).
- Filter/aggregate before joins → less shuffle (3.3).
- Pre-aggregation (MVs, aggregate tables) → BI queries read MB instead of TB.
- Under Editions: separate reservations, sensible baseline + autoscaling, avoid peaks by scheduling.

**Step 3, prove and protect.** Before/after numbers per pattern. Guardrails: `maximum_bytes_billed` on jobs, `require_partition_filter`, custom daily quotas, budget alerts, labels on every job.

**Summary:**

> "I treat cost like a performance bug. First I find the top query patterns by bytes billed and slot-hours in `INFORMATION_SCHEMA.JOBS`. Then I read their plans and fix the biggest ones with partitioning, clustering, incremental MERGE and pre-aggregation. Then I show the same metrics before and after on a dashboard, and add guardrails so the cost doesn't come back."

### 5.2 Partition and cluster design for a sales fact

**Domain.** Retail: `orders`, `order_lines`, `stores` (~500), `products` (~200k), `customers` (millions). About 3 years of history, maybe 2–5 billion lines.

**Questions to ask first:**

1. How do people filter? (Almost always a date range; often store/region; often product/category.)
2. How do late changes arrive? (Returns and corrections up to N days later.)
3. How big is a day? (If 3M lines/day ≈ a few hundred MB/day → daily partitions are fine.)

**Design.**

```sql
CREATE TABLE core.fct_sales_line (
  order_date   DATE NOT NULL,      -- business date, local store time
  order_id     STRING NOT NULL,
  line_no      INT64 NOT NULL,
  store_id     STRING NOT NULL,
  product_id   STRING NOT NULL,
  customer_id  STRING,
  channel      STRING,             -- 'store' / 'web' / 'app'
  qty          INT64,
  gross_amount NUMERIC,
  discount     NUMERIC,
  net_amount   NUMERIC,
  _source_ts   TIMESTAMP,          -- from CDC, for dedup ordering
  _loaded_at   TIMESTAMP,
  PRIMARY KEY (order_id, line_no) NOT ENFORCED
)
PARTITION BY order_date
CLUSTER BY store_id, product_id
OPTIONS (require_partition_filter = TRUE,
         description = 'Grain: one row per order line');
```

**Why these choices.**

- **Partition by business date** (`order_date`), not by load time: users filter by business date, and late corrections go to the right partition. (A raw landing table can be ingestion-time partitioned instead.)
- **Store the date as a column**, derived once with the right time zone. `DATE(order_ts)` in UTC would split a Finnish evening into two days.
- **Cluster by `store_id`, then `product_id`**: dashboards filter by store/region most; product second. Under 4 columns. If queries are mostly by product, swap the order. Check the real query patterns in `JOBS` first.
- **`require_partition_filter`** protects against Looker explores without a date.
- **Not clustering on `channel`**: 3 values; low value as a first cluster column.

**Same pattern for event data.** A high-volume event table (for example ShopFlow clickstream events) follows the same pattern: `PARTITION BY DATE(event_ts) CLUSTER BY customer_id, product_id`. At a few billion rows per day, daily partitions are large and well-sized. Six years of daily partitions ≈ 2,190 partitions, well under the 10,000 limit.

### 5.3 Incremental and idempotent loading

**Goal.** A DAG run for logical date D can be **rerun any number of times** and give the same result.

**Pattern A, partition overwrite (best for facts with a known reprocessing window).**

```sql
-- Reprocess the last 3 business days atomically.
DECLARE run_date DATE DEFAULT @run_date;          -- from Airflow {{ ds }}
DECLARE from_date DATE DEFAULT DATE_SUB(run_date, INTERVAL 2 DAY);

MERGE core.fct_sales_line T
USING (
  SELECT * FROM stg.sales_line
  WHERE order_date BETWEEN from_date AND run_date
  QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id, line_no ORDER BY _source_ts DESC) = 1
) S
ON FALSE                                            -- never match: delete + insert
WHEN NOT MATCHED BY SOURCE
     AND T.order_date BETWEEN from_date AND run_date THEN DELETE
WHEN NOT MATCHED THEN INSERT ROW;
```

This is the trick dbt's `insert_overwrite` uses: an atomic "replace these partitions" in one statement. Alternatives: a load/query job writing to `table$YYYYMMDD` with `WRITE_TRUNCATE`, or `DELETE` + `INSERT` in a transaction (4.17).

**Pattern B, MERGE upsert (for dimensions and for late updates by key).**
See the full MERGE in the [BigQuery deep dive](../deep-dives/bigquery.md) (section 7). The two non-negotiables:

1. **Deduplicate the source** with `QUALIFY ROW_NUMBER()`.
2. **Restrict the target** in the `ON` clause with a partition range (e.g. last N days), so the MERGE doesn't scan the whole table.

**SCD Type 2 dimension, short version.**

```sql
MERGE core.dim_product T
USING (
  SELECT s.product_id AS join_key, s.* FROM stg.product s       -- rows to update/close
  UNION ALL
  SELECT NULL AS join_key, s.*                                  -- new versions to insert
  FROM stg.product s
  JOIN core.dim_product d
    ON d.product_id = s.product_id AND d.is_current AND d.row_hash != s.row_hash
) S
ON T.product_id = S.join_key AND T.is_current
WHEN MATCHED AND T.row_hash != S.row_hash THEN
  UPDATE SET is_current = FALSE, valid_to = S._source_ts
WHEN NOT MATCHED THEN
  INSERT (product_id, name, category, row_hash, valid_from, valid_to, is_current)
  VALUES (S.product_id, S.name, S.category, S.row_hash, S._source_ts, NULL, TRUE);
```

**Why this is "simplifying load chains".** Old load chains often have: truncate staging → load → full rebuild of core → full rebuild of marts. Each step rewrites TBs (file rewrites, time travel storage, slot time). Incremental partition overwrite touches only a few days. Fewer, idempotent steps also mean simpler DAGs and safe retries.

### 5.4 Near-real-time ingestion and freshness

**Target.** "Near real-time sales data model". First, ask: **what freshness does the business really need?** 1 minute? 15 minutes? The answer drives cost.

```text
 POS / ERP database
      │  Datastream (CDC from the DB log)
      ▼
 raw_pos.orders, raw_pos.order_lines      ← BigQuery CDC tables
   PRIMARY KEY NOT ENFORCED, max_staleness = 5–15 min
      │
      │  every 5–15 min: Dataform/Composer incremental MERGE (small window)
      │  or views on top, if the logic is simple
      ▼
 core.fct_sales_line (partitioned, clustered)
      │
      │  MV / aggregate table, refreshed incrementally
      ▼
 mart_sales.mv_daily_store_product  ──▶  Looker (BI Engine, BI reservation)
```

**Options for the raw layer:**

| Source | Path |
|---|---|
| Operational DB (orders) | **Datastream → BigQuery CDC tables** (no code, `max_staleness`) |
| Event stream (clickstream, POS events) | **Pub/Sub → BigQuery subscription** (no transform), or **Dataflow → Storage Write API** (transform/enrich) |
| Files every hour | **Batch load jobs** (free) |

**Freshness as a metric.** Define freshness as `now - max(source event time)` in the serving table, and alert on it.

```sql
SELECT TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(_source_ts), MINUTE) AS freshness_min
FROM core.fct_sales_line
WHERE order_date = CURRENT_DATE('Europe/Helsinki');
```

**Cost traps in near-real-time:**

1. A MERGE every 5 minutes that scans the full target (4.9). Restrict to the last day or two.
2. Very small `max_staleness` on CDC tables → many background apply jobs.
3. Many tiny DMLs → DML queueing. One writer per table.
4. Dashboards on raw CDC tables → each query may merge pending changes at query time.

**If you know AWS.** A typical AWS pattern is Kinesis → Firehose → S3 (micro-batches). The GCP equivalents: Pub/Sub ≈ Kinesis Data Streams, Pub/Sub BigQuery subscription ≈ Firehose to a warehouse, Dataflow ≈ Kinesis Data Analytics/Flink or Spark Structured Streaming, Storage Write API ≈ the low-level writer.

### 5.5 Making BigQuery efficient for Looker (and conversational BI)

Looker generates SQL from LookML and sends it to BigQuery. Every tile, every filter change, and every chatbot question is a query. So:

1. **Serve from small, pre-aggregated tables.** Daily store × product aggregate (MV or Dataform table). Looker **aggregate awareness** picks the aggregate table automatically when possible.
2. **Force date filters.** `always_filter` / `sql_always_where` in LookML + `require_partition_filter` in BigQuery.
3. **Cluster by the columns users filter on** (store, region, category).
4. **Use caching on purpose.** Looker caches with datagroups; BigQuery caches identical queries. Avoid `CURRENT_TIMESTAMP()` in generated SQL.
5. **BI Engine** for sub-second interactive use, after the model is small.
6. **Separate reservation for BI** (Editions), so ETL peaks don't make dashboards slow.
7. **Governance in BigQuery** (row policies, policy tags), so the conversational layer can't bypass it.
8. **A clean semantic model**: clear names, descriptions, one definition of "net sales". A conversational BI tool (an LLM writing queries through Looker's semantic layer) is only as good as the model and descriptions it sees.

### 5.6 Pricing model choice and workload isolation

A common decision: "Should we move from on-demand to Editions?"

- **Measure** the current on-demand spend and the slot usage pattern (`JOBS_TIMELINE`). If average usage is steady and high, Editions with a baseline is likely cheaper. If usage is spiky and low, on-demand may be cheaper.
- **Design reservations by workload:** `etl` (baseline for nightly loads, autoscale), `bi` (small baseline, autoscale for business hours), and maybe a `background` reservation for CDC apply.
- **Know the trap:** after moving to Editions, "bytes scanned" no longer equals cost. Teams must learn to look at slot-hours. Bad SQL gets *slower* (waits for slots) instead of more expensive (until autoscaling hits the max).

### 5.7 Metrics for load duration, throughput, stability and cost

Every platform team should track these. Here is how to get each from BigQuery.

| Metric | Source | Example measure |
|---|---|---|
| Load duration | Composer task durations + `JOBS.end_time - start_time` per label | p50/p95 runtime per DAG, critical path length |
| Throughput | `JOBS.total_bytes_processed`, rows inserted (`dml_statistics`), Write API metrics | GB or rows per minute per pipeline |
| Stability | `JOBS.error_result`, Airflow task failures/retries | failure rate per pipeline per week |
| Freshness | `MAX(_source_ts)` in serving tables | minutes behind source |
| Cost | `total_bytes_billed`, `total_slot_ms` by label; billing export | € per pipeline per day, € per dashboard |

```sql
-- Stability + cost per pipeline per day (labels set by Composer operators)
SELECT DATE(creation_time) AS day,
       (SELECT value FROM UNNEST(labels) WHERE key = 'pipeline') AS pipeline,
       COUNT(*)                                        AS jobs,
       COUNTIF(error_result IS NOT NULL)               AS failed_jobs,
       SUM(total_slot_ms) / 1000 / 3600                AS slot_hours,
       SUM(total_bytes_billed) / POW(1024, 4)          AS tib_billed,
       APPROX_QUANTILES(TIMESTAMP_DIFF(end_time, start_time, SECOND), 100)[OFFSET(95)] AS p95_runtime_s
FROM `region-eu`.INFORMATION_SCHEMA.JOBS
WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
GROUP BY 1, 2
ORDER BY 1, slot_hours DESC;
```

### 5.8 Governance and safe change

- **Dataset-level IAM** with groups and service accounts, managed in Terraform.
- **Policy tags and masking** for PII (customer email, phone).
- **Row access policies** for country/store-region access.
- **Authorized views** for sharing the mart with other teams without access to `core`.
- **Testing with clones**: clone production tables into a test dataset, run the new MERGE, compare row counts and aggregate fingerprints (see the reconciliation query in the [BigQuery deep dive](../deep-dives/bigquery.md), section 11).
- **Recovery**: time travel and snapshots before big changes.

### 5.9 Modernization smells you will find in an old BigQuery EDW

You will probably inherit some of these.

| Smell | Why it hurts | Modern fix |
|---|---|---|
| **Date-sharded tables** (`sales_20261022`) | Many tables, metadata overhead, wildcard queries, no partition features | One partitioned table |
| Full `CREATE OR REPLACE` of large facts daily | Rewrites everything, high slot and storage cost | Partition overwrite / incremental MERGE |
| `SELECT *` views stacked 5 deep | Hides what is read; pruning gets hard to reason about | Explicit columns, fewer layers, materialize key layers |
| Same CTE referenced many times | May be evaluated more than once | Materialize to a temp/intermediate table |
| Legacy SQL, legacy streaming | Old features, cost | GoogleSQL, Storage Write API |
| No labels on jobs | Can't attribute cost | Labels from Composer on every job |
| Everyone has project-level admin | Risk; no audit | Dataset IAM via Terraform |

---

## 6. Spark / Fabric / AWS → BigQuery mental model

| Spark / Fabric / AWS | BigQuery / GCP | Difference to remember |
|---|---|---|
| Spark executor core | **Slot** | You don't size it; BigQuery assigns slots |
| Spark job → stages → tasks | Query → **stages** → work units | Same idea; shuffle is a separate in-memory service |
| Shuffle files on local disk | **Disaggregated in-memory shuffle** | Spill shows in the plan as "bytes spilled" |
| Broadcast hash join / sort-merge join | **Broadcast join / hash (shuffle) join** | Optimizer chooses; filter early |
| Parquet / Delta files in S3 / OneLake | **Capacitor** files in **Colossus** | You never see the files |
| `partitionBy("date")` | `PARTITION BY order_date` | Max 10,000 partitions |
| Z-order / `OPTIMIZE` | **Clustering** + automatic reclustering | Free, background, up to 4 columns |
| `VACUUM`, small-file compaction | Automatic compaction | Nothing to schedule |
| Delta time travel | **Time travel** 2–7 days + fail-safe 7 days | Window is fixed by dataset setting |
| Delta `MERGE INTO` | `MERGE` | Same semantics; dedupe source; restrict target |
| Delta Change Data Feed / CDC apply job | **CDC tables** (`_CHANGE_TYPE`, `max_staleness`) | BigQuery applies changes for you |
| Fabric capacity units (F SKU) | **Editions reservations** (baseline + autoscale) | Or on-demand per TiB |
| Fabric workspace | Project + datasets | IAM mostly at dataset level |
| Bronze / Silver / Gold | `raw` / `stg`+`core` / `mart` datasets | Same medallion idea, often separate projects |
| Fabric Direct Lake / Power BI cache | **BI Engine**, MVs, Looker caching | |
| Kinesis Data Streams / Kafka | **Pub/Sub** (Managed Kafka also exists) | |
| Kinesis Firehose → S3/Redshift | **Pub/Sub BigQuery subscription** | No code |
| AWS DMS | **Datastream** | Writes into BigQuery CDC tables |
| Glue / EMR Spark | **Dataflow** (Beam), **Dataproc** (Spark) | Dataflow = serverless streaming and batch |
| Step Functions / MWAA | **Cloud Composer** (managed Airflow) | |
| Glue Data Catalog / Purview | **Dataplex / Data Catalog**, policy tags | |
| CloudWatch / Cost Explorer | Cloud Monitoring, **INFORMATION_SCHEMA**, billing export | |

**The most important explanation:** in Spark you optimize by giving the engine *more or better resources* and tuning it (partitions, memory, AQE). In BigQuery you mostly optimize by **reducing the work**: fewer bytes (pruning, columns), less shuffle (filter before join, pre-aggregate, nest), fewer rewrites (incremental), and by **choosing the pricing model and reservations**. "Add more slots" as the first step is Spark thinking. In BigQuery, measure and reduce the work first.

---

## 7. Hands-on mini lab (free sandbox)

**Setup.**

- Open https://console.cloud.google.com/bigquery. Create a project. The **BigQuery sandbox** needs no credit card.
- The public dataset `bigquery-public-data.thelook_ecommerce` (tables `orders`, `order_items`, `products`, `users`, `events`, ...) lives in the **US multi-region**. Create your lab dataset in `US` too, or the queries will fail (location rule, 4.1).
- **Sandbox limits to know** (check current docs): tables and partitions expire after 60 days by default, and the sandbox does **not support DML** (`INSERT`/`UPDATE`/`DELETE`/`MERGE`) or streaming. For exercises 5 and 6 enable billing (the free trial credit is enough; these queries stay inside the 1 TiB free tier).
- The data is small (MBs to a few GB) and regenerated regularly, so your numbers will differ. Small tables also hit the 10 MB on-demand minimum per table. Watch the *direction* of the change, not exact numbers.

### Exercise 1: columnar reads and LIMIT
Type each query in the editor and **don't run it**. Read the estimate in the top right ("This query will process X").

```sql
SELECT * FROM `bigquery-public-data.thelook_ecommerce.order_items`;
SELECT * FROM `bigquery-public-data.thelook_ecommerce.order_items` LIMIT 10;
SELECT order_id, sale_price FROM `bigquery-public-data.thelook_ecommerce.order_items`;
```
**Observe:** the first two have the **same** estimate (LIMIT doesn't reduce the scan). The third is much smaller (only 2 columns are read). This is Capacitor (3.5) in action.

CLI version:

```bash
bq query --use_legacy_sql=false --dry_run \
  'SELECT order_id, sale_price FROM `bigquery-public-data.thelook_ecommerce.order_items`'
```

### Exercise 2: build a partitioned and clustered copy

```sql
CREATE SCHEMA IF NOT EXISTS lab OPTIONS (location = 'US');

-- Unpartitioned copy (baseline)
CREATE OR REPLACE TABLE lab.sales_line_flat AS
SELECT oi.id AS line_id, oi.order_id, oi.user_id, oi.product_id,
       DATE(oi.created_at) AS order_date, oi.status, oi.sale_price,
       p.category, p.brand, p.department
FROM `bigquery-public-data.thelook_ecommerce.order_items` oi
JOIN `bigquery-public-data.thelook_ecommerce.products` p ON p.id = oi.product_id
WHERE DATE(oi.created_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 50 DAY);  -- sandbox: keep < 60 days

-- Partitioned + clustered copy
CREATE OR REPLACE TABLE lab.sales_line
PARTITION BY order_date
CLUSTER BY category, product_id
OPTIONS (require_partition_filter = TRUE) AS
SELECT * FROM lab.sales_line_flat;
```
Why the 50-day filter: in the sandbox, partitions older than 60 days expire automatically. With billing enabled you can drop the filter and copy all years.

### Exercise 3: partition pruning in a dry run

```sql
-- a) Fails: require_partition_filter
SELECT SUM(sale_price) FROM lab.sales_line;

-- b) Prunes: compare the estimate with the flat table
SELECT SUM(sale_price) FROM lab.sales_line
WHERE order_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) AND CURRENT_DATE();

SELECT SUM(sale_price) FROM lab.sales_line_flat
WHERE order_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) AND CURRENT_DATE();

-- c) Does pruning survive a function on the column?
SELECT SUM(sale_price) FROM lab.sales_line
WHERE FORMAT_DATE('%Y%m', order_date) = FORMAT_DATE('%Y%m', CURRENT_DATE());
```
**Observe:** (a) errors. (b) the partitioned estimate is about 7/50 of the flat one. (c) check whether the planner still accepts it and what it estimates; then rewrite it with `order_date >= DATE_TRUNC(CURRENT_DATE(), MONTH)` and compare.

### Exercise 4: clustering effect (actual vs. estimate)

```sql
SELECT COUNT(*), SUM(sale_price)
FROM lab.sales_line
WHERE order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  AND category = 'Jeans';
```
Run it. Compare the **dry-run estimate** with **"Bytes billed"** in Job information. **Observe:** on big tables the billed bytes are lower than the estimate because of block pruning. On this small table you may see little or no difference (few blocks, 10 MB minimum). That itself is a lesson: clustering pays off on large tables. Open **Execution details** and look at the stages, records read, and wait/read/compute/write bars.

### Exercise 5: MERGE idempotency (needs billing enabled)

```sql
CREATE OR REPLACE TABLE lab.fct_sales
PARTITION BY order_date CLUSTER BY product_id AS
SELECT * FROM lab.sales_line_flat WHERE FALSE;     -- empty, same schema

-- Run this MERGE twice
MERGE lab.fct_sales T
USING (
  SELECT * FROM lab.sales_line_flat
  WHERE order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY line_id ORDER BY order_date DESC) = 1
) S
ON T.line_id = S.line_id
   AND T.order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)   -- restrict target scan
WHEN MATCHED THEN UPDATE SET status = S.status, sale_price = S.sale_price
WHEN NOT MATCHED THEN INSERT ROW;

SELECT COUNT(*) FROM lab.fct_sales;    -- same number after run 1 and run 2
```
**Observe:** the row count doesn't change on the second run (idempotent). In Job information, check "DML affected rows". Then try a source with duplicates (remove the `QUALIFY` and `UNION ALL` the source with itself) and see the MERGE fail with "must match at most one source row".

### Exercise 6: time travel

```sql
DELETE FROM lab.fct_sales WHERE category = 'Jeans';   -- "accident" (needs billing)

SELECT COUNT(*) FROM lab.fct_sales
FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 MINUTE)
WHERE category = 'Jeans';
```
**Observe:** the deleted rows are still visible in the past version. Restore them with `INSERT ... SELECT ... FOR SYSTEM_TIME AS OF`.

### Exercise 7: measure yourself with INFORMATION_SCHEMA

```sql
SELECT creation_time, job_type, statement_type, cache_hit,
       total_bytes_processed, total_bytes_billed, total_slot_ms,
       TIMESTAMP_DIFF(end_time, start_time, MILLISECOND) AS runtime_ms,
       LEFT(query, 80) AS sql_start
FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_USER
WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
ORDER BY creation_time DESC;
```
**Observe:** run the same SELECT twice and see `cache_hit = TRUE` and 0 bytes billed for the second run. Compare `total_slot_ms` for the MERGE vs. the simple SELECTs. Bonus: `UNNEST(job_stages)` to see the stages of your MERGE.

**Bonus (optional):** create the MV from 4.8 on `lab.sales_line` and check in the execution details whether a `GROUP BY category` query on the base table was rewritten to use the MV.

---

## 8. Glossary

| Term | Meaning |
|---|---|
| **Active storage** | Table/partition data modified in the last 90 days; normal storage price. |
| **Admin project** | Project that holds reservations and commitments. |
| **Assignment** | Link from a project/folder/org to a reservation, per job type. |
| **Authorized view** | A view allowed to read a source dataset, so users can query the view without access to the base tables. |
| **Autoscaling** | Editions feature that adds slots above the baseline when needed, up to a max. |
| **Baseline slots** | Slots always allocated to a reservation and always billed. |
| **BI Engine** | In-memory acceleration layer for BI queries, reserved in GB. |
| **BigLake** | External tables (e.g. on GCS, Iceberg) with BigQuery fine-grained security and metadata caching. |
| **Borg** | Google's cluster manager that runs BigQuery's services on machines. |
| **Broadcast join** | Join where the small side is copied to every worker; no shuffle of the big side. |
| **Capacitor** | BigQuery's internal columnar file format with encoding, compression and statistics. |
| **CDC table** | Table with a primary key and `max_staleness` that accepts `_CHANGE_TYPE` upserts/deletes. |
| **Clone** | Writable copy that shares storage with its base; you pay for changes only. |
| **Clustering** | Sorting data in a table/partition by up to 4 columns to enable block pruning. |
| **CMEK** | Customer-managed encryption keys (Cloud KMS). |
| **Colossus** | Google's distributed file system; stores BigQuery data. |
| **Commitment** | 1- or 3-year purchase of slots for a discount. |
| **Copy-on-write** | Changes are written as new files; old files are replaced, not edited. |
| **Dataset** | Container of tables/views with a fixed location; main IAM boundary. |
| **Datastream** | Managed CDC service from databases into BigQuery/GCS. |
| **Dremel** | BigQuery's distributed query execution engine. |
| **Dry run** | Validate a query and estimate bytes without running it (free). |
| **Editions** | Capacity pricing tiers: Standard, Enterprise, Enterprise Plus. |
| **Fail-safe** | 7 days after the time-travel window; recovery via Google support. |
| **Hash (shuffle) join** | Both sides repartitioned by join key through shuffle. |
| **INFORMATION_SCHEMA** | Region-qualified system views for jobs, storage, tables, reservations. |
| **Job** | Any unit of work: query, load, copy, extract. Runs and is billed in a project. |
| **Job labels** | Key-value tags on jobs for cost attribution. |
| **Jupiter** | Google's high-bandwidth data center network. |
| **Logical / physical storage billing** | Bill on uncompressed bytes vs. compressed bytes (physical includes time travel and fail-safe). |
| **Long-term storage** | Data not modified for 90 consecutive days; about 50% cheaper. |
| **Materialized view** | Precomputed, incrementally refreshed query result; can be used by automatic query rewrite. |
| **`max_staleness`** | Option (CDC tables, MVs) that allows results up to a given age, trading freshness for cost. |
| **MERGE** | One statement that inserts, updates and deletes by matching source and target. |
| **Nested / repeated fields** | `STRUCT` and `ARRAY` columns that store child records inside a row. |
| **On-demand pricing** | Pay per TiB processed by queries. |
| **Partition** | Segment of a table by date/time, ingestion time, or integer range; max 10,000 per table. |
| **Partition decorator** | `table$YYYYMMDD` to address one partition in load/query jobs. |
| **Policy tag** | Column-level security label from a taxonomy; controls who can read a column. |
| **Pruning** | Skipping partitions or blocks using metadata, so they are never read. |
| **Query result cache** | Free reuse of an identical query's result for about 24 hours if tables didn't change. |
| **Reservation** | Named pool of slots (Editions). |
| **Row access policy** | Row-level filter applied per user/group. |
| **Shuffle tier** | Separate in-memory service that moves data between query stages. |
| **Skew** | Uneven data distribution: one worker gets much more data (max ≫ avg in the plan). |
| **Slot** | Virtual unit of compute in BigQuery. |
| **Snapshot** | Read-only point-in-time copy; pay for differences from the base. |
| **Spill** | Shuffle data written to disk because it didn't fit in memory; slow. |
| **Stage** | Step of a query plan, run in parallel by many workers. |
| **Storage Read API** | gRPC API to read table data in parallel streams (Arrow/Avro). |
| **Storage Write API** | gRPC API for streaming/batch writes; supports exactly-once with committed streams. |
| **Streaming buffer** | Write-optimized area for legacy streamed rows before conversion to columnar storage. |
| **Time travel** | Query or restore past table versions within 2–7 days. |
| **`_CHANGE_TYPE`** | Pseudo-column (`UPSERT`/`DELETE`) used when writing to CDC tables. |
| **`require_partition_filter`** | Table option that rejects queries without a partition filter. |
| **`maximum_bytes_billed`** | Job setting that makes a query fail if it would bill more than the limit. |

---

**Next steps:** do the lab (about 1 hour). Then try to explain the measure → fix → prove method from section 5.1 in your own words.
