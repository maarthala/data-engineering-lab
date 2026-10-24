# Data migration and EDW modernization on GCP

> **How to use this page.** It teaches how to plan and run a legacy enterprise data warehouse (EDW) → BigQuery migration from the fundamentals: the phases, the GCP tools and how they work inside, validation, cutover, and modernizing a warehouse that is already on GCP. For a short revision sheet, use the [migration and modernization deep dive](../deep-dives/migration-modernization.md). The platform side (projects, IAM, Terraform) is in [The GCP platform: IAM and Terraform](platform-iam-terraform.md).
>
> **Three key ideas:**
>
> 1. A migration plan is not complete until it explains **how the team proves the data is correct** (validation) and **how it switches** (cutover and rollback).
> 2. Use clear layer names. **Bronze** = raw, as-is, with lineage. **Silver** = cleaned, deduplicated, conformed. **Gold** = business models and aggregates. On GCP these are usually `raw_*` → `stg_*` → `mart_*` datasets.
> 3. Measure everything: baselines before, **numbers** after each wave.

---

## 1. What it is and why it exists

### Migration vs. modernization

- **Migration** = moving data, code and users from system A to system B, without losing or corrupting anything, and without breaking the people who use it. The success test: *"the business can't tell the difference, except that it's better."*
- **Modernization** = changing **how** the platform works to make it simpler, faster, cheaper or more reliable: fewer hops, incremental loads instead of full reloads, standard ingestion patterns, infrastructure as code, better models.

They often happen together, but they are different risks. A migration risks **correctness and continuity**. A modernization risks **breaking consumers who depend on the old shape**.

### An analogy: moving a restaurant

- **Migration** is moving the restaurant to a new building. Every recipe, ingredient and table booking must arrive. You open the new kitchen **while the old one still serves** (parallel run), you **taste-test** dishes from both kitchens (validation), and you move the guests on a **fixed date** (cutover), keeping the old kitchen ready for a few weeks in case something goes wrong (rollback).
- **Modernization** is redesigning the kitchen you already have: one central delivery door instead of five (shared ingestion), fewer people carrying the same box from room to room (fewer hops), and a written recipe book that everyone follows (Terraform modules, Dataform). You can't close the restaurant while you do it, so you change **one station at a time**, and the guests keep getting their food (compatibility views).

### Migrating INTO GCP vs. modernizing WITHIN GCP

| | Into GCP | Within GCP |
|---|---|---|
| Example | Teradata / Redshift / Hadoop / S3 → BigQuery | Ingestion spread across domain projects → shared ingestion projects |
| Main risk | Data correctness, SQL dialect differences, egress cost, volume | Hidden consumers, broken dependencies, IAM changes, location constraints |
| Main tools | Storage Transfer Service, BigQuery Migration Service, DTS, DVT | INFORMATION_SCHEMA.JOBS, lineage, table copies, views, Terraform, Dataform |
| Both need | Assessment, waves, validation, cutover, rollback, decommission, handover | |

Moving the load layer into shared GCP ingestion projects and simplifying warehouse load chains is **modernizing within GCP**. Moving a legacy EDW (Teradata, Oracle, Redshift, an on-prem SQL Server) into BigQuery is **migrating into GCP**. Many programmes do both, and the **same framework** works for both.

### Migration strategies (the "R's")

| Strategy | Meaning | Data-platform example | When |
|---|---|---|---|
| **Rehost** (lift and shift) | Move as-is to new infrastructure | Hadoop Spark jobs → Dataproc with minimal changes; HDFS → GCS | Deadline pressure (data center exit), low appetite for change |
| **Replatform** | Small changes to fit the target platform | Teradata SQL translated to BigQuery SQL; same model, same schedules | Most warehouse migrations |
| **Refactor / re-architect** | Redesign | Nightly full extracts → Datastream CDC; date-sharded tables → partitioned; scattered scheduled queries → Dataform | Where the old design is the actual problem |
| **Retire** | Don't migrate it | Tables no one queried in 6 months; reports nobody opens | Always check first: often a large share is unused |
| **Retain** | Leave it where it is for now | A system with a planned end-of-life next year | When migration cost > remaining value |
| **Repurchase** | Replace with SaaS / managed product | A custom ingestion tool → a managed connector | When a product does it well enough |

A real migration is a **mix**: rehost some things to move quickly, refactor where it pays off, retire the rest.

---

## 2. Where it fits in a modern GCP data platform

A typical EDW modernization on GCP aims for a leaner, faster, more stable data warehouse: a near real-time sales model in BigQuery, Looker and conversational BI on top, and the load layer moved into shared ingestion projects. Concretely:

1. **Consolidate ingestion** into shared ingestion projects **without breaking** downstream marts, Looker dashboards and exports.
2. **Simplify load chains**: remove hops, make loads incremental, restructure Composer DAGs.
3. **Measure** load duration, throughput, stability and cost, **before and after**, to prove the improvement.
4. **Hand over** patterns to internal teams, and **decommission** the old pipelines so the savings are real.

Every one of these is a small migration: move something, validate it, switch consumers, remove the old one. A full legacy EDW → BigQuery migration uses exactly the same steps, at a larger scale.

---

## 3. Architecture: how a migration works

### 3.1 The phased framework

```text
  ┌─────────┐   ┌─────────┐   ┌───────────────────┐   ┌──────────┐   ┌──────────┐   ┌──────────────────────┐
  │ ASSESS  │──►│ DESIGN  │──►│ MIGRATE            │──►│ VALIDATE │──►│ CUT OVER │──►│ OPTIMIZE · HAND OVER │
  │         │   │         │   │ bulk history +     │   │ (daily,  │   │ + roll-  │   │ · DECOMMISSION       │
  │inventory│   │target,  │   │ ongoing deltas     │   │ during   │   │  back    │   │                      │
  │usage    │   │waves,   │   │ code + IaC         │   │ parallel │   │  window  │   │                      │
  │lineage  │   │exit     │   │                    │   │ run)     │   │          │   │                      │
  │baseline │   │criteria │   │                    │   │          │   │          │   │                      │
  └─────────┘   └─────────┘   └───────────────────┘   └──────────┘   └──────────┘   └──────────────────────┘
       ▲                                                                                        │
       └───────────────── repeat per WAVE (domain or source), lessons fed back ─────────────────┘
```

### 3.2 The data path when migrating INTO GCP (files from another cloud)

```text
 SOURCE (AWS)                    TRANSFER                         GCP
 ───────────                     ────────                         ───
 S3 (millions of small files)
   │ 1. compact (Spark/EMR or         ┌──────────────────────┐
   │    Dataproc reading S3)          │ Storage Transfer     │     gs://landing-history/
   ▼                                  │ Service (agentless)  │       (Parquet, large files,
 S3 staging (large Parquet) ─────────►│ bulk once + incr.    ├───►     lineage columns)
                                      │ sync until cutover   │          │ 3. load jobs (free)
                                      └──────────────────────┘          ▼
 Kinesis / Kafka (live) ── 2. bridge / dual consume ──► Pub/Sub ──► Dataflow ──► raw_* tables
                                                                                │ 4. Dataform
                                                                                ▼
                                                                     stg_* → mart_*  → Looker
                          5. Data Validation Tool / reconciliation SQL compares source vs. target
```

### 3.3 The data path when modernizing WITHIN GCP (consolidating ingestion)

```text
 BEFORE                                         AFTER
 ──────                                         ─────
 prj-sales:   own Datastream → sales_raw.orders  prj-ingest-prod: Datastream → raw_pos.orders  (one owner)
 prj-finance: own extract  → fin_raw.orders                          │
 (same source ingested twice, 2 schedules,                           │ read cross-project
  2 sets of credentials, nobody owns it)        prj-sales:   sales_raw.orders  = VIEW → raw_pos.orders
                                                prj-finance: fin_raw.orders    = VIEW → raw_pos.orders
                                                (compatibility views; consumers unchanged; then
                                                 consumers move to the new name; views retired)
```

---

## 4. Core concepts, one by one

### 4.1 Assess

- **What.** Build a fact base before you plan: what exists, how big it is, who uses it, how it depends on other things, and how well it performs today.
- **How it works.**
    1. **Inventory**: sources, tables (rows, size, growth per day), pipelines and schedules, consumers (dashboards, exports, APIs, ML), SLAs, owners. On GCP, `INFORMATION_SCHEMA.TABLES`, `TABLE_STORAGE` and Cloud Asset Inventory give much of this automatically.
    2. **Usage analysis** from query logs: which tables are read, by whom, how often, when last. On GCP that is `INFORMATION_SCHEMA.JOBS` with `referenced_tables`. On another warehouse it's its query history (Teradata DBQL, Redshift system tables, Snowflake `QUERY_HISTORY`).
    3. **Dependency / lineage mapping**: which pipeline writes which table, and which tables feed which reports. Sources: query logs (read tables → destination table), orchestration code, Dataplex data lineage (automatic for BigQuery jobs when enabled), Looker's content usage.
    4. **Complexity scoring** per object: SQL features (procedures, UDFs, dialect-specific functions), number of dependencies, data quality, PII, criticality.
    5. **Baselines**: today's load duration, failure rate, freshness, cost per day. Without a baseline, you can't prove improvement.
- **Why it matters.** It tells you what **not** to migrate (often a big share is unused), finds the **hidden consumers** that would break, and gives you the "before" numbers.
- **Example: usage of every table in the last 90 days.**

```sql
SELECT
  rt.project_id,
  rt.dataset_id,
  rt.table_id,
  COUNT(*)                     AS query_count,
  COUNT(DISTINCT j.user_email) AS distinct_users,
  MAX(j.creation_time)         AS last_queried
FROM `region-eu`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION AS j,
     UNNEST(j.referenced_tables) AS rt
WHERE j.creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
  AND j.job_type = 'QUERY'
  AND j.state = 'DONE'
GROUP BY 1, 2, 3
ORDER BY query_count DESC;
```

Notes: `JOBS_BY_ORGANIZATION` needs org-level permission; otherwise use `JOBS_BY_PROJECT` (alias `JOBS`) per project. Job history is kept about 180 days. `referenced_tables` is not filled for cache hits. Check how views appear in it in the current docs before relying on it for view-heavy schemas.

- **Complexity score example.**

| Object | Size | Readers (90d) | Dependencies | SQL complexity | PII | Score | Decision |
|---|---|---|---|---|---|---|---|
| `sales_raw.orders` | 2 TB | 40 | 12 | Low | Yes | Medium | Wave 1 |
| `fin_raw.gl_extract` | 50 GB | 3 | 2 | High (procedures) | No | High | Wave 3 |
| `tmp_marketing_2021_*` | 300 GB | 0 | 0 | — | — | — | **Retire** |

- **Common mistake.** Skipping usage analysis and migrating everything "to be safe". You pay to migrate, validate and run dead tables forever.

### 4.2 Design

- **What.** Decide the target and the plan: target architecture, approach per workload, waves, the validation strategy and the exit criteria.
- **How it works.**
    - **Target architecture**: layers (`raw_*` → `stg_*` → `mart_*`), project layout, location, naming, IAM, Terraform modules (see [The GCP platform: IAM and Terraform](platform-iam-terraform.md)).
    - **Wave planning**: group by domain or source. **Wave 1 = high value, low complexity**, to prove the pattern and build trust. Hard and risky things come later, when the team and tooling are mature. Each wave has its own validate → cutover → decommission.
    - **Validation strategy**: which checks at which level (section 5.1), tolerances, who signs off.
    - **Exit criteria**: measurable conditions for cutover, agreed with the business **before** the parallel run. Example: *"14 consecutive daily runs with zero unexplained differences in counts and hashes per partition; the top 10 reports match within 0.01%; freshness ≤ 15 min; nightly load ≤ 2 h; business owner sign-off."*
- **Why it matters.** Without exit criteria agreed upfront, the parallel run never ends ("let's wait one more week") or ends on a feeling.
- **Common mistake.** Designing the whole target model before moving any data. Agree the model for wave 1 in time-boxed workshops, with a decision log, and move.

### 4.3 Migrate: bulk history vs. ongoing deltas

```text
 time ─────────────────────────────────────────────── T0 (boundary) ─────────────────►
 │◄────────── HISTORY: one bulk copy ───────────────►│◄──── DELTAS: CDC / stream / ────►
 │  (Storage Transfer, export→load, DTS)              │      incremental batch
 │                                                    │  keeps target in sync during
 │                                                    │  the parallel run
                                          small overlap window around T0,
                                          removed by idempotent MERGE on the key
```

- **Bulk history**: moved once (or a few times), big, can be slow. Optimize for **cost and throughput**: compact, compress, columnar formats, one transfer.
- **Ongoing deltas**: everything that changes after the bulk snapshot. Database CDC (Datastream), streaming (Pub/Sub + Dataflow), or incremental batch by a watermark column.
- **The boundary T0** is a timestamp, a log position (LSN/SCN/binlog) or a Kafka offset. History owns everything before it; deltas own everything after. Load the deltas with a **small overlap** (e.g. from T0 minus one hour) and make writes **idempotent** (MERGE on the natural key), so the overlap creates no duplicates and late events create no gaps.
- **Common mistake.** Taking the bulk snapshot without recording the exact boundary. Then you can't prove no rows were lost or duplicated between history and deltas.

### 4.4 Cloud Storage basics

GCS is where migrated files land. You must know how it behaves.

| Concept | How it works | Why it matters in a migration |
|---|---|---|
| **Bucket** | Global unique name; a **location** (region, dual-region, multi-region) fixed at creation | Put it in the same location as the BigQuery datasets (e.g. `EU`) so loads are local |
| **Object** | **Immutable.** You can't append or edit; you write a new object (or a new version) | Writers create new files; "updating" means overwriting the whole object |
| **"Folders"** | The namespace is flat; `/` in names is a convention (hierarchical namespace buckets exist as an option; check docs) | Prefix design matters for listing and for Hive-style partitions (`dt=2026-10-01/`) |
| **Consistency** | **Strongly consistent** for reads after writes, overwrites, deletes, and for listing | Unlike old S3 behaviour, a loader can list right after a write and see the file |
| **Storage classes** | Standard, Nearline (30-day minimum), Coldline (90-day), Archive (365-day) | Landing = Standard; old raw files → colder classes; early deletion of cold objects is charged |
| **Lifecycle rules** | Change class or delete by age, prefix, version count | Automatic cleanup of landing files after they're loaded |
| **Versioning / soft delete** | Keep old versions; soft delete keeps deleted objects for a retention period | Protection against accidental overwrite during migration |
| **Retention policy / bucket lock / object holds** | Objects can't be deleted before a period | Compliance archives (WORM) |
| **Checksums** | Objects have CRC32C (and MD5 for non-composite objects) | Transfer tools verify integrity end to end |

### 4.5 Storage Transfer Service (STS)

- **What.** A managed service that copies large numbers of objects into (or between) GCS buckets, from **Amazon S3**, **Azure Blob Storage**, HTTP(S) URL lists, other GCS buckets, **on-prem file systems** and HDFS.
- **How it works.**
    - You create a **transfer job**: source, sink bucket, options, and an optional **schedule**. Each run is a **transfer operation**.
    - For **cloud sources** (S3, Azure), it is **agentless**: Google's workers list the source and copy in parallel. For S3 it authenticates with an access key or, better, an **AWS IAM role** trusted for Google's service identity (federation, no long-lived AWS key).
    - For **on-prem POSIX file systems and HDFS**, you run **transfer agents** (Docker containers) on machines near the data, grouped in **agent pools**. They read locally and upload. You can **cap bandwidth** so production links aren't saturated.
    - **Incremental sync**: on each run, STS compares source and sink (by name and metadata like size/checksum/modification) and copies only **new or changed** objects. Options: overwrite never / if different / always; delete source objects after transfer; delete sink objects not in the source (mirror).
    - **Filters and manifests**: include/exclude prefixes, last-modified windows, or a manifest file listing exactly which objects to copy.
    - **Event-driven transfers**: react to new objects (S3 via an SQS queue, GCS via Pub/Sub) instead of a schedule.
    - **Integrity**: it validates checksums and logs per-object results to Cloud Logging.
- **Why it matters.** It gives you a repeatable bulk copy and a "keep in sync until cutover" mechanism without writing copy code.
- **Example.** Job 1: one-time copy of `s3://shopflow-staging/compacted/` → `gs://prj-ingest-prod-landing-history/`. Job 2: daily incremental sync of new files until cutover, then disabled.
- **Common mistakes.** (1) Transferring **millions of tiny files**: per-object overhead makes it slow, and S3 charges per LIST/GET request. **Compact first.** (2) Forgetting **source egress cost**: AWS charges for data leaving S3 to the internet; plan **one** bulk transfer, not repeated reads. (3) Bucket in a different location from the target datasets.

### 4.6 Transfer Appliance

- **What.** A physical storage device Google ships to your data center. You copy data onto it locally, ship it back, and Google uploads it to your bucket. Data is encrypted.
- **When.** Hundreds of TB to PB with a slow or expensive network. A rough rule: if the network transfer would take weeks, consider it. Capacities and availability per country change, so **check current docs**.
- **Common mistake.** Forgetting the **deltas** that arrive while the device is in transit. You still need STS or another incremental sync from the snapshot point.

### 4.7 BigQuery Data Transfer Service (DTS)

- **What.** A managed, scheduled loader **into BigQuery**.
- **Sources**: Google SaaS (Ads, YouTube, …), **Amazon S3**, **Azure Blob Storage**, **GCS**, and **warehouse migrations** from **Amazon Redshift** and **Teradata** (Teradata uses an on-prem agent). **Dataset copies** between projects/regions and **scheduled queries** also run on DTS.
- **How it works.** A **transfer config** with a schedule, a source, and a destination dataset. Each run loads new data (for files: by URI pattern / modification time). Behind the scenes it uses BigQuery load jobs.
- **When to use it vs. STS.** STS lands **files in GCS** (you keep a raw copy, flexible). DTS loads **directly into BigQuery tables** (fewer moving parts). For a big migration, STS → GCS → load jobs is often the better choice, because a raw file copy in GCS is a great safety net and validation source.
- **Check current docs** for the exact source list; it grows.

### 4.8 BigQuery Migration Service

A set of free tools for migrating **warehouses** to BigQuery.

| Tool | What it does | How |
|---|---|---|
| **Migration assessment** | Analyses the source warehouse's metadata and **query logs**: object inventory, usage, query complexity, estimated translation effort | You extract metadata and logs with the `dwh-migration-dumper` CLI, upload to GCS, run the assessment; results land in BigQuery with a Looker Studio report |
| **Batch SQL translation** | Translates many SQL scripts (DDL, DML, procedures) from a source dialect to GoogleSQL | Put files in GCS, run a translation job, get translated files and an error/warning report |
| **Interactive SQL translator** | Translate a query in the BigQuery console | Useful for learning and for one-off queries |
| **Translation API** | Programmatic translation | Put translation into CI |

- **How it works inside.** It parses the source dialect into an intermediate representation and generates GoogleSQL. You can give **metadata** (from the dumper) so it resolves names and types correctly, and **configuration YAML** to rename objects (e.g. map `PROD_DB.SALES` to `prj-dwh-sales-prod.stg_sales`).
- **Source dialects** include Teradata, Redshift, Snowflake, Oracle, SQL Server/Synapse, Netezza, Vertica, Hive/Spark SQL/Presto and more; **check docs** for the current list.
- **Common mistake.** Trusting translation = correctness. It translates **syntax**; **semantics** can differ (NULL handling in string concatenation, integer division, implicit casts, case-sensitive string comparison, date functions, `CHAR` padding). Every translated object still goes through **validation**.

### 4.9 BigQuery load jobs and file formats

- **What.** A **load job** reads files from GCS (or local) into a table. Load jobs use a **shared slot pool** and are **free** (you pay storage), unless you assign them to your own reservation.
- **How it works.** BigQuery splits the input into chunks and loads in parallel. Write modes: `WRITE_APPEND`, `WRITE_TRUNCATE`, `WRITE_EMPTY`. You can load into **one partition** with a partition decorator (`table$20261001`) and `WRITE_TRUNCATE` to replace just that day, which makes reloads **idempotent**. A load job is **atomic**: all files load or none do.
- **Limits** exist (load jobs per table per day, per project per day, files and bytes per job, max compressed CSV/JSON file size). **Check current quotas** before you design a "one job per file" pattern for millions of files; that pattern hits limits.

| Format | Inside | Verdict for migration |
|---|---|---|
| **Parquet** | Columnar, typed schema, compressed per column, splittable | ✅ Best for big history loads; small and fast |
| **Avro** | Row-based, schema in the file, compressed blocks, splittable; logical types for dates/timestamps | ✅ Excellent; very exact types (enable logical types) |
| **ORC** | Columnar like Parquet | ✅ Fine (common from Hive) |
| **CSV** | Text, no types, escaping and encoding issues; **gzip CSV can't be split**, so it loads slower | ⚠️ Only if forced |
| **JSON (newline-delimited)** | Text, nested OK, larger, slower to parse | ⚠️ OK for semi-structured |

```bash
bq load \
  --source_format=PARQUET \
  --time_partitioning_field=reading_date \
  --clustering_fields=building_id,sensor_id \
  'prj-ingest-prod:raw_energy.readings_history' \
  'gs://prj-ingest-prod-landing-history/readings/year=2021/*.parquet'
```

- **Why typed formats matter.** With CSV you discover type problems in production (`"N/A"` in a number column, different date formats). Parquet/Avro carry types, so the load either works or fails clearly.
- **Common mistake.** Loading with schema **auto-detect** for the history and getting different inferred types per year. Define the schema explicitly (in Terraform or a JSON schema file).

### 4.10 External tables and BigLake for staged validation

- **What.** An **external table** lets BigQuery query files in GCS **without loading** them. **BigLake tables** are an improved version: access goes through a **connection** (a service account), so users don't need bucket access, and they support fine-grained security and **metadata caching** for speed. BigLake can also cover Iceberg tables, and (with BigQuery Omni) data in S3/Azure; check docs.
- **How it helps a migration.**
    - Stage the **source extract** in GCS as Parquet and define an external/BigLake table on it. Now source and target are **both queryable in BigQuery**, and row-level comparison is one SQL query (section 5.1).
    - Explore and profile the history **before** deciding the final model.
    - Hive-style partition paths (`dt=2026-10-01/`) become partition columns.
- **Common mistake.** Using external tables as the long-term warehouse for hot data. They are slower and have fewer optimizations than native tables. Load what you query often.

### 4.11 The Data Validation Tool (DVT)

- **What.** An open-source Python CLI from Google (`google-pso-data-validator`, repo `professional-services-data-validator`) that compares a **source** and a **target**. Connectors include BigQuery, Teradata, Oracle, SQL Server, PostgreSQL, MySQL, Snowflake, Redshift, Hive, Spanner, files and more (check docs).
- **How it works.** It generates the **same logical query on both sides** (using the Ibis library to write SQL for each engine), runs them, and compares results. Validation types:
    - **schema**: column names and types
    - **column**: aggregates (count, sum, min, max, avg), optionally **grouped** by columns like date
    - **row**: per-row comparison using a **hash** of selected columns or a concat, joined on primary keys
    - **custom-query**: compare results of your own SQL on both sides

    Results can be written to a **BigQuery table**, which is your reconciliation history.

```bash
# Register connections once
data-validation connections add --connection-name pg_src Postgres \
  --host 10.0.0.5 --port 5432 --user validator --password "$PG_PASS" --database pos
data-validation connections add --connection-name bq_tgt BigQuery \
  --project-id prj-ingest-prod

# Counts and sums per day
data-validation validate column \
  -sc pg_src -tc bq_tgt \
  -tbls public.orders=prj-ingest-prod.raw_pos.orders \
  --count '*' --sum net_amount \
  --grouped-columns order_date

# Row hashes on all columns, joined by primary key
data-validation validate row \
  -sc pg_src -tc bq_tgt \
  -tbls public.orders=prj-ingest-prod.raw_pos.orders \
  --primary-keys order_id --hash '*'
```

(Flag names change between versions; check `data-validation --help`.)

- **Why it matters.** It solves the **cross-engine hashing problem** for you, because it normalizes the columns in the same way on both sides before hashing. A common setup: DVT scheduled in Composer, writing results to BigQuery.
- **Common mistake.** Running row-level validation on billions of rows every day. Run column/aggregate validation per partition daily, and row validation only on **samples** or on **partitions that failed**.

### 4.12 Dataproc for lift-and-shift Spark

- **What.** Managed Spark and Hadoop clusters, plus a **serverless** option for Spark batches (check the current product name; it was renamed in 2025).
- **How it works.** Clusters start in about 90 seconds or less, so you use **ephemeral clusters per job** instead of one big always-on cluster. **GCS replaces HDFS** through the Cloud Storage connector (`gs://` paths). The **BigQuery connector** reads/writes BigQuery through the Storage Read/Write APIs.
- **Why it matters.** Self-managed Spark can move to Dataproc with few code changes: **rehost** first, then refactor heavy jobs into BigQuery SQL or Dataflow later. Dataproc is also a good place to run the **compaction** of small files.
- **Common mistake.** Copying the old cluster sizing and leaving it running 24/7. Size per job, use autoscaling, and shut down.

---

## 5. What matters most in practice

### 5.1 Validation in depth

#### The levels

| Level | What you compare | Catches |
|---|---|---|
| 1. **Schema** | Column names, types, nullability (after the agreed type mapping) | Missing columns, wrong types, truncation |
| 2. **Volume** | Row counts **per partition** (day × building, day × store) | Lost or duplicated loads |
| 3. **Aggregates** | SUM/MIN/MAX of key numbers, `COUNT(DISTINCT key)`, NULL counts per column, per partition | Wrong transformations, rounding, unit errors |
| 4. **Row hashes** | A hash of every row, aggregated per partition; drill down on mismatch | Any changed value |
| 5. **Report / business** | The same report from both systems (top 10 reports, month-end) | Logic differences, the things the business actually sees |
| 6. **Non-functional** | Load duration, freshness, cost, failure rate | "Correct but too slow" |

#### Aggregated hashes per partition, with drill-down

The examples in this section use a large, high-volume table, `raw_energy.readings` (one row per sensor reading, partitioned by day), because its size makes the technique necessary. The same pattern works for sales lines per day × store.

Comparing 2.6 billion rows one by one every day is expensive. Instead:

```text
 Step 1: per partition (day × building): count, sum(hash), xor(hash)  → compare ~20 rows/day
            │ all equal → PASS
            │ mismatch in (2026-10-03, building 17)
            ▼
 Step 2: same metrics per (hour × sensor) inside that partition       → find the bad hour/sensor
            ▼
 Step 3: row-level diff only for that hour/sensor (anti-join on key)   → the exact rows
```

A canonical hash in BigQuery:

```sql
-- Target side (BigQuery). The source side must build EXACTLY the same string.
WITH canon AS (
  SELECT
    reading_date,
    building_id,
    TO_HEX(MD5(CONCAT(
      CAST(sensor_id AS STRING), '|',
      FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%E6S', reading_ts, 'UTC'), '|',
      COALESCE(FORMAT('%.4f', reading_value), '<NULL>'), '|',
      COALESCE(TRIM(status), '<NULL>')
    ))) AS row_hash
  FROM `prj-ingest-prod.raw_energy.readings`
  WHERE reading_date BETWEEN @from_date AND @to_date
)
SELECT
  reading_date,
  building_id,
  COUNT(*) AS row_count,
  -- first 8 hex chars = 32-bit number; summing them per partition fits in INT64
  SUM(CAST(CONCAT('0x', SUBSTR(row_hash, 1, 8)) AS INT64)) AS hash_sum,
  BIT_XOR(CAST(CONCAT('0x', SUBSTR(row_hash, 1, 15)) AS INT64)) AS hash_xor
FROM canon
GROUP BY reading_date, building_id;
```

Why each piece:

- **MD5 on a canonical string**, not `FARM_FINGERPRINT`: MD5 exists in almost every engine (Postgres `md5()`, Spark `md5()`, BigQuery `MD5()`), so both sides can compute the **same** value. `FARM_FINGERPRINT` is great but only inside BigQuery.
- **Why both SUM and XOR.** XOR alone has a trap: **two identical rows cancel each other out** (`x XOR x = 0`), so a duplicated row pair is invisible. SUM sees duplicates. Together with `row_count`, they are a strong check.
- **Overflow.** The SUM uses a 32-bit slice so that a partition with up to about two billion rows can't overflow INT64. The XOR can use a bigger slice because XOR never overflows.
- The source side, for example in Postgres: `('x' || substr(md5(s), 1, 8))::bit(32)::bigint`. In Spark: `conv(substr(md5(s), 1, 8), 16, 10)` cast to a number.

Row-level drill-down, when the source extract is staged in GCS as a BigLake/external table:

```sql
-- Rows in source but not in target (swap the sides for the other direction)
SELECT sensor_id, reading_ts, reading_value, status
FROM `prj-ingest-prod.migration_stage.readings_src_ext`
WHERE reading_date = '2026-10-03' AND building_id = 17
EXCEPT DISTINCT
SELECT sensor_id, reading_ts, reading_value, status
FROM `prj-ingest-prod.raw_energy.readings`
WHERE reading_date = '2026-10-03' AND building_id = 17;
```

#### Cross-engine hashing pitfalls

The hash only matches if **both sides build byte-for-byte the same string**. These are the things that break it:

| Pitfall | What goes wrong | Fix in the canonical string |
|---|---|---|
| **Different hash functions** | `FARM_FINGERPRINT` vs. Postgres `hashtext` vs. Spark `hash()` (Murmur3) | Use MD5 or SHA-256 on both sides, output as lowercase hex |
| **NULL handling** | BigQuery `CONCAT` returns NULL if any input is NULL; Postgres `concat()` **skips** NULLs; `||` returns NULL in Postgres | `COALESCE(col, '<NULL>')` with a sentinel, on every nullable column |
| **Empty string vs. NULL** | Oracle treats `''` as NULL | Decide a rule and apply it on both sides |
| **Floating point** | `0.1 + 0.2` printed as `0.30000000000000004` in one engine and `0.3` in another | Round/format to a fixed scale (`FORMAT('%.4f')`), or compare money as `NUMERIC` |
| **Decimal formatting** | `10.50` vs. `10.5`, scientific notation | Fixed scale formatting |
| **Timestamps and timezones** | Local time vs. UTC; `TIMESTAMP` vs. `DATETIME`; DST shifts cause "off by one hour/day" | Convert to UTC, format as ISO with fixed precision |
| **Timestamp precision** | Nanoseconds (some sources) vs. microseconds (BigQuery) | Truncate to microseconds or milliseconds on both sides |
| **String collation and case** | SQL Server is often case-insensitive; BigQuery comparisons are case-sensitive | Don't change data, but know that joins/dedup may behave differently; compare raw bytes |
| **Trailing spaces** | `CHAR(10)` pads with spaces in Teradata/Oracle; target uses `STRING` | `TRIM`/`RTRIM` consistently, or agree to keep padding |
| **Encoding / Unicode** | Latin-1 vs. UTF-8; `ä` composed vs. decomposed | Convert to UTF-8; normalize (NFC) if needed |
| **Booleans and enums** | `1/0` vs. `true/false` vs. `Y/N` | Map to one representation |
| **Column order** | Hashing columns in different orders | Fixed, explicit column list |

#### Report-level validation
Data can match at row level and the report can still differ (a filter, a join, a timezone in the BI layer). Pick the **top N reports** by usage and the **financially important** ones (month-end, quarter-end). Run them on both systems for the same period and compare the numbers within an agreed tolerance. The **business owner** signs off. This is the check the business actually trusts.

#### A reconciliation table

```sql
CREATE TABLE IF NOT EXISTS `prj-ingest-prod.ops_migration.reconciliation` (
  run_id          STRING    NOT NULL,   -- one ID per validation run
  run_ts          TIMESTAMP NOT NULL,
  wave            STRING,               -- e.g. "wave1_pos"
  object_name     STRING    NOT NULL,   -- e.g. "raw_pos.orders"
  partition_key   STRING,               -- e.g. "2026-10-03|store=17"
  check_type      STRING    NOT NULL,   -- schema | count | aggregate | hash | report | freshness
  metric          STRING    NOT NULL,   -- e.g. "row_count", "sum(net_amount)"
  source_value    STRING,
  target_value    STRING,
  abs_diff        NUMERIC,
  tolerance       NUMERIC,
  status          STRING    NOT NULL,   -- PASS | FAIL | ACCEPTED
  accepted_reason STRING,               -- documented known difference
  ticket          STRING                -- link to the issue
)
PARTITION BY DATE(run_ts)
CLUSTER BY object_name, status;
```

And the query behind the exit-criteria dashboard:

```sql
-- How many consecutive clean days does each object have?
WITH daily AS (
  SELECT
    object_name,
    DATE(run_ts) AS run_date,
    LOGICAL_AND(status IN ('PASS', 'ACCEPTED')) AS clean
  FROM `prj-ingest-prod.ops_migration.reconciliation`
  GROUP BY object_name, run_date
),
last_fail AS (
  SELECT object_name, MAX(IF(NOT clean, run_date, NULL)) AS last_bad_day
  FROM daily
  GROUP BY object_name
)
SELECT
  d.object_name,
  COUNTIF(d.run_date > COALESCE(l.last_bad_day, DATE '1970-01-01')) AS consecutive_clean_days
FROM daily AS d
JOIN last_fail AS l USING (object_name)
GROUP BY d.object_name
ORDER BY consecutive_clean_days;
```

#### Automating daily checks during the parallel run
- A Composer DAG runs every morning after both systems finished their loads: DVT (or the SQL above) for yesterday's partitions **plus a rolling window** (late data can change older partitions).
- Results go to the reconciliation table; FAILs open an alert to the migration channel.
- Every difference is either **fixed** or **ACCEPTED** with a reason (e.g. "the old system rounded to 2 decimals; agreed with finance"). Nothing is silently ignored.
- The dashboard shows consecutive clean days per object, which **is** the exit criterion.

### 5.2 Cutover in depth

#### Patterns

| Pattern | How it works | Risk | Use it for |
|---|---|---|---|
| **Big bang** | Everything switches at once on one date | High; rollback is hard | Small, simple systems |
| **Phased (waves)** | Switch domain by domain or source by source | Medium | Most migrations |
| **Parallel run** | Both systems run on the same input; compare; then switch | Low; costs double for a while | Critical and financial data |
| **Strangler fig** | New pieces take over one by one; the old system shrinks until it's gone | Low | Pipeline and DAG modernization |
| **Blue-green (data)** | Build the new table/dataset next to the old one; consumers read a **view**; swap the view to switch | Low; rollback = swap back | Table rebuilds, model changes, ingestion moves |

They combine: e.g. **waves**, each with a **parallel run**, switched with **blue-green views**.

#### Blue-green with views

```sql
-- Consumers always read the stable name:
--   prj-dwh-sales-prod.mart_sales.fct_sales  (a VIEW)

-- Today it points to blue:
CREATE OR REPLACE VIEW `prj-dwh-sales-prod.mart_sales.fct_sales` AS
SELECT * FROM `prj-dwh-sales-prod.mart_sales_blue.fct_sales`;

-- Cutover: point to green (one metadata change, effective immediately)
CREATE OR REPLACE VIEW `prj-dwh-sales-prod.mart_sales.fct_sales` AS
SELECT * FROM `prj-dwh-sales-prod.mart_sales_green.fct_sales`;

-- Rollback: run the first statement again.
```

Things to check: the view's **column list and types** must be identical (consumers may break on a new column order with `SELECT *` in their code); **IAM**: consumers need access to the green dataset, or the view must be **authorized** on it; **location**: both datasets in the same location.

#### The cutover runbook

1. **Exit criteria met** (from the dashboard) and **sign-off** recorded.
2. **Freeze**: no schema or logic changes on the old side from a few days before cutover. Otherwise you validate a moving target.
3. **Fixed boundary**: pick T0 (timestamp, LSN, offset). Final sync of deltas up to T0 + overlap. Verify the counts around the boundary.
4. **Switch writers** first (the new pipeline becomes the only writer), then **switch readers** (views, Looker connections, exports, API configs).
5. **Hypercare**: watch freshness, counts, errors and user tickets closely for the first days; the team is on call.
6. **Rollback plan**, written and **tested** before cutover: what triggers it (e.g. "a P1 data issue not fixable in 4 hours"), who decides, the steps (swap views back, re-enable old pipelines), and how long the old system stays **read-only and in sync** (e.g. 2–4 weeks, covering one month-end).
7. **Decommission** after the rollback window: stop old pipelines, revoke old credentials and IAM, archive what compliance needs, delete the rest, cancel licenses. **This is when the savings become real.**
8. **Communication**: a calendar date announced early, what changes for users (usually nothing, thanks to views), a contact channel, and a "we're live" message.

### 5.3 Modernizing within GCP: consolidating ingestion into a shared project

This is the core of most within-GCP modernizations. The steps, in detail:

**Step 1. Map the current state.**

- List every ingestion pipeline: where it runs (project, Composer environment, scheduled query, Cloud Function), which source, which target table, which credentials.
- Find **who reads each raw table**, across the organization:

```sql
SELECT
  rt.project_id || '.' || rt.dataset_id || '.' || rt.table_id AS raw_table,
  j.project_id    AS job_project,     -- where the reading job ran
  j.user_email,                       -- human or SA (Looker, Dataform, DAGs)
  COUNT(*)        AS jobs_90d,
  MAX(j.creation_time) AS last_read
FROM `region-eu`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION AS j,
     UNNEST(j.referenced_tables) AS rt
WHERE j.creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
  AND rt.dataset_id IN ('sales_raw', 'fin_raw', 'ecom_raw')
GROUP BY 1, 2, 3
ORDER BY raw_table, jobs_90d DESC;
```

- Add consumers that don't show up as BigQuery jobs in your region: exports from other locations, external tools using the Storage Read API (check `INFORMATION_SCHEMA` and audit logs), Looker content, data shared via Analytics Hub.
- Find **duplicates**: the same source ingested twice by two teams. These are the big wins.

**Step 2. Build the new ingestion project with Terraform** (see [The GCP platform: IAM and Terraform](platform-iam-terraform.md)): one `ingestion_source` module call per source, **same location** as the existing warehouse (if today's datasets are in `EU`, the new ones must be in `EU`, otherwise views and joins won't work).

**Step 3. Run the new ingestion in parallel**, writing to `prj-ingest-prod.raw_<source>`. Backfill history with **table copy jobs** if the old raw table is good, or with a fresh load from the source.

| Need | How |
|---|---|
| Copy a table to another project, same location | Copy job: `bq cp` or `CREATE TABLE ... COPY`; no query cost |
| Cheap writable copy for testing | `CREATE TABLE ... CLONE` (you pay only for changed data) |
| Read-only point-in-time copy | `CREATE SNAPSHOT TABLE` |
| Whole dataset, also cross-region | Dataset copy (DTS) |
| Change a dataset's location | Not possible in place: copy to a new dataset in the new location |

**Step 4. Reconcile** old raw vs. new raw daily (5.1). Both are in BigQuery, so this is simple SQL: counts and hashes per partition, then `EXCEPT DISTINCT` drill-down. Expect real differences: the old pipeline may have had bugs, different dedup, or different timezone handling. Decide and document each one.

**Step 5. Switch consumers with compatibility views.** The old table name becomes a view on the new table, so consumer SQL keeps working:

```sql
-- 1. Stop the old writer (it must not write any more).
-- 2. Keep the old table under another name, for rollback:
ALTER TABLE `prj-sales.sales_raw.orders` RENAME TO orders_legacy;

-- 3. Create the compatibility view with the old name and old column shape:
CREATE VIEW `prj-sales.sales_raw.orders`
OPTIONS (description = "DEPRECATED: compatibility view. Use prj-ingest-prod.raw_pos.orders. Removal planned 2027-03-31.")
AS
SELECT
  order_id,
  store_id,
  order_ts,
  net_amount,
  _ingested_at AS load_ts          -- keep old column names if they changed
FROM `prj-ingest-prod.raw_pos.orders`;
```

Things that bite you here:

- **You can't replace a table with a view in one step**; rename or drop the table first (as above).
- **IAM**: a normal view is evaluated with the **querying user's** permissions on the underlying table. So either consumers get `dataViewer` on `raw_pos`, or you make the view an **authorized view** on the `raw_pos` dataset.
- **Location**: the view and the table must be in the same location.
- **Views can't be written to**, don't support partition decorators (`orders$20261001`) and behave differently with some features (time travel, streaming inserts, legacy SQL). Check every consumer type in your lineage list.
- **Performance**: consumers that filtered on the partition column must still filter on it through the view; check that the new table is partitioned/clustered the same way (or better).

**Step 6. Migrate consumers to the new name, then deprecate.** Track who still uses the compatibility views (the lineage query above, filtered on the view names). Contact the owners, give a date, and remove the view when usage is zero. Then **delete** `orders_legacy` and the old pipeline after the rollback window.

**Step 7. Measure** (5.5).

### 5.4 Simplifying load chains

#### Remove hops

```text
 BEFORE:  source → GCS → raw_a.orders → copy → sales_raw.orders → copy → stg_orders_tmp → stg_orders
 AFTER:   source → raw_pos.orders (CDC) → stg_sales.orders (incremental Dataform model)
```

Each hop adds latency, storage, cost, a failure point, and a schedule to coordinate. Ask for each hop: *what does it add?* If the answer is "nothing", remove it.

#### Incremental instead of full reload
A full reload of a 2 TB table every night scans and rewrites 2 TB. An incremental load touches only the changed partitions:

```sql
MERGE `prj-dwh-sales-prod.stg_sales.orders` AS t
USING (
  SELECT * EXCEPT (rn)
  FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC) AS rn
    FROM `prj-ingest-prod.raw_pos.orders`
    WHERE updated_at >= TIMESTAMP_SUB(@last_watermark, INTERVAL 1 HOUR)  -- overlap for late data
  )
  WHERE rn = 1
) AS s
ON t.order_id = s.order_id
   AND t.order_date >= @min_changed_date      -- prune target partitions
WHEN MATCHED AND s.updated_at > t.updated_at THEN
  UPDATE SET store_id = s.store_id, net_amount = s.net_amount,
             updated_at = s.updated_at, order_date = s.order_date
WHEN NOT MATCHED THEN
  INSERT (order_id, store_id, net_amount, updated_at, order_date)
  VALUES (s.order_id, s.store_id, s.net_amount, s.updated_at, s.order_date);
```

(`@min_changed_date` is computed first from the source batch, so the MERGE only scans the target partitions that can change. The `updated_at > t.updated_at` condition makes reruns idempotent.)

#### De-shard date-sharded tables
Old BigQuery designs used one table per day: `events_20261001`, `events_20261002`, … Problems: thousands of tables, schema per table, metadata overhead, wildcard queries are slower, and you can't cluster across shards. Convert to **one partitioned table**:

```sql
CREATE TABLE `prj-ingest-prod.raw_web.events_partitioned`
PARTITION BY event_date
CLUSTER BY customer_id
AS
SELECT
  PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) AS event_date,
  *
FROM `prj-ingest-prod.raw_web.events_*`
WHERE _TABLE_SUFFIX BETWEEN '20250101' AND '20251231';   -- one year per statement
```

Details that matter:

- A single job can modify only a limited number of partitions, and a table has a maximum number of partitions (check current limits). For many years of daily shards, **load in batches** (e.g. one year per statement, then `INSERT ... SELECT` the next years).
- **Name trap**: the wildcard `events_*` matches every table that starts with `events_`. If you name the new table `events_partitioned`, later wildcard queries will **include it**. Use a different prefix (e.g. `web_events`) or a different dataset.
- Consumers using `events_*` with `_TABLE_SUFFIX` filters must be rewritten to filter on `event_date`. Find them with the lineage query.

#### Move scattered scheduled queries into Dataform (or dbt)
Scheduled queries spread across projects have no version control, no dependency graph, no tests, and run on fixed times ("run at 03:00 and hope the load finished"). In Dataform:

- each model is a `.sqlx` file in git, with `ref()` to its inputs, so Dataform builds the **dependency graph** and runs independent models **in parallel**;
- **assertions** (not null, unique, row conditions) run as part of the build;
- **incremental** models generate MERGE logic;
- Composer triggers Dataform after the ingestion for that source has finished (event or sensor), not at a fixed time.

#### Critical-path analysis
The total load time is set by the **longest dependent chain**, not by the sum of all tasks. Speed up the critical path; optimizing anything else doesn't change the finish time.

```text
 raw_pos (25m) ─► stg_orders (40m) ─► fct_sales (35m) ─► agg_daily (10m)   = 110 min ◄ critical path
 raw_erp (15m) ─► stg_products (5m) ──┘
 raw_ecom (20m) ─► stg_web (10m) ─────────────────────► agg_web (5m)        =  35 min
```

Here, making `stg_products` faster gains nothing. Making `stg_orders` incremental (40 → 8 min) cuts the whole chain by 32 minutes. Get the durations from Airflow task history or from BigQuery jobs with labels:

```sql
SELECT
  (SELECT value FROM UNNEST(labels) WHERE key = 'dag_id')  AS dag_id,
  (SELECT value FROM UNNEST(labels) WHERE key = 'task_id') AS task_id,
  DATE(creation_time) AS run_date,
  TIMESTAMP_DIFF(end_time, start_time, SECOND) AS duration_s,
  total_bytes_billed,
  total_slot_ms
FROM `region-eu`.INFORMATION_SCHEMA.JOBS
WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
  AND EXISTS (SELECT 1 FROM UNNEST(labels) WHERE key = 'dag_id')
ORDER BY run_date, dag_id, task_id;
```

### 5.5 Measuring the improvement

A modernization should define and track metrics for load duration, throughput, stability and cost. Define them **before** you change anything (the baseline from assess), and report them **after** each wave.

| Metric | Definition | Source |
|---|---|---|
| **Load duration** | End-to-end time of the nightly chain (first task start → last mart ready); per source | Airflow task history, job labels |
| **Freshness** | `now() - max(event_time)` per mart, compared with the SLA | A freshness check table in `ops_ingestion` |
| **Throughput** | Rows or GB loaded per hour / per run | Load audit table, job stats |
| **Stability** | Failed runs / total runs; reruns; incidents per month; MTTR | Airflow metadata, incident log |
| **Cost** | Bytes billed and slot-hours per pipeline per day; storage; Dataflow/Composer cost | `INFORMATION_SCHEMA.JOBS` + billing export with labels |
| **Simplicity** | Number of pipelines, hops, scheduled queries, duplicate ingestions | Inventory |

A clear result statement looks like: *"Wave 1 removed 3 duplicate ingestions and 2 hops; the sales chain went from 110 to 62 minutes, failures from 6 to 1 per month, and query cost for the chain dropped by about 40%."* (Use real numbers when you have them.)

### 5.6 Risks and mitigations

| Risk | Why it happens | Mitigation |
|---|---|---|
| **Schema agreement takes forever** | Many stakeholders, no decision owner | Time-boxed workshops, a decision log, a named business owner per domain, start with wave 1 tables |
| **Data quality surprises** | Old data has years of edge cases | Profile early (assessment), quarantine tables for bad rows, agreed rules, flag instead of drop |
| **Hidden consumers** | Scripts, exports, BI tools nobody documented | Lineage from 90–180 days of query logs + audit logs **before** switching; compatibility views; announce dates |
| **SQL dialect differences** | Translation is syntactic | BigQuery Migration Service + validation per object; a list of known semantic traps |
| **Egress cost** | Leaving AWS/Azure costs per GB | Compact and compress first, one bulk transfer, consider Transfer Appliance for huge volumes |
| **Cost of the parallel run** | Two systems at once | Budget it upfront, time-box it (exit criteria), run only what is needed in parallel |
| **Timezone / precision mismatches** | Different defaults | UTC in raw, NUMERIC for money, canonical formatting in validation |
| **Location mismatch (within GCP)** | New datasets in a different location | Location fixed in the Terraform module default + org policy |
| **IAM breaks on switch** | Views need access to the new dataset | Authorized views or explicit grants; test with consumer identities in test env |
| **People / handover** | The internal team can't run it after the project team leaves | Pair from wave 1, they lead wave 2+, runbooks, docs, module READMEs |

---

## 6. Mapping from AWS and Fabric

| AWS / Fabric concept | What it is | GCP equivalent |
|---|---|---|
| S3 buckets with millions of small files | Object storage | **GCS buckets** (strongly consistent, same small-file problem) |
| Spark compaction (self-managed) | Batch processing | **Dataproc** (clusters or serverless Spark) |
| S3 → OneLake copy | Bulk transfer | **Storage Transfer Service** (S3 → GCS) |
| AWS DataSync (on-prem → S3) | Agent-based transfer | **STS with transfer agents** |
| AWS Snowball | Physical transfer | **Transfer Appliance** |
| Kinesis Data Streams / Kafka | Streaming | **Pub/Sub** (or Managed Service for Apache Kafka) |
| Kinesis Firehose → S3 | Managed delivery | **Pub/Sub → BigQuery subscription** or **Dataflow** |
| AWS DMS (CDC) | Database replication | **Datastream** |
| AWS SCT (schema conversion) | SQL translation | **BigQuery Migration Service** (batch/interactive translator) |
| Fabric bronze / silver / gold | Medallion layers | `raw_*` / `stg_*` / `mart_*` datasets |
| Fabric Data Pipelines / notebooks | Orchestration + transforms | **Composer** + **Dataform/dbt** + BigQuery SQL |
| Fabric shortcuts | Query data in place | **BigLake / external tables** |
| Fabric workspaces per domain | Isolation | **Projects / datasets** per domain |
| Parallel run with separate Kafka consumer groups | Parallel run | Same idea: a separate subscription or consumer group for the new pipeline |
| Reconciliation (counts, hashes per partition) | Validation | **DVT** or reconciliation SQL + `ops_migration.reconciliation` table |

---

## 7. Glossary

| Term | Meaning |
|---|---|
| **Agent pool (STS)** | A group of transfer agents (containers) for on-prem or HDFS transfers |
| **Assessment** | Fact-finding phase: inventory, usage, lineage, complexity, baselines |
| **Authorized view** | A view allowed to read a dataset its users can't read directly |
| **Baseline** | Metrics measured before the change, to prove improvement later |
| **BigLake table** | An external table accessed through a connection, with fine-grained security and caching |
| **BigQuery Migration Service** | Assessment and SQL translation tools for warehouse migrations |
| **Blue-green** | Build new next to old; switch by changing a pointer (a view) |
| **Boundary (T0)** | The timestamp/offset where ownership moves from history to deltas |
| **Bulk / history load** | The one-time copy of existing data |
| **CDC** | Change data capture: streaming inserts/updates/deletes from a database log |
| **Clone / snapshot** | Cheap writable copy / read-only point-in-time copy of a BigQuery table |
| **Compaction** | Rewriting many small files into fewer large files |
| **Compatibility view** | A view with the old name and shape that points to the new table |
| **Critical path** | The longest chain of dependent tasks; it sets the total runtime |
| **Cutover** | The moment consumers and writers switch to the new system |
| **Data Transfer Service (DTS)** | Managed scheduled loads into BigQuery (SaaS, S3, Azure, Redshift, Teradata, dataset copy) |
| **De-sharding** | Turning `table_YYYYMMDD` shards into one partitioned table |
| **Decommission** | Removing the old system after the rollback window |
| **Delta / incremental** | Only the data that changed since the last load |
| **DVT** | Data Validation Tool: open-source source-vs-target comparison CLI |
| **Egress** | Data leaving a cloud or region; usually charged per GB |
| **Exit criteria** | Measurable conditions that must be met before cutover |
| **Freeze** | A period with no changes to the old system before cutover |
| **Hypercare** | Intensive monitoring and support right after cutover |
| **Idempotent** | Running it again gives the same result (no duplicates) |
| **Lineage** | Which data flows into which tables and reports |
| **Load job** | A BigQuery job that loads files into a table (free on the shared pool) |
| **Parallel run** | Old and new systems running at the same time on the same input |
| **Reconciliation** | Comparing source and target metrics and recording the results |
| **Refactor / replatform / rehost / retire** | Migration strategies (see section 1) |
| **Rollback** | Returning to the old system if the new one fails |
| **Storage Transfer Service (STS)** | Managed bulk and incremental object copy into GCS |
| **Strangler fig** | Replacing a system piece by piece until the old one is gone |
| **Transfer Appliance** | A physical device for offline transfer of very large data |
| **Wave** | A group of objects migrated and switched together |
