# Data migration and EDW modernization

> Two kinds of work: migrating *into* GCP, and **EDW modernization within GCP** (moving the load layer to shared projects, simplifying chains). Always cover **validation, cutover and rollback**, with measurable numbers.

---

## 1. The migration framework

```
 1. ASSESS → 2. DESIGN → 3. BUILD & MIGRATE → 4. VALIDATE → 5. CUT OVER → 6. OPTIMIZE & HAND OVER
     ▲                                                                          │
     └────────────────────────── repeat per WAVE (domain / source) ─────────────┘
```

### 1. Assess

- **Inventory:** sources, tables, volumes (TB, rows/day), pipelines, schedules, **consumers** (reports, APIs, exports), SLAs, owners.
- **Dependencies:** lineage graph (which pipelines feed which reports).
- **Usage:** which tables are actually used? (Query logs, `INFORMATION_SCHEMA.JOBS`, last access.) **Don't migrate dead tables.** Often 20–40% are unused.
- **Complexity scoring:** SQL dialect features, stored procedures, UDFs, data quality, PII.
- **Baseline metrics:** current runtime, cost, failure rate. You need them to prove improvement later.
- **Tools:** the **BigQuery Migration Assessment** (for Teradata, Redshift, Snowflake, Oracle and similar) analyses logs and metadata and estimates effort.

### 2. Design

- **Target architecture:** layers (raw/stg/mart), projects, datasets, naming, IAM, IaC.
- **Approach per workload** (the "R's"):
    | Approach | Meaning | When |
    |---|---|---|
    | **Rehost / lift-and-shift** | Move as-is (for example Spark on Dataproc) | Speed matters, low risk tolerance |
    | **Replatform** | Small changes to fit the target (SQL translated to BigQuery) | Most warehouse migrations |
    | **Refactor / re-architect** | Redesign (new model, CDC instead of batch) | Where the old design is the problem |
    | **Retire** | Don't migrate | Unused tables and reports |
- **Wave plan:** group by domain or source. **Wave 1 = high value, low complexity**, to prove the pattern and build trust.
- **Validation strategy and exit criteria**, agreed **upfront** with the business.

### 3. Build and migrate

- **Historical data (bulk):** one-time copy (see the tools table)
- **Ongoing data (delta):** CDC (Datastream) or incremental batch, so the target stays in sync during the parallel run
- **Code:** SQL translation, pipeline rebuild, DAG factory
- **Infrastructure as code** from day 1

### 4. Validate
See section 4. This is the step most often underestimated.

### 5. Cut over
See section 5.

### 6. Optimize and hand over

- Tune partitioning, clustering, cost guardrails
- Decommission old systems (licenses, storage) to **realize the savings**
- Documentation, runbooks, training, handover to internal teams

---

## 2. GCP migration tools

| Need | Tool |
|---|---|
| Files from S3 / Azure Blob / HTTP / on-prem → GCS | **Storage Transfer Service** (managed, scheduled, incremental, checksums) |
| Petabytes, poor network | **Transfer Appliance** (physical device) |
| SaaS or warehouse → BigQuery (Redshift, Teradata, S3, Azure, Google Ads, …) | **BigQuery Data Transfer Service** |
| Database CDC → BigQuery | **Datastream** |
| SQL dialect translation (Teradata, Redshift, Snowflake, Oracle, Hive, Spark SQL → BigQuery) | **BigQuery Migration Service**: batch and interactive **SQL translator** |
| Assess an existing warehouse | **BigQuery Migration Assessment** |
| Validate source vs. target | **Data Validation Tool (DVT)**: open source, compares counts, aggregates, row hashes, schema |
| Hadoop / Spark jobs | **Dataproc** (lift-and-shift), later refactor to BigQuery/Dataflow |
| Large file loads into BigQuery | **Load jobs** from GCS (Parquet/Avro preferred: schema included, splittable, compressed). Free |
| Copy datasets between projects or regions | **BigQuery dataset copy** (Data Transfer Service) / table copy jobs |

### File format choice for bulk loads

| Format | Verdict |
|---|---|
| **Parquet / Avro** | ✅ Best: schema, types, compression, fast parallel load |
| ORC | ✅ Fine |
| CSV | ⚠️ No types, escaping issues, larger. Only if forced |
| JSON (newline-delimited) | ⚠️ OK for semi-structured, but slower and larger |

---

## 3. Migrating INTO GCP: S3 → GCS → BigQuery

```
S3 (scattered small files)
  │ 1) compact first (Spark/Dataproc or in place) → fewer, larger Parquet files (128 MB–1 GB)
  │ 2) Storage Transfer Service S3 → GCS (one-time bulk + incremental sync until cutover)
  ▼
GCS landing (raw, as-is, lineage preserved)
  │ 3) BigQuery load jobs (free) or BigLake/external tables to query in place first
  ▼
BigQuery raw_* → stg_* → mart_*   (partitioned + clustered)
```

Lessons learned:

- **Small files hurt twice:** S3 LIST and GET request costs plus slow reads. **Compact before moving.**
- **Egress cost:** moving out of AWS costs per GB. Plan a **single** bulk transfer instead of repeated reads.
- **Keep lineage** (source path, load time) as columns.
- External tables / **BigLake** let you query GCS data before fully loading it, which is useful for validating early.

---

## 4. Validation

### Levels

| Level | Check | Example |
|---|---|---|
| **1. Schema** | Columns, types, nullability match (after agreed mapping) | DVT schema validation |
| **2. Volume** | Row counts per table **per partition** (day, building, store) | `COUNT(*) GROUP BY date` on both sides |
| **3. Aggregates** | SUM/MIN/MAX/AVG of key numeric columns, count distinct of keys, per partition | Net sales per store per day |
| **4. Row-level hash** | Hash of each row (or of the concatenated columns), aggregated per partition; drill down only on mismatches | `BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(t)))` |
| **5. Business / report** | The same report from old and new systems matches within tolerance | Monthly sales report, signed off by the business |
| **6. Non-functional** | Runtime, freshness, cost meet targets | Nightly load ≤ 2h, freshness ≤ 15 min |

### Practicalities

- **Make it automated and repeatable.** Validation runs **daily during the parallel run**, not once.
- **Store results in a reconciliation table** (`table, partition, metric, source_value, target_value, diff, status`) and build a dashboard. Sign-off is based on it.
- **Known, accepted differences** (timezone handling, rounding, fixed bugs) are **documented** and agreed, not hidden.
- **Hashing across systems:** functions differ (MD5 in Postgres vs. `FARM_FINGERPRINT` in BigQuery). Use the **same algorithm** on both sides (e.g. MD5 on a canonical string), or compare aggregates. DVT handles this for you.
- **Floating point:** compare with a tolerance, or cast to NUMERIC.
- **Timezones:** a classic source of off-by-one-day mismatches. Normalize to UTC in raw, convert in marts.

```sql
-- Reconciliation in BigQuery (target side), the same logic run on the source side
SELECT DATE(order_ts) AS d, store_id,
       COUNT(*) AS rows_cnt,
       SUM(net_amount) AS net_sum,
       COUNT(DISTINCT order_id) AS orders,
       BIT_XOR(FARM_FINGERPRINT(CONCAT(order_id,'|',CAST(line_no AS STRING),'|',CAST(net_amount AS STRING)))) AS fp
FROM mart_sales.fct_sales_line
WHERE DATE(order_ts) BETWEEN @from AND @to
GROUP BY 1, 2;
```

---

## 5. Cutover and rollback

### Patterns

| Pattern | How | Risk | Use |
|---|---|---|---|
| **Big bang** | Switch everything at once | High | Small systems only |
| **Phased / waves** | Switch domain by domain | Medium | Most migrations |
| **Parallel run** | Old and new both run; compare; then switch | Low (costs double for a while) | Critical data, finance |
| **Strangler fig** | New system takes over piece by piece; the old one shrinks | Low | Pipeline/DAG modernization |
| **Blue-green (data)** | Build the new table/dataset next to the old one, swap a **view** or pointer | Low, instant rollback | Table-level rebuilds, model changes |

### Cutover checklist

1. **Exit criteria met:** e.g. **N consecutive days** with zero unexplained reconciliation differences, report sign-off, performance and freshness SLAs met.
2. **Freeze window:** no schema or logic changes on the old side during cutover.
3. **Final sync:** CDC/incremental catches up to a **fixed boundary** (timestamp or log position). Ownership is clear: before it = history, after it = new pipelines.
4. **Switch consumers:** point Looker connections, views and downstream exports at the new tables. **Views as an abstraction layer** make this a metadata change.
5. **Monitor intensively** for the first days (freshness, counts, errors).
6. **Rollback plan:** keep the old system **read-only** and in sync (or restorable) for an agreed period. Switching the views back = rollback.
7. **Decommission** after the rollback window: stop pipelines, archive, delete, cancel licenses.
8. **Communicate:** stakeholders know the date, what changes, and who to contact.

### Example: parallel run for a streaming platform

A 3-month parallel run covering month-end and quarter-end. Both platforms consume the same Kafka topics through separate consumer groups. Writes are idempotent MERGEs on (sensor_id, timestamp). Daily counts and aggregated hashes per site are compared, plus report comparisons. Exit criteria are agreed upfront. Cutover happens at a fixed timestamp boundary, and the old platform stays read-only for a few weeks as rollback.

---

## 6. Migrating WITHIN GCP: consolidating ingestion

**"Move the load layer into shared GCP ingestion projects."** Ingestion is probably spread across domain projects today. The task is to consolidate it **without breaking downstream** consumers.

### Step by step

1. **Map current state:** every ingestion pipeline, its project, its target tables, and **who reads those tables** (`INFORMATION_SCHEMA.JOBS` with `referenced_tables`, Looker usage, scheduled queries).
2. **Build the new ingestion project** with Terraform (see [GCP platform, §5](platform-iam-terraform.md)) and the module per source.
3. **Run the new ingestion in parallel**, writing to `prj-ingest-prod.raw_<source>`.
4. **Reconcile** the old raw tables against the new ones (counts, hashes per partition).
5. **Switch consumers via views:** replace the old raw table with a **view pointing to the new location**, so consumers don't change their SQL, then migrate them to the new name over time.
    ```sql
    -- in the old location, after validation:
    CREATE OR REPLACE VIEW old_project.old_raw.orders AS
    SELECT * FROM `prj-ingest-prod.raw_pos.orders`;
    ```
    (Mind **location**: cross-region queries aren't allowed, so both must be in the same location, e.g. `EU`.)
6. **Decommission** old pipelines and tables after a grace period. Track usage of the compatibility views and remove them when unused.
7. **Measure:** fewer pipelines, lower cost, faster load chain, better stability. These are the modernization metrics.

### Moving or copying data between projects or regions

| Need | How |
|---|---|
| Same region, another project | Table **copy jobs** (free, fast) or `CREATE TABLE ... COPY` / `CLONE` |
| Whole dataset | BigQuery **dataset copy** (Data Transfer Service) |
| Another region | Dataset copy across regions (incurs network cost), or export → GCS → load |
| Change a dataset's location | Not possible in place, so copy to a new dataset |

### Simplifying load chains

- Remove **redundant hops** (raw → copy → copy → staging) and land once.
- Replace **full reloads** with incremental or partition-overwrite loads.
- Replace **sharded tables** (`events_YYYYMMDD`) with partitioned tables.
- Replace **sequential DAG chains** with dependency-driven ones (Datasets + dbt/Dataform `ref()`), so independent parts run in parallel.
- Replace **scheduled queries scattered across projects** with version-controlled Dataform/dbt.
- **Measure the critical path before and after.**

---

## 7. Common migration risks and mitigations

| Risk | Mitigation |
|---|---|
| Schema agreement drags on | Time-boxed design workshops, start with wave 1 tables, a decision log, a business owner per domain |
| Data quality surprises in old data | Profile early (assessment), a quarantine table for bad rows, agreed rules for each issue |
| Hidden consumers break | Lineage from query logs **before** switching; views as a compatibility layer |
| SQL dialect differences | Automated translation + tests; watch for NULL handling, implicit casts, date functions, case sensitivity |
| Cost spike during parallel run | Budget for it upfront, a time-boxed parallel run, cost labels |
| Egress cost | Compress, compact, one bulk transfer, Transfer Appliance if huge |
| Timezone and precision mismatches | UTC in raw, NUMERIC for money, tolerance in reconciliation |
| People: the team can't run it after handover | Pairing, docs, runbooks, the team owns wave 2+ with review support |

---
