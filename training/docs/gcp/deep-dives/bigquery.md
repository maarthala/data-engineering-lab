# BigQuery deep dive

> How BigQuery works, what drives runtime and cost, and how to design and tune tables and queries for a near real-time sales model.

---

## 1. Architecture: how BigQuery actually works

```
         ┌──────────── Your SQL ────────────┐
         ▼                                  │
   ┌───────────┐   query plan (stages)      │
   │  Dremel   │ ───────────────────────────┘
   │ (compute) │  thousands of "slots" (virtual CPUs) run stages in parallel
   └─────┬─────┘
         │  Jupiter network (petabit) + in-memory SHUFFLE between stages
   ┌─────▼─────┐
   │ Colossus  │  distributed storage; data stored in COLUMNAR format (Capacitor)
   │ (storage) │  compressed, replicated, encrypted
   └───────────┘
```

Key consequences:

1. **Storage and compute are separate.** You pay for each independently. Storage is cheap and compute is where cost hides.
2. **Columnar storage.** A query reads **only the columns it references**, so `SELECT *` = read everything = pay for everything.
3. **`LIMIT` does not reduce bytes scanned** (on regular tables). The scan happens, then the result is trimmed.
4. **Slots** are units of compute. A query is split into **stages**, and data is **shuffled** between stages, as in Spark. Big joins and `GROUP BY`s cause big shuffles.
5. **No indexes in the classic sense.** Performance comes from **partition pruning**, **cluster block pruning**, and fewer bytes and shuffles. (Search indexes exist, but for text and point lookups.)
6. **Serverless.** There are no clusters to size. You control cost through **what you scan** and **which pricing model** you use.

### Spark mental model mapping (for Spark / Fabric users)

| Spark | BigQuery |
|---|---|
| Executor cores | Slots |
| Stages + shuffle | Query stages + shuffle |
| Partition pruning on Parquet | Partition pruning |
| Z-order / sort within files | Clustering |
| Small files problem | Handled internally; BigQuery compacts storage itself |
| `cache()` | Query results cache (24h), materialized views, BI Engine |

---

## 2. Pricing

### Compute

| Model | You pay for | Good when | Optimize by |
|---|---|---|---|
| **On-demand** | **Bytes scanned** (per TiB; first 1 TiB per month free) | Spiky, low or unpredictable usage | Scan fewer bytes: partitions, clusters, fewer columns |
| **Capacity (Editions: Standard, Enterprise, Enterprise Plus)** | **Slot-hours** (reservations with baseline + autoscaling) | Steady, heavy workloads; predictable bills | Use fewer slot-ms: less shuffle, less recomputation, better scheduling |

- With Editions, a query that scans 10 TB but finishes fast can be cheap, while an inefficient join that runs for an hour is expensive. **The metric changes from bytes to slot-time.**
- Reservations are assigned to projects, folders or the org. You can separate **ETL** and **BI** reservations so dashboards aren't starved by batch loads.
- Rule of thumb: first check whether the project is on on-demand or Editions, because the fix is different. On-demand means reduce bytes. Editions means reduce slot-time and contention.

### Storage

| Type | Detail |
|---|---|
| **Active** | Table or partition modified in the last 90 days |
| **Long-term** | **Not modified for 90 consecutive days**: about 50% cheaper automatically, same performance |
| **Logical billing** | Billed on uncompressed bytes (default historically) |
| **Physical billing** | Billed on **compressed** bytes **including time travel and fail-safe**. Often much cheaper for compressible data. Set per dataset. |

Tip: partitioning by date also helps storage cost, because old partitions that aren't touched become **long-term storage**, while the whole table would stay "active" if any part changed and there were no partitions.

### Free operations

- **Batch load jobs** (from GCS) are free, using a shared slot pool
- **Copy jobs**, exports, and metadata operations are mostly free
- **Cached query results** are free

### Ingestion cost

- **Storage Write API**: recommended for streaming, cheaper than legacy `insertAll` and supports exactly-once
- Legacy streaming inserts: older, priced per GB

---

## 3. Partitioning

**What:** a table is physically split into segments. A filter on the partition column lets BigQuery **skip entire partitions**.

| Type | Example | Use for |
|---|---|---|
| **Time-unit column** | `PARTITION BY DATE(order_ts)` (hour/day/month/year) | Most fact tables |
| **Ingestion time** | `PARTITION BY _PARTITIONDATE` | Raw landing tables without a reliable event time |
| **Integer range** | `PARTITION BY RANGE_BUCKET(customer_id, GENERATE_ARRAY(0, 1000000, 10000))` | Non-time data |

Rules and limits:

- **Max 10,000 partitions per table.** Daily partitions = about 27 years. Hourly partitions run out fast, so use hourly only with expiration.
- Aim for partitions that aren't tiny. If daily partitions hold only a few MB each, partition by **month** and rely on clustering.
- `require_partition_filter = TRUE` makes **unfiltered queries fail**, which is a cheap guardrail.
- `partition_expiration_days` drops old partitions automatically (staging and raw tables).
- You can **overwrite a single partition** with the `table$20261023` decorator or `WRITE_TRUNCATE` on a partition, which is the key to **idempotent** loads.

### When pruning does NOT work (classic follow-up question)

```sql
-- ✅ prunes
WHERE order_date BETWEEN '2026-10-01' AND '2026-10-23'

-- ❌ may not prune: filter on a different column
WHERE created_at > '2026-10-01'     -- table is partitioned by order_date

-- ❌ may not prune: partition column hidden behind a function or cast on the wrong side
WHERE FORMAT_DATE('%Y%m', order_date) = '202610'

-- ⚠️ subquery-driven filter: not always pruned reliably
WHERE order_date >= (SELECT MAX(load_date) FROM ctl.watermark)

-- ✅ fix: compute the constant first (scripting)
DECLARE since DATE DEFAULT (SELECT MAX(load_date) FROM ctl.watermark);
SELECT ... WHERE order_date >= since;
```

Other causes: a **view** that doesn't pass the filter down in the way expected, a **join** where the filter is applied only to the other table, or a Looker explore without a mandatory date filter (`always_filter`).

**How to check:** run a **dry run** (`--dry_run` or the editor estimate) and compare the "bytes processed" estimate with and without the filter.

---

## 4. Clustering

**What:** within each partition, data is **sorted by up to 4 columns** and stored in blocks with min/max metadata. Filters on those columns **skip blocks**.

```sql
CREATE TABLE sales.fct_order_line (
  order_id STRING, line_no INT64, order_ts TIMESTAMP,
  store_id STRING, product_id STRING, customer_id STRING,
  qty INT64, net_amount NUMERIC
)
PARTITION BY DATE(order_ts)
CLUSTER BY store_id, product_id
OPTIONS (require_partition_filter = TRUE, partition_expiration_days = NULL);
```

Rules:

- **Order matters.** Put the column filtered most often first. Filtering on the 2nd column only gives less benefit.
- Good for **high-cardinality** columns that are filtered or joined on (customer_id, sensor_id, store_id).
- **Automatic re-clustering** happens in the background for free. No `OPTIMIZE` or `VACUUM` is needed (unlike Delta).
- Cost estimates in a dry run are an **upper bound** for clustered tables. The actual bytes billed are often lower.
- **Clustering without partitioning** is valid for tables that aren't time-based.

| Partition vs. cluster | Partition | Cluster |
|---|---|---|
| Granularity | Coarse (day/month) | Fine (blocks) |
| Columns | 1 | Up to 4 |
| Cost estimate before running | Exact | Upper bound |
| Partition-level operations (overwrite, expire) | ✅ | ❌ |
| Cardinality | Low/medium (dates) | High is fine |

**Example:** `PARTITION BY DATE(order_ts) CLUSTER BY store_id, product_id`. A query for one store over 1 week reads 7 partitions and only that store's blocks.

---

## 5. Query performance: reading the execution plan

In the console, **Execution details** (or `INFORMATION_SCHEMA.JOBS` → `job_stages`) shows each stage with:

- **Wait / Read / Compute / Write** time (avg vs. max per worker)
    - **max ≫ avg** = **data skew** (one hot key, such as a null key or a single huge customer)
- **Bytes shuffled**: big joins and aggregations
- **Bytes spilled to disk**: shuffle didn't fit in memory, which is very slow
- **Slot time consumed**: the real compute cost under Editions

### Fixes by symptom

| Symptom | Cause | Fix |
|---|---|---|
| Huge bytes read | No pruning, `SELECT *` | Partition/cluster filters, select needed columns |
| Huge shuffle on join | Joining two big tables | **Filter and aggregate before joining**; join on clustered keys |
| Skew (max ≫ avg) | Hot key, e.g. `NULL` or `'unknown'` | Filter or handle nulls separately; salt the hot key |
| Spill to disk | Huge `ORDER BY` without `LIMIT`, giant `ARRAY_AGG`, window over the whole table | Add `LIMIT`, partition windows (`PARTITION BY`), pre-aggregate |
| Same CTE used 3 times | BigQuery **may re-evaluate a CTE each time it's referenced** | Materialize to a temp table |
| Slow `COUNT(DISTINCT)` | Exact distinct is expensive | `APPROX_COUNT_DISTINCT` (HyperLogLog++) if ~1% error is fine |
| Many small DML statements | Single-row `UPDATE`s in loops | Batch into one `MERGE` |
| Join explodes rows | Many-to-many join | Check the grain and deduplicate first |

### SQL habits that pay off

```sql
-- Deduplicate: keep the latest record per key
SELECT * FROM staging.orders
QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC) = 1;

-- Filter early, then join
WITH s AS (
  SELECT store_id, product_id, SUM(net_amount) amt
  FROM sales.fct_order_line
  WHERE DATE(order_ts) = @run_date
  GROUP BY 1, 2)
SELECT p.category, SUM(s.amt)
FROM s JOIN sales.dim_product p USING (product_id)
GROUP BY 1;

-- Exclude a heavy column instead of listing 80 columns
SELECT * EXCEPT (raw_payload) FROM raw.events WHERE ...;
```

---

## 6. Materialized views, BI Engine, caching

| Feature | What | When |
|---|---|---|
| **Query result cache** | Identical query on unchanged tables returns free within ~24h | Automatic; broken by non-deterministic functions like `CURRENT_TIMESTAMP()` |
| **Materialized view (MV)** | Precomputed result, **incrementally refreshed**; the optimizer can **rewrite** queries on the base table to use it | Repeated aggregations (dashboards). Limited SQL support (aggregations, some joins). `max_staleness` option trades freshness for cost |
| **Scheduled aggregate table** | Your own `CREATE OR REPLACE` / `MERGE` on a schedule | Complex logic that MVs don't support |
| **BI Engine** | In-memory acceleration reserved by GB | Sub-second dashboards (Looker, Looker Studio) |
| **Looker PDTs / aggregate awareness** | Looker-managed pre-aggregates | See [Looker and the semantic layer](looker-semantic-layer.md) |

```sql
CREATE MATERIALIZED VIEW sales.mv_sales_daily_store
PARTITION BY order_date
CLUSTER BY store_id
OPTIONS (enable_refresh = TRUE, refresh_interval_minutes = 15, max_staleness = INTERVAL "30" MINUTE)
AS
SELECT DATE(order_ts) order_date, store_id, SUM(net_amount) net_sales, COUNT(*) lines
FROM sales.fct_order_line
GROUP BY 1, 2;
```

---

## 7. DML, MERGE and incremental loading

### Load patterns (from simplest to most powerful)

| Pattern | SQL | Idempotent? | Notes |
|---|---|---|---|
| Full refresh | `CREATE OR REPLACE TABLE ... AS SELECT` | ✅ | Simple, but expensive at scale. Fine for small dimensions |
| Append | `INSERT INTO ... SELECT` | ❌ on rerun | Duplicates on retry. Avoid unless deduplicated downstream |
| **Partition overwrite** | Write to `table$YYYYMMDD` with `WRITE_TRUNCATE`, or dbt `insert_overwrite` | ✅ | **Best for time-partitioned facts.** Reprocess N days cheaply |
| **MERGE (upsert)** | `MERGE ... WHEN MATCHED / NOT MATCHED` | ✅ | For dimensions and late updates. Restrict the target scan with a partition filter in `ON` |

### MERGE that also prunes the target (important at scale)

```sql
MERGE sales.fct_order_line T
USING (
  SELECT * FROM staging.order_line_changes
  WHERE _ingest_date = @run_date
  QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id, line_no ORDER BY _source_ts DESC) = 1
) S
ON  T.order_id = S.order_id AND T.line_no = S.line_no
AND DATE(T.order_ts) >= DATE_SUB(@run_date, INTERVAL 3 DAY)   -- ← limits the target scan
WHEN MATCHED AND S._is_deleted THEN DELETE
WHEN MATCHED THEN UPDATE SET qty = S.qty, net_amount = S.net_amount
WHEN NOT MATCHED AND NOT S._is_deleted THEN
  INSERT (order_id, line_no, order_ts, store_id, product_id, customer_id, qty, net_amount)
  VALUES (S.order_id, S.line_no, S.order_ts, S.store_id, S.product_id, S.customer_id, S.qty, S.net_amount);
```

Gotchas:

- **Deduplicate the source first.** If more than one source row matches one target row, the MERGE **fails**.
- Without the partition condition, the MERGE scans the **entire** target table every run, which is a common hidden cost.
- Concurrent DML on the same table can be **queued or fail**. Serialize writers per table (one owner pipeline per table).

### Multi-statement transactions

```sql
BEGIN TRANSACTION;
  DELETE FROM sales.fct_order_line WHERE DATE(order_ts) = @run_date;
  INSERT INTO sales.fct_order_line SELECT ... ;
COMMIT TRANSACTION;
```

---

## 8. Streaming and CDC into BigQuery

| Method | Use |
|---|---|
| **Storage Write API** | Recommended streaming API: exactly-once (committed type), high throughput, cheaper |
| **Pub/Sub BigQuery subscription** | Pub/Sub → BigQuery with **no Dataflow**, for simple ingestion without transformation |
| **Dataflow** `WriteToBigQuery` | When you need transformation or enrichment in the stream |
| **Datastream** | Database CDC → BigQuery (see [Streaming and CDC](streaming-cdc.md)) |

### BigQuery CDC tables (what Datastream uses under the hood)

- The table has a **primary key** (`PRIMARY KEY (id) NOT ENFORCED`) and a **`max_staleness`** option.
- Writers send rows with the `_CHANGE_TYPE` pseudo-column (`UPSERT` / `DELETE`) and optionally `_CHANGE_SEQUENCE_NUMBER` for ordering.
- BigQuery applies changes in the background. Queries see data **at most `max_staleness` old**.
    - Smaller staleness = more frequent background merges = **more cost**.
    - With a larger staleness, BigQuery can merge changes on the fly at query time if the background apply is behind.

```sql
ALTER TABLE raw_pos.orders SET OPTIONS (max_staleness = INTERVAL 10 MINUTE);
```

---

## 9. Data governance and security in BigQuery (often a follow-up)

| Need | Feature |
|---|---|
| Team sees only some **columns** (PII) | **Policy tags** (column-level security) from a Data Catalog taxonomy; `Fine-Grained Reader` role |
| Mask instead of hide | **Dynamic data masking** on policy tags (hash, nullify, last-4 digits) |
| Team sees only some **rows** (e.g. own country) | **Row-level access policies**: `CREATE ROW ACCESS POLICY ... FILTER USING (country = 'FI')` |
| Share a query result without granting base tables | **Authorized views**, or authorized datasets/routines |
| Recover a deleted or overwritten table | **Time travel** (2–7 days, `FOR SYSTEM_TIME AS OF`), then **fail-safe** (+7 days, via support) |
| Cheap, safe test copies | **Table clones** (pay only for changed data) and **snapshots** |
| Encryption keys | Google-managed by default; **CMEK** via Cloud KMS if required |
| Data residency | Dataset **location** (e.g. `EU`, `europe-north1` Finland). Can't be changed later, so copy the dataset to move it |

```sql
-- Time travel: what did the table look like before the bad load?
SELECT * FROM sales.fct_order_line
FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR)
WHERE DATE(order_ts) = '2026-10-22';
```

---

## 10. Monitoring and cost control with INFORMATION_SCHEMA

```sql
-- 1) Top cost offenders (by normalized query pattern)
SELECT user_email,
       query_info.query_hashes.normalized_literals AS pattern,
       ANY_VALUE(LEFT(query, 200))                 AS sample_sql,
       COUNT(*)                                    AS runs,
       SUM(total_bytes_billed)/POW(1024,4)         AS tib_billed,
       SUM(total_slot_ms)/1000/3600                AS slot_hours
FROM `region-eu`.INFORMATION_SCHEMA.JOBS
WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
  AND job_type = 'QUERY' AND state = 'DONE'
GROUP BY 1, 2 ORDER BY slot_hours DESC LIMIT 20;

-- 2) Cost per pipeline (requires job labels)
SELECT l.value AS pipeline, SUM(total_slot_ms)/1000/3600 AS slot_hours
FROM `region-eu`.INFORMATION_SCHEMA.JOBS, UNNEST(labels) l
WHERE l.key = 'pipeline' AND creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
GROUP BY 1 ORDER BY 2 DESC;

-- 3) Biggest tables and storage
SELECT table_schema, table_name,
       total_logical_bytes/POW(1024,4)  AS logical_tib,
       total_physical_bytes/POW(1024,4) AS physical_tib,
       long_term_logical_bytes/POW(1024,4) AS long_term_tib
FROM `region-eu`.INFORMATION_SCHEMA.TABLE_STORAGE
ORDER BY logical_tib DESC LIMIT 20;
```

### Guardrails

| Guardrail | Where |
|---|---|
| `maximum_bytes_billed` | Per query or job config: the query fails instead of costing a fortune |
| Custom quotas | "Query usage per day" per project or per user |
| `require_partition_filter` | Per table |
| Reservation max slots / autoscale cap | Editions |
| **Job labels** (`pipeline`, `team`, `env`) | Every job, so you can attribute cost |
| Budget alerts | Cloud Billing |
| Billing export to BigQuery | Cost dashboard |

---

## 11. BigQuery-specific SQL worth knowing

```sql
-- Nested and repeated (denormalized order with lines): no join needed
SELECT order_id, l.product_id, l.qty
FROM sales.orders_nested, UNNEST(lines) AS l;

-- STRUCT and ARRAY_AGG to build nested
SELECT order_id, ARRAY_AGG(STRUCT(product_id, qty, net_amount)) AS lines
FROM sales.fct_order_line WHERE DATE(order_ts) = '2026-10-22' GROUP BY order_id;

-- Hash for reconciliation (aggregated, not row by row)
SELECT DATE(order_ts) d, COUNT(*) n,
       BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(t))) AS fp
FROM sales.fct_order_line t
WHERE DATE(order_ts) BETWEEN '2026-10-01' AND '2026-10-22'
GROUP BY d;

-- Wildcard tables (legacy sharded tables events_2026*)
SELECT COUNT(*) FROM `proj.ds.events_*` WHERE _TABLE_SUFFIX BETWEEN '20261001' AND '20261022';

-- Safe functions (don't fail the whole job on bad data)
SELECT SAFE_CAST(amount AS NUMERIC), SAFE.PARSE_DATE('%Y-%m-%d', d) FROM raw.x;

-- JSON
SELECT JSON_VALUE(payload, '$.customer.id') FROM raw.events;

-- Gaps in sensor data (completeness check)
SELECT sensor_id, COUNT(*) AS readings, 86400/5 AS expected   -- 1 reading every 5s
FROM iot.readings WHERE DATE(reading_ts) = '2026-10-22'
GROUP BY sensor_id HAVING COUNT(*) < 0.98 * 86400/5;
```

Note: **sharded tables** (`events_20261022`, one table per day) are a legacy anti-pattern. Migrate them to **one partitioned table**. That's a typical modernization task.

---
