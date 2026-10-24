# Dataform and dbt from the inside

> **Goal of this lesson:** after reading it, you can explain why SQL transformation frameworks exist, how dbt and Dataform work inside (compile, dependency graph, materializations), which SQL they actually send to BigQuery, and how to build a tested, incremental, CI-checked sales mart with either tool.
>
> **Why it matters:** simplifying warehouse load chains, strong DWH modelling, a near real-time sales data model on BigQuery, incremental modelling, production-grade SQL, testing, documentation and CI/CD all meet here. The transformation layer is where most of the warehouse logic lives.
>
> **Quick reference:** [Composer, Airflow and Dataform deep dive](../deep-dives/composer-airflow-dataform.md) §5 (comparison table and short examples). Orchestration side: [Cloud Composer and Airflow](composer-airflow.md). BigQuery internals (partitioning, MERGE cost): [BigQuery](bigquery.md). Modelling (Kimball, SCD2): [DWH modelling](dwh-modelling.md).
>
> **Versions:** dbt features below refer to dbt Core 1.8–1.10 (unit tests from 1.8, microbatch and YAML snapshots from 1.9). Dataform refers to Dataform in BigQuery with Dataform core 3.x (`workflow_settings.yaml`). Both evolve quickly; check current docs before quoting a detail.

---

## 1. What it is and why it exists

### The ELT shift
In the old **ETL** world, a tool (Informatica, SSIS, a Spark job) extracted data, transformed it on its own servers, and loaded the result into the warehouse. In the cloud, the warehouse itself (BigQuery) is the most powerful compute engine you have. So we do **ELT**: **E**xtract and **L**oad raw data into BigQuery first, then **T**ransform it **inside BigQuery with SQL**.

That creates a new problem. Now you have **hundreds of SQL statements** that build tables from other tables. Without a framework you get:

- SQL files run in a hand-maintained order (or 80 hand-wired Airflow tasks).
- Copy-pasted `CREATE OR REPLACE TABLE` boilerplate, partition settings forgotten.
- No tests: nobody knows a key became duplicated until a dashboard is wrong.
- No documentation or lineage: "Which tables break if I change this column?"
- No environments: developers test in production datasets.

### What dbt and Dataform are
**dbt** (data build tool, by dbt Labs) and **Dataform** (by Google, built into BigQuery) are **SQL transformation frameworks**. You write each table as a **SELECT statement** (a "model"), plus a little config. The framework:

1. **compiles** your templated SQL into plain BigQuery SQL,
2. **finds the dependencies** between models from `ref()` calls and builds a DAG,
3. **wraps** each SELECT in the right DDL/DML (`CREATE OR REPLACE TABLE`, `CREATE VIEW`, `MERGE`) — this is called the **materialization**,
4. **runs** the statements in dependency order, in parallel where possible,
5. **tests** the results (unique, not null, relationships, custom checks),
6. **documents** everything and shows **lineage**,
7. lets the same code run in **dev, CI and prod** by changing only the target project/dataset.

Everything is plain text in **git**, so you get code review, history and CI.

### The analogy
Think of a **build system for software** (like Maven or make). Each source file says what it depends on; the build tool figures out the order, rebuilds only what's needed, and runs the tests. dbt and Dataform are **a build system for tables**. A model is a source file, `ref()` is an import, the materialization is the compiler's output format, and tests are unit tests for data.

---

## 2. Where it fits in a modern GCP data platform

```text
Datastream / Dataflow / loads ──► raw_* datasets (bronze)            ◄─ owned by ingestion (Composer, Datastream)
                                        │
                          ┌─────────────┴──────────── dbt or Dataform project ─────────────┐
                          │  staging (stg_*):   rename, cast, dedupe CDC, 1:1 with sources │
                          │  intermediate (int_*): joins, business rules, enrichment        │
                          │  marts: fct_sales_line, dim_customer (SCD2), dim_product,       │
                          │         agg_sales_daily  → star schema for Looker               │
                          │  tests / assertions on every layer                              │
                          └─────────────┬──────────────────────────────────────────────────┘
                                        │ triggered by Composer (Asset event) or own schedule
                                        ▼
                               Looker (LookML on the marts) → conversational BI
```

In a typical EDW modernization on GCP, the team will:

- **Move SQL logic out of Airflow** into dbt or Dataform, so Airflow only triggers "build the sales domain".
- Build the **near real-time sales model** with **incremental** models (MERGE every 15 minutes, cheap and correct).
- Add **tests** and **CI** so changes don't break Looker.
- Choose (or follow the team's choice) between dbt and Dataform.

---

## 3. Architecture: how they work under the hood

### 3.1 The common pipeline: parse → compile → graph → execute

```text
 Your project (git)                     Framework                              BigQuery
┌──────────────────────┐   1. parse   ┌──────────────────────────┐
│ models/*.sql (dbt)    │ ──────────► │ read all files + configs │
│ definitions/*.sqlx    │             │ resolve ref()/source()    │
│ *.yml / config blocks │             └────────────┬─────────────┘
│ macros / includes(JS) │                          │ 2. build DAG of nodes/actions
└──────────────────────┘                           ▼
                                       ┌──────────────────────────┐
                                       │ graph: stg → int → fct   │  (cycle? → error)
                                       │ (dbt: manifest.json;     │
                                       │  Dataform: compiled graph)│
                                       └────────────┬─────────────┘
                                                    │ 3. compile templates
                                                    ▼    ref('x') → `proj.dataset.x`
                                       ┌──────────────────────────┐
                                       │ plain SELECT per model   │
                                       │ + materialization wraps  │
                                       │ it in DDL/DML            │
                                       └────────────┬─────────────┘
                                                    │ 4. execute in topological order,
                                                    │    N in parallel (threads / concurrency)
                                                    ▼
                                       CREATE OR REPLACE TABLE ... AS SELECT   ──►  BigQuery jobs
                                       MERGE INTO ... USING (...)              ──►  (each one a job,
                                       CREATE OR REPLACE VIEW ...              ──►   billed normally)
                                                    │ 5. tests/assertions = SELECTs that must
                                                    ▼    return zero rows
                                       results, logs, artifacts (run_results.json, docs)
```

**Key insight:** the framework is a **compiler and scheduler for SQL**. It does no data processing itself. **All compute happens in BigQuery** as normal query jobs, and you pay normal BigQuery prices. (dbt Core and Dataform are free; dbt Cloud is a paid service.)

### 3.2 Step by step

**1. Parse.** The framework reads every model file and config. In dbt, parsing produces `target/manifest.json`: a JSON document with every node (model, test, seed, snapshot, source), its config, and its `depends_on` list. In Dataform, compilation produces a **compiled graph** (JSON) of **actions** (tables, views, incrementals, operations, assertions) with their dependency targets.

**2. Build the DAG from `ref()`.** When a model says `{{ ref('stg_pos__order_lines') }}` (dbt) or `${ref("stg_pos_order_lines")}` (Dataform), two things happen: the framework **records an edge** in the graph, and it **replaces the call with the fully qualified table name** of the target environment. You never write `my-prod-project.staging.order_lines` by hand. That's why the same code runs in dev and prod: the name is resolved at compile time from the target. A cycle (A refs B, B refs A) is a compile error.

**3. Compile templates.** dbt uses **Jinja** (`{{ }}`, `{% %}`): loops, ifs, macros. Dataform uses **JavaScript template literals** (`${ }`) and JS functions. The result is a plain SQL SELECT you can read in `target/compiled/` (dbt) or in the Dataform UI ("compiled queries").

**4. Materialize.** The **materialization** decides what statement wraps the SELECT:

| Materialization | Statement BigQuery receives (simplified) |
|---|---|
| view | `CREATE OR REPLACE VIEW x AS (SELECT ...)` |
| table | `CREATE OR REPLACE TABLE x PARTITION BY ... CLUSTER BY ... OPTIONS(...) AS (SELECT ...)` |
| incremental, first run / full refresh | Same as table |
| incremental, later runs | `MERGE INTO x USING (SELECT ... new rows) ON key ...` or `INSERT INTO x SELECT ...` |
| ephemeral (dbt) | Nothing; the SELECT is inlined as a CTE into the models that ref it |
| materialized view | `CREATE MATERIALIZED VIEW x ... AS SELECT ...` (BigQuery maintains it) |

**5. Execute in order, in parallel.** The runner walks the DAG in **topological order**. A node starts when all its parents succeeded. Independent nodes run at the same time: dbt's `threads` setting (for example 8) caps parallel BigQuery jobs; Dataform has its own concurrency. If a node fails, its **descendants are skipped**, but unrelated branches continue.

**6. Test.** A test is a SELECT that returns the **bad rows**. Zero rows = pass. For example `unique` on `order_id` compiles to roughly:

```sql
SELECT order_id, COUNT(*) AS n
FROM `proj.mart_sales.fct_sales_line`
WHERE order_id IS NOT NULL
GROUP BY order_id
HAVING COUNT(*) > 1
```

### 3.3 Environments
Both tools separate **code** from **target**:

- **dbt:** `profiles.yml` defines **targets** (dev, ci, prod) with project, dataset, location, auth method, threads. `dbt build --target prod` changes where every `ref()` points.
- **Dataform:** `workflow_settings.yaml` has defaults (project, dataset, location); **compilation overrides** (in workspaces and release configurations) change the project, add a **schema suffix** or **table prefix**, and set **vars** per environment.

So a developer builds into `dbt_marthala_mart_sales` or `mart_sales_ws_marthala`, CI builds into `mart_sales_pr_123`, prod builds into `mart_sales`. Same code.

---

## 4. dbt in depth

### 4.1 Project structure

```text
retail_dbt/
├─ dbt_project.yml          # project name, paths, default configs per folder
├─ profiles.yml             # (often in ~/.dbt/) connection targets: dev, ci, prod
├─ packages.yml             # dbt_utils, dbt_expectations, ...
├─ models/
│  ├─ staging/
│  │  ├─ pos/
│  │  │  ├─ _pos__sources.yml        # source definitions + freshness
│  │  │  ├─ _pos__models.yml         # tests + docs for staging models
│  │  │  └─ stg_pos__order_lines.sql
│  │  └─ crm/stg_crm__customers.sql
│  ├─ intermediate/sales/int_sales__order_lines_enriched.sql
│  └─ marts/sales/
│     ├─ _sales__models.yml
│     ├─ fct_sales_line.sql
│     ├─ dim_product.sql
│     └─ agg_sales_daily.sql
├─ snapshots/ (or YAML snapshots in dbt 1.9+)
├─ seeds/  store_regions.csv
├─ tests/  assert_sales_reconcile_with_pos.sql     # singular tests
├─ macros/ generate_schema_name.sql, cents_to_eur.sql
└─ target/  (generated: manifest.json, compiled/, run/, run_results.json)
```

```yaml
# dbt_project.yml
name: retail
version: "1.0.0"
profile: retail
models:
  retail:
    staging:
      +materialized: view
      +schema: staging
    intermediate:
      +materialized: ephemeral
    marts:
      +materialized: table
      sales:
        +schema: mart_sales
        +tags: ["sales"]
```

```yaml
# profiles.yml
retail:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: oauth                 # uses gcloud application-default credentials
      project: retail-dev
      dataset: dbt_marthala         # personal dev dataset
      location: EU
      threads: 8
      job_execution_timeout_seconds: 600
      maximum_bytes_billed: 50000000000   # safety: fail queries scanning > 50 GB
    prod:
      type: bigquery
      method: oauth                 # in prod: the runner's service account via ADC / impersonation
      project: retail-prod
      dataset: analytics
      location: EU
      threads: 16
```

**Watch out: `generate_schema_name`.** By default dbt builds a custom schema as `<target_dataset>_<custom_schema>`, for example `analytics_mart_sales`, not `mart_sales`. Most teams override the macro so prod uses the clean name and dev keeps a personal prefix. Know this; it confuses everyone once.

```sql
-- macros/generate_schema_name.sql
{% macro generate_schema_name(custom_schema_name, node) -%}
  {%- if target.name == 'prod' and custom_schema_name is not none -%}
    {{ custom_schema_name | trim }}
  {%- elif custom_schema_name is none -%}
    {{ target.schema }}
  {%- else -%}
    {{ target.schema }}_{{ custom_schema_name | trim }}
  {%- endif -%}
{%- endmacro %}
```

### 4.2 Sources, models, ref and source
- **Source:** a table that dbt does **not** build (raw data loaded by Datastream or Airflow). Declared in YAML, used with `{{ source('pos', 'order_lines') }}`. Sources appear in lineage and can have **freshness** checks.
- **Model:** a `.sql` file with one SELECT. The file name is the table name.
- **`ref('model')`:** dependency on another model; resolved to the target's table name.

```yaml
# models/staging/pos/_pos__sources.yml
sources:
  - name: pos
    database: retail-ingest          # the shared ingestion project
    schema: raw_pos
    loaded_at_field: _ingested_at
    freshness:
      warn_after:  {count: 30, period: minute}
      error_after: {count: 2,  period: hour}
    tables:
      - name: order_lines
```

```sql
-- models/staging/pos/stg_pos__order_lines.sql
-- Staging: rename, cast, keep the latest CDC version of each line
WITH src AS (
  SELECT * FROM {{ source('pos', 'order_lines') }}
),
latest AS (
  SELECT *
  FROM src
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY order_id, line_no
    ORDER BY _source_ts DESC) = 1
)
SELECT
  CAST(order_id AS STRING)         AS order_id,
  CAST(line_no AS INT64)           AS line_no,
  store_id, product_id, customer_id,
  TIMESTAMP(order_ts)              AS order_ts,
  qty,
  CAST(unit_price AS NUMERIC)      AS unit_price,
  CAST(discount AS NUMERIC)        AS discount,
  is_deleted,
  _source_ts
FROM latest
```

`dbt source freshness` checks `MAX(_ingested_at)` against the thresholds. This is a cheap way to catch "the data didn't arrive" before building marts.

### 4.3 Materializations in detail
| Materialization | When to use | Cost / trade-off |
|---|---|---|
| **view** | Staging layers, light logic, always-fresh | No storage; every query re-runs the logic |
| **table** | Small/medium marts, dimensions | Full rebuild every run: scans all inputs every time |
| **incremental** | Big facts, near real-time | Processes only new/changed rows; more logic to get right |
| **ephemeral** | Reusable logic that shouldn't be a table | Inlined as a CTE; can't be queried or tested directly; overuse makes compiled SQL huge |
| **materialized_view** | Simple aggregations BigQuery can maintain automatically | BigQuery MV limits (supported SQL, refresh behaviour); see the [BigQuery lesson](bigquery.md) |

BigQuery-specific configs you will use: `partition_by`, `cluster_by`, `require_partition_filter`, `partition_expiration_days`, `labels`, `hours_to_expiration` (dev), `kms_key_name`.

```sql
-- models/marts/sales/dim_product.sql
{{ config(
    materialized='table',
    cluster_by=['product_id'],
    labels={'domain': 'sales', 'layer': 'mart'}
) }}
SELECT p.product_id, p.product_name, p.category, p.brand, p.list_price
FROM {{ ref('stg_erp__products') }} AS p
```

### 4.4 Incremental models: the most important part

#### How `is_incremental()` works
`is_incremental()` returns **true** only when **all** are true:

1. the model is configured `materialized='incremental'`,
2. the target table **already exists**,
3. the run is **not** `--full-refresh`.

On the first run (or a full refresh), dbt builds the whole table with `CREATE OR REPLACE TABLE`. Later runs only process the rows selected inside the `{% if is_incremental() %}` block and apply them with the chosen **strategy**. `{{ this }}` refers to the existing target table.

```sql
-- models/marts/sales/fct_sales_line.sql
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['order_id', 'line_no'],
    partition_by={'field': 'order_date', 'data_type': 'date', 'granularity': 'day'},
    cluster_by=['store_id', 'product_id'],
    on_schema_change='append_new_columns',
    incremental_predicates=[
      "DBT_INTERNAL_DEST.order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)"
    ],
    tags=['sales', '15min']
) }}

SELECT
  l.order_id,
  l.line_no,
  DATE(l.order_ts, 'Europe/Helsinki')           AS order_date,
  l.order_ts,
  l.store_id,
  l.product_id,
  l.customer_id,
  l.qty,
  l.qty * l.unit_price - l.discount              AS net_amount,
  l.is_deleted,
  l._source_ts
FROM {{ ref('stg_pos__order_lines') }} AS l
{% if is_incremental() %}
  -- lookback window: re-read the last 3 hours to catch late CDC events
  WHERE l._source_ts > (SELECT TIMESTAMP_SUB(MAX(_source_ts), INTERVAL 3 HOUR) FROM {{ this }})
{% endif %}
```

#### The strategies on BigQuery and the SQL they generate

**1. `merge` (default on BigQuery).** With `unique_key`, dbt generates roughly:

```sql
MERGE INTO `proj.mart_sales.fct_sales_line` AS DBT_INTERNAL_DEST
USING (
  SELECT ... FROM `proj.staging.stg_pos__order_lines` AS l
  WHERE l._source_ts > (SELECT TIMESTAMP_SUB(MAX(_source_ts), INTERVAL 3 HOUR)
                        FROM `proj.mart_sales.fct_sales_line`)
) AS DBT_INTERNAL_SOURCE
ON  DBT_INTERNAL_SOURCE.order_id = DBT_INTERNAL_DEST.order_id
AND DBT_INTERNAL_SOURCE.line_no  = DBT_INTERNAL_DEST.line_no
AND DBT_INTERNAL_DEST.order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)   -- incremental_predicates
WHEN MATCHED THEN UPDATE SET order_date = DBT_INTERNAL_SOURCE.order_date, ... 
WHEN NOT MATCHED THEN INSERT (order_id, line_no, ...) VALUES (...)
```

- Without `unique_key`, merge just **inserts** (append): reruns create duplicates.
- **Cost:** a MERGE must find matching rows in the **target**. Without a filter on the target's partition column, BigQuery **scans the whole target table** every run. On a 2 TB fact table every 15 minutes, that's very expensive. `incremental_predicates` add a filter on `DBT_INTERNAL_DEST` so BigQuery prunes target partitions. The trade-off: a late update for an order older than 7 days would be inserted as a new row instead of updating (duplicate). Choose the window from the data's real lateness.
- `merge_update_columns` / `merge_exclude_columns` control which columns are updated.
- Good for: updates to existing rows (CDC), near real-time, late data with keys.

**2. `insert_overwrite`.** Replaces **whole partitions**. No `unique_key` needed. Two modes:

- **Dynamic** (no `partitions` config): dbt first writes the new rows to a temporary table, finds which partitions they touch, then replaces those partitions:

```sql
DECLARE dbt_partitions_for_replacement ARRAY<DATE>;
CREATE OR REPLACE TABLE `proj.mart_sales.agg_sales_daily__dbt_tmp`
  OPTIONS(expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 12 HOUR))
AS (SELECT ... new rows ...);
SET (dbt_partitions_for_replacement) = (
  SELECT AS STRUCT ARRAY_AGG(DISTINCT order_date) FROM `proj.mart_sales.agg_sales_daily__dbt_tmp`);
MERGE INTO `proj.mart_sales.agg_sales_daily` AS DBT_INTERNAL_DEST
USING (SELECT * FROM `proj.mart_sales.agg_sales_daily__dbt_tmp`) AS DBT_INTERNAL_SOURCE
ON FALSE
WHEN NOT MATCHED BY SOURCE AND DBT_INTERNAL_DEST.order_date IN UNNEST(dbt_partitions_for_replacement)
  THEN DELETE
WHEN NOT MATCHED THEN INSERT (...) VALUES (...);
```

- **Static** (`partitions=['date(...)', ...]` given in config): dbt skips the "find partitions" step; cheaper, but you must compute the list yourself (often from Airflow's `ds` passed as a var).
- `copy_partitions: true` (in `partition_by`) uses BigQuery **copy jobs** to replace partitions instead of MERGE; copy jobs are free of query cost.
- **Cost:** the target scan is limited to the affected partitions. Very efficient for **aggregates by day** and for **reprocessing whole days**. Not good when the new rows touch many partitions sparsely (you rewrite each full partition).
- Good for: daily aggregates, event tables where you reprocess "the last N days" fully. **Idempotent by design**, like Airflow's partition overwrite.

**3. `microbatch` (dbt 1.9+).** You configure the **time column** and batch size; dbt splits the work into independent time batches and runs each one, using `insert_overwrite` semantics per batch on BigQuery.

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='microbatch',
    event_time='order_ts',
    batch_size='day',
    begin='2024-01-01',
    lookback=2,
    partition_by={'field': 'order_date', 'data_type': 'date'}
) }}
SELECT ... FROM {{ ref('stg_pos__order_lines') }}   -- no is_incremental() block needed
```

- dbt automatically filters upstream models that have `event_time` configured to the batch's window.
- Each batch can be retried separately (`dbt retry`), and backfills are explicit: `dbt run --select fct_x --event-time-start 2026-09-01 --event-time-end 2026-10-01`.
- It is the dbt equivalent of Airflow's "one run per data interval". Good for large time-series facts; less suitable when rows are updated by key long after the event time.

**4. `insert_overwrite` vs. `merge` decision:**

| Question | merge | insert_overwrite |
|---|---|---|
| Rows updated by key (CDC)? | Yes | Only if you rebuild whole partitions |
| Need `unique_key`? | Yes (else append) | No |
| Target scan | Whole table unless predicates | Only replaced partitions |
| Late data | Via lookback window + key | Reprocess last N partitions fully |
| Typical model | `fct_sales_line` (near real-time) | `agg_sales_daily` |

#### `on_schema_change`
What happens when the model's columns change on an incremental run: `ignore` (default; new columns silently not added), `fail`, `append_new_columns`, `sync_all_columns` (also drops removed columns). Default `ignore` is a classic surprise: you add a column and it never appears.

### 4.5 Snapshots (SCD Type 2)
A **snapshot** records how rows of a **mutable** source change over time. Each run compares the source with the snapshot table and **closes** changed rows and **inserts** new versions. dbt adds columns: `dbt_scd_id`, `dbt_updated_at`, `dbt_valid_from`, `dbt_valid_to` (NULL = current, or a configured far-future date with `dbt_valid_to_current` in 1.9+).

```yaml
# snapshots/customers.yml   (YAML snapshots, dbt 1.9+)
snapshots:
  - name: snap_customer
    relation: ref('stg_crm__customers')
    config:
      schema: snapshots
      unique_key: customer_id
      strategy: timestamp
      updated_at: updated_at
      hard_deletes: invalidate      # ignore | invalidate | new_record (1.9+)
```

| Strategy | How it detects a change | Use when |
|---|---|---|
| **timestamp** | `updated_at` of the source row is newer than the snapshot's | The source has a reliable `updated_at` (preferred: cheaper, exact) |
| **check** | Compares the listed `check_cols` (or `all`) to the current snapshot row | No reliable timestamp; costs more, and changes between two runs are merged into one |

**Important internals:**

- A snapshot only sees the source **at the moment it runs**. If a customer changes twice between two runs, you capture only the last state. For **true history** from CDC, build SCD2 from the **change stream** itself (every CDC event has a timestamp) in an incremental model instead.
- Snapshots are **not idempotent in the "rebuild" sense**: you can't recreate past history from the current source. Never drop a snapshot table. Keep them in their own dataset.
- Then build `dim_customer` as a model on top of the snapshot (rename columns to `valid_from`, `valid_to`, `is_current`, add a surrogate key).

Older syntax (before 1.9): a `{% snapshot snap_customer %} {{ config(target_schema=..., unique_key=..., strategy=..., updated_at=...) }} SELECT ... {% endsnapshot %}` block in `snapshots/`. Both still work.

### 4.6 Tests
| Kind | What | Example |
|---|---|---|
| **Generic data tests** | Reusable, declared in YAML: `unique`, `not_null`, `accepted_values`, `relationships` | `order_id` is not null |
| **Package tests** | From packages: `dbt_utils.unique_combination_of_columns`, `dbt_utils.expression_is_true`, `dbt_expectations.*` | Composite key unique |
| **Singular tests** | A `.sql` file in `tests/` returning bad rows | Sales total equals POS control total |
| **Unit tests** (1.8+) | Fixed input rows → expected output rows for one model's logic; no real data needed | `net_amount = qty*price - discount` |
| **Source freshness** | `dbt source freshness` | Raw POS data < 30 min old |

```yaml
# models/marts/sales/_sales__models.yml
models:
  - name: fct_sales_line
    description: "One row per POS order line (latest state). Grain: order_id + line_no."
    config:
      contract: {enforced: true}
    data_tests:                                     # 'tests:' also works; 1.8 renamed it
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns: [order_id, line_no]
    columns:
      - name: order_id
        data_type: string
        constraints: [{type: not_null}]
        data_tests: [not_null]
      - name: line_no
        data_type: int64
      - name: order_date
        data_type: date
      - name: store_id
        data_type: string
        data_tests:
          - relationships: {to: ref('dim_store'), field: store_id}
      - name: net_amount
        data_type: numeric
        data_tests:
          - dbt_utils.expression_is_true:
              expression: ">= -10000"
              config: {severity: warn}
      # ... every column must be listed when the contract is enforced

unit_tests:
  - name: net_amount_subtracts_discount
    model: fct_sales_line
    overrides:
      macros: {is_incremental: false}
    given:
      - input: ref('stg_pos__order_lines')
        rows:
          - {order_id: "A1", line_no: 1, qty: 2, unit_price: 10, discount: 5,
             order_ts: "2026-10-22 10:00:00", store_id: "S1", product_id: "P1",
             customer_id: "C1", is_deleted: false, _source_ts: "2026-10-22 10:00:05"}
    expect:
      rows:
        - {order_id: "A1", line_no: 1, net_amount: 15}
```

```sql
-- tests/assert_daily_sales_match_pos_control.sql   (singular test: returns bad rows)
SELECT f.order_date, f.total, c.control_total
FROM (SELECT order_date, SUM(net_amount) AS total
      FROM {{ ref('fct_sales_line') }}
      WHERE order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY) AND NOT is_deleted
      GROUP BY 1) AS f
JOIN {{ source('pos', 'daily_control_totals') }} AS c USING (order_date)
WHERE ABS(f.total - c.control_total) > 0.01
```

Test configs: `severity: warn|error`, `warn_if`, `error_if` (thresholds like `">10"`), `where` (limit a test to recent partitions — important for cost on big tables), `store_failures: true` (save bad rows to a table for debugging).

**`dbt build`** runs seeds, models, snapshots and tests **in DAG order**: each model's tests run right after it, and if a test fails with `error`, the **downstream models are skipped**. That's how bad data is blocked before reaching Looker. (`dbt run` + `dbt test` separately does not block.)

### 4.7 Model contracts
With `contract: {enforced: true}`, dbt checks **before building** that the model's SELECT returns exactly the declared columns and data types. On BigQuery it also creates the table with the declared column definitions, and BigQuery enforces `not_null` constraints. Primary and foreign key constraints are created as metadata only (BigQuery does not enforce them). Use contracts on **mart models that Looker depends on**: a column rename then fails in CI instead of breaking a dashboard. (Related: model **versions** let you publish `fct_sales_line_v2` alongside v1 during a breaking change.)

### 4.8 Seeds, macros, Jinja, packages
- **Seeds:** small CSV files in `seeds/` loaded with `dbt seed` (mappings, region lists). Not for real data.
- **Jinja:** `{% set %}`, `{% for %}`, `{% if %}`, `{{ var('x') }}`, `{{ env_var('X') }}`, `{{ target.name }}`.
- **Macros:** reusable Jinja functions in `macros/`.

```sql
-- macros/money.sql
{% macro to_eur(amount_col, currency_col) -%}
  CASE {{ currency_col }} WHEN 'EUR' THEN {{ amount_col }}
       ELSE {{ amount_col }} * fx.rate END
{%- endmacro %}
```

```sql
-- generate one column per payment type with a loop
SELECT order_id,
{% for p in ['card', 'cash', 'mobile'] %}
  SUM(IF(payment_type = '{{ p }}', amount, 0)) AS {{ p }}_amount{{ ',' if not loop.last }}
{% endfor %}
FROM {{ ref('stg_pos__payments') }}
GROUP BY order_id
```

- **Packages:** `packages.yml` → `dbt deps`. **dbt_utils** (surrogate keys `generate_surrogate_key`, `unique_combination_of_columns`, `date_spine`, `star`), **dbt_expectations** (rich tests), **codegen**, **audit_helper** (compare old vs. new model output during migrations: very useful for the strangler pattern).
- **Common mistake:** too much Jinja. If you need to read compiled SQL to understand a model, simplify it.

### 4.9 Docs and lineage
`dbt docs generate` creates `catalog.json` (column types from BigQuery `INFORMATION_SCHEMA`) and a static website with descriptions and a **lineage graph**. `persist_docs` can write descriptions into BigQuery table/column descriptions (visible in the console, Dataplex and Looker). For documentation and handover, generated docs plus a short README per domain work well.

### 4.10 Selectors, tags and node selection
```bash
dbt build --select tag:sales                     # everything tagged sales
dbt build --select +fct_sales_line               # the model and all its parents
dbt build --select fct_sales_line+               # the model and all its children
dbt build --select source:pos+                   # everything downstream of POS sources
dbt build --select path:models/marts/sales
dbt build --select config.materialized:incremental
dbt build --select tag:15min --exclude tag:heavy
dbt build --selector nightly                     # named selector from selectors.yml
```

Tags + selectors are how **one Airflow task per domain or per frequency** works: the 15-minute DAG runs `tag:15min`, the nightly DAG runs `tag:nightly`.

### 4.11 State, defer and slim CI
Problem: a PR changes one model. Building all 400 models in CI takes an hour and costs money.

**Slim CI:**

1. Save the **production manifest** (`target/manifest.json` from the last prod run) somewhere CI can read (GCS).
2. In CI: `dbt build --select state:modified+ --defer --state ./prod_manifest --target ci`

- `state:modified+` = models whose code/config changed **compared to the prod manifest**, plus their children.
- `--defer` = when a selected model refs an **unchanged** parent that wasn't built in CI, resolve `ref()` to the **prod** table instead. So CI builds only 3 models but reads real upstream data.
- `dbt clone` (1.6+) can create zero-copy clones of prod tables into the CI dataset (on BigQuery it uses table clones), useful for testing **incremental** models against realistic existing tables.
- `dbt retry` re-runs only what failed in the previous command.

dbt Cloud implements this automatically for PRs ("CI jobs"). With dbt Core you script it in Cloud Build / GitHub Actions.

### 4.12 dbt Core vs. dbt Cloud
| | dbt Core | dbt Cloud (now "dbt platform") |
|---|---|---|
| Cost | Free, open source | Paid per seat/usage |
| Runs where | Anywhere: laptop, container, Cloud Run, Composer pod | dbt Labs' SaaS |
| Scheduler | None; use Composer, Cloud Build, Workflows | Built-in jobs, CI jobs, IDE, docs hosting, Semantic Layer |
| Best fit | Natural with Composer already in place | If the organization already pays for it |

dbt Labs also released a new Rust-based engine (**dbt Fusion**) in 2025 with faster parsing and SQL understanding; it is still evolving, so check current docs before discussing it in detail.

### 4.13 Running dbt from Airflow
| Option | How | Pros | Cons |
|---|---|---|---|
| **Single `dbt build` task** in a container | `KubernetesPodOperator` / `GKEStartPodOperator` / Cloud Run job with a dbt image: `dbt build --select tag:sales --target prod` | Simple, isolated dependencies, dbt handles retries via `dbt retry` | Airflow sees one task; you read dbt logs for model detail |
| **Astronomer Cosmos** | Renders the dbt project into an Airflow TaskGroup/DAG: one task per model (+ its tests) | Per-model visibility, retries, timing in Airflow UI | Many tasks for the scheduler; parsing the dbt project at DAG parse time (use the manifest load mode); dbt must be installable in or next to Composer |
| `BashOperator` with dbt installed in Composer | `bash_command="dbt build ..."` | Quick | **Dependency conflicts** between dbt and Airflow packages; runs on Composer workers |
| dbt Cloud job | `DbtCloudRunJobOperator` | Uses dbt Cloud features | Needs dbt Cloud |

```python
# Option 1: one task per domain, in its own container
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow import Dataset

dbt_sales = KubernetesPodOperator(
    task_id="dbt_build_sales",
    name="dbt-build-sales",
    image="europe-north1-docker.pkg.dev/retail-platform/dbt/retail-dbt:1.4.2",   # versioned image from CI
    cmds=["dbt"],
    arguments=["build", "--select", "tag:sales", "--target", "prod",
               "--vars", '{"run_date": "{{ ds }}"}'],
    namespace="composer-user-workloads",     # check Composer docs for the namespace/config to use
    service_account_name="dbt-runner",        # Workload Identity → GCP SA with BigQuery rights
    get_logs=True,
    outlets=[Dataset("bigquery://retail-prod/mart_sales")],
)
```

```python
# Option 2: Cosmos (check Cosmos docs for current API)
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig
from cosmos.constants import LoadMode

sales = DbtTaskGroup(
    group_id="sales",
    project_config=ProjectConfig("/home/airflow/gcs/dags/dbt/retail",
                                 manifest_path="/home/airflow/gcs/dags/dbt/retail/target/manifest.json"),
    profile_config=ProfileConfig(profile_name="retail", target_name="prod",
                                 profiles_yml_filepath="/home/airflow/gcs/dags/dbt/profiles.yml"),
    render_config=RenderConfig(load_method=LoadMode.DBT_MANIFEST, select=["tag:sales"]),
)
```

My default recommendation: **one `dbt build` per domain in a container**, triggered by Assets, unless the team needs per-model retries in the Airflow UI.

---

## 5. Dataform in depth

### 5.1 What Dataform is in GCP
Dataform was a startup; Google acquired it in 2020 and rebuilt it as a **service inside BigQuery**. You find it in the BigQuery console under **Dataform**. It is **free**; you pay only for the BigQuery jobs it runs. It works **only with BigQuery**.

```text
GCP project (e.g. retail-transform)
└─ Dataform (region, e.g. europe-north1)
   └─ Repository  "retail-sales"  ──── linked to ──►  Git remote (GitHub/GitLab/Bitbucket/Azure DevOps)
      │                                               (auth token stored in Secret Manager)
      ├─ Development workspaces  (one per developer/feature; like a git branch with an editor)
      │     edit SQLX → compile → run selected actions into a dev dataset → commit → push
      │
      ├─ Release configurations  (compile a git branch/tag on a schedule or on demand,
      │     apply compilation overrides: project, schema suffix, table prefix, vars)
      │     → produces a "compilation result"
      │
      ├─ Workflow configurations (cron schedule + which actions/tags to run
      │     from a release config's latest compilation result + service account)
      │     → produces "workflow invocations" (one per run, with per-action status)
      │
      └─ executes as a service account → BigQuery jobs
```

| Concept | Meaning | dbt analogy |
|---|---|---|
| **Repository** | A Dataform project, optionally linked to a git remote | A dbt project repo |
| **Workspace** | Your editable copy in the console, on a branch | Your local clone + `dbt` CLI |
| **Compilation result** | The compiled graph for a specific commit + overrides | `manifest.json` + compiled SQL |
| **Release configuration** | "Compile `main` with prod overrides, every hour" | A deployment target (`--target prod`) |
| **Workflow configuration** | "Run tag `sales` from that release every 15 min" | A dbt Cloud job / an Airflow schedule |
| **Workflow invocation** | One execution | One `dbt build` run |
| **Action** | One table, view, incremental, operation, assertion, declaration | A node |

**Service account:** executions run as a service account that needs BigQuery rights on the target datasets. Google has been tightening this (custom service accounts and "act as" permission checks); check the current Dataform IAM docs when setting it up.

### 5.2 Project files
```text
retail-sales/
├─ workflow_settings.yaml        # Dataform core 3.x (older projects: dataform.json)
├─ package.json                  # only if you use npm packages
├─ definitions/
│  ├─ sources/pos.sqlx           # declarations
│  ├─ staging/stg_pos_order_lines.sqlx
│  ├─ intermediate/int_sales_enriched.sqlx
│  ├─ marts/fct_sales_line.sqlx
│  ├─ marts/dim_customer_history.sqlx
│  └─ assertions/sales_reconcile.sqlx
└─ includes/
   ├─ constants.js
   └─ helpers.js
```

```yaml
# workflow_settings.yaml
defaultProject: retail-prod
defaultLocation: EU
defaultDataset: analytics
defaultAssertionDataset: dataform_assertions
dataformCoreVersion: 3.0.0      # pin the version you use; check current releases
vars:
  lookback_hours: "3"
```

### 5.3 SQLX structure
A `.sqlx` file has a **config block** (JSON-like JavaScript object) and a **SQL body** with `${...}` JavaScript expressions. Optional `pre_operations { }` and `post_operations { }` blocks run SQL before/after the main statement, in the same script.

| Built-in | Meaning |
|---|---|
| `${ref("name")}` | Dependency + fully qualified name (`ref({schema: "x", name: "y"})` also works) |
| `${self()}` | This action's own table name |
| `${resolve("name")}` | The name **without** creating a dependency |
| `${incremental()}` | True when this is an incremental run (table exists, not full refresh) |
| `${when(cond, "sql if true", "sql if false")}` | Inline conditional |
| `${dataform.projectConfig.vars.x}` | A compilation variable |
| `${name()}`, `${database()}`, `${schema()}` | Parts of this action's name |

**Declaration** (like a dbt source):

```sql
-- definitions/sources/pos.sqlx
config {
  type: "declaration",
  database: "retail-ingest",
  schema: "raw_pos",
  name: "order_lines"
}
```

### 5.4 Table types
| `type` | What Dataform executes (simplified) |
|---|---|
| `view` | `CREATE OR REPLACE VIEW` (add `materialized: true` for a BigQuery materialized view) |
| `table` | `CREATE OR REPLACE TABLE ... PARTITION BY ... CLUSTER BY ... AS SELECT` |
| `incremental` | First run / full refresh: `CREATE OR REPLACE TABLE`. Later: `INSERT INTO` (no `uniqueKey`) or `MERGE` (with `uniqueKey`) |
| `operations` | Your own SQL statements as-is (DDL, DML, procedures). With `hasOutput: true` it can be `ref`'d |
| `assertion` | A SELECT that must return zero rows; stored as a view in the assertion dataset |
| `declaration` | No SQL; registers an external table for `ref()` |
| `test` | Unit test for a table/view's SQL with fake inputs (check current docs for support in the console vs. CLI) |

```sql
-- definitions/staging/stg_pos_order_lines.sqlx
config {
  type: "view",
  schema: "staging",
  description: "Latest CDC version per POS order line",
  columns: { order_id: "POS order id", line_no: "Line number within the order" }
}

SELECT
  CAST(order_id AS STRING) AS order_id,
  CAST(line_no AS INT64)   AS line_no,
  store_id, product_id, customer_id,
  TIMESTAMP(order_ts)      AS order_ts,
  qty, CAST(unit_price AS NUMERIC) AS unit_price, CAST(discount AS NUMERIC) AS discount,
  is_deleted, _source_ts
FROM ${ref("order_lines")}
QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id, line_no ORDER BY _source_ts DESC) = 1
```

### 5.5 Incremental tables in Dataform
```sql
-- definitions/marts/fct_sales_line.sqlx
config {
  type: "incremental",
  schema: "mart_sales",
  uniqueKey: ["order_id", "line_no"],          // → MERGE on these columns
  bigquery: {
    partitionBy: "order_date",
    clusterBy: ["store_id", "product_id"],
    updatePartitionFilter: "order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)",  // prune target in MERGE
    labels: { domain: "sales", layer: "mart" }
  },
  assertions: {
    uniqueKey: ["order_id", "line_no"],
    nonNull: ["order_id", "line_no", "order_date", "store_id"],
    rowConditions: ["qty <> 0 OR is_deleted"]
  },
  tags: ["sales", "15min"]
}

pre_operations {
  -- Compute the checkpoint once, as a script variable (keeps the main filter a constant)
  DECLARE checkpoint DEFAULT (
    ${when(incremental(),
      `SELECT TIMESTAMP_SUB(MAX(_source_ts), INTERVAL ${dataform.projectConfig.vars.lookback_hours} HOUR) FROM ${self()}`,
      `SELECT TIMESTAMP("1970-01-01")`)}
  );
}

SELECT
  order_id, line_no,
  DATE(order_ts, "Europe/Helsinki")   AS order_date,
  order_ts, store_id, product_id, customer_id, qty,
  qty * unit_price - discount          AS net_amount,
  is_deleted, _source_ts
FROM ${ref("stg_pos_order_lines")}
WHERE _source_ts > checkpoint
```

What Dataform runs on an incremental execution (simplified):

```sql
DECLARE checkpoint DEFAULT (SELECT TIMESTAMP_SUB(MAX(_source_ts), INTERVAL 3 HOUR)
                            FROM `retail-prod.mart_sales.fct_sales_line`);
MERGE `retail-prod.mart_sales.fct_sales_line` T
USING (SELECT ... FROM `retail-prod.staging.stg_pos_order_lines` WHERE _source_ts > checkpoint) S
ON T.order_id = S.order_id AND T.line_no = S.line_no
   AND T.order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)      -- updatePartitionFilter
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT (...) VALUES (...);
```

Notes:

- `${when(incremental(), ...)}` is the Dataform version of dbt's `{% if is_incremental() %}`.
- `protected: true` prevents a **full refresh** from rebuilding the table (use for tables whose history can't be recreated from the source).
- Full refresh: run with "full refresh" checked in the UI, `--full-refresh` in the CLI, or `fully_refresh_incremental_tables_enabled` in an invocation config.
- The `pre_operations` + `DECLARE` pattern is a documented Dataform trick. A filter written as a subquery (`WHERE ts > (SELECT MAX(ts) FROM ...)`) can prevent BigQuery from pruning partitions on the **source** in some cases; a script variable makes the filter a constant. Always confirm with a dry run (bytes processed).

### 5.6 Assertions
**Built-in assertions** in the config block (`uniqueKey`, `uniqueKeys`, `nonNull`, `rowConditions`) create assertion actions automatically. **Manual assertions** are SQLX files of type `assertion`:

```sql
-- definitions/assertions/sales_reconcile_with_pos.sqlx
config { type: "assertion", tags: ["sales"] }

SELECT f.order_date, f.total, c.control_total
FROM (SELECT order_date, SUM(net_amount) AS total
      FROM ${ref("fct_sales_line")}
      WHERE order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY) AND NOT is_deleted
      GROUP BY 1) f
JOIN ${ref("daily_control_totals")} c USING (order_date)
WHERE ABS(f.total - c.control_total) > 0.01
```

- If the query returns rows, the assertion **fails** and the invocation reports it.
- Assertions are views in the **assertion dataset**, so you can query them to see the failing rows.
- To make downstream actions wait for (and be blocked by) a table's assertions, use dependency settings such as `dependOnDependencyAssertions` / `includeDependentAssertions` (check current docs for exact names and behaviour).
- Like dbt tests, filter assertions to recent partitions on big tables; a full-table uniqueness check every 15 minutes is expensive.

### 5.7 JavaScript includes and definitions
- **`includes/*.js`**: constants and functions, available in every SQLX file by file name.

```javascript
// includes/constants.js
const TZ = "Europe/Helsinki";
const PAYMENT_TYPES = ["card", "cash", "mobile"];
module.exports = { TZ, PAYMENT_TYPES };
```

```sql
-- in a .sqlx file
SELECT order_id,
  ${constants.PAYMENT_TYPES.map(p => `SUM(IF(payment_type = '${p}', amount, 0)) AS ${p}_amount`).join(",\n  ")}
FROM ${ref("stg_pos_payments")}
GROUP BY order_id
```

- **`definitions/*.js`**: generate actions in code with `publish()`, `operate()`, `assert()`, `declare()`. This is how you build **many similar tables from a list** (like a dbt macro loop or an Airflow DAG factory):

```javascript
// definitions/staging_generated.js
const tables = ["customers", "products", "stores"];
tables.forEach(t => {
  publish(`stg_erp_${t}`, { type: "view", schema: "staging", tags: ["erp"] })
    .query(ctx => `SELECT * EXCEPT(_metadata) FROM ${ctx.ref(`raw_erp_${t}`)}`);
});
```

### 5.8 Environments and compilation overrides
- **Workspace compilation overrides** (repository settings): for example schema suffix `_${workspaceName}` so every developer writes into their own datasets automatically.
- **Release configurations**: one per environment, with overrides for **Google Cloud project**, **schema suffix**, **table prefix**, and **vars**. For example "prod" compiles `main` into `retail-prod`; "staging" compiles `main` into `retail-stage`.
- Common pattern: **one repository, one release config per environment**, or one Dataform repository per environment project, all pointing to the same git repo.
- `vars` let you pass values like a lookback window or a run date (from Composer) into SQL.

### 5.9 Scheduling Dataform
| Option | When |
|---|---|
| **Workflow configurations** (built in) | Simple cron schedules, no dependencies on other systems |
| **Cloud Composer** | Dataform must run **after ingestion** or before other steps; you already have Composer (common in GCP data platforms) |
| **Cloud Workflows + Cloud Scheduler** | Serverless, cheap, simple chains |
| Event-driven | Pub/Sub/Eventarc → Cloud Function → Dataform API |

```python
# Composer: compile from git, then invoke only the sales tag (Google provider operators)
from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator, DataformCreateWorkflowInvocationOperator)

compile_sales = DataformCreateCompilationResultOperator(
    task_id="dataform_compile",
    project_id="retail-transform", region="europe-north1", repository_id="retail-sales",
    compilation_result={
        "git_commitish": "main",
        "code_compilation_config": {"vars": {"run_date": "{{ ds }}"}},
    },
)
run_sales = DataformCreateWorkflowInvocationOperator(
    task_id="dataform_run_sales",
    project_id="retail-transform", region="europe-north1", repository_id="retail-sales",
    workflow_invocation={
        "compilation_result": "{{ task_instance.xcom_pull('dataform_compile')['name'] }}",
        "invocation_config": {"included_tags": ["sales"], "transitive_dependencies_included": False},
    },
    asynchronous=True,        # then wait with DataformWorkflowInvocationStateSensor
)
compile_sales >> run_sales
```

### 5.10 Git integration and CI
- Link the repository to a remote git repo (HTTPS token or SSH key in Secret Manager). Developers commit and push from workspaces, or work locally with the **Dataform CLI** (`npm i -g @dataform/cli`; `dataform compile`, `dataform run --dry-run`, `dataform run --tags sales --schema-suffix pr_123`, `dataform test`; check CLI docs for flags in your version).
- CI on a PR: `dataform compile` (catches syntax and graph errors), then `dataform run` with a **PR schema suffix** into a CI project, run assertions, then drop the PR datasets.
- Production runs always come from a **release configuration** pointing at `main` (or a tag), never from someone's workspace.

### 5.11 SCD2 in Dataform
Dataform has **no built-in snapshot**. Options:

1. A community package (for example `dataform-scd`) that generates the snapshot logic.
2. Build it yourself. A robust pattern: an **append-only history** of versions (incremental insert of rows whose attribute hash changed), plus a **view** that derives `valid_to` with `LEAD()`. This is simple and safe to rerun:

```sql
-- definitions/marts/customer_versions.sqlx: one row per new version
config {
  type: "incremental",
  schema: "mart_sales",
  uniqueKey: ["customer_id", "valid_from"],      // MERGE → rerun-safe
  bigquery: { partitionBy: "DATE(valid_from)", clusterBy: ["customer_id"] },
  protected: true                                 // history can't be rebuilt from current source
}

WITH src AS (
  SELECT customer_id, name, segment, city, updated_at AS valid_from,
         FARM_FINGERPRINT(TO_JSON_STRING(STRUCT(name, segment, city))) AS attr_hash
  FROM ${ref("stg_crm_customers")}
),
last_version AS (
  ${when(incremental(),
    `SELECT customer_id, attr_hash FROM ${self()}
     QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY valid_from DESC) = 1`,
    `SELECT CAST(NULL AS STRING) AS customer_id, CAST(NULL AS INT64) AS attr_hash LIMIT 0`)}
)
SELECT s.*
FROM src s
LEFT JOIN last_version l USING (customer_id)
WHERE l.attr_hash IS NULL OR l.attr_hash != s.attr_hash
```

```sql
-- definitions/marts/dim_customer.sqlx: SCD2 view for Looker
config { type: "view", schema: "mart_sales" }
SELECT
  FARM_FINGERPRINT(CONCAT(customer_id, CAST(valid_from AS STRING))) AS customer_sk,
  customer_id, name, segment, city, valid_from,
  COALESCE(LEAD(valid_from) OVER w, TIMESTAMP "9999-12-31") AS valid_to,
  LEAD(valid_from) OVER w IS NULL                            AS is_current
FROM ${ref("customer_versions")}
WINDOW w AS (PARTITION BY customer_id ORDER BY valid_from)
```

(If the CDC stream gives you every change, feed **all change events** into `customer_versions` instead of the current-state staging table; then you capture every intermediate version, not only the state at run time.)

---

## 6. Side-by-side comparison and when to choose each

| | **dbt** | **Dataform** |
|---|---|---|
| Owner | dbt Labs (Core open source; Cloud paid) | Google; part of BigQuery; free |
| Warehouses | Many (BigQuery, Snowflake, Databricks, Fabric, Redshift…) | BigQuery only |
| Templating | Jinja + YAML | JavaScript + SQLX config blocks |
| Dependency | `{{ ref('m') }}`, `{{ source('s','t') }}` | `${ref("m")}`, declarations |
| Materializations | view, table, incremental, ephemeral, materialized_view, snapshot; custom ones possible | view (incl. materialized), table, incremental, operations |
| Incremental strategies (BigQuery) | merge, insert_overwrite (dynamic/static, copy_partitions), microbatch | INSERT or MERGE (`uniqueKey`), `updatePartitionFilter`; partition replacement only by hand (operations / pre_operations) |
| SCD2 | **Snapshots** built in | Manual pattern or package |
| Tests | Generic, singular, package tests, **unit tests**, contracts, freshness | Assertions (built-in + manual), unit `test` actions |
| Slim CI | `state:modified+`, `--defer`, `dbt clone` | Compile + run selected actions with suffix; less built-in state comparison |
| Docs / lineage | `dbt docs`, large ecosystem (catalogs, Dataplex/other integrations) | Lineage graph in the console; integrates with Dataplex lineage |
| UI | dbt Cloud IDE (paid) or local editor | Web IDE in the BigQuery console (free) |
| Scheduling | External (Composer) or dbt Cloud | Workflow configs, Composer, Workflows |
| Ecosystem | Very large: packages, community, hiring pool | Smaller |
| Auth / IAM | Runner's service account / OAuth | GCP IAM natively, service account per workflow |

**When to choose dbt:**

- Multi-cloud or possible future warehouse changes; teams that already know dbt.
- You need snapshots, microbatch, unit tests, contracts, slim CI out of the box.
- Large projects (hundreds of models, many contributors) that benefit from packages and selectors.

**When to choose Dataform:**

- BigQuery-only platform, want **zero extra licences and infrastructure**.
- Analysts work in the BigQuery console; GCP IAM and audit logs in one place.
- Simple to medium projects; Google support for the whole stack.

**In short:** they solve the same problem: SQL models with dependencies, incremental logic, tests and environments in git. Dataform is native to BigQuery and free; dbt is multi-warehouse with a bigger ecosystem and more built-in features like snapshots, microbatch and slim CI. A good default is to follow what the team already uses; the design principles (layers, incremental with lookback, tests, CI on a PR dataset) are the same.

---

## 7. Core concepts, one by one (what → how → why → example → mistake)

### 7.1 `ref()` and the DAG
- **What:** a declared dependency on another model.
- **How:** recorded as an edge at parse time; replaced by the environment's table name at compile time.
- **Why:** correct order, parallelism, lineage, environment switching, selective builds (`+model+`).
- **Example:** `FROM {{ ref('stg_pos__order_lines') }}` / `FROM ${ref("stg_pos_order_lines")}`.
- **Mistake:** hard-coding `retail-prod.staging.order_lines`. The dependency is invisible, dev and CI read prod, and the order may be wrong.

### 7.2 Layers (staging → intermediate → marts)
- **What:** staging = 1:1 cleaning of each source (rename, cast, dedupe CDC); intermediate = joins and business rules; marts = facts and dimensions for consumers.
- **How:** folder structure + configs per folder (materialization, dataset, tags).
- **Why:** each layer has one job, so changes are local, and Looker only sees stable marts.
- **Mistake:** marts that read raw tables directly, or staging that already joins five sources.

### 7.3 Materialization choice
- **What / how:** see sections 3.2 and 4.3.
- **Why:** it decides cost and freshness. A view over a 2 TB table queried by Looker 1,000 times a day re-scans each time; a table built once is cheaper.
- **Mistake:** everything as `table` (full rebuilds every 15 minutes) or everything as `view` (slow, expensive dashboards).

### 7.4 Incremental logic
- **What:** process only new or changed rows.
- **How:** `is_incremental()` / `incremental()` adds a filter; the strategy decides MERGE / INSERT / partition replace.
- **Why:** cost and latency for near real-time.
- **Mistake:** no lookback, no `unique_key`, filter that scans the full source (section 10).

### 7.5 Tests and assertions
- **What:** SELECTs that return bad rows.
- **How:** run after the model (dbt build / Dataform invocation); failures block downstream (with the right settings).
- **Why:** "green pipeline ≠ correct data".
- **Mistake:** tests only in dev, or full-table tests on huge tables every run (expensive); no tests on business keys.

### 7.6 Environments and CI
- **What:** same code, different targets.
- **How:** dbt targets + `generate_schema_name`; Dataform compilation overrides.
- **Why:** safe development, PR validation.
- **Mistake:** developers sharing one dev dataset and overwriting each other; CI building into prod datasets.

### 7.7 Idempotency in transformations
- **What:** rerunning the model gives the same result.
- **How:** MERGE on keys; partition overwrite; deterministic SQL (no `CURRENT_TIMESTAMP()` as a business value, no `ROW_NUMBER()` without a full tie-breaker, no `LIMIT` without `ORDER BY`).
- **Why:** Airflow retries and backfills call dbt/Dataform again.
- **Mistake:** `ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _source_ts)` when two CDC events have the same `_source_ts` → a different row wins on each run. Add a tie-breaker (`ORDER BY _source_ts DESC, _change_seq DESC`).

---

## 8. What matters most in practice

### 8.1 Moving SQL out of Airflow
A legacy Composer setup often has DAGs with dozens of `BigQueryInsertJobOperator` tasks wired in SQL dependency order. Moving them to dbt/Dataform:

1. Each SQL becomes a model; hard-coded table names become `ref()`/`source()`.
2. The graph is now derived from code; Airflow keeps **one task per domain** (`dbt build --select tag:sales` or a Dataform invocation with `included_tags`).
3. Add tests at the same time (keys unique and not null at least).
4. Compare outputs old vs. new (`audit_helper.compare_relations`, or `EXCEPT DISTINCT` both ways) before switching.
Result: fewer DAGs and edges in Airflow, tests and lineage for free, simpler reruns (`dbt build --select model+`).

### 8.2 The near real-time sales model with incremental MERGE
- Source: Datastream CDC tables (or `_cdc` change tables) in the ingestion project.
- Staging dedupes to the latest version per key, with a deterministic tie-breaker.
- `fct_sales_line`: incremental **merge** on `(order_id, line_no)`, partitioned by `order_date`, clustered by `store_id, product_id`, with a **lookback** on `_source_ts` and **target partition pruning** (`incremental_predicates` / `updatePartitionFilter`).
- Run every 5–15 minutes (Composer Asset trigger or Dataform workflow config). Each run should scan **megabytes to a few GB**, not the whole table; check `total_bytes_processed` in `INFORMATION_SCHEMA.JOBS`.
- Deletes: CDC deletes arrive as `is_deleted = true`; either keep a soft-delete flag (Looker filters it) or add `WHEN MATCHED AND S.is_deleted THEN DELETE` via a custom merge (in dbt with a custom strategy/macro; in Dataform with an `operations` MERGE).
- Daily: `agg_sales_daily` with **insert_overwrite** for the last N days, cheap and idempotent.

### 8.3 Incremental correctness: lookback, keys, late data
- **Lookback** (re-read a window before the watermark) catches late or out-of-order events. Without it, a row that arrives with an older `_source_ts` than the current max is **lost forever**.
- **unique_key + MERGE** makes the overlap harmless (rows in the window are updated, not duplicated).
- **Target pruning window** must be larger than the real lateness of updates (measure it: `MAX(_ingested_at - order_ts)`).
- **Periodic full refresh or reconciliation** (weekly, or a reconciliation test) catches drift.

### 8.4 Cost control in models
- Partition + cluster every big model; set `require_partition_filter` on huge facts so Looker queries must filter.
- Incremental filters that **prune the source** (a constant or a script variable on the partition/cluster column).
- Prune the **target** in MERGE.
- Tests with `where:` on recent partitions.
- `maximum_bytes_billed` in dev/CI profiles.
- **Labels** on every model (`labels={'domain':'sales'}` / `bigquery.labels`) to attribute cost per domain in `INFORMATION_SCHEMA.JOBS`. dbt also adds a `dbt_invocation_id` query comment/label option (`query-comment` with `job-label: true`); check current docs.

### 8.5 Tests that matter (not 500 useless tests)
- On every mart: **primary key unique + not null** (the grain).
- **Relationships** from facts to dimensions (orphan keys break Looker joins).
- **Accepted values** on status/channel columns.
- **Reconciliation** with the source (totals per day).
- **Freshness** on sources.
- **Unit tests** for tricky logic (net amount, currency conversion, SCD logic).
- Severity: `error` for things that make data wrong; `warn` for things to investigate.

### 8.6 CI on a PR dataset
```text
PR opened
 ├─ sqlfluff lint (dialect bigquery, templater dbt/jinja)
 ├─ dbt deps && dbt parse            | dataform compile
 ├─ download prod manifest from GCS  |
 ├─ dbt build --select state:modified+ --defer --state prod/ --target ci
 │     (target ci: project retail-ci, dataset pr_<number>, maximum_bytes_billed set)
 │                                   | dataform run --schema-suffix pr_<number> --tags <changed>
 ├─ contracts + tests + unit tests must pass
 └─ cleanup: drop datasets pr_<number>_* after merge/close (or set default table expiration on CI datasets)
Merge to main
 ├─ build docs, upload prod manifest after the next prod run
 └─ release: new dbt image tag (used by Composer) | Dataform release config compiles main
```
Use **Workload Identity Federation** for CI authentication, a dedicated **CI project** with a budget, and CI datasets with a default **table expiration** so forgotten PR tables disappear.

### 8.7 Orchestration boundary
Airflow triggers; dbt/Dataform know the models. Pass **parameters** (like `run_date` for backfills) from Airflow as vars. For a backfill of a partition-based model: `dbt build --select agg_sales_daily --vars '{"start_date":"2026-09-01","end_date":"2026-09-30"}'` with the model using those vars, or microbatch's `--event-time-start/--event-time-end`. Emit an **Asset event** after a successful build so downstream (Looker refresh) can react.

### 8.8 Documentation and handover
Descriptions in YAML / SQLX `description` and `columns`, persisted to BigQuery; generated lineage; a README per domain explaining grain, keys, refresh frequency, owners and tests. When patterns are handed over to internal teams, a template project ("how we build an incremental fact") is a good deliverable.

---

## 9. Worked project: the retail sales mart

### 9.1 Model graph

```text
sources (retail-ingest)                staging (views)            intermediate          marts (mart_sales)
raw_pos.order_lines ──────────► stg_pos__order_lines ─────┐
raw_pos.daily_control_totals (for reconciliation)          ├─► int_sales__lines_enriched ─► fct_sales_line (incremental merge, 15 min)
raw_erp.products ─────────────► stg_erp__products ────────┤                              └► agg_sales_daily (insert_overwrite, last 3 days)
raw_erp.stores ───────────────► stg_erp__stores ──────────┘
raw_crm.customers ────────────► stg_crm__customers ─► snap_customer (SCD2) ─► dim_customer
                                stg_erp__products ─► dim_product      stg_erp__stores ─► dim_store
```

Grain: `fct_sales_line` = one row per order line (latest state). `dim_customer` = one row per customer version.

### 9.2 dbt version of the key pieces

`fct_sales_line.sql` is in section 4.4 (merge, lookback, `incremental_predicates`). It reads from `int_sales__lines_enriched` in the full project; in the example it reads staging directly to keep it short.

```sql
-- models/marts/sales/agg_sales_daily.sql
{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'order_date', 'data_type': 'date', 'copy_partitions': true},
    cluster_by=['store_id'],
    tags=['sales', 'nightly']
) }}
SELECT order_date, store_id, product_id,
       SUM(qty) AS qty, SUM(net_amount) AS net_amount, COUNT(DISTINCT order_id) AS orders
FROM {{ ref('fct_sales_line') }}
WHERE NOT is_deleted
{% if is_incremental() %}
  AND order_date >= DATE_SUB(CURRENT_DATE('Europe/Helsinki'), INTERVAL 3 DAY)   -- replaces 4 full partitions
{% endif %}
GROUP BY 1, 2, 3
```

(Using `CURRENT_DATE` here is acceptable because the strategy **replaces whole partitions**: rerunning gives the same result for those days. For backfills, swap it for a var: `{{ var('start_date', ...) }}`.)

```sql
-- models/marts/sales/dim_customer.sql   (on top of the snapshot from section 4.5)
{{ config(materialized='table', cluster_by=['customer_id']) }}
SELECT
  {{ dbt_utils.generate_surrogate_key(['customer_id', 'dbt_valid_from']) }} AS customer_sk,
  customer_id, name, segment, city,
  dbt_valid_from                                       AS valid_from,
  COALESCE(dbt_valid_to, TIMESTAMP '9999-12-31')       AS valid_to,
  dbt_valid_to IS NULL                                 AS is_current
FROM {{ ref('snap_customer') }}
```

Tests: the YAML in section 4.6 (composite key, not null, relationships, contract, unit test) plus the reconciliation singular test. Add to `dim_customer`: `unique` on `customer_sk`, and a test that each `customer_id` has exactly one `is_current = true` row:

```sql
-- tests/assert_one_current_customer_version.sql
SELECT customer_id FROM {{ ref('dim_customer') }}
WHERE is_current GROUP BY customer_id HAVING COUNT(*) > 1
```

Joining the fact to the SCD2 dimension "as of" the order time (point-in-time) is done in the mart or in LookML:

```sql
LEFT JOIN {{ ref('dim_customer') }} c
  ON f.customer_id = c.customer_id
 AND f.order_ts >= c.valid_from AND f.order_ts < c.valid_to
```

### 9.3 Dataform version of the key pieces
- `fct_sales_line.sqlx`: section 5.5 (MERGE with `uniqueKey`, `updatePartitionFilter`, `pre_operations` checkpoint, built-in assertions).
- `customer_versions.sqlx` + `dim_customer.sqlx`: section 5.11.
- Reconciliation assertion: section 5.6.
- Daily aggregate with partition replacement (Dataform has no insert_overwrite strategy, so use `operations` or an incremental table with a delete in `pre_operations`):

```sql
-- definitions/marts/agg_sales_daily.sqlx
config {
  type: "incremental",
  schema: "mart_sales",
  bigquery: { partitionBy: "order_date", clusterBy: ["store_id"] },
  tags: ["sales", "nightly"]
}
pre_operations {
  ${when(incremental(),
    `DELETE FROM ${self()} WHERE order_date >= DATE_SUB(CURRENT_DATE("Europe/Helsinki"), INTERVAL 3 DAY)`)}
}
SELECT order_date, store_id, product_id,
       SUM(qty) AS qty, SUM(net_amount) AS net_amount, COUNT(DISTINCT order_id) AS orders
FROM ${ref("fct_sales_line")}
WHERE NOT is_deleted
${when(incremental(), `AND order_date >= DATE_SUB(CURRENT_DATE("Europe/Helsinki"), INTERVAL 3 DAY)`)}
GROUP BY 1, 2, 3
```
The DELETE and the INSERT run in the same script; for strict atomicity, wrap them in a transaction or use an `operations` action with `BEGIN TRANSACTION ... COMMIT TRANSACTION`.

### 9.4 Orchestration
- `fct_sales_line` (tag `15min`): Composer DAG `transform__sales_15min` scheduled on the Asset `raw_pos.order_lines` (or every 15 minutes with `max_active_runs=1`), one task `dbt build --select tag:15min` / Dataform invocation `included_tags: ["15min"]`. Then emit Asset `mart_sales.fct_sales_line`.
- Nightly (tag `nightly`): snapshot, dimensions, `agg_sales_daily`, reconciliation tests.

### 9.5 CI on a PR dataset
As section 8.6. Concretely for dbt, the CI target:

```yaml
    ci:
      type: bigquery
      method: oauth
      project: retail-ci
      dataset: "pr_{{ env_var('PR_NUMBER') }}"
      location: EU
      threads: 8
      maximum_bytes_billed: 20000000000
```

```bash
gcloud storage cp gs://retail-dbt-artifacts/prod/manifest.json prod/
dbt deps
dbt build --target ci --select state:modified+ --defer --state prod/
```
For Dataform: `dataform compile && dataform run --schema-suffix "pr_${PR_NUMBER}" --tags sales` with CI credentials pointing at `retail-ci`.

---

## 10. Common mistakes (and the fix)

| Mistake | What goes wrong | Fix |
|---|---|---|
| **Incremental without a lookback** (`WHERE ts > MAX(ts)`) | Late or out-of-order rows are skipped forever | Lookback window (`MAX(ts) - INTERVAL 3 HOUR`) + MERGE on key |
| **Missing `unique_key`** on merge | MERGE becomes INSERT; the lookback creates duplicates | Always set the grain's key; test it with `unique_combination_of_columns` / `uniqueKey` assertion |
| **Incremental filter causes a full scan** | Filter on a non-partition column, or a function on the partition column, or a subquery that blocks pruning; every 15-minute run scans TBs | Filter the **source** on its partition/cluster column with a constant or script variable; dry-run to check bytes |
| **MERGE scans the whole target** | Cost grows with table size every run | `incremental_predicates` (dbt) / `updatePartitionFilter` (Dataform); or insert_overwrite for aggregates |
| **`SELECT *` in models** | Upstream column added/removed silently changes marts; `on_schema_change: ignore` hides it; more bytes scanned | Explicit column lists in staging and marts; contracts on marts |
| **Non-deterministic models** | Reruns give different results: `ROW_NUMBER()` without full tie-breaker, `LIMIT` without `ORDER BY`, `CURRENT_TIMESTAMP()` as a business column, `RAND()` | Deterministic ordering with tie-breakers; take timestamps from the data; use run vars for dates |
| **Wrong grain / fan-out joins** | Joining facts to a dimension with duplicate keys multiplies revenue | Unique tests on dimension keys; relationship tests; check row counts before/after joins |
| **`on_schema_change` left at `ignore`** | New column never appears in the incremental table | `append_new_columns` or `sync_all_columns`, or `fail` to force a decision |
| **Snapshot from a current-state table when you need full history** | Intermediate changes between runs are lost | Build SCD2 from CDC change events |
| **Full refresh of a protected history table** | History lost | `full_refresh: false` (dbt config) / `protected: true` (Dataform); never drop snapshots |
| **Overusing ephemeral** | Huge compiled SQL, logic repeated in many queries, hard to debug | Use views or tables for shared logic |
| **Tests scanning full big tables every run** | Testing costs more than building | `where:` config on recent partitions, or test on nightly runs only |
| **Hard-coded project/dataset names** | Dev and CI read/write prod | `ref()` / `source()` / declarations everywhere |
| **dbt installed into Composer's Python** | Package conflicts break Composer | Run dbt in its own container image |
| **No `generate_schema_name` override** | Datasets named `analytics_mart_sales` in prod | Override the macro deliberately |

---

## 11. Mapping from other platforms

| Other platform | dbt / Dataform equivalent | Note |
|---|---|---|
| **Fabric notebooks** with Spark SQL cells building Delta tables in order | Models with `ref()`; the framework decides the order | No manual cell/notebook ordering |
| Fabric **medallion** (bronze/silver/gold lakehouses) | Sources → staging → intermediate → marts | Same layering idea |
| Spark **Delta `MERGE INTO`** for upserts | Incremental `merge` strategy / Dataform `uniqueKey` | Same SQL concept; on BigQuery watch target pruning |
| Spark **`insertOverwrite` with dynamic partition overwrite** | dbt `insert_overwrite` (dynamic) | Same semantics: replace touched partitions |
| Spark **watermark / checkpoint** in incremental jobs | `MAX(_source_ts)` from `{{ this }}` / `${self()}` with lookback | In batch SQL, the "checkpoint" is the target table itself |
| Fabric **Warehouse stored procedures** called from pipelines | Models (or Dataform `operations` / dbt `run-operation` for procedural steps) | Prefer declarative SELECT models over procedures |
| Fabric **Data Pipeline** running notebooks in sequence | Airflow triggers `dbt build` / Dataform invocation | The pipeline no longer knows the model order |
| Delta **time travel / history** for SCD | dbt **snapshots** / SCD2 pattern | Time travel is not SCD2; BigQuery time travel is only 2–7 days |
| Great Expectations / manual checks in notebooks | dbt tests, Dataform assertions | Declarative, run with every build |
| AWS **Glue jobs / Athena CTAS** | `table` materialization (`CREATE OR REPLACE TABLE AS SELECT`) | |
| Git + PR checks on Spark code | Slim CI on a PR dataset | Builds only changed models against prod upstreams |
| dbt in Fabric (dbt-fabric adapter) | Same dbt, BigQuery adapter | Models move with small SQL dialect changes |

In short: in Fabric or Databricks, a bronze-silver-gold pipeline built with notebooks and MERGE into Delta tables must know the order of steps. With dbt or Dataform, each model is a SELECT with `ref()`; the framework builds the graph, applies MERGE or partition overwrite, and runs tests after each model. Upserts, partition overwrite and watermarks with a safety window map directly, but the order, tests and environments come for free.

---

## 12. Hands-on exercise (3–4 hours)

**Important:** the free **BigQuery sandbox** has limits; at the time of writing it does **not support DML** (so MERGE-based incremental runs fail) and tables expire after 60 days (check current sandbox limits). For views and tables the sandbox is fine. For incremental models, enable billing on a personal project: small tests stay within the free tier (1 TB of queries per month), and set `maximum_bytes_billed`.

### Part A: dbt Core locally
```bash
python3 -m venv .venv && source .venv/bin/activate
pip install dbt-core dbt-bigquery          # pin versions in real projects
gcloud auth application-default login       # for method: oauth
dbt init retail                             # choose bigquery, oauth, your project, dataset dbt_<you>, location US
cd retail && dbt debug                      # checks the connection
```

1. **Source:** declare `bigquery-public-data.thelook_ecommerce.order_items` and `.users` as sources (database `bigquery-public-data`, schema `thelook_ecommerce`; the dataset is in the US location, so use location US).
2. **Staging views:** `stg_thelook__order_items` (cast, rename), `stg_thelook__users`.
3. **Table:** `dim_user` with `cluster_by`.
4. **Incremental:** `fct_order_items` partitioned by `DATE(created_at)`, `unique_key=['id']`, merge, with a lookback on `created_at`. Run `dbt build` twice. In the BigQuery console, open **Job history** and compare the compiled MERGE and **bytes processed** of run 1 (CREATE) vs. run 2 (MERGE). Then add `incremental_predicates` and compare again.
5. **Tests:** `unique` + `not_null` on keys, `relationships` from fact to `dim_user`, one singular test, one unit test (dbt 1.8+). Break a test on purpose and see downstream models get skipped in `dbt build`.
6. **Snapshot:** create a small table of your own (`CREATE TABLE dbt_<you>.customers AS SELECT 1 id, 'A' segment, CURRENT_TIMESTAMP() updated_at`), snapshot it, update the row, snapshot again, look at `dbt_valid_from/to`.
7. **Look inside:** open `target/compiled/` and `target/run/` to see the SQL dbt generated; open `target/manifest.json` and find `depends_on` for your fact.
8. **Selectors:** `dbt build --select +fct_order_items`, `dbt ls --select tag:...`.
9. **Docs:** `dbt docs generate && dbt docs serve`; view the lineage graph.
10. **Slim CI simulation:** copy `target/manifest.json` to `prod/`, change one model, run `dbt ls --select state:modified+ --state prod/`.

### Part B: Dataform in the BigQuery console
1. BigQuery console → **Dataform** → **Create repository** (choose region; set a service account per the setup prompts).
2. Create a **development workspace**, click **Initialize workspace**.
3. In `definitions/`, add a **declaration** for `bigquery-public-data.thelook_ecommerce.order_items` (pick a repository region/location compatible with the US public dataset; location mismatches are a common error).
4. Add `stg_order_items.sqlx` (view) and `fct_order_items.sqlx` (incremental, `uniqueKey: ["id"]`, `partitionBy`, built-in assertions).
5. Look at **Compiled graph** and the **compiled queries** of the incremental table (both the full and the incremental versions).
6. **Start execution** → run all; run again; compare the job SQL and bytes in job history.
7. Add a manual assertion that fails; see how it's reported.
8. Optional: connect the repository to a GitHub repo, create a **release configuration** (compile `main`) and a **workflow configuration** (run tag `daily` every day).

### Part C: connect to Airflow (if you did the Composer and Airflow lab)
Run `dbt build --select fct_order_items` from a `BashOperator` in your local Astro project (fine for a lab; use a container in production) and pass `{{ ds }}` as a var.

---

## 14. Glossary

| Term | Meaning |
|---|---|
| **ELT** | Extract, Load, then Transform inside the warehouse |
| **Model** | A SELECT that defines a table or view (dbt `.sql`, Dataform `.sqlx`) |
| **Source (dbt) / declaration (Dataform)** | An external table the framework reads but doesn't build |
| **`ref()`** | Declares a dependency and resolves the table name per environment |
| **Manifest** | dbt's JSON of all nodes, configs and dependencies (`manifest.json`) |
| **Compiled graph / compilation result** | Dataform's compiled set of actions and dependencies |
| **Materialization** | How a model is persisted: view, table, incremental, ephemeral, materialized view |
| **Incremental model** | Processes only new/changed rows on later runs |
| **`is_incremental()` / `incremental()`** | True when the target exists and it isn't a full refresh |
| **`{{ this }}` / `${self()}`** | The model's own target table |
| **`unique_key` / `uniqueKey`** | Key columns used to MERGE |
| **Incremental strategy** | merge, insert_overwrite, microbatch (dbt on BigQuery) |
| **`incremental_predicates` / `updatePartitionFilter`** | Extra filter on the target in MERGE to prune partitions |
| **Lookback window** | Re-reading a period before the watermark to catch late data |
| **Full refresh** | Rebuild an incremental model from scratch |
| **Snapshot** | dbt's SCD2 mechanism (timestamp or check strategy) |
| **SCD2** | Slowly Changing Dimension type 2: keep history with valid_from/valid_to |
| **Generic / singular test** | Reusable YAML test / one-off SQL test returning bad rows |
| **Unit test (dbt 1.8+)** | Fixed inputs → expected outputs for a model's logic |
| **Assertion (Dataform)** | A query that must return zero rows |
| **Contract** | Enforced column names and types for a model |
| **Seed** | Small CSV loaded as a table |
| **Macro** | Reusable Jinja function (dbt) |
| **Includes** | JavaScript files with constants/functions (Dataform) |
| **Operations** | Dataform action with custom SQL statements |
| **Target / profile** | dbt connection and environment settings |
| **`generate_schema_name`** | dbt macro deciding dataset names |
| **Compilation overrides** | Dataform per-environment project, schema suffix, table prefix, vars |
| **Workspace** | Dataform's per-developer editable branch in the console |
| **Release configuration** | Dataform: compile a git ref with overrides, on a schedule |
| **Workflow configuration** | Dataform: scheduled execution of selected actions |
| **Workflow invocation** | One Dataform execution |
| **Selector / tag** | Ways to choose a subset of models to run |
| **State / defer** | Comparing with a prod manifest; resolving unbuilt refs to prod |
| **Slim CI** | Building only changed models (and children) in CI |
| **Cosmos** | Library that renders a dbt project as Airflow tasks |
| **dbt Core / dbt Cloud** | Open-source CLI / paid SaaS platform |
| **Lineage** | The graph showing which tables feed which |
| **Grain** | What one row of a table represents |
