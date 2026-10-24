# Cloud Composer (Airflow) + Dataform/dbt

> Simplifying warehouse load chains and designing Composer/Airflow DAGs at scale, with Dataform/dbt as the transformation layer. Remember: **failed reruns usually mean non-idempotent tasks**.

---

## 1. Cloud Composer: what it is

**Managed Apache Airflow.** A Composer environment contains:

```
Composer environment
 ├─ GKE (Autopilot in Composer 2/3): scheduler, workers, triggerer, web server
 ├─ Cloud SQL: Airflow metadata DB (DAG runs, task instances, XCom)
 └─ GCS bucket
      ├─ dags/      ← deploy DAG files here (CI/CD copies them)
      ├─ plugins/
      └─ data/, logs/
```

- **Versions:** Composer 2 and **Composer 3** (simpler networking, more managed). Know which Airflow version you're on: **Airflow 2.x** vs. **Airflow 3** (Datasets were renamed **Assets** in Airflow 3).
- **Scaling:** worker autoscaling (min/max), scheduler count, environment size. Cost = an always-on environment plus workers.
- **Environments per stage:** dev and prod Composer (or dev = local Airflow via Docker / `composer-dev`).
- Python packages are installed via PyPI settings on the environment. **Pin versions**, because a bad package update can break all DAGs.

---

## 2. Airflow core concepts (quick refresher)

| Concept | Meaning |
|---|---|
| **DAG** | A workflow definition (Python), with a schedule, start_date and catchup |
| **Task / Operator** | A unit of work: `BigQueryInsertJobOperator`, `PythonOperator`, `KubernetesPodOperator`, … |
| **DAG run** | One execution for one **logical date / data interval** |
| **`data_interval_start/end`** | The time window this run is responsible for. **The key to idempotency** |
| **Catchup** | If `True`, Airflow creates runs for all missed intervals since `start_date` |
| **Sensor** | Waits for a condition (file, table, other DAG) |
| **Deferrable operator** | Waits asynchronously in the **triggerer**, without occupying a worker slot |
| **XCom** | Small metadata passing between tasks (**not data**) |
| **Pool** | Limits concurrency for a resource, e.g. max 10 parallel BigQuery jobs |
| **Dataset / Asset** | Data-aware scheduling: a DAG runs when an upstream DAG **updates** a dataset |
| **Dynamic task mapping** | `.expand()`: create N tasks at runtime (one per table or file) |
| **TaskFlow API** | `@dag` / `@task` decorators, a cleaner Python style |

---

## 3. The 7 principles for DAGs at scale

### Principle 1: Airflow orchestrates, it doesn't compute

- ❌ `PythonOperator` that downloads 5 GB into pandas on the worker
- ✅ `BigQueryInsertJobOperator` that runs SQL **in BigQuery**; Dataflow/Dataproc for heavy non-SQL work; `KubernetesPodOperator` / Cloud Run jobs for custom containers
- Workers stay small and stable, and failures are about the job, not out-of-memory workers.

### Principle 2: idempotent tasks (fixes "failed reruns")

| ❌ Not idempotent | ✅ Idempotent |
|---|---|
| `INSERT INTO t SELECT ... WHERE dt = CURRENT_DATE()` | Overwrite partition `t${{ ds_nodash }}` with `WRITE_TRUNCATE` |
| Uses "now" | Uses `{{ data_interval_start }}` / `{{ ds }}` |
| `CREATE TABLE t` (fails if it exists) | `CREATE OR REPLACE` / `CREATE TABLE IF NOT EXISTS` |
| Appends files with random names | Deterministic output paths per interval |
| Side effects such as emails or API calls inside the transformation task | Side effects separated, guarded |

**Definition:** running the same task for the same interval **once or ten times gives the same result.**

```python
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

load_sales = BigQueryInsertJobOperator(
    task_id="load_fct_sales_partition",
    configuration={
        "query": {
            "query": "{% include 'sql/fct_sales.sql' %}",   # uses @run_date
            "useLegacySql": False,
            "destinationTable": {
                "projectId": "{{ var.value.project }}",
                "datasetId": "mart_sales",
                "tableId": "fct_sales_line${{ ds_nodash }}",   # ← partition decorator
            },
            "writeDisposition": "WRITE_TRUNCATE",                 # ← overwrite only this partition
            "queryParameters": [{"name": "run_date", "parameterType": {"type": "DATE"},
                                 "parameterValue": {"value": "{{ ds }}"}}],
        },
        "labels": {"pipeline": "sales", "layer": "mart"},          # ← cost attribution
    },
)
```

### Principle 3: safe backfills

- Follows from idempotency: `airflow dags backfill -s 2026-09-01 -e 2026-09-30 sales_daily`, or **clear** task instances in the UI.
- Control the blast radius with `max_active_runs`, `max_active_tasks`, **pools** for BigQuery, and running in batches (weeks, not years at once).
- Large historical rebuilds are often better as **one set-based SQL** over the whole range than 365 daily runs.

### Principle 4: express dependencies properly

| Situation | Use |
|---|---|
| Steps within one pipeline | Task dependencies `a >> b` inside **one DAG** |
| SQL model dependencies (stg → int → mart) | **dbt/Dataform `ref()`**, so Airflow doesn't need to know them |
| DAG B needs DAG A's output | **Datasets/Assets** (data-aware scheduling) instead of `ExternalTaskSensor` |
| Waiting on an external system (file arrives, API ready) | **Deferrable** sensor, or event-driven (GCS notification → Pub/Sub → trigger) |
| Tightly coupled DAGs that always run together | **Merge them** into one DAG |

```python
from airflow import Dataset          # Airflow 3: from airflow.sdk import Asset
from airflow.decorators import dag, task
import pendulum

RAW_POS = Dataset("bigquery://proj/raw_pos/orders")

@dag(schedule="*/15 * * * *", start_date=pendulum.datetime(2026, 1, 1), catchup=False)
def ingest_pos():
    @task(outlets=[RAW_POS])
    def check_and_publish(): ...
    check_and_publish()

@dag(schedule=[RAW_POS], start_date=pendulum.datetime(2026, 1, 1), catchup=False)
def build_sales_mart():           # runs whenever RAW_POS is updated, no sensor needed
    ...
```

**Why `ExternalTaskSensor` chains are bad at 300 DAGs:**

- They couple schedules (logical dates must align, or you need `execution_delta`)
- They hold worker slots while waiting (unless deferrable)
- Failures cascade, nobody sees the full graph, and reruns become a puzzle

### Principle 5: reusable ingestion patterns with a DAG factory

```yaml
# configs/sources/pos_orders.yaml
source: pos_orders
type: datastream          # or: gcs_files, api, jdbc
owner: sales-domain
schedule: "*/15 * * * *"
target_dataset: raw_pos
freshness_sla_minutes: 15
quality_checks: [not_null:order_id, unique:order_id,line_no]
```

```python
# dags/factory.py: one file generates all ingestion DAGs from configs
import glob, yaml
from airflow import DAG

for path in glob.glob("/home/airflow/gcs/dags/configs/sources/*.yaml"):
    cfg = yaml.safe_load(open(path))
    dag_id = f"ingest__{cfg['source']}"
    globals()[dag_id] = build_ingestion_dag(cfg)   # standard tasks: load → validate → publish dataset
```

Result: **onboarding a new source = one YAML file in a PR.** Every DAG gets the same retries, alerting, labels, quality checks and naming.

### Principle 6: reliability by default

```python
default_args = {
    "owner": "sales-domain",
    "retries": 3,
    "retry_delay": pendulum.duration(minutes=2),
    "retry_exponential_backoff": True,
    "execution_timeout": pendulum.duration(hours=1),
    "on_failure_callback": alert_to_chat,      # Slack/Teams/PagerDuty
}
```

- **Data-quality checks as tasks** (dbt tests, Dataform assertions, `BigQueryCheckOperator`). **A green DAG ≠ correct data.**
- Freshness monitoring outside the DAG: did the data actually arrive?
- **Pools** to avoid flooding BigQuery or the source DB.
- Keep **top-level DAG code light**: no API or DB calls at import time, because the scheduler parses files constantly.

### Principle 7: observability and metrics (load duration, throughput, stability, cost)

| Metric | Source |
|---|---|
| DAG/task duration and trend | Airflow metadata DB / Composer monitoring |
| Failure rate, retries per task | Airflow metadata |
| Sensor wait time | Task duration of sensors |
| Freshness (end-to-end) | `now - MAX(_source_ts)` in BigQuery |
| Cost per pipeline | BigQuery job **labels** + `INFORMATION_SCHEMA.JOBS` |
| Rows processed | Task logs / audit table written by each load |

Build one **Looker dashboard** from these. It makes load duration, throughput, stability and cost visible and trackable.

---

## 4. Restructuring hundreds of tangled DAGs

1. **Inventory and measure** (week 1)
    - Parse all DAGs and extract dependencies (sensors, triggers) to build the **real dependency graph**
    - From the metadata DB: failure rate, duration, retries and sensor wait per DAG; the **critical path** of the nightly chain
    - Classify failures: upstream not ready / data issue / infrastructure / **non-idempotent rerun**
2. **Define the target design**
    - **Ingestion layer:** DAG-factory-generated DAGs, one per source, in the shared ingestion project, publishing a Dataset
    - **Transformation layer:** **dbt/Dataform** per domain, triggered by Datasets; model dependencies via `ref()`
    - **Serving layer:** Looker datagroup triggers, or MV refresh
    - Standards: naming, default_args, labels, alerting, pools, idempotency rules
3. **Migrate domain by domain** (strangler pattern)
    - Pick the domain with the **most failures / highest business value** first
    - Run old and new **in parallel** and compare outputs, then switch and delete the old DAGs
4. **Enforce in CI** so it doesn't decay again (section 6)
5. **Prove it**: before/after on DAG count, failure rate, nightly runtime, cost

Typical outcome: *"300 DAGs often collapse to a few dozen ingestion DAGs from one factory, plus one transformation DAG per domain."*

---

## 5. Dataform vs. dbt (the transformation layer)

| | **Dataform** | **dbt (Core / Cloud)** |
|---|---|---|
| Vendor | Google, **integrated in BigQuery** (free; you pay BigQuery compute) | dbt Labs; open source Core + paid Cloud |
| Language | **SQLX** (SQL + config block + JavaScript) | SQL + **Jinja** + YAML |
| Warehouses | BigQuery only | Many |
| Dependencies | `${ref("table")}` | `{{ ref('model') }}` |
| Tests | **Assertions** (uniqueness, nonNull, rowConditions, custom) | Tests (unique, not_null, relationships, accepted_values, custom, unit tests) |
| Incremental | `type: "incremental"` + `${when(incremental(), ...)}` | `materialized='incremental'`, strategies `merge`, `insert_overwrite` |
| SCD2 | Manual / community patterns | **Snapshots** built in |
| Orchestration | Workflow configs (built-in scheduling) or Composer/Workflows | dbt Cloud jobs or Airflow (Cosmos, BashOperator) |
| Ecosystem | Smaller | Large (packages, docs site) |

In short: both solve the same problem: SQL models with dependencies, tests and incremental logic, version-controlled. On a BigQuery-only platform Dataform is native and free. dbt has a larger ecosystem. Usually follow what the team already uses.

### Dataform example (SQLX)

```sql
config {
  type: "incremental",
  schema: "mart_sales",
  uniqueKey: ["order_id", "line_no"],
  bigquery: { partitionBy: "order_date", clusterBy: ["store_id", "product_id"] },
  assertions: { uniqueKey: ["order_id", "line_no"], nonNull: ["order_id", "store_id"] },
  tags: ["sales", "15min"]
}

SELECT order_id, line_no, DATE(order_ts) AS order_date, store_id, product_id,
       qty, net_amount, _source_ts
FROM ${ref("stg_pos_order_lines")}
${when(incremental(),
  `WHERE _source_ts > (SELECT IFNULL(MAX(_source_ts), TIMESTAMP '1970-01-01') FROM ${self()})
       - INTERVAL 30 MINUTE`)}   -- overlap window, MERGE on uniqueKey makes it idempotent
```

### dbt example

```sql
-- models/marts/sales/fct_sales_line.sql
{{ config(materialized='incremental', unique_key=['order_id','line_no'],
          incremental_strategy='merge',
          partition_by={'field':'order_date','data_type':'date'},
          cluster_by=['store_id','product_id']) }}
SELECT * FROM {{ ref('stg_pos__order_lines') }}
{% if is_incremental() %}
WHERE _source_ts > (SELECT MAX(_source_ts) FROM {{ this }}) - INTERVAL 30 MINUTE
{% endif %}
```

```yaml
# models/marts/sales/_sales.yml
models:
  - name: fct_sales_line
    columns:
      - name: order_id
        tests: [not_null]
      - name: store_id
        tests:
          - relationships: {to: ref('dim_store'), field: store_id}
```

### Running dbt from Composer

- **Astronomer Cosmos** turns each dbt model into an Airflow task automatically (good visibility).
- Or a single `dbt build --select tag:sales` in a `KubernetesPodOperator` / Cloud Run job (simpler, with isolated dependencies).

---

## 6. Testing Airflow in CI/CD

```python
# tests/test_dag_integrity.py
import pytest
from airflow.models import DagBag

@pytest.fixture(scope="session")
def dagbag():
    return DagBag(dag_folder="dags/", include_examples=False)

def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}, dagbag.import_errors

def test_standards(dagbag):
    for dag_id, dag in dagbag.dags.items():
        assert dag.tags, f"{dag_id} has no tags"
        assert dag.default_args.get("retries", 0) >= 1, f"{dag_id} has no retries"
        assert dag.default_args.get("owner") not in (None, "airflow"), f"{dag_id} has no owner"

def test_factory_builds_expected_tasks():
    from dags.factory import build_ingestion_dag
    dag = build_ingestion_dag({"source": "x", "type": "gcs_files", "schedule": "@daily", ...})
    assert {"load", "validate", "publish"} <= set(dag.task_ids)
```

Pipeline:

```
PR opened
 ├─ lint: ruff/black, sqlfluff (SQL style)
 ├─ unit tests: pytest (DagBag integrity, factory, custom operators, Python transforms)
 ├─ SQL: BigQuery dry run of every query (syntax + bytes estimate)
 ├─ dbt/Dataform: compile + build + test on a PR-specific dev dataset (e.g. dbt --target ci, schema = pr_123)
 └─ plan for infrastructure changes (Terraform)
merge to main
 ├─ deploy DAGs to dev Composer bucket (gsutil rsync / gcloud composer environments storage dags import)
 ├─ smoke run in dev
 └─ promote to prod (manual approval or tag)
```

Tools: **Cloud Build** or GitHub Actions with **Workload Identity Federation** (no service account keys).

---

## 7. Airflow anti-patterns

| Anti-pattern | Why it's bad | Fix |
|---|---|---|
| Heavy code at DAG top level (DB calls, API calls) | The scheduler parses every file constantly, so the scheduler gets slow | Move it inside tasks |
| Passing data through XCom | The metadata DB bloats | Pass references (table names, GCS paths) |
| `datetime.now()` in task logic | Not idempotent, breaks backfills | `data_interval_start` |
| Dynamic `start_date` (`datetime.now()`) | Unpredictable scheduling | A fixed start_date |
| One mega-DAG for everything | Every failure blocks everything | Split by domain/layer, connect with Datasets |
| 300 copy-pasted DAGs | Drift, inconsistent fixes | DAG factory |
| Non-deferrable sensors with `mode="poke"` and long waits | Workers are blocked | Deferrable / reschedule mode / event-driven |
| `catchup=True` by accident on a new DAG with an old start_date | Hundreds of runs flood BigQuery | `catchup=False` by default; backfill deliberately |

---
