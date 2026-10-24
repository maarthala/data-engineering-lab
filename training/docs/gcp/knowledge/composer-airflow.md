# Cloud Composer and Apache Airflow from the inside

> **Goal of this lesson:** after reading it, you can explain how Airflow works inside, how Cloud Composer runs it on GCP, and how you would design and restructure DAGs at scale: idempotent, backfill-safe, well connected, tested, and observable.
>
> **Why it matters:** in many GCP data platforms, Cloud Composer is the central orchestrator. Typical work includes simplifying warehouse load chains, restructuring Composer/Airflow DAGs, running Airflow at scale with testing and CI/CD, and tracking metrics for load duration, throughput, stability and cost.
>
> **Quick reference:** [Composer, Airflow and Dataform deep dive](../deep-dives/composer-airflow-dataform.md) (cheat sheet). This lesson explains the *why* behind those notes.
>
> **Versions:** Airflow 2.x is still the most common version in production. Airflow 3 (released April 2025) changed several things. When a detail differs, this lesson says so. Always check which version your Composer environment runs.

---

## 1. What it is and why it exists

### The problem
A data warehouse is not one job. It is hundreds of jobs that must run **in the right order**, **at the right time**, and **again when they fail**. For example:

1. Wait until the POS export file lands in GCS.
2. Load it into a raw table.
3. Check it (row count, no null keys).
4. Build the staging, then the fact table, then the mart.
5. Tell Looker the data is fresh.

You could do this with cron and shell scripts. But cron cannot answer: *Did step 2 finish? What if step 3 failed yesterday? How do I rerun only last Tuesday? Which jobs are slow?* You need an **orchestrator**.

### What Airflow is
**Apache Airflow** is an open-source orchestrator. You write workflows as **Python code**. Each workflow is a **DAG** (Directed Acyclic Graph): a set of **tasks** with dependencies and no loops. Airflow:

- **schedules** each DAG (for example every day, or when upstream data changes),
- **runs** the tasks in dependency order on workers,
- **records** every run and every task attempt in a database,
- **retries** failed tasks and **alerts** people,
- gives you a **UI** to see history, logs, and to rerun things.

### The analogy
Think of Airflow as **the conductor of an orchestra**. The conductor does not play the violin. The conductor decides *who plays when*, keeps time, and notices when someone misses an entry. In data terms: **Airflow does not process data. BigQuery, Dataflow and Dataproc process data. Airflow tells them when to start and checks that they finished.** Remember this sentence; it is principle number one for good orchestration.

### What Cloud Composer is
**Cloud Composer** is Google's **managed Airflow**. Google runs the Airflow components for you on GKE (Kubernetes), with a Cloud SQL database and a GCS bucket for your DAG files. You upload Python files; Composer runs them. It is the GCP equivalent of **AWS MWAA** (Managed Workflows for Apache Airflow).

---

## 2. Where it fits in a modern GCP data platform

```text
Sources (POS, ERP, e-commerce DBs, files, APIs)
        │
        │  Datastream (CDC) / Dataflow / file drops
        ▼
┌─────────────────────────── shared ingestion projects ─────────────────────────┐
│  raw_* datasets in BigQuery  ◄── Composer: ingestion DAGs (DAG factory, YAML)  │
└───────────────────────────────────────────────────────────────────────────────┘
        │  Asset/Dataset event: "raw_pos.orders updated"
        ▼
┌─────────────────────────── EDW / domain projects ─────────────────────────────┐
│  staging → intermediate → marts   ◄── Composer triggers dbt/Dataform per domain│
│  data quality checks              ◄── tasks that block bad data                │
└───────────────────────────────────────────────────────────────────────────────┘
        │
        ▼
Looker (semantic layer) → conversational BI
```

In a typical EDW modernization on GCP, the data engineering team will:

1. **Inherit** a Composer environment with many DAGs that grew over years. Some are copy-pasted. Many wait on each other with sensors. Reruns fail.
2. **Simplify** the "load chains": fewer DAGs, clearer dependencies, SQL logic moved into dbt or Dataform.
3. **Standardize** ingestion with reusable patterns (a DAG factory driven by config).
4. **Make it production-grade**: idempotent tasks, safe backfills, tests in CI, alerts, metrics.
5. **Measure** load duration, throughput, stability and cost, and show before/after.

---

## 3. Architecture: how Airflow works under the hood

### 3.1 The components

```text
                 ┌──────────────────────────┐
  DAG files ───► │  DAG processor           │  parses .py files, every N seconds
  (dags/ folder) │  (part of scheduler in   │  writes "serialized DAGs" to the DB
                 │   Airflow 2; separate in │
                 │   Airflow 3)             │
                 └────────────┬─────────────┘
                              │ serialized DAG (JSON)
                              ▼
┌───────────────┐      ┌───────────────────────────┐      ┌──────────────────────┐
│ Webserver /   │◄────►│   METADATA DATABASE       │◄────►│   SCHEDULER          │
│ API server    │      │   (PostgreSQL / MySQL;    │      │   scheduling loop:   │
│ (UI, REST API)│      │    Cloud SQL in Composer) │      │   - create DAG runs  │
└───────────────┘      │  dag, dag_run,            │      │   - check deps       │
                       │  task_instance, xcom,     │      │   - queue tasks      │
                       │  connection, variable,    │      └─────────┬────────────┘
                       │  slot_pool, trigger, ...  │                │ send task
                       └──────▲───────────▲────────┘                ▼
                              │           │              ┌──────────────────────┐
                              │           │              │  EXECUTOR            │
                              │           │              │  (Celery: puts task  │
                              │           │              │   on a queue, Redis) │
                              │           │              └─────────┬────────────┘
                              │           │                        ▼
                              │   ┌───────┴────────┐     ┌──────────────────────┐
                              │   │  TRIGGERER     │     │  WORKERS             │
                              │   │  asyncio loop, │     │  run the task code   │
                              │   │  waits for     │     │  (call BigQuery API, │
                              │   │  deferred tasks│     │   wait for the job)  │
                              │   └────────────────┘     └─────────┬────────────┘
                              └─────────── state, heartbeat ───────┘
                                     logs ──► GCS / Cloud Logging
```

| Component | What it does | What breaks when it is unhealthy |
|---|---|---|
| **DAG processor** | Imports every DAG file, builds the DAG objects, serializes them to the DB | New DAGs don't appear; changes are not picked up; "DAG import errors" |
| **Scheduler** | Decides what should run now; creates DAG runs and task instances; sends ready tasks to the executor | Tasks stay in `scheduled` or `queued`; everything is late |
| **Executor** | The *strategy* for running tasks (local process, Celery queue, Kubernetes pod). Lives inside the scheduler process | Tasks stuck in `queued` |
| **Workers** | Processes that actually execute task code | Tasks fail with "zombie" or are killed (out of memory) |
| **Triggerer** | Runs an asyncio event loop that waits for many deferred tasks cheaply | Deferred tasks never resume |
| **Webserver / API server** | UI and REST API. Reads from the DB, never runs tasks | You cannot see things, but pipelines still run |
| **Metadata DB** | The single source of truth for state | Everything stops |

### 3.2 The DAG processor and parse time
Airflow does not "load a DAG once". It **re-imports every Python file in the dags folder again and again** (by default about every 30 seconds per file; the setting is `min_file_process_interval`). Each import:

1. runs **all top-level code** in the file,
2. collects the `DAG` objects it finds,
3. **serializes** them to JSON and stores them in the `serialized_dag` table.

The scheduler and the webserver use these serialized DAGs. In Airflow 2 the parsing runs as subprocesses of the scheduler by default (it can be run standalone with `airflow dag-processor`). In **Airflow 3 the DAG processor is always a separate component**.

**Why this matters:** if a DAG file calls an API, queries BigQuery or reads a big file **at the top level**, that call runs every 30 seconds, forever, for every file. With 300 DAG files this makes parsing slow, the scheduler falls behind, and tasks start late. The setting `dagbag_import_timeout` (default 30 s) kills slow imports and you get an import error.

```python
# BAD: runs at every parse, i.e. every ~30 seconds
from google.cloud import bigquery
tables = [r.table_id for r in bigquery.Client().list_tables("raw_pos")]   # API call at parse time

# GOOD: the list comes from a config file in the repo, or the call is inside a task
import yaml, pathlib
CONFIG = yaml.safe_load(pathlib.Path(__file__).with_name("tables.yaml").read_text())
```

### 3.3 The scheduling loop
The scheduler runs a loop, many times per second. Simplified, one iteration does this:

```text
loop:
  1. Create DAG runs:
       for each DAG whose next run is due (dag.next_dagrun_create_after <= now):
           create a dag_run row (state = queued → running)
           create task_instance rows for all tasks (state = none)
  2. Schedule task instances:
       for each running DAG run:
           for each task instance:
               are all upstream tasks done according to the trigger_rule?
               if yes → state = scheduled
  3. Enqueue ("critical section", uses a DB row lock):
       pick scheduled tasks, ordered by priority_weight,
       respecting: parallelism, pools, max_active_tasks, max_active_tis_per_dag
       → state = queued, hand them to the executor
  4. Process executor events (task finished / failed) and heartbeats
  5. Check for zombies (running tasks whose worker stopped heartbeating)
```

You can run **more than one scheduler** (Airflow 2.0+) for high availability and throughput. They coordinate through row-level locks in the database (`SELECT ... FOR UPDATE SKIP LOCKED`). Composer lets you set the number of schedulers.

### 3.4 The life of one task instance (the state machine)
A **task instance** is one task in one DAG run. Its state moves like this:

```text
                         ┌──────────────────────────────────────────┐
                         │                                          │
  none ──► scheduled ──► queued ──► running ──┬──► success           │
   │                                          ├──► failed            │
   │                                          ├──► up_for_retry ─────┘ (after retry_delay → scheduled)
   │                                          ├──► up_for_reschedule ─► scheduled (sensor in reschedule mode)
   │                                          ├──► deferred ──► (triggerer fires) ──► scheduled
   │                                          └──► skipped (e.g. branch not taken)
   │
   ├──► upstream_failed   (an upstream task failed, trigger_rule all_success)
   ├──► skipped           (upstream was skipped)
   └──► removed           (task no longer exists in the DAG)
```

| State | Meaning |
|---|---|
| `none` | Created, dependencies not checked/met yet |
| `scheduled` | Dependencies met; waiting for a free slot |
| `queued` | Sent to the executor; waiting for a worker to pick it up |
| `running` | A worker is executing it and heartbeating |
| `success` / `failed` | Final |
| `up_for_retry` | Failed, but has retries left; waits `retry_delay` |
| `up_for_reschedule` | A sensor in `reschedule` mode gave its slot back and will try again later |
| `deferred` | Handed to the triggerer; no worker slot used |
| `upstream_failed` | Never ran because something upstream failed |
| `skipped` | Intentionally not run |

**How to use this in debugging:**

- Many tasks stuck in **`scheduled`** → no free slots: pools full, `max_active_tasks` reached, or `parallelism` reached.
- Many tasks stuck in **`queued`** → the executor/workers can't take them: workers at max, autoscaling slow, worker crashed.
- Tasks failing as **zombies** → the worker died, usually **out of memory** because a task did heavy computation on the worker.

### 3.5 Executors
The executor decides **where and how** a queued task runs.

| Executor | How it works | Use |
|---|---|---|
| `SequentialExecutor` | One task at a time, in the scheduler process | Only for testing (SQLite). Removed in Airflow 3 |
| `LocalExecutor` | Runs tasks as subprocesses on the scheduler machine | Small setups, local dev |
| `CeleryExecutor` | Scheduler puts tasks on a message queue (Redis/RabbitMQ). A fleet of long-running worker processes pulls from the queue | **Most common in production. Cloud Composer 2 and 3 use Celery workers** on GKE, with autoscaling |
| `KubernetesExecutor` | One new Kubernetes pod per task | Strong isolation, slower start (seconds per task) |
| `CeleryKubernetesExecutor` | Both, chosen by queue name | Airflow 2; in 2.10+ and 3 you configure multiple executors instead |

In Composer you **don't choose the executor**. You get Celery workers that autoscale between a min and max count. If one task needs special dependencies or lots of memory, you run it in its own container with **`KubernetesPodOperator`** (or `GKEStartPodOperator`, or a **Cloud Run job**). That gives you per-task isolation without changing the executor.

Each Celery worker runs several tasks at once. The setting `celery.worker_concurrency` controls how many. Composer sets a default based on worker size.

### 3.6 The triggerer and deferrable operators
Many tasks spend most of their time **waiting**: waiting for a BigQuery job, for a file, for a Dataflow job. A normal operator waits **inside a worker slot**. 200 tasks waiting = 200 slots blocked, doing nothing.

A **deferrable operator** works differently:

```text
worker:     start BigQuery job ──► "defer" (save a Trigger: job_id, poll interval) ──► slot is FREE
                                          │
                                          ▼  stored in the `trigger` table
triggerer:  one asyncio event loop runs thousands of triggers:
            "await is job_id done?" ... "await is job_id done?" ... DONE → fire event
                                          │
                                          ▼
scheduler:  task instance back to `scheduled` → worker runs the operator's "resume" method
            (e.g. read the job result, raise if failed) → success
```

- The trigger is small async Python code. The triggerer process runs many of them concurrently on one event loop (cheap, because `asyncio` waiting uses almost no CPU).
- Many Google provider operators support it: `BigQueryInsertJobOperator(..., deferrable=True)`, `GCSObjectExistenceSensor(..., deferrable=True)`, Dataflow and Dataproc operators.
- In Composer you must **enable the triggerer** (set triggerer count ≥ 1) for deferrable operators to work.

### 3.7 The metadata database: what is stored
| Table (simplified names) | Content |
|---|---|
| `dag` | One row per DAG: is_paused, next run dates, file location |
| `serialized_dag`, `dag_code` | The parsed DAG structure and source |
| `dag_run` | One row per DAG run: run_id, logical date, data interval, state, start/end, run_type (scheduled, manual, backfill, asset_triggered) |
| `task_instance` | One row per task per run: state, try_number, start/end, duration, hostname, pool, queue |
| `xcom` | Small values passed between tasks |
| `connection`, `variable` | Credentials and config (unless you use a secrets backend) |
| `slot_pool` | Pools and their slot counts |
| `trigger` | Active triggers for deferred tasks |
| `dataset`/`asset`, `dataset_event`, `dataset_dag_run_queue` | Data-aware scheduling state |
| `log` | Audit log of user actions |
| `sla_miss` | Airflow 2 only (SLAs were removed in Airflow 3) |

**Why it matters:** this database is your **best diagnostic source**. Failure rates, durations, retries, sensor wait times, and the critical path can all be computed from `dag_run` and `task_instance` (section 8.1). It is also a risk: if it grows huge (old runs, big XComs), the scheduler and UI get slow. Clean it regularly (`airflow db clean`; Composer has a database maintenance option and guidance).

### 3.8 XCom
**XCom** ("cross-communication") lets a task pass a **small value** to a later task, for example a job ID, a file path, or a row count.

- By default XComs are stored **in the metadata DB**. The return value of a TaskFlow `@task` is pushed as an XCom automatically.
- **Pass references, not data.** Pass `"gs://bucket/pos/2026-10-22/"` or `"raw_pos.orders$20261022"`, never a DataFrame of 1 million rows. Big XComs bloat the DB and slow everything.
- You can configure a custom **XCom backend** (for example storing XCom values in GCS, with only a reference in the DB). The common-io provider has an object-storage XCom backend.

### 3.9 Airflow 3 architecture changes (good to know at a high level)
| Area | Airflow 2 | Airflow 3 |
|---|---|---|
| Workers and the DB | Workers connect to the metadata DB directly | Workers talk to the **Task Execution API** on the API server; no direct DB access (better security and isolation) |
| UI | Flask webserver | New React UI served by the **API server** (`airflow api-server`) |
| DAG processor | Optional separate process | Always separate |
| DAG authoring imports | `airflow.decorators`, `airflow.models` | **Task SDK**: `from airflow.sdk import dag, task, Asset` |
| Datasets | `Dataset` | Renamed **`Asset`**, plus `@asset` decorator |
| Backfill | CLI process (`airflow dags backfill`) | Managed by the scheduler, started from UI/API/CLI (`airflow backfill create`) |
| SLAs | `sla=` on tasks | Removed; replaced by **Deadline Alerts** (added in 3.1, check current docs) |
| `execution_date` | Deprecated alias of logical date | Removed |
| Default for cron schedules | Data-interval timetable | Cron *trigger* timetable (see section 4.4) |
| DAG versioning | Latest code only | DAG runs are tied to the DAG version they started with |

---

## 4. Time semantics in depth (the most misunderstood part)

### 4.1 The basic idea: a run processes an interval
In Airflow 2, a scheduled DAG run does **not** mean "run at this time". It means "**process the data of this time interval**". Each run has:

| Field | Meaning |
|---|---|
| `data_interval_start` | Start of the period this run is responsible for |
| `data_interval_end` | End of the period (exclusive) |
| `logical_date` | The run's identity; for interval timetables it equals `data_interval_start`. (Old name: `execution_date`) |
| `ds` | `logical_date` as `YYYY-MM-DD` |
| `ds_nodash` | `YYYYMMDD`, handy for partition decorators like `table$20261022` |

### 4.2 Why the daily run for 2026-10-22 starts on 2026-10-23
With `schedule="@daily"` (Airflow 2 default timetable):

```text
          data interval of the run "2026-10-22"
   ├──────────────────────────────────────────────┤
2026-10-22 00:00                          2026-10-23 00:00 ──► the run STARTS here
   = data_interval_start = logical_date      = data_interval_end
   ds = "2026-10-22"
```

The run starts **after its interval ends**, because only then is the data for that day complete. A batch job for "sales of 22 October" can only run once 22 October is over. So:

- The run with `ds = 2026-10-22` starts shortly after midnight on 23 October.
- If `start_date = 2026-10-01` and `schedule="@daily"`, the **first** run is for interval 1 Oct → 2 Oct and it starts on 2 Oct.

This confuses everyone at first. Remember: **the logical date is the start of the data interval, not the time the run starts.**

### 4.3 start_date, catchup, and backfill
- **`start_date`**: the earliest data interval the DAG is allowed to have. Must be a **fixed** date (`pendulum.datetime(2026, 1, 1, tz="Europe/Helsinki")`), **never** `datetime.now()`. A moving start_date means the scheduler can never decide the next run reliably.
- **`catchup=True`**: when the DAG is unpaused, Airflow creates runs for **every missed interval** between `start_date` (or the last run) and now. A new DAG with `start_date` one year ago and `catchup=True` creates **365 runs** immediately. That can flood BigQuery.
- **`catchup=False`**: only the latest interval is scheduled. Airflow 2's config default `catchup_by_default` is True; **Airflow 3 changed the default to False**. Always set it explicitly in the DAG.
- **Backfill**: you *deliberately* run a DAG for a past date range, for example after fixing a bug or adding a new column.
    - Airflow 2: `airflow dags backfill -s 2026-09-01 -e 2026-09-30 sales_daily`. This runs as a long CLI process. In Composer it's often easier and safer to **clear** the task instances for the date range in the UI or with `airflow tasks clear`, which lets the normal scheduler rerun them.
    - Airflow 3: backfills are scheduler-managed: `airflow backfill create --dag-id sales_daily --from-date 2026-09-01 --to-date 2026-09-30`, or from the UI, with options for max active runs and whether to reprocess existing runs.

**Backfills only work if tasks are idempotent and use the interval variables** (section 6.1). If the SQL uses `CURRENT_DATE()`, a backfill of September processes today 30 times.

### 4.4 Timetables
A **timetable** is the class that decides the data intervals and run times. `schedule` accepts:

| `schedule=` value | Meaning |
|---|---|
| `"@daily"`, `"0 2 * * *"` | Cron |
| `timedelta(hours=1)` | Fixed delta |
| `None` | Only manual or API-triggered runs |
| `"@once"` | One run |
| `[Dataset(...)]` / `[Asset(...)]` | Data-aware: run when upstream data is updated |
| A timetable object | Custom logic, for example `CronTriggerTimetable`, `EventsTimetable`, or your own (for example "business days only") |

Two built-in cron timetables behave differently:

| Timetable | Interval | Run time | `ds` of the run that starts 2026-10-23 02:00 |
|---|---|---|---|
| `CronDataIntervalTimetable` (Airflow 2 default for cron strings) | Previous cron tick → this tick | At the **end** of the interval | `2026-10-22` |
| `CronTriggerTimetable` | Zero-length by default (start = end = trigger time), optional `interval=` | At the cron tick | `2026-10-23` |

**Airflow 3 gotcha:** in Airflow 3 the default for cron strings changed to the trigger-style behaviour (the config `[scheduler] create_cron_data_intervals` defaults to False). So a DAG migrated from Airflow 2 to 3 with `schedule="0 2 * * *"` may suddenly get a **different `ds`** (today instead of yesterday). This breaks SQL that uses `{{ ds }}` to mean "yesterday". Fix: choose the timetable explicitly, or compute the date you need explicitly. Check the current docs for your version.

### 4.5 Jinja templating and macros
Airflow renders **Jinja templates** in operator fields **at run time on the worker**, using the context of that task instance. Only fields listed in the operator's `template_fields` are rendered. Files with extensions in `template_ext` (for example `.sql`) are read and rendered too.

| Template | Example value (run for 2026-10-22, daily) |
|---|---|
| `{{ ds }}` | `2026-10-22` |
| `{{ ds_nodash }}` | `20261022` |
| `{{ data_interval_start }}` | `2026-10-22T00:00:00+00:00` (a pendulum datetime) |
| `{{ data_interval_end }}` | `2026-10-23T00:00:00+00:00` |
| `{{ data_interval_end \| ds }}` | `2026-10-23` |
| `{{ macros.ds_add(ds, -7) }}` | `2026-10-15` |
| `{{ macros.ds_format(ds, "%Y-%m-%d", "%Y/%m/%d") }}` | `2026/10/22` |
| `{{ run_id }}` | `scheduled__2026-10-22T00:00:00+00:00` |
| `{{ params.region }}` | DAG/task param |
| `{{ var.value.gcp_project }}` | An Airflow Variable |
| `{{ dag_run.conf }}` | Config passed when triggering manually |

```python
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

build_daily = BigQueryInsertJobOperator(
    task_id="build_fct_sales_daily",
    configuration={
        "query": {
            # sql/fct_sales_daily.sql is rendered by Jinja: it can use {{ ds }} too
            "query": "{% include 'sql/fct_sales_daily.sql' %}",
            "useLegacySql": False,
            "destinationTable": {
                "projectId": "{{ var.value.gcp_project }}",
                "datasetId": "mart_sales",
                "tableId": "fct_sales_daily${{ ds_nodash }}",   # partition decorator
            },
            "writeDisposition": "WRITE_TRUNCATE",
        }
    },
)
```

**Common mistakes:**

- Using `{{ ds }}` in normal Python code at the top of the file (for example in an f-string). Templates are rendered only inside templated operator fields, at run time. At parse time it's just text.
- Using `datetime.now()` inside a task instead of `data_interval_start`. Reruns and backfills then use the wrong date.
- Time zones: Airflow stores everything in UTC. If the business day is Helsinki time, set a timezone-aware `start_date` (pendulum) so the daily interval matches the local day, and be explicit in SQL (`DATE(ts, "Europe/Helsinki")`).

---

## 5. Cloud Composer architecture

### 5.1 What an environment contains

```text
 Your project                         Google-managed tenant project
┌──────────────────────────────┐    ┌───────────────────────────────────────┐
│ GCS bucket (per environment) │    │ Cloud SQL (PostgreSQL): metadata DB    │
│  ├─ dags/     ◄── CI deploys │    │ Airflow web server / API (Composer 2:  │
│  ├─ plugins/                 │    │   in tenant project)                   │
│  ├─ data/                    │    │                                        │
│  └─ logs/                    │    │ Composer 3: the GKE cluster with       │
│                              │    │ scheduler, workers, triggerer, DAG     │
│ Composer 2: GKE Autopilot    │    │ processor is ALSO here (you don't see  │
│ cluster with scheduler,      │    │ or manage it)                          │
│ workers, triggerer, Redis    │    └───────────────────────────────────────┘
│                              │
│ Service account of the env   │──► calls BigQuery, GCS, Dataflow, Dataproc...
│ Cloud Logging / Monitoring   │◄── task logs, scheduler logs, metrics
└──────────────────────────────┘
```

- **GCS bucket:** Composer creates one bucket per environment. Folder `dags/` holds DAG files and anything they import (SQL files, YAML configs, helper modules). `plugins/` holds custom plugins/operators. `data/` is shared scratch space. `logs/` holds task logs (also streamed to Cloud Logging).
- **How DAGs sync:** in Composer 2 and 3 the bucket is mounted into the Airflow components with **Cloud Storage FUSE** at `/home/airflow/gcs/` (for example `/home/airflow/gcs/dags/`). When CI copies a file to `gs://<bucket>/dags/`, the DAG processor sees it on the next parse cycle (usually within a minute or two). There is no "deploy button"; the file *is* the deployment. That is why CI must test before copying.
- **Metadata DB:** Cloud SQL for PostgreSQL, managed by Google, in the tenant project. You don't connect to it directly in normal operation; you use the Airflow UI, REST API, CLI through `gcloud composer environments run`, or a maintenance DAG.
- **Service account:** every environment runs as a service account. Tasks that call BigQuery use this identity unless you set a different `impersonation_chain` on the operator. In a shared-ingestion setup, use **least privilege** and impersonation per domain.

### 5.2 Composer 2 vs. Composer 3 (general picture; check current docs for details)
| | Composer 2 | Composer 3 |
|---|---|---|
| Where Airflow runs | GKE **Autopilot** cluster in **your** project | Infrastructure in the Google **tenant** project; no cluster visible to you |
| Networking | You plan GKE IP ranges, private/public mode, PSC or VPC peering | Simpler: attach the environment to your VPC (network attachment) when needed; switch private/public more easily |
| Scaling | Workloads config: scheduler, web server, workers (min/max), triggerer CPU/memory/count | Similar workloads config, plus a separately sized DAG processor |
| Airflow versions | Airflow 2 | Airflow 2, and Airflow 3 in newer releases (check current support) |
| Upgrades | In-place upgrades with "Composer image" versions | More managed, more frequent "Airflow builds" |
| Composer 1 | Old, end of support. If an organization still runs it, migration is a likely project |

In short: Composer 3 moves more of the infrastructure under Google's management, so you spend less time on GKE and networking and more on the DAGs. The DAG design principles are the same in both.

### 5.3 Configuration you will touch
| Setting | What and why |
|---|---|
| **Environment size / workloads** | Scheduler count and CPU/memory, worker CPU/memory, **min and max workers**, triggerer count, web server size. Scale workers for parallel tasks; scale schedulers/DAG processor for many DAG files |
| **PyPI packages** | Install extra Python packages (for example `apache-airflow-providers-…` versions, `pyyaml`). **Pin versions.** Every package update rebuilds the image and can break all DAGs; dependency conflicts are a classic Composer pain. Heavy or conflicting dependencies (dbt, pandas versions) go into a container run by `KubernetesPodOperator` or Cloud Run instead |
| **Airflow config overrides** | For example `core.max_active_tasks_per_dag`, `celery.worker_concurrency`, `scheduler.min_file_process_interval`, `secrets.backend` |
| **Environment variables** | For example `ENV=prod`, `GCP_PROJECT`. DAG code reads them to choose targets |
| **Airflow connections and variables** | Credentials and config. Better: store them in **Secret Manager** (next point) |
| **Private IP** | Workers have no public IP; access Google APIs via Private Google Access; the UI can be restricted. Standard for enterprise customers |
| **Maintenance windows, snapshots** | When Google may upgrade components; environment snapshots for disaster recovery and migration |

### 5.4 Secret Manager as the secrets backend
Instead of storing passwords in the Airflow DB, configure:

```text
[secrets]
backend = airflow.providers.google.cloud.secrets.secret_manager.CloudSecretManagerBackend
backend_kwargs = {"connections_prefix": "airflow-connections", "variables_prefix": "airflow-variables", "sep": "-"}
```

Then a secret named `airflow-connections-erp_db` in Secret Manager becomes the connection `erp_db`. Airflow checks the secrets backend first, then environment variables, then the DB. Benefits: secrets managed by Terraform and IAM, rotated centrally, never in code or in the Airflow DB. For GCP services you usually don't need a stored secret at all: the environment's service account (or impersonation) authenticates.

### 5.5 Cost
Composer is **always on**: you pay for the environment (scheduler, web server, DB, minimum workers) even when nothing runs, plus extra workers when they scale up. The pricing model is in compute units and storage (check current pricing). Practical points:

- A small environment costs in the order of a few hundred dollars per month; big ones much more. One environment per stage (dev, test, prod) multiplies this.
- Keep **minimum workers low** and let autoscaling handle peaks.
- Workers that **wait** cost money: use deferrable operators.
- Workers that **compute** cost money and risk OOM: push compute to BigQuery/Dataflow.
- The bigger cost is usually **BigQuery** triggered by DAGs. Label every job (`labels: {pipeline, layer, dag_id}`) to attribute cost (see the [BigQuery lesson](bigquery.md)).
- Alternatives for very simple flows: **Cloud Workflows + Cloud Scheduler**, or Dataform's own workflow configurations. They are serverless and cheap but have no rich backfill/retry history like Airflow.

---

## 6. Core concepts, one by one

Each concept: **what → how it works inside → why it matters → example → common mistake**.

### 6.1 Idempotency
- **What:** a task is idempotent if running it **once or ten times for the same interval gives the same final result**.
- **How:** the task (1) reads only the data of its own interval, defined by `data_interval_start/end` (never "now"), and (2) writes in a way that **replaces** its previous output instead of adding to it: partition overwrite, MERGE on a business key, `CREATE OR REPLACE`, deterministic file paths.
- **Why:** Airflow will rerun tasks: retries, manual clears, backfills, zombie recovery. A task can also fail **after** BigQuery finished the job (for example the worker died while polling); then the retry runs the job again. If the write appends, you get duplicates. This is the most common cause of "frequent failed reruns" in legacy Composer setups.

```sql
-- sql/fct_sales_daily.sql: reads ONLY the interval of this run
SELECT
  DATE(order_ts, "Europe/Helsinki") AS order_date,
  store_id, product_id,
  SUM(qty) AS qty, SUM(net_amount) AS net_amount
FROM `{{ var.value.gcp_project }}.stg_pos.order_lines`
WHERE order_ts >= TIMESTAMP("{{ data_interval_start }}")
  AND order_ts <  TIMESTAMP("{{ data_interval_end }}")
GROUP BY 1, 2, 3
-- Written with WRITE_TRUNCATE to fct_sales_daily${{ ds_nodash }}: replaces only that partition
```

Alternative: a **MERGE** on keys for late-arriving or updated rows (see the [BigQuery lesson](bigquery.md) for MERGE details). Or `DELETE` the interval then `INSERT` inside one multi-statement transaction (`BEGIN TRANSACTION; ... COMMIT TRANSACTION;`).

| Not idempotent | Idempotent |
|---|---|
| `INSERT INTO t SELECT ... WHERE dt = CURRENT_DATE()` | Overwrite partition `t$YYYYMMDD` with `WRITE_TRUNCATE` |
| `datetime.now()` in Python | `data_interval_start` from the context |
| `CREATE TABLE t` | `CREATE OR REPLACE TABLE` / `IF NOT EXISTS` |
| Write `gs://b/out/{uuid}.csv` | Write `gs://b/out/dt={{ ds }}/part-*.csv`, delete prefix first |
| Send an email inside the load task | Separate notification task; the load can rerun safely |

- **Common mistake:** "idempotent because we use MERGE", while the source query still uses `CURRENT_DATE()`. The **read side** and the **write side** must both be deterministic.

### 6.2 Dependencies inside a DAG and trigger rules
- **What:** `a >> b` means b runs after a. A task's **trigger rule** decides when it may run based on upstream states.
- **How:** in step 2 of the scheduling loop, the scheduler evaluates each task's trigger rule against its upstream task instances.
- **Why:** correct trigger rules make cleanup and alert tasks work.

| Trigger rule | Runs when |
|---|---|
| `all_success` (default) | All upstream succeeded |
| `all_done` | All upstream finished, any state (good for cleanup) |
| `one_failed` | At least one upstream failed (good for a custom alert) |
| `none_failed` | No upstream failed (skips are OK; good after a branch) |
| `none_failed_min_one_success` | Common after `BranchPythonOperator` |

```python
extract >> load >> [check_counts, check_keys] >> publish
cleanup = BashOperator(task_id="cleanup", bash_command="...", trigger_rule="all_done")
```

- **Common mistake:** after a branch, the join task has `all_success` and gets **skipped** because one branch was skipped. Use `none_failed_min_one_success`.

### 6.3 Cross-DAG dependencies: ExternalTaskSensor vs. Datasets/Assets
**ExternalTaskSensor** waits until a task in **another DAG** with the **same logical date** (or `logical_date - execution_delta`) is in `success`.

Problems at scale:

- It **couples schedules**. If DAG A runs at 01:00 and DAG B at 02:00, you need `execution_delta=timedelta(hours=1)`. Change A's schedule and B silently waits forever.
- It **holds a worker slot** while waiting (unless reschedule mode or deferrable).
- Failures **cascade** through chains of sensors. Nobody can see the full graph. After 5 years you have 300 DAGs with 500 sensors.

**Data-aware scheduling with Datasets (Airflow 2.4+) / Assets (Airflow 3):**

```text
Producer DAG                         Metadata DB                         Consumer DAG
task(outlets=[RAW_POS]) succeeds ──► dataset_event row for RAW_POS ──►  dataset_dag_run_queue:
                                      (timestamp, source run)            "consumer waits for RAW_POS ✓"
                                                                          │ all required datasets have
                                                                          ▼ new events since last run?
                                                                 scheduler creates a consumer DAG run
                                                                 (run_type = dataset/asset triggered)
```

- **What:** a Dataset/Asset is just a **URI string** naming some data (for example `bigquery://proj/raw_pos/orders`). A producer task declares `outlets=[dataset]`. A consumer DAG declares `schedule=[dataset]`.
- **How inside:** when a task with outlets **succeeds**, Airflow writes a dataset event. The scheduler checks which DAGs depend on that dataset; when all their required datasets have new events, it creates a run. Airflow 2.9+ supports logical expressions (`schedule=(ds_a & ds_b) | ds_c`) and combining a time schedule with datasets (`DatasetOrTimeSchedule`). Events can also be created from outside via the REST API (2.9+).
- **Why:** producers and consumers are **decoupled**. The consumer doesn't care when the producer runs. The UI shows a dataset dependency graph. No worker slots are wasted.
- **Important detail:** Airflow **does not look at the data**. "Updated" means "the producer task succeeded". So the producer must only succeed when the data is really there and valid (put the quality check before the task with the outlet).
- **Another detail:** a dataset-triggered run has no natural "daily" data interval. If the consumer needs to know *which* data changed, read it from the triggering events (`triggering_dataset_events` / `triggering_asset_events` in the context) or process "everything new since last watermark" idempotently with MERGE.

```python
# Airflow 2.x
from airflow import Dataset
from airflow.decorators import dag, task
import pendulum

RAW_POS = Dataset("bigquery://retail-ingest/raw_pos/orders")

@dag(schedule="*/15 * * * *", start_date=pendulum.datetime(2026, 1, 1, tz="UTC"), catchup=False, tags=["ingest"])
def ingest_pos():
    @task(outlets=[RAW_POS])
    def load_and_validate():
        ...  # load, check row counts; raise if bad → no dataset event
    load_and_validate()

@dag(schedule=[RAW_POS], start_date=pendulum.datetime(2026, 1, 1, tz="UTC"), catchup=False, tags=["sales"])
def build_sales_mart():
    ...

ingest_pos(); build_sales_mart()
# Airflow 3: from airflow.sdk import Asset, dag, task ; RAW_POS = Asset("bigquery://...")
```

- **When to keep or choose something else:**
    - Two DAGs always run together and are tightly coupled → **merge them into one DAG**.
    - The upstream is **outside Airflow** (a file, another system) → event-driven trigger (GCS notification → Pub/Sub → trigger the DAG via REST API or a Cloud Function), or a **deferrable** sensor.
    - Model-to-model SQL dependencies → put them in **dbt/Dataform `ref()`**, not in Airflow at all.
- **Common mistake:** putting `outlets` on a task that "starts" the load but doesn't wait for it, so consumers start before the data is ready.

### 6.4 Sensors: poke vs. reschedule vs. deferrable
A **sensor** is an operator that waits for a condition (file exists, partition exists, API ready).

| Mode | How it works | Worker slot while waiting | Use |
|---|---|---|---|
| `poke` (default) | Loop inside the worker: check, `sleep(poke_interval)`, check... | **Held all the time** | Only very short waits (seconds/minutes) |
| `reschedule` | Check once, then end with state `up_for_reschedule`; scheduler reschedules after `poke_interval` | Free between checks | Long waits with low check frequency |
| deferrable (`deferrable=True`) | Hands an async trigger to the triggerer | **Free**; the triggerer waits | Best default for long waits, if the sensor supports it |

Always set **`timeout`** (total wait; then fail) and a sensible `poke_interval`. Also consider `soft_fail=True` (skip instead of fail) when missing data is acceptable.

```python
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor

wait_file = GCSObjectExistenceSensor(
    task_id="wait_pos_export",
    bucket="retail-landing",
    object="pos/{{ ds }}/_SUCCESS",
    deferrable=True,          # waits in the triggerer, no worker slot
    timeout=60 * 60 * 3,      # fail after 3 hours
    poke_interval=120,
)
```

- **Common mistake:** 200 poke-mode sensors that each wait 6 hours. They fill every worker slot; real work queues behind them; Composer autoscales workers that do nothing but sleep. This is a classic "Composer is slow and expensive" root cause.

### 6.5 Pools, priority and concurrency settings
| Setting | Scope | What it limits |
|---|---|---|
| `parallelism` (`core.parallelism`) | Per scheduler | Max running task instances in total |
| `max_active_tasks` | Per DAG (all its runs together) | Max running tasks of this DAG |
| `max_active_runs` | Per DAG | Max concurrent DAG runs (key for backfills) |
| `max_active_tis_per_dag` | Per task | Max concurrent instances of one task across runs |
| **Pool** (`pool="bq_heavy"`, `pool_slots=2`) | Across DAGs | Max concurrent tasks using a shared resource. `default_pool` has 128 slots by default |
| `priority_weight` + `weight_rule` | Ordering | Which scheduled task gets a free slot first. Default rule `downstream` sums the weights of downstream tasks, so tasks with long downstream chains go first |
| `celery.worker_concurrency` | Per worker | Tasks per worker process |

- **Why it matters:** pools protect **shared systems**: the source ERP database (max 4 parallel extracts), BigQuery slot reservations, an API rate limit. Priority makes the critical sales path win over low-value reports.
- **Common mistake:** a backfill of 365 days with `max_active_runs=16` and no pool: 16 × N BigQuery jobs at once, the reservation is saturated, and the morning sales mart is late. Set `max_active_runs=2–4` for backfills and use a pool.

### 6.6 TaskFlow API
- **What:** decorators `@dag` and `@task` (Airflow 2.0+) to write DAGs as plain Python functions.
- **How:** a `@task` function call at DAG-definition time doesn't run the function. It creates a task and returns an `XComArg` placeholder. Passing it to another task creates a dependency **and** an XCom pull at run time.
- **Why:** less boilerplate, clear data flow for small values. You can mix TaskFlow tasks with classic operators.

```python
from airflow.decorators import dag, task   # Airflow 3: from airflow.sdk import dag, task
import pendulum

@dag(schedule="@daily", start_date=pendulum.datetime(2026, 10, 1, tz="Europe/Helsinki"), catchup=False)
def file_ingest():
    @task
    def list_files(data_interval_start=None) -> list[str]:
        # context values can be injected as arguments
        day = data_interval_start.strftime("%Y-%m-%d")
        return [f"gs://retail-landing/pos/{day}/part-{i}.csv" for i in range(3)]   # small list: OK as XCom

    @task
    def count(paths: list[str]) -> int:
        return len(paths)

    count(list_files())

file_ingest()
```

- **Common mistake:** returning big objects (DataFrames) from `@task` functions. They are pickled/serialized into XCom → DB bloat.

### 6.7 Dynamic task mapping
- **What:** create **N task instances at run time** from a list (Airflow 2.3+): `.expand()`.
- **How:** the upstream task returns a list (XCom). When the scheduler reaches the mapped task, it creates one task instance per element, with `map_index` 0..N-1. `.partial()` sets constant arguments. The limit is `max_map_length` (default 1024).
- **Why:** one DAG can load "all tables in this source" or "all files that arrived today", without generating code. Each mapped instance retries independently.

```python
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

load = GCSToBigQueryOperator.partial(
    task_id="load_table",
    bucket="retail-landing",
    source_format="PARQUET",
    write_disposition="WRITE_TRUNCATE",
    pool="bq_loads",
).expand_kwargs([
    {"source_objects": ["erp/customers/{{ ds }}/*.parquet"], "destination_project_dataset_table": "raw_erp.customers"},
    {"source_objects": ["erp/products/{{ ds }}/*.parquet"],  "destination_project_dataset_table": "raw_erp.products"},
])
```

- **Mapping vs. DAG factory:** mapping = dynamic at **run time** inside one DAG (the list can change every run). Factory = many DAGs generated at **parse time** from config (each has its own schedule, owner, alerts).
- **Common mistake:** mapping over 10,000 items; you hit `max_map_length`, the UI is slow, the DB grows. Batch the items instead.

### 6.8 DAG factories from YAML config
- **What:** one Python module that reads config files and **generates DAG objects**. Onboarding a new source = one YAML file in a pull request.
- **How:** at parse time, the factory loops over config files and registers each DAG in the module's globals (Airflow finds DAG objects in module-level variables; in Airflow 2.4+ DAGs created in a `with DAG()` context or by `@dag` are also auto-registered).
- **Why:** this is how you build "reusable ingestion patterns". Every DAG gets the same retries, timeouts, labels, alerts, pools, quality checks and naming. A fix in the factory fixes all DAGs.

```yaml
# dags/configs/sources/pos_orders.yaml
source: pos_orders
pattern: gcs_parquet            # gcs_parquet | datastream_merge | api_extract
owner: sales-domain
schedule: "15 * * * *"
landing_prefix: pos/orders
target: raw_pos.orders
partition_field: order_date
quality_checks:
  - {type: not_null, column: order_id}
  - {type: row_count_min, value: 1}
publishes: bigquery://retail-ingest/raw_pos/orders
```

```python
# dags/ingestion_factory.py
import pathlib, yaml, pendulum
from airflow import DAG, Dataset
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.operators.empty import EmptyOperator
from common.callbacks import alert_on_failure      # dags/common/callbacks.py

CONFIG_DIR = pathlib.Path(__file__).parent / "configs" / "sources"

def build_ingestion_dag(cfg: dict) -> DAG:
    with DAG(
        dag_id=f"ingest__{cfg['source']}",
        schedule=cfg["schedule"],
        start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
        catchup=False,
        max_active_runs=1,
        tags=["ingestion", cfg["owner"], cfg["pattern"]],
        default_args={
            "owner": cfg["owner"],
            "retries": 3,
            "retry_delay": pendulum.duration(minutes=5),
            "retry_exponential_backoff": True,
            "execution_timeout": pendulum.duration(hours=1),
            "on_failure_callback": alert_on_failure,
        },
    ) as dag:
        load = GCSToBigQueryOperator(
            task_id="load",
            bucket="retail-landing",
            source_objects=[f"{cfg['landing_prefix']}/{{{{ ds }}}}/*.parquet"],
            destination_project_dataset_table=f"{cfg['target']}${{{{ ds_nodash }}}}",
            source_format="PARQUET",
            write_disposition="WRITE_TRUNCATE",       # idempotent per partition
            time_partitioning={"type": "DAY", "field": cfg["partition_field"]},
            pool="bq_loads",
        )
        checks = [
            BigQueryCheckOperator(
                task_id=f"check_{i}",
                sql=render_check(cfg["target"], chk),   # returns SQL whose first row must be truthy
                use_legacy_sql=False,
            )
            for i, chk in enumerate(cfg.get("quality_checks", []))
        ]
        publish = EmptyOperator(task_id="publish", outlets=[Dataset(cfg["publishes"])])
        load >> checks >> publish
    return dag

for path in sorted(CONFIG_DIR.glob("*.yaml")):
    cfg = yaml.safe_load(path.read_text())          # local file read: cheap at parse time
    globals()[f"ingest__{cfg['source']}"] = build_ingestion_dag(cfg)
```

(`render_check` is your own small helper that turns a check config into SQL. The `publish` task only exists to emit the Dataset event after all checks pass. Note the doubled braces `{{{{ ds }}}}`: inside a Python f-string they produce the literal Jinja `{{ ds }}`.)

- **Common mistakes:** reading configs from a database or API at parse time (slow, fragile); no schema validation of YAML (one typo breaks all generated DAGs: validate with pydantic or JSON Schema in CI); a factory so generic that nobody understands it. Keep a few clear **patterns**, not one function with 40 flags.
- Note: **DAG Factory** (the open-source `dag-factory` library) does the same thing from YAML without your own code. A custom factory gives you more control over standards.

### 6.9 "Orchestrate, don't compute"
- **What:** Airflow tasks **start and monitor** work in systems built for it. They don't process data themselves.
- **How:** an operator like `BigQueryInsertJobOperator` sends a job request to the BigQuery API, then polls (or defers) until the job is done. The worker uses almost no CPU or memory. The heavy work happens in BigQuery's distributed engine.
- **Why:** Composer workers are small, shared, and autoscaled for orchestration. Running pandas on 5 GB there causes OOM kills (zombie tasks), slows other tasks on the same worker, and doesn't scale. Separation also keeps the logic testable and portable (SQL in dbt/Dataform; Beam code in Dataflow).

| Work | Where it runs | Operator |
|---|---|---|
| SQL transformations | BigQuery | `BigQueryInsertJobOperator`, or dbt/Dataform |
| Load files into BigQuery | BigQuery load job (free) | `GCSToBigQueryOperator` |
| Export tables | BigQuery extract job | `BigQueryToGCSOperator` |
| Streaming/batch pipelines, non-SQL logic | Dataflow | `DataflowStartFlexTemplateOperator`, `BeamRunPythonPipelineOperator` |
| Spark | Dataproc / Dataproc Serverless | `DataprocSubmitJobOperator`, `DataprocCreateBatchOperator` |
| Custom code with special dependencies | A container | `KubernetesPodOperator`, `GKEStartPodOperator`, `CloudRunExecuteJobOperator` |
| Dataform runs | Dataform | `DataformCreateCompilationResultOperator`, `DataformCreateWorkflowInvocationOperator` |
| Checks | BigQuery | `BigQueryCheckOperator`, `BigQueryValueCheckOperator`, `BigQueryIntervalCheckOperator`, `BigQueryColumnCheckOperator`, `BigQueryTableCheckOperator` |
| Wait for data | Sensors | `GCSObjectExistenceSensor`, `BigQueryTablePartitionExistenceSensor` |

- **Common mistake:** `PythonOperator` with `bigquery.Client().query(sql).result()` and then `to_dataframe()` to transform rows in Python. Use SQL in BigQuery.

### 6.10 Retries, timeouts and callbacks
| Parameter | Meaning | Typical value |
|---|---|---|
| `retries` | How many extra attempts | 2–3 for API/infrastructure errors |
| `retry_delay`, `retry_exponential_backoff`, `max_retry_delay` | Wait between attempts | 2–5 min, exponential |
| `execution_timeout` | Max runtime of one attempt; then the task fails | Set it always; e.g. 2× normal duration |
| `dagrun_timeout` (DAG) | Max runtime of a whole DAG run | Catch stuck runs |
| `sensor timeout` | Max wait for a sensor | Based on the SLA |
| `on_failure_callback`, `on_retry_callback`, `on_success_callback` | Python functions called on events | Alert to chat/PagerDuty with dag_id, task_id, ds, log URL |

- **Why:** retries handle transient errors (quota, network, 503s). Timeouts turn "hangs forever" into a visible failure. Callbacks route alerts to the owner.
- **Common mistake:** retries on a non-idempotent task: each retry adds duplicates. Retries are only safe **after** you make tasks idempotent. Another mistake: alerting on every retry → alert fatigue. Alert on final failure, and send a daily summary of retries.

### 6.11 Data quality checks as tasks
A green DAG only means **no exception was raised**. It does not mean the data is correct. Add checks as **blocking tasks** between load and publish:

```python
from airflow.providers.google.cloud.operators.bigquery import BigQueryValueCheckOperator, BigQueryCheckOperator

no_dupes = BigQueryCheckOperator(
    task_id="no_duplicate_order_lines",
    sql="""
      SELECT COUNT(*) = 0 FROM (
        SELECT order_id, line_no FROM `raw_pos.orders`
        WHERE order_date = '{{ ds }}'
        GROUP BY 1, 2 HAVING COUNT(*) > 1)
    """,
    use_legacy_sql=False,
)
```

Other checks: row count vs. source or vs. last week (`BigQueryIntervalCheckOperator`), null keys, freshness (`MAX(_ingested_at)` within N minutes), reconciliation of totals (sum of sales vs. source system). For model-level tests, use **dbt tests** or **Dataform assertions** (see [Dataform and dbt](dataform-dbt.md)).

---

## 7. What matters most in practice

These are the topics that decide whether a Composer platform is healthy or painful to run.

### 7.1 "Failed reruns" = non-idempotent tasks
When someone says "reruns fail", list the causes in order:

1. **Appending writes** (`INSERT`, `WRITE_APPEND`) → duplicates or primary-key-like checks fail on rerun.
2. **"Now"-based logic** (`CURRENT_DATE()`, `datetime.now()`) → a rerun of yesterday processes today.
3. **Non-reentrant DDL** (`CREATE TABLE` without `IF NOT EXISTS`) → "already exists" errors.
4. **Hidden state**: temp tables with fixed names shared by parallel runs; files left half-written.
5. **Ordering assumptions**: task assumes yesterday's run already succeeded (use `depends_on_past=True` only when truly needed, or design it out).

Fix: parameterize by the data interval, write with partition overwrite or MERGE, make DDL idempotent, use run-specific temp names (`tmp_{{ ds_nodash }}_{{ ti.try_number }}`), and test it: **run the same date twice in CI/dev and compare row counts**.

### 7.2 Backfills you can trust
- Precondition: idempotent tasks using interval variables.
- **Blast radius:** `max_active_runs` small, a **pool** for BigQuery, run in **batches** (a month at a time), check results after the first batch.
- **Order:** if tables depend on previous days (cumulative metrics, SCD2), run in date order (`max_active_runs=1` or `depends_on_past=True`).
- **Large history:** 3 years × daily = 1,095 runs with Airflow overhead and 1,095 small BigQuery jobs. Often better: **one set-based SQL** that rebuilds the whole range (or a dbt `--full-refresh`), then let daily runs continue.
- **Downstream:** a backfill of raw data must trigger rebuilds of dependent marts. With Datasets, producer success creates events, so consumers run. With dbt, run `dbt build --select model+` for the range.

### 7.3 Dependencies: the right tool at each level
| Level | Tool |
|---|---|
| Steps of one pipeline | Tasks in **one DAG** |
| SQL models (stg → int → mart) | **dbt/Dataform `ref()`**, one task in Airflow |
| Pipeline to pipeline (ingest → transform) | **Datasets/Assets** |
| Outside system → Airflow | Event (Pub/Sub → trigger via API) or **deferrable** sensor |
| Same logical date across two DAGs that must align | If unavoidable, `ExternalTaskSensor(mode="reschedule" or deferrable=True)` with explicit `execution_delta`; better merge the DAGs |

### 7.4 Airflow vs. dbt/Dataform vs. BigQuery: who does what
| Airflow (Composer) | dbt / Dataform | BigQuery |
|---|---|---|
| **When** things run (schedules, events) | **What** the SQL models are and their order (`ref`) | **Executes** the SQL |
| Cross-system steps (wait for file, start Dataflow, call API, load GCS → BQ) | Materializations (table, incremental, view), incremental logic | Storage, partitioning, clustering |
| Retries, timeouts, alerts, backfill control | Tests/assertions, docs, lineage | MERGE, scripting, procedures |
| One task: `dbt build --select tag:sales` | Hundreds of models | Slots and cost |

The rule: **Airflow should not know about individual SQL models.** If you see a DAG with 80 `BigQueryInsertJobOperator` tasks wired by hand in the order of SQL dependencies, that is a candidate to move into dbt/Dataform. You get tests, lineage, and fewer DAG edits.

### 7.5 Top-level code, parse time, and scheduler health
- Parse cost grows with **number of files × parse cost per file**.
- Rules: no network calls at top level; heavy imports inside tasks (`import pandas` inside the function); keep factories reading **local** config; use `.airflowignore` to skip non-DAG folders (sql/, configs/, tests/).
- Measure: `airflow dags report` shows parse time per file. Composer shows "DAG processor" metrics and parse errors. Target: each file well under 1–2 seconds.

### 7.6 Waiting without wasting
Replace poke sensors with **deferrable** versions; use `mode="reschedule"` when no deferrable version exists; replace cross-DAG sensors with Datasets; replace "poll a bucket every 5 minutes" with **GCS notifications → Pub/Sub** where possible. Measure "sensor wait time" before and after (section 9).

### 7.7 Standardization at scale: factory + defaults + policies
- **DAG factory** for repeatable patterns.
- **Shared `default_args`** module (owner, retries, timeouts, callbacks).
- **Cluster policies** (`airflow_local_settings.py`, `dag_policy` / `task_policy`): Airflow calls these functions for every DAG/task at parse time. You can **enforce** rules (fail DAGs without an owner, force a pool for BigQuery operators, set default timeouts). In Composer you place `airflow_local_settings.py` in the `config/` or `plugins` area per Composer docs (check current docs for the exact location).
- **CI tests** that check the same standards (section 9).

### 7.8 Environments and promotion
dev → test/stage → prod, each its own Composer environment (or local Airflow for dev), each with its own GCS bucket and service account. The **same code** is promoted; differences come from **environment variables** (`ENV`, project IDs) and Secret Manager, never from editing code per environment.

### 7.9 Observability and metrics (define and track them)
Load duration, throughput, stability, cost. Section 9.3 explains what to measure and where.

### 7.10 Upgrades and Airflow 3 readiness
Upgrading Airflow or provider packages can change behaviour (time semantics in Airflow 3, removed `execution_date`, removed SLAs, `airflow.sdk` imports). Approach: inventory deprecated features with `ruff` Airflow rules (the `AIR` rule set flags Airflow 3 incompatibilities), test in a copy environment (Composer snapshots), migrate DAG by DAG.

---

## 8. A worked case: restructuring 300 tangled DAGs

**Situation:** about 300 DAGs, many cross-DAG `ExternalTaskSensor`s, frequent failed reruns, the nightly chain finishes at 09:30 instead of 06:00, and nobody wants to touch it.

### 8.1 Diagnose with data (week 1–2)
**a) Extract the real dependency graph.** Load the DagBag in a script; for each DAG list tasks, `ExternalTaskSensor` targets (`external_dag_id`, `external_task_id`), `TriggerDagRunOperator` targets, datasets, and BigQuery tables read/written (from SQL or from `INFORMATION_SCHEMA.JOBS` referenced_tables + labels). Draw the graph. Find cycles of waiting, orphans, duplicates.

**b) Query run history** (Airflow metadata DB via a maintenance DAG, or the REST API). Example on the metadata DB (PostgreSQL):

```sql
-- Failure rate, retries and p95 duration per task over 30 days
SELECT dag_id, task_id,
       COUNT(*)                                             AS runs,
       AVG(CASE WHEN state = 'failed' THEN 1 ELSE 0 END)    AS failure_rate,
       AVG(try_number - 1)                                  AS avg_retries,   -- semantics of try_number vary slightly by version
       PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration) AS p95_seconds
FROM task_instance
WHERE start_date > NOW() - INTERVAL '30 days'
GROUP BY dag_id, task_id
ORDER BY failure_rate DESC, p95_seconds DESC;

-- Sensor wait: total seconds spent in sensor tasks per DAG
SELECT dag_id, SUM(duration)/3600 AS sensor_hours
FROM task_instance
WHERE operator LIKE '%Sensor%' AND start_date > NOW() - INTERVAL '30 days'
GROUP BY dag_id ORDER BY sensor_hours DESC;
```

Export these daily to BigQuery so you can trend them in Looker.

**c) Classify failures** (read logs of the top 50 failures): upstream not ready / data issue / infrastructure (quota, OOM, timeout) / **non-idempotent rerun** / code bug.

**d) Find the critical path** of the nightly chain: the longest chain of dependent tasks by duration. Improving anything not on it doesn't make the chain finish earlier.

### 8.2 Target architecture
```text
Layer 1: INGESTION (shared ingestion project)
  DAG factory → ingest__<source> DAGs (one per source, standard pattern)
  each: wait (deferrable/event) → load (idempotent) → checks → publish Asset
            │ Asset events
            ▼
Layer 2: TRANSFORMATION (per domain)
  transform__sales  schedule = [raw_pos.orders, raw_erp.products, ...]
     └─ one task: dbt build --select tag:sales  (or Dataform workflow invocation)
        dbt handles the 120 model dependencies, tests, incremental logic
            │ Asset event "mart_sales ready"
            ▼
Layer 3: SERVING
  refresh Looker datagroup / notify / export
Standards: naming, default_args, pools, labels, alerts, idempotency, CI checks
```

### 8.3 Migration plan
1. **Quick wins first (weeks 2–4):** make the top failing tasks idempotent; switch long poke sensors to deferrable/reschedule; add pools to protect BigQuery and source DBs; set `catchup=False` and timeouts. These reduce failures fast and build trust.
2. **Build the factory and the transformation layer** with one pilot domain (the one with the most failures or the highest value, for example sales).
3. **Strangler pattern:** run the new flow in parallel with the old one, writing to a shadow dataset. **Compare outputs** (row counts, sums by day, `EXCEPT DISTINCT` diffs). When equal for N days, switch consumers, pause old DAGs, then delete them.
4. **Domain by domain**, never big bang. Keep a migration board: DAG → new home → status.
5. **Lock in the gains with CI** (DagBag tests, standards tests, cluster policies) so the mess doesn't return.

### 8.4 Metrics to prove success
| Metric | Before (example) | After (example target) |
|---|---|---|
| Number of DAGs | 300 | ~40 ingestion (factory) + ~8 domain transformation |
| Cross-DAG sensors | 500 | ~0 (Assets) |
| Nightly chain finish (p50/p95) | 07:40 / 09:30 | 05:30 / 06:00 |
| Task failure rate | 4% | < 0.5% |
| Manual reruns per week | 25 | < 3 |
| Sensor worker-hours per day | 300 | ~0 |
| BigQuery cost per day for the chain | € X | −30% (no duplicate work, pruned queries) |

These numbers are only examples; measure your own baseline first. The point is **you measure before and after**.

---

## 9. Testing, CI/CD and observability

### 9.1 Test levels
| Level | What | How |
|---|---|---|
| Static | Lint, format, Airflow-3 compatibility | `ruff` (incl. `AIR` rules), `sqlfluff` for SQL |
| **DAG integrity** | All DAGs import, no cycles, standards met | `DagBag` test with pytest |
| **Unit** | Factory output, custom operators, Python helpers, callbacks | pytest, mock GCP clients |
| SQL | Syntax and bytes estimate | BigQuery **dry run** of every rendered query |
| Transformation | Models build and tests pass | `dbt build` / Dataform run on a CI dataset |
| Integration | One DAG end-to-end on dev data | `airflow dags test <dag> <date>` or `dag.test()` in dev Composer or local |
| Idempotency | Same date twice → same result | Run twice, compare counts/checksums |

```python
# tests/test_dag_integrity.py
import pytest
from airflow.models import DagBag

@pytest.fixture(scope="session")
def dagbag():
    return DagBag(dag_folder="dags/", include_examples=False)

def test_no_import_errors(dagbag):
    assert not dagbag.import_errors, dagbag.import_errors   # a cycle also shows up as an import error

@pytest.mark.parametrize("required", ["owner", "retries"])
def test_default_args(dagbag, required):
    for dag_id, dag in dagbag.dags.items():
        assert required in dag.default_args, f"{dag_id} missing {required}"

def test_no_catchup_surprises(dagbag):
    for dag_id, dag in dagbag.dags.items():
        assert dag.catchup is False or "backfill-ok" in dag.tags, dag_id

def test_parse_time(dagbag):
    # DagBag records per-file parse stats
    slow = [s for s in dagbag.dagbag_stats if s.duration.total_seconds() > 2]
    assert not slow, slow
```

```python
# tests/test_factory.py
from ingestion_factory import build_ingestion_dag

def test_factory_standard_shape():
    dag = build_ingestion_dag({
        "source": "t", "pattern": "gcs_parquet", "owner": "x", "schedule": "@daily",
        "landing_prefix": "t", "target": "raw_t.t", "partition_field": "d",
        "quality_checks": [{"type": "not_null", "column": "id"}],
        "publishes": "bigquery://p/raw_t/t",
    })
    assert {"load", "check_0", "publish"} <= set(dag.task_ids)
    assert dag.get_task("load").write_disposition == "WRITE_TRUNCATE"
```

Local run of one DAG for one date, with real task execution (no scheduler needed):

```bash
airflow dags test sales_daily 2026-10-22
airflow tasks test sales_daily build_fct_sales_daily 2026-10-22   # one task, no state saved
```

### 9.2 CI/CD pipeline
```text
Pull request
 ├─ ruff + sqlfluff
 ├─ pytest: DagBag integrity, standards, factory, operators (in a container with the SAME
 │          Airflow + provider versions as Composer: pin a constraints file)
 ├─ render SQL templates for a sample date → BigQuery dry run (syntax + bytes)
 ├─ dbt build / Dataform run on pr_<number> dataset (see the Dataform and dbt lesson)
 └─ terraform plan (environment config, if changed)
Merge to main
 ├─ deploy to DEV Composer:
 │     gcloud storage rsync --recursive --delete-unmatched-destination-objects dags/ gs://<dev-bucket>/dags/
 │     (or: gcloud composer environments storage dags import --environment dev --location europe-north1 --source dags/)
 ├─ smoke test: trigger key DAGs for a test date, wait, check state
Release (tag / manual approval)
 └─ same artifacts → PROD Composer bucket
```

- Use **Cloud Build** or **GitHub Actions** with **Workload Identity Federation** (no service account key files).
- Deploy **all** files the DAGs need (dags/, SQL, configs, common modules) together, so you never have half a release in the bucket. Watch out for `--delete` removing files that someone uploaded by hand (that's a good thing: the bucket should only contain what's in git).
- Environment config (PyPI packages, Airflow overrides, env vars, workers) is managed with **Terraform** (`google_composer_environment`), not clicked in the console.
- Local development: **Composer Local Development CLI** (`composer-dev`) runs a local Airflow with the same image as a Composer version, or use the Astro CLI / docker compose (section 11).

### 9.3 Observability
| What to measure | Where it comes from | Why |
|---|---|---|
| DAG run duration (p50/p95) and **finish time** of critical DAGs | `dag_run` table; Composer metrics in Cloud Monitoring | "Load duration"; SLA to the business |
| Task duration trend | `task_instance` | Finds the slow step / regression |
| **Failure rate and retries** per DAG/task | `task_instance` | "Stability" |
| **Sensor wait time** | Duration of sensor tasks, or time in `deferred` | Finds wasted waiting and upstream lateness |
| Queue time (`queued` → `running`) | `task_instance.queued_dttm` vs. `start_date` | Shows worker shortage |
| Rows processed / bytes | Audit table written by each load; `INFORMATION_SCHEMA.JOBS` | "Throughput" |
| **Cost per pipeline** | BigQuery job **labels** (`dag_id`, `task_id`, `pipeline`) + `INFORMATION_SCHEMA.JOBS` | "Cost" |
| **Data freshness** | `NOW() - MAX(_ingested_at)` per table | The real user experience |
| Scheduler health | Composer monitoring: scheduler heartbeat, DAG parse time, DAG processor errors, worker pod evictions | Platform health |
| SLA / deadline misses | Airflow 2 `sla` (`sla_miss` table); Airflow 3.1+ Deadline Alerts; or your own freshness checks | Promise to users |

- **Logs:** task logs go to the environment bucket and **Cloud Logging** (log names like `airflow-worker`, `airflow-scheduler`). You can create **log-based metrics** and alerts (for example count of "Task failed" per DAG).
- **Alerting:** Airflow callbacks for task failures to the owning team's channel; **Cloud Monitoring alerting policies** for platform problems (scheduler heartbeat missing, environment unhealthy, many queued tasks); freshness checks for data problems. Every alert needs an **owner** and a **runbook** link.
- **Dashboard:** export Airflow metadata + `INFORMATION_SCHEMA.JOBS` to BigQuery daily; build one Looker dashboard: duration, failure rate, freshness, cost per pipeline. This is the concrete deliverable behind "define and track metrics".

---

## 10. Mapping from other platforms

| Other platform | Airflow / Composer equivalent | Key difference |
|---|---|---|
| **Fabric Data Pipeline** (activities, dependencies on success/failure/completion) | DAG with tasks; trigger rules (`all_success`, `one_failed`, `all_done`) | Airflow is code (Python), versioned in git, with templated intervals and backfill built in |
| Fabric pipeline **schedule trigger** | `schedule=` + timetable | Airflow runs are per **data interval**, not just "at this time" |
| Fabric "Invoke pipeline" activity | `TriggerDagRunOperator`, or better Datasets/Assets | Prefer data-aware triggers over direct calls |
| Fabric **Notebook activity** running Spark | `DataprocCreateBatchOperator` / `DataprocSubmitJobOperator`, or SQL in BigQuery | In GCP, most notebook-style SQL work moves to BigQuery + dbt/Dataform |
| Fabric **ForEach** activity | Dynamic task mapping `.expand()` | Each mapped instance retries separately |
| Fabric pipeline **parameters** | `params`, `dag_run.conf`, Jinja templates | |
| Fabric monitoring hub | Airflow UI (grid view, Gantt), Cloud Monitoring | You can query the metadata DB yourself |
| **AWS MWAA** | **Cloud Composer** | Same Airflow. S3 dags folder ↔ GCS dags folder; IAM role ↔ service account; Secrets Manager backend ↔ Secret Manager backend |
| **AWS Step Functions** (state machine, Wait states, Retry/Catch) | DAG + retries + trigger rules + sensors | Step Functions is serverless, event-driven, JSON/ASL, no "data interval" or backfill concept. Closest GCP equivalent is **Cloud Workflows** |
| EventBridge rule → Step Functions | GCS notification → Pub/Sub → trigger DAG via REST API; or Datasets | |
| **Self-managed Spark jobs** (spark-submit from cron) | Dataproc operators from Airflow | Airflow starts and monitors; the cluster computes |
| Kinesis Firehose → S3 → batch job | Pub/Sub → Dataflow → BigQuery (streaming, no Airflow); Airflow only for batch steps around it | Streaming jobs are long-running; Airflow should not "run" them every 5 minutes |
| Daily Spark batch aggregations | DAG per domain with idempotent partition overwrite per day | Same idea as Spark `insertOverwrite` per partition |

In short: if you know Fabric or Data Factory pipelines, Airflow is the same idea but as code. The pipeline is a Python file in git, and every run carries its data interval, which makes reruns and backfills first-class. The most common bugs after a platform migration are reruns that duplicate data, which is why every task should be idempotent.

---

## 11. Hands-on exercise (2–3 hours)

### Option A: Astro CLI (easiest local Airflow)
```bash
brew install astro                 # macOS; needs Docker Desktop running
mkdir airflow-lab && cd airflow-lab
astro dev init                     # creates dags/, include/, tests/, Dockerfile, requirements.txt
astro dev start                    # UI at http://localhost:8080 (check output for login)
```
Recent Astro Runtime images ship **Airflow 3**. To practise Airflow 2 (closer to many Composer environments), set an Airflow-2-based Runtime image in the `Dockerfile` (check Astronomer docs for version tags).

### Option B: official docker compose
Download `docker-compose.yaml` from the Airflow docs ("Running Airflow in Docker") for your chosen version, then `docker compose up airflow-init && docker compose up`.

### Option C: Composer Local Development CLI
`composer-dev` runs the same image as a given Composer version locally (check Google docs for install steps). Best for matching production.

### Exercise steps
1. **Time semantics.** Create `dags/lab_intervals.py`:

```python
import pendulum
from airflow.decorators import dag, task   # Airflow 3: from airflow.sdk import dag, task

@dag(schedule="@daily", start_date=pendulum.datetime(2026, 10, 15, tz="UTC"), catchup=True,
     max_active_runs=2, tags=["lab"])
def lab_intervals():
    @task
    def show(ds=None, data_interval_start=None, data_interval_end=None, run_id=None):
        print(f"run_id={run_id} ds={ds} start={data_interval_start} end={data_interval_end}")
    show()

lab_intervals()
```
   Unpause it. Count how many runs catchup creates. Check that the run with `ds=2026-10-15` covers 15 → 16 October. (On Airflow 3, compare with `schedule=CronDataIntervalTimetable("0 0 * * *", timezone="UTC")` vs. the default and see the `ds` difference.)

2. **Idempotency.** Add a task that writes `include/out/sales_{ds}.csv` with `mode="a"` (append). Clear one run twice; see duplicates. Change to `mode="w"` (overwrite); clear again; result is stable. That's partition overwrite in miniature.

3. **Retries and callbacks.** Make a task fail randomly (`random.random() < 0.5`), set `retries=3`, `retry_delay=10s`, and an `on_failure_callback` that prints the context. Watch `up_for_retry` in the grid view.

4. **Sensors.** Use a `FileSensor` (connection `fs_default`) with `mode="poke"` and then `mode="reschedule"`; watch the state `up_for_reschedule`. Try a deferrable `TimeDeltaSensorAsync` / `DateTimeSensorAsync` (names differ by version) and watch the `deferred` state and the triggerer.

5. **Datasets/Assets.** Create a producer DAG with `outlets=[Dataset("file://lab/sales")]` and a consumer with `schedule=[Dataset("file://lab/sales")]`. Trigger the producer; the consumer starts on its own. Look at the Datasets/Assets view.

6. **Dynamic mapping.** A task returns `["a", "b", "c"]`; map a task over it with `.expand()`. Look at the mapped instances.

7. **Factory.** Write two YAML files and a small factory that generates two DAGs. Break one YAML on purpose and see the import error in the UI.

8. **Tests.** Put the DagBag test in `tests/` and run `astro dev pytest` (or `pytest` inside the container). Run `astro dev run dags test lab_intervals 2026-10-20`.

9. **Optional, real BigQuery.** With your BigQuery sandbox project: `gcloud auth application-default login`, mount your ADC into the container (Astro docs explain), create a `google_cloud_default` connection, and run a `BigQueryInsertJobOperator` that overwrites `lab.daily$YYYYMMDD` from `bigquery-public-data.thelook_ecommerce.order_items` filtered by `{{ ds }}`. Run the same date twice and confirm the row count is unchanged. (Sandbox tables expire after 60 days and DML has limits; check sandbox limits.)

---

## 13. Glossary

| Term | Meaning |
|---|---|
| **DAG** | Directed Acyclic Graph: a workflow of tasks with dependencies and no cycles |
| **Task** | A node in a DAG; an instance of an operator |
| **Operator** | A template for a task type (`BigQueryInsertJobOperator`, `BashOperator`) |
| **Sensor** | An operator that waits for a condition |
| **Task instance** | One task in one DAG run, with a state and try number |
| **DAG run** | One execution of a DAG for one logical date / data interval |
| **Logical date** | Identity of a run; for interval timetables, the start of the data interval (old name: execution_date) |
| **Data interval** | The time window a run is responsible for (`data_interval_start` to `data_interval_end`) |
| **`ds` / `ds_nodash`** | Logical date as `YYYY-MM-DD` / `YYYYMMDD` |
| **Timetable** | The class that defines run times and data intervals |
| **Catchup** | Automatically creating runs for missed past intervals |
| **Backfill** | Deliberately running a DAG for a past date range |
| **Idempotent** | Same result no matter how many times it runs for the same input |
| **Scheduler** | Component that creates runs and decides which tasks run |
| **DAG processor** | Component that parses DAG files into serialized DAGs |
| **Executor** | How tasks are run (Local, Celery, Kubernetes) |
| **Worker** | Process that executes tasks |
| **Triggerer** | Component running async triggers for deferred tasks |
| **Deferrable operator** | Operator that frees its worker slot while waiting, handing off to the triggerer |
| **Metadata DB** | Database holding all Airflow state |
| **XCom** | Small values passed between tasks, stored in the metadata DB by default |
| **Pool** | Named set of slots limiting concurrency for a shared resource |
| **Priority weight** | Ordering of tasks competing for slots |
| **Trigger rule** | Condition on upstream states for a task to run |
| **Dataset / Asset** | URI naming data; used for data-aware scheduling (Asset is the Airflow 3 name) |
| **Dynamic task mapping** | Creating N task instances at run time with `.expand()` |
| **TaskFlow API** | `@dag` / `@task` decorator style |
| **DAG factory** | Code that generates many DAGs from configuration |
| **Cluster policy** | Function in `airflow_local_settings.py` that checks or mutates every DAG/task |
| **Zombie task** | A task marked running whose worker stopped heartbeating |
| **Jinja templating** | Rendering `{{ ... }}` expressions in operator fields at run time |
| **Secrets backend** | Where Airflow looks up connections/variables (e.g. Secret Manager) |
| **Cloud Composer** | Google's managed Airflow service |
| **Environment (Composer)** | One managed Airflow installation: GKE workloads, Cloud SQL DB, GCS bucket |
| **Composer bucket** | GCS bucket with `dags/`, `plugins/`, `data/`, `logs/` |
| **Workload Identity Federation** | Lets CI (GitHub, etc.) authenticate to GCP without key files |
| **Strangler pattern** | Replacing a system piece by piece, running old and new in parallel |
| **Critical path** | The longest chain of dependent tasks; it decides when the whole chain finishes |
| **Cosmos** | Astronomer library that renders a dbt project as Airflow tasks |
