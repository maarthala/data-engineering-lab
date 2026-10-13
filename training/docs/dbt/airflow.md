# Run dbt from Airflow

## Concept
So far you've typed `dbt build` yourself. In production it runs **on a schedule**, right after
the Bronze load, and somebody hears about it when a test fails. That's Airflow's job
([Airflow basics](../unit5/basics.md)): dbt builds the models in the right order, Airflow decides
**when** dbt runs and **what comes before and after** it.

In the lab, a DAG task runs dbt with one helper, **`lab_dbt()`**, next to the `lab_spark()` you
used in [Schedule ingestion & Spark jobs](../unit5/schedule.md):

```python
from lab_spark import lab_dbt
lab_dbt("build")            # = `dbt build` in your project
```

What it does, step by step:

1. Takes **your project** from your bucket: `files/src/dbt/shop/`, the mirror of Jupyter's
   `dbt/shop/` folder.
2. Signs in **as you**: your lakehouse, with your jobs login (the same one `lab_spark()` uses;
   your notebook login changes at every Jupyter start, so jobs get their own).
3. Runs **`dbt deps`** if the project has a `packages.yml`, then your command, and prints dbt's
   output into the **task log**.
4. **Fails the task** if dbt fails, so a failing test turns the task red.

Your `profiles.yml` works unchanged: it reads your lakehouse and login from environment
variables, and `lab_dbt()` sets those for the run.

## Lab
You need the project from the previous lessons in `dbt/shop/`, and the Airflow UI.

!!! warning "Airflow runs the copy in your bucket"
    Files you **save in Jupyter's editor** reach your bucket straight away. Files created or
    changed **from the terminal** (`cat >`, `sed`, or files dbt writes) only reach it at the next
    server start (**File → Hub Control Panel → Stop My Server → Start**). If the DAG runs an old
    version of a model, that's why. Check in **📁 My files** → `files/src/dbt/shop/`.

### 1 · One task: build everything
New file in Jupyter's **`dags/`** folder, **`<you>_dbt_shop.py`** (with your username, like every
DAG: [Airflow basics](../unit5/basics.md#in-the-lab-your-dags-live-in-jupyters-dags-folder)):

```python
from datetime import datetime

from airflow.sdk import dag, task


@dag(dag_id="<you>_dbt_shop", schedule="@daily", start_date=datetime(2026, 1, 1), catchup=False)
def dbt_shop():

    @task
    def dbt_build():
        from lab_spark import lab_dbt
        lab_dbt("build")                 # seeds, models, snapshots and tests, in DAG order

    dbt_build()


dbt_shop()
```

Every line is from [Airflow basics](../unit5/basics.md): `@dag` with an id, a schedule and no
catchup, one `@task`. The import is **inside** the task, so it only runs when the task runs.

In the Airflow UI, find **`<you>_dbt_shop`** (after ~30 s), switch it **on**, click **▶ Trigger**,
and open the task's **Logs**:

```text
lab_dbt: you are dbtlab · project dbt/shop (19 files) · iceberg = dbtlab_lake
lab_dbt: $ dbt deps
16:19:20  Running with dbt=1.12.5
16:19:21  Installing dbt-labs/dbt_utils
16:19:21  Installed from version 1.4.1
16:19:21  Up to date!
lab_dbt: $ dbt build
16:19:25  Running with dbt=1.12.5
16:19:26  Registered adapter: spark=1.11.0
16:19:26  Unable to do partial parsing because saved manifest not found. Starting full parse.
16:19:34  Found 5 models, 6 data tests, 1 seed, 1 snapshot, 1 source, 1 exposure, 633 macros
16:19:36  1 of 14 START sql view model silver.stg_orders ................................. [RUN]
16:19:36  1 of 14 OK created sql view model silver.stg_orders ............................ [OK in 0.32s]
16:19:36  2 of 14 START seed file silver.channels ........................................ [RUN]
16:19:37  2 of 14 OK loaded seed file silver.channels .................................... [INSERT 4 in 1.19s]
16:19:37  3 of 14 START snapshot snapshots.orders_snapshot ............................... [RUN]
16:19:42  3 of 14 OK snapshotted snapshots.orders_snapshot ............................... [OK in 4.74s]
…
16:19:51  13 of 14 START sql table model gold.daily_orders_by_channel .................... [RUN]
16:19:52  13 of 14 OK created sql table model gold.daily_orders_by_channel ............... [OK in 1.50s]
16:19:53  14 of 14 NO-OP exposure sales_dashboard ........................................ [NO-OP in 0.00s]
16:19:53  Done. PASS=13 WARN=0 ERROR=0 SKIP=0 NO-OP=1 REUSED=0 TOTAL=14
```

(This run is by the member `dbtlab`; yours says your name.) The same output as in your terminal:
dbt doesn't know or care that Airflow started it.

!!! note "A second run appears when you switch it on"
    With `schedule="@daily"` and `catchup=False`, switching the DAG on immediately runs the most
    recent day (a *scheduled* run), besides the one you trigger by hand. Both build the same
    tables; dbt models are **idempotent**, so running twice gives the same result.

### 2 · When a test fails, the task fails
Make a test fail on purpose: a singular test that always returns a row,
**`tests/zz_always_fails.sql`** (save it in Jupyter's editor):

```sql
select 1 as always_fails
```

Trigger the DAG again. The task turns **red** (`failed`), and its log ends with:

```text
16:27:29  4 of 15 FAIL 1 zz_always_fails ................................................. [FAIL 1 in 0.22s]
16:27:37  Completed with 1 error, 0 partial successes, and 0 warnings:
16:27:37  [ERROR]: in test zz_always_fails (tests/zz_always_fails.sql)
16:27:37  Done. PASS=13 WARN=0 ERROR=1 SKIP=0 NO-OP=1 REUSED=0 TOTAL=15
```

followed by Airflow's *Task failed with exception*. Now Airflow's retries, alerts and the red
square in the **Grid** work for data quality too. **Delete the file** before you continue.

### 3 · One task per layer
One `dbt build` task is simple, and often enough. Splitting it shows **where** a run failed, and
lets other tasks sit in between (here, the snapshot after the staging tests):

```python
@dag(dag_id="<you>_dbt_shop", schedule="@daily", start_date=datetime(2026, 1, 1), catchup=False)
def dbt_shop():

    @task
    def staging():
        from lab_spark import lab_dbt
        lab_dbt("build -s staging channels")      # the staging views, the seed + their tests

    @task
    def snapshot():
        from lab_spark import lab_dbt
        lab_dbt("snapshot")

    @task
    def marts():
        from lab_spark import lab_dbt
        lab_dbt("build -s marts")                  # the gold tables + their tests

    staging() >> snapshot() >> marts()


dbt_shop()
```

- **`-s staging channels`**: two selectors separated by a space mean **either**: everything in
  the `staging` folder, plus the `channels` seed. A folder name selects every model in it.
- **`-s marts`**: the four Gold models (and the exposure defined in that folder).
- **`>>`**: the order, as in Unit {unit:unit5/basics.md}. If a staging test fails, `snapshot` and `marts`
  never start: the same protection `dbt build` gives inside one run, now visible as tasks.

All three turn green:

```text
staging=success  snapshot=success  marts=success
```

In the `staging` log: `Done. PASS=8 …`; in `marts`: `Done. PASS=4 … NO-OP=1`.

!!! info "Bigger projects: one task per model"
    Teams with hundreds of models often use **Astronomer Cosmos**, an Airflow package that reads
    a dbt project and creates **one Airflow task per model and test**, with dbt's DAG as the
    Airflow DAG. The idea is the same as step 3, generated automatically.

### 4 · Where it fits: after Bronze
A real pipeline loads Bronze first, then runs dbt on what was loaded. The pattern, with a
Bronze task like `bronze_day` from [Schedule ingestion & Spark jobs](../unit5/schedule.md) in
front (and the dbt sources pointing at the tables it writes):

```python
    bronze_day() >> staging() >> snapshot() >> marts()
```

Spark (Python) for ingestion, dbt (SQL) for Silver and Gold, Airflow for the order and the
schedule: the split from [Why dbt](why-dbt.md).

## 🎯 This runs unchanged on Azure, Databricks, Snowflake & Fabric
Every platform can run dbt on a schedule: a **dbt task** in Databricks Jobs, **dbt Projects** on
Snowflake (run by Tasks), a **dbt job** in Fabric Data Factory, an Azure Data Factory pipeline
calling dbt, or **dbt Cloud**'s own scheduler. Airflow + dbt, as here, is the most common
open-source pairing.

## You can now…
- Run your dbt project from an Airflow task with **`lab_dbt()`**, signed in as you
- Read dbt's output in the task log, and know a failed test **fails the task**
- Split a build into tasks per layer with **selectors** and `>>`
- Explain why the project Airflow runs is the one in **your bucket**
- Name **Cosmos** as the one-task-per-model option for big projects
