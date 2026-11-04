# 🔀 Pipelines

**Tools → Pipelines** opens Airflow, signed in with your lab account. One Airflow serves the whole lab;
you see and run your own DAGs (and your team's). You don't upload DAGs to Airflow: you save a `.py`
file in your bucket and Airflow loads it. Lessons: [Airflow basics](../unit5/basics.md) and the ones
after it.

## Where your DAGs come from
Airflow reads every member's `files/src/dags/` folder about every 30 seconds.

| You save the file in | It lands in | Airflow shows it |
|---|---|---|
| Jupyter: `my_files/dags/` | your bucket, `files/src/dags/` | as one of your DAGs |
| Jupyter: `<team>_team/dags/` (e.g. `sales_team/dags/`) | your team's bucket, `files/src/dags/` | as one of your team's DAGs |
| [📁 My files](my-files.md) or the [🪣 Storage](storage.md) console: `files/src/dags/` | the same folder | the same |

Your `my_files/dags/` folder starts with one example: `load_sample_orders.py`.

## Naming
One Airflow means every `dag_id` must be unique, so:

| Whose DAG | The `dag_id` starts with | Example |
|---|---|---|
| yours | your username and `_` | `demouser_hello_shopflow` |
| your team's | `team_<team>_` | `team_sales_daily_load` |

A DAG with another name doesn't load. Airflow lists it as an import error that names the prefix to use,
e.g. *DAG id 'hello' must start with 'demouser_'*. Each DAG also gets a tag with its owner.

## What you see
- Your own DAGs and your team's: you can see, run, pause and delete them, and their runs.
- The lab's own DAGs, if there are any: read only.
- Other members' DAGs: not at all.

## Running and pausing
1. A new DAG starts paused. Switch the toggle next to its name on to unpause it; it then follows its
   `schedule`.
2. **Trigger** (the ▶ button) starts a run now.
3. Open the DAG for its runs and tasks: each task's colour is its state. Click a task, then **Logs**,
   for what it printed.
4. Switch the toggle off to pause it again.

## Where the tasks run
| Stack | A task of your DAG runs |
|---|---|
| Team cluster | in a pod of its own, next to Airflow, with only your logins: it can only act as you |
| Laptop | inside Airflow (a single-machine test stack) |

Inside a task these give you the same lab a notebook has, signed in as the DAG's owner:

| Function | What it does |
|---|---|
| `lab_spark()` | a Spark session: `my` = your lakehouse, `shared`, your team's `team_<team>` |
| `run_notebook(notebook)` | runs one of your notebooks |
| `lab_dbt(command="build", project="shop")` | runs one of your dbt projects |
| `key_vault(name, field=None)` | one of your secrets |

Import them in the task, e.g. `from lab_spark import lab_spark`. A team's DAG runs as the team: it has
`team_<team>` and `shared`, no `my`. The Spark work shows on the [⚡ Spark UI](spark-flink-ui.md).

Airflow needs the **Airflow** unit. If its tile is greyed out, a lab manager switches it on:
`make status` on a laptop, **Manage → ⚙️ Settings → Services** on a team cluster.

## Related lessons
- [Airflow basics](../unit5/basics.md)
- [Build the medallion DAG](../unit5/medallion-dag.md)
- [Schedule ingestion & Spark jobs](../unit5/schedule.md)
- [The development workflow: notebook → job → DAG](../unit5/dev-workflow.md)
- [Run dbt from Airflow](../dbt/airflow.md)
