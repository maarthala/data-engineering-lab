"""Pipeline: reload bronze.sample_orders by running the notebook load_sample_orders.ipynb.

Lives in your dags/ folder (files/src/dags in your bucket) — Airflow picks it up in ~30 s.
The task runs your notebook from notebooks/ as you (your own lakehouse, `my`), cell by
cell; if a cell fails, the task fails and its log shows which one.

Run it: Airflow → __USER___load_sample_orders → switch it on → ▶ Trigger.
"""
from datetime import datetime

from airflow.sdk import dag, task


@dag(
    dag_id="__USER___load_sample_orders",   # must start with your username + "_"
    start_date=datetime(2026, 1, 1),
    schedule=None,                        # manual; try "@daily" to run it every day
    catchup=False,
    tags=["starter", "bronze"],
)
def load_sample_orders():

    @task
    def run_load_notebook():
        from lab_spark import run_notebook
        run_notebook("load_sample_orders.ipynb")

    run_load_notebook()


load_sample_orders()
