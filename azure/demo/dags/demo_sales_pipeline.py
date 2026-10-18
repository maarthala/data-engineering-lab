"""Demo pipeline: Bronze → Silver → Gold, by running the three demo notebooks in order.

Put this file in Jupyter's my_files/dags/ folder — Airflow picks it up in ~30 s.
Each task runs one notebook from my_files/notebooks/ as you (`my` = your lakehouse), cell by
cell; if a cell fails, the task fails and its log shows which one.

The dag_id is <your username>_demo_sales — it takes your username from the folder Airflow
loaded the file from, so the file works for any member without editing.
"""
from datetime import datetime

from airflow.sdk import dag, task

# Airflow loads members' DAGs from .../learners/<username>/... — a dag_id must start with <username>_
OWNER = __file__.split("/learners/", 1)[-1].split("/", 1)[0].replace("-", "_") if "/learners/" in __file__ else "demo"


@dag(
    dag_id=f"{OWNER}_demo_sales",
    description="Sales CSVs → bronze.sales → silver.sales → gold.sales_daily / gold.top_products",
    start_date=datetime(2026, 1, 1),
    schedule=None,                 # manual: ▶ Trigger. "@daily" would run it every night
    catchup=False,
    tags=["demo", "medallion"],
)
def demo_sales():

    @task
    def bronze():
        from lab_spark import run_notebook
        run_notebook("demo_01_bronze.ipynb")

    @task
    def silver():
        from lab_spark import run_notebook
        run_notebook("demo_02_silver.ipynb")

    @task
    def gold():
        from lab_spark import run_notebook
        run_notebook("demo_03_gold.ipynb")

    bronze() >> silver() >> gold()


demo_sales()
