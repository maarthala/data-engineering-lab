"""Bronze → Silver → Gold for ShopFlow — fills the course's SHARED lake (polaris_lake,
the `iceberg` catalog of Trino / SQLPad / Superset; `shared` in learner notebooks).

Same jobs as the local stack's DAG (local/code/shared/jobs), but run through Spark
Connect: this Airflow has no Spark, and SPARK_REMOTE points at the spark-connect server,
which holds the iceberg (Polaris) config and the Postgres driver. The job scripts are
mounted from the chart (files/medallion-jobs) at /opt/airflow/jobs.
"""
import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

JOBS = "/opt/airflow/jobs"


def job(script):
    return f"python {JOBS}/{script} --catalog iceberg"


with DAG(
    dag_id="shopflow_medallion",
    description="Bronze → Silver → Gold for ShopFlow (the shared lake)",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["unit5", "medallion"],
) as dag:
    bronze = BashOperator(task_id="bronze", bash_command=job("ingest_bronze.py"))
    silver = BashOperator(task_id="silver", bash_command=job("build_silver.py"))
    gold = BashOperator(task_id="gold", bash_command=job("build_gold.py"))

    bronze >> silver >> gold
