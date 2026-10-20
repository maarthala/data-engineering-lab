from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
import pendulum

JOBS = "/code/shared/jobs"                    # compose; on k8s these are git-synced with the DAGs
SPARK_MASTER = "spark://spark-master:7077"

def spark_job(script: str) -> str:
    """A spark-submit command for one medallion job (writes the iceberg catalog)."""
    return (
        f"spark-submit --master {SPARK_MASTER} "
        "--conf spark.sql.catalogImplementation=in-memory "   # this stack has no Hive Metastore
        "--conf spark.cores.max=2 "                           # share the cluster with notebooks
        f"{JOBS}/{script} --catalog iceberg"
    )

with DAG(
    dag_id="shopflow_medallion",
    description="Bronze → Silver → Gold for ShopFlow",
    schedule=None,                  # run manually in this lesson; 5.3 adds a schedule
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["unit5", "medallion"],
) as dag:

    bronze = BashOperator(task_id="bronze", bash_command=spark_job("ingest_bronze.py"))
    silver = BashOperator(task_id="silver", bash_command=spark_job("build_silver.py"))
    gold   = BashOperator(task_id="gold",   bash_command=spark_job("build_gold.py"))

    # the medallion order — this is the whole point of the DAG
    bronze >> silver >> gold