"""ShopFlow on Google Cloud, every day: files in Cloud Storage -> Bronze -> Silver (Dataflow) -> Gold.

Cloud Composer (managed Airflow). Change the three settings below, then copy this file into the
environment's dags/ folder.
"""
import pendulum
from airflow import DAG
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowConfiguration
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

PROJECT = "your-project-id"            # gcloud config get-value project
BUCKET = "your-project-id-shopflow"    # the bucket from the Cloud Storage lesson
REGION = "us-central1"                 # the region you use everywhere

GOLD_SQL = """
create or replace table shopflow_gold.daily_sales as
select date(o.order_ts)                         as order_date,
       o.channel,
       count(distinct o.order_id)               as orders,
       round(sum(i.quantity * i.unit_price), 2) as revenue,
       sum(i.quantity)                          as units
from shopflow_silver.orders o
join shopflow_bronze.order_items i using (order_id)
where o.status = 'delivered'
group by order_date, o.channel;
"""

with DAG(
    dag_id="shopflow_gcp",
    schedule="0 6 * * *",                       # every day at 06:00 UTC
    start_date=pendulum.datetime(2026, 6, 1, tz="UTC"),
    catchup=False,
    tags=["shopflow", "gcp"],
) as dag:

    bronze_items = GCSToBigQueryOperator(
        task_id="bronze_order_items",
        bucket=BUCKET,
        source_objects=["shopflow/order_items/*.csv"],
        destination_project_dataset_table=f"{PROJECT}.shopflow_bronze.order_items",
        source_format="CSV",
        skip_leading_rows=1,
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
        location=REGION,
    )

    silver_orders = BeamRunPythonPipelineOperator(
        task_id="silver_orders",
        runner="DataflowRunner",
        py_file=f"gs://{BUCKET}/code/silver_orders.py",
        pipeline_options={
            "input": f"gs://{BUCKET}/shopflow/orders/*.csv",
            "output_table": f"{PROJECT}:shopflow_silver.orders",
            "rejected": f"gs://{BUCKET}/rejected/orders",
            "temp_location": f"gs://{BUCKET}/tmp",
        },
        dataflow_config=DataflowConfiguration(
            job_name="silver-orders", project_id=PROJECT, location=REGION),
    )

    gold = BigQueryInsertJobOperator(
        task_id="gold_daily_sales",
        configuration={"query": {"query": GOLD_SQL, "useLegacySql": False}},
        project_id=PROJECT,
        location=REGION,
    )

    [bronze_items, silver_orders] >> gold
