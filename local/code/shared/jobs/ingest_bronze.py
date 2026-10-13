"""ShopFlow Bronze ingest — land raw Postgres tables as Iceberg tables.

Run by Airflow (orchestration unit) or by hand:
  spark-submit --master spark://spark-master:7077 \
    --conf spark.sql.catalogImplementation=in-memory \
    ingest_bronze.py --catalog iceberg
"""
import argparse
import os
from pyspark.sql import SparkSession


def read_pg(spark, table):
    return (spark.read.format("jdbc")
            .option("url", "jdbc:postgresql://postgres:5432/shopflow")
            .option("dbtable", table)
            .option("user", "learner").option("password", "learner")   # read-only course login
            .option("driver", "org.postgresql.Driver").load())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", default="iceberg")
    ap.add_argument("--date", default=None)      # accepted for scheduling; full rebuild here
    ap.add_argument("--mode", default="full")
    args = ap.parse_args()
    cat = args.catalog

    spark = SparkSession.builder.appName("shopflow_ingest_bronze").getOrCreate()

    # the cluster holds no Polaris login: this platform job uses the one Airflow gives it

    if os.environ.get("POLARIS_CREDENTIAL"):

        spark.conf.set(f"spark.sql.catalog.{cat}.credential", os.environ["POLARIS_CREDENTIAL"])
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cat}.bronze")
    for t in ["customers", "products", "orders", "order_items"]:
        read_pg(spark, t).writeTo(f"{cat}.bronze.{t}").using("iceberg").createOrReplace()
        print(f"wrote {cat}.bronze.{t}")
    print("BRONZE_ROWS", spark.table(f"{cat}.bronze.orders").count())
    spark.stop()


if __name__ == "__main__":
    main()
