"""Run a Spark job from Airflow the decoupled, k8s-native way. Airflow does NOT
contain Spark; instead KubernetesPodOperator launches a short-lived pod (from the
spark image) that runs spark-submit against the standalone cluster
(spark-master in the stack's namespace). Airflow only orchestrates — Spark lives in its own
ephemeral pod, so the two are fully decoupled.

The pod runs in client mode: the driver lives in THIS pod, executors run on the
Spark worker. A bare pod has no DNS name, so we pin spark.driver.host to the pod
IP (routable via the CNI) and bind to 0.0.0.0 — otherwise executors can't reach
the driver. (Jar conflicts that used to break the executor<->worker handshake are
now fixed in the spark image itself, so no runtime jar surgery is needed.)
"""
from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

SPARK_IMAGE = "ghcr.io/maarthala/de-stack/spark:latest"

SUBMIT = (
    "exec /opt/spark/bin/spark-submit --master spark://spark-master.de-stack.svc:7077 "
    "--conf spark.driver.host=$(hostname -i) "
    "--conf spark.driver.bindAddress=0.0.0.0 "
    "--conf spark.cores.max=2 "                  # share the cluster with spark-connect
    "--class org.apache.spark.examples.SparkPi "
    "/opt/spark/examples/jars/spark-examples_*.jar 20"
)

with DAG(
    dag_id="spark_k8s_dag",
    description="Run Spark on the standalone cluster via KubernetesPodOperator",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["spark", "k8s"],
) as dag:
    start = EmptyOperator(task_id="start")

    spark_pi = KubernetesPodOperator(
        task_id="spark_pi",
        name="airflow-spark-pi",
        namespace="de-jobs",                # Airflow may start pods only there
        image=SPARK_IMAGE,
        # The image is ctr-imported onto the node. Without this, k8s defaults `:latest` to
        # Always and re-pulls ~3.7 GB from GHCR on every run (which may also be stale).
        image_pull_policy="IfNotPresent",
        cmds=["/bin/bash", "-c"],
        arguments=[SUBMIT],
        get_logs=True,
        in_cluster=True,
        on_finish_action="delete_pod",
    )

    end = EmptyOperator(task_id="end")

    start >> spark_pi >> end
