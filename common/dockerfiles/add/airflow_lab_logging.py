"""Airflow task logs of members' tasks (k8s): kept in the OWNER's bucket, files/logs/airflow/….

A member's task runs in its own pod (airflow_local_settings.pod_mutation_hook), which is gone when
the task ends — so the log is uploaded at the end, with the owner's own key (it may write only
their bucket): <owner>-lake/files/logs/airflow/dag_id=…/run_id=…/task_id=…/attempt=N.log. Airflow's
UI reads it from there (the api-server's key reads every bucket); the member also finds their logs
in My files. The stack's own tasks keep their logs on the Airflow pod's disk, as before.

Config: [logging] logging_config_class = airflow_lab_logging.LOGGING_CONFIG, remote_logging = True,
remote_base_log_folder = s3://… (any — Airflow's own S3 reader below is not used).
"""
import io
import os
import pathlib
import urllib.error

from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG as LOGGING_CONFIG  # noqa: F401
from airflow.configuration import conf

PREFIX = "files/logs/airflow/"
BASE = pathlib.Path(os.path.expanduser(conf.get("logging", "base_log_folder")))


def _bucket_of(dag_id):
    """The owner's bucket of a learner DAG (its folder in the learners bundle, if the dag_id has
    that folder's prefix), else None."""
    from airflow.models.dag import DagModel
    from airflow.utils.session import create_session
    from airflow_local_settings import owner_prefix
    with create_session() as session:
        row = session.query(DagModel.bundle_name, DagModel.relative_fileloc).filter(DagModel.dag_id == dag_id).first()
    if not row or row[0] != "learners" or "/" not in (row[1] or ""):
        return None
    owner = row[1].split("/", 1)[0]
    return owner + "-lake" if dag_id.startswith(owner_prefix(owner)) else None


class MemberLogIO:
    processors = ()

    def upload(self, path, ti=None):
        bucket = os.environ.get("LAB_JOB_BUCKET")          # only in a member's task pod
        if not bucket:
            return
        import lakehouse as lh
        local = pathlib.Path(path)
        local = local if local.is_absolute() else BASE / local
        if not local.is_file():
            return
        data = local.read_bytes()
        lh.put_object(bucket, PREFIX + local.relative_to(BASE).as_posix(), io.BytesIO(data), len(data), "text/plain")

    def read(self, relative_path, ti):
        bucket = _bucket_of(ti.dag_id)
        if not bucket:
            return [], None
        import lakehouse as lh
        try:
            with lh.get_object(bucket, PREFIX + relative_path) as r:
                text = r.read().decode("utf-8", "replace")
        except urllib.error.HTTPError:
            return [], None
        return [f"s3://{bucket}/{PREFIX}{relative_path}"], [text]


REMOTE_TASK_LOG = MemberLogIO()
