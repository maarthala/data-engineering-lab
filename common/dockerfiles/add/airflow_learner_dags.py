"""Learner DAGs from the object store — one DAG bundle for ALL learners.

Every learner bucket <user>-lake has a files/src/dags/ folder (created by lakehouse.provision).
On each refresh this bundle lists the learner buckets and syncs each files/src/dags/ folder to
<bundle dir>/<user>/ — so a learner saves a .py into dags/ in Jupyter (synced to files/src/dags/),
or uploads it with My files / the RustFS console, and it shows up in Airflow a little later.
New learners appear without touching the Airflow config (one bundle, not one each).

DAG ids must start with <user>_ (one shared Airflow) — enforced by the dag_policy in
airflow_local_settings.py.

Which key, where (members' DAG code only ever lands where no admin login is):
  LAB_DAGS_S3_KEY / _SECRET  storage user dags-reader — reads every bucket's files/src/dags/ and
                             nothing else (airflow_lab_guard.py): the members' DAG processor
                             (and, local stack, the scheduler, which runs member tasks there)
  LAB_JOB_OWNER + LAB_JOB_S3_KEY / _SECRET   a member's task pod (k8s): only the owner's folder,
                             with the owner's own key (lab_jobs.py)
  neither                    nothing is synced (k8s: the Airflow pod — it never gets member code)
"""
import logging
import os
import shutil
from pathlib import Path

from airflow.dag_processing.bundles.base import BaseDagBundle

SUFFIX = "-lake"
ENDPOINT = os.environ.get("LAKE_S3_ENDPOINT", "http://storage:9000")
log = logging.getLogger(__name__)


def _client():
    key = os.environ.get("LAB_DAGS_S3_KEY") or os.environ.get("LAB_JOB_S3_KEY")
    secret = os.environ.get("LAB_DAGS_S3_SECRET") or os.environ.get("LAB_JOB_S3_SECRET")
    if not (key and secret):
        return None
    import boto3
    from botocore.config import Config
    return boto3.client("s3", endpoint_url=ENDPOINT, aws_access_key_id=key, aws_secret_access_key=secret,
                        region_name=os.environ.get("AWS_REGION", "us-east-1"),
                        config=Config(s3={"addressing_style": "path"}))


def _sync(s3, bucket, prefix, local):
    """Mirror bucket/prefix into `local`: new or changed files are downloaded, gone ones removed."""
    want = {}
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for o in page.get("Contents", []):
            rel = o["Key"][len(prefix):]
            if rel and not rel.endswith("/") and ".." not in Path(rel).parts:
                want[rel] = o
    for rel, o in want.items():
        path = local / rel
        mtime = o["LastModified"].timestamp()
        if path.is_file() and path.stat().st_size == o["Size"] and path.stat().st_mtime >= mtime:
            continue
        path.parent.mkdir(parents=True, exist_ok=True)
        s3.download_file(bucket, o["Key"], str(path))
        os.utime(path, (mtime, mtime))
    if local.is_dir():
        for path in sorted(local.rglob("*"), reverse=True):
            rel = path.relative_to(local).as_posix()
            if "__pycache__" in path.parts:
                continue
            if path.is_file() and rel not in want:
                path.unlink()
            elif path.is_dir() and not any(path.iterdir()):
                path.rmdir()


class LearnerDagsBundle(BaseDagBundle):
    supports_versioning = False

    def __init__(self, *, prefix="files/src/dags/", **kwargs):
        kwargs.pop("aws_conn_id", None)                 # (older config)
        super().__init__(**kwargs)
        self.prefix = prefix

    @property
    def path(self) -> Path:
        return self.base_dir

    def get_current_version(self):
        return None

    def initialize(self) -> None:
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.refresh()
        super().initialize()

    def refresh(self) -> None:
        s3 = _client()
        if s3 is None:
            log.info("learner DAGs: no key here — nothing synced")
            return
        with self.lock():
            owner = os.environ.get("LAB_JOB_OWNER")
            users = [owner] if owner else [b["Name"][: -len(SUFFIX)] for b in s3.list_buckets().get("Buckets", [])
                                           if b["Name"].endswith(SUFFIX)]
            for user in users:
                try:
                    _sync(s3, user + SUFFIX, self.prefix, self.base_dir / user)
                except Exception as e:          # one broken bucket mustn't hide everyone's DAGs
                    log.warning("learner DAGs: skip %s: %s", user, e)
            for d in self.base_dir.iterdir():   # learner removed → their DAGs go too
                if d.is_dir() and d.name not in users:
                    shutil.rmtree(d, ignore_errors=True)
