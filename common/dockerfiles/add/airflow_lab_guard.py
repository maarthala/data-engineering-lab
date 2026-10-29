"""airflow_lab_guard — what Airflow's TRUSTED side does for the members' DAG processor.

Members' DAG files run (top-level code, DAG callbacks) in their own DAG processor
(`airflow dag-processor --bundle-name learners`), which holds no admin login:
  - database login airflow_parser: Airflow's tables minus logins/permissions, connections,
    variables, other runs' XComs and rendered values (grant_parser)
  - storage user dags-reader: every bucket's files/src/dags/ and nothing else (ensure_dags_reader)
  - no FAB auth manager → it can't write permissions; sync_perms gives each member's role access
    to their own DAGs instead (owner = the DAG's folder, only if the dag_id has that prefix — a
    tampered row grants nothing to anyone else)

Run in a trusted Airflow container: `python airflow_lab_guard.py` (grants + reader once, then
permissions every 30 s). Env: LAB_DAGS_READER_SECRET or LAB_DAGS_S3_SECRET (the reader's), PARSER_ROLE.
"""
import json
import logging
import os
import subprocess
import time
import urllib.parse

log = logging.getLogger("lab-guard")
PARSER_ROLE = os.environ.get("PARSER_ROLE", "airflow_parser")
# no access at all (logins, secrets, other members' data) … (trigger stays: asset watchers are
# triggers — the triggerer imports no member code and holds no admin login, see airflow.yaml)
DENY = ("ab_%", "session", "revoked_token", "connection", "connection_test_request", "variable", "xcom",
        "rendered_task_instance_fields", "hitl_detail", "hitl_detail_history", "task_state_store",
        "asset_state_store", "alembic_version_fab")
# … or only this
ONLY = {"log": "INSERT, SELECT (id)", "alembic_version": "SELECT", "team": "SELECT", "dag_bundle_team": "SELECT"}
READER_POLICY = {"Version": "2012-10-17", "Statement": [
    {"Effect": "Allow", "Action": ["s3:ListAllMyBuckets"], "Resource": ["arn:aws:s3:::*"]},
    {"Effect": "Allow", "Action": ["s3:ListBucket"], "Resource": ["arn:aws:s3:::*-lake"],
     "Condition": {"StringLike": {"s3:prefix": ["files/src/dags/*"]}}},
    {"Effect": "Allow", "Action": ["s3:GetObject"], "Resource": ["arn:aws:s3:::*-lake/files/src/dags/*"]}]}


def grant_parser():
    """(Re)grant airflow_parser after each start — a migration may have added tables."""
    from sqlalchemy import text
    from airflow.settings import engine
    with engine.begin() as c:
        if not c.execute(text("SELECT 1 FROM pg_roles WHERE rolname = :r"), {"r": PARSER_ROLE}).scalar():
            log.warning("no database role %s — the members' DAG processor can't sign in", PARSER_ROLE)
            return False
        tables = [r[0] for r in c.execute(text("SELECT tablename FROM pg_tables WHERE schemaname = 'public'"))]
        c.execute(text(f"GRANT USAGE ON SCHEMA public TO {PARSER_ROLE}"))
        c.execute(text(f"GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO {PARSER_ROLE}"))
        c.execute(text(f"GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO {PARSER_ROLE}"))
        for t in tables:
            deny = any(t == d or (d.endswith("%") and t.startswith(d[:-1])) for d in DENY)
            if deny or t in ONLY:
                c.execute(text(f'REVOKE ALL ON TABLE "{t}" FROM {PARSER_ROLE}'))
            if t in ONLY:
                c.execute(text(f'GRANT {ONLY[t]} ON TABLE "{t}" TO {PARSER_ROLE}'))
    return True


def ensure_dags_reader():
    """Storage user + policy dags-reader with the secret from LAB_DAGS_S3_SECRET (idempotent)."""
    import lakehouse as lh
    # LAB_DAGS_READER_SECRET where this process must not sync member DAGs itself (k8s: the Airflow pod)
    key = os.environ.get("LAB_DAGS_S3_KEY", "dags-reader")
    secret = os.environ.get("LAB_DAGS_READER_SECRET") or os.environ.get("LAB_DAGS_S3_SECRET", "")
    if not (key and secret):
        return False
    lh._admin("PUT", "add-canned-policy", {"name": key}, json.dumps(READER_POLICY).encode())
    u = urllib.parse.urlparse(lh.S3_ENDPOINT)
    env = dict(os.environ, MC_HOST_lab=f"{u.scheme}://{lh.S3_KEY}:{lh.S3_SECRET}@{u.netloc}")
    for args in (["admin", "user", "add", "lab", key, secret], ["admin", "policy", "attach", "lab", key, "--user", key]):
        r = subprocess.run(["mc", "--json", *args], env=env, capture_output=True, text=True, timeout=30)
        if r.returncode and "already" not in r.stdout + r.stderr:
            raise RuntimeError(f"mc {' '.join(args[:3])}: {(r.stdout + r.stderr).strip()[:200]}")
    return True


_synced = {}


def sync_perms(full=False):
    """Each learner DAG: only its owner's role may see / run it (see learner_access)."""
    from airflow.models.dag import DagModel
    from airflow.providers.fab.www.security_appless import ApplessAirflowSecurityManager
    from airflow.utils.session import create_session
    from airflow_local_settings import learner_access, owner_prefix
    n = 0
    with create_session() as session:
        rows = session.query(DagModel.dag_id, DagModel.relative_fileloc).filter(
            DagModel.bundle_name == "learners", DagModel.is_stale.is_(False)).all()
        sm = ApplessAirflowSecurityManager(session=session)
        for dag_id, rel in rows:
            owner = (rel or "").split("/", 1)[0]
            if "/" not in (rel or "") or not dag_id.startswith(owner_prefix(owner)):
                continue
            if not full and _synced.get(dag_id) == owner:
                continue
            sm.sync_perm_for_dag(dag_id, learner_access(owner))
            _synced[dag_id] = owner
            n += 1
    return n


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="[lab-guard] %(message)s")
    for step in (grant_parser, ensure_dags_reader):
        for attempt in range(20):                   # storage / database may still be starting
            try:
                log.info("%s: %s", step.__name__, step())
                break
            except Exception as e:  # noqa: BLE001
                log.warning("%s: %s — retrying", step.__name__, e)
                time.sleep(15)
    last_full = 0
    while True:
        try:
            full = time.time() - last_full > 600
            n = sync_perms(full)
            last_full = time.time() if full else last_full
            if n:
                log.info("DAG permissions synced: %d", n)
        except Exception as e:  # noqa: BLE001
            log.warning("sync_perms: %s", e)
        time.sleep(30)
