"""lab_spark() — a Spark session for a learner's DAG, signed in AS THAT LEARNER (lab_dbt(): their dbt project;
key_vault(): one of their secrets).

Learner DAGs (synced from <user>-lake/dags/ by the "learners" bundle) run in the shared
Airflow, which has no learner credentials. This helper gives a task the same Spark a notebook
has: `iceberg` = the learner's own lakehouse (<user>_lake), `shared` = the course's shared lake.

    from airflow.sdk import dag, task

    @dag(dag_id="ravi_medallion", schedule=None, start_date=...)
    def ravi_medallion():
        @task
        def bronze():
            from lab_spark import lab_spark
            spark = lab_spark()                     # signs in as ravi
            spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.bronze")
            ...

Who: the folder of the running DAG's file (learners/<user>/…), so a learner's DAG gets
their own lakehouse. The login is a dedicated Polaris principal <user>_jobs holding the
learner's own principal-role (so it can do what they can); it's created on first use and its
secret kept in the Airflow Variable lab_jobs_<user>. (The learner's notebook login is
re-keyed on every Jupyter start, so jobs need their own.)
"""
import json
import os
import re
import secrets

import lakehouse as lh

SPARK_REMOTE = os.environ.get("SPARK_REMOTE", "sc://spark-connect:15002")
BUNDLE = "/learners/"


def _current_user():
    from airflow.sdk import get_current_context
    loc = str(getattr(get_current_context()["dag"], "fileloc", "") or "")
    if BUNDLE not in loc:
        raise RuntimeError("lab_spark() is for learner DAGs (saved in your bucket's dags/ folder)")
    return loc.split(BUNDLE, 1)[1].split("/", 1)[0]          # bucket prefix, e.g. ravi or ravi-k


def _works(cred):
    cid, secret = cred.split(":", 1)
    st, _ = lh._call("POST", "/api/catalog/v1/oauth/tokens", form={
        "grant_type": "client_credentials", "client_id": cid, "client_secret": secret,
        "scope": "PRINCIPAL_ROLE:ALL"})
    return st == 200


def _jobs_credential(name):
    """client_id:secret of <name>_jobs (created + bound to the learner's role on first use)."""
    from airflow.sdk import Variable
    key = f"lab_jobs_{name}"
    cred = Variable.get(key, default=None)
    if cred and _works(cred):
        return cred
    # first use — or the stored login no longer works (secret reset elsewhere): make a fresh one
    t, m, jobs = lh._admin_token(), "/api/management/v1", f"{name}_jobs"
    lh._ok(lh._call("POST", f"{m}/principals", {"principal": {"name": jobs}}, t)[0], "jobs principal")
    lh._ok(lh._call("PUT", f"{m}/principals/{jobs}/principal-roles", {"principalRole": {"name": name}}, t)[0],
           "jobs role")
    secret = secrets.token_urlsafe(24)
    st, _ = lh._call("POST", f"{m}/principals/{jobs}/reset", {"clientId": jobs, "clientSecret": secret}, t)
    if st != 200:
        raise RuntimeError(f"jobs login for {name}: HTTP {st}")
    cred = f"{jobs}:{secret}"
    Variable.set(key, cred)
    return cred


def _jobs_storage_key(name):
    """(key, secret) of the learner's own-bucket storage user <user>-jobs (kept in the Airflow
    Variable lab_jobs_s3_<name>; minted again if it stopped working)."""
    import urllib.error
    from airflow.sdk import Variable
    var = f"lab_jobs_s3_{name}"
    saved = Variable.get(var, default=None)
    if saved:
        key, secret = saved.split(":", 1)
        try:
            lh._s3_as(key, secret, "GET", lh.bucket_name(name), query={"list-type": "2", "max-keys": "1"}).close()
            return key, secret
        except urllib.error.HTTPError:
            pass
    key, secret = lh.storage_key(name, "jobs")
    Variable.set(var, f"{key}:{secret}")
    return key, secret


def lab_spark(user=None):
    """Spark Connect session: iceberg = <user>_lake (as <user>_jobs), shared = the course lake."""
    from pyspark.sql import SparkSession
    name = lh.lake_name(user or _current_user())
    if not re.fullmatch(r"[a-z0-9_]+", name):
        raise ValueError(f"bad learner name {name!r}")
    cred = _jobs_credential(name)
    spark = SparkSession.builder.remote(SPARK_REMOTE).getOrCreate()
    # same settings as the server's own `iceberg` catalog — only the warehouse + login differ
    base = "spark.sql.catalog.iceberg"
    conf = {k: v for k, v in spark.conf.getAll.items() if k == base or k.startswith(base + ".")}
    for cat, warehouse in (("iceberg", f"{name}_lake"), ("shared", lh.SHARED)):
        for k, v in conf.items():
            spark.conf.set(f"spark.sql.catalog.{cat}" + k[len(base):], v)
        spark.conf.set(f"spark.sql.catalog.{cat}.warehouse", warehouse)
        spark.conf.set(f"spark.sql.catalog.{cat}.credential", cred)
    # raw files (s3a://…): the cluster has no storage keys — use the learner's own (jobs) key
    key, secret = _jobs_storage_key(name)
    spark.conf.set("fs.s3a.access.key", key)
    spark.conf.set("fs.s3a.secret.key", secret)
    print(f"lab_spark: you are {name} · iceberg = {name}_lake · shared = {lh.SHARED}")
    return spark


def run_notebook(notebook, user=None):
    """Run one of the learner's notebooks (files/src/notebooks/<notebook> in their bucket) from a
    DAG task, the way Jupyter would: `spark` = lab_spark() (signed in as them) and `%%sql`.
    Cells run top to bottom; the task fails on the first cell that fails.

        @task
        def load():
            from lab_spark import run_notebook
            run_notebook("load_sample_orders.ipynb")
    """
    import json
    from IPython.core.interactiveshell import InteractiveShell

    who = user or _current_user()
    key = f"{lh.SRC}notebooks/{notebook}"
    with lh.get_object(lh.bucket_name(lh.lake_name(who)), key) as r:
        nb = json.loads(r.read())
    spark = lab_spark(who)
    shell = InteractiveShell.instance()
    shell.user_ns["spark"] = spark

    def sql(line, cell):                              # same %%sql as in the notebooks
        return spark.sql(cell).limit(1000).toPandas()
    shell.register_magic_function(sql, magic_kind="cell", magic_name="sql")

    cells = [c for c in nb.get("cells", []) if c.get("cell_type") == "code"]
    print(f"run_notebook: {key} — {len(cells)} code cells")
    for i, c in enumerate(cells, 1):
        src = "".join(c.get("source", []))
        result = shell.run_cell(src)
        err = result.error_in_exec or result.error_before_exec
        if err:
            raise RuntimeError(f"{notebook}: cell {i} failed: {err}")
        if result.result is not None:
            print(result.result)
        print(f"run_notebook: cell {i}/{len(cells)} ok")


DBT = os.environ.get("LAB_DBT", "/usr/local/bin/dbt")
DBT_GENERATED = ("target/", "logs/", "dbt_packages/")


def lab_dbt(command="build", project="shop", user=None):
    """Run a dbt command on the learner's dbt project from a DAG task, signed in AS THEM.

        @task
        def build():
            from lab_spark import lab_dbt
            lab_dbt("build")                       # = `dbt build` in ~/work/dbt/shop

    The project is their bucket's files/src/dbt/<project>/ (Jupyter's dbt/<project>/ folder,
    mirrored on save), copied to a fresh folder for this run. Their own profiles.yml is used:
    it reads LAKE_WAREHOUSE / LAKE_CREDENTIAL, set here to <user>_lake and the <user>_jobs login
    (same as lab_spark). packages.yml → `dbt deps` first. The task fails if dbt fails.
    """
    import shlex
    import subprocess
    import tempfile

    who = user or _current_user()
    name = lh.lake_name(who)
    if not re.fullmatch(r"[a-z0-9_]+", name) or not re.fullmatch(r"[A-Za-z0-9_-]+", project):
        raise ValueError(f"bad learner or project name {name!r} / {project!r}")
    bucket, prefix = lh.bucket_name(name), f"{lh.SRC}dbt/{project}/"
    with tempfile.TemporaryDirectory(prefix=f"dbt-{name}-") as tmp:
        n = 0
        for key in lh.list_all(bucket, prefix):
            rel = key[len(prefix):]
            if not rel or rel.endswith("/") or rel.startswith(DBT_GENERATED):
                continue
            path = os.path.join(tmp, rel)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with lh.get_object(bucket, key) as r, open(path, "wb") as f:
                f.write(r.read())
            n += 1
        if not os.path.exists(os.path.join(tmp, "dbt_project.yml")):
            raise RuntimeError(f"no dbt project in {bucket}/{prefix} (save it in Jupyter's dbt/{project}/ folder)")
        print(f"lab_dbt: you are {name} · project dbt/{project} ({n} files) · iceberg = {name}_lake")
        env = {**os.environ, "SPARK_REMOTE": SPARK_REMOTE, "LAKE_WAREHOUSE": f"{name}_lake",
               "LAKE_CREDENTIAL": _jobs_credential(name), "DBT_PROFILES_DIR": tmp,
               "DBT_SEND_ANONYMOUS_USAGE_STATS": "false", "DBT_USE_COLORS": "false"}
        steps = ([["deps"]] if os.path.exists(os.path.join(tmp, "packages.yml")) else []) + [shlex.split(command)]
        for args in steps:
            print(f"lab_dbt: $ dbt {' '.join(args)}", flush=True)
            proc = subprocess.Popen([DBT, *args], cwd=tmp, env=env, text=True,
                                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            for line in proc.stdout:
                print(line.rstrip(), flush=True)
            if proc.wait():
                raise RuntimeError(f"dbt {' '.join(args)} failed (exit {proc.returncode}) — see the log above")


def _vault_fields(name, user):
    import labvault
    name_ = lh.lake_name(user or _current_user())
    if not re.fullmatch(r"[a-z0-9_]+", name_):
        raise ValueError(f"bad learner name {name_!r}")
    d = labvault.fields(name_, name)
    if d is None:
        raise KeyError(f"no secret {name!r} for {name_} — add it on 🔑 My secrets")
    return d


def key_vault(name, field=None, user=None):
    """One of the learner's secrets ("My secrets", the key vault) in a DAG task — the same as
    key_vault() in a notebook:

        @task
        def fetch():
            from lab_spark import key_vault
            key = key_vault("WEATHER_API_KEY")
            pw = key_vault("db", "password")       # one field of a secret with several

    Whose: the running DAG's owner (as lab_spark), read with Airflow's vault login. Never logged."""
    d = _vault_fields(name, user)
    if field is not None:
        if field not in d:
            raise KeyError(f"secret {name!r} has no field {field!r} — its fields: {', '.join(sorted(d)) or '—'}")
        return d[field]
    if "value" in d:
        return d["value"]
    return next(iter(d.values())) if len(d) == 1 else json.dumps(d)


def key_vault_dict(name, user=None):
    """Every field of one of the learner's secrets as a dict (as key_vault_dict() in a notebook)."""
    return _vault_fields(name, user)


lab_secret = key_vault          # the first name (DAGs written before the rename keep working)
