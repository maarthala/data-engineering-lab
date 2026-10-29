"""lab_spark() — a Spark session for a learner's DAG, signed in AS THAT LEARNER (lab_dbt(): their dbt project;
key_vault(): one of their secrets).

Learner DAGs (synced from <user>-lake/dags/ by the "learners" bundle) run in the shared
Airflow, which has no learner credentials. This helper gives a task the same Spark a notebook
has: `my` = the learner's own lakehouse (<user>_lake), `shared` = the course's shared lake, and
their team's lakehouse by its name, team_<team>.

    from airflow.sdk import dag, task

    @dag(dag_id="ravi_medallion", schedule=None, start_date=...)
    def ravi_medallion():
        @task
        def bronze():
            from lab_spark import lab_spark
            spark = lab_spark()                     # signs in as ravi
            spark.sql("CREATE NAMESPACE IF NOT EXISTS my.bronze")
            ...

Who: the folder of the running DAG's file (learners/<user>/…), so a learner's DAG gets
their own lakehouse — always its owner (user= may only name the owner). A TEAM's DAG
(learners/team-<team>/…, from the team bucket) runs as the team (login team_<team>_jobs), so it
keeps working whoever is in the team: it has team_<team> and `shared`, and no `my`. The logins
(Polaris <user>_jobs holding the learner's own principal-role, storage key <user>-jobs, a vault
token for their secrets) come from lab_jobs.py: on k8s the scheduler puts them into the task's
own pod (LAB_JOB_* env) and the task never sees an admin login; on the local stack the task
asks lab_jobs itself.
"""
import json
import os
import re

import lakehouse as lh

SPARK_REMOTE = os.environ.get("SPARK_REMOTE", "sc://spark-connect:15002")
BUNDLE = "/learners/"


def _current_user():
    from airflow.sdk import get_current_context
    loc = str(getattr(get_current_context()["dag"], "fileloc", "") or "")
    if BUNDLE not in loc:
        raise RuntimeError("lab_spark() is for learner DAGs (saved in your bucket's dags/ folder)")
    return loc.split(BUNDLE, 1)[1].split("/", 1)[0]          # bucket prefix, e.g. ravi or ravi-k


def _job(user=None):
    """The logins this task runs with (lab_jobs.job_env): the pod's own env on k8s, else asked
    for here (local stack). Always the DAG's owner."""
    owner = os.environ.get("LAB_JOB_OWNER") or _current_user()
    if user and user != owner:
        raise PermissionError(f"this DAG runs as {owner}, not {user!r}")
    if os.environ.get("LAB_JOB_CREDENTIAL"):
        return {k: v for k, v in os.environ.items() if k.startswith("LAB_JOB_")}
    import lab_jobs
    return lab_jobs.job_env(owner)


def _whose(user=None):
    """(name, own catalog, team) the running task works as: a member (ravi, ravi_lake, their
    team) or — for a team's DAG — the team (team_sales, team_sales, sales)."""
    j = _job(user)
    return j["LAB_JOB_NAME"], j["LAB_JOB_CATALOG"], j["LAB_JOB_TEAM"] or None


def lab_spark(user=None):
    """Spark Connect session as <user>_jobs: my = <user>_lake, shared = the course lake, and
    their team's lakehouse team_<team> — each also by its real name (as in SQLPad): <user>_lake,
    polaris_lake. (`iceberg`, the default catalog, is my too — the older name.) In a team's DAG:
    team_<team> and shared (iceberg = team_<team>)."""
    from pyspark.sql import SparkSession
    j = _job(user)
    name, own, team = j["LAB_JOB_NAME"], j["LAB_JOB_CATALOG"], j["LAB_JOB_TEAM"] or None
    is_team = own == name
    cred = j["LAB_JOB_CREDENTIAL"]
    spark = SparkSession.builder.remote(SPARK_REMOTE).getOrCreate()
    # same settings as the server's own `iceberg` catalog — only the warehouse + login differ
    base = "spark.sql.catalog.iceberg"
    conf = {k: v for k, v in spark.conf.getAll.items() if k == base or k.startswith(base + ".")}
    cats = [("iceberg", own), ("shared", lh.SHARED), (lh.SHARED, lh.SHARED)]
    if not is_team:
        cats += [("my", own), (own, own)]
    if team:
        cats += [(lh.team_catalog(team), lh.team_catalog(team))]
    for cat, warehouse in cats:
        for k, v in conf.items():
            spark.conf.set(f"spark.sql.catalog.{cat}" + k[len(base):], v)
        spark.conf.set(f"spark.sql.catalog.{cat}.warehouse", warehouse)
        spark.conf.set(f"spark.sql.catalog.{cat}.credential", cred)
    # raw files (s3a://…): the cluster has no storage keys — use the learner's own (jobs) key
    spark.conf.set("fs.s3a.access.key", j["LAB_JOB_S3_KEY"])
    spark.conf.set("fs.s3a.secret.key", j["LAB_JOB_S3_SECRET"])
    print(f"lab_spark: " + (f"team {team} · {own}" if is_team else
          f"you are {name} · my = {own}" + (f" · your team: {lh.team_catalog(team)}" if team else ""))
          + f" · shared = {lh.SHARED}")
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

    who = _job(user)["LAB_JOB_OWNER"]
    key = f"{lh.SRC}notebooks/{notebook}"
    with lh.get_object(lh.bucket_name(lh.lake_name(who)), key) as r:      # (a team's DAG: the team bucket)
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
            lab_dbt("build")                       # = `dbt build` in ~/work/my_files/dbt/shop

    The project is their bucket's files/src/dbt/<project>/ (Jupyter's dbt/<project>/ folder,
    mirrored on save), copied to a fresh folder for this run. Their own profiles.yml is used:
    it reads LAKE_WAREHOUSE / LAKE_CREDENTIAL, set here to <user>_lake and the <user>_jobs login
    (same as lab_spark). packages.yml → `dbt deps` first. The task fails if dbt fails.
    """
    import shlex
    import subprocess
    import tempfile

    j = _job(user)
    name, own, team = j["LAB_JOB_NAME"], j["LAB_JOB_CATALOG"], j["LAB_JOB_TEAM"] or None
    if not re.fullmatch(r"[A-Za-z0-9_-]+", project):
        raise ValueError(f"bad project name {project!r}")
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
        print(f"lab_dbt: {'team ' + team if own == name else 'you are ' + name} · project dbt/{project}"
              f" ({n} files) · lakehouse {own}")
        env = {**os.environ, "SPARK_REMOTE": SPARK_REMOTE, "LAKE_WAREHOUSE": own,
               "LAKE_CREDENTIAL": j["LAB_JOB_CREDENTIAL"], "DBT_PROFILES_DIR": tmp,
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


def _vault_fields(name, user, team=False):
    import labvault
    name_, own, t = _whose(user)
    if own == name_:                               # a team's DAG: always the team's folder
        team = team or True
    if team:                                       # team="sales" (or True: the owner's team)
        if not t:
            raise KeyError(f"{name_} is not in a team")
        if team is not True and team != t:
            raise PermissionError(f"{'this DAG belongs to' if own == name_ else name_ + ' is in'} the team {t}, "
                                  f"not {team!r}")
        folder = labvault.team_folder(t)
    else:
        folder = name_
    d = labvault.fields(folder, name)
    if d is None and not team:                     # not theirs — is it their team's?
        if t and labvault.fields(labvault.team_folder(t), name) is not None:
            raise KeyError(f"no secret {name!r} in {name_}'s folder — the team {t} has one: "
                           f"key_vault({name!r}, team={t!r})")
    if d is None:
        raise KeyError(f"no secret {name!r} in {folder} — add it on 🔑 My secrets{' → Team' if team else ''}")
    return d


def key_vault(name, field=None, user=None, team=False):
    """One of the learner's secrets ("My secrets", the key vault) in a DAG task — the same as
    key_vault() in a notebook:

        @task
        def fetch():
            from lab_spark import key_vault
            key = key_vault("WEATHER_API_KEY")
            pw = key_vault("db", "password")       # one field of a secret with several
            tok = key_vault("API_TOKEN", team="sales")  # one of the owner's team's secrets

    Whose: the running DAG's owner (as lab_spark), read with the task's own vault token. Never logged.
    In a team's DAG: always the team's secrets."""
    d = _vault_fields(name, user, team)
    if field is not None:
        if field not in d:
            raise KeyError(f"secret {name!r} has no field {field!r} — its fields: {', '.join(sorted(d)) or '—'}")
        return d[field]
    if "value" in d:
        return d["value"]
    return next(iter(d.values())) if len(d) == 1 else json.dumps(d)


def key_vault_dict(name, user=None, team=False):
    """Every field of one of the learner's (team="<team>": their team's) secrets as a dict."""
    return _vault_fields(name, user, team)


lab_secret = key_vault          # the first name (DAGs written before the rename keep working)
