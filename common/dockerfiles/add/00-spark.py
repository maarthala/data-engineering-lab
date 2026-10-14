# Auto-create a Databricks-style `spark` session in every notebook (IPython startup).
# Uses SPARK_REMOTE (sc://spark-connect:15002) set in the image, so no config needed.
# Wrapped so a down spark-connect doesn't break kernel startup.
from os import environ as _os_env

# Under JupyterHub every learner has their own lakehouse (LAKE_* set by the Hub's spawn
# hook): `iceberg` then means THEIR catalog with THEIR Polaris login, so lesson code
# (iceberg.bronze/silver/gold…) reads and writes the learner's own tables. Empty outside
# the Hub → the shared defaults from spark-defaults apply, as before.
_LAKE = {k: v for k, v in {
    "spark.sql.catalog.iceberg.warehouse": _os_env.get("LAKE_WAREHOUSE"),
    "spark.sql.catalog.iceberg.credential": _os_env.get("LAKE_CREDENTIAL"),
}.items() if v}
# Raw files (s3a://…): the cluster holds no storage keys — the session brings its own. Under
# JupyterHub that's the learner's own-bucket key; elsewhere whatever key this server was given.
_S3 = {k: v for k, v in {
    "fs.s3a.access.key": _os_env.get("LAKE_S3_KEY") or _os_env.get("AWS_ACCESS_KEY_ID"),
    "fs.s3a.secret.key": _os_env.get("LAKE_S3_SECRET") or _os_env.get("AWS_SECRET_ACCESS_KEY"),
}.items() if v}

try:
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    # point `iceberg` at the learner's own lakehouse BEFORE the catalog is first used
    for _k, _v in {**_LAKE, **_S3}.items():
        spark.conf.set(_k, _v)
    print(f"✓ `spark` ready via Spark Connect (Spark {spark.version})")
    if _LAKE:
        print(f"✓ you are {_os_env.get('LAKE_USER', '?')} · "
              f"iceberg = your lakehouse ({_LAKE['spark.sql.catalog.iceberg.warehouse']})")
except Exception as _e:  # noqa: BLE001
    print(f"⚠ `spark` not created ({_e}).")
    print("  Start it once spark-connect is up: spark = SparkSession.builder.getOrCreate()")


# --- My secrets: the learner's own secrets, in the lab's key vault (OpenBao) -----------------
# The Hub gives this server VAULT_ADDR + VAULT_TOKEN, a token that reaches only secret/<you>/….
# Values are fetched when asked for, never loaded into os.environ or printed.
def _vault(method, key="", body=None):
    import json, os, urllib.error, urllib.parse, urllib.request
    addr, token, me = os.environ.get("VAULT_ADDR"), os.environ.get("VAULT_TOKEN"), os.environ.get("LAKE_USER")
    if not (addr and token and me):
        raise RuntimeError("no key vault login on this server — restart it "
                           "(File → Hub Control Panel → Stop My Server → Start)")
    kind = "metadata" if method == "LIST" else "data"
    url = f"{addr}/v1/secret/{kind}/{me}/{urllib.parse.quote(key, safe='')}"
    if method == "LIST":
        method, url = "GET", url + "?list=true"
    data = json.dumps({"data": body}).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"X-Vault-Token": token, "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            raw = r.read()
            return json.loads(raw) if raw else None
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        if e.code == 403:
            raise PermissionError("the key vault refused your token (it lasts a week) — restart your "
                                  "server: File → Hub Control Panel → Stop My Server → Start") from None
        raise RuntimeError(f"key vault: HTTP {e.code}") from None


def lab_secret(name):
    """The value of one of your secrets from the lab's "My secrets" page (the key vault):
        key = lab_secret("WEATHER_API_KEY")
    Never printed; a missing name raises KeyError."""
    body = _vault("GET", name)
    if body is None:
        raise KeyError(f"no secret {name!r} — add it on 🔑 My secrets (lab_secrets() lists yours)")
    return body["data"]["data"]["value"]


def lab_secrets():
    """The names of your secrets (never the values)."""
    body = _vault("LIST")
    return sorted(k for k in (body or {}).get("data", {}).get("keys", []) if not k.endswith("/"))


def lab_secret_set(name, value, secret=True):
    """Save a secret from code (same as 🔑 My secrets → Save). secret=False: the page shows it."""
    import re, time
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,63}", name):
        raise ValueError("name: letters, digits and _ only, not starting with a digit (max 64)")
    _vault("POST", name, {"value": str(value), "secret": "1" if secret else "0",
                          "updated": time.strftime("%Y-%m-%d %H:%M", time.gmtime())})
    print(f"✓ secret {name} saved")


def lab_env(quiet=False):
    """Gone: "My variables" became 🔑 My secrets, read with lab_secret("NAME")."""
    print('My variables moved to 🔑 My secrets: read a value with lab_secret("NAME") — '
          f"yours: {', '.join(lab_secrets()) or '—'}")


# --- More catalogs: your own extra catalogs + ones other learners shared with you ------
def use_catalog(catalog, alias=None, quiet=False):
    """Make another Polaris catalog usable in Spark, with YOUR login (same server and
    settings as `iceberg`). Catalogs come from the lab's "My catalogs" page:
        use_catalog("kiran_sales")
        spark.sql("SELECT * FROM kiran_sales.sales.orders")
    Polaris decides what you may do in it (owner, or what was shared with you)."""
    alias = alias or catalog
    base = "spark.sql.catalog.iceberg"
    for k, v in spark.conf.getAll.items():
        if k == base or k.startswith(base + "."):
            spark.conf.set("spark.sql.catalog." + alias + k[len(base):], v)
    spark.conf.set(f"spark.sql.catalog.{alias}.warehouse", catalog)
    if not quiet:
        print(f"✓ catalog `{alias}` → Polaris catalog {catalog}")
    return alias


# The course's shared lake (read-only for learners) as `shared` — e.g. shared.gold.daily_sales —
# next to your own `iceberg`. Only under JupyterHub; elsewhere `iceberg` IS the shared lake.
if _LAKE:
    try:
        use_catalog(_os_env.get("SHARED_CATALOG", "polaris_lake"), "shared", quiet=True)
        print("✓ shared = the course's shared lake (read-only) — e.g. shared.gold.daily_sales")
    except Exception as _e:  # noqa: BLE001
        print(f"⚠ shared catalog not set ({_e})")


# --- Materialized views for `%%sql` ------------------------------------------------
# Spark only runs CREATE MATERIALIZED VIEW inside a Declarative Pipeline, and Trino
# can't create them on a REST catalog (Polaris). So `%%sql` turns these statements into
# a one-view pipeline run (python -m pyspark.pipelines.cli). The view is a normal
# Iceberg table in Polaris; its SQL is kept in the `mv.definition` table property so
# REFRESH can re-run it. Supported:
#   CREATE [OR REPLACE] MATERIALIZED VIEW [IF NOT EXISTS] cat.ns.view AS <query>
#   REFRESH MATERIALIZED VIEW cat.ns.view
#   DROP MATERIALIZED VIEW [IF EXISTS] cat.ns.view
#   SHOW MATERIALIZED VIEWS IN cat.ns
import json as _json
import os as _os
import re as _re

_MV_STORAGE = _os.environ.get("MV_PIPELINE_STORAGE", "s3a://demo-bucket/pipelines/mv")
_MV_NAME = r"([A-Za-z0-9_`]+\.[A-Za-z0-9_`]+\.[A-Za-z0-9_`]+)"
_MV_CREATE = _re.compile(
    r"CREATE\s+(OR\s+REPLACE\s+)?MATERIALIZED\s+VIEW\s+(IF\s+NOT\s+EXISTS\s+)?"
    + _MV_NAME + r"\s+AS\s+(.+?)\s*;?\s*$", _re.I | _re.S)
_MV_REFRESH = _re.compile(r"REFRESH\s+MATERIALIZED\s+VIEW\s+" + _MV_NAME + r"\s*;?\s*$", _re.I)
_MV_DROP = _re.compile(r"DROP\s+MATERIALIZED\s+VIEW\s+(IF\s+EXISTS\s+)?" + _MV_NAME + r"\s*;?\s*$", _re.I)
_MV_SHOW = _re.compile(r"SHOW\s+MATERIALIZED\s+VIEWS\s+IN\s+([A-Za-z0-9_`]+\.[A-Za-z0-9_`]+)\s*;?\s*$", _re.I)


def _mv_strip_comments(sql):
    return "\n".join(l for l in sql.splitlines() if not l.strip().startswith("--")).strip()


def _mv_definition(name):
    props = {r[0]: r[1] for r in spark.sql(f"SHOW TBLPROPERTIES {name}").collect()}
    return props.get("mv.definition")


def _mv_run(name, query):
    """Create/refresh one materialized view by running a single-view pipeline."""
    import subprocess, sys, tempfile, shutil
    catalog, ns, view = [p.strip("`") for p in name.split(".")]
    work = tempfile.mkdtemp(prefix="mv_")
    try:
        _os.makedirs(f"{work}/transformations")
        spec = {  # JSON is valid YAML
            "name": f"mv_{ns}_{view}", "catalog": catalog, "database": ns,
            "storage": f"{_MV_STORAGE}/{catalog}/{ns}/{view}",
            "libraries": [{"glob": {"include": "transformations/**"}}],
        }
        if _LAKE or _S3:   # the pipeline runs in its own session — give it the learner's lakehouse + key
            spec["configuration"] = {**_LAKE, **_S3}
        with open(f"{work}/spark-pipeline.yml", "w") as f:
            _json.dump(spec, f)
        with open(f"{work}/transformations/{view}.sql", "w") as f:
            f.write(f"CREATE MATERIALIZED VIEW {view} AS\n{query}\n")
        run = subprocess.run(
            [sys.executable, "-m", "pyspark.pipelines.cli", "run", "--spec", f"{work}/spark-pipeline.yml"],
            capture_output=True, text=True)
        if run.returncode != 0 or "Run is COMPLETED" not in run.stdout + run.stderr:
            tail = "\n".join((run.stdout + run.stderr).strip().splitlines()[-15:])
            raise RuntimeError(f"materialized view pipeline failed for {name}:\n{tail}")
    finally:
        shutil.rmtree(work, ignore_errors=True)
    # Keep the SQL on the table itself (survives refreshes) so REFRESH can re-run it.
    literal = query.replace("\\", "\\\\").replace("'", "\\'")
    spark.sql(f"ALTER TABLE {name} SET TBLPROPERTIES ('mv.definition' = '{literal}')")


def _mv_handle(sql):
    """Run a materialized-view statement, or return None if `sql` isn't one."""
    import pandas as pd
    sql = _mv_strip_comments(sql)
    if m := _MV_CREATE.match(sql):
        replace, if_not_exists, name, query = m.groups()
        if spark.catalog.tableExists(name):
            if if_not_exists:
                return pd.DataFrame([{"materialized_view": name, "action": "exists, skipped"}])
            if not replace and _mv_definition(name) is None:
                raise ValueError(f"{name} already exists and is not a materialized view")
            if not replace:
                raise ValueError(f"{name} already exists — use CREATE OR REPLACE or REFRESH")
        _mv_run(name, query)
        action = "replaced" if replace else "created"
    elif m := _MV_REFRESH.match(sql):
        name = m.group(1)
        query = _mv_definition(name) if spark.catalog.tableExists(name) else None
        if query is None:
            raise ValueError(f"{name} is not a materialized view created with %%sql")
        _mv_run(name, query)
        action = "refreshed"
    elif m := _MV_DROP.match(sql):
        if_exists, name = m.groups()
        if not spark.catalog.tableExists(name):
            if if_exists:
                return pd.DataFrame([{"materialized_view": name, "action": "not found, skipped"}])
            raise ValueError(f"{name} does not exist")
        if _mv_definition(name) is None:
            raise ValueError(f"{name} is a table, not a materialized view — use DROP TABLE")
        spark.sql(f"DROP TABLE {name}")
        return pd.DataFrame([{"materialized_view": name, "action": "dropped"}])
    elif m := _MV_SHOW.match(sql):
        ns = m.group(1)
        rows = []
        for t in spark.sql(f"SHOW TABLES IN {ns}").collect():
            name = f"{ns}.{t.tableName}"
            if (d := _mv_definition(name)) is not None:
                rows.append({"materialized_view": name, "definition": d})
        return pd.DataFrame(rows, columns=["materialized_view", "definition"])
    elif _re.match(r"(CREATE|REFRESH|DROP)\s+.*?MATERIALIZED\s+VIEW", sql, _re.I | _re.S):
        raise ValueError("use a fully-qualified name: catalog.namespace.view "
                         "(e.g. iceberg.gold.revenue_by_region)")
    else:
        return None
    rows = spark.table(name).count()
    return pd.DataFrame([{"materialized_view": name, "action": action, "rows": rows}])


# Databricks-style `%%sql` cell magic — runs Spark SQL over any catalog table or view.
try:
    from IPython.core.magic import register_cell_magic

    def _sql_vars(cell):
        """${name} → a notebook variable, else an environment variable (like Databricks'
        ${param}): e.g. s3a://${LAKE_BUCKET}/raw/… — your own bucket. Unknown names stay."""
        ns = get_ipython().user_ns  # noqa: F821
        return _re.sub(r"\$\{(\w+)\}", lambda m: str(ns.get(m[1], _os.environ.get(m[1], m[0]))), cell)

    @register_cell_magic
    def sql(line, cell):  # usage:  %%sql \n SELECT ... FROM iceberg.gold.tbl
        cell = _sql_vars(cell)
        mv = _mv_handle(cell)
        if mv is not None:
            return mv
        # Return a pandas DataFrame so the notebook renders a real table (not just
        # the Spark schema repr). Capped at 1000 rows for display, like Databricks.
        return spark.sql(cell).limit(1000).toPandas()
except Exception:  # not in IPython, or spark missing
    pass
