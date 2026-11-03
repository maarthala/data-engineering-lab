"""cdc_debezium — "📡 Ingestion" with Debezium (CDC_ENGINE=debezium): the same sources, destinations
and jobs as with OLake (labcdc.py), the work done by Kafka Connect instead of a container per run.

A job is two connectors in kafka-connect, made by cdc-runner:
  cdc-<job>-src   Debezium's Postgres source: a full load (snapshot), then every change read from the
                  source's replication slot → topics <member>.j<job>.<schema>.<table>
  cdc-<job>-sink  Tabular's Iceberg sink: the topics → the member's lakehouse, a commit every 10 s,
                  signed in as the member's job login (lab_jobs) — upserts one row per key (cdc), or
                  appends every change (append: the history)
Schedules:
  stream          both connectors always run — the tables are ~10–30 s behind the source
  manual / 15 min / hourly / daily
                  the connectors stay paused; a run resumes them, waits until the slot has read up
                  to where the source was when the run began and the sink has committed it all,
                  then pauses them again (the change stream waits in the slot meanwhile)
Passwords and keys never sit in a connector's config (Kafka keeps that in a topic): it names files
(${dir:/run/lab-secrets/cdc-<job>:<name>}) written here, on a volume only kafka-connect reads —
on k8s through the kafka-connect pod's helper container (connect_secrets.py, CDC_SECRETS_URL).
reconcile() keeps Kafka Connect in step with the jobs every few seconds: makes missing connectors,
pauses / resumes them, restarts failed tasks, and removes what deleted jobs left (connectors,
topics, secret files).
"""
import json
import os
import re
import shutil
import time
import urllib.error
import urllib.parse
import urllib.request

import labcdc
from labcdc import _q

CONNECT = os.environ.get("CONNECT_URL", "http://kafka-connect:8083").rstrip("/")
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
SECRETS = os.environ.get("CDC_SECRETS", "/run/lab-secrets")
POLARIS = os.environ.get("POLARIS_URL", "http://polaris:8181").rstrip("/")
S3_ENDPOINT = os.environ.get("LAKE_S3_ENDPOINT", "http://storage:9000")
COMMIT_MS = int(os.environ.get("CDC_COMMIT_MS", "10000"))     # the sink's Iceberg commit interval
TIMEOUT = int(os.environ.get("CDC_TIMEOUT_S", "1800"))        # a run longer than this is stopped
IDLE_S = 30                                                   # a quiet source: caught up after this long without news
CONN = re.compile(r"^cdc-(\d+)-(src|sink)$")
CONNECT_CONTAINER = os.environ.get("CONNECT_CONTAINER", "kafka-connect")   # local: its log, through the Docker socket
SECRETS_URL = os.environ.get("CDC_SECRETS_URL", "").rstrip("/")           # k8s: the pod's helper
SECRETS_TOKEN = os.environ.get("CDC_SECRETS_TOKEN", "")
EVENTS = 60                                                   # a stream's log keeps its last events
NOTE_S = 300                                                  # … and says how it's doing every 5 minutes


class DbzError(RuntimeError):
    pass


class Off(DbzError):
    """The Streaming unit (Kafka, Kafka Connect) isn't running."""


class SetupError(DbzError):
    """The job can't start as it is (e.g. its tables belong to something else): it stops, and says why."""


# ---- Kafka Connect's REST API ----------------------------------------------------------------
def _rest(method, path, body=None, ok404=False):
    req = urllib.request.Request(CONNECT + path, method=method, data=json.dumps(body).encode() if body is not None else None,
                                 headers={"Content-Type": "application/json", "Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            raw = r.read()
    except urllib.error.HTTPError as e:
        if ok404 and e.code == 404:
            return None
        msg = e.read()[:300].decode("utf-8", "replace")
        raise DbzError(f"Kafka Connect {method} {path.split('?')[0]}: HTTP {e.code} {msg}") from None
    except (urllib.error.URLError, OSError) as e:
        raise Off(f"Kafka Connect isn't running ({getattr(e, 'reason', e)}) — the Streaming unit is off: "
                  f"a lab manager switches it on ({labcdc.STREAMING_HOW})") from None
    return json.loads(raw) if raw else None


def _connectors():
    """{name: {"state": RUNNING|PAUSED|FAILED|…, "tasks": [{state, trace}]}} — the lab's (cdc-<job>-…)."""
    out = {}
    for name, x in (_rest("GET", "/connectors?expand=status") or {}).items():
        if CONN.match(name):
            st = x.get("status") or {}
            out[name] = {"state": (st.get("connector") or {}).get("state"), "tasks": st.get("tasks") or []}
    return out


# ---- Kafka Connect's log: the lines about one job's connectors -------------------------------
# Kafka's own client chatter (group joins, versions, config echoes) — not about the job's data
NOISE = re.compile(r"\[(Consumer|Producer|AdminClient) clientId=|Kafka (version|commitId|startTimeMs)|These configurations|"
                   r"brand-new (de)?compressor|GET /connectors|ConsumerConfig values|ProducerConfig values|AdminClientConfig values|"
                   r"^\s*[a-z0-9_.]+ = ")
def connect_log(jid, since=None, lines=40):
    """The last `lines` lines kafka-connect logged about job `jid`'s connectors (since a unix time),
    without Java stack frames — read through the Docker socket (local); [] where there's none."""
    import http.client
    import socket

    class _Sock(http.client.HTTPConnection):
        def connect(self):
            self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self.sock.settimeout(20)
            self.sock.connect("/var/run/docker.sock")
    if not os.path.exists("/var/run/docker.sock"):
        return _pod_log(jid, since, lines)
    try:
        c = _Sock("docker", timeout=20)
        q = "stdout=1&stderr=1&tail=4000" + (f"&since={int(since)}" if since else "")
        c.request("GET", f"/v1.43/containers/{CONNECT_CONTAINER}/logs?{q}")
        r = c.getresponse()
        raw = r.read()
        c.close()
        if r.status >= 400:
            return []
    except OSError:
        return []
    out, i = [], 0
    while i + 8 <= len(raw):                                   # the stream's 8-byte frame headers
        n = int.from_bytes(raw[i + 4:i + 8], "big")
        out.append(raw[i + 8:i + 8 + n])
        i += 8 + n
    return _keep(b"".join(out).decode("utf-8", "replace"), jid, lines)


def _keep(text, jid, lines):
    tag = re.compile(rf"\bcdc-{jid}-(src|sink)\b")
    keep = [l for l in text.splitlines() if tag.search(l) and not l.lstrip().startswith("at ") and not NOISE.search(l)]
    return [re.sub(r"\s*\([a-zA-Z0-9_.$]+:\d+\)\s*$", "", l)[:400] for l in keep[-lines:]]


def _pod_log(jid, since, lines):
    """k8s: the same lines from the kafka-connect pod's log (the runner may read pod logs)."""
    sa = "/var/run/secrets/kubernetes.io/serviceaccount"
    if not os.path.exists(f"{sa}/token"):
        return []
    import ssl
    try:
        ns, token = open(f"{sa}/namespace").read().strip(), open(f"{sa}/token").read().strip()
        ctx = ssl.create_default_context(cafile=f"{sa}/ca.crt")

        def get(path):
            req = urllib.request.Request("https://kubernetes.default.svc" + path, headers={"Authorization": f"Bearer {token}"})
            with urllib.request.urlopen(req, context=ctx, timeout=20) as r:
                return r.read()
        pods = json.loads(get(f"/api/v1/namespaces/{ns}/pods?labelSelector=" +
                              urllib.parse.quote("app.kubernetes.io/name=kafka-connect")))["items"]
        pod = next((p["metadata"]["name"] for p in pods if p["status"].get("phase") == "Running"), None)
        if not pod:
            return []
        q = "container=kafka-connect&tailLines=4000" + (f"&sinceSeconds={max(1, int(time.time() - since))}" if since else "")
        return _keep(get(f"/api/v1/namespaces/{ns}/pods/{pod}/log?{q}").decode("utf-8", "replace"), jid, lines)
    except Exception:  # noqa: BLE001 — the log is a help, not the job
        return []


# ---- names ----------------------------------------------------------------------------------
def prefix(j):
    return re.sub(r"[^A-Za-z0-9_-]", "_", j["member"]) + f".j{j['id']}"


def _src(jid):
    return f"cdc-{jid}-src"


def _sink(jid):
    return f"cdc-{jid}-sink"


def _secret_dir(jid):
    return f"{SECRETS}/cdc-{jid}"


def _secret(jid, key):
    return "${dir:" + _secret_dir(jid) + ":" + key + "}"


# ---- Postgres --------------------------------------------------------------------------------
def _pg(s, password=None):
    import psycopg2
    return psycopg2.connect(host=s["host"], port=s["port"], dbname=s["dbname"], user=s["username"],
                            password=password if password is not None else labcdc.password(s["member"], s["id"]),
                            connect_timeout=10)


def _first_line(e):
    return str(e).strip().splitlines()[0] if str(e).strip() else type(e).__name__


def _postgres_only(s):
    if s["kind"] != "postgres":
        raise DbzError(f"{labcdc.KINDS[s['kind']]['name']} isn't ready with Debezium yet — PostgreSQL only")


def check_source(s):
    """The connection + what Debezium needs: wal_level = logical, a login with REPLICATION, the
    publication (made here if missing). The replication slot is Debezium's to make (its first run)."""
    _postgres_only(s)
    notes, pre, ok, msg = [], [], True, "connected"
    try:
        c = _pg(s)
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "message": f"can't connect: {_first_line(e)}", "notes": [], "prerequisites": [], "at": _now()}
    c.autocommit = True
    with c, c.cursor() as cur:
        if s["cdc"]:
            cur.execute("SHOW wal_level")
            wal = cur.fetchone()[0]
            cur.execute("SELECT rolreplication OR rolsuper FROM pg_roles WHERE rolname = current_user")
            rep = bool(cur.fetchone()[0])
            pre = [{"name": "wal_level", "passed": wal == "logical", "required": True, "current": wal, "recommended": "logical"},
                   {"name": "login with REPLICATION", "passed": rep, "required": True, "current": str(rep).lower(),
                    "recommended": "true"}]
            if wal != "logical":
                ok, msg = False, "wal_level is not 'logical' — set it in postgresql.conf and restart Postgres"
            elif not rep:
                ok, msg = False, f"the login {s['username']} needs REPLICATION — ALTER ROLE {s['username']} REPLICATION;"
            else:
                cur.execute("SELECT 1 FROM pg_publication WHERE pubname = %s", (s["publication"],))
                if not cur.fetchone():
                    try:
                        cur.execute(f'CREATE PUBLICATION "{s["publication"]}" ' + (
                            "FOR TABLES IN SCHEMA " + ", ".join(f'"{x}"' for x in s["schemas"]) if s["schemas"] else "FOR ALL TABLES"))
                        notes.append(f"made publication {s['publication']}")
                    except Exception as e:  # noqa: BLE001
                        ok, msg = False, f"couldn't make publication {s['publication']}: {_first_line(e)}"
                        notes.append(f"run: CREATE PUBLICATION {s['publication']} FOR ALL TABLES;")
    c.close()
    return {"ok": ok, "message": msg, "notes": notes, "prerequisites": pre, "at": _now()}


def test_source(f):
    _postgres_only(f)
    try:
        _pg(f, f["password"]).close()
        return {"ok": True, "message": "connected"}
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "message": f"can't connect: {_first_line(e)}"}


def discover(s, password=None):
    """The source's tables (in its schemas, if it has any): columns, primary key — the page lists them;
    `streams` is the same shape OLake's discover gives, so a job is made the same way."""
    _postgres_only(s)
    try:
        c = _pg(s, password)
    except Exception as e:  # noqa: BLE001
        raise DbzError(f"can't connect: {_first_line(e)}") from None
    with c, c.cursor() as cur:
        where, args = "", ()
        if s.get("schemas"):
            where, args = " AND t.table_schema = ANY(%s)", (list(s["schemas"]),)
        cur.execute("SELECT t.table_schema, t.table_name, (SELECT count(*) FROM information_schema.columns c "
                    " WHERE c.table_schema = t.table_schema AND c.table_name = t.table_name) FROM information_schema.tables t "
                    "WHERE t.table_type = 'BASE TABLE' AND t.table_schema NOT IN ('pg_catalog', 'information_schema')"
                    + where + " ORDER BY 1, 2", args)
        tables = cur.fetchall()
        # pg_catalog: information_schema hides constraints from a login that may only read
        cur.execute("SELECT n.nspname, c.relname, a.attname FROM pg_index i JOIN pg_class c ON c.oid = i.indrelid "
                    "JOIN pg_namespace n ON n.oid = c.relnamespace "
                    "JOIN LATERAL unnest(i.indkey) WITH ORDINALITY k(attnum, ord) ON true "
                    "JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = k.attnum "
                    "WHERE i.indisprimary ORDER BY n.nspname, c.relname, k.ord")
        pks = {}
        for sch, tb, col in cur.fetchall():
            pks.setdefault((sch, tb), []).append(col)
    c.close()
    streams = [{"stream": {"namespace": sch, "name": tb, "source_defined_primary_key": pks.get((sch, tb), []),
                           "supported_sync_modes": ["cdc"] + (["full_refresh"] if pks.get((sch, tb)) else [])}}
               for sch, tb, _ in tables]
    return {"tables": [{"table": f"{sch}.{tb}", "columns": n, "modes": ["cdc"], "key": pks.get((sch, tb), [])}
                       for sch, tb, n in tables],
            "streams": {"streams": streams, "selected_streams": {}}}


# ---- the destination: the member's catalog, as their job login -------------------------------
def _job_env(member):
    import lab_jobs
    return lab_jobs.job_env(re.sub(r"_", "-", member))


def _polaris(member, method, path, body=None, token=None):
    """Polaris's Iceberg REST API as the member's job login → (status, json)."""
    if token is None:
        cid, _, sec = _job_env(member)["LAB_JOB_CREDENTIAL"].partition(":")
        form = urllib.parse.urlencode({"grant_type": "client_credentials", "client_id": cid, "client_secret": sec,
                                       "scope": "PRINCIPAL_ROLE:ALL"}).encode()
        with urllib.request.urlopen(urllib.request.Request(f"{POLARIS}/api/catalog/v1/oauth/tokens", data=form), timeout=20) as r:
            token = json.loads(r.read())["access_token"]
    req = urllib.request.Request(f"{POLARIS}/api/catalog/v1/{path}", method=method,
                                 data=json.dumps(body).encode() if body is not None else None,
                                 headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            raw = r.read()
            return r.status, (json.loads(raw) if raw else None), token
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            return e.code, json.loads(raw), token
        except ValueError:
            return e.code, {"error": {"message": raw[:200].decode("utf-8", "replace")}}, token


def check_destination(member, d):
    try:
        st, body, _ = _polaris(member, "GET", f"config?warehouse={urllib.parse.quote(d['catalog'])}")
        if st != 200:
            raise DbzError(f"HTTP {st} {(body.get('error') or {}).get('message', '')}")
        return {"ok": True, "message": f"signed in to {d['catalog']} as your job login", "at": _now()}
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "message": f"{d['catalog']}: {_first_line(e)}", "at": _now()}


def _ice_type(pg):
    """A Postgres column type (format_type) → the Iceberg type the sink converts the change into."""
    t = pg.lower()
    m = re.fullmatch(r"numeric\((\d+),(\d+)\)", t)
    if m and int(m.group(1)) <= 38:
        return f"decimal({m.group(1)},{m.group(2)})"
    if t.startswith("timestamp"):
        return "timestamptz" if t.endswith("with time zone") else "timestamp"
    if t.startswith("time"):
        return "time"
    return {"smallint": "int", "integer": "int", "bigint": "long", "real": "float", "double precision": "double",
            "boolean": "boolean", "date": "date", "bytea": "binary"}.get(t, "string")


def create_tables(j, s, d):
    """Each table made before the sink first writes, typed from the source (the sink alone would
    guess from the change's JSON: a timestamptz arrives as text) and with its key (upserts need it)."""
    c = _pg(s)
    with c, c.cursor() as cur:
        cols = {}
        for sch, tb, key in _tables(j):
            cur.execute("SELECT a.attname, format_type(a.atttypid, a.atttypmod), a.attnotnull FROM pg_attribute a "
                        "WHERE a.attrelid = (quote_ident(%s) || '.' || quote_ident(%s))::regclass AND a.attnum > 0 "
                        "AND NOT a.attisdropped ORDER BY a.attnum", (sch, tb))
            cols[(sch, tb)] = (cur.fetchall(), key)
    c.close()
    st, cfg, token = _polaris(j["member"], "GET", f"config?warehouse={urllib.parse.quote(d['catalog'])}")
    pre = urllib.parse.quote(((cfg or {}).get("overrides") or {}).get("prefix") or (cfg or {}).get("defaults", {}).get("prefix") or d["catalog"])
    todo = []
    for (sch, tb), (rows, key) in cols.items():          # first: a table there already must be a Debezium job's
        ns = _namespace(s, d, sch)
        st, body, _ = _polaris(j["member"], "GET", f"{pre}/namespaces/{ns}/tables/{tb}", token=token)
        if st != 200:
            todo.append((ns, tb, rows, key))
        elif ((body.get("metadata") or {}).get("properties") or {}).get("lab.cdc") != "debezium":
            raise SetupError(f"{d['catalog']}.{ns}.{tb} is there already, written by something else (another tool's columns) — "
                             f"choose a destination with another namespace, or delete that table in My catalogs")
    for ns, tb, rows, key in todo:
        fields = [{"id": i + 1, "name": n, "required": n in key or bool(nn), "type": _ice_type(t)}
                  for i, (n, t, nn) in enumerate(rows)]
        schema = {"type": "struct", "schema-id": 0, "fields": fields,
                  "identifier-field-ids": [f["id"] for f in fields if f["name"] in key] if j["mode"] == "cdc" else []}
        st, body, _ = _polaris(j["member"], "POST", f"{pre}/namespaces/{ns}/tables", token=token,
                               body={"name": tb, "schema": schema, "properties": {"format-version": "2", "lab.cdc": "debezium"}})
        if st not in (200, 409):
            raise DbzError(f"couldn't make {d['catalog']}.{ns}.{tb}: HTTP {st} {(body.get('error') or {}).get('message', '')}")


def _now():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


# ---- a job's connectors ---------------------------------------------------------------------
def _tables(j):
    """[(schema, table, [key columns])] of the job."""
    keep = set(j["tables"])
    out = []
    for x in (j["streams"] or {}).get("streams", []):
        st = x["stream"]
        if f"{st['namespace']}.{st['name']}" in keep:
            out.append((st["namespace"], st["name"], list(st.get("source_defined_primary_key") or [])))
    return out


def _namespace(s, d, schema):
    """Where a table lands: the destination's namespace, else <database>_<schema> (as with OLake)."""
    return d["namespace"] or re.sub(r"[^a-z0-9_]", "_", f"{s['dbname']}_{schema}".lower())


def _helper(method, path, data=None):
    """k8s: the kafka-connect pod's secrets helper (connect_secrets.py)."""
    req = urllib.request.Request(SECRETS_URL + path, method=method, data=data, headers={"X-Token": SECRETS_TOKEN})
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            raw = r.read()
            return json.loads(raw) if raw else None
    except (urllib.error.URLError, OSError) as e:
        raise Off(f"Kafka Connect's secrets helper isn't reachable ({getattr(e, 'reason', e)}) — the Streaming unit is off: "
                  f"a lab manager switches it on ({labcdc.STREAMING_HOW})") from None


def _secret_dirs():
    if SECRETS_URL:
        return _helper("GET", "/") or []
    return [n for n in os.listdir(SECRETS)] if os.path.isdir(SECRETS) else []


def _drop_secrets(jid):
    if SECRETS_URL:
        try:
            _helper("DELETE", f"/cdc-{jid}")
        except Off:
            pass
        return
    shutil.rmtree(_secret_dir(jid), ignore_errors=True)


def write_secrets(j, s):
    env = _job_env(j["member"])
    values = {"source-password": labcdc.password(s["member"], s["id"]), "catalog-credential": env["LAB_JOB_CREDENTIAL"],
              "s3-key": env["LAB_JOB_S3_KEY"], "s3-secret": env["LAB_JOB_S3_SECRET"]}
    if SECRETS_URL:
        for key, value in values.items():
            _helper("PUT", f"/cdc-{j['id']}/{key}", value.encode())
        return
    path = _secret_dir(j["id"])
    os.makedirs(path, exist_ok=True)
    for key, value in values.items():
        tmp = f"{path}/.{key}"
        with open(tmp, "w") as f:
            f.write(value)
        os.chmod(tmp, 0o644)                          # kafka-connect runs as another user; only it mounts the volume
        os.replace(tmp, f"{path}/{key}")


def source_config(j, s):
    tables = _tables(j)
    return {"connector.class": "io.debezium.connector.postgresql.PostgresConnector", "tasks.max": "1",
            "database.hostname": s["host"], "database.port": str(s["port"]), "database.dbname": s["dbname"],
            "database.user": s["username"], "database.password": _secret(j["id"], "source-password"),
            "database.sslmode": "disable",
            "topic.prefix": prefix(j), "plugin.name": "pgoutput",
            "slot.name": s["slot"], "publication.name": s["publication"], "publication.autocreate.mode": "disabled",
            "table.include.list": ",".join(re.escape(f"{sch}.{tb}") for sch, tb, _ in tables),
            "snapshot.mode": "initial", "heartbeat.interval.ms": "10000", "tombstones.on.delete": "false",
            "time.precision.mode": "connect", "decimal.handling.mode": "precise", "slot.drop.on.stop": "false"}


def _job_topics(j):
    return [f"{prefix(j)}.{sch}.{tb}" for sch, tb, _ in _tables(j)]


def _make_topics(j):
    """The job's topics before its connectors start: a sink subscribed to a topic not there yet would
    only see it at its next metadata refresh (minutes)."""
    from confluent_kafka.admin import NewTopic
    have = set(_admin().list_topics(timeout=10).topics)
    new = [NewTopic(t, num_partitions=1, replication_factor=1) for t in _job_topics(j) if t not in have]
    for f in (_admin().create_topics(new).values() if new else []):
        try:
            f.result(timeout=30)
        except Exception as e:  # noqa: BLE001 — made meanwhile
            if "TOPIC_ALREADY_EXISTS" not in str(e):
                raise


def sink_config(j, s, d):
    tables = _tables(j)
    ns = d["namespace"]
    cfg = {"connector.class": "io.tabular.iceberg.connect.IcebergSinkConnector", "tasks.max": "1",
           "topics": ",".join(_job_topics(j)),
           "iceberg.catalog.type": "rest", "iceberg.catalog.uri": f"{POLARIS}/api/catalog",
           "iceberg.catalog.warehouse": d["catalog"],
           "iceberg.catalog.credential": _secret(j["id"], "catalog-credential"),
           "iceberg.catalog.scope": "PRINCIPAL_ROLE:ALL",
           "iceberg.catalog.oauth2-server-uri": f"{POLARIS}/api/catalog/v1/oauth/tokens",
           "iceberg.catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
           "iceberg.catalog.s3.endpoint": S3_ENDPOINT, "iceberg.catalog.s3.path-style-access": "true",
           "iceberg.catalog.s3.access-key-id": _secret(j["id"], "s3-key"),
           "iceberg.catalog.s3.secret-access-key": _secret(j["id"], "s3-secret"),
           "iceberg.catalog.client.region": "us-east-1",
           "iceberg.tables.dynamic-enabled": "true", "iceberg.tables.route-field": "_cdc.target",
           "iceberg.tables.auto-create-enabled": "true", "iceberg.tables.evolve-schema-enabled": "true",
           "iceberg.control.commit.interval-ms": str(COMMIT_MS),
           "transforms": "dbz", "transforms.dbz.type": "io.tabular.iceberg.connect.transforms.DebeziumTransform",
           # {db} is the change's schema (Postgres): no namespace → <database>_<schema>, as with OLake
           "transforms.dbz.cdc.target.pattern": (ns if ns else re.sub(r"[^a-z0-9_]", "_", s["dbname"].lower()) + "_{db}") + ".{table}"}
    if j["mode"] == "cdc":                            # one row per key: updates replace, deletes remove
        cfg.update({"iceberg.tables.cdc-field": "_cdc.op", "iceberg.tables.upsert-mode-enabled": "true"})
        for sch, tb, key in tables:
            cfg[f"iceberg.table.{_namespace(s, d, sch)}.{tb}.id-columns"] = ",".join(key)
    return cfg


def _rows(j):
    s = _q("SELECT * FROM cdc_sources WHERE id = %s", (j["source_id"],), one=True)
    d = _q("SELECT * FROM cdc_destinations WHERE id = %s", (j["dest_id"],), one=True)
    return s, d


def ensure(j, state):
    """Both connectors there (made with `state` if missing; secrets and namespaces first) → their status."""
    s, d = _rows(j)
    conns = _connectors()
    if _src(j["id"]) in conns and _sink(j["id"]) in conns:
        return conns
    write_secrets(j, s)
    import lakehouse
    for ns in sorted({_namespace(s, d, sch) for sch, _, _ in _tables(j)}):
        try:
            lakehouse.create_namespace(j["member"], d["catalog"], ns)
        except ValueError:
            pass                                          # there already
    create_tables(j, s, d)
    _make_topics(j)
    for name, cfg in ((_sink(j["id"]), sink_config(j, s, d)), (_src(j["id"]), source_config(j, s))):
        if name not in conns:
            try:
                _rest("POST", "/connectors", {"name": name, "config": cfg, "initial_state": state})
            except DbzError as e:
                if "409" not in str(e):                   # made meanwhile (the reconcile loop)
                    raise
    time.sleep(2)
    return _connectors()


def _set_state(jid, state, conns):
    for name in (_sink(jid), _src(jid)) if state == "RUNNING" else (_src(jid), _sink(jid)):
        if name in conns and conns[name]["state"] != state:
            _rest("PUT", f"/connectors/{name}/{'resume' if state == 'RUNNING' else 'pause'}")


def _failed(conns, jid):
    """The first failed connector / task's error (its Java trace's first line), else None."""
    for name in (_src(jid), _sink(jid)):
        c = conns.get(name)
        if not c:
            continue
        for t in c["tasks"]:
            if t.get("state") == "FAILED":
                return f"{name}: " + _trace(t.get("trace"))
        if c["state"] == "FAILED":
            return f"{name} failed"
    return None


def _trace(tr):
    lines = [l.strip() for l in (tr or "").splitlines() if l.strip() and not l.strip().startswith("at ")]
    causes = [l for l in lines if l.startswith("Caused by:")]
    return (causes[-1] if causes else lines[0] if lines else "failed")[:600]


# ---- Kafka: how much a job's topics hold, and how far the sink has read ---------------------
_admin_client = None


def _admin():
    """One client, kept: a dropped one never answers its futures."""
    global _admin_client
    if _admin_client is None:
        from confluent_kafka.admin import AdminClient
        _admin_client = AdminClient({"bootstrap.servers": BOOTSTRAP})
    return _admin_client


def _topics(j):
    p = prefix(j) + "."
    return sorted(t for t in _admin().list_topics(timeout=10).topics if t.startswith(p))


def _ends(topics):
    """{(topic, partition): end offset}."""
    if not topics:
        return {}
    from confluent_kafka import Consumer, TopicPartition
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "lab-cdc-probe", "enable.auto.commit": False})
    try:
        md = c.list_topics(timeout=10)
        out = {}
        for t in topics:
            for p in (md.topics.get(t).partitions if md.topics.get(t) else {}):
                out[(t, p)] = c.get_watermark_offsets(TopicPartition(t, p), timeout=10)[1]
        return out
    finally:
        c.close()


def _committed(group, keys):
    """The consumer group's committed offsets for the (topic, partition)s → {(t, p): offset}."""
    if not keys:
        return {}
    from confluent_kafka import ConsumerGroupTopicPartitions, TopicPartition
    f = _admin().list_consumer_group_offsets([ConsumerGroupTopicPartitions(group, [TopicPartition(t, p) for t, p in keys])])
    out = {}
    for fut in f.values():
        for tp in fut.result(timeout=30).topic_partitions:
            out[(tp.topic, tp.partition)] = max(tp.offset, 0)
    return out


def lag(j):
    """(changes in the job's topics, changes the sink hasn't committed to Iceberg yet)."""
    ends = _ends(_topics(j))
    done = _committed(f"connect-{_sink(j['id'])}", list(ends))
    return sum(ends.values()), sum(max(0, e - done.get(k, 0)) for k, e in ends.items())


def _slot(s):
    """(confirmed_flush_lsn of the source's slot, the source's current WAL position) — as numbers."""
    c = _pg(s)
    with c, c.cursor() as cur:
        cur.execute("SELECT confirmed_flush_lsn - '0/0'::pg_lsn, pg_current_wal_lsn() - '0/0'::pg_lsn "
                    "FROM pg_replication_slots WHERE slot_name = %s", (s["slot"],))
        r = cur.fetchone()
        if not r:
            cur.execute("SELECT NULL::numeric, pg_current_wal_lsn() - '0/0'::pg_lsn")
            r = cur.fetchone()
    c.close()
    return (int(r[0]) if r[0] is not None else None), int(r[1])


# ---- a run (manual / scheduled): resume until caught up, then pause ---------------------------
def run(j, rid, log):
    """→ the number of changes it read. `log(line)` adds to the run's log."""
    s, d = _rows(j)
    _postgres_only(s)
    write_secrets(j, s)                                    # a key may have been renewed since
    t0 = int(time.time())
    conns = ensure(j, "PAUSED")
    _, target = _slot(s)
    total0, _ = lag(j)
    log(f"source {s['host']}/{s['dbname']} at WAL {target}; topics hold {total0} changes")
    _set_state(j["id"], "RUNNING", conns)
    log("resumed the connectors")
    start, last_total, last_move, phase = time.time(), total0, time.time(), "source"
    try:
        while True:
            if time.time() - start > TIMEOUT:
                raise DbzError("stopped: it ran longer than the time limit")
            time.sleep(3)
            conns = _connectors()
            err = _failed(conns, j["id"])
            if err:
                raise DbzError(err)
            try:
                total, behind = lag(j)
            except Exception as e:  # noqa: BLE001 — a new consumer group takes a moment: ask again
                print(f"[cdc] job {j['id']} lag: {e!r}", flush=True)
                continue
            if total != last_total:
                last_total, last_move = total, time.time()
            if phase == "source":
                flushed, _ = _slot(s)
                quiet = time.time() - last_move > IDLE_S and time.time() - start > 2 * IDLE_S
                if (flushed is not None and flushed >= target) or quiet:
                    log(f"read the source up to the run's start ({total - total0} changes)" if not quiet or (flushed or 0) >= target
                        else f"the source is quiet — read {total - total0} changes")
                    phase = "sink"
            if phase == "sink" and behind == 0 and total == last_total:
                log("the sink has committed everything to Iceberg")
                return total - total0
    finally:
        try:
            _set_state(j["id"], "PAUSED", _connectors())
            log("paused the connectors")
        except Exception as e:  # noqa: BLE001
            log(f"couldn't pause: {e}")
        lines = connect_log(j["id"], since=t0)
        if lines:
            log("— Kafka Connect, this job's connectors —\n" + "\n".join(lines))


# ---- keep Kafka Connect in step with the jobs ----------------------------------------------
def _event(meta, text):
    meta["events"] = (meta.get("events") or [])[-(EVENTS - 1):] + [time.strftime("%H:%M:%S ") + text]


def _stream_log(jid, meta):
    lines = connect_log(jid, since=meta.get("t0"))
    return "\n".join((meta.get("events") or []) + (["", "— Kafka Connect, this job's connectors (latest lines) —"] + lines if lines else []))


def _stream_run(j, conns):
    """A streaming job has one 'running' run row while it streams: its changes so far, how far behind,
    and its log — what happened (started, errors, restarts, how it's doing) + Kafka Connect's lines."""
    r = _q("SELECT id, meta FROM cdc_runs WHERE job_id = %s AND trigger = 'stream' AND status = 'running' ORDER BY id DESC LIMIT 1",
           (j["id"],), one=True)
    streaming = j["schedule"] == "stream" and j["enabled"]
    if not streaming:
        if r:
            meta = dict(r["meta"] or {})
            _event(meta, "■ stopped")
            _q("UPDATE cdc_runs SET status = 'ok', finished_at = now(), meta = %s, log = %s WHERE id = %s",
               (json.dumps(meta), _stream_log(j["id"], meta), r["id"]), rows=False)
        return
    total, behind = lag(j)
    err = _failed(conns, j["id"])
    if not r:
        meta = {"base": total, "behind": behind, "t0": int(time.time()), "noted": time.time()}
        _event(meta, f"▶ stream started — the topics hold {total} changes")
        _q("INSERT INTO cdc_runs (job_id, member, trigger, records, meta, log) VALUES (%s, %s, 'stream', 0, %s, %s)",
           (j["id"], j["member"], json.dumps(meta), "\n".join(meta["events"])), rows=False)
        _q("UPDATE cdc_jobs SET last_run_at = now() WHERE id = %s", (j["id"],), rows=False)
        return
    meta = dict(r["meta"] or {}, behind=behind, at=_now())
    records = max(0, total - int(meta.get("base", 0)))
    if err and err != meta.get("err"):
        _event(meta, f"✗ {err} — restarting it")
    elif not err and meta.get("err"):
        _event(meta, "✓ running again")
    meta["err"] = err
    if time.time() - float(meta.get("noted", 0)) > NOTE_S:
        _event(meta, f"{records} changes so far · {behind} on the way to Iceberg")
        meta["noted"] = time.time()
    _q("UPDATE cdc_runs SET records = %s, meta = %s, error = %s, log = %s WHERE id = %s",
       (records, json.dumps(meta), err, _stream_log(j["id"], meta), r["id"]), rows=False)


_off_said = [None]


def reconcile():
    jobs = {j["id"]: j for j in _q("SELECT * FROM cdc_jobs WHERE engine = 'debezium'")}
    running = {t["ref"] for t in _q("SELECT ref FROM cdc_tasks WHERE kind = 'run' AND status IN ('queued', 'running')")}
    try:
        conns = _connectors()
    except Off as e:                                     # the unit is off: nothing to keep in step — say so once
        global _admin_client
        _admin_client = None                             # (a Kafka client left alone keeps retrying, and logging)
        if _off_said[0] != "off":
            print(f"[cdc] {e}", flush=True)
            _off_said[0] = "off"
        return
    except DbzError as e:
        if "HTTP 404" in str(e) or "HTTP 503" in str(e):   # Kafka Connect still starting: next round
            return
        raise
    if _off_said[0] == "off":
        print("[cdc] Kafka Connect is up again (the Streaming unit)", flush=True)
    _off_said[0] = "on"
    # what deleted jobs left: their connectors, topics, secret files
    gone = sorted({int(CONN.match(n).group(1)) for n in conns} - set(jobs))
    for jid in gone:
        cleanup(jid)
    for name in _secret_dirs():
        m = re.fullmatch(r"cdc-(\d+)", name)
        if m and int(m.group(1)) not in jobs:
            _drop_secrets(int(m.group(1)))
    for j in jobs.values():
        if j["id"] in running:
            continue
        try:
            want = "RUNNING" if j["schedule"] == "stream" and j["enabled"] else "PAUSED"
            if want == "RUNNING" or _src(j["id"]) in conns:
                c = ensure(j, want) if not (_src(j["id"]) in conns and _sink(j["id"]) in conns) else conns
                if want == "RUNNING" and _failed(c, j["id"]):
                    s_, d_ = _rows(j)
                    create_tables(j, s_, d_)              # its tables taken by another tool → SetupError: it stops
                    write_secrets(j, s_)                  # a renewed key: the restarted task reads the files again
                    for name in (_src(j["id"]), _sink(j["id"])):
                        _rest("POST", f"/connectors/{name}/restart?includeTasks=true&onlyFailed=true")
                _set_state(j["id"], want, c)
                _stream_run(j, c)
        except SetupError as e:                          # it can't start: stop it, and say why on its page
            _q("UPDATE cdc_jobs SET enabled = false WHERE id = %s", (j["id"],), rows=False)
            r = _q("UPDATE cdc_runs SET status = 'failed', finished_at = now(), error = %s WHERE job_id = %s AND status = 'running' "
                   "AND trigger = 'stream' RETURNING id", (str(e), j["id"]), one=True)
            if not r:
                _q("INSERT INTO cdc_runs (job_id, member, trigger, status, finished_at, error) VALUES (%s, %s, 'stream', 'failed', now(), %s)",
                   (j["id"], j["member"], str(e)), rows=False)
        except Exception as e:  # noqa: BLE001 — the next round tries again
            print(f"[cdc] debezium job {j['id']}: {e}", flush=True)


def cleanup(jid):
    """A job is gone: its connectors, topics and secret files."""
    for name in (_src(jid), _sink(jid)):
        _rest("DELETE", f"/connectors/{name}", ok404=True)
    try:
        a = _admin()
        topics = [t for t in a.list_topics(timeout=10).topics if re.match(rf"^[^.]+\.j{jid}\.", t) or
                  re.match(rf"^__debezium-heartbeat\.[^.]+\.j{jid}$", t)]
        if topics:
            for f in a.delete_topics(topics).values():
                f.result(timeout=30)
    except Exception as e:  # noqa: BLE001
        print(f"[cdc] topics of job {jid}: {e}", flush=True)
    _drop_secrets(jid)
    print(f"[cdc] removed debezium job {jid}'s connectors and topics", flush=True)
