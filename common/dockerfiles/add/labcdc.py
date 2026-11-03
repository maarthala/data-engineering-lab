"""labcdc — "📡 Ingestion (CDC)": a member copies tables from their own database (Postgres, MySQL or
SQL Server) into their lakehouse
— a full load, then every insert / update / delete (change data capture) on a schedule.

Members keep, each only their own (portal database, like 📝 Mock exams):
  sources       a database they reach (KINDS) (host, port, database, user; the password is kept in the
                vault's admin-only _lab/cdc/<member>/<id>, never shown again), with CDC on or off
                (on: a publication + a logical replication slot, made by the first check)
  destinations  one of their lakehouse catalogs (+ an optional namespace) — no keys to type: a sync
                writes as the member's job login (lab_jobs: <member>_jobs + their own-bucket key)
  jobs          source → destination: the tables, full load + CDC or a full reload every run, how
                it runs (manual / cron — a cron expression, UTC / stream); each run's status, rows and log
The work itself — check a connection, list a source's tables, run a sync — is OLake's engine
(olakego/source-<kind>, `check` / `discover` / `sync`) in a container per call, started by the
cdc-runner service (cdc_runner.py), which picks up cdc_tasks rows. Nothing here talks to OLake.
CDC_ENGINE=debezium: new jobs are Debezium + Kafka Connect instead (cdc_debezium.py) — PostgreSQL
sources, and one more schedule, "stream" (always on); each job keeps the engine it was made with.
"""
import json
import os
import re
import secrets
import threading
import datetime

DB_URL = os.environ.get("PORTAL_DB_URL", "postgresql://portal:portal@postgres:5432/portal")
VAULT_PATH = "_lab/cdc"                       # vault (admin only): <member>/<source id> → {password}
# what CDC needs in each kind of database (the page shows it; Postgres's publication + slot are made
# by the first check, the others are server settings an admin of that database switches on)
KINDS = {"postgres": {"name": "PostgreSQL", "port": 5432,
                      "cdc": "wal_level = logical; the check makes a publication and a replication slot"},
         "mysql": {"name": "MySQL", "port": 3306,
                   "cdc": "the binary log on with binlog_format = ROW (and binlog_row_image = FULL); a login with "
                          "REPLICATION SLAVE, REPLICATION CLIENT and SELECT"},
         "mssql": {"name": "SQL Server", "port": 1433,
                   "cdc": "CDC switched on for the database (EXEC sys.sp_cdc_enable_db) and each table "
                          "(sys.sp_cdc_enable_table), and SQL Server Agent running"}}
ENGINE = os.environ.get("CDC_ENGINE", "olake")              # new jobs: olake | debezium
# a job's schedule: '' (manual), 'stream' (always on — debezium only) or a cron expression (UTC)
LEGACY = {"15m": "*/15 * * * *", "1h": "0 * * * *", "1d": "0 2 * * *"}     # the old presets
CONNECT = os.environ.get("CONNECT_URL", "http://kafka-connect:8083")      # debezium: the Streaming unit
STREAMING_HOW = ("Manage → ⚙️ Settings → Services → Streaming" if os.environ.get("KUBERNETES_SERVICE_HOST")
                 else "make status → Streaming → s")
MODES = {"cdc": "full load, then changes (CDC)",                 # one row per key: the latest (upsert)
         "append": "full load, then every change as a new row",  # the history: build SCD2 from it
         "full_refresh": "full reload every run"}                # olake only
STREAMING = ("cdc", "append")                                    # modes that read the change stream
NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9 _.-]{0,59}$")
IDENT = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")
HOST = re.compile(r"^[A-Za-z0-9.-]{1,253}$")
LIMITS = {"sources": 10, "destinations": 10, "jobs": 10}
# the lab's live shop database (lab-db, kept changing by cdc-sim): every member starts with it as a source
LAB_DB = os.environ.get("CDC_LAB_DB", "")                    # postgresql://reader:reader@lab-db:5432/shop

SCHEMA = """
CREATE TABLE IF NOT EXISTS cdc_sources (
  id bigserial PRIMARY KEY, member text NOT NULL, name text NOT NULL, host text NOT NULL, port int NOT NULL,
  dbname text NOT NULL, username text NOT NULL, schemas text[] NOT NULL DEFAULT '{}', cdc boolean NOT NULL,
  publication text, slot text, password text, checked jsonb, created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (member, name));
ALTER TABLE cdc_sources ADD COLUMN IF NOT EXISTS kind text NOT NULL DEFAULT 'postgres';
CREATE TABLE IF NOT EXISTS cdc_destinations (
  id bigserial PRIMARY KEY, member text NOT NULL, name text NOT NULL, catalog text NOT NULL, namespace text,
  checked jsonb, created_at timestamptz NOT NULL DEFAULT now(), UNIQUE (member, name));
CREATE TABLE IF NOT EXISTS cdc_jobs (
  id bigserial PRIMARY KEY, member text NOT NULL, name text NOT NULL,
  source_id bigint NOT NULL REFERENCES cdc_sources, dest_id bigint NOT NULL REFERENCES cdc_destinations,
  tables text[] NOT NULL, mode text NOT NULL, schedule text NOT NULL DEFAULT '', enabled boolean NOT NULL DEFAULT true,
  streams jsonb NOT NULL, state jsonb, created_at timestamptz NOT NULL DEFAULT now(), last_run_at timestamptz,
  UNIQUE (member, name));
CREATE TABLE IF NOT EXISTS cdc_runs (
  id bigserial PRIMARY KEY, job_id bigint NOT NULL REFERENCES cdc_jobs ON DELETE CASCADE, member text NOT NULL,
  started_at timestamptz NOT NULL DEFAULT now(), finished_at timestamptz, status text NOT NULL DEFAULT 'running',
  records bigint, error text, log text, trigger text);
CREATE INDEX IF NOT EXISTS cdc_runs_job ON cdc_runs (job_id, started_at DESC);
CREATE TABLE IF NOT EXISTS cdc_tasks (
  id bigserial PRIMARY KEY, member text NOT NULL, kind text NOT NULL, ref bigint, payload jsonb,
  status text NOT NULL DEFAULT 'queued', result jsonb, error text,
  created_at timestamptz NOT NULL DEFAULT now(), finished_at timestamptz);
CREATE INDEX IF NOT EXISTS cdc_tasks_queued ON cdc_tasks (status, id);
CREATE TABLE IF NOT EXISTS portal_cdc_seen (member text PRIMARY KEY);      -- got the lab source once (deleting it is fine)
UPDATE cdc_jobs SET schedule = CASE schedule WHEN '15m' THEN '*/15 * * * *' WHEN '1h' THEN '0 * * * *' ELSE '0 2 * * *' END
 WHERE schedule IN ('15m', '1h', '1d');
ALTER TABLE cdc_jobs ADD COLUMN IF NOT EXISTS engine text NOT NULL DEFAULT 'olake';
ALTER TABLE cdc_runs ADD COLUMN IF NOT EXISTS meta jsonb;                   -- debezium: a stream's base, how far behind
"""
_ready, _lock = False, threading.Lock()


class CdcError(ValueError):
    pass


def _conn():
    global _ready
    import psycopg2
    import psycopg2.extras
    c = psycopg2.connect(DB_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    if not _ready:
        with _lock:
            if not _ready:
                with c, c.cursor() as cur:
                    cur.execute("SELECT pg_advisory_xact_lock(4712)")
                    cur.execute(SCHEMA)
                _ready = True
    return c


def _q(sql, args=(), one=False, rows=True):
    c = _conn()
    try:
        with c, c.cursor() as cur:
            cur.execute(sql, args)
            if not rows:
                return cur.rowcount
            out = cur.fetchall() if cur.description else []
            return (out[0] if out else None) if one else out
    finally:
        c.close()


def _iso(t):
    return t.isoformat() if t else None


# ---- the source password: the vault's admin-only folder (or the row, without a vault) --------
def _vault():
    import labvault
    return labvault if labvault.enabled() else None


def save_password(member, sid, password):
    v = _vault()
    if not v:
        _q("UPDATE cdc_sources SET password = %s WHERE id = %s", (password, sid), rows=False)
        return
    v._ok(v._call("POST", f"{v.MOUNT}/data/{VAULT_PATH}/{member}/{sid}", {"data": {"password": password}})[0],
          "source password")


def password(member, sid):
    v = _vault()
    if not v:
        return (_q("SELECT password FROM cdc_sources WHERE id = %s", (sid,), one=True) or {}).get("password") or ""
    st, body = v._call("GET", f"{v.MOUNT}/data/{VAULT_PATH}/{member}/{sid}")
    return body["data"]["data"]["password"] if st == 200 else ""


def _drop_password(member, sid):
    v = _vault()
    if v:
        v._call("DELETE", f"{v.MOUNT}/metadata/{VAULT_PATH}/{member}/{sid}")


# ---- what the page shows -------------------------------------------------------------------
def _source_out(s):
    return {"id": s["id"], "name": s["name"], "kind": s["kind"], "host": s["host"], "port": s["port"], "database": s["dbname"],
            "username": s["username"], "schemas": s["schemas"], "cdc": s["cdc"], "publication": s["publication"],
            "slot": s["slot"], "checked": s["checked"], "created_at": _iso(s["created_at"])}


# ---- cron: minute hour day-of-month month day-of-week (UTC) ---------------------------------
CRON_FIELDS = ((0, 59), (0, 23), (1, 31), (1, 12), (0, 7))


def cron(expr):
    """'*/15 * * * *' → five sets of numbers (Sunday is 0 or 7); ValueError if it isn't one."""
    parts = str(expr or "").split()
    if len(parts) != 5:
        raise ValueError("five fields: minute hour day month weekday")
    out = []
    for f, (lo, hi) in zip(parts, CRON_FIELDS):
        vals = set()
        for part in f.split(","):
            rng, _, step = part.partition("/")
            step = int(step) if step else 1
            if rng == "*":
                a, b = lo, hi
            elif "-" in rng:
                a, b = (int(x) for x in rng.split("-", 1))
            else:
                a = int(rng)
                b = hi if step > 1 else a
            if step < 1 or not lo <= a <= b <= hi:
                raise ValueError(f"'{part}' is out of range {lo}–{hi}")
            vals.update(range(a, b + 1, step))
        out.append(vals)
    out[4] = {0 if d == 7 else d for d in out[4]}
    out.append((parts[2] != "*", parts[4] != "*"))         # cron's rule: both days given → either one
    return out


def cron_hits(c, t):
    mi, ho, dom, mo, dow, (rd, rw) = c
    if t.minute not in mi or t.hour not in ho or t.month not in mo:
        return False
    d, w = t.day in dom, (t.weekday() + 1) % 7 in dow
    return (d or w) if rd and rw else (d and w)


def cron_last(expr, now=None):
    """The latest time ≤ now the cron fired (looking back 8 days), else None."""
    c, t = cron(expr), (now or datetime.datetime.now(datetime.timezone.utc)).replace(second=0, microsecond=0)
    for _ in range(8 * 24 * 60):
        if cron_hits(c, t):
            return t
        t -= datetime.timedelta(minutes=1)
    return None


def cron_next(expr, now=None):
    c = cron(expr)
    t = (now or datetime.datetime.now(datetime.timezone.utc)).replace(second=0, microsecond=0) + datetime.timedelta(minutes=1)
    for _ in range(62 * 24 * 60):                       # a monthly cron, with room
        if cron_hits(c, t):
            return t
        t += datetime.timedelta(minutes=1)
    return None


def schedule_ok(engine, v):
    """'' | 'stream' (debezium) | a cron expression → the value to keep."""
    v = " ".join(str(v or "").split())
    v = LEGACY.get(v, v)
    if v == "stream":
        if engine != "debezium":
            raise CdcError("Datastream needs the Debezium engine")
        return v
    if v:
        try:
            cron(v)
        except ValueError as e:
            raise CdcError(f"cron: {e}") from None
    return v


def _next_run(j):
    if not j["schedule"] or j["schedule"] == "stream" or not j["enabled"]:
        return None
    try:
        return _iso(cron_next(j["schedule"]))
    except ValueError:
        return None


def modes(engine):
    return {k: v for k, v in MODES.items() if engine == "olake" or k != "full_refresh"}


_stream_seen = [0.0, False]


def streaming_on():
    """Debezium: is the Streaming unit (Kafka Connect) up? Asked at most every 10 s."""
    import socket
    import time
    import urllib.parse
    if ENGINE != "debezium":
        return True
    if time.time() - _stream_seen[0] > 10:
        u = urllib.parse.urlparse(CONNECT)
        try:
            socket.create_connection((u.hostname, u.port or 80), timeout=1).close()
            _stream_seen[1] = True
        except OSError:
            _stream_seen[1] = False
        _stream_seen[0] = time.time()
    return _stream_seen[1]


def kinds():
    return {k: v for k, v in KINDS.items() if ENGINE == "olake" or k == "postgres"}


def overview(member, catalogs=()):
    """Everything on the member's page: sources, destinations, jobs (+ last run), their catalogs."""
    src = _q("SELECT * FROM cdc_sources WHERE member = %s ORDER BY name", (member,))
    dst = _q("SELECT * FROM cdc_destinations WHERE member = %s ORDER BY name", (member,))
    jobs = _q("SELECT j.*, s.name AS source_name, d.name AS dest_name, d.catalog, "
              "(SELECT row_to_json(r) FROM (SELECT id, status, started_at, finished_at, records, error, trigger, meta FROM cdc_runs "
              " WHERE job_id = j.id ORDER BY started_at DESC LIMIT 1) r) AS last, "
              "EXISTS (SELECT 1 FROM cdc_tasks t WHERE t.kind = 'run' AND t.ref = j.id AND t.status IN ('queued', 'running')) AS busy "
              "FROM cdc_jobs j JOIN cdc_sources s ON s.id = j.source_id JOIN cdc_destinations d ON d.id = j.dest_id "
              "WHERE j.member = %s ORDER BY j.name", (member,))
    return {"sources": [_source_out(s) for s in src],
            "destinations": [{"id": d["id"], "name": d["name"], "catalog": d["catalog"], "namespace": d["namespace"],
                              "checked": d["checked"], "created_at": _iso(d["created_at"])} for d in dst],
            "jobs": [{"id": j["id"], "name": j["name"], "source": j["source_id"], "source_name": j["source_name"],
                      "destination": j["dest_id"], "dest_name": j["dest_name"], "catalog": j["catalog"],
                      "tables": j["tables"], "mode": j["mode"], "schedule": j["schedule"], "enabled": j["enabled"],
                      "engine": j["engine"], "next_run": _next_run(j),
                      "created_at": _iso(j["created_at"]), "last": j["last"], "busy": j["busy"]} for j in jobs],
            "catalogs": list(catalogs), "engine": ENGINE, "streaming": streaming_on(), "streaming_how": STREAMING_HOW,
            "modes": modes(ENGINE),
            "kinds": kinds()}


def _own(table, member, oid):
    row = _q(f"SELECT * FROM {table} WHERE id = %s AND member = %s", (int(oid), member), one=True)
    if not row:
        raise CdcError("not found")
    return row


def _name(v, what="name"):
    v = str(v or "").strip()
    if not NAME.fullmatch(v):
        raise CdcError(f"{what}: letters, digits, space, _ . - (up to 60)")
    return v


def _limit(table, member):
    n = _q(f"SELECT count(*) AS n FROM cdc_{table} WHERE member = %s", (member,), one=True)["n"]
    if n >= LIMITS[table]:
        raise CdcError(f"you have {n} {table} — the most there can be; delete one first")


def _slot_name(member):
    """<member>_<random> — made once per source and kept (an edit doesn't rename it: the old slot would be left over)."""
    return re.sub(r"[^a-z0-9_]", "_", member.lower())[:54] + "_" + secrets.token_hex(4)


# ---- sources -------------------------------------------------------------------------------
def _fields(body):
    """The connection fields of a source form, checked → (kind, host, port, database, user, schemas, cdc)."""
    kind = str(body.get("kind") or "postgres")
    if kind not in kinds():
        raise CdcError("database type: " + ", ".join(k["name"] for k in kinds().values()))
    host = str(body.get("host") or "").strip()
    if not HOST.fullmatch(host):
        raise CdcError("host: a name or address")
    try:
        port = int(body.get("port") or KINDS[kind]["port"])
    except ValueError:
        raise CdcError("port: a number") from None
    if not 0 < port < 65536:
        raise CdcError("port: 1–65535")
    db, user = str(body.get("database") or "").strip(), str(body.get("username") or "").strip()
    if not db or not user or len(db) > 63 or len(user) > 63:
        raise CdcError("database and user are needed")
    raw = body.get("schemas") or []
    if isinstance(raw, str):                          # "public, sales" as well as ["public", "sales"]
        raw = raw.split(",")
    schemas = [str(x).strip() for x in raw if str(x).strip()] if kind == "postgres" else []
    if any(not IDENT.fullmatch(s) for s in schemas):
        raise CdcError("schemas: lower-case names, e.g. public")
    return kind, host, port, db, user, schemas, bool(body.get("cdc"))


def try_source(member, body, kind_of_task):
    """Test connection / Browse tables on the form as it is — saved or not: a task whose payload is
    the form (its password, or the saved source's when left empty), wiped once the task is done."""
    kind, host, port, db, user, schemas, cdc = _fields(body)
    pw, sid = str(body.get("password") or ""), None
    if body.get("id"):
        sid = _own("cdc_sources", member, body["id"])["id"]
    elif not pw:
        raise CdcError("password is needed")
    return {"task": queue(member, kind_of_task, sid, {"kind": kind, "host": host, "port": port, "dbname": db,
                                                       "username": user, "schemas": schemas, "cdc": cdc, "password": pw})}


def save_source(member, body):
    """New source (no id) or a change to one (id; an empty password keeps the old one). → a check task."""
    kind, host, port, db, user, schemas, cdc = _fields(body)
    pg = kind == "postgres"                              # Postgres: a publication + a slot of its own
    pub = str(body.get("publication") or "").strip() or None
    if cdc and pub and not IDENT.fullmatch(pub):
        raise CdcError("publication: a lower-case name")
    name, pw = _name(body.get("name")), str(body.get("password") or "")
    if body.get("id"):
        s = _own("cdc_sources", member, body["id"])
        if _q("SELECT 1 FROM cdc_sources WHERE member = %s AND name = %s AND id <> %s", (member, name, s["id"]), one=True):
            raise CdcError("you have a source with that name")
        if s["kind"] != kind:
            raise CdcError("the database type can't change — add a new source")
        _q("UPDATE cdc_sources SET name = %s, host = %s, port = %s, dbname = %s, username = %s, schemas = %s, cdc = %s, "
           "publication = %s, slot = %s, checked = NULL WHERE id = %s",
           (name, host, port, db, user, schemas, cdc, (pub or "olake_pub") if cdc and pg else None,
            (s["slot"] or _slot_name(member)) if cdc and pg else None, s["id"]), rows=False)
        sid = s["id"]
    else:
        if not pw:
            raise CdcError("password is needed")
        _limit("sources", member)
        if _q("SELECT 1 FROM cdc_sources WHERE member = %s AND name = %s", (member, name), one=True):
            raise CdcError("you have a source with that name")
        sid = _q("INSERT INTO cdc_sources (member, name, kind, host, port, dbname, username, schemas, cdc, publication) "
                 "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id",
                 (member, name, kind, host, port, db, user, schemas, cdc, (pub or "olake_pub") if cdc and pg else None),
                 one=True)["id"]
        if cdc and pg:
            _q("UPDATE cdc_sources SET slot = %s WHERE id = %s", (_slot_name(member), sid), rows=False)
    if pw:
        save_password(member, sid, pw)
    return {"id": sid, "task": queue(member, "check_source", sid)}


def delete_source(member, sid):
    s = _own("cdc_sources", member, sid)
    used = _q("SELECT name FROM cdc_jobs WHERE source_id = %s", (s["id"],))
    if used:
        raise CdcError("used by job " + ", ".join(j["name"] for j in used) + " — delete it first")
    if s["slot"]:                     # cdc-runner drops the slot first (a left-over slot keeps the source's WAL)
        _q("UPDATE cdc_sources SET name = name || ' (deleting)' WHERE id = %s", (s["id"],), rows=False)
        return {"deleting": s["id"], "task": queue(member, "drop_source", s["id"])}
    drop_source(s)
    return {"deleted": s["id"]}


def drop_source(s):
    _q("DELETE FROM cdc_sources WHERE id = %s", (s["id"],), rows=False)
    _drop_password(s["member"], s["id"])


# ---- destinations --------------------------------------------------------------------------
def save_destination(member, body, catalogs):
    name, catalog = _name(body.get("name")), str(body.get("catalog") or "")
    if catalog not in catalogs:
        raise CdcError("choose one of your lakehouses")
    ns = str(body.get("namespace") or "").strip().lower() or None
    if ns and not IDENT.fullmatch(ns):
        raise CdcError("namespace: a lower-case name, e.g. bronze_cdc")
    if body.get("id"):
        d = _own("cdc_destinations", member, body["id"])
        _q("UPDATE cdc_destinations SET name = %s, catalog = %s, namespace = %s, checked = NULL WHERE id = %s",
           (name, catalog, ns, d["id"]), rows=False)
        did = d["id"]
    else:
        _limit("destinations", member)
        if _q("SELECT 1 FROM cdc_destinations WHERE member = %s AND name = %s", (member, name), one=True):
            raise CdcError("you have a destination with that name")
        did = _q("INSERT INTO cdc_destinations (member, name, catalog, namespace) VALUES (%s, %s, %s, %s) RETURNING id",
                 (member, name, catalog, ns), one=True)["id"]
    _make_namespace(member, catalog, ns)
    return {"id": did, "task": queue(member, "check_destination", did)}


def _make_namespace(member, catalog, ns):
    """The destination's namespace exists in the catalog straight away (My catalogs shows it), not
    only after the first run writes a table."""
    if not ns:
        return
    try:
        import lakehouse
        lakehouse.create_namespace(member, catalog, ns)
    except ValueError:
        pass                                          # there already (or a name Polaris can't take: OLake will say)
    except Exception as e:  # noqa: BLE001 — the first run makes it anyway
        print(f"[labcdc] namespace {catalog}.{ns}: {e}", flush=True)


def ensure_default_destination(member, catalog):
    """A member's first visit only: "My lakehouse" (their own catalog, namespace cdc) and the lab's live
    shop as a source, CDC on (its replication slot is made by the first check or run, not before — no
    slot for members who never use it). Once only, so deleting them sticks."""
    if not _q("INSERT INTO portal_cdc_seen (member) VALUES (%s) ON CONFLICT DO NOTHING RETURNING member", (member,), one=True):
        return
    if not _q("SELECT 1 FROM cdc_destinations WHERE member = %s", (member,), one=True):
        _q("INSERT INTO cdc_destinations (member, name, catalog, namespace) VALUES (%s, 'My lakehouse', %s, 'cdc') "
           "ON CONFLICT DO NOTHING", (member, catalog), rows=False)
        _make_namespace(member, catalog, "cdc")
    if not LAB_DB or _q("SELECT 1 FROM cdc_sources WHERE member = %s AND name = 'Lab shop (live)'", (member,), one=True):
        return
    import urllib.parse
    u = urllib.parse.urlparse(LAB_DB)
    sid = _q("INSERT INTO cdc_sources (member, name, kind, host, port, dbname, username, schemas, cdc, publication) "
             "VALUES (%s, 'Lab shop (live)', 'postgres', %s, %s, %s, %s, '{public}', true, 'olake_pub') RETURNING id",
             (member, u.hostname, u.port or 5432, u.path.lstrip("/"), urllib.parse.unquote(u.username or "")), one=True)["id"]
    _q("UPDATE cdc_sources SET slot = %s WHERE id = %s", (_slot_name(member), sid), rows=False)
    save_password(member, sid, urllib.parse.unquote(u.password or ""))


def delete_destination(member, did):
    d = _own("cdc_destinations", member, did)
    used = _q("SELECT name FROM cdc_jobs WHERE dest_id = %s", (d["id"],))
    if used:
        raise CdcError("used by job " + ", ".join(j["name"] for j in used) + " — delete it first")
    _q("DELETE FROM cdc_destinations WHERE id = %s", (d["id"],), rows=False)
    return {"deleted": d["id"]}


# ---- jobs ----------------------------------------------------------------------------------
def create_job(member, body):
    """From a finished discover task (its tables) + the member's choice of tables, mode, schedule."""
    name = _name(body.get("name"))
    s = _own("cdc_sources", member, body.get("source") or 0)
    d = _own("cdc_destinations", member, body.get("destination") or 0)
    mode, schedule = str(body.get("mode") or "cdc"), str(body.get("schedule") or "")
    if mode not in modes(ENGINE):
        raise CdcError("unknown mode")
    schedule = schedule_ok(ENGINE, schedule)
    if ENGINE == "debezium" and s["kind"] != "postgres":
        raise CdcError("with Debezium, PostgreSQL sources only (for now)")
    if mode in STREAMING and not s["cdc"]:
        raise CdcError("this source has CDC off — turn it on, or choose a full reload")
    if mode in STREAMING and _q("SELECT name FROM cdc_jobs WHERE source_id = %s AND mode IN ('cdc', 'append')", (s["id"],), one=True):
        raise CdcError("a CDC job already reads this source (one change stream = one job)")
    t = _q("SELECT * FROM cdc_tasks WHERE id = %s AND member = %s AND kind = 'discover' AND ref = %s AND status = 'done'",
           (int(body.get("discover") or 0), member, s["id"]), one=True)
    if not t:
        raise CdcError("list the source's tables first")
    found = {f"{x['stream']['namespace']}.{x['stream']['name']}": x for x in t["result"]["streams"]["streams"]}
    tables = [x for x in dict.fromkeys(body.get("tables") or []) if x in found]
    if not tables:
        raise CdcError("choose at least one table")
    if ENGINE == "debezium" and mode == "cdc":         # an upsert needs the row's key
        nokey = [x for x in tables if not found[x]["stream"].get("source_defined_primary_key")]
        if nokey:
            raise CdcError("no primary key: " + ", ".join(nokey) + " — leave them out, or choose every change (history)")
    _limit("jobs", member)
    if _q("SELECT 1 FROM cdc_jobs WHERE member = %s AND name = %s", (member, name), one=True):
        raise CdcError("you have a job with that name")
    if d["namespace"]:                   # two jobs writing one Iceberg table would mix their rows
        mine = {x.split(".", 1)[1] for x in tables}
        for o in _q("SELECT name, tables FROM cdc_jobs WHERE dest_id IN (SELECT id FROM cdc_destinations WHERE "
                    "member = %s AND catalog = %s AND namespace = %s)", (member, d["catalog"], d["namespace"])):
            same = sorted(mine & {x.split(".", 1)[1] for x in o["tables"]})
            if same:
                raise CdcError(f"job {o['name']} already writes {', '.join(same)} into {d['catalog']}.{d['namespace']} — "
                               "choose a destination with another namespace")
    jid = _q("INSERT INTO cdc_jobs (member, name, source_id, dest_id, tables, mode, schedule, streams, engine) "
             "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id",
             (member, name, s["id"], d["id"], tables, mode, schedule,
              json.dumps(streams_for(t["result"]["streams"], tables, mode, d["namespace"])), ENGINE), one=True)["id"]
    if schedule == "stream":                       # cdc-runner starts the stream (cdc_debezium.reconcile)
        return {"id": jid}
    return {"id": jid, "task": queue(member, "run", jid, {"trigger": "first run"})}


def streams_for(discovered, tables, mode, namespace):
    """OLake's streams file for the chosen tables: their sync mode, and the namespace they land in
    (the destination's, else <database>_<schema> — OLake's own default)."""
    keep, streams, selected = set(tables), [], {}
    for x in discovered["streams"]:
        st = x["stream"]
        key = f"{st['namespace']}.{st['name']}"
        if key not in keep:
            continue
        sync = "cdc" if mode in STREAMING else mode
        st = dict(st, sync_mode=sync if sync in st.get("supported_sync_modes", [sync]) else "full_refresh")
        if namespace:
            st["destination_database"] = namespace
        streams.append(dict(x, stream=st))
        sel = next((s for s in discovered.get("selected_streams", {}).get(st["namespace"], []) if s["stream_name"] == st["name"]),
                   {"stream_name": st["name"], "partition_regex": "", "normalization": True})
        selected.setdefault(st["namespace"], []).append(dict(sel, append_mode=mode == "append"))   # append: every change a row
    return {"selected_streams": selected, "streams": streams}


def set_job(member, jid, enabled=None, schedule=None):
    j = _own("cdc_jobs", member, jid)
    if schedule is not None:
        schedule = schedule_ok(j["engine"], schedule)
    _q("UPDATE cdc_jobs SET enabled = COALESCE(%s, enabled), schedule = COALESCE(%s, schedule) WHERE id = %s",
       (enabled, schedule, j["id"]), rows=False)
    return {"id": j["id"]}


def run_job(member, jid):
    j = _own("cdc_jobs", member, jid)
    if j["schedule"] == "stream":
        raise CdcError("this job streams all the time — nothing to run")
    if _busy(j["id"]):
        raise CdcError("this job is running already")
    return {"task": queue(member, "run", j["id"], {"trigger": "run now"})}


def _busy(jid):
    return bool(_q("SELECT 1 FROM cdc_tasks WHERE kind = 'run' AND ref = %s AND status IN ('queued', 'running')",
                   (jid,), one=True))


def delete_job(member, jid):
    j = _own("cdc_jobs", member, jid)
    if _busy(j["id"]):
        raise CdcError("this job is running — wait until it's done")
    _q("DELETE FROM cdc_jobs WHERE id = %s", (j["id"],), rows=False)
    return {"deleted": j["id"]}


def runs(member, jid):
    j = _own("cdc_jobs", member, jid)
    return {"job": j["id"], "runs": [dict(r, started_at=_iso(r["started_at"]), finished_at=_iso(r["finished_at"])) for r in _q(
        "SELECT id, status, started_at, finished_at, records, error, trigger, meta FROM cdc_runs WHERE job_id = %s "
        "ORDER BY started_at DESC LIMIT 20", (j["id"],))]}


def run_log(member, rid):
    r = _q("SELECT r.* FROM cdc_runs r WHERE r.id = %s AND r.member = %s", (int(rid), member), one=True)
    if not r:
        raise CdcError("not found")
    return {"id": r["id"], "status": r["status"], "log": r["log"] or "", "error": r["error"]}


# ---- tasks: the work for cdc-runner ---------------------------------------------------------
def queue(member, kind, ref, payload=None):
    return _q("INSERT INTO cdc_tasks (member, kind, ref, payload) VALUES (%s, %s, %s, %s) RETURNING id",
              (member, kind, ref, json.dumps(payload or {})), one=True)["id"]


def discover(member, sid):
    s = _own("cdc_sources", member, sid)
    return {"task": queue(member, "discover", s["id"])}


def task(member, tid):
    t = _q("SELECT id, kind, ref, status, result, error, created_at, finished_at FROM cdc_tasks WHERE id = %s AND member = %s",
           (int(tid), member), one=True)
    if not t:
        raise CdcError("not found")
    out = dict(t, created_at=_iso(t["created_at"]), finished_at=_iso(t["finished_at"]))
    if t["kind"] in ("discover", "browse") and t["result"]:          # the page needs the tables, not OLake's whole file
        out["result"] = {"tables": t["result"].get("tables", [])}
    return out


def forget(member):
    """A member is deleted: their jobs, runs, tasks, destinations, sources and stored passwords."""
    for s in _q("SELECT id FROM cdc_sources WHERE member = %s", (member,)):
        _drop_password(member, s["id"])
    for t in ("cdc_tasks", "cdc_jobs", "cdc_destinations", "cdc_sources", "portal_cdc_seen"):
        _q(f"DELETE FROM {t} WHERE member = %s", (member,), rows=False)
