"""labpubsub — "📨 Pub/Sub": a member makes a stream of business events — clicks, orders, payments,
stock moves — and event-sim (event_sim.py, in the Streaming unit) publishes them to a Kafka topic of
their own, <member>.<name>, until they pause or delete it. Members subscribe to it (Peek on the page
for now). Each event has an event_time (when it happened) and a publish_time (when it reached
Kafka); a stream can send some events late — random delays, or a device that was offline sending its
backlog at once — to practise event time vs processing time, watermarks and late data. Each stream is
a source in 📡 Ingestion too (labcdc.sync_streams): a job there lands its events in a table.

Kept in the portal database (like 📡 Ingestion): one row per stream with its settings and what
event-sim reports back (events sent, how many late, the last ones sent). home-api writes the
settings; event-sim makes / deletes the topics and does the publishing.
"""
import os
import re
import threading

DB_URL = os.environ.get("PORTAL_DB_URL", "postgresql://portal:portal@postgres:5432/portal")
STREAMING_HOW = ("Manage → ⚙️ Settings → Services → Streaming" if os.environ.get("KUBERNETES_SERVICE_HOST")
                 else "make status → Streaming → s")
# what a stream can carry: the page shows the fields; event_sim.py makes them
KINDS = {
    "click_events": {"name": "Click events", "key": "session_id",
                     "what": "A web shop's clickstream: sessions of page views, product views, searches, add / remove "
                             "from cart and checkouts, signed-in or anonymous, on web or the apps.",
                     "types": ["page_view", "search", "product_view", "add_to_cart", "remove_from_cart", "checkout"],
                     "fields": ["event_id", "event_type", "event_time", "publish_time", "session_id", "customer_id",
                                "device", "page", "product_id", "search_term", "referrer"]},
    "orders": {"name": "Orders", "key": "order_id",
               "what": "Order lifecycle events: placed (with its items), paid, shipped, delivered or cancelled — "
                       "several events per order, in order… unless they arrive late.",
               "types": ["order_placed", "order_paid", "order_shipped", "order_delivered", "order_cancelled"],
               "fields": ["event_id", "event_type", "event_time", "publish_time", "order_id", "customer_id", "items",
                          "total", "currency", "country"]},
    "payments": {"name": "Payments", "key": "payment_id",
                 "what": "A payment provider's events: authorised, captured, failed, refunded — with method and amount.",
                 "types": ["payment_authorized", "payment_captured", "payment_failed", "payment_refunded"],
                 "fields": ["event_id", "event_type", "event_time", "publish_time", "payment_id", "order_id",
                            "customer_id", "amount", "currency", "method", "reason"]},
    "inventory": {"name": "Stock moves", "key": "sku",
                  "what": "Stock changes per store and SKU: received, sold, reserved, adjusted (counts, damage).",
                  "types": ["stock_received", "stock_sold", "stock_reserved", "stock_adjusted"],
                  "fields": ["event_id", "event_type", "event_time", "publish_time", "sku", "store", "qty_change",
                             "reason"]},
}
# the Iceberg table a 📡 Ingestion job lands a stream in: its columns, typed — + _kafka (where it came from)
_ITEM = ("struct", [("product_id", "long"), ("quantity", "int"), ("price", "double")])
COLUMNS = {
    "click_events": [("session_id", "string"), ("customer_id", "long"), ("device", "string"), ("page", "string"),
                     ("product_id", "long"), ("search_term", "string"), ("referrer", "string")],
    "orders": [("order_id", "string"), ("customer_id", "long"), ("items", ("list", _ITEM)), ("total", "double"),
               ("currency", "string"), ("country", "string")],
    "payments": [("payment_id", "string"), ("order_id", "string"), ("customer_id", "long"), ("amount", "double"),
                 ("currency", "string"), ("method", "string"), ("reason", "string")],
    "inventory": [("sku", "string"), ("store", "string"), ("qty_change", "int"), ("reason", "string")],
}
FIRST = [("event_id", "string"), ("event_type", "string"), ("event_time", "timestamptz"), ("publish_time", "timestamptz")]
KAFKA = ("_kafka", ("struct", [("topic", "string"), ("partition", "int"), ("offset", "long"), ("timestamp", "timestamptz")]))


def iceberg_schema(kind):
    """The stream's table as an Iceberg REST schema (field ids numbered through the nesting)."""
    n = [0]

    def nid():
        n[0] += 1
        return n[0]

    def typ(t):
        if isinstance(t, str):
            return t
        if t[0] == "list":
            return {"type": "list", "element-id": nid(), "element-required": False, "element": typ(t[1])}
        return {"type": "struct", "fields": fields(t[1])}

    def fields(cols):
        out = []
        for name, t in cols:
            fid = nid()
            out.append({"id": fid, "name": name, "required": False, "type": typ(t)})
        return out
    return {"type": "struct", "schema-id": 0, "fields": fields(FIRST + COLUMNS[kind] + [KAFKA])}


# members subscribe from notebooks / Spark on Kafka's members' listener: a login each (SCRAM), allowed
# to read their own topics and use consumer groups named <member>.… (ACLs) — kept in their own vault
# folder as the secret "kafka" (username, password, bootstrap), so key_vault / lab_kafka() find it
MEMBERS_BOOTSTRAP = os.environ.get("KAFKA_MEMBERS_BOOTSTRAP", "kafka:9094")
VAULT_KEY = "kafka"
CHECK_S = 300                                        # how often a login's vault secret is looked at again

def spark_ddl(kind):
    """The event as a Spark DDL schema — from_json(col("value").cast("string"), <this>) in a notebook."""
    def typ(t):
        if isinstance(t, str):
            return {"string": "STRING", "long": "BIGINT", "int": "INT", "double": "DOUBLE", "timestamptz": "TIMESTAMP"}[t]
        if t[0] == "list":
            return f"ARRAY<{typ(t[1])}>"
        return "STRUCT<" + ", ".join(f"{n}: {typ(x)}" for n, x in t[1]) + ">"
    return ", ".join(f"{n} {typ(t)}" for n, t in FIRST + COLUMNS[kind])


RATES = (1, 5, 10, 20, 50, 100)                      # events per second; the most a member may pick is a setting
LATE_PCT = (0, 5, 20)                                # % of events that arrive late
LATE_MAX = {60: "up to 1 min", 600: "up to 10 min", 3600: "up to 1 h", 86400: "up to 1 day"}
PATTERNS = {"random": "random delays", "burst": "offline bursts — a device sends its backlog at once"}
LATE_AFTER_S = 5                                     # later than this counts (and shows) as late
AUTO_PAUSE_H = 8                                     # a running stream pauses itself after this long
RETENTION_H = 24                                     # a topic keeps its events this long
PARTITIONS = 3
NAME = re.compile(r"^[a-z][a-z0-9_]{1,39}$")
RESERVED = re.compile(r"^j\d+$")                     # <member>.j<n>.… are 📡 Ingestion's topics

SCHEMA = """
CREATE TABLE IF NOT EXISTS pubsub_streams (
  id bigserial PRIMARY KEY, member text NOT NULL, name text NOT NULL, kind text NOT NULL, topic text NOT NULL UNIQUE,
  rate int NOT NULL, late_pct int NOT NULL DEFAULT 0, late_max int NOT NULL DEFAULT 600,
  late_pattern text NOT NULL DEFAULT 'random',
  state text NOT NULL DEFAULT 'running',             -- running | paused | deleted (event-sim drops the topic, then the row)
  paused_why text, ready boolean NOT NULL DEFAULT false, error text,
  created_at timestamptz NOT NULL DEFAULT now(), resumed_at timestamptz NOT NULL DEFAULT now(),
  sent bigint NOT NULL DEFAULT 0, late bigint NOT NULL DEFAULT 0, last_at timestamptz,
  recent jsonb NOT NULL DEFAULT '[]', UNIQUE (member, name));
ALTER TABLE pubsub_streams ADD COLUMN IF NOT EXISTS recent_late jsonb NOT NULL DEFAULT '[]';   -- the last late ones
CREATE TABLE IF NOT EXISTS pubsub_logins (member text PRIMARY KEY, made_at timestamptz NOT NULL DEFAULT now(),
  checked_at timestamptz NOT NULL DEFAULT now());   -- a member's Kafka login (cdc-runner: kafka_logins)
CREATE TABLE IF NOT EXISTS pubsub_sim (id int PRIMARY KEY, beat timestamptz NOT NULL);   -- event-sim is alive
"""
_ready, _lock = False, threading.Lock()


class PubSubError(ValueError):
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
                    cur.execute("SELECT pg_advisory_xact_lock(4713)")
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


def _limits():
    import lakehouse as lh
    s = lh.lab_settings()
    return {"streams": s.get("pubsub_streams", 3), "rate": s.get("pubsub_rate", 20)}


def sim_alive():
    """Is event-sim running (the Streaming unit is on)? It beats every few seconds."""
    r = _q("SELECT now() - beat < interval '20 seconds' AS ok FROM pubsub_sim WHERE id = 1", one=True)
    return bool(r and r["ok"])


def _out(s, recent=False):
    o = {k: s[k] for k in ("id", "name", "kind", "topic", "rate", "late_pct", "late_max", "late_pattern", "state",
                           "paused_why", "ready", "error", "sent", "late")}
    o.update(created_at=_iso(s["created_at"]), resumed_at=_iso(s["resumed_at"]), last_at=_iso(s["last_at"]))
    if recent:
        o["recent"], o["recent_late"] = s["recent"], s["recent_late"]
    return o


def overview(member, jobs=None):
    """The member's streams (+ the 📡 Ingestion jobs landing each: `jobs`, {stream id: [job]}) and the choices."""
    rows = _q("SELECT * FROM pubsub_streams WHERE member = %s AND state <> 'deleted' ORDER BY name", (member,))
    jobs = jobs or {}
    return {"streams": [dict(_out(s), jobs=jobs.get(s["id"], [])) for s in rows], "kinds": KINDS,
            "columns": {k: [c for c, _ in FIRST + v] + [KAFKA[0]] for k, v in COLUMNS.items()},
            "ddl": {k: spark_ddl(k) for k in COLUMNS}, "rates": RATES, "late_pct": LATE_PCT,
            "late_max": LATE_MAX, "patterns": PATTERNS, "late_after_s": LATE_AFTER_S, "auto_pause_h": AUTO_PAUSE_H,
            "retention_h": RETENTION_H, "partitions": PARTITIONS, "limits": _limits(),
            "streaming": sim_alive(), "streaming_how": STREAMING_HOW}


def _own(member, sid):
    s = _q("SELECT * FROM pubsub_streams WHERE id = %s AND member = %s AND state <> 'deleted'", (int(sid), member), one=True)
    if not s:
        raise PubSubError("no such stream")
    return s


def _settings(body, lim):
    try:
        rate, pct, mx = int(body.get("rate", 5)), int(body.get("late_pct", 0)), int(body.get("late_max", 600))
    except (TypeError, ValueError):
        raise PubSubError("rate, late % and how late must be numbers")
    pattern = body.get("late_pattern") or "random"
    if rate not in RATES:
        raise PubSubError(f"rate: one of {', '.join(map(str, RATES))} events a second")
    if rate > lim["rate"]:
        raise PubSubError(f"the lab allows at most {lim['rate']} events a second per stream")
    if pct not in LATE_PCT:
        raise PubSubError("late events: 0, 5 or 20 %")
    if mx not in LATE_MAX:
        raise PubSubError("how late: 1 min, 10 min, 1 h or 1 day")
    if pattern not in PATTERNS:
        raise PubSubError("pattern: random or burst")
    return rate, pct, mx, pattern


def create(member, body):
    name, kind = (body.get("name") or "").strip(), body.get("kind")
    if kind not in KINDS:
        raise PubSubError("pick what the stream carries")
    if not NAME.fullmatch(name):
        raise PubSubError("name: 2–40 lowercase letters, digits or _, starting with a letter (e.g. click_events)")
    if RESERVED.fullmatch(name):
        raise PubSubError(f"{name}: names like j1, j2… are 📡 Ingestion's — pick another")
    lim = _limits()
    rate, pct, mx, pattern = _settings(body, lim)
    n = _q("SELECT count(*) AS n FROM pubsub_streams WHERE member = %s AND state <> 'deleted'", (member,), one=True)["n"]
    if n >= lim["streams"]:
        raise PubSubError(f"you have {n} streams — the lab allows {lim['streams']}; delete one first")
    topic = f"{member}.{name}"
    if _q("SELECT 1 FROM pubsub_streams WHERE topic = %s", (topic,), one=True):
        raise PubSubError(f"a stream called {name} exists (or is still being deleted) — pick another name")
    s = _q("INSERT INTO pubsub_streams (member, name, kind, topic, rate, late_pct, late_max, late_pattern) "
           "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING *", (member, name, kind, topic, rate, pct, mx, pattern), one=True)
    return _out(s)


def update(member, sid, body):
    """Change the rate / late settings, or pause / resume (state)."""
    s = _own(member, sid)
    rate, pct, mx, pattern = _settings({**{k: s[k] for k in ("rate", "late_pct", "late_max", "late_pattern")}, **body},
                                       _limits())
    state = body.get("state", s["state"])
    if state not in ("running", "paused"):
        raise PubSubError("state: running or paused")
    resumed = state == "running" and s["state"] != "running"
    return _out(_q("UPDATE pubsub_streams SET rate = %s, late_pct = %s, late_max = %s, late_pattern = %s, state = %s, "
                   "paused_why = CASE WHEN %s = 'paused' THEN 'by you' END, "
                   "resumed_at = CASE WHEN %s THEN now() ELSE resumed_at END WHERE id = %s RETURNING *",
                   (rate, pct, mx, pattern, state, state, resumed, s["id"]), one=True))


def delete(member, sid):
    s = _own(member, sid)
    _q("UPDATE pubsub_streams SET state = 'deleted' WHERE id = %s", (s["id"],), rows=False)
    return {"deleted": s["name"]}


def peek(member, sid):
    """The stream with the last events event-sim sent (newest first), each with its partition and offset —
    all, and the late ones."""
    return _out(_own(member, sid), recent=True)


def forget(member):
    """A member is deleted: their streams go (event-sim drops the topics when it next runs)."""
    _q("UPDATE pubsub_streams SET state = 'deleted' WHERE member = %s", (member,), rows=False)


# ---- members' Kafka logins (run by cdc-runner: it has the vault and Kafka's admin listener) ---------
def _acls(member):
    from confluent_kafka.admin import (AclBinding, AclOperation, AclPermissionType, ResourcePatternType,
                                       ResourceType)
    p, allow = f"User:{member}", AclPermissionType.ALLOW
    return [AclBinding(ResourceType.TOPIC, f"{member}.", ResourcePatternType.PREFIXED, p, "*", op, allow)
            for op in (AclOperation.READ, AclOperation.DESCRIBE)] + \
           [AclBinding(ResourceType.GROUP, f"{member}.", ResourcePatternType.PREFIXED, p, "*", op, allow)
            for op in (AclOperation.READ, AclOperation.DESCRIBE)]


def _login(admin, member, fresh):
    """The member's login in Kafka + its secret in their vault folder (the same password again if it's there)."""
    import secrets
    import labvault
    from confluent_kafka.admin import ScramCredentialInfo, ScramMechanism, UserScramCredentialUpsertion
    have = labvault.fields(member, VAULT_KEY) or {}
    pw = have.get("password") if have.get("username") == member else None
    if pw and not fresh:
        return
    pw = pw or secrets.token_urlsafe(24)
    up = UserScramCredentialUpsertion(member, ScramCredentialInfo(ScramMechanism.SCRAM_SHA_512, 8192), pw.encode())
    for f in admin.alter_user_scram_credentials([up]).values():
        f.result(timeout=30)
    for f in admin.create_acls(_acls(member)).values():
        f.result(timeout=30)
    labvault.put_fields(member, VAULT_KEY, {"username": member, "password": pw, "bootstrap": MEMBERS_BOOTSTRAP})
    print(f"[pubsub] Kafka login for {member}", flush=True)


def _unlogin(admin, member):
    import labvault
    from confluent_kafka.admin import (AclBindingFilter, AclOperation, AclPermissionType, ResourcePatternType,
                                       ResourceType, ScramMechanism, UserScramCredentialDeletion)
    for f in admin.alter_user_scram_credentials([UserScramCredentialDeletion(member, ScramMechanism.SCRAM_SHA_512)]).values():
        try:
            f.result(timeout=30)
        except Exception:  # noqa: BLE001 — none
            pass
    flt = AclBindingFilter(ResourceType.ANY, None, ResourcePatternType.ANY, f"User:{member}", None,
                           AclOperation.ANY, AclPermissionType.ANY)
    for f in admin.delete_acls([flt]).values():
        f.result(timeout=30)
    try:
        labvault.delete(member, VAULT_KEY)
    except Exception:  # noqa: BLE001 — a deleted member's folder is gone already
        pass
    print(f"[pubsub] removed {member}'s Kafka login", flush=True)


def kafka_logins(admin):
    """A login for every member with a stream (and only them); a vault secret deleted on My secrets
    comes back within CHECK_S."""
    want = {r["member"] for r in _q("SELECT DISTINCT member FROM pubsub_streams WHERE state <> 'deleted'")}
    have = {r["member"]: r for r in _q("SELECT member, now() - checked_at > make_interval(secs => %s) AS due "
                                       "FROM pubsub_logins", (CHECK_S,))}
    for m in sorted(want):
        if m not in have or have[m]["due"]:
            try:
                _login(admin, m, fresh=m not in have)
                _q("INSERT INTO pubsub_logins (member) VALUES (%s) ON CONFLICT (member) DO UPDATE SET checked_at = now()",
                   (m,), rows=False)
            except Exception as e:  # noqa: BLE001 — next round
                print(f"[pubsub] Kafka login for {m}: {e}", flush=True)
    for m in sorted(set(have) - want):
        try:
            _unlogin(admin, m)
            _q("DELETE FROM pubsub_logins WHERE member = %s", (m,), rows=False)
        except Exception as e:  # noqa: BLE001
            print(f"[pubsub] remove {m}'s Kafka login: {e}", flush=True)
