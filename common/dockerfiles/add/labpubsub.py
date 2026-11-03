"""labpubsub — "📨 Pub/Sub": a member makes a stream of business events — clicks, orders, payments,
stock moves — and event-sim (event_sim.py, in the Streaming unit) publishes them to a Kafka topic of
their own, <member>.<name>, until they pause or delete it. Members subscribe to it (Peek on the page
for now). Each event has an event_time (when it happened) and a publish_time (when it reached
Kafka); a stream can send some events late — random delays, or a device that was offline sending its
backlog at once — to practise event time vs processing time, watermarks and late data.

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
# the Iceberg table a stream lands in (🏞 Land in your lakehouse): its columns, typed — + _kafka (where it came from)
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
-- 🏞 landing in the lakehouse: cdc-runner keeps a sink connector ps-<id>-sink for it, and reports sink_status
ALTER TABLE pubsub_streams ADD COLUMN IF NOT EXISTS sink_catalog text;
ALTER TABLE pubsub_streams ADD COLUMN IF NOT EXISTS sink_namespace text;
ALTER TABLE pubsub_streams ADD COLUMN IF NOT EXISTS sink_table text;
ALTER TABLE pubsub_streams ADD COLUMN IF NOT EXISTS sink_on boolean NOT NULL DEFAULT false;
ALTER TABLE pubsub_streams ADD COLUMN IF NOT EXISTS sink_status jsonb;
ALTER TABLE pubsub_streams ADD COLUMN IF NOT EXISTS sink_link text;       -- this link (the table made for it says so)
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
                           "paused_why", "ready", "error", "sent", "late", "sink_on", "sink_status")}
    o["sink"] = ".".join((s["sink_catalog"], s["sink_namespace"], s["sink_table"])) if s["sink_catalog"] else None
    o.update(created_at=_iso(s["created_at"]), resumed_at=_iso(s["resumed_at"]), last_at=_iso(s["last_at"]))
    if recent:
        o["recent"], o["recent_late"] = s["recent"], s["recent_late"]
    return o


def overview(member, catalogs=()):
    rows = _q("SELECT * FROM pubsub_streams WHERE member = %s AND state <> 'deleted' ORDER BY name", (member,))
    return {"streams": [_out(s) for s in rows], "kinds": KINDS, "catalogs": list(catalogs),
            "columns": {k: [c for c, _ in FIRST + v] + [KAFKA[0]] for k, v in COLUMNS.items()}, "rates": RATES, "late_pct": LATE_PCT,
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


IDENT = re.compile(r"^[a-z][a-z0-9_]{0,62}$")


def land(member, sid, body, catalogs):
    """🏞 Land the stream in one of the member's catalogs: <catalog>.<namespace>.<table>, from the
    oldest event the topic still has; cdc-runner makes the table and the sink connector."""
    s = _own(member, sid)
    if s["sink_catalog"]:
        raise PubSubError(f"{s['name']} lands in {s['sink_catalog']}.{s['sink_namespace']}.{s['sink_table']} already — "
                          f"remove that first to choose another table")
    cat, ns, tb = body.get("catalog"), (body.get("namespace") or "").strip(), (body.get("table") or "").strip()
    if cat not in catalogs:
        raise PubSubError("pick one of your catalogs")
    if not IDENT.fullmatch(ns) or not IDENT.fullmatch(tb):
        raise PubSubError("namespace and table: lowercase letters, digits and _, starting with a letter")
    other = _q("SELECT name FROM pubsub_streams WHERE member = %s AND sink_catalog = %s AND sink_namespace = %s "
               "AND sink_table = %s AND state <> 'deleted'", (member, cat, ns, tb), one=True)
    if other:
        raise PubSubError(f"your stream {other['name']} lands in {cat}.{ns}.{tb} — pick another table")
    import secrets
    return _out(_q("UPDATE pubsub_streams SET sink_catalog = %s, sink_namespace = %s, sink_table = %s, sink_on = true, "
                   "sink_status = NULL, sink_link = %s WHERE id = %s RETURNING *",
                   (cat, ns, tb, secrets.token_hex(8), s["id"]), one=True))


def land_on(member, sid, on):
    s = _own(member, sid)
    if not s["sink_catalog"]:
        raise PubSubError("it doesn't land anywhere yet")
    return _out(_q("UPDATE pubsub_streams SET sink_on = %s WHERE id = %s RETURNING *", (bool(on), s["id"]), one=True))


def unland(member, sid):
    """Stop landing for good: the sink goes (the table stays)."""
    s = _own(member, sid)
    return _out(_q("UPDATE pubsub_streams SET sink_catalog = NULL, sink_namespace = NULL, sink_table = NULL, "
                   "sink_on = false, sink_status = NULL WHERE id = %s RETURNING *", (s["id"],), one=True))


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
