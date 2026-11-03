"""event-sim — publishes the events of every member's 📨 Pub/Sub stream (labpubsub.py) to Kafka.

It runs in the Streaming unit, next to Kafka. Every few seconds it reads the streams from the portal
database: a new one gets its topic (<member>.<name>, PARTITIONS partitions, kept RETENTION_H hours),
a deleted one loses it; then it publishes each running stream's events at its rate, keyed (orders by
order_id, clicks by session_id…), and reports back what it sent (counts, the last events with their
partition and offset — the page's Peek) and that it's alive.

The events use Tidewell's real customers, products and stores (lab-db, login reader), so they join
with what 📡 Ingestion copies from the live shop. Late events, when a stream asks for them:
  random  each late event is held back for a random time (LATE_AFTER_S × 2 … late_max), so it
          arrives after events that happened later — out of order, with its own event_time
  burst   a device / till goes offline: the events it makes are kept, then sent at once when it's
          back (after a random part of late_max) — a block of old events, in their own order
event_time is when it happened; publish_time (in the event, and the Kafka timestamp) when it was sent.
Held events live in memory: a restart of event-sim loses them (as a crashed phone would).
"""
import collections
import datetime
import heapq
import json
import os
import random
import time
import uuid

from confluent_kafka import KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

import labpubsub as ps

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
SHOP_DB = os.environ.get("EVENT_SIM_SHOP_DB", "postgresql://reader:reader@lab-db:5432/shop")
HOLD_MAX = int(os.environ.get("EVENT_SIM_HOLD_MAX", "20000"))    # held (late) events per stream, at most
TICK_S, SYNC_S, REF_S = 0.2, 3.0, 600.0
RECENT = 30

CURRENCY = {"US": "USD", "UK": "GBP", "JP": "JPY", "IN": "INR", "AU": "AUD", "SG": "SGD", "BR": "BRL"}
REFERRERS = ["google", "google", "instagram", "newsletter", "direct", "direct", "tiktok", "partner"]
SEARCHES = ["laptop", "headphones", "running shoes", "coffee", "gift", "desk lamp", "backpack", "phone case",
            "yoga mat", "kettle", "sale", "charger"]
DEVICES = ["web", "web", "web", "ios", "android"]
METHODS = ["card", "card", "card", "paypal", "ideal", "apple_pay"]
FAIL = ["insufficient_funds", "card_expired", "3ds_failed", "do_not_honor"]
REF = {"customers": [], "products": [], "stores": [], "at": 0.0}


def now_iso(t):
    return datetime.datetime.fromtimestamp(t, datetime.timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def eid():
    return uuid.uuid4().hex


# ---- Tidewell's customers, products and stores (lab-db) ---------------------------------------
def load_ref():
    if time.time() - REF["at"] < REF_S and REF["customers"]:
        return
    import psycopg2
    try:
        with psycopg2.connect(SHOP_DB, connect_timeout=5) as c, c.cursor() as cur:
            cur.execute("SELECT customer_id, country FROM customers")
            REF["customers"] = cur.fetchall()
            cur.execute("SELECT product_id, category, price::float FROM products")
            REF["products"] = cur.fetchall()
        with psycopg2.connect(SHOP_DB.rsplit("/", 1)[0] + "/store", connect_timeout=5) as c, c.cursor() as cur:
            cur.execute("SELECT code FROM stores ORDER BY code")
            REF["stores"] = [r[0] for r in cur.fetchall()]
    except Exception as e:                                       # lab-db not there: made-up ones
        print(f"event-sim: lab-db: {e} — using made-up customers and products", flush=True)
    if not REF["customers"]:
        REF["customers"] = [(i, random.choice(["NL", "UK", "DE", "US"])) for i in range(1, 501)]
    if not REF["products"]:
        REF["products"] = [(i, "misc", round(random.uniform(5, 300), 2)) for i in range(1, 41)]
    if not REF["stores"]:
        REF["stores"] = ["AMS-01", "LON-01", "BER-01"]
    REF["at"] = time.time()


def customer():
    return random.choice(REF["customers"])


def product():
    return random.choice(REF["products"])


def sku(pid):
    return f"SKU-{pid:04d}"


# ---- one generator per stream: event(t) → (key, event without publish_time) --------------------
class Clicks:
    def __init__(self, rate):
        self.sessions, self.rate = [], rate

    @staticmethod
    def _new(devices=DEVICES):
        c = customer() if random.random() < 0.6 else None
        return {"id": "s_" + uuid.uuid4().hex[:12], "customer_id": c[0] if c else None, "device": random.choice(devices),
                "left": random.randint(3, 25), "cart": [], "first": True}

    def _start(self, t):
        s = self._new()
        self.sessions.append(s)
        return s

    def event(self, t, offline=None):
        live = max(3, self.rate * 3)
        if offline is not None:                                  # a burst: the offline app's sessions, one after another
            if offline["left"] <= 0:
                offline.update(self._new(["ios", "android"]))
            s = offline
        elif len(self.sessions) < live and (not self.sessions or random.random() < 0.3):
            s = self._start(t)
        else:
            s = random.choice(self.sessions)
        e = {"event_id": eid(), "event_type": "page_view", "event_time": now_iso(t), "session_id": s["id"],
             "customer_id": s["customer_id"], "device": s["device"], "page": "/", "product_id": None,
             "search_term": None, "referrer": None}
        if s["first"]:
            s["first"] = False
            e["referrer"] = random.choice(REFERRERS)
            p = product()
            e["page"] = random.choice(["/", "/", f"/category/{p[1]}", f"/product/{p[0]}"])
        else:
            kinds = [("product_view", 35), ("page_view", 15), ("search", 10), ("add_to_cart", 15)]
            if s["cart"]:
                kinds += [("remove_from_cart", 4), ("checkout", 6)]
            k = random.choices([x for x, _ in kinds], [w for _, w in kinds])[0]
            e["event_type"] = k
            if k in ("product_view", "add_to_cart"):
                p = product()
                e["product_id"], e["page"] = p[0], f"/product/{p[0]}"
                if k == "add_to_cart":
                    s["cart"].append(p[0])
            elif k == "page_view":
                e["page"] = random.choice(["/", f"/category/{product()[1]}", "/cart", "/account", "/deals"])
            elif k == "search":
                e["search_term"] = random.choice(SEARCHES)
                e["page"] = "/search?q=" + e["search_term"].replace(" ", "+")
            elif k == "remove_from_cart":
                e["product_id"] = s["cart"].pop(random.randrange(len(s["cart"])))
                e["page"] = "/cart"
            else:
                e["page"], s["left"] = "/checkout", 0
        s["left"] -= 1
        if s["left"] <= 0 and s in self.sessions:
            self.sessions.remove(s)
        return s["id"], e

    def offline(self):
        return self._new(["ios", "android"])                     # the app, offline — not one of the live sessions


class Orders:
    NEXT = {"order_placed": "order_paid", "order_paid": "order_shipped", "order_shipped": "order_delivered"}
    WAIT = {"order_placed": (5, 90), "order_paid": (60, 600), "order_shipped": (300, 1800)}   # s until the next step

    def __init__(self, rate):
        self.open = collections.OrderedDict()                    # order_id → the order, its last step and when the next is due

    def _due(self, t):
        for o in self.open.values():                             # oldest first
            if o["next_at"] <= t:
                return o
        return None

    def event(self, t, offline=None):
        o = self._due(t) if random.random() < 0.6 else None
        if o is None:
            c = customer()
            items = [{"product_id": p[0], "quantity": random.randint(1, 3), "price": p[2]}
                     for p in random.sample(REF["products"], min(len(REF["products"]), random.randint(1, 4)))]
            o = {"order_id": "o_" + uuid.uuid4().hex[:12], "customer_id": c[0], "country": c[1],
                 "currency": CURRENCY.get(c[1], "EUR"), "items": items,
                 "total": round(sum(i["quantity"] * i["price"] for i in items), 2)}
            self.open[o["order_id"]] = o
            while len(self.open) > 5000:
                self.open.popitem(last=False)
            kind = "order_placed"
        else:
            kind = "order_cancelled" if o["status"] in ("order_placed", "order_paid") and random.random() < 0.08 \
                else self.NEXT[o["status"]]
        o["status"] = kind
        if kind in self.WAIT:
            o["next_at"] = t + random.uniform(*self.WAIT[kind])
            self.open.move_to_end(o["order_id"])
        else:
            self.open.pop(o["order_id"], None)
        e = {"event_id": eid(), "event_type": kind, "event_time": now_iso(t), "order_id": o["order_id"],
             "customer_id": o["customer_id"], "items": o["items"] if kind == "order_placed" else None,
             "total": o["total"], "currency": o["currency"], "country": o["country"]}
        return o["order_id"], e

    def offline(self):
        return None


class Payments:
    def __init__(self, rate):
        self.open = collections.OrderedDict()                    # payment_id → authorised / captured, next step due

    def event(self, t, offline=None):
        p = next((x for x in self.open.values() if x["next_at"] <= t), None) if random.random() < 0.5 else None
        if p is None:
            c = customer()
            p = {"payment_id": "p_" + uuid.uuid4().hex[:12], "order_id": "o_" + uuid.uuid4().hex[:12], "customer_id": c[0],
                 "amount": round(random.uniform(8, 900), 2), "currency": CURRENCY.get(c[1], "EUR"),
                 "method": random.choice(METHODS)}
            self.open[p["payment_id"]] = p
            while len(self.open) > 5000:
                self.open.popitem(last=False)
            kind, reason = "payment_authorized", None
        elif p["status"] == "payment_authorized":
            kind = "payment_failed" if random.random() < 0.1 else "payment_captured"
            reason = random.choice(FAIL) if kind == "payment_failed" else None
        else:
            kind, reason = "payment_refunded", random.choice(["returned", "damaged", "late_delivery"])
        p["status"] = kind
        if kind == "payment_authorized":
            p["next_at"] = t + random.uniform(2, 30)
            self.open.move_to_end(p["payment_id"])
        elif kind == "payment_captured" and random.random() < 0.06:  # a few are refunded later
            p["next_at"] = t + random.uniform(600, 3600)
            self.open.move_to_end(p["payment_id"])
        else:
            self.open.pop(p["payment_id"], None)
        e = {"event_id": eid(), "event_type": kind, "event_time": now_iso(t), "payment_id": p["payment_id"],
             "order_id": p["order_id"], "customer_id": p["customer_id"], "amount": p["amount"], "currency": p["currency"],
             "method": p["method"], "reason": reason}
        return p["payment_id"], e

    def offline(self):
        return None


class Stock:
    def __init__(self, rate):
        pass

    def event(self, t, offline=None):
        store = offline or random.choice(REF["stores"])
        k = random.choices(["stock_sold", "stock_received", "stock_reserved", "stock_adjusted"], [60, 10, 20, 10])[0]
        qty, reason = {"stock_sold": (-random.randint(1, 3), "sale"),
                       "stock_received": (random.randint(20, 120), "delivery"),
                       "stock_reserved": (-1, "web order"),
                       "stock_adjusted": (random.choice([-5, -2, -1, 1, 2, 4]), random.choice(["count", "damage"]))}[k]
        s = sku(product()[0])
        e = {"event_id": eid(), "event_type": k, "event_time": now_iso(t), "sku": s, "store": store,
             "qty_change": qty, "reason": reason}
        return s, e

    def offline(self):
        return random.choice(REF["stores"])                      # a till offline: its store's moves wait


GEN = {"click_events": Clicks, "orders": Orders, "payments": Payments, "inventory": Stock}


# ---- a stream: settings from the database + its generator, held events and counters ------------
class Stream:
    def __init__(self, row):
        self.id, self.topic, self.kind = row["id"], row["topic"], row["kind"]
        self.gen = GEN[self.kind](row["rate"])
        self.held = []                                           # (publish_at, seq, key, event) — random late events
        self.burst = None                                        # {"until", "events": [...], "who"}
        self.credit, self.seq = 0.0, 0
        self.sent = self.late = 0                                # since the last report
        self.recent = collections.deque(row["recent"] or [], maxlen=RECENT)
        self.recent_late = collections.deque(row["recent_late"] or [], maxlen=RECENT)
        self.last_at = None
        self.set(row)

    def set(self, row):
        self.row = row
        self.gen.rate = row["rate"]

    def lateness(self):
        return random.uniform(ps.LATE_AFTER_S * 2, self.row["late_max"])

    def tick(self, t, dt, send):
        r = self.row
        self.credit += r["rate"] * dt
        n, self.credit = int(self.credit), self.credit - int(self.credit)
        late = r["late_pct"] / 100.0
        for _ in range(n):
            if late and r["late_pattern"] == "burst":
                if self.burst is None:
                    self.burst = {"until": t + random.uniform(r["late_max"] / 4, r["late_max"]), "events": [],
                                  "who": self.gen.offline()}
                if random.random() < late and len(self.burst["events"]) < HOLD_MAX:
                    self.burst["events"].append(self.gen.event(t, self.burst["who"]))
                    continue
            elif late and random.random() < late and len(self.held) < HOLD_MAX:
                self.seq += 1
                key, e = self.gen.event(t)
                heapq.heappush(self.held, (t + self.lateness(), self.seq, key, e))
                continue
            send(self, *self.gen.event(t - random.uniform(0, 1.5)))   # on time: a second or so on the way
        while self.held and self.held[0][0] <= t:
            _, _, key, e = heapq.heappop(self.held)
            send(self, key, e)
        if self.burst and (t >= self.burst["until"] or len(self.burst["events"]) >= HOLD_MAX):
            for key, e in self.burst["events"]:                  # back online: the backlog, oldest first
                send(self, key, e)
            self.burst = None


def main():
    print(f"event-sim: Kafka {BOOTSTRAP}", flush=True)
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    prod = Producer({"bootstrap.servers": BOOTSTRAP, "linger.ms": 50, "compression.type": "lz4",
                     "message.timeout.ms": 30000,
                     "allow.auto.create.topics": False})          # a deleted stream's topic must stay deleted
    streams, last_sync = {}, 0.0

    def delivered(st, err, msg, late_s):
        if err:
            print(f"event-sim: {st.topic}: {err}", flush=True)
            return
        r = {"key": msg.key().decode(), "partition": msg.partition(), "offset": msg.offset(),
             "late_s": round(late_s, 1), "event": json.loads(msg.value())}
        st.sent += 1
        st.last_at = time.time()
        st.recent.appendleft(r)
        if late_s > ps.LATE_AFTER_S:
            st.late += 1
            st.recent_late.appendleft(r)

    def send(st, key, e):
        t = time.time()
        e = dict(e, publish_time=now_iso(t))
        late_s = t - datetime.datetime.fromisoformat(e["event_time"].replace("Z", "+00:00")).timestamp()
        prod.produce(st.topic, key=key, value=json.dumps(e, separators=(",", ":")), timestamp=int(t * 1000),
                     on_delivery=lambda err, msg: delivered(st, err, msg, late_s))

    def topics(rows):
        """make the new streams' topics, drop the deleted ones'"""
        new = [r for r in rows if r["state"] != "deleted" and not r["ready"]]
        if new:
            fs = admin.create_topics([NewTopic(r["topic"], ps.PARTITIONS, 1,
                                               config={"retention.ms": str(ps.RETENTION_H * 3600 * 1000)}) for r in new])
            for r in new:
                try:
                    fs[r["topic"]].result(15)
                    ok = None
                except KafkaException as e:
                    ok = None if e.args[0].code() == 36 else str(e.args[0].str())     # 36: it's there already
                ps._q("UPDATE pubsub_streams SET ready = %s, error = %s WHERE id = %s", (ok is None, ok, r["id"]), rows=False)
                r["ready"] = ok is None
        gone = [r for r in rows if r["state"] == "deleted"]
        if gone:
            fs = admin.delete_topics([r["topic"] for r in gone], operation_timeout=15)
            for r in gone:
                try:
                    fs[r["topic"]].result(20)
                except KafkaException as e:
                    if e.args[0].code() != 3:                    # 3: no such topic — fine
                        print(f"event-sim: delete {r['topic']}: {e}", flush=True)
                        continue
                ps._q("DELETE FROM pubsub_streams WHERE id = %s AND state = 'deleted'", (r["id"],), rows=False)
                streams.pop(r["id"], None)

    def sync():
        """report what was sent, read the settings, pause streams that ran AUTO_PAUSE_H, beat"""
        for st in streams.values():
            if st.sent or st.late:
                ps._q("UPDATE pubsub_streams SET sent = sent + %s, late = late + %s, last_at = to_timestamp(%s), "
                      "recent = %s, recent_late = %s WHERE id = %s",
                      (st.sent, st.late, st.last_at, json.dumps(list(st.recent)), json.dumps(list(st.recent_late)), st.id),
                      rows=False)
                st.sent = st.late = 0
        ps._q("UPDATE pubsub_streams SET state = 'paused', paused_why = 'auto' WHERE state = 'running' "
              "AND resumed_at < now() - make_interval(hours => %s)", (ps.AUTO_PAUSE_H,), rows=False)
        rows = ps._q("SELECT * FROM pubsub_streams")
        topics(rows)
        for r in rows:
            if r["state"] == "deleted" or not r["ready"]:
                continue
            if r["id"] in streams:
                streams[r["id"]].set(r)
            else:
                streams[r["id"]] = Stream(r)
        for sid in set(streams) - {r["id"] for r in rows}:
            streams.pop(sid)
        ps._q("INSERT INTO pubsub_sim VALUES (1, now()) ON CONFLICT (id) DO UPDATE SET beat = now()", rows=False)

    last = time.time()
    while True:
        t = time.time()
        if t - last_sync >= SYNC_S:
            try:
                load_ref()
                sync()
            except Exception as e:                               # the database or Kafka not up yet: try again
                print(f"event-sim: {type(e).__name__}: {e}", flush=True)
                time.sleep(5)
            last_sync = time.time()
        dt, last = t - last, t
        for st in list(streams.values()):
            if st.row["state"] == "running":
                st.tick(t, min(dt, 2.0), send)
        prod.poll(0)
        time.sleep(TICK_S)


if __name__ == "__main__":
    main()
