"""labschema — 📨 Pub/Sub's custom schemas: a member builds the event a stream carries from a library of
ready-made fields (ONTOLOGY: who, where, web, device, product, order, payment, shipping, stock, IoT,
finance, support, media, ids), in objects and arrays, and event-sim (event_sim.py) generates events of
that shape. Saving a changed schema makes a new revision, like a Pub/Sub topic's schema revisions: the
simulator switches to it (all at once, or a share of the events for a while — producers that update
one by one), so the stream's shape drifts the way real producers' do. Every event says which revision
made it (schema_revision).

A schema: {"key": <a top-level field, else event_id>, "fields": [node, …]}, a node:
  {"id", "name", "field": "<group>.<field>"}            a field from the library
  {"id", "name", "object": [node, …]}                   an object (nested, up to MAX_DEPTH levels)
  + "repeat": [min, max]                                an array of it (1–MAX_REPEAT)
  + "null_pct": 0–90                                    how often it's empty (null)
  + "values": ["a", "b", …]                             a text field picks from these instead
  + "as": "string"                                      sent as text ("12.5" instead of 12.5)
"id" stays when a field is renamed or changed, so a diff tells a rename from a remove + add.
Revisions must stay compatible (compatible()): a name keeps its shape and type in every revision — a
table built from one revision has to take the events of all the others (a typed column parses text, but
can't hold an object where a number was). A new shape needs a new name, as with a schema registry.
"""
import datetime
import json
import random
import re
import uuid

MAX_FIELDS, MAX_DEPTH, MAX_REPEAT, MAX_VALUES = 60, 3, 10, 20
NAME = re.compile(r"^[a-z][a-z0-9_]{0,39}$")
FIXED = ["event_id", "event_time", "publish_time", "schema_revision"]   # every event has these (late data needs the times)
TYPES = ("string", "long", "double", "boolean", "timestamptz", "date")
AS = {"string": TYPES}                                  # "as" → the library types it may change (text: any)

FIRST_NAMES = ["Emma", "Liam", "Olivia", "Noah", "Ava", "Lucas", "Mia", "Arjun", "Sofia", "Yuki", "Chen", "Amara",
               "Mateo", "Zara", "Ingrid", "Kwame", "Priya", "Hugo", "Lena", "Omar", "Aiko", "Diego", "Freya", "Ravi"]
LAST_NAMES = ["Jansen", "Smith", "Garcia", "Müller", "Tanaka", "Patel", "Kim", "Silva", "Novak", "Okafor", "Rossi",
              "Larsen", "Dubois", "Cohen", "Nguyen", "Kowalski", "Haddad", "Sato", "Fischer", "Mendes"]
CITIES = {"US": ["New York", "Chicago", "Austin", "Seattle"], "UK": ["London", "Manchester", "Leeds"],
          "NL": ["Amsterdam", "Rotterdam", "Utrecht"], "DE": ["Berlin", "Hamburg", "Munich"],
          "JP": ["Tokyo", "Osaka", "Kyoto"], "IN": ["Mumbai", "Bengaluru", "Delhi"], "AU": ["Sydney", "Melbourne"],
          "SG": ["Singapore"], "BR": ["São Paulo", "Rio de Janeiro"], "FR": ["Paris", "Lyon"]}
LATLON = {"US": (39.8, -98.6), "UK": (52.4, -1.5), "NL": (52.2, 5.3), "DE": (51.2, 10.4), "JP": (36.2, 138.3),
          "IN": (20.6, 78.9), "AU": (-25.3, 133.8), "SG": (1.35, 103.8), "BR": (-14.2, -51.9), "FR": (46.2, 2.2)}
CURRENCY = {"US": "USD", "UK": "GBP", "JP": "JPY", "IN": "INR", "AU": "AUD", "SG": "SGD", "BR": "BRL"}
LANG = {"US": "en", "UK": "en", "AU": "en", "SG": "en", "NL": "nl", "DE": "de", "FR": "fr", "JP": "ja", "IN": "hi", "BR": "pt"}
TZ = {"US": "America/New_York", "UK": "Europe/London", "NL": "Europe/Amsterdam", "DE": "Europe/Berlin",
      "JP": "Asia/Tokyo", "IN": "Asia/Kolkata", "AU": "Australia/Sydney", "SG": "Asia/Singapore",
      "BR": "America/Sao_Paulo", "FR": "Europe/Paris"}
ADJ = ["Wireless", "Classic", "Smart", "Eco", "Compact", "Pro", "Ultra", "Soft", "Travel", "Studio"]
NOUN = {"electronics": ["Headphones", "Speaker", "Charger", "Mouse", "Keyboard"], "home": ["Lamp", "Kettle", "Mug", "Pillow"],
        "sports": ["Yoga Mat", "Bottle", "Running Shoes"], "books": ["Notebook", "Cookbook", "Novel"]}
BRANDS = ["Northwind", "Acme", "Tidewell", "Lumen", "Kestrel", "Orbit", "Fjord", "Nimbus"]
COLORS = ["black", "white", "blue", "red", "green", "grey", "beige"]
PAGES = ["/", "/deals", "/cart", "/checkout", "/account", "/search", "/category/home", "/category/electronics", "/help"]
REFERRERS = ["google", "google", "instagram", "newsletter", "direct", "direct", "tiktok", "partner", None]
SEARCH = ["laptop", "headphones", "running shoes", "coffee", "gift", "desk lamp", "backpack", "kettle", "sale"]
BROWSERS = ["Chrome", "Chrome", "Safari", "Firefox", "Edge"]
OS = {"ios": ["17.6", "18.1", "18.2"], "android": ["13", "14", "15"], "windows": ["10", "11"], "macos": ["14.6", "15.1"]}
MODELS = {"ios": ["iPhone 14", "iPhone 15", "iPad Air"], "android": ["Pixel 8", "Galaxy S24", "Galaxy A54"],
          "windows": ["Desktop"], "macos": ["MacBook Air", "iMac"]}
CARRIERS = ["DHL", "UPS", "PostNL", "FedEx", "DPD", "Royal Mail"]
MERCHANTS = ["Tidewell", "Coffee Corner", "City Grocers", "FuelPoint", "StreamFlix", "Book Nook", "Air Lines Co"]
GENRES = ["drama", "comedy", "documentary", "sci-fi", "kids", "news", "sports"]


def _iso(t):
    return datetime.datetime.fromtimestamp(t, datetime.timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _day(t):
    return datetime.datetime.fromtimestamp(t, datetime.timezone.utc).strftime("%Y-%m-%d")


def _hex(n):
    return uuid.uuid4().hex[:n]


# ---- what one event is made from: the same customer / product / device … throughout one event ----------
REF = {"customers": [(i, random.choice(list(CURRENCY) + ["NL", "DE", "FR"])) for i in range(1, 501)],
       "products": [(i, random.choice(list(NOUN)), round(random.uniform(5, 300), 2)) for i in range(1, 41)],
       "stores": ["AMS-01", "LON-01", "BER-01"]}          # event-sim swaps in Tidewell's (lab-db)
PER_ITEM = {"product", "line"}                             # a new one for each element of an array


class Ctx:
    def __init__(self, state, t, ref, cache=None):
        self.s, self.t, self.ref, self.cache = state, t, ref, cache if cache is not None else {}

    def one(self, name, make):
        if name not in self.cache:
            self.cache[name] = make(self)
        return self.cache[name]

    def pool(self, name, n, make):
        """Ids that come back across events (sessions, devices, sensors …) — a pool of n, renewed slowly."""
        p = self.s.setdefault(name, [])
        if len(p) < n or random.random() < 0.02:
            v = make(self)
            if len(p) < n:
                p.append(v)
            else:
                p[random.randrange(n)] = v
            return v
        return random.choice(p)

    def child(self):
        return Ctx(self.s, self.t, self.ref, {k: v for k, v in self.cache.items() if k not in PER_ITEM})


def _customer(c):
    cid, country = random.choice(c.ref["customers"])
    r = random.Random(f"customer{cid}")                    # the same customer always has the same name …
    first, last = r.choice(FIRST_NAMES), r.choice(LAST_NAMES)
    return {"id": cid, "country": country, "first": first, "last": last, "city": r.choice(CITIES.get(country, ["Capital"])),
            "email": f"{first}.{last}{cid}@example.com".lower().replace("ü", "u"), "segment": r.choice(["new", "regular", "regular", "vip"]),
            "signup": _day(1.6e9 + r.random() * 1.2e8), "birth_year": r.randint(1950, 2006), "points": r.randint(0, 5000),
            "member": r.random() < 0.4, "opt_in": r.random() < 0.6, "phone": f"+{r.randint(1, 99)} {r.randint(100000000, 999999999)}"}


def _product(c):
    pid, cat, price = random.choice(c.ref["products"])
    r = random.Random(f"product{pid}")
    noun = r.choice(NOUN.get(str(cat).lower(), ["Item"]))
    return {"id": pid, "category": cat, "price": price, "name": f"{r.choice(ADJ)} {noun}", "brand": r.choice(BRANDS),
            "sku": f"SKU-{pid:04d}", "color": r.choice(COLORS), "rating": round(r.uniform(2.5, 5), 1)}


def _device(c):
    os_ = random.choice(["ios", "ios", "android", "android", "windows", "macos"])
    return {"id": "d_" + _hex(10), "os": os_, "os_version": random.choice(OS[os_]), "model": random.choice(MODELS[os_]),
            "type": "mobile" if os_ in ("ios", "android") else "desktop", "app": f"{random.randint(3, 5)}.{random.randint(0, 9)}.{random.randint(0, 20)}"}


def _line(c):
    return {"qty": random.choices([1, 1, 1, 2, 2, 3, 5], k=1)[0]}


cust = lambda c: c.one("customer", _customer)             # noqa: E731
prod = lambda c: c.one("product", _product)               # noqa: E731
dev = lambda c: c.one("device", lambda c: c.pool("devices", 300, _device))   # noqa: E731
line = lambda c: c.one("line", _line)                     # noqa: E731
geo = lambda c: c.one("geo", lambda c: (lambda b: (round(b[0] + random.uniform(-2, 2), 5), round(b[1] + random.uniform(-2, 2), 5)))(  # noqa: E731
    LATLON.get(cust(c)["country"], (0.0, 0.0))))
amount = lambda c: c.one("amount", lambda c: round(random.lognormvariate(3.6, 0.9), 2))   # noqa: E731
pick = random.choice


def _f(typ, gen, label):
    return {"type": typ, "gen": gen, "label": label}


# ---- the library: group → field → (type, how it's made, what it is) ------------------------------
ONTOLOGY = {
    "event": ("Event", {
        "event_type": _f("string", lambda c: pick(["created", "updated", "deleted"]), "what happened — set your own values"),
        "source": _f("string", lambda c: pick(["web", "ios", "android", "pos", "backend"]), "the app that sent it"),
        "sequence": _f("long", lambda c: c.s.__setitem__("seq", c.s.get("seq", 0) + 1) or c.s["seq"], "a counter per stream"),
        "trace_id": _f("string", lambda c: _hex(32), "a tracing id"),
        "correlation_id": _f("string", lambda c: c.one("corr", lambda c: "c_" + _hex(12)), "ties events of one request"),
        "is_test": _f("boolean", lambda c: random.random() < 0.02, "a test event"),
        "producer_version": _f("string", lambda c: pick(["1.4.2", "1.4.3", "1.5.0"]), "the sending app's version"),
        "environment": _f("string", lambda c: pick(["prod", "prod", "prod", "staging"]), "prod or staging"),
    }),
    "customer": ("Customer", {
        "customer_id": _f("long", lambda c: cust(c)["id"], "Tidewell's customer"),
        "first_name": _f("string", lambda c: cust(c)["first"], "first name"),
        "last_name": _f("string", lambda c: cust(c)["last"], "last name"),
        "full_name": _f("string", lambda c: cust(c)["first"] + " " + cust(c)["last"], "first + last"),
        "email": _f("string", lambda c: cust(c)["email"], "e-mail (example.com)"),
        "phone": _f("string", lambda c: cust(c)["phone"], "phone number (made up)"),
        "segment": _f("string", lambda c: cust(c)["segment"], "new / regular / vip"),
        "signup_date": _f("date", lambda c: cust(c)["signup"], "when they signed up"),
        "birth_year": _f("long", lambda c: cust(c)["birth_year"], "year of birth"),
        "loyalty_points": _f("long", lambda c: cust(c)["points"], "points balance"),
        "is_member": _f("boolean", lambda c: cust(c)["member"], "in the loyalty programme"),
        "marketing_opt_in": _f("boolean", lambda c: cust(c)["opt_in"], "may get marketing"),
        "language": _f("string", lambda c: LANG.get(cust(c)["country"], "en"), "language code"),
        "customer_country": _f("string", lambda c: cust(c)["country"], "their country code"),
    }),
    "geo": ("Location", {
        "country": _f("string", lambda c: cust(c)["country"], "country code"),
        "city": _f("string", lambda c: cust(c)["city"], "city"),
        "region": _f("string", lambda c: pick(["north", "south", "east", "west", "central"]), "region"),
        "postal_code": _f("string", lambda c: f"{random.randint(1000, 9999)}{pick(['AB', 'CD', 'XY', ''])}", "postal code"),
        "latitude": _f("double", lambda c: geo(c)[0], "latitude"),
        "longitude": _f("double", lambda c: geo(c)[1], "longitude"),
        "address_line": _f("string", lambda c: f"{random.randint(1, 250)} {pick(['Main', 'Market', 'Station', 'Park', 'Canal'])} {pick(['Street', 'Road', 'Lane'])}", "street address"),
        "timezone": _f("string", lambda c: TZ.get(cust(c)["country"], "UTC"), "time zone"),
        "ip": _f("string", lambda c: f"{random.randint(11, 223)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 254)}", "IP address"),
    }),
    "web": ("Web & app", {
        "session_id": _f("string", lambda c: c.one("session", lambda c: c.pool("sessions", 200, lambda c: "s_" + _hex(12))), "a visit"),
        "page": _f("string", lambda c: pick(PAGES + [f"/product/{prod(c)['id']}"] * 3), "page path"),
        "page_title": _f("string", lambda c: pick(["Home", "Deals", "Your cart", "Checkout", prod(c)["name"]]), "page title"),
        "url": _f("string", lambda c: "https://shop.example.com" + pick(PAGES), "full URL"),
        "referrer": _f("string", lambda c: pick(REFERRERS), "where they came from"),
        "utm_source": _f("string", lambda c: pick(["google", "newsletter", "instagram", "tiktok"]), "campaign source"),
        "utm_medium": _f("string", lambda c: pick(["cpc", "email", "social", "organic"]), "campaign medium"),
        "utm_campaign": _f("string", lambda c: pick(["black_friday", "spring_sale", "welcome", "retarget"]), "campaign name"),
        "search_term": _f("string", lambda c: pick(SEARCH), "what they searched"),
        "browser": _f("string", lambda c: pick(BROWSERS), "browser"),
        "user_agent": _f("string", lambda c: f"Mozilla/5.0 ({dev(c)['os']}) {pick(BROWSERS)}/{random.randint(100, 130)}.0", "user agent"),
        "screen_width": _f("long", lambda c: pick([390, 414, 768, 1280, 1440, 1920]), "pixels"),
        "duration_ms": _f("long", lambda c: int(random.expovariate(1 / 4000)), "time on the page"),
        "scroll_pct": _f("long", lambda c: random.randint(0, 100), "how far they scrolled"),
        "is_bot": _f("boolean", lambda c: random.random() < 0.03, "a crawler"),
        "ab_variant": _f("string", lambda c: pick(["A", "B"]), "A/B test group"),
    }),
    "device": ("Device", {
        "device_id": _f("string", lambda c: dev(c)["id"], "the phone / computer"),
        "device_type": _f("string", lambda c: dev(c)["type"], "mobile / desktop"),
        "os": _f("string", lambda c: dev(c)["os"], "operating system"),
        "os_version": _f("string", lambda c: dev(c)["os_version"], "its version"),
        "model": _f("string", lambda c: dev(c)["model"], "device model"),
        "app_version": _f("string", lambda c: dev(c)["app"], "the app's version"),
        "network": _f("string", lambda c: pick(["wifi", "wifi", "4g", "5g", "offline"]), "connection"),
        "battery_pct": _f("long", lambda c: random.randint(3, 100), "battery left"),
        "locale": _f("string", lambda c: LANG.get(cust(c)["country"], "en") + "-" + cust(c)["country"], "language-country"),
    }),
    "product": ("Product", {
        "product_id": _f("long", lambda c: prod(c)["id"], "Tidewell's product"),
        "product_name": _f("string", lambda c: prod(c)["name"], "name"),
        "sku": _f("string", lambda c: prod(c)["sku"], "stock keeping unit"),
        "category": _f("string", lambda c: prod(c)["category"], "category"),
        "brand": _f("string", lambda c: prod(c)["brand"], "brand"),
        "price": _f("double", lambda c: prod(c)["price"], "price"),
        "list_price": _f("double", lambda c: round(prod(c)["price"] * 1.2, 2), "price before discount"),
        "discount_pct": _f("long", lambda c: pick([0, 0, 0, 10, 15, 25]), "discount"),
        "color": _f("string", lambda c: prod(c)["color"], "colour"),
        "size": _f("string", lambda c: pick(["XS", "S", "M", "L", "XL"]), "size"),
        "rating": _f("double", lambda c: prod(c)["rating"], "average rating"),
        "in_stock": _f("boolean", lambda c: random.random() < 0.9, "available"),
    }),
    "order": ("Order", {
        "order_id": _f("string", lambda c: c.one("order", lambda c: "o_" + _hex(12)), "the order"),
        "order_status": _f("string", lambda c: pick(["placed", "paid", "shipped", "delivered", "cancelled"]), "status"),
        "quantity": _f("long", lambda c: line(c)["qty"], "how many (per line)"),
        "unit_price": _f("double", lambda c: prod(c)["price"], "price of one"),
        "line_total": _f("double", lambda c: round(line(c)["qty"] * prod(c)["price"], 2), "quantity × unit price"),
        "subtotal": _f("double", lambda c: amount(c), "before tax and shipping"),
        "tax": _f("double", lambda c: round(amount(c) * 0.21, 2), "tax"),
        "shipping_cost": _f("double", lambda c: pick([0.0, 0.0, 3.95, 4.95, 9.99]), "shipping"),
        "order_total": _f("double", lambda c: round(amount(c) * 1.21, 2), "what they paid"),
        "currency": _f("string", lambda c: CURRENCY.get(cust(c)["country"], "EUR"), "currency code"),
        "coupon_code": _f("string", lambda c: pick([None, None, None, "WELCOME10", "SPRING25", "FREESHIP"]), "coupon"),
        "channel": _f("string", lambda c: pick(["web", "app", "marketplace", "store"]), "sales channel"),
        "item_count": _f("long", lambda c: random.randint(1, 6), "items in the order"),
    }),
    "payment": ("Payment", {
        "payment_id": _f("string", lambda c: c.one("payment", lambda c: "p_" + _hex(12)), "the payment"),
        "amount": _f("double", lambda c: amount(c), "amount"),
        "payment_method": _f("string", lambda c: pick(["card", "card", "paypal", "ideal", "apple_pay", "klarna"]), "how they paid"),
        "card_brand": _f("string", lambda c: pick(["visa", "mastercard", "amex"]), "card brand"),
        "card_last4": _f("string", lambda c: f"{random.randint(0, 9999):04d}", "last 4 digits (made up)"),
        "payment_status": _f("string", lambda c: pick(["authorized", "captured", "captured", "failed", "refunded"]), "status"),
        "failure_reason": _f("string", lambda c: pick([None, None, "insufficient_funds", "card_expired", "3ds_failed"]), "why it failed"),
        "provider": _f("string", lambda c: pick(["stripe", "adyen", "mollie", "paypal"]), "payment provider"),
        "installments": _f("long", lambda c: pick([1, 1, 1, 3, 6]), "number of instalments"),
    }),
    "shipping": ("Shipping", {
        "shipment_id": _f("string", lambda c: c.one("shipment", lambda c: "sh_" + _hex(10)), "the parcel"),
        "carrier": _f("string", lambda c: pick(CARRIERS), "carrier"),
        "tracking_number": _f("string", lambda c: _hex(14).upper(), "tracking number"),
        "warehouse": _f("string", lambda c: pick(["WH-AMS", "WH-LON", "WH-BER", "WH-NYC"]), "warehouse"),
        "shipping_status": _f("string", lambda c: pick(["label_created", "picked_up", "in_transit", "out_for_delivery", "delivered"]), "status"),
        "eta": _f("timestamptz", lambda c: _iso(c.t + random.uniform(3600, 5 * 86400)), "expected delivery"),
        "weight_kg": _f("double", lambda c: round(random.uniform(0.1, 25), 2), "weight"),
        "delivery_type": _f("string", lambda c: pick(["standard", "express", "pickup_point"]), "delivery type"),
    }),
    "stock": ("Stock", {
        "store": _f("string", lambda c: c.one("store", lambda c: pick(c.ref["stores"])), "Tidewell's store"),
        "qty_change": _f("long", lambda c: pick([-3, -2, -1, -1, -1, 1, 2, 24, 48]), "stock + / −"),
        "stock_level": _f("long", lambda c: random.randint(0, 400), "on hand after"),
        "reason": _f("string", lambda c: pick(["sale", "delivery", "return", "damage", "count"]), "why it changed"),
        "bin_location": _f("string", lambda c: f"{pick('ABCDE')}{random.randint(1, 30):02d}-{random.randint(1, 6)}", "shelf"),
        "reorder_point": _f("long", lambda c: pick([10, 20, 50]), "reorder when below"),
    }),
    "iot": ("IoT sensor", {
        "sensor_id": _f("string", lambda c: c.one("sensor", lambda c: c.pool("sensors", 50, lambda c: "sn_" + _hex(8))), "the sensor"),
        "temperature_c": _f("double", lambda c: round(random.gauss(21, 4), 2), "temperature"),
        "humidity_pct": _f("double", lambda c: round(random.uniform(25, 80), 1), "humidity"),
        "pressure_hpa": _f("double", lambda c: round(random.gauss(1013, 8), 1), "air pressure"),
        "co2_ppm": _f("long", lambda c: int(random.gauss(650, 180)), "CO₂"),
        "vibration_mm_s": _f("double", lambda c: round(abs(random.gauss(2, 1.5)), 3), "vibration"),
        "sensor_battery_pct": _f("long", lambda c: random.randint(5, 100), "battery"),
        "firmware": _f("string", lambda c: pick(["2.1.0", "2.1.3", "2.2.0"]), "firmware version"),
        "sensor_status": _f("string", lambda c: pick(["ok", "ok", "ok", "warning", "error"]), "status"),
    }),
    "finance": ("Finance", {
        "account_id": _f("string", lambda c: c.one("account", lambda c: c.pool("accounts", 300, lambda c: "acc_" + _hex(8))), "bank account"),
        "transaction_id": _f("string", lambda c: c.one("txn", lambda c: "tx_" + _hex(14)), "the transaction"),
        "transaction_type": _f("string", lambda c: pick(["card_payment", "transfer", "atm", "direct_debit", "refund"]), "type"),
        "merchant": _f("string", lambda c: pick(MERCHANTS), "merchant"),
        "mcc": _f("string", lambda c: pick(["5411", "5812", "5541", "4899", "5942", "4511"]), "merchant category code"),
        "balance_after": _f("double", lambda c: round(random.uniform(-500, 12000), 2), "balance after"),
        "risk_score": _f("double", lambda c: round(random.betavariate(1.5, 8), 3), "fraud risk 0–1"),
        "is_fraud": _f("boolean", lambda c: random.random() < 0.01, "flagged as fraud"),
    }),
    "support": ("Support & feedback", {
        "ticket_id": _f("string", lambda c: c.one("ticket", lambda c: "t_" + _hex(8)), "support ticket"),
        "priority": _f("string", lambda c: pick(["low", "normal", "normal", "high", "urgent"]), "priority"),
        "topic": _f("string", lambda c: pick(["delivery", "refund", "account", "product", "payment"]), "about"),
        "sentiment": _f("string", lambda c: pick(["positive", "neutral", "negative"]), "sentiment"),
        "nps": _f("long", lambda c: random.choices(range(11), [2, 1, 1, 2, 2, 4, 5, 8, 12, 10, 8])[0], "0–10"),
        "stars": _f("long", lambda c: random.choices([1, 2, 3, 4, 5], [5, 5, 12, 30, 48])[0], "review stars"),
        "comment": _f("string", lambda c: pick(["Great, thanks!", "Arrived late.", "Works as expected", "Not what I ordered", "Fast delivery", None]), "free text"),
        "agent_id": _f("string", lambda c: f"ag_{random.randint(1, 40):03d}", "support agent"),
    }),
    "media": ("Media & content", {
        "content_id": _f("string", lambda c: c.one("content", lambda c: f"ct_{random.randint(1, 500):04d}"), "video / article"),
        "genre": _f("string", lambda c: pick(GENRES), "genre"),
        "watch_seconds": _f("long", lambda c: int(random.expovariate(1 / 600)), "time watched"),
        "position_seconds": _f("long", lambda c: random.randint(0, 5400), "where in the video"),
        "bitrate_kbps": _f("long", lambda c: pick([800, 1500, 3000, 6000]), "stream quality"),
        "quality": _f("string", lambda c: pick(["480p", "720p", "1080p", "4k"]), "resolution"),
        "buffering_ms": _f("long", lambda c: int(random.expovariate(1 / 300)), "waiting for data"),
        "subscription": _f("string", lambda c: pick(["free", "basic", "premium"]), "plan"),
    }),
    "ids": ("Ids & time", {
        "uuid": _f("string", lambda c: str(uuid.uuid4()), "a random UUID"),
        "request_id": _f("string", lambda c: c.one("request", lambda c: "req_" + _hex(16)), "the HTTP request"),
        "tenant_id": _f("string", lambda c: pick(["t_eu", "t_us", "t_apac"]), "the tenant"),
        "user_id": _f("string", lambda c: f"u_{cust(c)['id']:05d}", "app user (= the customer)"),
        "occurred_at": _f("timestamptz", lambda c: _iso(c.t), "a second timestamp (= event_time)"),
        "event_date": _f("date", lambda c: _day(c.t), "the day it happened"),
        "hour_of_day": _f("long", lambda c: datetime.datetime.fromtimestamp(c.t, datetime.timezone.utc).hour, "0–23 (UTC)"),
        "weekday": _f("string", lambda c: datetime.datetime.fromtimestamp(c.t, datetime.timezone.utc).strftime("%A"), "Monday …"),
        "tags": _f("string", lambda c: pick(["new", "promo", "gift", "bulk", "b2b"]), "a label (repeat it for a list)"),
        "note": _f("string", lambda c: pick(["", "call back", "fragile", "leave at door", None]), "free text"),
    }),
}
LIB = {f"{g}.{n}": f for g, (_, fs) in ONTOLOGY.items() for n, f in fs.items()}
TEMPLATES = {                                              # a start, to change
    "signups": {"key": "user_id", "fields": [
        {"name": "event_type", "field": "event.event_type", "values": ["signup", "login", "logout", "password_reset"]},
        {"name": "user_id", "field": "ids.user_id"}, {"name": "email", "field": "customer.email"},
        {"name": "device", "object": [{"name": "os", "field": "device.os"}, {"name": "app_version", "field": "device.app_version"}]},
        {"name": "country", "field": "geo.country"}]},
    "sensor_readings": {"key": "sensor_id", "fields": [
        {"name": "sensor_id", "field": "iot.sensor_id"}, {"name": "temperature_c", "field": "iot.temperature_c"},
        {"name": "humidity_pct", "field": "iot.humidity_pct", "null_pct": 5},
        {"name": "location", "object": [{"name": "city", "field": "geo.city"}, {"name": "lat", "field": "geo.latitude"},
                                        {"name": "lon", "field": "geo.longitude"}]},
        {"name": "status", "field": "iot.sensor_status"}]},
    "carts": {"key": "session_id", "fields": [
        {"name": "event_type", "field": "event.event_type", "values": ["cart_updated", "checkout_started", "cart_abandoned"]},
        {"name": "session_id", "field": "web.session_id"}, {"name": "customer_id", "field": "customer.customer_id", "null_pct": 30},
        {"name": "items", "repeat": [1, 5], "object": [{"name": "sku", "field": "product.sku"},
                                                       {"name": "qty", "field": "order.quantity"},
                                                       {"name": "price", "field": "product.price"}]},
        {"name": "currency", "field": "order.currency"}]},
}


class SchemaError(ValueError):
    pass


# ---- checking a member's schema ----------------------------------------------------------------
def validate(schema):
    """A clean copy of the schema (ids given to new nodes), or SchemaError saying what's wrong."""
    if not isinstance(schema, dict) or not isinstance(schema.get("fields"), list) or not schema["fields"]:
        raise SchemaError("add at least one field")
    count = [0]

    def node(n, depth, where):
        if not isinstance(n, dict):
            raise SchemaError(f"{where}: not a field")
        name = str(n.get("name") or "").strip()
        if not NAME.fullmatch(name):
            raise SchemaError(f"{where}{name or '(no name)'}: names are lowercase letters, digits and _, starting with a letter")
        count[0] += 1
        out = {"id": str(n.get("id") or _hex(6))[:12], "name": name}
        if "object" in n:
            if depth >= MAX_DEPTH:
                raise SchemaError(f"{where}{name}: objects nest {MAX_DEPTH} levels deep at most")
            out["object"] = nodes(n["object"], depth + 1, f"{where}{name}.")
        else:
            f = LIB.get(n.get("field"))
            if not f:
                raise SchemaError(f"{where}{name}: pick a field from the library")
            out["field"] = n["field"]
            if n.get("values"):
                vals = [str(v)[:40] for v in n["values"] if str(v).strip()][:MAX_VALUES]
                if f["type"] != "string":
                    raise SchemaError(f"{where}{name}: your own values are for text fields")
                if vals:
                    out["values"] = vals
            if n.get("as"):
                if n["as"] not in AS or f["type"] not in AS[n["as"]]:
                    raise SchemaError(f"{where}{name}: a {f['type']} can't be sent as {n['as']}")
                if n["as"] != f["type"]:
                    out["as"] = n["as"]
        if n.get("repeat"):
            try:
                lo, hi = int(n["repeat"][0]), int(n["repeat"][1])
            except (TypeError, ValueError, IndexError):
                raise SchemaError(f"{where}{name}: a list needs how many — from and to")
            if not 0 <= lo <= hi <= MAX_REPEAT or hi < 1:
                raise SchemaError(f"{where}{name}: a list has 0–{MAX_REPEAT} elements")
            out["repeat"] = [lo, hi]
        pct = int(n.get("null_pct") or 0)
        if not 0 <= pct <= 90:
            raise SchemaError(f"{where}{name}: empty 0–90 % of the time")
        if pct:
            out["null_pct"] = pct
        return out

    def nodes(ns, depth, where):
        if not isinstance(ns, list) or not ns:
            raise SchemaError(f"{where.rstrip('.') or 'the event'}: an object needs at least one field")
        out, seen = [], set()
        for n in ns:
            o = node(n, depth, where)
            if o["name"] in seen or (depth == 1 and o["name"] in FIXED):
                raise SchemaError(f"{where}{o['name']}: used twice" + (" (every event has it already)" if o["name"] in FIXED else ""))
            seen.add(o["name"])
            out.append(o)
        return out

    fields = nodes(schema["fields"], 1, "")
    if count[0] > MAX_FIELDS:
        raise SchemaError(f"{count[0]} fields — {MAX_FIELDS} at most")
    key = str(schema.get("key") or "")
    if key and not any(f["name"] == key and "field" in f and "repeat" not in f for f in fields):
        raise SchemaError(f"key {key}: pick a field at the top, not an object or a list")
    return {"key": key, "fields": fields}


# ---- making events -------------------------------------------------------------------------------
def _cast(v, typ):
    if v is None:
        return None
    if typ == "string":
        return ("true" if v else "false") if isinstance(v, bool) else str(v)
    if typ == "long":
        return int(round(float(v)))
    return float(v)


def _value(n, c):
    if n.get("null_pct") and random.random() * 100 < n["null_pct"]:
        return None
    if n.get("repeat"):
        k = random.randint(*n["repeat"])
        one = dict(n, repeat=None, null_pct=0)
        return [_value(one, c.child()) for _ in range(k)]
    if "object" in n:
        return {x["name"]: _value(x, c) for x in n["object"]}
    v = random.choice(n["values"]) if n.get("values") else LIB[n["field"]]["gen"](c)
    return _cast(v, n["as"]) if n.get("as") else v


def event(schema, revision, t, state, ref=None):
    """(key, event without publish_time) — the shape of `schema`, at event time t."""
    c = Ctx(state, t, ref or REF)
    e = {"event_id": uuid.uuid4().hex, "event_time": _iso(t), "schema_revision": revision}
    for n in schema["fields"]:
        e[n["name"]] = _value(n, c)
    k = e.get(schema.get("key")) if schema.get("key") else None
    return str(k if k is not None else e["event_id"]), e


def sample(schema, n=3):
    import time
    s = validate(schema)
    state = {}
    return [event(s, 1, time.time() - i * 2, state)[1] for i in range(n)]


# ---- its table and its Spark schema ------------------------------------------------------------
def _type(n):
    if "object" in n:
        t = ("struct", [(x["name"], _type(x)) for x in n["object"]])
    else:
        t = n.get("as") or LIB[n["field"]]["type"]
    return ("list", t) if n.get("repeat") else t


def columns(schema):
    """[(name, type)] in labpubsub.COLUMNS' form (event_id, event_time, publish_time come first)."""
    return [("schema_revision", "long")] + [(n["name"], _type(n)) for n in schema["fields"]]


def spark_type(t):
    if isinstance(t, str):
        return {"string": "STRING", "long": "BIGINT", "double": "DOUBLE", "boolean": "BOOLEAN", "timestamptz": "TIMESTAMP",
                "date": "DATE", "int": "INT"}[t]
    if t[0] == "list":
        return f"ARRAY<{spark_type(t[1])}>"
    return "STRUCT<" + ", ".join(f"{n}: {spark_type(x)}" for n, x in t[1]) + ">"


# ---- what changed between two revisions --------------------------------------------------------
WORDS = {"string": "text", "long": "a whole number", "double": "a decimal number", "boolean": "true / false",
         "timestamptz": "a timestamp", "date": "a date"}


def _shapes(fields, path=""):
    """{path: shape} — scalar <type> / list of … / object — what a table's column at that path is."""
    out = {}
    for n in fields:
        p = path + n["name"]
        if "object" in n:
            out[p] = "a list of objects" if n.get("repeat") else "an object"
            out.update(_shapes(n["object"], p + "[]." if n.get("repeat") else p + "."))
        else:
            t = WORDS[LIB[n["field"]]["type"]]
            out[p] = f"a list of {t}" if n.get("repeat") else t
    return out


def compatible(history, new):
    """Problems (text) if `new` uses a name with another shape or type than an earlier revision did."""
    seen = {}
    for rev, s in history:
        for p, shape in _shapes(s["fields"]).items():
            seen.setdefault(p, (shape, rev))
    out = []
    for p, shape in _shapes(new["fields"]).items():
        if p in seen and seen[p][0] != shape:
            was, rev = seen[p]
            out.append(f"{p.replace('[]', '')} was {was} in revision {rev} — it can't become {shape}: a table column keeps its type. "
                       "Give it a new name (the old one simply stops coming)")
    return out


def diff(old, new):
    """[{"change": added|removed|renamed|type|list|values|empty|key, "path", "detail"}] — by node id."""
    out = []

    def flat(ns, path=""):
        d = {}
        for n in ns:
            p = path + n["name"]
            d[n["id"]] = (p, n)
            if "object" in n:
                d.update(flat(n["object"], p + "."))
        return d

    def shape(n):
        t = _type(n)
        return json.dumps(t[1] if n.get("repeat") else t)

    a, b = flat(old["fields"]), flat(new["fields"])
    for i, (p, n) in b.items():
        if i not in a:
            out.append({"change": "added", "path": p, "detail": "object" if "object" in n else LIB[n["field"]]["type"] if "field" in n else ""})
            continue
        op, o = a[i]
        if op != p:
            out.append({"change": "renamed", "path": p, "detail": f"was {op}"})
        if "object" not in n and "object" not in o and shape(o) != shape(n):
            out.append({"change": "type", "path": p, "detail": f"{_type(o)} → {_type(n)}"})
        elif ("object" in n) != ("object" in o):
            out.append({"change": "type", "path": p, "detail": "a field ↔ an object"})
        if bool(o.get("repeat")) != bool(n.get("repeat")):
            out.append({"change": "list", "path": p, "detail": "now a list" if n.get("repeat") else "no longer a list"})
        if o.get("values") != n.get("values") and ("values" in o or "values" in n):
            out.append({"change": "values", "path": p, "detail": ", ".join(n.get("values") or ["(the library's)"])})
        if (o.get("null_pct") or 0) != (n.get("null_pct") or 0):
            out.append({"change": "empty", "path": p, "detail": f"{o.get('null_pct') or 0}% → {n.get('null_pct') or 0}% empty"})
    for i, (p, n) in a.items():
        if i not in b:
            out.append({"change": "removed", "path": p, "detail": ""})
    gone = {x["path"]: x for x in out if x["change"] == "removed"}
    for x in [x for x in out if x["change"] == "added" and x["path"] in gone]:   # the same name, made anew
        out.remove(gone[x["path"]])
        x.update(change="type", detail=f"made anew as {x['detail'] or 'a field'}")
    if (old.get("key") or "") != (new.get("key") or ""):
        out.append({"change": "key", "path": new.get("key") or "event_id", "detail": f"was {old.get('key') or 'event_id'}"})
    return out


def library():
    """The library for the page: [{group, label, fields: [{field, name, type, label}]}] + the templates."""
    return {"groups": [{"group": g, "label": lbl, "fields": [{"field": f"{g}.{n}", "name": n, "type": f["type"], "label": f["label"]}
                                                           for n, f in fs.items()]} for g, (lbl, fs) in ONTOLOGY.items()],
            "templates": TEMPLATES, "fixed": FIXED, "as": {k: list(v) for k, v in AS.items()},
            "limits": {"fields": MAX_FIELDS, "depth": MAX_DEPTH, "repeat": MAX_REPEAT, "values": MAX_VALUES}}
