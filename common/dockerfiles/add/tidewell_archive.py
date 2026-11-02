"""tidewell_archive — Tidewell's history: two years of its three systems (web shop, store tills,
loyalty card) as the old systems exported them, for Capstone 3's historical load.

  python tidewell_archive.py tidewell-history.zip      the download on the Capstone 3 page

The same rows seed the live databases (cdc-sim): they start from the archive's last days, with the
same ids, so the history and the live change stream overlap — as they do in a real migration.
Deterministic (a fixed seed): every run makes the same archive.

Layout of the zip (CSV, header row, UTF-8):
  web/customers.csv  web/products.csv  web/orders/YYYY-MM.csv  web/order_items/YYYY-MM.csv
  store/stores.csv   store/items.csv   store/sales/YYYY-MM.csv store/sale_lines/YYYY-MM.csv
  loyalty/members.csv                  loyalty/points/YYYY-MM.csv
Like the live systems: the tills write local time without a time zone and money in cents; cancelled
web orders are gone (their points reversals are not); members' emails are typed as the clerk typed them.
"""
import csv
import datetime as dt
import io
import random
import sys
import zipfile

SEED = 20261031
START, END = dt.date(2024, 11, 1), dt.date(2026, 10, 31)           # inclusive; END = the cut-off day
UTC = dt.timezone.utc

FIRST = ["Asha", "Ben", "Chen", "Dara", "Eli", "Fatima", "Goran", "Hana", "Ivan", "Jia", "Kofi", "Lena", "Mateo",
         "Nora", "Omar", "Priya", "Quinn", "Ravi", "Sara", "Tomas", "Uma", "Vik", "Wen", "Yara", "Zoe"]
LAST = ["Smith", "Garcia", "Khan", "Li", "Okafor", "Novak", "Rossi", "Silva", "Tanaka", "Weber", "Patel", "Nguyen"]
COUNTRIES = ["US", "UK", "DE", "IN", "FR", "NL", "SG", "BR", "JP", "AU"]
TIERS = ["bronze", "silver", "gold"]
CARD_TIERS = ["blue", "silver", "gold"]
PRODUCTS = [("Laptop", "electronics", 899), ("Headphones", "electronics", 129), ("Phone case", "accessories", 19),
            ("Desk lamp", "home", 39), ("Coffee mug", "home", 12), ("Backpack", "accessories", 59),
            ("Monitor", "electronics", 249), ("Keyboard", "electronics", 79), ("Notebook", "office", 6),
            ("Water bottle", "accessories", 18), ("Office chair", "home", 189), ("USB cable", "electronics", 9),
            ("Plant pot", "home", 15), ("Pen set", "office", 11), ("Running shoes", "sports", 99),
            ("Yoga mat", "sports", 35), ("Tea sampler", "food", 22), ("Chocolate box", "food", 14)]
STORES = [("NYC-01", "New York", "US", "America/New_York", "2018-09-01"), ("AMS-01", "Amsterdam", "NL", "Europe/Amsterdam", "2019-03-01"),
          ("LON-01", "London", "UK", "Europe/London", "2020-06-15"), ("BLR-01", "Bengaluru", "IN", "Asia/Kolkata", "2021-01-10"),
          ("SIN-01", "Singapore", "SG", "Asia/Singapore", "2022-04-20")]
FLOW = ["placed", "paid", "shipped", "delivered"]


def sku(product_id):
    return f"SKU-{product_id:04d}"


def spelled(rnd, email):
    """The same email as another system types it."""
    return rnd.choice([email, email, email.upper(), email.title(), f" {email}", f"{email} "])


def _local(t_utc, tz):
    from zoneinfo import ZoneInfo
    return t_utc.astimezone(ZoneInfo(tz)).replace(tzinfo=None)


def _seasonal(day):
    f = 1.6 if day.month in (11, 12) else 1.0                       # the holiday season
    return f * (1.25 if day.weekday() >= 5 else 1.0)                 # weekends


def build():
    """All tables, as lists of dicts — the systems as they were at the end of the cut-off day."""
    rnd = random.Random(SEED)
    T = {k: [] for k in ("customers", "products", "orders", "order_items", "stores", "items", "sales", "sale_lines",
                         "members", "points")}
    t0 = dt.datetime.combine(START, dt.time(), UTC)

    def at(day):
        return dt.datetime.combine(day, dt.time(), UTC) + dt.timedelta(seconds=rnd.randint(0, 86399))

    # products: one catalogue, two price lists that drift apart
    web_price, store_cents, price_at = {}, {}, {}
    for i, (name, cat, price) in enumerate(PRODUCTS, 1):
        web_price[i], store_cents[i] = float(price), int(price * 100)
        T["products"].append({"product_id": i, "name": name, "category": cat, "price": None, "updated_at": t0})
        T["items"].append({"sku": sku(i), "name": name.upper(), "category": cat, "price_cents": None, "updated_at": t0})
    for i, (code, city, country, tz, opened) in enumerate(STORES, 1):
        T["stores"].append({"store_id": i, "code": code, "city": city, "country": country, "timezone": tz, "opened_on": opened})

    members, by_email = [], {}                                          # member rows; cleaned email → member

    def add_member(name, email, joined, at_till):
        m = {"member_id": len(members) + 1, "card_number": f"LC{rnd.randint(10_000_000, 99_999_999)}",
             "full_name": name, "email": email, "phone": rnd.choice([None, f"+{rnd.randint(1, 99)} {rnd.randint(100_000_000, 999_999_999)}"]),
             "tier": rnd.choices(CARD_TIERS, [6, 3, 1])[0], "points_balance": 0, "joined_at": joined, "updated_at": joined}
        members.append(m)
        if email:
            by_email.setdefault(email.strip().lower(), m)
        if at_till:
            earn(m, "bonus", "welcome", 50, joined)
        return m

    def earn(m, channel, ref, pts, when):
        if not m or not pts:
            return
        T["points"].append({"txn_id": len(T["points"]) + 1, "member_id": m["member_id"], "channel": channel, "ref": ref,
                            "points": pts, "created_at": when})
        m["points_balance"] += pts
        m["updated_at"] = max(m["updated_at"], when)

    def new_customer(created):
        f, l = rnd.choice(FIRST), rnd.choice(LAST)
        email = f"{f.lower()}.{l.lower()}{rnd.randint(1, 9999)}@example.com"
        c = {"customer_id": len(T["customers"]) + 1, "name": f"{f} {l}", "email": email, "country": rnd.choice(COUNTRIES),
             "tier": rnd.choices(TIERS, [6, 3, 1])[0], "created_at": created, "updated_at": created}
        T["customers"].append(c)
        if rnd.random() < 0.4:
            add_member(c["name"], spelled(rnd, email), created, False)
        return c

    for _ in range(300):                                                # customers from before the archive
        new_customer(t0 - dt.timedelta(days=rnd.randint(1, 900), seconds=rnd.randint(0, 86399)))
    for _ in range(80):
        f, l = rnd.choice(FIRST), rnd.choice(LAST)
        add_member(f"{f} {l}", rnd.choice([f"{f.lower()}{l.lower()}{rnd.randint(1, 99)}@mail.test", None]),
                   t0 - dt.timedelta(days=rnd.randint(1, 900)), False)

    day, order_id, item_rows, sale_id = START, 0, [], 0
    while day <= END:
        s = _seasonal(day)
        if day.day == 1:                                                # prices move now and then
            for i in rnd.sample(range(1, 19), 3):
                web_price[i] = round(web_price[i] * rnd.choice([0.9, 0.95, 1.05, 1.1]), 2)
                T["products"][i - 1]["updated_at"] = at(day)
            for i in rnd.sample(range(1, 19), 2):
                store_cents[i] = int(store_cents[i] * rnd.choice([0.9, 0.95, 1.05, 1.1]))
                T["items"][i - 1]["updated_at"] = at(day)
        for _ in range(rnd.choice([1, 1, 2, 2, 3])):
            new_customer(at(day))
        if rnd.random() < 0.5:
            f, l = rnd.choice(FIRST), rnd.choice(LAST)
            add_member(f"{f} {l}", rnd.choice([f"{f.lower()}{l.lower()}{rnd.randint(1, 99)}@mail.test", None]), at(day), True)
        if rnd.random() < 0.05:                                         # a tier changes
            c = rnd.choice(T["customers"])
            c["tier"], c["updated_at"] = rnd.choice(TIERS), at(day)
        # web orders
        for _ in range(int(rnd.gauss(18, 4) * s)):
            c = rnd.choice(T["customers"])
            created = at(day)
            if created < c["created_at"]:
                created = c["created_at"] + dt.timedelta(minutes=5)
            order_id += 1
            lines, total = [], 0.0
            for line, pid in enumerate(rnd.sample(range(1, 19), rnd.randint(1, 3)), 1):
                q = rnd.randint(1, 3)
                lines.append({"order_id": order_id, "line": line, "product_id": pid, "quantity": q, "price": web_price[pid]})
                total += q * web_price[pid]
            total = round(total, 2)
            m = by_email.get(c["email"])
            earn(m, "web", f"order:{order_id}", int(total), created)
            if rnd.random() < 0.05:                                     # cancelled: the order is gone, the reversal stays
                earn(m, "web", f"order:{order_id}:cancelled", -int(total), created + dt.timedelta(hours=rnd.randint(1, 30)))
                continue
            age = (END - day).days
            steps = min(3, age if age < 3 else 3)                       # recent orders haven't arrived yet
            status, updated = FLOW[steps], created + dt.timedelta(hours=steps * rnd.randint(8, 40))
            T["orders"].append({"order_id": order_id, "customer_id": c["customer_id"], "status": status, "total": total,
                                "created_at": created, "updated_at": updated})
            item_rows.extend(lines)
        # till sales
        for st in T["stores"]:
            for _ in range(int(rnd.gauss(9, 3) * s)):
                sale_id += 1
                sold = at(day)
                ret = rnd.random() < 0.03
                card = rnd.choice(members)["card_number"] if rnd.random() < 0.5 else None
                total = 0
                for line, pid in enumerate(rnd.sample(range(1, 19), 1 if ret else rnd.randint(1, 4)), 1):
                    q = -1 if ret else rnd.randint(1, 3)
                    T["sale_lines"].append({"sale_id": sale_id, "line": line, "sku": sku(pid), "qty": q,
                                            "unit_price_cents": store_cents[pid]})
                    total += q * store_cents[pid]
                T["sales"].append({"sale_id": sale_id, "store_id": st["store_id"], "sold_at": _local(sold, st["timezone"]),
                                   "card_number": card, "payment": rnd.choices(["card", "cash", "mobile"], [6, 2, 2])[0],
                                   "total_cents": total, "_utc": sold})
                if card:
                    earn(_member_by_card(members, card), "store", f"sale:{sale_id}", total // 100, sold)
        # redemptions
        for _ in range(rnd.randint(1, 4)):
            m = rnd.choice(members)
            if m["points_balance"] >= 100:
                earn(m, "redeem", None, -rnd.choice([100, 200, 500] if m["points_balance"] >= 500 else [100]), at(day))
        day += dt.timedelta(days=1)

    T["order_items"] = item_rows
    T["members"] = members
    T["_next"] = {"order_id": order_id + 1, "sale_id": sale_id + 1, "txn_id": len(T["points"]) + 1,
                  "customer_id": len(T["customers"]) + 1, "member_id": len(members) + 1}
    for i in range(18):
        T["products"][i]["price"] = web_price[i + 1]
        T["items"][i]["price_cents"] = store_cents[i + 1]
    return T


_CARDS = {}


def _member_by_card(members, card):
    if len(_CARDS) != len(members):
        _CARDS.clear()
        _CARDS.update({m["card_number"]: m for m in members})
    return _CARDS[card]


COLUMNS = {
    "customers": ["customer_id", "name", "email", "country", "tier", "created_at", "updated_at"],
    "products": ["product_id", "name", "category", "price", "updated_at"],
    "orders": ["order_id", "customer_id", "status", "total", "created_at", "updated_at"],
    "order_items": ["order_id", "line", "product_id", "quantity", "price"],
    "stores": ["store_id", "code", "city", "country", "timezone", "opened_on"],
    "items": ["sku", "name", "category", "price_cents", "updated_at"],
    "sales": ["sale_id", "store_id", "sold_at", "card_number", "payment", "total_cents"],
    "sale_lines": ["sale_id", "line", "sku", "qty", "unit_price_cents"],
    "members": ["member_id", "card_number", "full_name", "email", "phone", "tier", "points_balance", "joined_at", "updated_at"],
    "points": ["txn_id", "member_id", "channel", "ref", "points", "created_at"],
}
SYSTEM = {"customers": "web", "products": "web", "orders": "web", "order_items": "web", "stores": "store", "items": "store",
          "sales": "store", "sale_lines": "store", "members": "loyalty", "points": "loyalty"}


def _fmt(v):
    if v is None:
        return ""
    if isinstance(v, dt.datetime):
        return v.isoformat(sep=" ")                     # aware → "+00:00"; the tills' local time → none
    if isinstance(v, float):
        return f"{v:.2f}"
    return str(v)


def monthly(T):
    """table → {month or None: rows} — the big tables by month (orders' items follow their order)."""
    month_of_order = {o["order_id"]: o["created_at"].strftime("%Y-%m") for o in T["orders"]}
    month_of_sale = {s["sale_id"]: s["sold_at"].strftime("%Y-%m") for s in T["sales"]}
    key = {"orders": lambda r: r["created_at"].strftime("%Y-%m"),
           "order_items": lambda r: month_of_order[r["order_id"]],
           "sales": lambda r: r["sold_at"].strftime("%Y-%m"),
           "sale_lines": lambda r: month_of_sale[r["sale_id"]],
           "points": lambda r: r["created_at"].strftime("%Y-%m")}
    out = {}
    for t, rows in T.items():
        if t.startswith("_"):
            continue
        if t in key:
            parts = {}
            for r in rows:
                parts.setdefault(key[t](r), []).append(r)
            out[t] = parts
        else:
            out[t] = {None: rows}
    return out


def write_zip(path):
    T = build()
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as z:
        for t, parts in monthly(T).items():
            for month, rows in sorted(parts.items(), key=lambda x: x[0] or ""):
                buf = io.StringIO()
                w = csv.writer(buf, lineterminator="\n")
                w.writerow(COLUMNS[t])
                for r in rows:
                    w.writerow([_fmt(r[c]) for c in COLUMNS[t]])
                name = f"tidewell-history/{SYSTEM[t]}/{t}.csv" if month is None else f"tidewell-history/{SYSTEM[t]}/{t}/{month}.csv"
                info = zipfile.ZipInfo(name, date_time=(END.year, END.month, END.day, 23, 59, 0))
                info.compress_type = zipfile.ZIP_DEFLATED            # fixed date: the same bytes every run
                z.writestr(info, buf.getvalue())
    return {t: len(rows) for t, rows in T.items() if not t.startswith("_")}


def tail(days=2):
    """What the live systems hold when they start: every customer, product, store, item and member,
    and the orders, sales and points of the archive's last `days` days (the overlap)."""
    T = build()
    since = dt.datetime.combine(END - dt.timedelta(days=days - 1), dt.time(), UTC)
    orders = [o for o in T["orders"] if o["created_at"] >= since]
    ids = {o["order_id"] for o in orders}
    sales = [s for s in T["sales"] if s["_utc"] >= since]
    sids = {s["sale_id"] for s in sales}
    return {
        "customers": T["customers"], "products": T["products"], "stores": T["stores"], "items": T["items"],
        "members": T["members"],
        "orders": orders, "order_items": [i for i in T["order_items"] if i["order_id"] in ids],
        "sales": sales, "sale_lines": [l for l in T["sale_lines"] if l["sale_id"] in sids],
        "points": [p for p in T["points"] if p["created_at"] >= since],
        "next": T["_next"],                                         # where the live ids carry on
    }


if __name__ == "__main__":
    counts = write_zip(sys.argv[1] if len(sys.argv) > 1 else "tidewell-history.zip")
    print(counts)
