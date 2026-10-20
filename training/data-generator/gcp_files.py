"""ShopFlow as files, for the Google Cloud unit (training/docs/gcp).

Writes two zips into training/docs/assets/gcp/:
  shopflow_gcp_files.zip   shopflow/{customers,products,orders,order_items}/*.csv  (2024-06-01 .. 2026-05-31)
  shopflow_new_day.zip     shopflow/{orders,order_items}/*_2026-06-01.csv          (the "new data" for Composer)

Same shape as seed.sql (6,000 customers, 200 products, 40,000 orders, ~100k lines). orders is messy on
purpose — duplicate lines, mixed-case status/channel, blank currency, unreadable customer ids and
timestamps — so the Dataflow lesson has something to clean. Fixed seed: same files every run.

    python3 training/data-generator/gcp_files.py
"""
import csv
import math
import io
import random
import zipfile
from datetime import date, datetime, timedelta
from pathlib import Path

OUT = Path(__file__).resolve().parent.parent / "docs" / "assets" / "gcp"
rnd = random.Random(42)

COUNTRIES = ["US", "US", "UK", "UK", "DE", "FR", "IN", "CA", "AU"]
TIERS = ["none", "none", "none", "none", "silver", "silver", "gold"]
CATEGORIES = ["Electronics", "Furniture", "Stationery", "Clothing", "Home & Kitchen", "Sports", "Books", "Toys"]
CHANNELS = ["web", "web", "web", "app", "app", "marketplace", "wholesale"]
STATUSES = ["delivered"] * 7 + ["shipped", "placed", "cancelled"]
CURRENCIES = ["USD", "USD", "USD", "GBP", "EUR", "INR"]
START = datetime(2024, 6, 1)


def csv_text(header, rows):
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(header)
    w.writerows(rows)
    return buf.getvalue()


def customers():
    return [[i, f"Customer {i}", f"customer{i}@example.com", rnd.choice(COUNTRIES),
             (date(2023, 1, 1) + timedelta(days=rnd.randint(0, 900))).isoformat(),
             rnd.choice(TIERS), str(rnd.random() < 0.6).lower()] for i in range(1, 6001)]


def products():
    rows = []
    for i in range(1, 201):
        price = round(5 + rnd.random() * 495, 2)
        rows.append([i, f"Product {i}", rnd.choice(CATEGORIES), f"{price:.2f}",
                     f"{price * (0.4 + rnd.random() * 0.3):.2f}"])
    return rows


def orders_and_items(first_id, n, day0, days, prices):
    orders, items = [], []
    for oid in range(first_id, first_id + n):
        ts = day0 + timedelta(days=rnd.randint(0, days - 1), seconds=rnd.randint(0, 86399))
        promo = rnd.randint(1, 12) if rnd.random() < 0.2 else ""
        orders.append([oid, 1 + min(int(-500 * math.log(1 - rnd.random())), 5999),
                       rnd.choice(CHANNELS), ts.strftime("%Y-%m-%d %H:%M:%S"),
                       rnd.choice(STATUSES), rnd.choice(CURRENCIES), promo])
        for pid in rnd.sample(range(1, 201), 1 + oid % 4):
            items.append([oid, pid, rnd.randint(1, 5), prices[pid]])
    return orders, items


def make_messy(orders):
    """The source system's export is not clean: change some values, then add duplicate lines."""
    for o in orders:
        r = rnd.random()
        if r < 0.03:
            o[4] = o[4].upper()                    # DELIVERED
        elif r < 0.06:
            o[4] = " " + o[4].capitalize() + " "   # " Delivered "
        if rnd.random() < 0.04:
            o[2] = o[2].capitalize()               # Web
        if rnd.random() < 0.02:
            o[5] = ""                              # currency missing -> USD
    for o in rnd.sample(orders, 60):
        o[1] = rnd.choice(["", "unknown", "N/A"])  # no customer -> rejected
    for o in rnd.sample(orders, 40):
        o[3] = rnd.choice(["", "not a date", "2025-13-45 10:00:00"])  # no timestamp -> rejected
    dups = rnd.sample(orders, 150)
    out = orders + [list(d) for d in dups]
    rnd.shuffle(out)
    return out


def write_zip(path, files):
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as z:
        for name, text in files.items():
            z.writestr(name, text)
    print(f"{path.name}: {path.stat().st_size // 1024} KB, " +
          ", ".join(f"{n.split('/')[-1]} {t.count(chr(10)) - 1} rows" for n, t in files.items()))


O_HEAD = ["order_id", "customer_id", "channel", "order_ts", "status", "currency", "promo_id"]
I_HEAD = ["order_id", "product_id", "quantity", "unit_price"]

cust, prod = customers(), products()
prices = {p[0]: p[3] for p in prod}
orders, items = orders_and_items(1, 40000, START, 730, prices)
new_orders, new_items = orders_and_items(40001, 800, datetime(2026, 6, 1), 1, prices)

write_zip(OUT / "shopflow_gcp_files.zip", {
    "shopflow/customers/customers.csv": csv_text(
        ["customer_id", "full_name", "email", "country", "signup_date", "loyalty_tier", "marketing_opt_in"], cust),
    "shopflow/products/products.csv": csv_text(["product_id", "name", "category", "price", "cost"], prod),
    "shopflow/orders/orders_2024-06-01_2026-05-31.csv": csv_text(O_HEAD, make_messy(orders)),
    "shopflow/order_items/order_items_2024-06-01_2026-05-31.csv": csv_text(I_HEAD, items),
})
write_zip(OUT / "shopflow_new_day.zip", {
    "shopflow/orders/orders_2026-06-01.csv": csv_text(O_HEAD, new_orders),
    "shopflow/order_items/order_items_2026-06-01.csv": csv_text(I_HEAD, new_items),
})
