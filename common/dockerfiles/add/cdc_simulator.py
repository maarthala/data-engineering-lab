"""cdc-sim — keeps the lab's three live databases (on lab-db) changing, like one company's systems,
so members can practise change data capture and joining sources in "📡 Ingestion":

  shop     the web shop      customers · products · orders · order_items
  store    the stores' tills stores · items · sales · sale_lines
  loyalty  the loyalty card  members · points

Every few seconds (random) one thing happens somewhere —
  web:     a new order (+ items) · an order moves on (placed → paid → shipped → delivered) · an order
           is cancelled (deleted, with its items) · a new customer · a price or a tier changes
  store:   a sale (some with a loyalty card) · a return (negative quantities) · a till price changes
  loyalty: points for a web order or a card sale · points redeemed · someone joins at a till ·
           a member's details change
The systems disagree the way real ones do: the stores know products by SKU (SKU-0001 ↔ product 1),
count money in cents and write local time without a time zone; loyalty members' emails are the web
shop's in another spelling (case, spaces), and some members never shopped online.
Old rows go (oldest delivered orders, sales, points), so the databases don't grow for ever.

On start it sets each database up once (idempotent): tables + a first batch of rows, the login
members use (reader / reader: SELECT + REPLICATION, so each member's job can have its own
replication slot — never writes) and a publication olake_pub for all tables.
Standard library + psycopg2. CDC_DB_URL: the owner login, on the shop database.
"""
import os
import random
import time
import urllib.parse

import psycopg2

DB_URL = os.environ.get("CDC_DB_URL", "postgresql://postgres:postgres@lab-db:5432/shop")
READER = os.environ.get("CDC_READER", "reader:reader")
MIN_S, MAX_S = float(os.environ.get("CDC_SIM_MIN_S", "2")), float(os.environ.get("CDC_SIM_MAX_S", "10"))
MAX_ORDERS = int(os.environ.get("CDC_SIM_MAX_ORDERS", "3000"))
MAX_SALES = int(os.environ.get("CDC_SIM_MAX_SALES", "4000"))
MAX_POINTS = int(os.environ.get("CDC_SIM_MAX_POINTS", "8000"))
DBS = ("shop", "store", "loyalty")

SCHEMA = {
    "shop": """
CREATE TABLE IF NOT EXISTS customers (
  customer_id serial PRIMARY KEY, name text NOT NULL, email text NOT NULL, country text NOT NULL,
  tier text NOT NULL DEFAULT 'bronze', created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS products (
  product_id serial PRIMARY KEY, name text NOT NULL, category text NOT NULL, price numeric(10,2) NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS orders (
  order_id serial PRIMARY KEY, customer_id int NOT NULL REFERENCES customers, status text NOT NULL DEFAULT 'placed',
  total numeric(12,2) NOT NULL DEFAULT 0, created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS order_items (
  order_id int NOT NULL REFERENCES orders ON DELETE CASCADE, line int NOT NULL, product_id int NOT NULL REFERENCES products,
  quantity int NOT NULL, price numeric(10,2) NOT NULL, PRIMARY KEY (order_id, line));
""",
    "store": """
CREATE TABLE IF NOT EXISTS stores (
  store_id serial PRIMARY KEY, code text NOT NULL UNIQUE, city text NOT NULL, country text NOT NULL,
  timezone text NOT NULL, opened_on date NOT NULL);
CREATE TABLE IF NOT EXISTS items (
  sku text PRIMARY KEY, name text NOT NULL, category text NOT NULL, price_cents int NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS sales (
  sale_id bigserial PRIMARY KEY, store_id int NOT NULL REFERENCES stores,
  sold_at timestamp NOT NULL,                       -- the store's local time, no time zone
  card_number text, payment text NOT NULL, total_cents int NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS sale_lines (
  sale_id bigint NOT NULL REFERENCES sales ON DELETE CASCADE, line int NOT NULL, sku text NOT NULL REFERENCES items,
  qty int NOT NULL, unit_price_cents int NOT NULL, PRIMARY KEY (sale_id, line));
""",
    "loyalty": """
CREATE TABLE IF NOT EXISTS members (
  member_id serial PRIMARY KEY, card_number text NOT NULL UNIQUE, full_name text NOT NULL, email text,
  phone text, tier text NOT NULL DEFAULT 'blue', points_balance int NOT NULL DEFAULT 0,
  joined_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS points (
  txn_id bigserial PRIMARY KEY, member_id int NOT NULL REFERENCES members ON DELETE CASCADE,
  channel text NOT NULL, ref text, points int NOT NULL, created_at timestamptz NOT NULL DEFAULT now());
""",
}
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
STORES = [("AMS-01", "Amsterdam", "NL", "Europe/Amsterdam", "2019-03-01"), ("LON-01", "London", "UK", "Europe/London", "2020-06-15"),
          ("BLR-01", "Bengaluru", "IN", "Asia/Kolkata", "2021-01-10"), ("NYC-01", "New York", "US", "America/New_York", "2018-09-01"),
          ("SIN-01", "Singapore", "SG", "Asia/Singapore", "2022-04-20")]
NEXT = {"placed": "paid", "paid": "shipped", "shipped": "delivered"}
C = {}                                            # database → connection (autocommit)


def url(db):
    u = urllib.parse.urlparse(DB_URL)
    return urllib.parse.urlunparse(u._replace(path="/" + db))


def connect(db):
    while True:
        try:
            c = psycopg2.connect(url(db), connect_timeout=5)
            c.autocommit = True
            return c
        except psycopg2.Error as e:
            print(f"[cdc-sim] waiting for {db}: {str(e).strip().splitlines()[0]}", flush=True)
            time.sleep(3)


def cur(db):
    return C[db].cursor()


def sku(product_id):
    return f"SKU-{product_id:04d}"


def spelled(email):
    """The same email as another system types it."""
    return random.choice([email, email, email.upper(), email.title(), f" {email}", f"{email} "])


# ---- setup -----------------------------------------------------------------------------------
def setup():
    """Databases, tables, the reader login and the publications — once; then, the first time, the
    systems as Tidewell's archive left them (tidewell_archive.tail: everyone and everything, plus the
    archive's last two days of orders, sales and points, with the same ids — the overlap)."""
    a = psycopg2.connect(url("postgres"), connect_timeout=5)
    a.autocommit = True                                             # CREATE DATABASE: not in a transaction
    with a.cursor() as k:
        for db in DBS:
            k.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db,))
            if not k.fetchone():
                k.execute(f'CREATE DATABASE "{db}"')
        user, pw = READER.split(":", 1)
        k.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (user,))
        if not k.fetchone():
            k.execute(f'CREATE ROLE "{user}" LOGIN REPLICATION PASSWORD %s', (pw,))
    a.close()
    for db in DBS:
        C[db] = connect(db)
        with cur(db) as k:
            k.execute(SCHEMA[db])
            k.execute(f'GRANT CONNECT ON DATABASE "{db}" TO "{user}"')
            k.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA public TO "{user}"')
            k.execute(f'ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO "{user}"')
            k.execute("SELECT 1 FROM pg_publication WHERE pubname = 'olake_pub'")
            if not k.fetchone():
                k.execute("CREATE PUBLICATION olake_pub FOR ALL TABLES")
    with cur("shop") as k:
        k.execute("SELECT count(*) FROM products")
        if k.fetchone()[0]:
            return
    import tidewell_archive
    t = tidewell_archive.tail()

    def load(db, table, rows):
        cols = tidewell_archive.COLUMNS[table]
        with cur(db) as k:
            for r in rows:
                k.execute(f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({', '.join(['%s'] * len(cols))})",
                          [r[c] for c in cols])
    for table in ("customers", "products", "orders", "order_items"):
        load("shop", table, t[table])
    for table in ("stores", "items", "sales", "sale_lines"):
        load("store", table, t[table])
    for table in ("members", "points"):
        load("loyalty", table, t[table])
    n = t["next"]                                                   # the live ids carry on after the archive's
    for db, seq, nxt in (("shop", "customers_customer_id_seq", n["customer_id"]), ("shop", "orders_order_id_seq", n["order_id"]),
                         ("shop", "products_product_id_seq", len(t["products"]) + 1),
                         ("store", "stores_store_id_seq", len(t["stores"]) + 1), ("store", "sales_sale_id_seq", n["sale_id"]),
                         ("loyalty", "members_member_id_seq", n["member_id"]), ("loyalty", "points_txn_id_seq", n["txn_id"])):
        with cur(db) as k:
            k.execute("SELECT setval(%s, %s, false)", (seq, nxt))
    print(f"[cdc-sim] made Tidewell's systems from its archive: {len(t['customers'])} customers, "
          f"{len(t['members'])} members, the last two days' {len(t['orders'])} orders and {len(t['sales'])} sales", flush=True)


# ---- loyalty helpers -------------------------------------------------------------------------
def add_member(name, email):
    with cur("loyalty") as k:
        k.execute("INSERT INTO members (card_number, full_name, email, phone, tier) VALUES (%s, %s, %s, %s, %s) "
                  "ON CONFLICT (card_number) DO NOTHING RETURNING member_id",
                  (f"LC{random.randint(10_000_000, 99_999_999)}", name, email,
                   random.choice([None, f"+{random.randint(1, 99)} {random.randint(100_000_000, 999_999_999)}"]),
                   random.choices(CARD_TIERS, [6, 3, 1])[0]))
        row = k.fetchone()
        return row[0] if row else None


def earn(member_id, channel, ref, pts):
    if not member_id or not pts:
        return
    with cur("loyalty") as k:
        k.execute("INSERT INTO points (member_id, channel, ref, points) VALUES (%s, %s, %s, %s)", (member_id, channel, ref, pts))
        k.execute("UPDATE members SET points_balance = points_balance + %s, updated_at = now() WHERE member_id = %s",
                  (pts, member_id))


def member_by_email(email):
    with cur("loyalty") as k:
        k.execute("SELECT member_id FROM members WHERE lower(trim(email)) = lower(%s) LIMIT 1", (email,))
        row = k.fetchone()
        return row[0] if row else None


# ---- web shop --------------------------------------------------------------------------------
def new_customer(join=True):
    f, l = random.choice(FIRST), random.choice(LAST)
    email = f"{f.lower()}.{l.lower()}{random.randint(1, 999)}@example.com"
    with cur("shop") as k:
        k.execute("INSERT INTO customers (name, email, country, tier) VALUES (%s, %s, %s, %s) RETURNING customer_id",
                  (f"{f} {l}", email, random.choice(COUNTRIES), random.choices(TIERS, [6, 3, 1])[0]))
        cid = k.fetchone()[0]
    if join and random.random() < 0.4:
        add_member(f"{f} {l}", spelled(email))
        return f"new web customer {cid} (+ loyalty card)"
    return f"new web customer {cid}"


def new_order(points=True):
    with cur("shop") as k:
        k.execute("SELECT customer_id, email FROM customers ORDER BY random() LIMIT 1")
        cust, email = k.fetchone()
        k.execute("INSERT INTO orders (customer_id) VALUES (%s) RETURNING order_id", (cust,))
        oid = k.fetchone()[0]
        k.execute("SELECT product_id, price FROM products ORDER BY random() LIMIT %s", (random.randint(1, 3),))
        total = 0
        for line, (pid, price) in enumerate(k.fetchall(), 1):
            q = random.randint(1, 3)
            k.execute("INSERT INTO order_items (order_id, line, product_id, quantity, price) VALUES (%s, %s, %s, %s, %s)",
                      (oid, line, pid, q, price))
            total += q * price
        k.execute("UPDATE orders SET total = %s WHERE order_id = %s", (total, oid))
    if points:
        earn(member_by_email(email), "web", f"order:{oid}", int(total))
    return f"new web order {oid}"


def move_order():
    with cur("shop") as k:
        k.execute("SELECT order_id, status FROM orders WHERE status <> 'delivered' ORDER BY random() LIMIT 1")
        row = k.fetchone()
        if not row:
            return new_order()
        k.execute("UPDATE orders SET status = %s, updated_at = now() WHERE order_id = %s", (NEXT[row[1]], row[0]))
    return f"web order {row[0]} {row[1]} → {NEXT[row[1]]}"


def cancel_order():
    with cur("shop") as k:
        k.execute("SELECT o.order_id, c.email, o.total FROM orders o JOIN customers c USING (customer_id) "
                  "WHERE o.status IN ('placed', 'paid') ORDER BY random() LIMIT 1")
        row = k.fetchone()
        if not row:
            return new_order()
        k.execute("DELETE FROM orders WHERE order_id = %s", (row[0],))            # its items go with it (cascade)
    earn(member_by_email(row[1]), "web", f"order:{row[0]}:cancelled", -int(row[2]))  # the points go back
    return f"web order {row[0]} cancelled (deleted)"


def change_price():
    with cur("shop") as k:
        k.execute("UPDATE products SET price = round(price * %s, 2), updated_at = now() WHERE product_id = "
                  "(SELECT product_id FROM products ORDER BY random() LIMIT 1) RETURNING product_id, price",
                  (random.choice([0.9, 0.95, 1.05, 1.1]),))
        pid, price = k.fetchone()
    return f"web price: product {pid} now {price}"


def change_tier():
    with cur("shop") as k:
        k.execute("UPDATE customers SET tier = %s, updated_at = now() WHERE customer_id = "
                  "(SELECT customer_id FROM customers ORDER BY random() LIMIT 1) RETURNING customer_id, tier",
                  (random.choice(TIERS),))
        cid, tier = k.fetchone()
    return f"web customer {cid} is {tier}"


# ---- stores ----------------------------------------------------------------------------------
def store_sale(hours_ago=0.0, returns=False):
    with cur("store") as k:
        k.execute("SELECT store_id, timezone FROM stores ORDER BY random() LIMIT 1")
        store, tz = k.fetchone()
        card = None
        if random.random() < 0.5:                                    # half the customers show a card
            with cur("loyalty") as m:
                m.execute("SELECT card_number FROM members ORDER BY random() LIMIT 1")
                card = m.fetchone()[0]
        k.execute("INSERT INTO sales (store_id, sold_at, card_number, payment) "
                  "VALUES (%s, (now() - make_interval(secs => %s)) AT TIME ZONE %s, %s, %s) RETURNING sale_id",
                  (store, hours_ago * 3600, tz, card, random.choices(["card", "cash", "mobile"], [6, 2, 2])[0]))
        sid = k.fetchone()[0]
        k.execute("SELECT sku, price_cents FROM items ORDER BY random() LIMIT %s", (random.randint(1, 4),))
        total = 0
        for line, (s, cents) in enumerate(k.fetchall(), 1):
            q = -1 if returns else random.randint(1, 3)
            k.execute("INSERT INTO sale_lines (sale_id, line, sku, qty, unit_price_cents) VALUES (%s, %s, %s, %s, %s)",
                      (sid, line, s, q, cents))
            total += q * cents
            if returns:
                break                                               # a return: one item back
        k.execute("UPDATE sales SET total_cents = %s WHERE sale_id = %s", (total, sid))
    if card:
        with cur("loyalty") as m:
            m.execute("SELECT member_id FROM members WHERE card_number = %s", (card,))
            row = m.fetchone()
        earn(row and row[0], "store", f"sale:{sid}", total // 100)
    return f"store {'return' if returns else 'sale'} {sid}"


def store_return():
    return store_sale(returns=True)


def store_price():
    with cur("store") as k:
        k.execute("UPDATE items SET price_cents = (price_cents * %s)::int, updated_at = now() WHERE sku = "
                  "(SELECT sku FROM items ORDER BY random() LIMIT 1) RETURNING sku, price_cents",
                  (random.choice([0.9, 0.95, 1.05, 1.1]),))
        s, cents = k.fetchone()
    return f"store price: {s} now {cents} cents"


# ---- loyalty ---------------------------------------------------------------------------------
def join_at_till():
    f, l = random.choice(FIRST), random.choice(LAST)
    mid = add_member(f"{f} {l}", random.choice([f"{f.lower()}{l.lower()}{random.randint(1, 99)}@mail.test", None]))
    earn(mid, "bonus", "welcome", 50)
    return f"loyalty: new member {mid}"


def redeem():
    with cur("loyalty") as k:
        k.execute("SELECT member_id, points_balance FROM members WHERE points_balance >= 100 ORDER BY random() LIMIT 1")
        row = k.fetchone()
    if not row:
        return join_at_till()
    earn(row[0], "redeem", None, -random.choice([100, 200, 500]) if row[1] >= 500 else -100)
    return f"loyalty: member {row[0]} redeemed points"


def member_update():
    with cur("loyalty") as k:
        what = random.choice(["phone", "email", "tier"])
        if what == "phone":
            k.execute("UPDATE members SET phone = %s, updated_at = now() WHERE member_id = "
                      "(SELECT member_id FROM members ORDER BY random() LIMIT 1) RETURNING member_id",
                      (f"+{random.randint(1, 99)} {random.randint(100_000_000, 999_999_999)}",))
        elif what == "email":                                       # someone tidies up their email
            k.execute("UPDATE members SET email = lower(trim(email)), updated_at = now() WHERE member_id = "
                      "(SELECT member_id FROM members WHERE email IS NOT NULL ORDER BY random() LIMIT 1) RETURNING member_id")
        else:
            k.execute("UPDATE members SET tier = %s, updated_at = now() WHERE member_id = "
                      "(SELECT member_id FROM members ORDER BY random() LIMIT 1) RETURNING member_id",
                      (random.choice(CARD_TIERS),))
        row = k.fetchone()
    return f"loyalty: member {row[0] if row else '?'} changed {what}"


def tidy():
    """Keep them small: the oldest delivered orders, sales and points go (deletes, too)."""
    with cur("shop") as k:
        k.execute("SELECT count(*) FROM orders")
        extra = k.fetchone()[0] - MAX_ORDERS
        if extra > 0:
            k.execute("DELETE FROM orders WHERE order_id IN (SELECT order_id FROM orders WHERE status = 'delivered' "
                      "ORDER BY created_at LIMIT %s)", (extra,))
    with cur("store") as k:
        k.execute("DELETE FROM sales WHERE sale_id IN (SELECT sale_id FROM sales ORDER BY sale_id "
                  "LIMIT greatest((SELECT count(*) FROM sales) - %s, 0))", (MAX_SALES,))
    with cur("loyalty") as k:
        k.execute("DELETE FROM points WHERE txn_id IN (SELECT txn_id FROM points ORDER BY txn_id "
                  "LIMIT greatest((SELECT count(*) FROM points) - %s, 0))", (MAX_POINTS,))


ACTIONS = [(new_order, 25), (move_order, 18), (new_customer, 5), (cancel_order, 5), (change_price, 4), (change_tier, 4),
           (store_sale, 25), (store_return, 3), (store_price, 2),
           (join_at_till, 3), (redeem, 3), (member_update, 4)]


def reconnect():
    for db in list(C):
        try:
            C[db].close()
        except psycopg2.Error:
            pass
        C[db] = connect(db)


def main():
    setup()
    print(f"[cdc-sim] changing shop, store and loyalty every {MIN_S:g}–{MAX_S:g} s", flush=True)
    n = 0
    while True:
        time.sleep(random.uniform(MIN_S, MAX_S))
        try:
            what = random.choices([a for a, _ in ACTIONS], [w for _, w in ACTIONS])[0]()
            n += 1
            if n % 50 == 0:
                tidy()
                print(f"[cdc-sim] {n} changes so far — last: {what}", flush=True)
        except psycopg2.Error as e:
            print(f"[cdc-sim] {str(e).strip().splitlines()[0]} — reconnecting", flush=True)
            reconnect()


if __name__ == "__main__":
    main()
