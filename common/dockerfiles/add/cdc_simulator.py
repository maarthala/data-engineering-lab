"""cdc-sim — keeps the lab's live shop database (cdc-db) changing, so members can watch change data
capture at work in "📡 Ingestion": every few seconds (random) one change, like a small web shop —
  a new customer · a new order (+ items) · an order moves on (placed → paid → shipped → delivered)
  · an order is cancelled (deleted, with its items) · a price changes · a customer changes tier
It keeps the tables small (the oldest delivered orders go), so the database doesn't grow for ever.

On start it sets the database up once (idempotent): the tables + a first batch of rows, the login
members use (reader / reader: SELECT on everything + REPLICATION, so each member's job can have its
own replication slot — never writes) and the publication olake_pub for all tables.
Standard library + psycopg2. CDC_DB_URL: the database's owner login.
"""
import os
import random
import time

import psycopg2

DB_URL = os.environ.get("CDC_DB_URL", "postgresql://postgres:postgres@cdc-db:5432/shop")
READER = os.environ.get("CDC_READER", "reader:reader")
MIN_S, MAX_S = float(os.environ.get("CDC_SIM_MIN_S", "2")), float(os.environ.get("CDC_SIM_MAX_S", "10"))
MAX_ORDERS = int(os.environ.get("CDC_SIM_MAX_ORDERS", "3000"))

SCHEMA = """
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
"""
FIRST = ["Asha", "Ben", "Chen", "Dara", "Eli", "Fatima", "Goran", "Hana", "Ivan", "Jia", "Kofi", "Lena", "Mateo",
         "Nora", "Omar", "Priya", "Quinn", "Ravi", "Sara", "Tomas", "Uma", "Vik", "Wen", "Yara", "Zoe"]
LAST = ["Smith", "Garcia", "Khan", "Li", "Okafor", "Novak", "Rossi", "Silva", "Tanaka", "Weber", "Patel", "Nguyen"]
COUNTRIES = ["US", "UK", "DE", "IN", "FR", "NL", "SG", "BR", "JP", "AU"]
TIERS = ["bronze", "silver", "gold"]
PRODUCTS = [("Laptop", "electronics", 899), ("Headphones", "electronics", 129), ("Phone case", "accessories", 19),
            ("Desk lamp", "home", 39), ("Coffee mug", "home", 12), ("Backpack", "accessories", 59),
            ("Monitor", "electronics", 249), ("Keyboard", "electronics", 79), ("Notebook", "office", 6),
            ("Water bottle", "accessories", 18), ("Office chair", "home", 189), ("USB cable", "electronics", 9),
            ("Plant pot", "home", 15), ("Pen set", "office", 11), ("Running shoes", "sports", 99),
            ("Yoga mat", "sports", 35), ("Tea sampler", "food", 22), ("Chocolate box", "food", 14)]
NEXT = {"placed": "paid", "paid": "shipped", "shipped": "delivered"}


def connect():
    while True:
        try:
            c = psycopg2.connect(DB_URL, connect_timeout=5)
            c.autocommit = True
            return c
        except psycopg2.Error as e:
            print(f"[cdc-sim] waiting for the database: {str(e).strip().splitlines()[0]}", flush=True)
            time.sleep(3)


def setup(cur):
    """Tables, a first batch of rows, the reader login and the publication — once (idempotent)."""
    cur.execute(SCHEMA)
    cur.execute("SELECT count(*) FROM products")
    if not cur.fetchone()[0]:
        for name, cat, price in PRODUCTS:
            cur.execute("INSERT INTO products (name, category, price) VALUES (%s, %s, %s)", (name, cat, price))
        for _ in range(60):
            new_customer(cur)
        for _ in range(150):
            new_order(cur)
        print("[cdc-sim] made the shop: products, customers, orders", flush=True)
    user, pw = READER.split(":", 1)
    cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (user,))
    if not cur.fetchone():
        cur.execute(f'CREATE ROLE "{user}" LOGIN REPLICATION PASSWORD %s', (pw,))
    cur.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA public TO "{user}"')
    cur.execute(f'ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO "{user}"')
    cur.execute("SELECT 1 FROM pg_publication WHERE pubname = 'olake_pub'")
    if not cur.fetchone():
        cur.execute("CREATE PUBLICATION olake_pub FOR ALL TABLES")


def new_customer(cur):
    f, l = random.choice(FIRST), random.choice(LAST)
    cur.execute("INSERT INTO customers (name, email, country, tier) VALUES (%s, %s, %s, %s) RETURNING customer_id",
                (f"{f} {l}", f"{f.lower()}.{l.lower()}{random.randint(1, 999)}@example.com", random.choice(COUNTRIES),
                 random.choices(TIERS, [6, 3, 1])[0]))
    return f"new customer {cur.fetchone()[0]}"


def new_order(cur):
    cur.execute("SELECT customer_id FROM customers ORDER BY random() LIMIT 1")
    cust = cur.fetchone()[0]
    cur.execute("INSERT INTO orders (customer_id) VALUES (%s) RETURNING order_id", (cust,))
    oid = cur.fetchone()[0]
    cur.execute("SELECT product_id, price FROM products ORDER BY random() LIMIT %s", (random.randint(1, 3),))
    total = 0
    for line, (pid, price) in enumerate(cur.fetchall(), 1):
        q = random.randint(1, 3)
        cur.execute("INSERT INTO order_items (order_id, line, product_id, quantity, price) VALUES (%s, %s, %s, %s, %s)",
                    (oid, line, pid, q, price))
        total += q * price
    cur.execute("UPDATE orders SET total = %s WHERE order_id = %s", (total, oid))
    return f"new order {oid}"


def move_order(cur):
    cur.execute("SELECT order_id, status FROM orders WHERE status <> 'delivered' ORDER BY random() LIMIT 1")
    row = cur.fetchone()
    if not row:
        return new_order(cur)
    cur.execute("UPDATE orders SET status = %s, updated_at = now() WHERE order_id = %s", (NEXT[row[1]], row[0]))
    return f"order {row[0]} {row[1]} → {NEXT[row[1]]}"


def cancel_order(cur):
    cur.execute("SELECT order_id FROM orders WHERE status IN ('placed', 'paid') ORDER BY random() LIMIT 1")
    row = cur.fetchone()
    if not row:
        return new_order(cur)
    cur.execute("DELETE FROM orders WHERE order_id = %s", row)            # its items go with it (cascade)
    return f"order {row[0]} cancelled (deleted)"


def change_price(cur):
    cur.execute("UPDATE products SET price = round(price * %s, 2), updated_at = now() WHERE product_id = "
                "(SELECT product_id FROM products ORDER BY random() LIMIT 1) RETURNING product_id, price",
                (random.choice([0.9, 0.95, 1.05, 1.1]),))
    pid, price = cur.fetchone()
    return f"product {pid} now {price}"


def change_tier(cur):
    cur.execute("UPDATE customers SET tier = %s, updated_at = now() WHERE customer_id = "
                "(SELECT customer_id FROM customers ORDER BY random() LIMIT 1) RETURNING customer_id, tier",
                (random.choice(TIERS),))
    cid, tier = cur.fetchone()
    return f"customer {cid} is {tier}"


def tidy(cur):
    """Keep it small: past MAX_ORDERS, the oldest delivered orders go (deletes, too)."""
    cur.execute("SELECT count(*) FROM orders")
    extra = cur.fetchone()[0] - MAX_ORDERS
    if extra > 0:
        cur.execute("DELETE FROM orders WHERE order_id IN (SELECT order_id FROM orders WHERE status = 'delivered' "
                    "ORDER BY created_at LIMIT %s)", (extra,))


ACTIONS = [(new_order, 40), (move_order, 30), (new_customer, 8), (cancel_order, 7), (change_price, 8), (change_tier, 7)]


def main():
    c = connect()
    with c.cursor() as cur:
        setup(cur)
    print(f"[cdc-sim] changing the shop every {MIN_S:g}–{MAX_S:g} s", flush=True)
    n = 0
    while True:
        time.sleep(random.uniform(MIN_S, MAX_S))
        try:
            with c.cursor() as cur:
                what = random.choices([a for a, _ in ACTIONS], [w for _, w in ACTIONS])[0](cur)
                n += 1
                if n % 50 == 0:
                    tidy(cur)
                    print(f"[cdc-sim] {n} changes so far — last: {what}", flush=True)
        except psycopg2.Error as e:
            print(f"[cdc-sim] {str(e).strip().splitlines()[0]} — reconnecting", flush=True)
            try:
                c.close()
            except psycopg2.Error:
                pass
            c = connect()


if __name__ == "__main__":
    main()
