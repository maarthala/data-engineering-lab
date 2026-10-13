"""Advance the ShopFlow business by one day: insert a day's new orders into Postgres.

Stands in for reality — in production this "new data" would arrive on its own. Runs first in the
daily DAG ("Schedule ingestion & Spark jobs" lesson):
  python simulate_day.py --date 2026-09-01 --orders 200
"""
import argparse
import random
import datetime as dt
import psycopg2

STATUSES = ["delivered"] * 7 + ["shipped", "placed", "cancelled"]
CHANNELS = ["web", "web", "app", "marketplace", "wholesale"]
CURRENCIES = ["USD", "USD", "GBP", "EUR", "INR"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="business date YYYY-MM-DD")
    ap.add_argument("--orders", type=int, default=200)
    args = ap.parse_args()
    day = dt.date.fromisoformat(args.date)

    conn = psycopg2.connect(host="postgres", dbname="shopflow", user="postgres", password="postgres")
    cur = conn.cursor()
    cur.execute("SELECT max(customer_id) FROM customers")
    n_cust = cur.fetchone()[0]
    cur.execute("SELECT product_id, price FROM products")
    products = cur.fetchall()

    made = 0
    for _ in range(args.orders):
        ts = dt.datetime.combine(day, dt.time(0)) + dt.timedelta(seconds=random.randint(0, 86399))
        cur.execute(
            "INSERT INTO orders (customer_id, channel, order_ts, status, currency) "
            "VALUES (%s, %s, %s, %s, %s) RETURNING order_id",
            (random.randint(1, n_cust), random.choice(CHANNELS), ts,
             random.choice(STATUSES), random.choice(CURRENCIES)),
        )
        oid = cur.fetchone()[0]
        for pid, price in random.sample(products, k=random.randint(1, 4)):
            cur.execute(
                "INSERT INTO order_items (order_id, product_id, quantity, unit_price) "
                "VALUES (%s, %s, %s, %s)",
                (oid, pid, random.randint(1, 5), price),
            )
        made += 1

    conn.commit()
    cur.close()
    conn.close()
    print(f"SIMULATED {made} orders for {args.date}")


if __name__ == "__main__":
    main()
