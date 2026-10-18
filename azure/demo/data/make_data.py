"""Make the demo's messy sales files (Nordic Coffee Co. online orders, one row = one order).

    python3 make_data.py      → sales_2026_h1.csv (Jan–Jun) and sales_2026_07.csv (July)

Same seed → the same files every time. The mess is on purpose, for the Silver step:
exact duplicate rows, missing ids / quantities / prices, "N/A" prices, zero or negative
quantities, two date formats, country / category in mixed case with spaces, empty cities.
"""
import csv
import random
from datetime import date, timedelta
from pathlib import Path

HERE = Path(__file__).parent

PRODUCTS = [  # product, category, price (EUR)
    ("Espresso Beans 1kg", "Coffee", 24.90), ("Filter Roast 500g", "Coffee", 13.50),
    ("Decaf Blend 500g", "Coffee", 14.90), ("Single Origin Kenya 250g", "Coffee", 11.90),
    ("Cold Brew Pack", "Coffee", 9.90),
    ("Green Tea 100g", "Tea", 7.50), ("Earl Grey 100g", "Tea", 6.90), ("Chai Mix 200g", "Tea", 8.90),
    ("Cinnamon Bun", "Bakery", 3.90), ("Blueberry Muffin", "Bakery", 3.50), ("Rye Bread", "Bakery", 5.20),
    ("Logo Mug", "Merch", 14.00), ("Travel Cup", "Merch", 22.00), ("Tote Bag", "Merch", 12.00),
    ("Hand Grinder", "Equipment", 49.00), ("Pour-over Kit", "Equipment", 39.00),
    ("Moka Pot", "Equipment", 34.00), ("Milk Frother", "Equipment", 29.00),
]
WEIGHTS = [9, 8, 4, 3, 4, 4, 4, 3, 10, 8, 5, 3, 2, 2, 1, 1, 1, 1]
CITIES = {
    "Finland": ["Helsinki", "Espoo", "Tampere", "Turku", "Oulu"],
    "Sweden": ["Stockholm", "Gothenburg", "Malmö", "Uppsala"],
    "Norway": ["Oslo", "Bergen", "Trondheim"],
    "Denmark": ["Copenhagen", "Aarhus", "Odense"],
    "Estonia": ["Tallinn", "Tartu"],
    "Germany": ["Berlin", "Hamburg", "Munich"],
}
COUNTRY_W = [30, 22, 12, 12, 8, 16]
CHANNELS, CHANNEL_W = ["web", "app", "store"], [45, 35, 20]
FIRST = ["Aino", "Eero", "Liam", "Elsa", "Noah", "Saga", "Oskar", "Maja", "Lukas", "Ida", "Emil",
         "Freja", "Leo", "Alma", "Mikael", "Sofia", "Hugo", "Ella", "Jonas", "Linnea"]
LAST = ["Virtanen", "Korhonen", "Andersson", "Nilsen", "Hansen", "Tamm", "Müller", "Lindqvist",
        "Mäkinen", "Johansson", "Berg", "Schmidt", "Larsen", "Saar", "Nieminen"]
HEADER = ["order_id", "order_date", "customer_name", "city", "country", "product", "category",
          "quantity", "unit_price", "channel"]


def orders(rng, first_id, start, days, base):
    """Clean orders: more on weekends, slowly growing, a summer bump for cold brew."""
    rows, oid = [], first_id
    for d in range(days):
        day = start + timedelta(days=d)
        n = int(base * (1 + d / 400) * (1.35 if day.weekday() >= 5 else 1) * rng.uniform(0.85, 1.15))
        for _ in range(n):
            p, cat, price = rng.choices(PRODUCTS, WEIGHTS)[0]
            if day.month >= 6 and rng.random() < 0.15:                    # summer: cold brew
                p, cat, price = PRODUCTS[4]
            country = rng.choices(list(CITIES), COUNTRY_W)[0]
            qty = rng.choices([1, 2, 3, 4, 6], [55, 25, 10, 6, 4])[0]
            rows.append([oid, day.isoformat(), f"{rng.choice(FIRST)} {rng.choice(LAST)}",
                         rng.choice(CITIES[country]), country, p, cat, qty, f"{price:.2f}",
                         rng.choices(CHANNELS, CHANNEL_W)[0]])
            oid += 1
    return rows


def make_messy(rng, rows, dupes):
    """Break some rows on purpose (each rule hits a few dozen rows), then add duplicates."""
    out = [r[:] for r in rows]
    for r in out:
        x = rng.random()
        if x < 0.06:   r[1] = date.fromisoformat(r[1]).strftime("%d/%m/%Y")           # other date format
        elif x < 0.10: r[4] = rng.choice([r[4].upper(), r[4].lower(), f"  {r[4]} "])  # messy country
        elif x < 0.13: r[6] = rng.choice([r[6].upper(), r[6].lower(), f"{r[6]} "])    # messy category
        elif x < 0.15: r[3] = ""                                                      # no city
        elif x < 0.16: r[7] = ""                                                      # no quantity
        elif x < 0.17: r[8] = rng.choice(["", "N/A"])                                 # no price
        elif x < 0.175: r[7] = rng.choice(["0", "-1", "-2"])                          # bad quantity
        elif x < 0.18: r[0] = ""                                                      # no order id
    out += [r[:] for r in rng.sample(out, dupes)]                                     # exact duplicates
    rng.shuffle(out)
    return out


def write(name, rows):
    with open(HERE / name, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(HEADER)
        w.writerows(rows)
    print(f"{name}: {len(rows)} rows")


if __name__ == "__main__":
    rng = random.Random(2026)
    h1 = orders(rng, 100001, date(2026, 1, 1), 181, 26)
    write("sales_2026_h1.csv", make_messy(rng, h1, 150))
    jul = orders(rng, h1[-1][0] + 1, date(2026, 7, 1), 31, 34)
    write("sales_2026_07.csv", make_messy(rng, jul, 30))
