# Meet the live shop

## Concept
Before you copy a database, look at it: which tables, how many rows, how fast it changes, and
whether it is ready for CDC. Here that database is the lab's **live shop**:

| | |
|---|---|
| Host · port | `lab-db` · `5432` |
| Database | `shop` |
| Login | `reader` / `reader` (read only, plus the right to replicate) |
| Tables | `customers`, `products`, `orders`, `order_items` (schema `public`) |

A small program keeps it busy, one change every 2–10 seconds:

| What happens | Change in the database |
|---|---|
| a new order (with 1–3 items) | `INSERT` into `orders` and `order_items`, then an `UPDATE` of the order's `total` |
| an order moves on: placed → paid → shipped → delivered | `UPDATE orders SET status = …` |
| an order is cancelled | `DELETE` from `orders` (its items go too) |
| a new customer · a customer changes tier · a price changes | `INSERT` / `UPDATE` |

### The company's other systems
The web shop isn't the company's only system. Two more live databases on `lab-db` keep changing
with it, read with the same `reader` / `reader` login and ready for CDC:

| Database | System | Tables | What doesn't match the web shop |
|---|---|---|---|
| `store` | the stores' tills | `stores`, `items`, `sales`, `sale_lines` | products by **SKU** (`SKU-0001` = web product 1) · money in **cents** · `sold_at` in the store's **local time, without a time zone** · returns are lines with a **negative** `qty` |
| `loyalty` | the loyalty card | `members`, `points` | a member's **email** is the web customer's in another spelling (case, spaces) · some members never shopped online · a store sale knows only the **card number** |

A web order by a card member earns points in `loyalty`, and so does a store sale with a card.
Joining the three into one view of customers and sales is classic data engineering: matching
keys, units and time zones. That's the [Tidewell project](../project/index.md): the three systems
belong to a retailer called Tidewell. 🐘 **My databases** shows all three with their tables.

## Lab
Open a notebook. You connect the same way as in
[SQL from Python](../unit3/databases.md): SQLAlchemy's `create_engine` plus pandas'
`read_sql`.

### 1 · What's in the shop
```python
import pandas as pd
from sqlalchemy import create_engine

shop = create_engine("postgresql+psycopg2://reader:reader@lab-db:5432/shop")

pd.read_sql("""SELECT 'customers' AS table_name, count(*) AS rows FROM customers
               UNION ALL SELECT 'products', count(*) FROM products
               UNION ALL SELECT 'orders', count(*) FROM orders
               UNION ALL SELECT 'order_items', count(*) FROM order_items""", shop)
```

```
    table_name  rows
0    customers   176
1     products    18
2       orders   582
3  order_items  1147
```

Your numbers will be different: the shop never stops. Run the cell again a minute later and
watch them grow.

### 2 · Watch it change
The newest changes first:

```python
pd.read_sql("""SELECT order_id, customer_id, status, total, updated_at
               FROM orders ORDER BY updated_at DESC LIMIT 5""", shop)
```

```
   order_id  customer_id   status    total                       updated_at
0       681          107   placed    79.08 2026-11-02 16:19:08.500624+00:00
1       656           78     paid   554.42 2026-11-02 16:18:59.023884+00:00
2       680           79   placed  2768.58 2026-11-02 16:18:56.363878+00:00
3       110            7     paid   155.00 2026-11-02 16:18:46.450697+00:00
4       302           68  shipped   226.89 2026-11-02 16:18:34.149681+00:00
```

Run it a few times. New orders appear at the top, and older ones move on to *paid* or
*shipped*. And the cancelled ones? They simply vanish. That's exactly what a timestamp-based
copy can't see, and why you'll use CDC.

How the orders are spread over their life cycle:

```python
pd.read_sql("SELECT status, count(*) AS orders FROM orders GROUP BY status ORDER BY orders DESC", shop)
```

```
      status  orders
0     placed     329
1       paid     156
2    shipped      62
3  delivered      35
```

Keep this result. In the next lessons you'll build the same table from the lake and compare.

### 3 · Is it ready for CDC?
Three things must be true. Postgres can tell you each of them:

```python
pd.read_sql("SHOW wal_level", shop)
```

```
  wal_level
0   logical
```

- **`SHOW wal_level`**: shows a server setting. It must be `logical`. The default, `replica`,
  doesn't keep enough detail to rebuild changed rows. Changing it needs a restart of the
  database, so on a real system it's a request to the database team.

```python
pd.read_sql("SELECT pubname, puballtables FROM pg_publication", shop)
```

```
     pubname  puballtables
0  olake_pub          True
```

- **`pg_publication`**: one of Postgres's *catalog tables* (tables that describe the database
  itself). `olake_pub` publishes all tables (`puballtables = True`).

```python
pd.read_sql("SELECT slot_name, active FROM pg_replication_slots", shop)
```

```
           slot_name  active
0  demouser_bf4bfd71   False
```

- **`pg_replication_slots`**: every bookmark in the change log. You'll see one per member who
  set up CDC on this shop: `<member>_<random>`. `active` is `True` only while a job is
  reading. Yours appears in the next lesson.

### 4 · Read only, on purpose
The `reader` login can't change the shop:

```python
from sqlalchemy import text
with shop.connect() as c:
    c.execute(text("UPDATE orders SET status = 'paid' WHERE order_id = 1"))
```

```
ProgrammingError: (psycopg2.errors.InsufficientPrivilege) permission denied for table orders
```

- **`text(...)`**: wraps a SQL string so SQLAlchemy runs it as it is.
- **`with shop.connect() as c`**: opens one connection and closes it at the end of the block.

That's the rule for any production source: the sync job gets a login that **reads** and may
**replicate**, never one that writes.

## You can now…
- connect to a source database from a notebook and size up its tables;
- check that a Postgres database is ready for CDC: `wal_level`, publication, slots;
- explain why the sync login is read-only.
