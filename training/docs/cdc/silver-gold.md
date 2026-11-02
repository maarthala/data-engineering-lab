# From changes to Silver and Gold

## Concept
The CDC tables are your **Bronze** layer: the source as it is, plus OLake's columns and the
tombstones ([Medallion architecture](../unit1/medallion.md)). Nobody downstream should have to
remember `WHERE _op_type <> 'd'`. So you build:

- **Silver**: one clean table per shop table, with deleted rows out and OLake's columns off. It is
  *the shop as it is now*.
- **Gold**: the answers the business asks for, built on Silver.

```mermaid
flowchart LR
  SRC["live shop<br/>(Postgres)"] -- "📡 job: CDC" --> B["Bronze<br/>my.shop_cdc.*<br/>+ _op_type, tombstones"]
  B -- "filter d, drop _ columns" --> S["Silver<br/>my.silver.shop_*"]
  S -- "join + aggregate" --> G["Gold<br/>revenue by category…"]
```

## Lab
### 1 · Silver: one function for all four tables
```python
META = ["_olake_id", "_olake_timestamp", "_op_type", "_cdc_timestamp", "_cdc_lsn"]

def current(table):
    """The table as it is in the shop now: deleted rows out, OLake's columns off."""
    return spark.table(f"my.shop_cdc.{table}").filter("_op_type <> 'd'").drop(*META)

spark.sql("CREATE NAMESPACE IF NOT EXISTS my.silver")
for t in ["customers", "products", "orders", "order_items"]:
    current(t).writeTo(f"my.silver.shop_{t}").using("iceberg").createOrReplace()
```

**Read it step by step:**

- **`META`**: the five columns OLake adds, kept in one list so you write them only once.
- **`.filter("_op_type <> 'd'")`**: drops the tombstones.
- **`.drop(*META)`**: `drop` takes column names as separate arguments. The **`*`** in front of a
  list *unpacks* it: `drop(*META)` is the same as
  `drop("_olake_id", "_olake_timestamp", "_op_type", "_cdc_timestamp", "_cdc_lsn")`.
- **`current(t)`** is a function, so all four tables get exactly the same treatment. When the
  rule changes, you change it in one place.
- **`createOrReplace()`**: Silver is rebuilt from Bronze every time. Bronze is small enough for
  that here. For big tables you'd `MERGE` only the changed keys
  ([MERGE & upserts](../unit2/merge.md)).

Check the counts:

```python
spark.sql("""
SELECT 'customers' AS table_name, count(*) AS rows FROM my.silver.shop_customers
UNION ALL SELECT 'products', count(*) FROM my.silver.shop_products
UNION ALL SELECT 'orders', count(*) FROM my.silver.shop_orders
UNION ALL SELECT 'order_items', count(*) FROM my.silver.shop_order_items""").show()
```

```
+-----------+----+
| table_name|rows|
+-----------+----+
|  customers| 182|
|   products|  18|
|     orders| 594|
|order_items|1175|
+-----------+----+
```

### 2 · Does the lake match the shop?
The same status count as in [Meet the live shop](live-shop.md), once from Silver and once
from the shop itself:

```python
import pandas as pd
from sqlalchemy import create_engine
shop = create_engine("postgresql+psycopg2://reader:reader@cdc-db:5432/shop")

spark.sql("""SELECT status, count(*) AS orders FROM my.silver.shop_orders
             GROUP BY status ORDER BY orders DESC""").show()
pd.read_sql("SELECT status, count(*) AS orders FROM orders GROUP BY status ORDER BY orders DESC", shop)
```

```
+---------+------+
|   status|orders|
+---------+------+
|   placed|   332|
|     paid|   164|
|  shipped|    62|
|delivered|    36|
+---------+------+

      status  orders
0     placed     335
1       paid     163
2    shipped      62
3  delivered      37
```

Close, but not equal, and that's correct. The lake shows the shop **as of the last run**. The
shop has moved on since: new orders came in and some orders moved to their next status.
This gap is called **latency** (or *freshness*). With a 15-minute schedule the lake is at most
about 15 minutes behind. Run the job, rebuild Silver, and the gap closes, then opens again.

!!! tip "Reconciling source and lake"
    Comparing counts between source and target is a standard check after every load. With a
    live source, compare *as of* the same moment, or allow for the changes since the run.

### 3 · Gold: revenue by category
```python
spark.sql("""
SELECT p.category,
       count(DISTINCT o.order_id)          AS orders,
       round(sum(i.quantity * i.price), 2) AS revenue
FROM my.silver.shop_orders o
JOIN my.silver.shop_order_items i ON i.order_id = o.order_id
JOIN my.silver.shop_products p    ON p.product_id = i.product_id
GROUP BY p.category
ORDER BY revenue DESC""").show()
```

```
+-----------+------+---------+
|   category|orders|  revenue|
+-----------+------+---------+
|electronics|   259|158468.14|
|       home|   228| 38119.51|
|     sports|   150| 18625.05|
|accessories|   184|  13060.2|
|       food|   124|  4212.85|
|     office|   137|  2455.81|
+-----------+------+---------+
```

- **`i.quantity * i.price`**: `order_items.price` is the price **at the time of the order**.
  The shop changes product prices now and then, and those changes must not rewrite old
  revenue. That's why the price is copied onto each item.
- Cancelled orders are gone from Silver, and so are their items, so they don't count as revenue.

### 4 · Gold: customers by tier
```python
spark.sql("""
SELECT c.tier,
       count(DISTINCT c.customer_id) AS customers,
       count(o.order_id)             AS orders,
       round(count(o.order_id) / count(DISTINCT c.customer_id), 1) AS orders_per_customer
FROM my.silver.shop_customers c
LEFT JOIN my.silver.shop_orders o ON o.customer_id = c.customer_id
GROUP BY c.tier ORDER BY customers DESC""").show()
```

```
+------+---------+------+-------------------+
|  tier|customers|orders|orders_per_customer|
+------+---------+------+-------------------+
|bronze|       91|   304|                3.3|
|silver|       58|   181|                3.1|
|  gold|       33|   109|                3.3|
+------+---------+------+-------------------+
```

- **`LEFT JOIN`** keeps customers without orders, so they still count as customers.
- **`count(DISTINCT c.customer_id)`**, not `count(*)`: after the join a customer appears once
  per order. `count(*)` would count orders, not customers
  ([Join pitfalls](../unit2/join-pitfalls.md)).
- A customer's **tier** is the one they have **now**. If a customer moved from bronze to gold
  last week, all their old orders now count under gold. If the business wants "the tier at the
  time of the order", you need history: [Slowly changing dimensions (SCD2)](../recipes/scd2.md).

## Check yourself
1. Why rebuild Silver with `createOrReplace()` instead of appending?
2. Silver shows 332 placed orders and the shop shows 335. Which one is wrong?
3. Why is revenue computed from `order_items.price` and not from `products.price`?

??? success "Answers"
    1. Bronze already holds the current state. Appending would add every order again on each
       rebuild. (For large tables: `MERGE` the changed keys instead.)
    2. Neither. The lake is as of the last run; the shop kept changing. That's latency.
    3. `products.price` is today's price. A price change would silently rewrite past revenue.

## You can now…
- turn CDC Bronze tables into clean Silver tables with one reusable function;
- reconcile lake and source, and explain the gap as latency;
- build Gold answers on Silver and avoid the double-counting trap.
