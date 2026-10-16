# Snowflake SQL basics on ShopFlow

You already know SQL from the lab. This lesson covers what is **different in Snowflake**: how
objects are organised, how tables are rebuilt and dropped, where the metadata lives, the
timestamp types — and the classic join pitfalls, on ShopFlow data. At the end you'll have built
a small Silver and Gold layer.

Every SQL file in this part starts with:

```sql
use role accountadmin;
use warehouse PRACTICE_WH;
use schema SHOPFLOW.BRONZE;
```

## Concept

### Database → schema → objects
Snowflake names everything with three parts: **`DATABASE.SCHEMA.OBJECT`** — e.g.
`SHOPFLOW.BRONZE.ORDERS`. That's the same three levels as the lab's **catalog.namespace.table**
(`iceberg.bronze.orders`). Objects are tables, views, stages, streams, tasks, functions…

```
SHOPFLOW
├── BRONZE    raw copy of the source
├── SILVER    cleaned / typed / joined
└── GOLD      business-ready marts
```

### Rebuilding tables and views
- **`create or replace table … as select`** (CTAS) builds a new table from a query and swaps it in
  **atomically** — readers see the old table until the new one is complete.
- **`create or replace view`** only replaces the definition. A view stores no data.
- The replaced table isn't gone at once — it's kept in **Time Travel** for a while
  ([Cloning, Time Travel & table types](clone-time-travel.md)).

### Dropping objects
- `drop table` for tables, `drop view` for views — Snowflake errors if the type doesn't match.
- `if exists` avoids an error when the object is already gone.
- **`undrop table` / `undrop schema` / `undrop database`** bring back a dropped object within its
  Time Travel period. The lab's Iceberg tables have no `UNDROP`.

### Timestamps
| Type | Stores | Shows |
|---|---|---|
| `TIMESTAMP_NTZ` | wall-clock time, **no** time zone | as stored |
| `TIMESTAMP_LTZ` | an instant in UTC | in your **session** time zone |
| `TIMESTAMP_TZ` | the time **and** its offset | with the offset |

ShopFlow uses `TIMESTAMP_NTZ` (like Postgres `TIMESTAMP`). There is **no automatic "last
modified" column** and no `ON UPDATE` trigger: `default current_timestamp()` fills a value on
insert only, so an `updated_at` column must be set by whoever updates the row.

## Lab

### 1 · Look at the metadata: `INFORMATION_SCHEMA`
Every database has an `INFORMATION_SCHEMA` schema with views about its objects:

```sql
select table_name, table_type, row_count, bytes, created, last_altered
from SHOPFLOW.INFORMATION_SCHEMA.TABLES
where table_schema = 'BRONZE'
order by row_count desc;
```

- **`table_type`** — `BASE TABLE` or `VIEW`.
- **`row_count`, `bytes`** — kept by Snowflake as metadata; reading them scans no data.
- **`last_altered`** — the last change of any kind to the table. dbt's source freshness uses it
  when a table has no timestamp column.
- Names in `INFORMATION_SCHEMA` are stored in **upper case**, so filter with `'BRONZE'`, not
  `'bronze'`.

Example output (first rows):

| TABLE_NAME | TABLE_TYPE | ROW_COUNT | … |
|---|---|---|---|
| ORDER_ITEMS | BASE TABLE | 100000 | … |
| ORDERS | BASE TABLE | 40000 | … |
| PAYMENTS | BASE TABLE | 35954 | … |

The same information as a command: `show tables in schema SHOPFLOW.BRONZE;`

### 2 · Silver: a clean orders table
Silver joins and types the data. One row per order line with the product and customer
attributes, cancelled orders removed:

```sql
create or replace table SHOPFLOW.SILVER.ORDER_LINES as
select o.order_id,
       o.order_ts,
       o.order_ts::date                as order_date,
       o.channel,
       o.status,
       o.customer_id,
       c.country,
       c.loyalty_tier,
       oi.product_id,
       p.category,
       oi.quantity,
       oi.unit_price,
       oi.quantity * oi.unit_price     as line_revenue,
       oi.quantity * (oi.unit_price - p.cost) as line_margin
from ORDERS o
join ORDER_ITEMS oi on oi.order_id   = o.order_id
join CUSTOMERS   c  on c.customer_id = o.customer_id
join PRODUCTS    p  on p.product_id  = oi.product_id
where o.status <> 'cancelled';

select count(*) from SHOPFLOW.SILVER.ORDER_LINES;
```

- **`::date`** — Snowflake's short cast, the same as `cast(o.order_ts as date)`.

Example: `90347` rows (100,000 lines minus the ~9,650 lines of cancelled orders).

### 3 · Gold: daily sales and a view on top

```sql
create or replace table SHOPFLOW.GOLD.DAILY_SALES as
select order_date,
       count(distinct order_id) as orders,
       sum(line_revenue)        as revenue,
       sum(line_margin)         as margin
from SHOPFLOW.SILVER.ORDER_LINES
group by order_date;

create or replace view SHOPFLOW.GOLD.MONTHLY_SALES as
select date_trunc('month', order_date) as month,
       sum(orders)  as orders,
       sum(revenue) as revenue
from SHOPFLOW.GOLD.DAILY_SALES
group by month;

select * from SHOPFLOW.GOLD.MONTHLY_SALES order by month limit 3;
```

- **`date_trunc('month', …)`** — first day of the month; the same function as in Trino.
- `group by month` — Snowflake lets you group by a **column alias** from the `select`.

Example output:

| MONTH | ORDERS | REVENUE |
|---|---|---|
| 2023-06-01 | 1451 | 2905593.39 |
| 2023-07-01 | 1490 | 2904795.29 |
| 2023-08-01 | 1534 | 3143068.60 |

Same Medallion, same SQL as the lab — you built
[a Gold mart in Spark](../unit4/spark-sql-gold.md) the same way.

### 4 · Pitfall: fan-out joins
`PAYMENTS` has one row per order. `ORDER_ITEMS` has **several** rows per order. Join both and sum
the payment — every payment is counted once **per line**:

```sql
-- WRONG: payment amount multiplied by the number of lines
select sum(p.amount) as paid
from PAYMENTS p
join ORDER_ITEMS oi on oi.order_id = p.order_id;

-- RIGHT: aggregate first, then join (or don't join at all)
select sum(amount) as paid from PAYMENTS;
```

Example: `216552448.08` vs `72240118.05` — the wrong total is **3×** too high (orders have
2.5 lines on average, and big orders have more lines). The fix is
always the same: **aggregate to the grain you need first, then join**.

### 5 · Pitfall: `count(*)` after a join
After joining orders to their lines, `count(*)` counts **lines**. To count orders, use
**`count(distinct order_id)`** — as `DAILY_SALES` above does.

### 6 · Pitfall: join keys of different types
Snowflake compares a `VARCHAR` key with a `NUMBER` key by converting the text — and fails as soon
as one value isn't a number. Cast explicitly so both sides have the same type:

```sql
select count(*)
from ORDERS o
join (select '42' as customer_id) x            -- a text key, like from a CSV
  on o.customer_id = x.customer_id::int;       -- cast explicitly
```

### 7 · Pitfall: keep rows with no match
6,000 customers signed up, but many never ordered. An inner join drops them; a **`left join`**
keeps them, and **`coalesce(…, 0)`** turns their missing count into 0:

```sql
select count(*)                                   as customers,
       count_if(coalesce(o.orders, 0) = 0)        as never_ordered
from CUSTOMERS c
left join (select customer_id, count(*) as orders
           from ORDERS group by customer_id) o
       on o.customer_id = c.customer_id;
```

- **`count_if(condition)`** — counts rows where the condition is true (Snowflake's shortcut for
  `sum(case when … then 1 else 0 end)`).

Example output: `6000 | 3542` — more than half never ordered, because orders are skewed toward a
few heavy buyers.

### 8 · Drop and undrop

```sql
drop table SHOPFLOW.GOLD.MONTHLY_SALES;             -- error: it's a view, not a table
drop view if exists SHOPFLOW.GOLD.NO_SUCH_VIEW;      -- no error, thanks to IF EXISTS
undrop table SHOPFLOW.GOLD.DAILY_SALES;              -- error: the table isn't dropped
drop table SHOPFLOW.GOLD.DAILY_SALES;
select count(*) from SHOPFLOW.GOLD.DAILY_SALES;      -- error: does not exist
undrop table SHOPFLOW.GOLD.DAILY_SALES;              -- back again, with all its rows
select count(*) from SHOPFLOW.GOLD.DAILY_SALES;
```

Run them one at a time and read each error — they're the ones you'll meet in real projects.

## Check yourself
1. A dashboard shows payment totals 2–3× too high after someone added a join to `ORDER_ITEMS`.
   Why? *(Fan-out — each payment repeated per order line. Aggregate first.)*
2. Which column in `INFORMATION_SCHEMA.TABLES` tells you a table changed recently, even without
   an `updated_at` column? *(`LAST_ALTERED`.)*
