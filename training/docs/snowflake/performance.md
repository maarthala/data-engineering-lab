# Micro-partitions, pruning & caching

## Concept

### Micro-partitions
In Spark you choose the partitioning: `partitionBy("dt")` writes one folder per date. In Snowflake
you **don't declare partitions**. Every table is split automatically into **micro-partitions** —
files of 50–500 MB (uncompressed), stored **column by column**, in the order the rows arrived.
For every micro-partition Snowflake keeps the **min and max of each column**.

### Pruning
When a query filters, say `where order_date = '2024-03-15'`, Snowflake checks those min/max
values and **skips every micro-partition whose range can't contain that date**. That's
**pruning**. It only works well when similar values sit together — if dates are scattered
randomly, every micro-partition covers nearly the whole date range and none can be skipped.

### Clustering
How well a table's rows are grouped by a column is its **clustering**. Data loaded in date order
is naturally well clustered on the date. A **clustering key** (`cluster by (…)`) asks Snowflake to
keep re-sorting the table in the background (**automatic clustering** — it costs credits). It's
worth it for large tables (many GB to TB) that are always filtered on the same columns — rarely
on small ones.

It's the managed cousin of what you did by hand in
[table maintenance](../unit4/table-maintenance.md): Iceberg `rewrite_data_files` with a sort
order, Delta `OPTIMIZE … ZORDER BY`.

### Caching — three caches
| Cache | Where | Lasts | Effect |
|---|---|---|---|
| **Result cache** | cloud services | 24 h (reset each reuse, max 31 days) | same query text + unchanged data → instant, **no warehouse needed** |
| **Warehouse cache** | the warehouse's local SSD | until the warehouse suspends | faster re-reads of the same micro-partitions |
| **Metadata** | cloud services | always | `count(*)`, `min`, `max` answered without scanning |

## Lab

ShopFlow's 100,000 order lines fit in one or two micro-partitions — too small to see pruning. So
make a big version of `ORDERS`: **20 million** synthetic orders, with ShopFlow's channels and
statuses, in **random date order**.

```sql
use role accountadmin;
use warehouse PRACTICE_WH;
create schema if not exists SHOPFLOW.PRACTICE;
use schema SHOPFLOW.PRACTICE;

create or replace table BIG_ORDERS as
select seq4()                                                          as order_id,
       dateadd(day, uniform(0, 1825, random()), '2021-01-01'::date)     as order_date,
       uniform(1, 6000, random())                                      as customer_id,
       array_construct('web','app','marketplace','wholesale')[uniform(0, 3, random())]::varchar as channel,
       uniform(500, 50000, random()) / 100                             as amount
from table(generator(rowcount => 20000000));
```

- **`table(generator(rowcount => n))`** — produces `n` empty rows to build from.
- **`seq4()`** — a number per row (0, 1, 2, …; can have gaps — fine for an id here).
- **`uniform(a, b, random())`** — a random whole number from `a` to `b`, both included.
- **`array_construct(…)[i]`** — the `i`-th value of a list, counting from **0**.

This takes ~30 s on X-Small.

### 1 · How well is it clustered?

```sql
select system$clustering_information('BIG_ORDERS', '(order_date)');
```

- **`system$clustering_information(table, '(columns)')`** — returns JSON describing how the
  micro-partitions overlap on those columns. Read two fields:
  - **`average_depth`** — how many micro-partitions overlap at a typical value. **1 is perfect**;
    high means the dates are spread over many partitions.
  - **`average_overlaps`** — how many other partitions each one overlaps with.

Here both are high: every micro-partition holds dates from all 5 years.

### 2 · Query and look at the Query Profile

```sql
select sum(amount) from BIG_ORDERS where order_date = '2023-06-15';
```

Open **Monitoring → Query History →** the query **→ Query Profile**. Click the **TableScan**
node and note **Partitions scanned** vs **Partitions total** — nearly all of them.

### 3 · Sort it and compare

```sql
create or replace table BIG_ORDERS_SORTED as
select * from BIG_ORDERS order by order_date;

select system$clustering_information('BIG_ORDERS_SORTED', '(order_date)');
select sum(amount) from BIG_ORDERS_SORTED where order_date = '2023-06-15';
```

`average_depth` is now close to 1, and the Query Profile shows **only a few partitions
scanned** — the same answer, a fraction of the work.

### 4 · A clustering key

```sql
alter table BIG_ORDERS cluster by (order_date);
show tables like 'BIG_ORDERS';         -- the cluster_by column shows LINEAR(ORDER_DATE)
```

Snowflake now re-clusters `BIG_ORDERS` in the background (it takes a while and uses credits).
Turn it off again so it doesn't keep spending:

```sql
alter table BIG_ORDERS suspend recluster;
```

### 5 · Result cache

```sql
-- run this twice: the 2nd run is instant (look for "QUERY RESULT REUSE" in the profile)
select channel, sum(amount) from BIG_ORDERS group by channel;

-- turn the result cache off to measure the real work
alter session set use_cached_result = false;
select channel, sum(amount) from BIG_ORDERS group by channel;
alter session set use_cached_result = true;
```

- **`alter session set use_cached_result = false`** — only for this session. Always do this when
  you **measure** performance; otherwise you're timing the cache.

### 6 · Metadata answers

```sql
select count(*), min(order_date), max(order_date) from BIG_ORDERS;
```

The profile shows **METADATA-BASED RESULT** — no table scan at all. Snowflake already knows the
row count and min/max of each micro-partition.

### 7 · Warehouse cache
Run the same `group by channel` query with `use_cached_result = false` twice more. In the
profile, **Percentage scanned from cache** goes up on the 2nd run — the micro-partitions are
already on the warehouse's SSD. Suspend the warehouse (`alter warehouse PRACTICE_WH suspend;`)
and the next run starts cold again.

## Check yourself
1. What's Snowflake's equivalent of Spark `partitionBy`? *(Nothing to declare — micro-partitions
   are automatic. A clustering key influences how rows are grouped into them.)*
2. Your performance test shows 0.1 s for a big query. Why might that be misleading?
   *(Result cache. Turn `use_cached_result` off while testing.)*
3. When is a clustering key worth its cost? *(Large tables, mostly filtered on the same
   columns, with poor natural clustering.)*

`BIG_ORDERS` stays for the [Cost & monitoring](cost.md) lesson; [Clean up](cleanup.md) drops it.
