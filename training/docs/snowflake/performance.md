# Micro-partitions, pruning & caching

## Concept

### Micro-partitions
In Spark you choose the partitioning: `partitionBy("dt")` writes one folder per date. In Snowflake
you **don't declare partitions**. Every table is split automatically into **micro-partitions** —
files of 50–500 MB (uncompressed), in the order the rows arrived.
For every micro-partition Snowflake keeps the **min and max of each column**.

One micro-partition is **one file** holding **all columns** of its rows — not one file per
column. **Inside** the file the values are grouped **column by column**: all `order_date`
values together, then all `amount` values, and so on:

```
micro-partition 1 (one file, rows 1–100,000)
├── order_date   2021-01-01, 2021-01-01, 2021-01-02, …
├── customer_id  4120, 87, 5531, …
├── channel      web, app, web, …
└── amount       12.50, 310.00, 48.99, …
```

A query that reads only `amount` reads only the `amount` part of each file and skips the
rest. Same idea as Parquet's columnar layout and row groups, from
[File & table formats](../unit1/formats.md).

### Pruning
When a query filters, say `where order_date = '2024-03-15'`, Snowflake checks those min/max
values and **skips every micro-partition whose range can't contain that date**. That's
**pruning**. It only works well when similar values sit together — if dates are scattered
randomly, every micro-partition covers nearly the whole date range and none can be skipped.

```
Loaded in date order                 Loaded in random order
partition 1: 2021-01-01 … 2021-03-31   partition 1: 2021-01-04 … 2025-12-29
partition 2: 2021-04-01 … 2021-06-30   partition 2: 2021-01-02 … 2025-12-31
partition 3: 2021-07-01 … 2021-09-30   partition 3: 2021-01-01 … 2025-12-30
→ where order_date = '2021-05-10'      → where order_date = '2021-05-10'
  reads 1 partition                      reads all 3
```

### How to avoid scattered data
Pick the column your queries **filter on most** (usually a date), then:

| Do | Why |
|---|---|
| **Load in that order.** Daily loads that append only the new day keep the table sorted by date on their own | the cheapest fix — no extra work at all |
| **Sort when you rebuild.** `create or replace table … as select … order by order_date` (CTAS with `order by`) | one-off sort, rows land grouped — you'll do it in step 3 |
| **Sort big backfills** before inserting: `insert into … select … order by order_date` | a backfill of old data in random order would scatter the table again |
| **Clustering key** on large tables: `alter table … cluster by (order_date)` | Snowflake keeps it sorted in the background — costs credits, step 4 |
| **Filter on the column itself**: `where order_date >= '2024-03-01'`, not `where to_char(order_date, 'YYYY-MM') = '2024-03'` | a function around the column can stop pruning — Snowflake can't compare its result with the min/max |
| **Few columns in the key**, coarse ones first: date before customer id | a key on a near-unique column (an id, a timestamp to the second) is expensive to keep sorted and gains little |

Check it with `system$clustering_information` (step 1): **`average_depth` close to 1** = well
grouped; high = scattered.

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

Example output:

```json
{
  "cluster_by_keys" : "LINEAR(order_date)",
  "notes" : "Clustering key columns contain high cardinality key ORDER_DATE which might result in expensive re-clustering. …",
  "total_partition_count" : 4,
  "total_constant_partition_count" : 0,
  "average_overlaps" : 3.0,
  "average_depth" : 4.0,
  "version" : "OPTIMA",
  "clustering_errors" : [ ]
}
```

| Field | Here | Means |
|---|---|---|
| `cluster_by_keys` | `LINEAR(order_date)` | the column(s) you asked about |
| `total_partition_count` | `4` | 20 million rows fit in just 4 micro-partitions — they're compressed |
| `total_constant_partition_count` | `0` | partitions where the column has **one** value only (perfectly pruned). None |
| `average_overlaps` | `3.0` | each partition's date range overlaps the **3 others** — all of them |
| `average_depth` | `4.0` | a typical date is in **4** partitions — every one. The worst possible with 4 |
| `notes` | *high cardinality …* | advice: `order_date` has many distinct values (1,826 days); keeping a clustering key on it sorted could cost credits. A warning, not an error |

**`total_constant_partition_count` when it isn't 0.** A partition is **constant** when the
column has the **same value in every row** of it — its min equals its max. Say the result were
`"total_constant_partition_count" : 30` out of 40 partitions, checked on `channel`:

```
partition 6:   channel min = app,         max = app   ← constant: only app
partition 7:   channel min = web,         max = web   ← constant: only web
partition 8:   channel min = app,         max = web   ← mixed: range includes app
partition 9:   channel min = marketplace, max = web   ← mixed: range doesn't include app
```

A filter `where channel = 'app'` checks each partition's min/max (text compares
alphabetically: `app` < `marketplace` < `web`):

| Partition | Read? | Why |
|---|---|---|
| 6 — constant `app` | **read** | every row is `app` |
| 7 — constant `web` | skipped | can't contain `app` |
| 8 — mixed `app … web` | **read** | `app` is inside its range — some rows may be `app` |
| 9 — mixed `marketplace … web` | skipped | `app` is below its min |

So it reads the constant `app` partitions **and** the mixed ones whose range includes `app`.
Constant partitions make pruning exact: a constant `web` partition is skipped for sure, and a
constant `app` partition has no wasted rows.
- Constant partitions are **finished**: automatic clustering never re-sorts them, so they cost
  no more credits.
- The **higher the share** of constant partitions, the better the table is clustered on that
  column.

You only get constant partitions when one value has **at least a whole partition of rows**. A
column with few values (`channel`: 4) on a big table gets many. `order_date` here doesn't:
20 million rows over 1,826 days is ~11,000 rows per day, far less than one partition — so even
perfectly sorted it stays `0`. That's fine; what matters for dates is a low `average_depth`.

So: the dates are fully scattered — every micro-partition holds dates from all 5 years, and a
filter on one day can't skip any of them. Your partition count may differ a little.

### 2 · Query and look at the Query Profile

```sql
select sum(amount) from BIG_ORDERS where order_date = '2023-06-15';
```

Open **Monitoring → Query History →** the query **→ Query Profile**. Click the **TableScan**
node and note **Partitions scanned** vs **Partitions total**.

Example (TableScan → Statistics):

| Statistic | Value |
|---|---|
| Bytes scanned | 69.12 MB |
| Percentage scanned from cache | 0.00% |
| Partitions scanned | **4** |
| Partitions total | 4 |

All of them — to find one day, Snowflake read the whole `order_date` and `amount` columns of
the table.

### 3 · Sort it and compare

```sql
create or replace table BIG_ORDERS_SORTED as
select * from BIG_ORDERS order by order_date;

select system$clustering_information('BIG_ORDERS_SORTED', '(order_date)');
select sum(amount) from BIG_ORDERS_SORTED where order_date = '2023-06-15';
```

Example output of `system$clustering_information`:

```json
{
  "cluster_by_keys" : "LINEAR(order_date)",
  "notes" : "Clustering key columns contain high cardinality key ORDER_DATE …",
  "total_partition_count" : 8,
  "total_constant_partition_count" : 0,
  "average_overlaps" : 0.0,
  "average_depth" : 1.0,
  "version" : "OPTIMA",
  "clustering_errors" : [ ]
}
```

| Field | Unsorted (step 1) | Sorted | Means |
|---|---|---|---|
| `total_partition_count` | 4 | 8 | the same rows in a different number of files — Snowflake picks the file sizes itself; nothing to worry about |
| `average_overlaps` | 3.0 | **0.0** | no partition's date range overlaps any other |
| `average_depth` | 4.0 | **1.0** | each date sits in exactly **one** partition — perfect |
| `total_constant_partition_count` | 0 | 0 | still 0: one day's ~11,000 rows don't fill a partition (see step 1) |

Each partition now holds one stretch of the calendar (about 7–8 months each). Open the second
query's Query Profile → **TableScan**:

| Statistic | Unsorted (step 2) | Sorted |
|---|---|---|
| Bytes scanned | 69.12 MB | **1.81 MB** |
| Percentage scanned from cache | 0.00% | 100.00% |
| Partitions scanned | 4 | **1** |
| Partitions total | 4 | 8 |

1 partition of 8, and **~38× fewer bytes** — the same answer, a fraction of the work.

- **Percentage scanned from cache** — how much came from the warehouse's local SSD (the
  **warehouse cache**, step 7) instead of storage. It can be 100% here because this warehouse
  just created that table, or because you ran the query before. To see it cold, empty the
  warehouse cache **and** switch off the result cache, then run it again:

  ```sql
  alter warehouse PRACTICE_WH suspend;          -- empties the warehouse cache
  alter session set use_cached_result = false;  -- don't reuse the earlier result
  select sum(amount) from BIG_ORDERS_SORTED where order_date = '2023-06-15';
  alter session set use_cached_result = true;
  ```

  Example (TableScan → Statistics):

  | Statistic | Value |
  |---|---|
  | Bytes scanned | 1.91 MB |
  | Percentage scanned from cache | **0.00%** |
  | Partitions scanned | **1** |
  | Partitions total | 8 |

  0% from cache, but still 1 partition of 8 — pruning and caching are separate.

  Without `use_cached_result = false`, the same query text returns **instantly from the result
  cache** (step 5): no warehouse runs, nothing is scanned — so there's no TableScan to look at,
  and in Query History it may not show under your warehouse.

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
-- run this twice: the 2nd run comes from the result cache
select channel, sum(amount) from BIG_ORDERS group by channel;
```

See the difference in the query history:

```sql
select start_time, warehouse_name, total_elapsed_time, bytes_scanned,
       left(query_text, 60) as query
from table(information_schema.query_history(result_limit => 20))
where query_text ilike 'select channel, sum(amount)%'
order by start_time desc;
```

- **`ilike`** — like `like`, but ignores upper/lower case.

Example output:

| START_TIME | WAREHOUSE_NAME | TOTAL_ELAPSED_TIME | BYTES_SCANNED | QUERY |
|---|---|---|---|---|
| 2026-10-17 00:23:36 | PRACTICE_WH | 140 | **0** | select channel, sum(amount) from BIG_ORDERS group by channel |
| 2026-10-17 00:20:27 | PRACTICE_WH | 1051 | 441972736 | select channel, sum(amount) from BIG_ORDERS group by channel |

The 2nd run (top row) scanned **0 bytes** in 140 ms — Snowflake handed back the saved result
without reading the table. That's the **result cache**: same query text, unchanged data. It
works across users too, as long as they have access to the table.

Turn it off to measure the real work:

```sql
alter session set use_cached_result = false;
select channel, sum(amount) from BIG_ORDERS group by channel;
alter session set use_cached_result = true;
```

- **`alter session set use_cached_result = false`** — only for this session. Always do this when
  you **measure** performance; otherwise you're timing the cache.

Run the history query again. Example output:

| START_TIME | WAREHOUSE_NAME | TOTAL_ELAPSED_TIME | BYTES_SCANNED | QUERY |
|---|---|---|---|---|
| 2026-10-17 00:26:17 | PRACTICE_WH | 808 | 441972736 | select channel, sum(amount) from BIG_ORDERS group by channel |
| 2026-10-17 00:23:36 | PRACTICE_WH | 140 | 0 | select channel, sum(amount) from BIG_ORDERS group by channel |
| 2026-10-17 00:20:27 | PRACTICE_WH | 1051 | 441972736 | select channel, sum(amount) from BIG_ORDERS group by channel |

With the result cache off, the 3rd run (top) reads the whole table again — the same
441,972,736 bytes as the 1st. The time differs a little (808 vs 1051 ms) — timings always vary
from run to run; **bytes scanned** is the number to compare.

### 6 · Metadata answers

```sql
select count(*), min(order_date), max(order_date) from BIG_ORDERS;
```

The profile shows **METADATA-BASED RESULT** — no table scan at all. Snowflake already knows the
row count and min/max of each micro-partition.

### 7 · Warehouse cache
The warehouse keeps the micro-partitions it read on its **local SSD** — but only while it runs.
`PRACTICE_WH` suspends after **60 seconds** idle, and suspending empties the cache. So run
these **together, in one go** (select all three lines → **Ctrl/Cmd + Enter**):

```sql
alter session set use_cached_result = false;   -- no result cache, so the table is read
select channel, sum(amount) from BIG_ORDERS group by channel;
select channel, sum(amount) from BIG_ORDERS group by channel;
```

Then compare the two runs:

```sql
select start_time, total_elapsed_time, bytes_scanned,
       percentage_scanned_from_cache
from table(information_schema.query_history(result_limit => 20))
where query_text ilike 'select channel, sum(amount)%'
order by start_time desc
limit 2;

alter session set use_cached_result = true;
```

- **`percentage_scanned_from_cache`** — the share of the bytes read from the warehouse's SSD
  instead of storage, from `0` to `1` (`1` = 100%).

Expected: both runs scan the **same bytes** (the whole table), but the 2nd reads most of them
from the cache (`percentage_scanned_from_cache` near `1`) and is usually faster. Suspend the
warehouse (`alter warehouse PRACTICE_WH suspend;`) — or wait over a minute — and the next run
is cold again: `0`.

| Cache | Skips | Needs |
|---|---|---|
| Result cache (step 5) | the **whole query** — 0 bytes scanned | same query text, unchanged data, `use_cached_result = true` |
| Warehouse cache (this step) | reading from **storage** — bytes are still scanned, from SSD | the warehouse still running since it read them |

## Check yourself
1. What's Snowflake's equivalent of Spark `partitionBy`? *(Nothing to declare — micro-partitions
   are automatic. A clustering key influences how rows are grouped into them.)*
2. Your performance test shows 0.1 s for a big query. Why might that be misleading?
   *(Result cache. Turn `use_cached_result` off while testing.)*
3. When is a clustering key worth its cost? *(Large tables, mostly filtered on the same
   columns, with poor natural clustering.)*

`BIG_ORDERS` stays for the [Cost & monitoring](cost.md) lesson; [Clean up](cleanup.md) drops it.
