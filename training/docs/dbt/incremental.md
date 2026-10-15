# Incremental models

## Concept
A **table** model is rebuilt from scratch on every run: all 1,000 orders, every time. That's
fine for 1,000 rows. For a billion, it's hours of work to change yesterday's few thousand.

An **incremental** model builds the full table **once**. After that, each run only selects the
**new or changed** rows and **merges** them in: update the rows that are already there, insert
the new ones. It's the `MERGE` (upsert) from [MERGE & upserts](../unit2/merge.md) and
[Transform to Silver](../unit4/transform-silver.md), with dbt writing the statement for you.

You tell dbt three things:

| Setting | Meaning |
|---|---|
| **`materialized='incremental'`** | build once, then add to it |
| **`unique_key='order_id'`** | how to recognise a row that's already in the table |
| a **filter** inside `{% if is_incremental() %}` | which rows count as "new" on later runs |

## Lab
Continue in `~/work/my_files/dbt/shop`.

### 1 · The incremental model
New file **`models/marts/fct_orders.sql`**, a fact table of orders
([Dimensional modeling](../unit1/dimensional-modeling.md)):

```sql
{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='merge'
) }}

select
    order_id,
    customer_id,
    channel,
    order_date,
    status,
    current_timestamp() as loaded_at
from {{ ref('stg_orders') }}

{% if is_incremental() %}
-- only the last 3 days of orders already in the table, and anything newer
where order_date >= (select date_sub(max(order_date), 3) from {{ this }})
{% endif %}
```

- **`{{ config(…) }}`** sets the materialization in the model, as in
  [Models, ref() and materializations](models-ref.md). **`incremental_strategy='merge'`**: apply
  the changes with `MERGE` (Iceberg supports it).
- **`current_timestamp() as loaded_at`**: when the row was written. It shows which rows each run
  touched.
- **`{% if … %} … {% endif %}`**: a Jinja **if**. `{{ … }}` *prints* a value; `{% … %}` is a
  *statement* (if, for, set) and prints nothing itself. The `where` line is only part of the SQL
  when the condition is true.
- **`is_incremental()`** is true when the table **already exists** and this is a normal run.
  On the first run, it's false: no filter, every row.
- **`{{ this }}`**: the model's own table, `gold.fct_orders`.
- **`date_sub(d, 3)`**: the date 3 days before `d`. So "new" = dated at most 3 days before the
  newest order already loaded, or later. Why not just *after* the newest? Next step shows it.

### 2 · First run: the full table

```bash
dbt run -s fct_orders
```
```text
16:02:44  1 of 1 START sql incremental model gold.fct_orders ............................. [RUN]
16:02:45  1 of 1 OK created sql incremental model gold.fct_orders ........................ [OK in 0.98s]
16:02:45  Done. PASS=1 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=1
```

In a notebook:

```sql
%%sql
SELECT count(*) AS orders, max(order_date) AS newest FROM my.gold.fct_orders
```

| | orders | newest |
|---|---|---|
| 0 | 1000 | 2025-05-29 |

All 1,000 orders: the table didn't exist, so `is_incremental()` was false.

### 3 · New data arrives
Simulate the next day's Bronze load in your starter table: two new orders, and two existing
orders cancelled, **998** (dated 2025-05-29) and **557** (2025-05-25). In a notebook:

```sql
%%sql
INSERT INTO my.bronze.sample_orders VALUES
  (1001, 42, 'web', TIMESTAMP_NTZ '2025-05-30 09:15:00', 'placed', 'EUR', NULL),
  (1002, 77, 'app', TIMESTAMP_NTZ '2025-05-30 11:40:00', 'placed', 'USD', NULL)
```
```sql
%%sql
UPDATE my.bronze.sample_orders SET status = 'cancelled' WHERE order_id IN (998, 557)
```

- **`INSERT INTO … VALUES (…), (…)`** adds rows, one `( )` per row, values in column order.
  **`TIMESTAMP_NTZ '…'`** writes a timestamp value (the column's type: a timestamp without time
  zone).
- **`UPDATE … SET status = … WHERE …`** changes the matching rows in place, as `MERGE` does
  for matched rows.

### 4 · Second run: only the changes

```bash
dbt run -s fct_orders
```
```text
16:02:58  1 of 1 START sql incremental model gold.fct_orders ............................. [RUN]
16:03:01  1 of 1 OK created sql incremental model gold.fct_orders ........................ [OK in 2.76s]
16:03:01  Done. PASS=1 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=1
```

What dbt ran is in `target/run/shop/models/marts/fct_orders.sql`: your `SELECT` (with the
`where`) goes into a temporary view `fct_orders__dbt_tmp`, then:

```sql
merge into gold.fct_orders as DBT_INTERNAL_DEST
    using fct_orders__dbt_tmp as DBT_INTERNAL_SOURCE
    on
            DBT_INTERNAL_SOURCE.order_id = DBT_INTERNAL_DEST.order_id
    when matched then update set
       *
    when not matched then insert *
```

The **`unique_key`** became the `ON` condition. The `where` selected orders from **2025-05-26**
on (the newest loaded date, 2025-05-29, minus 3 days): 5 rows instead of 1,002. Check the four
orders that changed:

```sql
%%sql
SELECT order_id, order_date, status, loaded_at FROM my.gold.fct_orders
WHERE order_id IN (557, 998, 1001, 1002) ORDER BY order_id
```

| | order_id | order_date | status | loaded_at |
|---|---|---|---|---|
| 0 | 557 | 2025-05-25 | shipped | 2026-10-13 16:02:44.742014 |
| 1 | 998 | 2025-05-29 | cancelled | 2026-10-13 16:02:59.598102 |
| 2 | 1001 | 2025-05-30 | placed | 2026-10-13 16:02:59.598102 |
| 3 | 1002 | 2025-05-30 | placed | 2026-10-13 16:02:59.598102 |

- **1001, 1002**: new, **inserted** (`count(*)` is now 1,002).
- **998**: already there, inside the window, so it was **updated** to `cancelled`.
- **557**: also cancelled in Bronze, but dated 2025-05-25, **outside** the 3-day window: not
  selected, still `shipped`, with the first run's `loaded_at`.

That's the trade-off of every incremental model: it only sees what the filter lets through.
The **lookback window** (3 days here) catches late changes to recent orders. Choose it from how
late your data really changes: a filter of only "newer than the newest" would also have missed
998.

### 5 · Rebuild everything: `--full-refresh`
When the filter missed something, or you changed the model's columns, rebuild from scratch:

```bash
dbt run -s fct_orders --full-refresh
```
```text
16:03:12  1 of 1 START sql incremental model gold.fct_orders ............................. [RUN]
16:03:13  1 of 1 OK created sql incremental model gold.fct_orders ........................ [OK in 0.94s]
16:03:13  Done. PASS=1 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=1
```

```sql
%%sql
SELECT order_id, status FROM my.gold.fct_orders WHERE order_id IN (557, 998) ORDER BY order_id
```

| | order_id | status |
|---|---|---|
| 0 | 557 | cancelled |
| 1 | 998 | cancelled |

**`--full-refresh`** makes `is_incremental()` false for this run: the table is rebuilt from all
rows, and 557 is right again. On a big table that's the slow, expensive run, so it's a repair
tool, not a habit.

### 6 · Put your starter table back
Undo the simulated day, so the counts in other lessons still match:

```sql
%%sql
DELETE FROM my.bronze.sample_orders WHERE order_id IN (1001, 1002)
```
```sql
%%sql
UPDATE my.bronze.sample_orders SET status = 'delivered' WHERE order_id = 998
```
```sql
%%sql
UPDATE my.bronze.sample_orders SET status = 'shipped' WHERE order_id = 557
```

Then `dbt run -s fct_orders --full-refresh` once more.

!!! info "When incremental, when table?"
    Start with **table**: simpler, always correct. Switch to **incremental** when rebuilding
    takes too long, and only with a reliable column to filter on (a date or a loaded-at
    timestamp) plus a `unique_key`. For data that is only ever **added**, never changed (event
    logs), use `incremental_strategy='append'` with no `unique_key`: dbt runs a plain
    `insert into … select …`, with no matching.

## 🎯 This runs unchanged on Azure, Databricks, Snowflake & Fabric
`is_incremental()`, `{{ this }}`, `unique_key` and `--full-refresh` are core dbt. The strategies
differ per adapter: `merge` on Databricks (Delta) and Snowflake, `delete+insert` or `append`
on some others. The model code stays the same.

## You can now…
- Build an **incremental** model with `config(materialized='incremental', unique_key=…)`
- Filter new rows with **`{% if is_incremental() %}`** and **`{{ this }}`**
- Explain the **`MERGE`** dbt runs, and what the lookback window catches and misses
- Repair or rebuild an incremental table with **`--full-refresh`**
- Choose between **table** and **incremental**
