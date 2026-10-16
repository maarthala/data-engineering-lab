# Cost & monitoring

## Concept
On Snowflake the bill **is** the operations dashboard: you pay per second of warehouse time, so
slow queries, idle warehouses and forgotten tasks show up directly as money. You met the views in
[The system views](system-views.md); this lesson uses them for cost. Two places to look:

| Source | Delay | History | Use for |
|---|---|---|---|
| **`SNOWFLAKE.ACCOUNT_USAGE`** views | up to ~45 min (some up to 3 h) | 365 days | reports, trends, the whole account |
| **`INFORMATION_SCHEMA`** table functions | real time | 7–14 days | "what just happened" in one database |

A **resource monitor** caps credits: it notifies at a percentage and can **suspend** warehouses
when the quota is used up.

What to watch for:
- **Spilling** to local / remote storage — the warehouse is too small for the query.
- **Low pruning** — `partitions_scanned ≈ partitions_total` on big tables
  ([Micro-partitions, pruning & caching](performance.md)).
- **Long `auto_suspend`** — warehouses billed while idle.
- **Tasks and dynamic tables** left running.

## Lab

```sql
use role accountadmin;
use warehouse PRACTICE_WH;
```

### 1 · Credits per warehouse

```sql
select warehouse_name, sum(credits_used) as credits
from snowflake.account_usage.warehouse_metering_history
where start_time >= dateadd(day, -7, current_timestamp())
group by warehouse_name
order by credits desc;
```

- **`dateadd(unit, n, value)`** — add `n` units; a negative `n` goes back in time.

Example output: `PRACTICE_WH | 0.62` — the whole course so far costs well under one credit. If
it's empty, your usage is newer than the view's delay; try again later.

### 2 · The slowest queries, with pruning and spilling

```sql
select query_id,
       left(query_text, 60)            as query,
       warehouse_name,
       total_elapsed_time / 1000       as seconds,
       partitions_scanned,
       partitions_total,
       bytes_spilled_to_local_storage
from snowflake.account_usage.query_history
where start_time >= dateadd(day, -1, current_timestamp())
order by total_elapsed_time desc
limit 20;
```

- **`left(text, n)`** — the first `n` characters.

The `BIG_ORDERS` queries are near the top: compare `partitions_scanned` on `BIG_ORDERS` vs
`BIG_ORDERS_SORTED`.

The same in real time, for your session's recent queries:

```sql
select query_text, total_elapsed_time, rows_produced
from table(information_schema.query_history(result_limit => 10))
order by start_time desc;
```

### 3 · Storage

```sql
select usage_date, database_name,
       average_database_bytes / power(1024, 2) as mb,
       average_failsafe_bytes / power(1024, 2) as failsafe_mb
from snowflake.account_usage.database_storage_usage_history
where usage_date >= dateadd(day, -7, current_date())
order by usage_date desc, mb desc;
```

`SHOPFLOW` holds `BIG_ORDERS` (20 M rows) — still only a few hundred MB compressed. Fail-safe
bytes show up for permanent tables you replaced or dropped.

### 4 · A resource monitor

```sql
create or replace resource monitor PRACTICE_MONITOR
  with credit_quota    = 10
       frequency       = monthly
       start_timestamp = immediately
  triggers on 80  percent do notify
           on 100 percent do suspend;

alter warehouse PRACTICE_WH set resource_monitor = PRACTICE_MONITOR;

show resource monitors;
```

- **`do notify`** — e-mail account admins (turn on notifications under your profile).
- **`do suspend`** — suspend the warehouses after running queries finish;
  **`do suspend_immediate`** cancels them too.

Keep it for the rest of your trial — a 10-credit monthly cap on `PRACTICE_WH`.

### 5 · Budgets in Snowsight
**Admin → Cost Management** shows credits per day, per warehouse and per service type — the same
data as step 1, as charts. Check it once a week while you practise.

## Check yourself
1. A query spills gigabytes to remote storage. What do you change? *(Use a bigger warehouse for
   that query, or make the query process less data.)*
2. You need the queries of the last 10 minutes. `ACCOUNT_USAGE.QUERY_HISTORY` shows nothing.
   Why? *(Its delay. Use the `information_schema.query_history()` table function.)*
