# Streams, tasks, SCD2 & dynamic tables

Three ways to keep a table up to date inside Snowflake — no Airflow, no Spark job:

| Way | You write | Snowflake does |
|---|---|---|
| **Stream + task** | the change logic (`MERGE`) and a schedule | tracks the changes, runs it on schedule |
| **Plain SQL SCD2** | an `UPDATE` + an `INSERT` | nothing extra — the pattern behind dbt snapshots |
| **Dynamic table** | only the `SELECT` and how fresh it must be | everything: change tracking, refresh, scheduling |

## Concept

### Streams — change data capture
A **stream** on a table records the **rows that changed** (insert, update, delete) since the
stream was last **consumed**. Selecting from it shows the changed rows plus three columns:

- **`METADATA$ACTION`** — `INSERT` or `DELETE`
- **`METADATA$ISUPDATE`** — `TRUE` when the row is half of an update (an update shows up as a
  `DELETE` of the old row + an `INSERT` of the new one)
- **`METADATA$ROW_ID`** — a stable id for the row

A `select` doesn't move the stream on. Using the stream in a **DML statement** (`insert`,
`merge`, …) that commits **consumes** it — afterwards it's empty until new changes arrive.

### Tasks — scheduled SQL
A **task** runs one SQL statement (or a procedure) on a **schedule** (`'5 MINUTE'` or a cron
expression) or **after another task** — chains of tasks form a **task graph**, a DAG like
Airflow's. Tasks are created **suspended**; you `resume` them.

### Dynamic tables
A **dynamic table** is a table defined by a query plus a **target lag** — "never more than 5
minutes behind the sources". Snowflake refreshes it itself, **incrementally** where it can. It's
the declarative version of stream + task, close to the
[materialized views](../unit4/materialized-views.md) you built in the lab and to a dbt
incremental model.

## Lab

```sql
use role accountadmin;
use warehouse PRACTICE_WH;
use schema SHOPFLOW_DEV.PRACTICE;
```

### 1 · A stream on ORDERS

```sql
-- the target: a copy we keep in sync
create or replace table ORDERS_CURRENT as
select * from SHOPFLOW_DEV.BRONZE.ORDERS;

create or replace stream ORDERS_STREAM on table SHOPFLOW_DEV.BRONZE.ORDERS;

select count(*) from ORDERS_STREAM;    -- 0: nothing changed yet
```

### 2 · Make changes to the source
Ship 5 placed orders, and add a new order:

```sql
update SHOPFLOW_DEV.BRONZE.ORDERS
   set status = 'shipped'
 where order_id in (select order_id from SHOPFLOW_DEV.BRONZE.ORDERS
                    where status = 'placed' order by order_id limit 5);

insert into SHOPFLOW_DEV.BRONZE.ORDERS
  (order_id, customer_id, channel, order_ts, status, currency, promo_id)
values (40001, 72, 'app', current_timestamp()::timestamp_ntz, 'placed', 'USD', null);

select order_id, status, metadata$action, metadata$isupdate
from ORDERS_STREAM
order by order_id, metadata$action;
```

Snowflake's `update` has no `limit`, so a subquery picks the 5 orders.

Example output:

| ORDER_ID | STATUS | METADATA$ACTION | METADATA$ISUPDATE |
|---|---|---|---|
| 1 | placed | DELETE | TRUE |
| 1 | shipped | INSERT | TRUE |
| … | … | … | … |
| 40001 | placed | INSERT | FALSE |

Each updated order appears **twice** (old row deleted, new row inserted); the new order once.

### 3 · Apply the changes with MERGE (consumes the stream)

```sql
merge into ORDERS_CURRENT t
using (select * from ORDERS_STREAM where metadata$action = 'INSERT') s
   on t.order_id = s.order_id
when matched then update set t.status = s.status
when not matched then insert
     (order_id, customer_id, channel, order_ts, status, currency, promo_id)
     values (s.order_id, s.customer_id, s.channel, s.order_ts, s.status, s.currency, s.promo_id);

select count(*) from ORDERS_STREAM;    -- 0 again: consumed
```

The result shows `number of rows inserted: 1, number of rows updated: 5`. It's the same `MERGE`
you wrote in the lab ([MERGE & upserts](../unit2/merge.md)) — the stream just hands you **only
the changes** instead of the whole table.

!!! example "Exercise — deletes"
    This `MERGE` ignores deleted orders. Delete one order in `SHOPFLOW_DEV.BRONZE.ORDERS`, then
    extend the `MERGE`: use the whole stream as the source and add
    `when matched and s.metadata$action = 'DELETE' and not s.metadata$isupdate then delete`.

### 4 · Automate it with a task

```sql
create or replace task MERGE_ORDERS_TASK
    warehouse = PRACTICE_WH
    schedule  = '1 MINUTE'
when system$stream_has_data('SHOPFLOW_DEV.PRACTICE.ORDERS_STREAM')
as
merge into ORDERS_CURRENT t
using (select * from ORDERS_STREAM where metadata$action = 'INSERT') s
   on t.order_id = s.order_id
when matched then update set t.status = s.status
when not matched then insert
     (order_id, customer_id, channel, order_ts, status, currency, promo_id)
     values (s.order_id, s.customer_id, s.channel, s.order_ts, s.status, s.currency, s.promo_id);

alter task MERGE_ORDERS_TASK resume;
```

- **`when system$stream_has_data(…)`** — the task skips its run (and starts no warehouse) when
  the stream is empty. Cheap to schedule often.

Now change the source and wait a minute:

```sql
update SHOPFLOW_DEV.BRONZE.ORDERS set status = 'delivered' where order_id = 40001;

-- after ~1 minute:
select name, state, scheduled_time, completed_time, error_message
from table(information_schema.task_history(task_name => 'MERGE_ORDERS_TASK'))
order by scheduled_time desc
limit 5;

select status from ORDERS_CURRENT where order_id = 40001;    -- delivered
```

- **`information_schema.task_history()`** — a table function with each run: `SCHEDULED`,
  `SUCCEEDED`, `SKIPPED` (stream empty) or `FAILED` with the error.

!!! warning "Suspend it now"
    A resumed task keeps running — and spending credits — until you stop it:
    ```sql
    alter task MERGE_ORDERS_TASK suspend;
    ```

It's the lab's Airflow schedule ([Airflow basics](../unit5/basics.md)) moved inside the
database: fine for SQL-only pipelines; an orchestrator is still better across many systems.

### 5 · SCD Type 2 in plain SQL
Keep the **history** of each customer's country and loyalty tier — the pattern you built in the
lab's [SCD2 recipe](../recipes/scd2.md) and that a [dbt snapshot](../dbt/snapshots.md) does for
you.

```sql
create or replace table DIM_CUSTOMER (
  customer_id  int,
  country      varchar,
  loyalty_tier varchar,
  valid_from   timestamp_ntz,
  valid_to     timestamp_ntz,
  is_current   boolean
);

-- initial load: every customer, valid from sign-up
insert into DIM_CUSTOMER
select customer_id, country, loyalty_tier, signup_date::timestamp_ntz, null, true
from SHOPFLOW_DEV.BRONZE.CUSTOMERS;
```

A customer moves to Germany and is upgraded to gold:

```sql
update SHOPFLOW_DEV.BRONZE.CUSTOMERS
   set country = 'DE', loyalty_tier = 'gold'
 where customer_id = 72;
```

**Step 1 — close** the current version of every changed customer:

```sql
update DIM_CUSTOMER d
   set valid_to = current_timestamp()::timestamp_ntz, is_current = false
  from SHOPFLOW_DEV.BRONZE.CUSTOMERS s
 where d.customer_id = s.customer_id
   and d.is_current
   and (d.country <> s.country or d.loyalty_tier <> s.loyalty_tier);
```

- **`update … from …`** — update using another table: Snowflake's join-in-an-update.

**Step 2 — open** a new current version for everyone without one (changed and brand-new
customers):

```sql
insert into DIM_CUSTOMER
select s.customer_id, s.country, s.loyalty_tier, current_timestamp()::timestamp_ntz, null, true
from SHOPFLOW_DEV.BRONZE.CUSTOMERS s
left join DIM_CUSTOMER d
       on d.customer_id = s.customer_id and d.is_current
where d.customer_id is null;

select * from DIM_CUSTOMER where customer_id = 72 order by valid_from;
```

Example output:

| CUSTOMER_ID | COUNTRY | LOYALTY_TIER | VALID_FROM | VALID_TO | IS_CURRENT |
|---|---|---|---|---|---|
| 72 | FR | none | 2023-09-27 00:00:00 | 2026-10-16 14:02:11 | FALSE |
| 72 | DE | gold | 2026-10-16 14:02:15 | NULL | TRUE |

Compare with the columns a dbt snapshot creates: `dbt_valid_from`, `dbt_valid_to`,
`dbt_scd_id`, `dbt_updated_at` — the same idea.

### 6 · A dynamic table: revenue per channel

```sql
create or replace dynamic table CHANNEL_REVENUE_DT
    target_lag = '1 minute'
    warehouse  = PRACTICE_WH
as
select o.channel,
       count(distinct o.order_id)       as orders,
       sum(i.quantity * i.unit_price)   as revenue
from SHOPFLOW_DEV.BRONZE.ORDERS o
join SHOPFLOW_DEV.BRONZE.ORDER_ITEMS i on i.order_id = o.order_id
where o.status <> 'cancelled'
group by o.channel;

select * from CHANNEL_REVENUE_DT order by revenue desc;
```

- **`target_lag`** — the most the table may fall behind its sources (minimum 1 minute). Or
  `downstream`: refresh only when a dynamic table that reads this one needs it.

Add an order line and wait a minute — the table updates itself:

```sql
insert into SHOPFLOW_DEV.BRONZE.ORDER_ITEMS values (40001, 1, 10, 99.00);

-- after ~1 minute:
select * from CHANNEL_REVENUE_DT where channel = 'app';

select name, state, refresh_action, refresh_start_time
from table(information_schema.dynamic_table_refresh_history(name => 'CHANNEL_REVENUE_DT'))
order by refresh_start_time desc
limit 5;
```

- **`refresh_action`** — `INCREMENTAL` (only the changes were processed), `FULL` or
  `NO_DATA`.

!!! note "Why it says FULL — `refresh_mode`"
    A dynamic table is created with **`refresh_mode = AUTO`** unless you say otherwise: Snowflake
    picks `INCREMENTAL` when it can refresh the query that way and quietly falls back to `FULL`
    when the query uses something it can't (this one's `count(distinct …)` over a join is such a
    case). The choice is made once, at creation. See what it picked, and why:

    ```sql
    show dynamic tables like 'CHANNEL_REVENUE_DT';
    select "name", "refresh_mode", "refresh_mode_reason"
    from table(result_scan(last_query_id()));
    ```

    To choose yourself, add `refresh_mode` to the `create`:

    ```sql
    create or replace dynamic table CHANNEL_REVENUE_DT
        target_lag   = '1 minute'
        warehouse    = PRACTICE_WH
        refresh_mode = INCREMENTAL      -- AUTO (default) | INCREMENTAL | FULL
    as
    select …;
    ```

    With `INCREMENTAL`, the `create` **fails** if the query can't be refreshed incrementally, and
    the error says why — a quick way to learn what's supported.

Stop it refreshing:

```sql
alter dynamic table CHANNEL_REVENUE_DT suspend;
```

!!! info "Compare"
    **Streams + tasks** — full control over each step. **Dynamic tables** — declare the result,
    Snowflake schedules it; chain them into pipelines. **Materialized views** *[Enterprise]* —
    one table only, **no joins**, always current, used automatically by the optimizer.

## Check yourself
1. You `select * from ORDERS_STREAM` five times. Is it empty afterwards? *(No — only a DML
   statement that commits consumes a stream.)*
2. Why does an update show up as two rows in a stream? *(A `DELETE` of the old row and an
   `INSERT` of the new, both with `METADATA$ISUPDATE = TRUE`.)*
3. Your dynamic table should stay within 10 minutes of the sources, and nothing more. What do you
   set? *(`target_lag = '10 minutes'`.)*
