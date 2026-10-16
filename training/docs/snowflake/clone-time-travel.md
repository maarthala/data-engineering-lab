# Cloning, Time Travel & table types

## Concept

### Zero-copy cloning
A **clone** copies only **metadata** — it points at the same micro-partitions as the original. It
is instant, costs **no extra storage** at first, and from then on the two are independent: when
you change rows in the clone, only the changed micro-partitions are written new (and billed).
You can clone a **table, schema or whole database**.

Use it for dev/test copies of production, safe experiments, and a backup before a risky change.

### Time Travel
Snowflake keeps the **old versions** of changed or dropped data for the **retention period**:
default **1 day**, up to **90 days** *[Enterprise]*. While it's kept you can:

- query a table **as it was** — `at (offset => -600)`, `at (timestamp => …)`,
  `before (statement => '<query id>')`
- **`undrop`** a dropped table, schema or database
- **clone** from a point in the past

### Fail-safe
After Time Travel ends, permanent data stays **7 more days** in **Fail-safe** — but only
**Snowflake Support** can recover it (for disasters, not for you to query).

```
change ─► Time Travel (0–90 days, you can query/undrop) ─► Fail-safe (7 days, Support only) ─► gone
```

The lab's Iceberg tables have the same idea: every write is a **snapshot**, and you can read an
old one (`for version as of …`) until snapshots are expired.

### Table types
| Type | Time Travel | Fail-safe | Lives until | Use for |
|---|---|---|---|---|
| **Permanent** (default) | 0–90 days | 7 days | dropped | production data |
| **Transient** | 0–1 day | none | dropped | staging / data you can rebuild (cheaper) |
| **Temporary** | 0–1 day | none | **end of session** | scratch work in one session |
| **External** | none | none | dropped | querying files in S3 / ADLS / GCS without loading |

**dbt link:** dbt creates its Snowflake models as **transient** by default — they're rebuilt
every run anyway, so Fail-safe storage would be wasted money.

## Lab

```sql
use role accountadmin;
use warehouse PRACTICE_WH;
```

### 1 · Clone the whole database

```sql
create or replace database SHOPFLOW_DEV clone SHOPFLOW;

select count(*) from SHOPFLOW_DEV.BRONZE.ORDERS;      -- 40000, instantly
```

Change the clone — the original is untouched:

```sql
delete from SHOPFLOW_DEV.BRONZE.ORDERS where status = 'cancelled';

select count(*) from SHOPFLOW_DEV.BRONZE.ORDERS;      -- ~36000
select count(*) from SHOPFLOW.BRONZE.ORDERS;          -- 40000, unchanged
```

**dbt link:** point a dbt `dev` target at `SHOPFLOW_DEV` and develop against a full copy of
production data, for free.

### 2 · Query the past
From here on, work in the clone:

```sql
use schema SHOPFLOW_DEV.BRONZE;

select count(*) from ORDERS;                          -- after the delete
select count(*) from ORDERS at (offset => -60);       -- 60 seconds ago
```

- **`at (offset => -n)`** — the table as it was `n` **seconds** ago.

!!! note "A clone has no history from before it was created"
    If you made `SHOPFLOW_DEV` 2 minutes ago, `offset => -600` fails ("Time travel data is not
    available"). Use a smaller offset — or query the original database's history instead.

### 3 · Recover rows by statement id

```sql
delete from ORDERS where channel = 'wholesale';
set delete_qid = last_query_id();

select count(*) from ORDERS before (statement => $delete_qid);   -- before the delete

insert into ORDERS
select * from ORDERS before (statement => $delete_qid)
where channel = 'wholesale';

select count(*) from ORDERS where channel = 'wholesale';         -- back
```

- **`last_query_id()`** — the id of the previous statement in this session.
- **`set name = …`** / **`$name`** — a session variable, and how you read it.
- **`before (statement => id)`** — the table **just before** that statement ran. Exact, so you
  don't have to guess a time.

### 4 · Undrop

```sql
drop table CUSTOMERS;
select count(*) from CUSTOMERS;      -- error: does not exist
undrop table CUSTOMERS;
select count(*) from CUSTOMERS;      -- 6000
```

### 5 · Retention period

```sql
show tables like 'ORDERS';                                  -- retention_time = 1
alter table ORDERS set data_retention_time_in_days = 3;
show tables like 'ORDERS';                                  -- retention_time = 3
```

Longer retention means old versions are stored longer — and billed. Set it per table, schema or
database; `0` turns Time Travel off.

### 6 · Clone from the past
Combine both features — a copy of `ORDERS` as it was before the wholesale delete:

```sql
create table ORDERS_BEFORE_DELETE clone ORDERS before (statement => $delete_qid);
```

### 7 · Table types

```sql
create schema if not exists SHOPFLOW_DEV.PRACTICE;
use schema SHOPFLOW_DEV.PRACTICE;

create transient table STG_ORDERS as select * from SHOPFLOW_DEV.BRONZE.ORDERS;
create temporary table TMP_ORDERS as select * from SHOPFLOW_DEV.BRONZE.ORDERS;

show tables;    -- the "kind" column: TRANSIENT, TEMPORARY
```

Open a **new** SQL file (a new session) and run `show tables in schema SHOPFLOW_DEV.PRACTICE;` —
`TMP_ORDERS` isn't there: temporary tables are visible only in the session that made them.

```sql
alter table STG_ORDERS set data_retention_time_in_days = 5;   -- error: transient max is 1
```

## Check yourself
1. A table was dropped 10 days ago with 1-day retention. Can you `undrop` it?
   *(No — it's in Fail-safe. Only Snowflake Support can recover it.)*
2. You clone a 1 TB database. How much extra storage do you pay for right away? *(None — until
   you change data in the clone.)*
3. Which table type for a staging table you rebuild every night? *(Transient — no Fail-safe
   cost.)*

`SHOPFLOW_DEV` is your playground for the next lessons — keep it.
