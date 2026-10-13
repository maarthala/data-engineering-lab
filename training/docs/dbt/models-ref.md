# Models, ref() and materializations

## Concept
A model reads another model with **`{{ ref('model_name') }}`**. That one function does two jobs:

1. dbt replaces it with the model's real table name (`silver.stg_orders`).
2. dbt records the **dependency**: this model must be built **after** `stg_orders`. All the
   `ref()`s together are the project's **DAG**, and `dbt run` builds models in DAG order.

`source()` points at raw tables; **`ref()` points at models**. Never type a model's table name
directly: dbt wouldn't know the order, and the lineage graph would have a hole.

How a model is built is its **materialization**:

| Materialization | dbt creates | Good for |
|---|---|---|
| **`view`** | a view: only the SQL is stored; it runs each time someone queries it | staging: cheap, always current |
| **`table`** | a table: the result is computed once and stored | marts that dashboards read: fast to query |
| **`ephemeral`** | nothing: its SQL is pasted into the models that use it, as a CTE | small helper steps you don't need to query |
| **`incremental`** | a table that only adds or updates new rows on each run | big tables ([later in the unit](#coming-up)) |

Your `dbt_project.yml` already sets the defaults: `staging/` → view, `marts/` → table.

## Lab
The **marts**: two Gold tables built from `stg_orders`.

### 1 · Two marts that `ref()` the staging model
Create the folder `models/marts/` and two files.

**`models/marts/orders_by_channel.sql`**:

```sql
select
    channel,
    count(*)                    as orders,
    count(distinct customer_id) as customers
from {{ ref('stg_orders') }}
group by channel
```

**`models/marts/daily_orders.sql`**:

```sql
select
    order_date,
    count(*)                                              as orders,
    sum(case when status = 'cancelled' then 1 else 0 end) as cancelled
from {{ ref('stg_orders') }}
group by order_date
```

- **`count(distinct customer_id)`**: how many *different* customers ordered
  ([Joins & aggregations](../unit2/joins-aggregations.md)).
- **`sum(case when … then 1 else 0 end)`**: counts only the rows that match, here the
  cancelled orders ([Conditional logic, NULLs & filtering](../unit2/conditional.md)).

### 2 · Build everything: `dbt run`
No `-s` this time, so dbt builds every model, in DAG order:

```bash
dbt run
```
```text
15:30:00  Found 3 models, 1 source, 516 macros
15:30:02  1 of 3 START sql view model silver.stg_orders .................................. [RUN]
15:30:02  1 of 3 OK created sql view model silver.stg_orders ............................. [OK in 0.27s]
15:30:02  2 of 3 START sql table model gold.daily_orders ................................. [RUN]
15:30:03  2 of 3 OK created sql table model gold.daily_orders ............................ [OK in 1.04s]
15:30:03  3 of 3 START sql table model gold.orders_by_channel ............................ [RUN]
15:30:04  3 of 3 OK created sql table model gold.orders_by_channel ....................... [OK in 1.12s]
15:30:04  Finished running 2 table models, 1 view model in 0 hours 0 minutes and 3.94 seconds (3.94s).
15:30:04  Completed successfully
15:30:04  Done. PASS=3 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=3
```

- **`stg_orders` first**, because both marts `ref()` it. You never said so; dbt read it from
  the SQL.
- The marts are **`sql table model gold.…`**: the `marts/` defaults from `dbt_project.yml`.
- The *unused configuration paths* warning is gone: `marts/` has models now.

**What dbt actually ran** is saved in `target/run/`. Open
`target/run/shop/models/marts/daily_orders.sql`:

```sql
create or replace table gold.daily_orders
    using iceberg
      as
      select
    order_date,
    count(*)                                              as orders,
    sum(case when status = 'cancelled' then 1 else 0 end) as cancelled
from silver.stg_orders
group by order_date
```

Your `SELECT`, wrapped in `CREATE OR REPLACE TABLE … USING iceberg AS`: the `table`
materialization plus `+file_format: iceberg`. `{{ ref('stg_orders') }}` became
`silver.stg_orders`.

### 3 · Gold is readable everywhere
A table, unlike dbt's views, can be read by Trino. In **SQLPad** (your *Lakehouse (Trino)*
connection):

```sql
SELECT * FROM <you>_lake.gold.orders_by_channel ORDER BY orders DESC
```

| channel | orders | customers |
|---|---|---|
| web | 424 | 346 |
| app | 278 | 246 |
| marketplace | 160 | 149 |
| wholesale | 138 | 129 |

The same table is ready for a Superset dashboard
([Dashboards from the Gold layer](../unit7/dashboards.md)).

### 4 · Choose what to build: selectors
On a big project you rarely rebuild everything. **`dbt ls`** lists what a selection would
include, without building anything:

```bash
dbt ls --resource-type model
```
```text
shop.marts.daily_orders
shop.marts.orders_by_channel
shop.staging.stg_orders
```

```bash
dbt ls -s +daily_orders
```
```text
shop.marts.daily_orders
shop.staging.stg_orders
source:shop.bronze.sample_orders
```

- **`--resource-type model`**: only models (not sources or, later, tests).
- **`+daily_orders`**: the model **and everything before it** (its *parents*), back to the
  source. **`daily_orders+`** is the model and everything *after* it. So
  `dbt run -s +daily_orders` rebuilds exactly what `daily_orders` needs.
- Names in the list are `project.folder.model`.

Rebuild just one mart after changing its SQL:

```bash
dbt run -s orders_by_channel
```
```text
15:30:26  1 of 1 START sql table model gold.orders_by_channel ............................ [RUN]
15:30:27  1 of 1 OK created sql table model gold.orders_by_channel ....................... [OK in 1.11s]
15:30:27
15:30:27  Finished running 1 table model in 0 hours 0 minutes and 2.34 seconds (2.34s).
15:30:27
15:30:27  Completed successfully
15:30:27
15:30:27  Done. PASS=1 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=1
```

### 5 · Set the materialization in the model: `config()`
Folder defaults can be overridden in the model itself, with **`{{ config(…) }}`** as the first
line. Try an **ephemeral** helper: `models/staging/stg_cancelled.sql`:

```sql
{{ config(materialized='ephemeral') }}
select order_id, order_date
from {{ ref('stg_orders') }}
where status = 'cancelled'
```

and a mart that uses it, `models/marts/cancelled_per_day.sql`:

```sql
select order_date, count(*) as cancelled
from {{ ref('stg_cancelled') }}
group by order_date
```

```bash
dbt compile -s cancelled_per_day
```
```text
Compiled node 'cancelled_per_day' is:
with __dbt__cte__stg_cancelled as (

select order_id, order_date
from silver.stg_orders
where status = 'cancelled'
) select order_date, count(*) as cancelled
from __dbt__cte__stg_cancelled
group by order_date
```

`stg_cancelled` didn't become a view or a table. dbt pasted its SQL into `cancelled_per_day`
as a **CTE** (`with … as (…)`, from [CTEs & complex analytics](../unit2/ctes.md)). You can't
query an ephemeral model; it only exists inside the models that `ref()` it.

When you're done, delete both files. If you ran `dbt run` meanwhile, dbt also built
`gold.cancelled_per_day`, and **deleting a model's file doesn't drop its table**: dbt only
manages what's in the project now. Drop it yourself in a notebook:

```sql
%%sql
DROP TABLE iceberg.gold.cancelled_per_day
```

## Coming up
Next in the unit: **tests** that check the data on every build, **incremental** models,
**macros** and Jinja, **snapshots** for history, the **docs site** with the lineage graph, and
running the project from **Airflow**.

## 🎯 This runs unchanged on Azure, Databricks, Snowflake & Fabric
`ref()`, `config()`, selectors and the four materializations are core dbt and work the same on
every platform. The generated DDL differs (`USING delta` on Databricks, Snowflake's
`CREATE OR REPLACE TRANSIENT TABLE`), but your models don't change.

## You can now…
- Read one model from another with `{{ ref() }}`, and explain how dbt gets the build order from it
- Choose a materialization: **view**, **table**, **ephemeral** (and know **incremental** is next)
- Build the whole project with `dbt run`, and find the SQL dbt ran in `target/run/`
- Select models with `-s model`, `+model` and `model+`, and preview a selection with `dbt ls`
- Override a folder default with `{{ config(materialized=…) }}`
- Query dbt's Gold tables from SQLPad
