# Sources and staging models

## Concept
dbt never loads data; it starts from tables that are already in the lakehouse. Those raw
tables are **sources**: you *declare* them once in a YAML file, and models read them with
**`{{ source('schema', 'table') }}`** instead of typing the table name.

The first layer of models on top of a source is **staging**: one model per source table,
named **`stg_<table>`**. A staging model does light, row-by-row cleanup: pick and rename
columns, cast types. No joins, no aggregates. It's the **Silver** step from
[Transform to Silver](../unit4/transform-silver.md), written as a single `SELECT`.

Why declare sources, instead of `from bronze.sample_orders`?

- **Lineage**: dbt knows the model depends on that raw table, so the graph starts there.
- **One place to change**: if the raw table moves, you change one line of YAML, not every model.
- **Tests and docs** can be attached to the source itself (later in the unit).

## Lab
Your source is the starter table every member has: **`bronze.sample_orders`**, 1,000 ShopFlow
orders ([Your lab account & workspace](../setup/workspace.md)).

### 1 · Declare the source
In Jupyter's file browser, open `my_files/dbt/shop/models/`, create a folder **`staging`**, and in it a
new file **`sources.yml`**:

```yaml
version: 2

sources:
  - name: bronze                 # the schema the raw table lives in
    tables:
      - name: sample_orders      # iceberg.bronze.sample_orders — your starter table
```

- **`version: 2`**: the format version of dbt's YAML files. Always `2`.
- **`sources:`** is a **list** (each item starts with `- `). One item per source schema.
- **`name: bronze`**: the schema. **`tables:`**: the tables in it you want to use.

### 2 · Write the staging model
New file **`models/staging/stg_orders.sql`**:

```sql
select
    order_id,
    customer_id,
    channel,
    cast(order_ts as date) as order_date,
    status,
    currency
from {{ source('bronze', 'sample_orders') }}
```

- A model is **only a `SELECT`**: no `CREATE`, no `;`. The file name, `stg_orders`, becomes
  the view's name.
- **`cast(order_ts as date) as order_date`**: the order timestamp cut to a date
  ([Conditional logic, NULLs & filtering](../unit2/conditional.md) has `CAST`).
- **`{{ source('bronze', 'sample_orders') }}`**: Jinja again. dbt replaces it with the real
  table name when it **compiles** the model.

### 3 · See the SQL dbt will run: `dbt compile`
In the terminal (`cd ~/work/my_files/dbt/shop` first):

```bash
dbt compile -s stg_orders
```
```text
15:29:06  Running with dbt=1.12.5
15:29:06  Registered adapter: spark=1.11.0
15:29:06  Unable to do partial parsing because saved manifest not found. Starting full parse.
15:29:09  [WARNING]: Configuration paths exist in your dbt_project.yml file which do not apply to any resources.
There are 1 unused configuration paths:
- models.shop.marts
15:29:09  Found 1 model, 1 source, 516 macros
15:29:09
15:29:09  Concurrency: 1 threads (target='dev')
15:29:09
Compiled node 'stg_orders' is:
select
    order_id,
    customer_id,
    channel,
    cast(order_ts as date) as order_date,
    status,
    currency
from bronze.sample_orders
```

- **`dbt compile`** turns your models into plain SQL **without running anything**. The last
  line shows `{{ source(…) }}` became **`bronze.sample_orders`**.
- **`-s stg_orders`** (`--select`): only this model. Without `-s`, dbt takes every model.
- *Unable to do partial parsing…* appears on the first run only: dbt saves what it learned
  about the project in `target/` and reuses it next time.
- The **WARNING** says `dbt_project.yml` has settings for `models/marts/`, but that folder has
  no models yet. It goes away in the next lesson.
- **516 macros**: dbt's own built-in macros, plus your one.

### 4 · Build it: `dbt run`

```bash
dbt run -s stg_orders
```
```text
15:29:18  1 of 1 START sql view model silver.stg_orders .................................. [RUN]
15:29:18  1 of 1 OK created sql view model silver.stg_orders ............................. [OK in 0.23s]
15:29:18
15:29:18  Finished running 1 view model in 0 hours 0 minutes and 1.31 seconds (1.31s).
15:29:18
15:29:18  Completed successfully
15:29:18
15:29:18  Done. PASS=1 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=1
```

(From here on the outputs skip the first lines, which are the same every time.)

**`dbt run`** compiles each selected model and **builds** it in your lakehouse. `stg_orders` is
in `models/staging/`, so the folder defaults from `dbt_project.yml` apply: a **view** in the
**silver** schema, `silver.stg_orders`. dbt wrapped your `SELECT` in
`CREATE OR REPLACE VIEW silver.stg_orders AS …` for you.

### 5 · Look at the result
**With dbt**: `dbt show` runs a model's `SELECT` and prints the first rows:

```bash
dbt show -s stg_orders --limit 5
```
```text
Previewing node 'stg_orders':
| order_id | customer_id | channel   | order_date | status    | currency |
| -------- | ----------- | --------- | ---------- | --------- | -------- |
|        1 |        1430 | wholesale | 2024-11-04 | delivered | GBP      |
|        2 |         667 | app       | 2023-11-15 | placed    | USD      |
|        3 |         490 | web       | 2023-10-01 | delivered | USD      |
|        4 |          63 | web       | 2024-12-09 | placed    | USD      |
|        5 |         229 | web       | 2023-12-19 | cancelled | USD      |
```

**In a notebook**: it's a normal view in your lakehouse, so `%%sql`
([Query tables with %%sql](../unit3/sql-magic.md)) works:

```sql
%%sql
SELECT channel, count(*) AS orders FROM iceberg.silver.stg_orders GROUP BY channel ORDER BY orders DESC
```
| | channel | orders |
|---|---|---|
| 0 | web | 424 |
| 1 | app | 278 |
| 2 | marketplace | 160 |
| 3 | wholesale | 138 |

!!! warning "Views made by dbt are Spark views: SQLPad and Superset can't read them"
    In SQLPad, `SELECT * FROM <you>_lake.silver.stg_orders` fails with
    *Cannot read unsupported dialect 'spark' for view*. A view stores its SQL, and this SQL is
    Spark's. Trino (behind SQLPad and Superset) only runs views written in its own dialect.
    **Tables** have no such problem, which is one reason Gold marts are tables (next lesson).

## 🎯 This runs unchanged on Azure, Databricks, Snowflake & Fabric
`sources.yml`, `source()` and staging models are standard dbt, identical on every platform. On
Databricks a source is a Unity Catalog table, on Snowflake a table in a database and schema, on
Fabric a Lakehouse or Warehouse table: only the names in `sources.yml` change.

## You can now…
- Declare a raw table as a **source** in `sources.yml`
- Write a **staging** model (`stg_…`) that reads it with `{{ source() }}`
- See the SQL dbt will run with `dbt compile`, and build models with `dbt run -s …`
- Preview a model with `dbt show`, and query it from a notebook with `%%sql`
- Explain why SQLPad and Superset can't read dbt's views, but can read its tables
