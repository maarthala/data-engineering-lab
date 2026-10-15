# Query tables with `%%sql`

## Concept
Notebooks on this stack ship a **`%%sql` cell magic** — put it on the first line of a cell and write
plain SQL, exactly like Databricks. It runs through the pre-created **`spark`** session
([3.7](spark.md)), so it queries the same governed catalogs: **`my`** = *your own* lakehouse,
**`shared`** = the course's shared lake (what Trino and Superset call `iceberg` — see
[0.3](../setup/workspace.md)). Results render as a table.

No setup, no connection — `spark` and `%%sql` are already there when the notebook opens.

> `%%sql` is for **quick exploration** in a notebook. When you *build* Gold tables with the same
> SQL — `spark.sql("…")` in a real pipeline — that's [4.4](../unit4/spark-sql-gold.md).

!!! info "Three rules for every `%%sql` cell"
    1. **Qualify with the catalog** — `my.<namespace>.<table>`. A bare `SHOW SCHEMAS` or
       `orders` targets Spark's *default* catalog (not the lakehouse) and errors.
    2. **No trailing `;`** — `%%sql` runs one `spark.sql(...)`, and Spark's parser rejects a
       trailing semicolon.
    3. **One statement per cell** — unlike SQLPad, you can't stack several with `;`.

## Lab

### 1 · See what namespaces exist
First, look at what the catalog holds. `SHOW SCHEMAS IN iceberg` lists the **namespaces** — the
[medallion](../unit1/medallion.md) tiers `bronze` / `silver` / `gold` (raw → cleaned →
business-ready):

```sql
%%sql
SHOW SCHEMAS IN iceberg
```
> `SHOW NAMESPACES IN iceberg` is the same command — Iceberg calls schemas **namespaces**. They
> list **even when empty** (SQLPad's sidebar, by contrast, hides empty ones).

!!! note "The medallion tiers are empty right now — that's expected"
    `bronze` / `silver` / `gold` are **built later, in [Unit 4](../unit4/fundamentals.md)** (Spark
    reads the sources and writes these tables). Following the course in order, they exist as
    namespaces but hold **no tables yet** — so `SELECT * FROM my.gold.…` would say *table not
    found*. No problem: below you'll **create your own schema and data** to practise on, then meet
    the pipeline's tables in Unit {unit:unit4/fundamentals.md}. (Want real data right now? The ready-made tables are in the
    shared lake: `SELECT * FROM shared.gold.daily_sales LIMIT 10`.)

### 2 · Create your own schema, table & data
`%%sql` isn't read-only — you can **create** objects. Make a scratch **namespace**, add a table,
and insert a few rows (one statement per cell, no `;`):

```sql
%%sql
CREATE SCHEMA IF NOT EXISTS my.sandbox
```
> `CREATE SCHEMA` = `CREATE NAMESPACE` = `CREATE DATABASE` in Spark — all synonyms.
> `IF NOT EXISTS` makes it safe to re-run.

```sql
%%sql
CREATE TABLE IF NOT EXISTS my.sandbox.my_customers (
  customer_id int,
  full_name   string,
  country     string
)
```
> Note Spark's type names: **`string`** (not `varchar`), `int`, `double`, `date`, … Because the
> `my` catalog *is* an Iceberg catalog, the table is created as **Iceberg** automatically —
> no `USING iceberg` needed.

```sql
%%sql
INSERT INTO my.sandbox.my_customers VALUES
  (1, 'Ada Lovelace',      'UK'),
  (2, 'Alan Turing',       'UK'),
  (3, 'Grace Hopper',      'US'),
  (4, 'Katherine Johnson', 'US')
```

!!! warning "403 / Forbidden is governance, not your SQL"
    In `my` (your own lakehouse) you can create anything. In `shared` (read-only) or in a
    catalog another member shared with you, writing needs a **Polaris grant** — a **403 /
    Forbidden** there is RBAC, not a SQL mistake. See [Unit 6](../unit6/polaris.md).

### 3 · Explore & query your data
`sandbox` now has a table — discover and query it exactly as you would any catalog table:

```sql
%%sql
SHOW TABLES IN my.sandbox          -- your new table shows up
```
```sql
%%sql
DESCRIBE my.sandbox.my_customers   -- columns + types
```
```sql
%%sql
SELECT * FROM my.sandbox.my_customers
```
```sql
%%sql
SELECT country, count(*) AS customers
FROM my.sandbox.my_customers
GROUP BY country
ORDER BY customers DESC
```

The result renders as a table (a pandas DataFrame under the hood, ≤1000 rows for display). Your
table is a **real, governed Iceberg table** — visible to Trino, Superset, SQLPad, and the Polaris
Console. Tidy up when you're done experimenting:
```sql
%%sql
DROP TABLE my.sandbox.my_customers
```

### 4 · The same `%%sql`, on the pipeline's tables (after Unit {unit:unit4/fundamentals.md})
Once [Unit 4](../unit4/fundamentals.md) has built Bronze → Silver → Gold, the **identical** `%%sql`
queries them — no new skills, just real tables. A Gold lookup:

```sql
%%sql
SELECT * FROM my.gold.daily_sales
ORDER BY order_date DESC
LIMIT 10
```

…and an aggregate over Silver (`silver.orders` already carries each line's customer country):

```sql
%%sql
SELECT country, count(DISTINCT order_id) AS orders, sum(line_amount) AS revenue
FROM my.silver.orders
GROUP BY country
ORDER BY revenue DESC
```

### 5 · Materialized views
`%%sql` also understands `CREATE / REFRESH / DROP / SHOW MATERIALIZED VIEW`: a stored query whose
result is kept as a table and recomputed when you refresh it. They're built on Gold data, so they're
taught in **[4.5 Materialized views](../unit4/materialized-views.md)**, after you've built Gold.

### `%%sql` vs `spark.sql(...)`
Two ways to run SQL — pick by what you need next:

| | `%%sql` (cell magic) | `spark.sql("...")` (Python) |
|---|---|---|
| Best for | quick look, exploring, final display | capturing results to keep working in Python |
| Returns | a rendered table (pandas, ≤1000 rows) | a **Spark DataFrame** (lazy, full size) |
| Capture to a variable | ✗ (display only) | ✓ `df = spark.sql(...)` |

So explore with `%%sql`; when you need to **use** the result (transform, join in Python, write it
back), switch to `spark.sql(...)`:

```python
df = spark.sql("SELECT * FROM shared.gold.daily_sales WHERE revenue > 80000")
df.count()                              # keep working with the DataFrame in Python
```
(After Unit {unit:unit4/fundamentals.md} the same pattern captures your own Gold table:
`spark.sql("SELECT * FROM my.gold.daily_sales")`.)

!!! tip "Raw files need a view first"
    `%%sql` works on **catalog tables** with no setup. To `%%sql` a raw file in object storage,
    register it as a temp view once (see [3.9](upload-register.md)):
    ```python
    # your own files — e.g. the history you export in {lesson:unit4/read-bronze.md}
    spark.read.parquet("s3a://demouser-lake/files/source/shopflow/history/orders") \
         .createOrReplaceTempView("orders_hist")
    ```
    ```sql
    %%sql
    SELECT status, count(*) FROM orders_hist GROUP BY status
    ```
    The view lives only for this kernel session; catalog tables persist.

## 🎯 This runs unchanged on Azure, Databricks, Snowflake & Fabric
`%%sql` **is** Databricks' notebook SQL cell. Same idea in Snowflake worksheets and Fabric
notebooks — write SQL against the catalog, get a table back. The `catalog.schema.table` addressing is
identical.

## You can now…
- Run SQL in a notebook with `%%sql` — qualifying the `my` catalog, no `;`, one per cell
- **Discover** what exists with `SHOW SCHEMAS / SHOW TABLES / DESCRIBE IN iceberg`
- **Create** your own namespace, table, and rows (`CREATE SCHEMA` / `CREATE TABLE` / `INSERT`)
- Choose `%%sql` (display) vs `spark.sql()` (capture & keep working) appropriately
- Register a raw file as a temp view to `%%sql` it
