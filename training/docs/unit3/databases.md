# SQL from Python — Postgres & the lakehouse

## Concept
You've run SQL in SQLPad and Superset ([Unit 2](../unit2/intro.md)). But the everyday
data-engineering move is to run SQL **from Python** and pull the result straight into a pandas
DataFrame — so you can then transform it, call an API, or load it somewhere else. Two connections
matter, both on our stack:

- **Postgres** — the raw ShopFlow OLTP source (`customers`, `products`, `orders`, `order_items`).
- **The lakehouse** (via Spark) — the Bronze/Silver/Gold tables (once built in Unit {unit:unit4/fundamentals.md}).

### How Python talks to a database
Python can't speak a database's private language on its own — it needs a translator. Three
layers do the work, and every cell below sits on top of them:

- **A driver** — a small library that speaks the database's wire protocol (the exact bytes it
  expects). For Postgres that driver is **psycopg2**; for the lakehouse, Spark does this
  job. You rarely call the driver directly, but it's the thing actually sending SQL over the
  network and decoding the reply.
- **SQLAlchemy** — a layer *on top* of the driver that gives every database the **same** entry
  point: `create_engine(<URL>)`. Swap the URL and the same Python code talks to a different
  database. Think of it as a universal adapter over the many drivers.
- **pandas** — once you have an engine (or a connection), `pandas.read_sql(query, engine)` sends
  your SQL, waits for the rows, and hands them back as a **DataFrame** you can transform, plot, or
  write elsewhere.

So the everyday move is: **send SQL → get rows → land them in a DataFrame.** This echoes the
Unit {unit:unit2/intro.md} idea of *"same SQL, two homes"* — here it becomes *same Python, two homes*: identical
code reads Postgres or the lakehouse, and only the connection URL changes.

```python
import pandas as pd
```

`import pandas as pd` loads the pandas library under its conventional nickname `pd`, so every
later call is `pd.read_sql(...)` rather than the longer `pandas.read_sql(...)`. This single
import powers every cell on the page.

## Lab

### Postgres, via SQLAlchemy + pandas
`pandas.read_sql` runs a query and hands you a DataFrame. Point a **SQLAlchemy engine** at the
ShopFlow database:

```python
from sqlalchemy import create_engine

pg = create_engine("postgresql+psycopg2://learner:learner@postgres:5432/shopflow")

# Run SQL, get a DataFrame back
customers = pd.read_sql("SELECT * FROM customers LIMIT 5", pg)
customers
```

**Read it step by step:**

- **`from sqlalchemy import create_engine`** — pull in the one SQLAlchemy function you need.
  **`create_engine`** builds an **engine**: a reusable, lazy connection factory. It doesn't open a
  connection immediately; it opens them on demand and pools them for reuse, so you make the engine
  once and hand it to every query.
- **`create_engine("postgresql+psycopg2://learner:learner@postgres:5432/shopflow")`** — the
  string is a **connection URL**, and its shape is worth learning because you'll write one for
  every database. Read it in pieces:

    `postgresql+psycopg2` → *dialect* `postgresql` spoken through the *driver* `psycopg2`
    · `learner:learner` → *username*`:`*password* — the course's database login: it reads the
      shared ShopFlow / AdventureWorks tables and creates your own, but can't change the shared ones
    · `@postgres:5432` → *host*`:`*port* (here `postgres` is the service name on our stack)
    · `/shopflow` → the *database* to connect to.

- **`pg = ...`** — store the engine in `pg`. This is now your handle to the ShopFlow database,
  reused by every Postgres query below.
- **`pd.read_sql("SELECT * FROM customers LIMIT 5", pg)`** — **`read_sql`** takes two things: the
  **SQL text** to run, and the **engine** to run it on. It sends the query to Postgres, collects
  the rows, and builds a DataFrame from them. `LIMIT 5` keeps it to a quick sample.
- **`customers`** — a bare variable on the last line makes the notebook *display* it. What comes
  back is an ordinary DataFrame: five rows, one column per column in the `customers` table.

Push heavy work **down into the database** (filter/aggregate in SQL, return only the small
result) — don't pull whole tables into pandas:

```python
by_country = pd.read_sql("""
    SELECT c.country,
           count(DISTINCT o.order_id)       AS orders,
           sum(oi.quantity * oi.unit_price) AS revenue
    FROM orders o
    JOIN order_items oi ON oi.order_id = o.order_id
    JOIN customers   c  ON c.customer_id = o.customer_id
    WHERE o.status = 'delivered'
    GROUP BY c.country
    ORDER BY revenue DESC
""", pg)
by_country
```

**Read it step by step:**

- The **triple-quoted string** (`"""…"""`) lets the SQL span many lines — handy for a real query.
  Everything inside is exactly the join-and-aggregate you built in [Unit 2](../unit2/joins-aggregations.md):
  join `orders × order_items × customers`, keep delivered orders, then `GROUP BY c.country`.
- **`count(DISTINCT o.order_id)`** and **`sum(oi.quantity * oi.unit_price)`** run *inside
  Postgres*, so only one small row per country crosses the wire.
- **`read_sql(..., pg)`** — same call as before, just a bigger query on the same `pg` engine.
- **What comes back** — a compact DataFrame with one row per country and columns `country`,
  `orders`, `revenue`, already sorted by revenue.

!!! tip "Pushdown: let the database do the heavy lifting"
    The alternative — `pd.read_sql("SELECT * FROM orders", pg)` and then joining/summing in
    pandas — drags every raw row across the network into your notebook's memory. **Pushdown**
    means writing the filter and aggregate *in SQL* so the database returns only the small answer.
    Rule of thumb: shrink the data in the database, shape the small result in pandas.

### Parameterised queries (never format user input into SQL)
When a value in your query comes from *outside* — a variable, a form field, a function argument —
never glue it into the SQL string yourself. Pass it as a **parameter** and let the driver insert
it safely:

```python
country = "US"
df = pd.read_sql(
    "SELECT * FROM customers WHERE country = %(c)s LIMIT 10",
    pg, params={"c": country},
)
```

**Read it step by step:**

- **`country = "US"`** — the value we want to filter on. Imagine it came from a user, not a
  constant.
- **`%(c)s`** — a **named placeholder**, *not* a Python format specifier. It marks a hole in the
  SQL named `c`; the driver fills that hole later. (This `%(name)s` style is what psycopg2
  expects.)
- **`params={"c": country}`** — the values for the placeholders, keyed by name. The driver sends
  the SQL and the value *separately* and quotes/escapes the value correctly, so it can only ever be
  read as **data**, never as SQL.
- **What comes back** — up to 10 matching customer rows as a DataFrame.

!!! warning "Why not f-string the value in? SQL injection"
    Writing `f"... WHERE country = '{country}'"` looks convenient but is dangerous. If the value
    were `x' OR '1'='1`, the string would become `WHERE country = 'x' OR '1'='1'` — suddenly
    matching *every* row, or worse (dropping a table). This is **SQL injection**. Placeholders
    plus `params=` close the hole: the value can never change the *shape* of the query, only fill
    a slot in it. Make it a habit for **every** externally-supplied value.

### Writing back: not to the production database
So far data has flowed *out* of the database. The mirror call exists: **`to_sql`** takes a
DataFrame and writes it *into* a table. But our ShopFlow and AdventureWorks databases play the
**production** systems of the shop, and like at most companies, data engineers may **read**
production but never change it. Your `learner` login is read-only, so try it and see:

```python
by_country.to_sql("country_revenue", pg, if_exists="replace", index=False)
# → InternalError: cannot execute CREATE TABLE in a read-only transaction
```

That error is the point: you don't stage results in the source system. A computed result goes to
**your own lakehouse**, where you're the owner. Turn the pandas DataFrame into a Spark one and
save it as a table:

```python
spark.createDataFrame(by_country).writeTo("iceberg.gold.country_revenue").createOrReplace()
spark.table("iceberg.gold.country_revenue").toPandas()      # read it back
```

**Read it step by step:**

- **`to_sql(...)`**: the pandas method for writing a DataFrame into a database table, the mirror
  image of `read_sql`. It works on databases you're allowed to write to, such as a team's
  reporting database, but not on production.
- **`spark.createDataFrame(by_country)`**: copies the small pandas DataFrame into Spark.
- **`.writeTo("iceberg.gold.country_revenue")`**: the target table in **your own** lakehouse
  (`iceberg` = your `demouser_lake`), in its `gold` layer.
- **`.createOrReplace()`**: creates the table, or replaces it if it already exists, so you can
  re-run the cell. (`.append()` would add rows to an existing table instead.)
- **The final `spark.table(...).toPandas()`** reads the table straight back so you can confirm the
  write landed.

### Query the lakehouse from Python
The lakehouse tables (built in [Unit 4](../unit4/spark-sql-gold.md)) are read through **Spark**,
not through a database driver. Your notebook already has a `spark` session, and `toPandas()` turns
the result into the same kind of DataFrame you got from Postgres:

```python
# the course's shared lake: a ready-made Gold mart (you build your own in Unit {unit:unit4/fundamentals.md})
daily = spark.sql(
    "SELECT * FROM shared.gold.daily_sales ORDER BY order_date DESC LIMIT 14"
).toPandas()
daily
```

**Read it step by step:**

- **`spark.sql(...)`** runs the query on the Spark cluster. Spark goes through Polaris, so you see
  only the tables you're allowed to see.
- **`shared.gold.daily_sales`** is a table in the course's **shared** lake. `iceberg` is **your own**
  lakehouse. Once you've built tables in [Unit 4](../unit4/fundamentals.md), read them with
  `spark.sql("SELECT * FROM iceberg.gold.daily_sales").toPandas()`.
- **`.toPandas()`** brings the result back into Python as a plain pandas DataFrame, the same as
  `pd.read_sql` does for Postgres. Keep it small (use `LIMIT` or aggregate first), because the
  whole result is copied into your notebook.

!!! tip "Postgres vs the lakehouse from Python: which connection?"
    Use the **Postgres** engine to read the *raw source*, and **Spark** to read the *lakehouse*
    (`iceberg` / `shared`). Both give you ordinary DataFrames that you can then transform with
    [pandas](pandas.md). If you want to query the lakehouse with plain SQL instead, use
    **SQLPad** ([Unit 2](../unit2/intro.md)).

## Challenge
Using the **Postgres** connection, pull *delivered revenue per product category* into a
DataFrame, then (in pandas) add a `pct_of_total` column showing each category's share of overall
revenue. Sort by revenue, highest first.

??? note "Solution"
    ```python
    cat = pd.read_sql("""
        SELECT p.category, sum(oi.quantity * oi.unit_price) AS revenue
        FROM orders o
        JOIN order_items oi ON oi.order_id = o.order_id
        JOIN products    p  ON p.product_id = oi.product_id
        WHERE o.status = 'delivered'
        GROUP BY p.category
    """, pg)

    cat["pct_of_total"] = (cat["revenue"] / cat["revenue"].sum() * 100).round(1)
    cat.sort_values("revenue", ascending=False)
    ```

    **Read it step by step:**

    - **`pd.read_sql(...)`** — the join + aggregate runs *in Postgres*, returning one small row
      per category with `category` and `revenue`.
    - **`cat["revenue"].sum()`** — pandas adds up the `revenue` column across all categories to
      get the grand total.
    - **`cat["pct_of_total"] = (cat["revenue"] / total * 100).round(1)`** — each category's
      revenue divided by that total, times 100, rounded to one decimal — assigned as a **new
      column**.
    - **`cat.sort_values("revenue", ascending=False)`** — order the DataFrame by revenue, highest
      first.

    Note the split of labour: the **join + aggregate** runs in the database (fast, less data
    moved); the **share-of-total** is a quick pandas step on the small result.

!!! tip "🎯 The same pattern on Azure, Databricks, Snowflake & Fabric"
    **What you just did:** ran SQL from Python and moved results in/out as DataFrames.

    - **Azure Databricks** — `spark.read.jdbc(...)` / `spark.sql(...)`, or `pd.read_sql`; results
      to Spark or pandas.
    - **Snowflake** — the **Snowflake Connector for Python** / Snowpark: `session.sql(...).to_pandas()`.
    - **Microsoft Fabric** — notebooks read the Lakehouse SQL endpoint or run `spark.sql`.
    - **Azure Data Factory** — the **Copy activity** moves data between a database and the lake
      no-code (the managed version of these reads/writes).

    "Run SQL, get a DataFrame, transform, write it back" is the daily rhythm everywhere.

## Key terms, at a glance
| Term | Plain meaning |
|---|---|
| **Driver** | Library that speaks a DB's wire protocol — `psycopg2` (Postgres) |
| **SQLAlchemy engine** | A reusable DB connection factory built by `create_engine(url)` — makes/pools connections on demand |
| **Connection URL** | `dialect+driver://user:pass@host:port/database` — swap it to point at a different DB |
| **`pd.read_sql`** | Run a query on an engine/connection → DataFrame |
| **`to_sql`** | Write a DataFrame → a database table (`if_exists`, `index`); never into production |
| **Read-only production** | Data engineers query the source systems but don't change them; results go to the lakehouse |
| **Parameterised query** | Placeholders (`%(c)s`) + `params={...}` — pass values as *data*, never string-format them in |
| **SQL injection** | The attack a parameterised query prevents: user input changing the query's shape |
| **Pushdown** | Do filter/aggregate in the DB; return only the small result |
| **`toPandas()`** | Turns a Spark result into a pandas DataFrame — how Python reads the lakehouse |

## You can now…
- Run SQL against Postgres from Python and get a DataFrame (`pd.read_sql`)
- Explain why you don't write to the production database, and save a result to your lakehouse instead
- Read the lakehouse from Python with Spark (`spark.sql(...).toPandas()`)
- Split work sensibly between the database (heavy SQL) and pandas (light shaping)
