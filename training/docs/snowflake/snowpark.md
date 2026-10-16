# Snowpark — DataFrames in Snowflake

## Concept
**Snowpark** is a DataFrame API for Python (also Java and Scala) that looks almost like
PySpark — but the work runs **inside Snowflake**: each DataFrame is turned into SQL and executed
on your warehouse. No cluster to manage, and the data never leaves Snowflake unless you
`collect()` it.

| PySpark (the lab) | Snowpark |
|---|---|
| `spark.table("bronze.orders")` | `session.table("BRONZE.ORDERS")` |
| `df.groupBy(...).agg(...)` | `df.group_by(...).agg(...)` |
| `F.sum`, `F.countDistinct` | `sum_`, `count_distinct` from `snowflake.snowpark.functions` |
| `df.write.saveAsTable(...)` | `df.write.save_as_table(...)` |
| lazy until an action | lazy until an action (`show`, `collect`, `save_as_table`) |
| `.explain()` | `.explain()` — shows the generated SQL |

You'll run it in a **Snowflake Notebook** — Python in Snowsight, nothing to install.

## Lab

### 1 · Create a notebook
Snowsight → **Projects → Notebooks → + Notebook**:

- **Location:** database `SHOPFLOW`, schema `GOLD`
- **Warehouse:** `PRACTICE_WH`

A notebook already has a **session** — you get it with `get_active_session()`.

### 2 · Revenue per category — in Snowpark

```python
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, sum as sum_, count_distinct

session = get_active_session()

orders = session.table("SHOPFLOW.BRONZE.ORDERS").filter(col("STATUS") != "cancelled")
items  = session.table("SHOPFLOW.BRONZE.ORDER_ITEMS")
prods  = session.table("SHOPFLOW.BRONZE.PRODUCTS")

category_revenue = (
    orders.join(items, orders["ORDER_ID"] == items["ORDER_ID"])
          .join(prods, items["PRODUCT_ID"] == prods["PRODUCT_ID"])
          .group_by(prods["CATEGORY"])
          .agg(count_distinct(orders["ORDER_ID"]).alias("ORDERS"),
               sum_(items["QUANTITY"] * items["UNIT_PRICE"]).alias("REVENUE"))
          .sort(col("REVENUE").desc())
)

category_revenue.show()
```

- **`get_active_session()`** — the notebook's connection, already logged in as you.
- **`col("NAME")`** — a column reference. Column names are **upper case**, like the tables.
- **`orders["ORDER_ID"]`** — a column of a specific DataFrame; needed when both sides of a join
  have one with that name.

Example output:

```
------------------------------------------
|"CATEGORY"      |"ORDERS"  |"REVENUE"   |
------------------------------------------
|Books           |11375     |10901740.18 |
|Home & Kitchen  |11824     |9899384.12  |
|...             |...       |...         |
------------------------------------------
```

### 3 · See the SQL it runs

```python
category_revenue.explain()
```

It prints the SQL Snowpark generated and the plan — one `SELECT` with two joins and a
`GROUP BY`. Find it in **Query History** too.

### 4 · Save as a Gold table

```python
category_revenue.write.save_as_table("SHOPFLOW.GOLD.CATEGORY_REVENUE", mode="overwrite")
session.table("SHOPFLOW.GOLD.CATEGORY_REVENUE").count()     # 8 categories
```

- **`mode="overwrite"`** — replace the table if it exists (`"append"` adds rows).

### 5 · Bring a small result to pandas

```python
df = category_revenue.to_pandas()
df.head()
```

- **`to_pandas()`** — runs the query and downloads the result into a pandas DataFrame. Fine for
  8 rows; never on millions — like `collect()` / `toPandas()` in Spark.

!!! info "Outside Snowsight"
    The same code runs on your laptop with `pip install snowflake-snowpark-python`, building
    the session yourself:
    `Session.builder.configs({"account": "ORGNAME-ACCOUNTNAME", "user": …, "authenticator":
    "externalbrowser", "role": …, "warehouse": "PRACTICE_WH", "database": "SHOPFLOW"}).create()`.
    Prefer key-pair or browser login over a password.

!!! info "Learn next"
    Python **UDFs** and **stored procedures** (your Python, run by Snowflake), **Snowflake
    Cortex** AI functions in SQL, and the pandas-compatible **Snowpark pandas API**.

## Check yourself
1. Where does `category_revenue.group_by(...)` run — in the notebook or in Snowflake?
   *(In Snowflake, as SQL. The notebook only builds the query.)*
2. What's the risk of `to_pandas()` on `BIG_ORDERS`? *(20 M rows downloaded into the notebook's
   memory — slow, or out of memory.)*
