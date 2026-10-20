# Notebook: build Gold in BigQuery Studio

## Concept

### Notebooks inside BigQuery
**BigQuery Studio** has notebooks built in. They're **Colab Enterprise** notebooks: Jupyter
notebooks that Google hosts, saved in your project, next to your SQL queries. It's the same idea
as the lab's Jupyter: cells of Python and SQL, with results under each cell.

A notebook needs a **runtime**, a small VM that runs your Python. It's started for you the first
time you run a cell, and it **bills while it runs**: a few cents an hour. The end of this lesson
shows how to stop it.

### Two ways to talk to BigQuery from a notebook
| | `%%bigquery` cell | BigQuery DataFrames (`bigframes`) |
|---|---|---|
| You write | SQL | pandas-style Python |
| Runs | in BigQuery | in BigQuery: `bigframes` turns your Python into SQL |
| Result | a pandas DataFrame in the notebook | a DataFrame that stays in BigQuery until you look at it |
| Lab equivalent | `%%sql` in Jupyter | Spark DataFrames |

Neither pulls the whole table into the VM. The work runs in BigQuery, so a small runtime is
enough even for big tables. Pulling data into **plain pandas** (`.to_pandas()`) does bring it
into the VM's memory: do that only for small results such as a chart's 24 monthly points.

### Gold for ShopFlow
Two business tables, built from Silver orders and the Bronze lines and products:

- **`shopflow_gold.daily_sales`**: per day and channel: delivered orders, revenue, units. Built
  with **SQL**.
- **`shopflow_gold.top_products`**: per product: revenue, margin, units sold. Built with
  **bigframes**.

Like the lab's Gold, revenue counts **delivered** orders only, and adds amounts as they are (in
each order's own currency). That's fine for practice; a real mart would convert to one currency.

## Lab

### 1 · Create the notebook
Console → **BigQuery** → **BigQuery Studio** → the **▾** next to **+** → **Notebook** → **Empty
notebook**. If Google asks to enable APIs (Vertex AI, Dataform, Compute Engine), click
**Enable**. Rename it (top left) to `shopflow_gold`.

The first time you run a cell, the notebook **connects to a runtime** (top right shows
*Connecting…*, then a green tick). That takes about a minute.

### 2 · Look at Silver with `%%bigquery`
In the first cell:

```python
%%bigquery
select channel, count(*) as orders
from shopflow_silver.orders
group by channel
order by orders desc
```

- **`%%bigquery`** on the first line makes the whole cell a BigQuery query. The result shows
  as a table under the cell. (If you see *Cell magic `%%bigquery` not found*, run
  `%load_ext bigquery_magics` in a cell above it once.)

| | channel | orders |
|---|---|---|
| 0 | web | 17238 |
| 1 | app | 11283 |
| 2 | wholesale | 5701 |
| 3 | marketplace | 5678 |

### 3 · Gold with SQL: `daily_sales`
```python
%%bigquery
create or replace table shopflow_gold.daily_sales as
select date(o.order_ts)                        as order_date,
       o.channel,
       count(distinct o.order_id)              as orders,
       round(sum(i.quantity * i.unit_price), 2) as revenue,
       sum(i.quantity)                         as units
from shopflow_silver.orders o
join shopflow_bronze.order_items i using (order_id)
where o.status = 'delivered'
group by order_date, o.channel
```

- **`create or replace table … as select`** builds the table from the query, replacing it if
  it exists. That makes it safe to rerun, like `WRITE_TRUNCATE`.
- **`date(timestamp)`** keeps the day and drops the time.
- **`join … using (order_id)`** joins on the column with that name in both tables, and keeps one
  copy of it.
- **`round(x, 2)`** rounds to cents. `unit_price` is a `FLOAT`, and sums of floats come out like
  `16076.340000000004` without it.

Check it:

```python
%%bigquery
select count(*) as row_count, sum(orders) as orders, round(sum(revenue), 2) as revenue,
       min(order_date) as first_day, max(order_date) as last_day
from shopflow_gold.daily_sales
```

| | row_count | orders | revenue | first_day | last_day |
|---|---|---|---|---|---|
| 0 | 2919 | 28005 | 53550514.93 | 2024-06-01 | 2026-05-31 |

28,005 orders: exactly the delivered count in Silver.

!!! warning "Reserved words can't be column names"
    `rows`, `range`, `groups`, `partition`, `window` and a few dozen others are **reserved
    keywords** in BigQuery SQL. `select count(*) as rows` fails with *Syntax error: Unexpected
    keyword ROWS*. Pick another name (`row_count`), or put the name in backticks:
    `` as `rows` ``. Trino and Spark have similar lists, but they're not the same.

### 4 · Gold with Python: `top_products`
In a new cell:

```python
import bigframes.pandas as bpd

bpd.options.bigquery.location = "us-central1"     # your region

orders   = bpd.read_gbq("shopflow_silver.orders")
items    = bpd.read_gbq("shopflow_bronze.order_items")
products = bpd.read_gbq("shopflow_bronze.products")

delivered = orders[orders["status"] == "delivered"][["order_id"]]
lines = items.merge(delivered, on="order_id").merge(products, on="product_id")
lines["revenue"] = lines["quantity"] * lines["unit_price"]
lines["margin"]  = lines["quantity"] * (lines["unit_price"] - lines["cost"])

top = (lines.groupby(["product_id", "name", "category"], as_index=False)
            .agg({"revenue": "sum", "margin": "sum", "quantity": "sum"})
            .sort_values("revenue", ascending=False))

top.to_gbq("shopflow_gold.top_products", if_exists="replace")
top.head(5)
```

- **`import bigframes.pandas as bpd`**: BigQuery DataFrames, with the pandas API you know from
  the lab. It comes installed in BigQuery Studio notebooks.
- **`bpd.options.bigquery.location`**: where your datasets are. Set it before the first read.
- **`bpd.read_gbq("dataset.table")`**: a DataFrame **backed by the table**. No rows are copied
  into the notebook.
- **`df[df["status"] == "delivered"]`**: keep the rows where the condition is true.
  **`[["order_id"]]`** keeps just that column (a list of names gives back a DataFrame).
- **`a.merge(b, on="col")`**: an inner join on `col`, like SQL's `join … using (col)`.
- **`df["new"] = expression`** adds a column computed per row.
- **`groupby([...], as_index=False).agg({col: "sum", …})`**: one row per group, summing each
  column. `as_index=False` keeps the group columns as ordinary columns.
- **`sort_values("revenue", ascending=False)`**: highest revenue first.
- **`to_gbq("dataset.table", if_exists="replace")`** writes the result as a BigQuery table,
  replacing it if it exists.
- **`head(5)`**: the first 5 rows. Only now does bigframes run the query and bring 5 rows into the
  notebook.

Output (rounded here):

| product_id | name | category | revenue | margin | quantity |
|---|---|---|---|---|---|
| 173 | Product 173 | Home & Kitchen | 564400.32 | 282258.36 | 1164 |
| 2 | Product 2 | Home & Kitchen | 549609.45 | 215263.23 | 1131 |
| 156 | Product 156 | Stationery | 526781.94 | 221019.71 | 1087 |
| 160 | Product 160 | Books | 517800.30 | 288660.18 | 1101 |
| 6 | Product 6 | Home & Kitchen | 515042.55 | 270431.37 | 1089 |

!!! tip "See the SQL bigframes wrote"
    BigQuery Studio → **Query history** (Personal history) shows the queries that bigframes
    ran: the `merge`s became `JOIN`s, the `groupby` a `GROUP BY`.

### 5 · A quick chart in the notebook
```python
%%bigquery monthly
select date_trunc(order_date, month) as month, round(sum(revenue), 2) as revenue
from shopflow_gold.daily_sales
group by month
order by month
```

```python
monthly.plot(x="month", y="revenue", title="ShopFlow delivered revenue per month", figsize=(10, 4))
```

- **`%%bigquery monthly`**: the result is also saved in a pandas DataFrame named `monthly`.
- **`date_trunc(date, month)`**: the first day of that date's month. In BigQuery the unit comes
  **second** and is a keyword (in Trino it's `date_trunc('month', d)`).
- **`df.plot(x=…, y=…)`**: a line chart from pandas (matplotlib draws it). The result has only
  24 rows, so pandas is fine here.

The first months:

| month | revenue |
|---|---|
| 2024-06-01 | 2142164.80 |
| 2024-07-01 | 2077430.39 |
| 2024-08-01 | 2282856.45 |

### 6 · Stop the runtime
When you're done: the **▾** next to *Connect* (top right) → **Disconnect and delete runtime**.
The notebook itself is saved in BigQuery Studio (Explorer → *Notebooks*) and starts a new
runtime when you open it again.

## Check yourself
1. Does `bpd.read_gbq("shopflow_silver.orders")` copy 39,900 rows into the notebook?
   *(No. The DataFrame stays in BigQuery; only `head()` or `to_pandas()` bring rows over.)*
2. Write the BigQuery version of Trino's `date_trunc('month', order_date)`.
   *(`date_trunc(order_date, month)`.)*
3. What keeps billing after you close the browser tab? *(The runtime VM, until it's deleted or
   stops after being idle. Delete it at the end.)*
