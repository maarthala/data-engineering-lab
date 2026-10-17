# Your Snowflake trial & the ShopFlow data

This part of the course happens **in Snowflake itself**, not in the lab. You bring a free
Snowflake **trial account**, load the same **ShopFlow** data you know from the lab with one SQL
file, and then practise every topic of the **SnowPro Core** certification on it — warehouses,
micro-partitions, caching, cloning, Time Travel, JSON, stages, streams, tasks, dynamic tables,
roles, masking and cost control.

!!! info "Why your own account?"
    Snowflake is a managed cloud service — there is no open-source copy of it to run in the lab.
    The SQL you learned in the lab transfers almost unchanged; what's new here is the
    **platform**: how Snowflake stores, caches, versions and secures your data. You only learn that
    on the real thing.

## Concept

### The trial account
A Snowflake trial is **free for 30 days** and comes with **US$400 of credits**. You don't need a
credit card. When you sign up you choose:

- **Edition — pick *Enterprise*.** A few topics (90-day Time Travel, masking and row access
  policies, multi-cluster warehouses, materialized views) need Enterprise. Lessons mark them
  *[Enterprise]*.
- **Cloud and region** — any. AWS in a region near you is fine.

You pay for **compute** (a *warehouse* running, billed per second) and **storage**. The ShopFlow
data is tiny, so storage costs nearly nothing. Compute is where credits go — every lesson uses
one **X-Small** warehouse that stops itself after 60 seconds idle. The whole Snowflake part of
the course uses a few credits.

### Snowsight — the web UI
**Snowsight** is Snowflake's browser UI — the website you log into. The name doesn't appear as
a menu; when a lesson says "in Snowsight", it means this website. You'll use:

- **Projects → Workspaces** (older accounts: **Worksheets**) — a SQL editor. Each file runs
  against the role, warehouse, database and schema picked at the top (or set with `use …`).
- **Monitoring → Query History** — every query you ran, with its **Query Profile** (the
  execution plan, like the Spark UI or Trino's query page).
- **Catalog → Explorer** (some accounts: **Database Explorer**; older accounts: **Data → Databases**) — browse databases,
  schemas and tables.

!!! note "Menu names change"
    Snowflake updates the web UI often — sometimes while you're using it. A menu may have a
    new name (e.g. **Catalog → External data** became **Catalog → Connections**), but the page
    behind it is the same. If a name in a lesson doesn't match, look for the nearest one.

**Run a statement:** put the cursor in it and press **Ctrl/Cmd + Enter**.
**Run a whole file:** **Ctrl/Cmd + Shift + Enter**.
**End every statement with `;`** — Snowsight finds where one statement stops by the `;`. If the
statement above is missing it, the next one fails with *syntax error … unexpected 'show'* (or
`select`, …) — the two run together as one.

### Account identifier
Every account has an identifier `ORGNAME-ACCOUNTNAME`, e.g. `ZEYYXVA-EP36955`. Find yours in
Snowsight: click your name (bottom left) → **Account** → **View account details**. Tools
outside Snowsight (dbt, Python, BI tools) need it, plus a user, role, warehouse, database and
schema.

!!! warning "Don't add the domain yourself"
    Clients add `.snowflakecomputing.com` themselves. If you type it into an *account* field you
    get a doubled host name (`…snowflakecomputing.com.snowflakecomputing.com`) and an **SSL
    certificate error**.

### Roles you'll use
Everything in Snowflake is done **as a role**. The setup runs as `ACCOUNTADMIN`, the top role a
trial gives you. That keeps the practice simple; the security lesson shows how a real project
would use smaller roles instead.

## Lab

### 1 · Replace the trial's default warehouse
A new trial account comes with one warehouse, **`COMPUTE_WH`**, and every new SQL file uses it.
Check it before you start. Open Snowsight → **Projects → Workspaces** → **+ Add new → SQL File**
(older accounts: **Worksheets → + → SQL Worksheet**) and run:

```sql
use role accountadmin;
show warehouses;
```

- **`show warehouses`** — every warehouse with its `state` (`STARTED` / `SUSPENDED`), `size`
  and `auto_suspend` (seconds idle before it stops).

Example output:

| name | state | type | size | … | auto_suspend | auto_resume |
|---|---|---|---|---|---|---|
| COMPUTE_WH | SUSPENDED | STANDARD | X-Small | … | 600 | true |

Look at `size` and `auto_suspend` in your result. This course needs one **X-Small** warehouse
that stops after **60 seconds** idle; `COMPUTE_WH` was made with the trial's own settings (often a
longer auto-suspend, so it keeps billing after you stop). If `COMPUTE_WH` is in the list, drop
it:

```sql
drop warehouse if exists COMPUTE_WH;
```

- **`drop warehouse if exists`** — removes it; `if exists` avoids an error if your account never
  had one. Dropping a warehouse deletes **no data** — it's only compute.

!!! warning "Is your account shared?"
    Only drop `COMPUTE_WH` in your **own** trial account. In an account others use, leave it
    alone and just add `PRACTICE_WH` next to it.

Now create the course's own warehouse, **`PRACTICE_WH`**, with the right settings. Every lesson
uses it:

```sql
create warehouse if not exists PRACTICE_WH
    warehouse_size      = 'XSMALL'
    auto_suspend        = 60
    auto_resume         = true
    initially_suspended = true;

use warehouse PRACTICE_WH;
```

- **`create warehouse if not exists`** — makes a new warehouse; `if not exists` skips it if you
  already have one with that name (safe to run again).
- **`warehouse_size = 'XSMALL'`** — the smallest size, 1 credit per hour while running. Each size
  up doubles the credits.
- **`auto_suspend = 60`** — stops itself after 60 seconds with no queries, so it stops billing.
- **`auto_resume = true`** — starts again by itself when the next query arrives.
- **`initially_suspended = true`** — created stopped; it costs nothing until you run a query.
- **`use warehouse`** — the queries you run next in this SQL file use `PRACTICE_WH`.

Check it:

```sql
show warehouses;
```

Example output:

| name | state | type | size | … | auto_suspend | auto_resume |
|---|---|---|---|---|---|---|
| PRACTICE_WH | SUSPENDED | STANDARD | X-Small | … | 60 | true |

### 2 · Download and run the setup file

[:material-download: **shopflow_snowflake_setup.sql**](../assets/snowflake/shopflow_snowflake_setup.sql){: download="shopflow_snowflake_setup.sql" }

1. In the same SQL file (or a new one), replace everything with the whole setup file (or use
   **Import** / drag the file in).
2. Run all: **Ctrl/Cmd + Shift + Enter**. It finishes in under a minute.
3. At the top of the editor, the warehouse shows **`PRACTICE_WH`** — the file runs on the
   warehouse you just created.

What the file creates:

```
SHOPFLOW               database
├── BRONZE             the 12 ShopFlow source tables (raw copy of the source)
├── SILVER             empty — you fill it
└── GOLD               empty — you fill it
```

It is the same medallion layout you built in the lab: **Bronze** raw, **Silver** clean,
**Gold** business marts. The tables and columns match the lab's
[ShopFlow schema](../unit0/schema.md) exactly — upper-cased, because Snowflake stores unquoted
names in upper case (`orders` and `ORDERS` are the same table).

!!! note "Same data shape, generated in Snowflake"
    Like the lab's `seed.sql`, the file **generates** the rows with random values — there's
    nothing to upload. The tables, columns, row counts and patterns (skewed customers, mostly
    delivered orders, ~6% returns) are the lab's. The exact values are not, so your numbers in
    the lessons will differ from the examples. That's expected.

### 3 · Check the result
The last statement of the file prints a row count per table:

| TABLE_NAME | ROW_COUNT |
|---|---|
| CUSTOMERS | 6000 |
| INVENTORY | 200 |
| MARKETING_CAMPAIGNS | 15 |
| ORDERS | 40000 |
| ORDER_ITEMS | 100000 |
| PAYMENTS | ~36000 |
| PRODUCTS | 200 |
| PROMOTIONS | 12 |
| RETURNS | ~4000 |
| REVIEWS | ~8000 |
| SUPPLIERS | 20 |
| SUPPORT_TICKETS | ~3200 |

Rows marked `~` are random (for example, ~6% of delivered lines become returns), so yours will be
close but not equal.

### 4 · Your first queries
Open a new SQL file and set the context once at the top:

```sql
use role accountadmin;
use warehouse PRACTICE_WH;
use schema SHOPFLOW.BRONZE;

select status, count(*) as orders
from ORDERS
group by status
order by orders desc;
```

- **`use role` / `use warehouse` / `use schema`** set the context for the session. `use schema
  SHOPFLOW.BRONZE` sets the database and the schema in one go. After that, `ORDERS` means
  `SHOPFLOW.BRONZE.ORDERS`.

Example output (your counts differ):

| STATUS | ORDERS |
|---|---|
| delivered | 27834 |
| shipped | 4082 |
| cancelled | 4046 |
| placed | 4038 |

That's the same query you ran in Trino on day one — the SQL hasn't changed, only where it runs.

### 5 · Your setup file, re-runnable
The file starts with `create or replace database SHOPFLOW`, so running it again **wipes and
rebuilds** ShopFlow. Do that any time an exercise leaves the data in a mess.

## Check yourself
1. Which Snowsight page shows how a query was executed?
   *(Monitoring → Query History → the query → Query Profile.)*
2. What costs credits in this course? *(The warehouse while it runs. Storage for ShopFlow is
   tiny.)*

!!! tip "Cost safety from day one"
    Every lesson that starts something that keeps running (a task, a dynamic table) also stops
    it. The last lesson, [Clean up & checklist](cleanup.md), removes everything. If you take a
    break of several days, run that cleanup.
