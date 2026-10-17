# Functions & procedures — your own logic in Snowflake

## Concept
In the lab you gave a repeated calculation a name with Trino's `WITH FUNCTION`
([Functions](../unit2/functions.md)) — but it lived for **one query**. In Snowflake you **create**
a function once; it's saved in a schema like a table, and every query (and every role you grant
it to) can call it.

| Kind | Returns | Called with | Typical use |
|---|---|---|---|
| **UDF** (user-defined function) | one value per row | `select f(col) …` | a formula, cleaning a value |
| **UDTF** (table function) | rows | `select * from table(f(…))` | a parameterised query, splitting a value into rows |
| **Stored procedure** | one result, once | `call p(…)` | several statements: DDL, DML, admin jobs, a task's body |

The body can be **SQL** or **Python** (also Java, JavaScript, Scala). Python runs **inside**
Snowflake on your warehouse — no server of your own.

!!! note "The arguments are part of the name"
    `MARGIN_PCT(number, number)` and `MARGIN_PCT(float, float)` would be **two different
    functions** (overloading). That's why `describe`, `grant` and `drop` need the argument types.

## Lab

```sql
use role accountadmin;
use warehouse PRACTICE_WH;
use schema SHOPFLOW_DEV.PRACTICE;

show user functions in schema SHOPFLOW_DEV.PRACTICE;    -- what exists already
```

### 1 · A SQL UDF: product margin

```sql
create or replace function MARGIN_PCT(price number(10,2), cost number(10,2))
  returns number(5,1)
  comment = 'Gross margin % of a product'
as
$$
  round((price - cost) / nullif(price, 0) * 100, 1)
$$;

select name, category, price, cost, MARGIN_PCT(price, cost) as margin
from SHOPFLOW_DEV.BRONZE.PRODUCTS
order by margin desc
limit 5;
```

- **`$$ … $$`** — the body. For a SQL UDF it's **one expression** (no `select`, no `;`).
- **`returns`** — the type of the answer. `number(5,1)`: up to 5 digits, 1 after the point.
- **`nullif(price, 0)`** — a price of 0 gives `NULL` instead of a divide-by-zero error.
- Call it like a built-in — in `select`, `where`, `group by`, a view, a dynamic table.

### 2 · A Python UDF: e-mail domain

```sql
create or replace function EMAIL_DOMAIN(email varchar)
  returns varchar
  language python
  runtime_version = '3.11'
  handler = 'domain'
as
$$
def domain(email):
    if not email or "@" not in email:
        return None
    return email.split("@", 1)[1].lower()
$$;

select EMAIL_DOMAIN(email) as domain, count(*) as customers
from SHOPFLOW_DEV.BRONZE.CUSTOMERS
group by domain
order by customers desc;
```

- **`handler`** — which Python function Snowflake calls, once per row.
- **`runtime_version`** — the Python version.
- Python is the choice when SQL gets awkward: parsing, regex-heavy rules, a library. Third-party
  packages go in `packages = ('…')`; the first time, an ORGADMIN accepts the Anaconda terms
  (Admin → Terms).

!!! tip "SQL first"
    A SQL UDF is faster than a Python one (no Python per row). Use Python only for what SQL can't
    do simply.

### 3 · A table function (UDTF): a customer's orders

```sql
create or replace function ORDERS_OF(cid int)
  returns table (order_id int, status varchar, order_ts timestamp_ntz)
as
$$
  select order_id, status, order_ts
  from SHOPFLOW_DEV.BRONZE.ORDERS
  where customer_id = cid
$$;

select * from table(ORDERS_OF(72)) order by order_ts;
```

- **`returns table (…)`** — the columns it returns.
- **`table(…)`** — call it in the `FROM`. Join it like a table, or once per row with `lateral`:
  `select c.full_name, o.* from SHOPFLOW_DEV.BRONZE.CUSTOMERS c, table(ORDERS_OF(c.customer_id)) o`.

### 4 · A stored procedure: stop the practice objects

A procedure runs **statements**, not a value per row — here, the "things that run on their own"
from the [clean-up](cleanup.md):

```sql
create or replace procedure SUSPEND_PRACTICE()
  returns varchar
  language sql
as
$$
begin
  alter task if exists SHOPFLOW_DEV.PRACTICE.MERGE_ORDERS_TASK suspend;
  alter dynamic table if exists SHOPFLOW_DEV.PRACTICE.CHANNEL_REVENUE_DT suspend;
  return 'practice task and dynamic table suspended';
end;
$$;

call SUSPEND_PRACTICE();
```

- **`begin … end;`** — Snowflake Scripting: statements, variables (`let n int := 0;`), `if`,
  `for` loops, exception handling.
- **`call`** — runs it once and returns its value.
- A **task** can run a procedure on a schedule — `create task … as call SUSPEND_PRACTICE();` —
  the way to run several statements from one task.

### 5 · Register a Python function from Snowpark

In the [Snowpark](snowpark.md) notebook, a Python function becomes a saved SQL function:

```python
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.types import StringType

session = get_active_session()

def email_domain(email: str) -> str:
    return email.split("@", 1)[1].lower() if email and "@" in email else None

session.sql("create stage if not exists SHOPFLOW_DEV.PRACTICE.UDF_STAGE").collect()
session.udf.register(
    email_domain,
    name="SHOPFLOW_DEV.PRACTICE.EMAIL_DOMAIN_PY",
    return_type=StringType(), input_types=[StringType()],
    is_permanent=True, stage_location="@SHOPFLOW_DEV.PRACTICE.UDF_STAGE",
    replace=True,
)
```

Then from any worksheet:

```sql
select email, EMAIL_DOMAIN_PY(email) from SHOPFLOW_DEV.BRONZE.CUSTOMERS limit 5;
```

- **`is_permanent=True`** — saved in the schema; the code goes to the stage. Without it the
  function lasts only as long as the notebook session.
- **`session.sproc.register(…)`** does the same for a stored procedure.

### 6 · Find, inspect, share and drop

```sql
show user functions in schema SHOPFLOW_DEV.PRACTICE;
show procedures like 'SUSPEND_PRACTICE';
describe function MARGIN_PCT(number, number);
select get_ddl('function', 'MARGIN_PCT(number, number)');

-- let the reader role from the security lesson use it
grant usage on function SHOPFLOW_DEV.PRACTICE.MARGIN_PCT(number, number) to role REPORTING_READER;

drop function if exists MARGIN_PCT(number, number);
```

- A role needs **`usage`** on the function (plus on its database and schema) to call it — not on
  the tables it reads inside: a function runs with its **owner's** rights.
- **`create secure function`** hides the body from people who can only use it — required for
  functions shared with another account (data sharing).

!!! info "Compare"
    **Trino `WITH FUNCTION`** — one query only. **Spark** — `spark.udf.register(...)` for the
    session, or `CREATE FUNCTION` in the Databricks catalog. **Snowflake** — saved in a
    schema, granted like a table, SQL or Python.

## Check yourself
1. What's the difference between `select MARGIN_PCT(price, cost)` and `call SUSPEND_PRACTICE()`?
   *(A UDF returns a value for every row of a query; a procedure runs its statements once.)*
2. Why does `drop function MARGIN_PCT` fail without the argument types?
   *(The types are part of the function's identity — there could be several `MARGIN_PCT`s.)*
3. A reader can call `ORDERS_OF(72)` but has no `select` on `ORDERS`. Does it work?
   *(Yes — the function runs with its owner's rights; the reader only needs `usage` on it.)*
