# Functions: built-in, lambdas, and your own

## Concept
A **function** takes values in and gives a value back: `upper('uk')` → `'UK'`. You've already used
plenty: `SUM`, `ROUND`, `COALESCE`, `CAST`. Trino ships **hundreds** more, and knowing the right one
turns a 20-line workaround into one call.

Functions come in a few kinds:

| Kind | Works on | Returns | Example |
|---|---|---|---|
| **Scalar** | one row at a time | one value per row | `upper(country)`, `date_trunc('month', ts)` |
| **Aggregate** | a group of rows | one value per group | `sum(x)`, `count_if(...)`, `approx_distinct(...)` |
| **Window** | a group, without collapsing rows | one value per row | `rank() OVER (...)` ([2.3](window-functions.md)) |
| **Lambda** (higher-order) | the elements of an array | an array or a value | `transform(arr, x -> x * 2)` |
| **Your own** (user-defined) | whatever you define | whatever you define | `WITH FUNCTION margin_pct(...)` |

## Lab
> Run these in **SQLPad** on the **Lakehouse (Trino) — &lt;your username&gt;** connection (not *ShopFlow — OLTP*) or **Superset SQL Lab** (see [2.1](intro.md)).

### 1 · Find the function you need
```sql
SHOW FUNCTIONS LIKE '%date%';
```

The result lists every matching function with its argument types and a one-line description. The
full catalogue is in the Trino docs ("Functions and operators").

**`LIKE '%date%'`** matches text by pattern: `%` stands for "any characters, or none". So `'%date%'`
means "contains `date` anywhere" and matches `date_trunc`, `date_add` and `current_date`. `'date%'`
would mean "starts with `date`".

### 2 · Text
```sql
SELECT full_name,
       upper(full_name)                     AS shout,
       split_part(full_name, ' ', 1)        AS first_name,
       length(full_name)                    AS chars,
       split_part(email, '@', 2)            AS email_domain,
       regexp_like(email, '^[^@]+@[^@]+$')  AS looks_valid,
       country || '-' || loyalty_tier       AS segment
FROM shopflow.public.customers
LIMIT 5;
```

- **`upper(s)`** / **`lower(s)`**: the text in capitals / small letters (`'Ann Lee'` → `'ANN LEE'`).
- **`length(s)`**: how many characters the text has (`'Ann Lee'` → `7`, the space counts).
- **`split_part(s, sep, n)`**: the n-th piece after splitting on `sep`. Perfect for names, emails
  and codes. `split_part('Ann Lee', ' ', 1)` → `'Ann'`, and `split_part(email, '@', 2)` → the
  part after the `@`, the domain.
- **`regexp_like(s, pattern)`**: `true` if the text matches a **regular expression**, a pattern
  language for text. Here `'^[^@]+@[^@]+$'` reads: `^` start, `[^@]+` one or more characters that
  aren't `@`, then one `@`, then more non-`@` characters, `$` end. So `ann@shop.com` → `true`,
  `ann@@shop` → `false`. Its siblings `regexp_extract` (pull out the matching part) and
  `regexp_replace` (swap it for something else) help when simple splits aren't enough.
- **`||`**: glue strings together (same as `concat(...)`).

### 3 · Dates and times
```sql
SELECT order_id,
       order_ts,
       CAST(order_ts AS date)                         AS order_date,
       date_trunc('month', order_ts)                  AS order_month,
       day_of_week(order_ts)                          AS dow,
       format_datetime(order_ts, 'EEEE')              AS day_name,
       date_diff('day', order_ts, current_timestamp)  AS days_ago,
       date_add('day', 30, order_ts)                  AS return_deadline
FROM shopflow.public.orders
ORDER BY order_ts DESC
LIMIT 5;
```

Each new function, for an order placed on `2024-03-11 14:07:22`:

| Function | Result | What it does |
|---|---|---|
| `date_trunc('month', order_ts)` | `2024-03-01 00:00:00` | rounds **down** to the start of the month (or `'day'`, `'week'`, `'year'`) |
| `day_of_week(order_ts)` | `1` | day of the week as a number: 1 = Monday … 7 = Sunday |
| `format_datetime(order_ts, 'EEEE')` | `Monday` | the timestamp as text in a chosen format; `EEEE` = full day name, `yyyy-MM-dd` = `2024-03-11` |
| `current_timestamp` | now | the date and time the query runs (no brackets) |
| `date_diff('day', a, b)` | whole days from `a` to `b` | `b` minus `a` in the unit you name: `'day'`, `'hour'`, `'month'` |
| `date_add('day', 30, order_ts)` | `2024-04-10 14:07:22` | adds 30 days; a negative number goes back in time |

`date_trunc` is the workhorse of reporting: `GROUP BY date_trunc('month', order_ts)` gives monthly
totals.

### 4 · Numbers
```sql
SELECT name,
       price,
       cost,
       price - cost                                    AS profit,
       round((price - cost) / price * 100, 1)          AS margin_pct,
       ceil(price)                                     AS rounded_up,
       greatest(price * 0.9, cost)                     AS sale_price_floor
FROM shopflow.public.products
LIMIT 5;
```

- **`ceil(x)`**: rounds **up** to the next whole number (`19.01` → `20`). Its opposite `floor(x)`
  rounds down; `round(x, 1)` rounds to the nearest, here with 1 decimal.
- **`greatest` / `least`**: pick the largest or smallest of their arguments **within one row**.
  That's different from `max`/`min`, which work **across rows**. `greatest(price * 0.9, cost)`
  gives the 10%-off price, but never less than what the product cost.

### 5 · Aggregate functions beyond SUM and COUNT
```sql
SELECT channel,
       count(*)                                    AS orders,
       count_if(status = 'cancelled')              AS cancelled,
       round(100.0 * count_if(status = 'cancelled') / count(*), 1) AS cancel_pct,
       approx_distinct(customer_id)                AS customers,
       min(order_ts)                               AS first_order,
       array_join(array_agg(DISTINCT currency), ', ') AS currencies
FROM shopflow.public.orders
GROUP BY channel
ORDER BY orders DESC;
```

- **`count_if(condition)`**: count only the rows where the condition is true. It replaces
  `SUM(CASE WHEN … THEN 1 ELSE 0 END)`.
- **`approx_distinct`**: a fast **estimate** of `count(DISTINCT …)`, within about 2%. On billions of rows
  it's dramatically cheaper.
- **`array_agg`**: collect a group's values into an **array**, one list per group.
  `array_agg(DISTINCT currency)` keeps each value once, e.g. `['EUR', 'USD']`.
- **`array_join(array, ', ')`**: turn an array into one piece of text, with `', '` between the
  elements: `['EUR', 'USD']` → `'EUR, USD'`. Together they list every currency used per channel.

Arrays lead to…

### 6 · Arrays and lambdas
An **array** is a list of values inside **one** column of **one** row, written `ARRAY[1, 2, 3]`.
A **lambda** is a tiny unnamed function, written `x -> x * 10`: "take `x`, return `x * 10`".
Array functions apply it to every element:

```sql
SELECT transform(ARRAY[1, 2, 3], x -> x * 10)                 AS times_ten,   -- [10, 20, 30]
       filter(ARRAY[5, 15, 25], x -> x > 10)                  AS over_ten,    -- [15, 25]
       reduce(ARRAY[1, 2, 3, 4], 0, (s, x) -> s + x, s -> s)   AS total;       -- 10
```

- **`transform(array, lambda)`**: runs the lambda on every element and returns a new array of
  the results, the same length.
- **`filter(array, lambda)`**: keeps only the elements where the lambda returns `true`, so the
  array can get shorter.
- **`reduce(...)`**: folds the array into a single value, explained next.

**Read `reduce` part by part:** `reduce(array, start, step, finish)` folds a whole array into
**one** value, carrying a running value `s` along the way:

| Part | Here | Meaning |
|---|---|---|
| `array` | `ARRAY[1, 2, 3, 4]` | the values to walk through |
| `start` | `0` | the running value `s` begins at 0 |
| `step` | `(s, x) -> s + x` | for each element `x`: new `s` = old `s` + `x` |
| `finish` | `s -> s` | what to return at the end, here `s` unchanged |

Step by step, `s` goes `0 → 1 → 3 → 6 → 10`. Each element is added to the running total, so
`total = 10`. Change `step` to change the logic: `(s, x) -> s * x` with `start` = `1` gives the
product (`24`), and `(s, x) -> greatest(s, x)` gives the largest value.

**Now on real data.** Which customers buy the most expensive items? Each order has one or more
**order lines** (one product, a quantity and a price). The query works in two steps:

1. **Collect:** for each customer, `array_agg` gathers the value of every line they ever bought
   (`quantity × unit_price`) into **one array**, so each customer becomes one row.
2. **Count:** `cardinality` counts the elements of an array. `filter` with the lambda `v -> v > 500`
   keeps only the lines worth more than 500, and `cardinality` then counts those.

```sql
WITH per_customer AS (
  SELECT o.customer_id,
         array_agg(i.quantity * i.unit_price) AS line_values
  FROM shopflow.public.orders o
  JOIN shopflow.public.order_items i ON i.order_id = o.order_id
  GROUP BY o.customer_id
)
SELECT customer_id,
       cardinality(line_values)                      AS lines,
       cardinality(filter(line_values, v -> v > 500)) AS big_lines
FROM per_customer
ORDER BY big_lines DESC
LIMIT 5;
```

**`cardinality(array)`** returns **how many elements an array has**:

```sql
SELECT cardinality(ARRAY[932.88, 1108.45, 52.38, 324.69]) AS four,   -- 4
       cardinality(ARRAY[])                               AS zero;   -- 0
```

It's the array version of `count`, but they work at different levels. `count(*)` counts **rows**
across a group. `cardinality` counts the **elements inside one array**, within a single row. After
`array_agg`, each customer is one row holding an array, so `cardinality` is the right tool.
`cardinality(filter(...))` runs from the inside out: `filter` first shrinks the array, then
`cardinality` counts what's left.

**Follow one customer through it.** Customer 1529 bought four lines, so step 1 gives them:

| customer_id | line_values |
|---|---|
| 1529 | `[932.88, 1108.45, 52.38, 324.69]` |

Step 2: `cardinality(line_values)` = **4** lines. `filter(…, v -> v > 500)` keeps
`[932.88, 1108.45]`, so `big_lines` = **2**.

**The end result:** the five customers with the most lines over 500. Your numbers will differ, because
ShopFlow keeps adding orders:

| customer_id | lines | big_lines |
|---|---|---|
| 15 | 250 | 151 |
| 3 | 233 | 145 |
| 32 | 229 | 141 |
| 34 | 231 | 141 |
| 75 | 244 | 140 |

Read the first row as: customer 15 bought 250 lines, and 151 of them were worth more than 500.

### 7 · Your own function: `WITH FUNCTION`
When a calculation repeats, give it a name. Trino lets you define a **SQL function inline**, at the
top of the query that uses it:

```sql
WITH FUNCTION margin_pct(price decimal(10,2), cost decimal(10,2))
  RETURNS double
  RETURN round(CAST((price - cost) / price * 100 AS double), 1)
SELECT name, price, cost, margin_pct(price, cost) AS margin
FROM shopflow.public.products
ORDER BY margin DESC
LIMIT 5;
```

- **`WITH FUNCTION name(args) RETURNS type RETURN expression`**: declare it, then use it in the
  `SELECT` below.
- Each argument gets a **type**: `decimal(10,2)` is an exact number with up to 10 digits, 2 of them
  after the decimal point (right for money). `RETURNS double` means the answer is a floating-point
  number (fast, slightly approximate), which is why the expression `CAST`s to `double`.
- It lives **only for this query**, just like a CTE.

!!! info "Why not `CREATE FUNCTION` and keep it?"
    A **saved** function (`CREATE FUNCTION demouser_lake.my_lab.margin_pct …`) needs a catalog that can
    store functions. Our lakehouse catalog stores **tables and views**, not functions, so Trino
    answers *"This connector does not support creating functions"*. Open-source Spark 4.1 can't
    save SQL functions either. So on this stack:

    - reusable **SQL logic** → put it in a **view** ([2.5](views.md)) or a CTE;
    - a reusable **calculation** → `WITH FUNCTION` in the query;
    - logic that needs **Python** → a Spark **UDF** ([3.7](../unit3/spark.md#your-own-functions-in-python-udfs)).

## Common mistakes
| Symptom | Cause | Fix |
|---|---|---|
| `Function 'xyz' not registered` | wrong name, or another engine's function | `SHOW FUNCTIONS LIKE '%xyz%'` |
| `Unexpected parameters … for function` | argument types don't match | `CAST` the argument, check `SHOW FUNCTIONS` |
| Integer division gives `0` | `1 / 3` on integers is integer maths | multiply by `100.0` or `CAST(… AS double)` first |
| `approx_distinct` differs from `count(DISTINCT)` | it's an estimate (about 2% error) | fine for dashboards; use the exact one for finance |

## Key terms, at a glance
| Term | Plain meaning |
|---|---|
| **Scalar function** | one value in → one value out, per row |
| **Aggregate function** | many rows in → one value per group |
| **Lambda** | a tiny inline function: `x -> x * 2` |
| **Array** | a list of values inside one column of one row: `ARRAY[1, 2, 3]` |
| **Higher-order function** | a function that takes a lambda (`transform`, `filter`, `reduce`) |
| **`cardinality`** | the number of elements in an array |
| **UDF** | user-defined function: one you write yourself |
| **`WITH FUNCTION`** | a SQL UDF that lives for one query (Trino) |

## You can now…
- Find functions with `SHOW FUNCTIONS` and use the text, date, number and aggregate essentials
- Replace `SUM(CASE …)` with `count_if`, and know when `approx_distinct` is good enough
- Use **lambdas** with `transform`, `filter` and `reduce` on arrays
- Write your own SQL function with `WITH FUNCTION`, and know where reusable logic belongs here

## 🎯 This runs unchanged on Azure, Databricks, Snowflake & Fabric
Function **names** vary a little by engine; the **ideas** are identical everywhere:

| This stack (Trino) | Databricks / Spark SQL | Snowflake | Fabric / T-SQL |
|---|---|---|---|
| `count_if(c)` | `count_if(c)` | `COUNT_IF(c)` | `SUM(CASE WHEN c THEN 1 ELSE 0 END)` |
| `approx_distinct(x)` | `approx_count_distinct(x)` | `APPROX_COUNT_DISTINCT(x)` | `APPROX_COUNT_DISTINCT(x)` |
| `date_diff('day', a, b)` | `datediff(b, a)` | `DATEDIFF(day, a, b)` | `DATEDIFF(day, a, b)` |
| `transform(arr, x -> …)` | `transform(arr, x -> …)` | `TRANSFORM(arr, x -> …)` | n/a |
| `cardinality(arr)` | `size(arr)` | `ARRAY_SIZE(arr)` | n/a |
| `WITH FUNCTION` (inline) | `CREATE FUNCTION … RETURN` (saved in the catalog) | `CREATE FUNCTION` (SQL / Python) | `CREATE FUNCTION` |

In **Databricks** and **Snowflake**, SQL functions can be **saved** in the catalog and governed like
tables. That's the part our open-source stack doesn't have yet.
