# Conditional logic, NULLs & filtering

## Concept
Raw data is messy: missing values, codes that need labels, categories you want as columns.
Four everyday tools handle almost all of it — and you'll use them in **every** transform:

- **`CASE`** — SQL's if/else. Turn values into labels, buckets, or flags.
- **Conditional aggregation** — `SUM(CASE WHEN …)` or the `FILTER (WHERE …)` clause to
  compute *"how many of X"* per group, or to **pivot** rows into columns.
- **`COALESCE` / `NULLIF`** — handle `NULL`s: supply a default, or guard against divide-by-zero.
- **`CAST` / `TRY_CAST`** — convert types safely (text → number, timestamp → date).

### What `NULL` actually means
Before any of this makes sense, you need to know what `NULL` is. **`NULL` is not zero, and it is
not an empty string** — it means the value is **unknown or absent**. An order with no promotion
has `promo_id = NULL`: there simply *is* no promotion, not "a promotion worth 0".

Because `NULL` means "unknown", it behaves in surprising ways and needs special handling:

- **`NULL` is contagious in arithmetic.** `NULL + 5` is `NULL`, `NULL * 2` is `NULL`. Any sum
  touching an unknown value becomes unknown.
- **You can't test it with `=`.** `promo_id = NULL` is never true (it's "unknown"); you must write
  `promo_id IS NULL` / `IS NOT NULL` instead.
- **Aggregates skip it.** `COUNT(col)` and `SUM(col)` quietly ignore `NULL`s (you saw this with
  `COUNT(col)` in [2.2](joins-aggregations.md)).

The tools in this lesson exist largely to *tame* `NULL`: `COALESCE` replaces it with a default,
`NULLIF` deliberately *creates* one to dodge a divide-by-zero, and `TRY_CAST` produces one instead
of crashing on bad input.

### How `CASE` works — SQL's if/else
**`CASE`** is the closest SQL has to an `if`/`else if`/`else` block. You list conditions top to
bottom; the **first `WHEN` that is true wins**, and its `THEN` value is returned. If none match,
you get the `ELSE` value (or `NULL` if there's no `ELSE`). The whole thing evaluates to a *single
value per row*, so you can drop it right into a `SELECT` list like any other column:

```
CASE
  WHEN <condition> THEN <value>     -- first true wins
  WHEN <condition> THEN <value>
  ELSE <fallback>                   -- optional; NULL if omitted
END
```

That one construct powers everything else in this lesson — labelling rows, counting only *some* of
them, and pivoting rows into columns are all just `CASE` wearing different hats.

## Lab
> Run these in **SQLPad** on the **Lakehouse (Trino) — &lt;your username&gt;** connection (not *ShopFlow — OLTP*) or **Superset SQL Lab** (see [2.1](intro.md)). In Superset,
> pick the **shopflow / public** schema and skip the `USE` line below.

```sql
USE shopflow.public;
```

`USE` sets the default catalog + schema so you can write `orders` instead of the full
`shopflow.public.orders` every time.

### 1 · `CASE` — label each order's outcome
Orders carry a raw `status` code (`delivered`, `cancelled`, `pending`, …). This query keeps the
count per status but *also* rolls each status up into a plain-English **outcome** the business
understands — revenue, lost, or in progress:

```sql
SELECT status,
       CASE
         WHEN status = 'delivered' THEN 'revenue'
         WHEN status = 'cancelled' THEN 'lost'
         ELSE 'in progress'
       END AS outcome,
       count(*) AS orders
FROM orders
GROUP BY status
ORDER BY orders DESC;
```

**Read it clause by clause:**

- **`SELECT status`** — show the raw status code as-is, so you can see the mapping.
- **`CASE WHEN status = 'delivered' THEN 'revenue' …`** — the if/else. For each row, SQL checks the
  `WHEN`s in order: a delivered order becomes `'revenue'`, a cancelled one becomes `'lost'`.
- **`ELSE 'in progress'`** — anything that matched *no* `WHEN` (pending, shipped, …) falls through
  to this default.
- **`END AS outcome`** — `END` closes the `CASE`; **`AS outcome`** names the new derived column.
- **`count(*) AS orders`** — how many rows in each group. `count(*)` counts every row (it never
  skips `NULL`s — see the note below).
- **`GROUP BY status` / `ORDER BY orders DESC`** — one row per status, most common first.

The result shows each raw status alongside its friendly label — the same trick you'd use to bucket
ages into "child / adult / senior" or prices into "cheap / mid / premium".

!!! note "`count(*)` vs `count(col)`"
    **`count(*)`** counts rows — it always sees every row. **`count(col)`** counts only rows where
    `col` is **not `NULL`**. That difference is the engine behind conditional aggregation, coming up
    next.

### 2 · Conditional aggregation — many counts in one row
**Conditional aggregation** means *"aggregate only the rows that meet a condition"* — count or sum a
subset, per group, and put several such subsets side by side on **one row**. There are two ways to
write it, and they produce identical results:

- **`FILTER (WHERE …)`** — attach a condition to an aggregate; it counts/sums only the rows that
  pass. `count(*) FILTER (WHERE status = 'delivered')` = "how many delivered, in this group".
- **`SUM(CASE WHEN … THEN 1 ELSE 0 END)`** — the older, universal trick: emit `1` for matching rows
  and `0` for the rest, then sum. The `1`s add up to exactly the matching count.

Here both appear together, per country, so you can see they agree:

```sql
SELECT c.country,
       count(*)                                              AS total_orders,
       count(*) FILTER (WHERE o.status = 'delivered')        AS delivered,
       count(*) FILTER (WHERE o.status = 'cancelled')        AS cancelled,
       sum(CASE WHEN o.status = 'delivered' THEN 1 ELSE 0 END) AS delivered_via_case
FROM orders o
JOIN customers c ON c.customer_id = o.customer_id
GROUP BY c.country
ORDER BY total_orders DESC;
```

**Read it clause by clause:**

- **`count(*) AS total_orders`** — the plain total per country (no condition).
- **`count(*) FILTER (WHERE o.status = 'delivered')`** — of those, how many were delivered. The
  `FILTER` restricts *this one aggregate* without touching the others on the row.
- **`count(*) FILTER (WHERE o.status = 'cancelled')`** — same idea for cancellations.
- **`sum(CASE WHEN o.status = 'delivered' THEN 1 ELSE 0 END)`** — the `CASE` form of the delivered
  count: `1` for each delivered row, `0` otherwise, summed. Its value equals the `delivered` column
  above — same answer, portable syntax.
- **`GROUP BY c.country`** — one row per country, with total / delivered / cancelled all on it.

This is how you turn *"how many of X per group?"* into a compact report: several conditional counts
living together on a single row, instead of one query per condition.

### 3 · Pivot rows into columns
A **pivot** takes values that live *down* a column (the three `channel` values: `web`, `app`,
`marketplace`) and spreads them *across* as separate columns. It's conditional aggregation again —
one `SUM(CASE WHEN channel = 'X' …)` per channel — and it's the classic "SQL pivot". Here: revenue
per category, split by channel:

```sql
SELECT p.category,
       sum(CASE WHEN o.channel = 'web'         THEN oi.quantity*oi.unit_price END) AS web_rev,
       sum(CASE WHEN o.channel = 'app'         THEN oi.quantity*oi.unit_price END) AS app_rev,
       sum(CASE WHEN o.channel = 'marketplace' THEN oi.quantity*oi.unit_price END) AS market_rev
FROM orders o
JOIN order_items oi ON oi.order_id = o.order_id
JOIN products   p  ON p.product_id = oi.product_id
WHERE o.status = 'delivered'
GROUP BY p.category
ORDER BY p.category;
```

**Read it clause by clause:**

- **`GROUP BY p.category`** — one row per product category (the row grain).
- **`sum(CASE WHEN o.channel = 'web' THEN oi.quantity*oi.unit_price END) AS web_rev`** — for each
  row, if the channel is `web`, contribute that line's revenue; otherwise contribute `NULL`. `SUM`
  ignores the `NULL`s, so this column adds up **web revenue only**.
- The `app` and `marketplace` columns are the same recipe with a different channel. Each channel
  becomes its **own column**.
- **`WHERE o.status = 'delivered'`** — count only realised revenue.

Notice there's **no `ELSE`** here: when the channel doesn't match, `CASE` returns `NULL`, and since
`SUM` skips `NULL`s that row simply doesn't add to *that* channel's total. The result is one row per
category with `web_rev`, `app_rev`, and `market_rev` side by side — rows (channels) turned into
columns.

!!! info "Why the pivot column list is fixed"
    You had to name `web`, `app`, and `marketplace` by hand — one `CASE` each. SQL can't invent
    columns from data it hasn't seen, so a new channel means a new line in the query. That's the
    trade-off of the `CASE` pivot: it's 100% portable but manual. (Some engines add a `PIVOT`
    operator as sugar over exactly this.)

### 4 · `COALESCE` — supply a default for `NULL`
**`COALESCE(a, b, c, …)`** returns the **first argument that isn't `NULL`**, scanning left to right.
Its everyday job is *"use this value, but if it's missing fall back to that."* Here `promo_id` is
`NULL` whenever no promotion applied, and we replace those blanks with the label `'no promo'`:

```sql
SELECT order_id,
       COALESCE(CAST(promo_id AS varchar), 'no promo') AS promo
FROM orders
LIMIT 10;
```

**Read it clause by clause:**

- **`CAST(promo_id AS varchar)`** — turn the numeric `promo_id` into text so it can sit in the same
  column as the word `'no promo'` (a column must hold one type). More on `CAST` below.
- **`COALESCE(…, 'no promo')`** — if that cast value is `NULL` (no promotion), use `'no promo'`
  instead; otherwise keep the promo id.
- **`LIMIT 10`** — just a sample.

Every row now shows either a promo id or the friendly `'no promo'` — no bare `NULL`s to confuse a
dashboard or a downstream calculation.

### 5 · `NULLIF` — guard against divide-by-zero
**`NULLIF(a, b)`** returns `NULL` when `a` equals `b`, and otherwise returns `a`. That sounds odd
until you meet its killer use: **divide-by-zero protection**. Dividing by `0` errors out; dividing
by `NULL` quietly yields `NULL`. So wrapping a denominator in `NULLIF(denominator, 0)` turns a
potential crash into a harmless `NULL`. Here it protects a cancellation-rate calculation per
country:

```sql
SELECT c.country,
       count(*) FILTER (WHERE o.status = 'cancelled') AS cancelled,
       count(*)                                       AS total,
       round(100.0 * count(*) FILTER (WHERE o.status = 'cancelled')
             / NULLIF(count(*), 0), 1)                AS cancel_pct
FROM orders o
JOIN customers c ON c.customer_id = o.customer_id
GROUP BY c.country
ORDER BY cancel_pct DESC;
```

**Read it clause by clause:**

- **`count(*) FILTER (WHERE o.status = 'cancelled') AS cancelled`** — cancelled orders in the group
  (conditional count from step 2).
- **`count(*) AS total`** — all orders in the group; this is the denominator.
- **`100.0 * cancelled / NULLIF(total, 0)`** — the percentage. Multiplying by `100.0` (a decimal,
  not `100`) forces decimal division so you don't lose the fraction to integer maths.
- **`NULLIF(count(*), 0)`** — if a country somehow has `0` orders, the denominator becomes `NULL`
  rather than `0`, so the division yields `NULL` instead of throwing a divide-by-zero error.
- **`round(…, 1)`** — round the percentage to one decimal place.
- **`ORDER BY cancel_pct DESC`** — worst cancellation rate first.

Any group with a `NULL` result is simply "no data to compute a rate" — far safer than a query that
aborts partway through.

### 6 · `CAST` / `TRY_CAST` — convert types safely
Every column has a **type** (text, integer, date, timestamp…). **`CAST(value AS type)`** converts a
value from one type to another — but if the value doesn't fit the target type, `CAST` **errors and
stops the whole query**. **`TRY_CAST`** does the same conversion but returns **`NULL`** instead of
erroring on bad input — invaluable when ingesting dirty data where a stray `'USD'` sits in a column
you expected to be numeric:

```sql
SELECT CAST(order_ts AS date)      AS order_date,   -- timestamp → date
       TRY_CAST(currency AS integer) AS bad_cast     -- 'USD' → NULL, no error
FROM orders
LIMIT 5;
```

**Read it clause by clause:**

- **`CAST(order_ts AS date)`** — chop a full timestamp (`2024-03-11 14:07:22`) down to just the
  date (`2024-03-11`). This one always succeeds, because a timestamp *is* a valid date.
- **`TRY_CAST(currency AS integer)`** — `currency` holds text like `'USD'`, which is not a number.
  Plain `CAST` would abort the query here; `TRY_CAST` shrugs and returns `NULL` for every row, so
  the query still completes.

**Rule of thumb:** reach for `CAST` when you're certain the data fits the target type (a known
timestamp → date), and `TRY_CAST` when the input might be messy and you'd rather get a `NULL` than a
failed job. In Silver-layer cleaning, `TRY_CAST` is your friend.

!!! tip "`FILTER` vs `CASE`"
    `count(*) FILTER (WHERE …)` is the cleaner, modern form and works on **Trino, Spark/
    Databricks, and Postgres**. `SUM(CASE WHEN … THEN 1 ELSE 0 END)` is the universal form
    that *also* works on **Snowflake and Fabric (T-SQL)**. When in doubt, reach for `CASE`.

## Challenge
Build a **channel scorecard**: one row per `channel` showing total orders, delivered orders,
delivered %, cancelled %, and total delivered revenue. Sort by delivered revenue, highest first.

!!! tip "Which pieces do you need?"
    It's everything from this lesson at once: **`GROUP BY o.channel`** for one row per channel,
    **conditional counts** (`FILTER (WHERE …)`) for delivered and cancelled, **`NULLIF(…, 0)`** to
    protect each percentage's denominator, and a **`SUM(CASE WHEN status = 'delivered' …)`** to add
    up only delivered revenue. **Watch out:** you need `order_items` for revenue, but joining it
    puts *one row per line item*, not per order — so count **`DISTINCT o.order_id`**, not `count(*)`.

??? note "Solution"
    ```sql
    SELECT o.channel,
           count(DISTINCT o.order_id)                                     AS orders,
           count(DISTINCT o.order_id) FILTER (WHERE o.status = 'delivered') AS delivered,
           round(100.0 * count(DISTINCT o.order_id) FILTER (WHERE o.status = 'delivered')
                 / NULLIF(count(DISTINCT o.order_id), 0), 1)              AS delivered_pct,
           round(100.0 * count(DISTINCT o.order_id) FILTER (WHERE o.status = 'cancelled')
                 / NULLIF(count(DISTINCT o.order_id), 0), 1)             AS cancelled_pct,
           round(sum(CASE WHEN o.status = 'delivered'
                          THEN oi.quantity*oi.unit_price ELSE 0 END), 2) AS delivered_revenue
    FROM orders o
    JOIN order_items oi ON oi.order_id = o.order_id
    GROUP BY o.channel
    ORDER BY delivered_revenue DESC;
    ```

    !!! warning "The join fan-out trap"
        `delivered_revenue` needs `order_items` (revenue lives on the line items), but that join
        makes each order appear **once per line item**. So a plain `count(*)` would count *line
        items, not orders* (here ~2.5× too high), and the percentages would be item-weighted.
        Counting **`DISTINCT o.order_id`** restores the true per-order numbers. Rule of thumb: the
        moment you join a one-to-many table, every parent-level `count`/`avg`/`sum(parent_col)`
        needs a `DISTINCT` or a pre-aggregation step.

!!! tip "🎯 This runs unchanged on Azure, Databricks, Snowflake & Fabric"
    **What you just did:** used `CASE`, conditional aggregation (pivot), `COALESCE`/`NULLIF`,
    and `CAST`/`TRY_CAST` to shape and clean data.

    - **Azure Databricks** / **Snowflake** — `CASE`, `COALESCE`, `NULLIF`, `CAST`/`TRY_CAST`
      and `SUM(CASE WHEN …)` pivots are standard ANSI SQL and run unchanged.
    - **Microsoft Fabric** — same in the SQL endpoint (T-SQL uses `TRY_CAST`/`TRY_CONVERT`;
      pivots via `CASE` or the `PIVOT` operator).
    - **Azure Data Factory** — the no-code analog is the **Derived Column** (CASE/coalesce
      expressions) and **Pivot** transformations in a Mapping Data Flow.

    Only the `FILTER (WHERE …)` shorthand isn't on Snowflake/Fabric — the `CASE` form is
    100% portable.

## Key terms, at a glance
| Term | Plain meaning |
|---|---|
| **`NULL`** | Unknown / absent — not `0`, not `''`; test with `IS NULL`, spreads through arithmetic |
| **`CASE WHEN … THEN … ELSE … END`** | SQL if/else — first true `WHEN` wins; derives labels, buckets, flags |
| **Conditional aggregation** | Count/sum only rows meeting a condition, per group |
| **`FILTER (WHERE …)`** | Cleaner shorthand for conditional aggregation (Trino/Spark/Postgres) |
| **`SUM(CASE WHEN … THEN 1 ELSE 0 END)`** | The portable form of a conditional count |
| **Pivot** | Turn row values into side-by-side columns, one `SUM(CASE WHEN …)` each |
| **`COALESCE(a,b,…)`** | First non-`NULL` argument — supply a default for missing data |
| **`NULLIF(a,b)`** | `NULL` when `a=b` — classic divide-by-zero guard on a denominator |
| **`CAST(x AS type)`** | Convert a value's type; **errors** on bad input |
| **`TRY_CAST(x AS type)`** | Same conversion, but yields `NULL` instead of erroring — for dirty data |

## You can now…
- Turn values into labels and buckets with `CASE`
- Compute per-group conditional counts and **pivot** rows into columns
- Handle `NULL`s safely with `COALESCE` and `NULLIF`
- Convert types with `CAST`, and survive dirty input with `TRY_CAST`
