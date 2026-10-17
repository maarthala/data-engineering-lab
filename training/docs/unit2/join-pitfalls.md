# Join pitfalls

## Concept
A query with a wrong join usually **doesn't fail** — it returns a number that looks fine and is
wrong. These mistakes are the same in every SQL engine: Trino, Postgres, Spark, Snowflake,
BigQuery. Learn to spot them once and you'll spot them everywhere.

| Pitfall | What goes wrong | The fix |
|---|---|---|
| Fan-out | a sum is counted 2–3× | aggregate to the grain you need **first**, then join |
| `COUNT(*)` after a join | counts lines, not orders | `COUNT(DISTINCT …)` |
| Inner join drops rows | customers without orders vanish | `LEFT JOIN` + `COALESCE` |
| `WHERE` on a left join | the `LEFT JOIN` silently becomes an inner join | put the condition in `ON` |
| `NOT IN` with a `NULL` | returns no rows at all | `NOT EXISTS` |
| Keys of different types | error, or wrong matches | `CAST` one side |

Two words used below:

- **Grain** — what one row means. `orders`: one row per order. `order_items`: one row per
  order **line**. `payments`: one row per order.
- **Fan-out** — joining to a table with a finer grain repeats every row of the coarser table, as
  you saw in [Joins & aggregations](joins-aggregations.md).

## Lab
Run these in **SQLPad** on the **Lakehouse (Trino) — &lt;your username&gt;** connection, like the
rest of this unit.

```sql
USE shopflow.public;
```

ShopFlow's data is generated at random, so your numbers differ a little from the examples —
the **pattern** is what matters.

### 1 · Fan-out: a sum counted several times
Question: *how much was paid, and how many items were sold, per channel?* The money is in
`payments` (one row per order), the items in `order_items` (several rows per order). Join both:

```sql
-- WRONG: every payment is repeated once per order line
SELECT o.channel,
       SUM(p.amount)    AS paid,
       SUM(oi.quantity) AS items
FROM orders o
JOIN payments    p  ON p.order_id  = o.order_id
JOIN order_items oi ON oi.order_id = o.order_id
GROUP BY o.channel
ORDER BY o.channel;
```

Example output:

| channel | paid | items |
|---|---|---|
| app | 59511692.29 | 75882 |
| marketplace | 31115591.56 | 39610 |
| web | 90165547.68 | 115788 |
| wholesale | 30118734.96 | 38729 |

`items` is right; `paid` is about **3× too high**. An order with 3 lines has 3 joined rows — each
carrying the same payment, and `SUM` adds it 3 times.

The fix: bring `order_items` to the grain of an **order first**, then join — one row meets one
row, nothing is repeated:

```sql
-- RIGHT: aggregate order_items to one row per order, then join
SELECT o.channel,
       SUM(p.amount) AS paid,
       SUM(i.items)  AS items
FROM orders o
JOIN payments p ON p.order_id = o.order_id
JOIN (SELECT order_id, SUM(quantity) AS items
      FROM order_items
      GROUP BY order_id) i ON i.order_id = o.order_id
GROUP BY o.channel
ORDER BY o.channel;
```

- **`JOIN (SELECT … GROUP BY order_id) i`** — a **subquery** in `FROM`: its result is used like a
  table, named `i`. It has one row per order, the same grain as `payments`.

Example output:

| channel | paid | items |
|---|---|---|
| app | 19812128.58 | 75882 |
| marketplace | 10327858.62 | 39610 |
| web | 30099564.22 | 115788 |
| wholesale | 9981281.32 | 38729 |

Same `items`, and now `paid` matches `payments` on its own. **Check:** whenever a sum looks
"a bit high", count the rows per key on each side of the join.

### 2 · `COUNT(*)` after a join counts lines

```sql
SELECT COUNT(*)                   AS rows_,
       COUNT(DISTINCT o.order_id) AS orders
FROM orders o
JOIN order_items oi ON oi.order_id = o.order_id;
```

Example output:

| rows_ | orders |
|---|---|
| 100000 | 40000 |

After the join there's one row per **line**. `COUNT(*)` counts rows; to count orders, count each
order **once** with `COUNT(DISTINCT o.order_id)`.

### 3 · An inner join drops rows without a match
*How many customers never ordered?* A plain `JOIN` can't answer it — customers without orders
aren't in its result. A **`LEFT JOIN`** keeps them, with `NULL` on the right side:

```sql
SELECT COUNT(*)                                    AS customers,
       count_if(COALESCE(o.orders, 0) = 0)         AS never_ordered
FROM customers c
LEFT JOIN (SELECT customer_id, COUNT(*) AS orders
           FROM orders
           GROUP BY customer_id) o
       ON o.customer_id = c.customer_id;
```

- **`COALESCE(o.orders, 0)`** — `0` when there's no match (`o.orders` is `NULL`).
- **`count_if(condition)`** — counts the rows where the condition is true; the same as
  `SUM(CASE WHEN … THEN 1 ELSE 0 END)`.

Example output:

| customers | never_ordered |
|---|---|
| 6000 | 3515 |

With an inner `JOIN` instead, `customers` would be `2485` — more than half the customers gone,
without an error.

### 4 · `WHERE` on a left join turns it into an inner join
*Every customer, with their cancelled orders if they have any.* Two ways to write it look
almost the same:

```sql
-- WRONG: the filter is in WHERE
SELECT COUNT(DISTINCT c.customer_id) AS customers
FROM customers c
LEFT JOIN orders o ON o.customer_id = c.customer_id
WHERE o.status = 'cancelled';

-- RIGHT: the filter is part of the join
SELECT COUNT(DISTINCT c.customer_id) AS customers
FROM customers c
LEFT JOIN orders o ON o.customer_id = c.customer_id
                  AND o.status = 'cancelled';
```

Example: `1300` vs `6000`.

- **`WHERE`** runs **after** the join. Customers without a cancelled order have
  `o.status = NULL`, `NULL = 'cancelled'` isn't true, so `WHERE` throws them away — the
  `LEFT JOIN` behaves like an inner join.
- **`ON … AND …`** filters only **which orders join**. Every customer stays; those with no
  cancelled order get `NULL`s.

Rule: a condition on the **right** table of a `LEFT JOIN` goes in **`ON`**, not `WHERE`.

### 5 · `NOT IN` and a `NULL`

```sql
SELECT 'found' AS result
WHERE 1 NOT IN (2, 3, NULL);
```

No rows. "1 is not 2, not 3, and not *unknown*" — SQL can't be sure 1 isn't the unknown value,
so the answer is unknown, and the row is dropped. If the list in `NOT IN (SELECT …)` contains
**a single `NULL`**, the query returns **nothing**.

Use **`NOT EXISTS`** instead — it ignores `NULL`s:

```sql
SELECT COUNT(*) AS never_ordered
FROM customers c
WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.customer_id);
```

- **`NOT EXISTS (subquery)`** — true when the subquery returns no rows for this customer.
  `SELECT 1` because only "is there a row?" matters, not its columns.

Example output: `3515` — the same as step 3.

### 6 · Join keys of different types
Keys from files (CSV, JSON, an API) often arrive as **text**: `'42'`, not `42`. Comparing a
number with text fails in Trino:

```sql
SELECT COUNT(*)
FROM orders o
JOIN (SELECT '42' AS customer_id) x       -- a text key, like from a CSV
  ON o.customer_id = x.customer_id;
```

An error like `Cannot apply operator: integer = varchar(2)`. **Cast** one side so both have the
same type:

```sql
SELECT COUNT(*)
FROM orders o
JOIN (SELECT '42' AS customer_id) x
  ON o.customer_id = CAST(x.customer_id AS integer);
```

- **`CAST(value AS type)`** — converts a value to another type.

Example output: `83` — customer 42's orders.

Engines differ here: Trino and Postgres **refuse** to compare the types; others (Snowflake,
MySQL) **convert silently** and can fail later or match the wrong rows. Casting yourself works
everywhere.

## Check yourself
1. A dashboard's payment total jumped 3× after someone joined `order_items` to add an item
   count. Why, and what's the fix? *(Fan-out — each payment repeated per line. Aggregate
   `order_items` per order first, then join.)*
2. `LEFT JOIN orders … WHERE o.status = 'shipped'` returns fewer customers than the
   `customers` table has. Why? *(The `WHERE` drops customers without a shipped order — move the
   condition into `ON`.)*
3. `WHERE id NOT IN (SELECT id FROM other)` returns nothing, though you know some ids are
   missing. What's the likely cause? *(`other.id` contains a `NULL`. Use `NOT EXISTS`.)*
