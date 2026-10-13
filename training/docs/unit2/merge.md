# Changing data: MERGE & upserts

## Concept
Everything so far only **read** data. But data engineering is mostly about **changing** a
target table as new data arrives: apply updates, add new rows, remove deleted ones — without
duplicating anything. The workhorse for this is **`MERGE`** (a.k.a. **upsert** =
*update-or-insert*):

```
MERGE INTO target  USING source  ON <match>
  WHEN MATCHED       THEN UPDATE / DELETE
  WHEN NOT MATCHED   THEN INSERT
```

In one atomic statement it decides, row by row, whether each incoming record is an **update**
to an existing row or a **brand-new** row. This is exactly how you'll build the **Silver**
layer in [Unit 4](../unit4/transform-silver.md): merge each day's changed and late-arriving
orders into the clean table.

### How MERGE actually works

Read the shape above as an English sentence. There are two tables and a rule:

- **`MERGE INTO target`** — the table you want to *change* (rows will be updated, inserted, or
  deleted here). This is the **target**.
- **`USING source`** — the table (or batch of new rows) you're bringing *in*. This is the
  **source** — think "today's incoming data".
- **`ON <match>`** — the rule that decides, for each source row, whether a matching row already
  exists in the target. It's a **join key**, just like the `ON` you met in
  [joins](joins-aggregations.md) — usually a primary key like `ON t.customer_id = s.customer_id`.

Once the `ON` rule pairs things up, every source row falls into one of two buckets, and you say
what to do with each:

- **`WHEN MATCHED THEN …`** — the key **already exists** in the target. You typically `UPDATE`
  that row with the fresh values (or `DELETE` it — more on that later).
- **`WHEN NOT MATCHED THEN …`** — the key is **new**. You `INSERT` it as a brand-new row.

That two-branch behaviour is the whole point of an **upsert** (*update-or-insert*): one command
that adds the new rows *and* refreshes the changed ones, without you having to figure out in
advance which is which — and crucially, **without creating duplicates**. Doing this by hand
(a separate `INSERT` for new rows plus an `UPDATE` for changed ones) is fiddly and easy to get
wrong; `MERGE` does it in a single, all-or-nothing statement.

!!! note "Why we need a lakehouse table for this"
    You can't `MERGE` a folder of files, and you shouldn't write to the live `shopflow`
    source (it's read-only for us). `MERGE`, `UPDATE`, and `DELETE` need the **ACID table
    format** from [1.3](../unit1/formats.md). So this lab writes to a scratch **Iceberg**
    table in the `iceberg` catalog — a real lakehouse table you can safely experiment on.

!!! info "What ACID buys you here"
    **ACID** (Atomic, Consistent, Isolated, Durable) is the guarantee that a change either
    lands **completely or not at all**, even if the query touches thousands of rows or crashes
    halfway. A plain pile of Parquet/CSV files has no such guarantee — a half-finished `MERGE`
    could leave the table corrupted. A lakehouse **table format** like **Iceberg** (or Delta on
    Databricks) adds a transaction log on top of the files, which is what makes
    `MERGE` / `UPDATE` / `DELETE` safe and atomic. No ACID table → no `MERGE`.

## Lab
> Run these in **SQLPad** on the **Lakehouse (Trino) — &lt;your username&gt;** connection (not *ShopFlow — OLTP*) (see [2.1](intro.md)) — this lab **writes**
> (`CREATE`/`INSERT`/`MERGE`), and **Superset SQL Lab is read-only**, so it can't run it. The
> writes go to **your own** catalog `demouser_lake` (fully qualified), not `shopflow`.

!!! note "Signed in with your own account?"
    The examples use the default lab account **`demouser`** — its catalog is `demouser_lake`. If you
    registered your own account, replace `demouser` with your username (SQLPad: find & replace,
    `Ctrl/Cmd + H`), e.g. `ravi_lake`.


### 1 · Set up a scratch table

Before we can change anything, we need a table we *own* and can safely wreck. We build it in a
throwaway **scratch (sandbox) schema** so the real ShopFlow data is never at risk (your own
sandbox — the source stays untouched):

```sql
CREATE SCHEMA IF NOT EXISTS demouser_lake.sandbox;

DROP TABLE IF EXISTS demouser_lake.sandbox.dim_customer;
CREATE TABLE demouser_lake.sandbox.dim_customer (
  customer_id int,
  full_name   varchar,
  country     varchar,
  updated_at  date
);

INSERT INTO demouser_lake.sandbox.dim_customer VALUES
  (1, 'Ada Lovelace', 'UK', DATE '2024-01-01'),
  (2, 'Alan Turing',  'UK', DATE '2024-01-01');
```

**Read it clause by clause:**

- **`CREATE SCHEMA IF NOT EXISTS demouser_lake.sandbox`** — a **schema** is a folder for tables. This
  makes an empty `sandbox` schema inside your own `demouser_lake` catalog. **`IF NOT EXISTS`** means "skip
  it if it's already there" — so re-running the lab never errors out.
- **`DROP TABLE IF EXISTS …dim_customer`** — delete any old copy of the table first, so you always
  start from a clean, known state. (`IF EXISTS` again avoids an error on the first run.)
- **`CREATE TABLE …dim_customer ( … )`** — define a new, empty table and its **columns** with their
  **types**: `customer_id int` (a whole number), `full_name`/`country` as `varchar` (text), and
  `updated_at` as a `date`. The name `dim_customer` hints this is a **dimension** table — a
  clean, deduplicated list of customers, exactly the kind of table you build in Silver/Gold.
- **`INSERT INTO …dim_customer VALUES ( … ), ( … )`** — add rows. **`INSERT … VALUES`** hand-writes
  literal rows; here two customers, both last touched on `2024-01-01`. **`DATE '2024-01-01'`** is
  how you write a date literal.

After this block the table holds two rows: Ada (id 1) and Alan (id 2). That `updated_at` column
is going to matter — it's the timestamp we'll use later to reject stale, out-of-order data.

### 2 · The upsert

Now the star of the lesson. Imagine a **daily batch** arrives with two rows: one is a *change* to
a customer we already have, the other is a *brand-new* customer. A single `MERGE` handles both:

```sql
MERGE INTO demouser_lake.sandbox.dim_customer AS t
USING (VALUES
         (2, 'Alan M. Turing', 'UK', DATE '2024-06-01'),   -- changed name
         (3, 'Grace Hopper',   'US', DATE '2024-06-01')     -- brand new
      ) AS s(customer_id, full_name, country, updated_at)
ON t.customer_id = s.customer_id
WHEN MATCHED     THEN UPDATE SET full_name = s.full_name, updated_at = s.updated_at
WHEN NOT MATCHED THEN INSERT (customer_id, full_name, country, updated_at)
                     VALUES (s.customer_id, s.full_name, s.country, s.updated_at);

SELECT * FROM demouser_lake.sandbox.dim_customer ORDER BY customer_id;
-- id 2 updated, id 3 inserted, id 1 untouched
```

**Read it clause by clause:**

- **`MERGE INTO demouser_lake.sandbox.dim_customer AS t`** — the **target** we're changing, nicknamed
  `t`. (Just like table aliases in [joins](joins-aggregations.md), `AS t` / `AS s` keep the query
  short and let us say which side a column comes from.)
- **`USING (VALUES (…), (…)) AS s(customer_id, full_name, country, updated_at)`** — the **source**.
  Instead of a real table, we hand-write the incoming batch inline with **`VALUES`**, and
  **`AS s(…)`** names its columns so the rest of the query can refer to `s.full_name`,
  `s.customer_id`, and so on. This is a stand-in for "today's file of changes".
- **`ON t.customer_id = s.customer_id`** — the **match rule**. For each source row, does a target
  row with the same `customer_id` already exist?
- **`WHEN MATCHED THEN UPDATE SET full_name = …, updated_at = …`** — the key *did* exist (customer
  2). Overwrite that row's name and timestamp with the incoming values.
- **`WHEN NOT MATCHED THEN INSERT (…) VALUES (…)`** — the key *didn't* exist (customer 3).
  Add it as a brand-new row, mapping each source column into the target.

**Before → after:** id 2 (Alan Turing → **Alan M. Turing**, timestamp bumped to `2024-06-01`)
is **updated**; id 3 (Grace Hopper) is **inserted**; id 1 (Ada) isn't in the batch at all, so it
is **left completely untouched**. That's the upsert doing update *and* insert in one shot.

### 3 · Conditional match — only update if the incoming row is *newer*

Real event streams deliver data **out of order** (remember the ShopFlow event stream): a
correction from June can arrive *after* you've already applied a fresher July value. If you
blindly overwrote, you'd clobber good data with stale data. The fix is a **conditional match** —
extra `AND` conditions on the `WHEN MATCHED` branch that must *also* be true before the update
fires (`WHEN MATCHED AND …`):

```sql
MERGE INTO demouser_lake.sandbox.dim_customer AS t
USING (VALUES (1, 'Ada L.', DATE '2023-12-01')) AS s(customer_id, full_name, updated_at)
ON t.customer_id = s.customer_id
WHEN MATCHED AND s.updated_at > t.updated_at
     THEN UPDATE SET full_name = s.full_name, updated_at = s.updated_at;
-- no-op: the incoming row is OLDER than what's already there
```

**Read it clause by clause:**

- **`USING (VALUES (1, 'Ada L.', DATE '2023-12-01')) AS s(…)`** — one incoming row for customer 1,
  stamped **December 2023**.
- **`ON t.customer_id = s.customer_id`** — it *matches* Ada (id 1 exists in the target).
- **`WHEN MATCHED AND s.updated_at > t.updated_at`** — the guard. The update only runs if the
  source timestamp is **later** than the one already stored. Ada's stored `updated_at` is
  `2024-01-01`; the incoming row is `2023-12-01`, which is **older**, so `s.updated_at > t.updated_at`
  is false and the branch is **skipped**.

**Before → after:** *nothing changes*. The stale correction is correctly ignored — the table keeps
its newer value. This tiny `AND` is what makes a Silver upsert safe to run on messy, late-arriving
data.

!!! warning "A MERGE with no matching branch is not an error — it's a no-op"
    Because the guard failed, this `MERGE` updates zero rows. That's intentional, not a bug.
    A `MERGE` only changes rows whose branch condition is satisfied; if none qualify, it simply
    leaves the table as-is. Re-running it a hundred times has the same (zero) effect.

### 4 · Delete through MERGE

`MERGE` isn't only for adding and updating — a matched branch can also **`DELETE`** the target row.
A classic use is a **GDPR erasure request**: a batch of customer ids that must be removed from the
table entirely.

```sql
MERGE INTO demouser_lake.sandbox.dim_customer AS t
USING (VALUES (3)) AS s(customer_id)
ON t.customer_id = s.customer_id
WHEN MATCHED THEN DELETE;
```

**Read it clause by clause:**

- **`USING (VALUES (3)) AS s(customer_id)`** — the "delete list": here just customer id 3 (the
  Grace Hopper row we inserted in step 2).
- **`ON t.customer_id = s.customer_id`** — find the matching target row.
- **`WHEN MATCHED THEN DELETE`** — where it matches, **remove the target row** instead of updating it.

**Before → after:** id 3 is **deleted**; ids 1 and 2 are untouched. (There's no `WHEN NOT MATCHED`
branch, so ids in the delete list that *don't* exist are simply ignored — no error.)

### 5 · Plain `UPDATE` / `DELETE`

`MERGE` shines when you're reconciling a whole batch, but for one-off changes the simple
**`UPDATE`** and **`DELETE`** statements also work on a lakehouse table — the same ACID guarantee
applies:

```sql
UPDATE demouser_lake.sandbox.dim_customer SET country = 'United Kingdom' WHERE country = 'UK';
DELETE FROM demouser_lake.sandbox.dim_customer WHERE customer_id = 1;
SELECT * FROM demouser_lake.sandbox.dim_customer ORDER BY customer_id;
```

**Read it clause by clause:**

- **`UPDATE … SET country = 'United Kingdom' WHERE country = 'UK'`** — rewrite the `country` value
  on **every row where the `WHERE` condition holds**. Both remaining rows currently say `'UK'`, so
  both are relabelled to `'United Kingdom'`. Leave off the `WHERE` and it would change *all* rows —
  so always double-check the filter.
- **`DELETE FROM … WHERE customer_id = 1`** — remove the rows matching the `WHERE` (just Ada, id 1).
- The final **`SELECT * … ORDER BY customer_id`** just reads the table back so you can see the
  result. `*` means "all columns".

**Before → after:** id 2's country becomes `United Kingdom`, then id 1 is removed — leaving only
customer 2 in the table.

!!! tip "MERGE makes pipelines *idempotent*"
    Re-run the upsert with the same batch and the result is identical — no duplicates. That
    **idempotency** (from [1.1](../unit1/what-is-de.md)) is what lets you safely re-run a
    failed job or backfill a day without corrupting the table.

## Challenge
Create `demouser_lake.sandbox.orders_silver (order_id int, status varchar, updated_at date)` and seed
it with two delivered orders. Then MERGE a daily batch that (a) updates order 1's status to
`returned`, (b) inserts a new order 3, and (c) tries to update order 2 with an **older**
timestamp (which should be ignored). Verify the final table.

!!! tip "Which clauses do you need?"
    This is a mini Silver upsert. Build the table with `CREATE TABLE` + `INSERT … VALUES`, then a
    single `MERGE` that combines both branches: **`WHEN MATCHED AND s.updated_at > t.updated_at
    THEN UPDATE`** (so the older order-2 row is guarded out) and **`WHEN NOT MATCHED THEN INSERT`**
    (so the new order 3 lands). One statement does all three cases at once.

??? note "Solution"
    ```sql
    DROP TABLE IF EXISTS demouser_lake.sandbox.orders_silver;
    CREATE TABLE demouser_lake.sandbox.orders_silver (order_id int, status varchar, updated_at date);
    INSERT INTO demouser_lake.sandbox.orders_silver VALUES
      (1, 'delivered', DATE '2024-05-01'),
      (2, 'delivered', DATE '2024-05-02');

    MERGE INTO demouser_lake.sandbox.orders_silver AS t
    USING (VALUES
             (1, 'returned',  DATE '2024-05-10'),   -- newer update
             (2, 'cancelled', DATE '2024-04-01'),   -- OLDER → ignore
             (3, 'delivered', DATE '2024-05-11')    -- new
          ) AS s(order_id, status, updated_at)
    ON t.order_id = s.order_id
    WHEN MATCHED AND s.updated_at > t.updated_at
         THEN UPDATE SET status = s.status, updated_at = s.updated_at
    WHEN NOT MATCHED
         THEN INSERT (order_id, status, updated_at) VALUES (s.order_id, s.status, s.updated_at);

    SELECT * FROM demouser_lake.sandbox.orders_silver ORDER BY order_id;
    -- 1 → returned, 2 → still delivered (older ignored), 3 → inserted
    ```

!!! tip "🎯 This runs unchanged on Azure, Databricks, Snowflake & Fabric"
    **What you just did:** upserted a table with `MERGE` (`WHEN MATCHED` / `WHEN NOT MATCHED`),
    conditional updates, and `DELETE` — the core of building Silver.

    - **Azure Databricks** — `MERGE INTO` on a Delta table is near-identical (this is *the*
      Databricks Silver-build pattern; `WHEN NOT MATCHED BY SOURCE` adds deletes).
    - **Snowflake** — the same `MERGE INTO … WHEN MATCHED / WHEN NOT MATCHED` on a table.
    - **Microsoft Fabric** — `MERGE` in the Warehouse (T-SQL); Spark `MERGE INTO` in a Lakehouse.
    - **Azure Data Factory** — the no-code analog is the **Alter Row** transformation
      (upsert/update/delete policies) writing to a sink.

    `MERGE` is one of the highest-value DE skills — and it's essentially portable everywhere.

## Key terms, at a glance
| Term | Plain meaning |
|---|---|
| **Upsert** | Insert new rows, update existing ones — in one step |
| **`MERGE`** | The SQL statement that performs an upsert (update-or-insert) |
| **`MERGE INTO … USING … ON`** | target you change, source you bring in, and the key that matches them |
| **Target / Source** | The table being changed / the incoming batch of rows |
| **`WHEN MATCHED`** | Branch that runs when the key already exists (UPDATE/DELETE) |
| **`WHEN NOT MATCHED`** | Branch that runs for brand-new keys (INSERT) |
| **Conditional match** | `WHEN MATCHED AND <cond>` — e.g. only update if newer |
| **`USING (VALUES …) AS s(…)`** | Hand-written inline source rows with named columns |
| **`CREATE TABLE` / `INSERT … VALUES`** | Define an empty table / add literal rows to it |
| **ACID** | Atomic/Consistent/Isolated/Durable — a change lands fully or not at all |
| **Idempotent** | Re-running gives the same result (no duplicates) |
| **Late-arriving data** | Records that show up out of order — guard with a condition |
| **Scratch (sandbox) table** | A throwaway table you own and can safely experiment on |

## You can now…
- Upsert a table with `MERGE` using `WHEN MATCHED` / `WHEN NOT MATCHED`
- Guard updates against older/late data with `WHEN MATCHED AND …`
- Delete through MERGE, and run plain `UPDATE` / `DELETE` on lakehouse tables
- Explain why MERGE needs a table format and how it makes pipelines idempotent
