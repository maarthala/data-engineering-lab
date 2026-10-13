# Slowly changing dimensions (SCD Type 2)

## Concept
A **dimension** table (from Silver/Gold) is a clean, deduplicated list of business entities —
customers, products, stores. But the real world *changes*: a customer moves country, a product
gets recategorised. The question is what you do with the change. There are two classic answers,
and the difference is entirely about **history**:

- **SCD Type 1 — overwrite.** You just update the row in place. The new value replaces the old
  one and the previous value is **gone forever**. This is exactly the plain `UPDATE` /
  `WHEN MATCHED THEN UPDATE` you already met in [MERGE & upserts](../unit2/merge.md). Simple, but
  the table can only ever answer "what is true *now*".

- **SCD Type 2 — keep history.** Instead of overwriting, you **close** the old version and
  **add** a new one, so the table remembers *every* value the attribute ever had, and *when* each
  was valid. Now you can ask "which country was this customer in **last March**?" — the kind of
  point-in-time question that finance, compliance, and analytics constantly need.

Type 2 buys that time-travel with three **tracking columns** on every row:

| Column | Meaning |
|---|---|
| **`valid_from`** | the date this version of the row started being true |
| **`valid_to`** | the date it stopped (`null` = still true today) |
| **`is_current`** | a convenience flag: `true` on exactly the live version |

So one customer can have **several rows** — one per era of their life — with only the newest one
flagged `is_current = true` and left "open" (`valid_to = null`).

!!! info "Why this needs a lakehouse table"
    Type 2 does an `UPDATE` (to close the old row) and an `INSERT` (to open the new one). Both
    need the **ACID table format** from [Unit 1](../unit1/formats.md) — a half-finished update
    must never leave the table in a broken state. So this recipe writes to an **Iceberg** table
    in the `iceberg` catalog, the same governed lakehouse format you build Silver on in
    [Transform to Silver](../unit4/transform-silver.md).

### Why it takes *two* statements
Here's the subtlety that trips people up. You might reach for a single `MERGE`, but Type 2 needs
to do **two things to the same customer** in one batch: *close* their old row **and** *open* a new
one. A `MERGE`'s `WHEN MATCHED` branch can only touch **one target row per source row** — it can
update the old version *or* insert the new version, but not both for the same key. So we split it:

1. **A `MERGE`** that only **closes** the current row of any customer whose attributes changed
   (stamps `valid_to`, flips `is_current` to `false`).
2. **An `INSERT`** that **opens** a fresh current row — both for the customers we just closed
   *and* for brand-new customers who never had a row at all.

That's the whole pattern. Let's build it.

## Lab
> Run these cells in a **Jupyter/Spark notebook** against the `iceberg` catalog. This lab
> **writes**, so it uses a throwaway `recipes` schema you own — the real ShopFlow data is never
> touched.

### 1 · The dimension table + initial load

First we create the dimension with its three tracking columns and load two customers as they
stand on day one:

```python
spark.sql("CREATE SCHEMA IF NOT EXISTS iceberg.recipes")
spark.sql("DROP TABLE IF EXISTS iceberg.recipes.dim_customer")
spark.sql("""
CREATE TABLE iceberg.recipes.dim_customer (
  customer_id int, full_name string, country string,
  valid_from date, valid_to date, is_current boolean)
""")

spark.sql("""
INSERT INTO iceberg.recipes.dim_customer VALUES
  (1,'Ada','UK', date'2024-01-01', null, true),
  (2,'Ravi','IN', date'2024-01-01', null, true)
""")
```

**Read it step by step:**

- **`CREATE SCHEMA IF NOT EXISTS iceberg.recipes`** — a **schema** is a folder for tables. This
  makes an empty `recipes` schema in the `iceberg` catalog. `IF NOT EXISTS` means "skip if it's
  already there", so the cell is safe to re-run.
- **`DROP TABLE IF EXISTS …dim_customer`** — delete any old copy first so you always start from a
  clean, known state. `IF EXISTS` avoids an error on the very first run.
- **`CREATE TABLE …dim_customer ( … )`** — define the empty dimension. Alongside the business
  columns (`customer_id`, `full_name`, `country`) come the **three Type 2 tracking columns**:
  `valid_from` / `valid_to` (dates) and `is_current` (a boolean). The `dim_` prefix marks this as
  a **dimension** table, exactly the kind you produce in Silver/Gold.
- **`INSERT INTO …dim_customer VALUES ( … ), ( … )`** — hand-write the day-one rows. Note the
  shape of a **current** row: `valid_from` is set to when it started, **`valid_to` is `null`**
  (still true), and **`is_current` is `true`**. `date'2024-01-01'` is how you write a date
  literal. Both Ada (UK) and Ravi (IN) start out as open, current rows.

### 2 · Today's batch + the two-step SCD2 upsert

Now a **daily batch** arrives. It carries two rows: one is a *change* to a customer we already
have (Ravi moved from IN to SG), the other is a *brand-new* customer (Mei). We apply the Type 2
pattern — **close, then open**:

```python
# today's batch: customer 2 moved to SG (a CHANGE), customer 3 is BRAND NEW
spark.sql("""
CREATE OR REPLACE TEMPORARY VIEW staged AS
SELECT * FROM VALUES (2,'Ravi','SG'), (3,'Mei','SG') AS s(customer_id, full_name, country)
""")

# step 1 — CLOSE the current row of any customer whose attributes changed
spark.sql("""
MERGE INTO iceberg.recipes.dim_customer t
USING staged s ON t.customer_id = s.customer_id AND t.is_current = true
WHEN MATCHED AND (t.country <> s.country OR t.full_name <> s.full_name)
     THEN UPDATE SET valid_to = current_date(), is_current = false
""")

# step 2 — INSERT a new current row for changed customers AND brand-new ones
spark.sql("""
INSERT INTO iceberg.recipes.dim_customer
SELECT s.customer_id, s.full_name, s.country,
       current_date() AS valid_from, cast(null AS date) AS valid_to, true AS is_current
FROM staged s
LEFT JOIN iceberg.recipes.dim_customer c
       ON c.customer_id = s.customer_id AND c.is_current = true
WHERE c.customer_id IS NULL          -- brand-new customer
   OR c.country   <> s.country       -- or an attribute changed
   OR c.full_name <> s.full_name
""")

spark.sql("""SELECT customer_id, country, valid_from, valid_to, is_current
             FROM iceberg.recipes.dim_customer ORDER BY customer_id, valid_from""").show()
```

**Read it step by step — the staged batch:**

- **`CREATE OR REPLACE TEMPORARY VIEW staged AS SELECT * FROM VALUES … AS s(…)`** — register
  today's incoming rows as a named **temporary view** so the SQL below can refer to it as
  `staged`. `VALUES (2,'Ravi','SG'), (3,'Mei','SG')` hand-writes the batch, and `AS s(customer_id,
  full_name, country)` names its columns. A temp view lives only in this Spark session — nothing
  is written to disk. This is a stand-in for "today's file of changes".

**Read it step by step — step 1, CLOSE the old version:**

- **`MERGE INTO iceberg.recipes.dim_customer t USING staged s`** — the upsert from
  [MERGE & upserts](../unit2/merge.md): compare the **target** dimension `t` against the
  **source** batch `s`.
- **`ON t.customer_id = s.customer_id AND t.is_current = true`** — the match rule. We only pair a
  batch row against the customer's **current** row (`is_current = true`) — we never want to reopen
  a row we already closed in a past run.
- **`WHEN MATCHED AND (t.country <> s.country OR t.full_name <> s.full_name)`** — a **conditional
  match**: only act when an attribute *actually changed* (`<>` = "not equal"). If the batch
  repeats a customer with identical values, nothing fires — the pattern stays **idempotent**.
- **`THEN UPDATE SET valid_to = current_date(), is_current = false`** — this is the **close**.
  We stamp the row's `valid_to` with today's date and flip `is_current` to `false`, sealing that
  era of the customer's history. We do **not** touch the business columns — the old row keeps its
  old country, forever, as the historical record.
- **After step 1:** Ravi's IN row is now closed (`valid_to` set, `is_current = false`). Ada is
  unchanged (not in the batch). Mei doesn't match anything yet — a MERGE alone can't add her, which
  is exactly why we need step 2.

**Read it step by step — step 2, OPEN the new version:**

- **`INSERT INTO iceberg.recipes.dim_customer SELECT …`** — insert one fresh row per qualifying
  batch record, with the tell-tale shape of a **current** row: `valid_from = current_date()`,
  **`valid_to = null`** (cast to `date`), and **`is_current = true`**.
- **`LEFT JOIN iceberg.recipes.dim_customer c ON c.customer_id = s.customer_id AND c.is_current
  = true`** — join each batch row to the customer's current row *if one exists*. A **left** join
  keeps every batch row even when there's **no** matching current row — in that case all the `c.`
  columns come back `null`. That "no match" is how we detect a brand-new customer.
- **`WHERE c.customer_id IS NULL OR c.country <> s.country OR c.full_name <> s.full_name`** — the
  filter that picks **exactly** the rows that deserve a new current version:
    - **`c.customer_id IS NULL`** — the left join found no current row → this is a **brand-new**
      customer (Mei). Insert her first-ever row.
    - **`c.country <> s.country OR c.full_name <> s.full_name`** — a current row exists but an
      attribute **changed** (Ravi: IN → SG). Insert the new era. Note this is the *same* change
      test as step 1's guard, so the two statements stay in lock-step: whoever we closed, we now
      open; plus the brand-new folks.
    - Customers whose current row is **unchanged** match neither clause, so they get **no** new
      row — no duplicates.
- **The final `SELECT … ORDER BY customer_id, valid_from`** just reads the table back so you can
  eyeball the result, sorted so each customer's history reads top-to-bottom.

**The verified result:**

```
+-----------+-------+----------+----------+----------+
|customer_id|country|valid_from|valid_to  |is_current|
+-----------+-------+----------+----------+----------+
|          1|UK     |2024-01-01|null      |true      |
|          2|IN     |2024-01-01|2024-09-07|false     |
|          2|SG     |2024-09-07|null      |true      |
|          3|SG     |2024-09-07|null      |true      |
+-----------+-------+----------+----------+----------+
```

Read the story in the rows:

- **Customer 1 (Ada)** wasn't in the batch, so she's **untouched** — one open, current UK row.
- **Customer 2 (Ravi)** now has **two rows**: the old **IN** row is **closed** (`valid_to` stamped
  with today, `is_current = false`), and a new **SG** row is **open** (`valid_to = null`,
  `is_current = true`). That's Type 2 keeping history — you can still see he *used to* be in IN.
- **Customer 3 (Mei)** is **brand new**: a single open, current SG row.

(`valid_to` and `valid_from` show `2024-09-07` because that's `current_date()` on the day the lab
was run — yours will show *your* run date.)

!!! tip "SCD2 pipelines are idempotent"
    Re-run the whole batch and nothing changes: step 1's guard finds no *newly* changed current
    rows, and step 2's `WHERE` finds nothing new or changed. Like `MERGE`, this **idempotency**
    (from [1.1](../unit1/what-is-de.md)) is what lets you safely re-run a failed job or backfill a
    day without polluting the dimension with duplicate versions.

## Challenge
Our change test compares just two columns by hand (`country`, `full_name`). That gets unwieldy
fast. **Option A:** rewrite the change detection to compare a **hash** of all business columns in
one shot (so adding a tracked column is a one-line change). **Option B:** handle a **delete** —
when a customer disappears from the source, *close* their current row (stamp `valid_to`,
`is_current = false`) but keep the history, rather than adding a new version.

!!! tip "Which piece changes?"
    For the hash, replace both the `WHEN MATCHED AND (…)` guard and the `WHERE … <> …` clause with
    a single comparison of `md5(concat_ws('|', …))` over the source vs. the current row. For the
    delete, you need a second `MERGE` (or a `WHEN NOT MATCHED BY SOURCE` branch, where supported)
    that closes current rows whose `customer_id` is absent from the batch.

??? note "Solution"
    **Option A — hash-based change detection.** Compute one fingerprint per side and compare it,
    so any tracked column that differs trips the change:

    ```python
    # step 1 — close changed rows, detected by a hash of ALL business columns
    spark.sql("""
    MERGE INTO iceberg.recipes.dim_customer t
    USING staged s ON t.customer_id = s.customer_id AND t.is_current = true
    WHEN MATCHED AND md5(concat_ws('|', t.full_name, t.country))
                  <> md5(concat_ws('|', s.full_name, s.country))
         THEN UPDATE SET valid_to = current_date(), is_current = false
    """)

    # step 2 — open new rows for brand-new OR hash-changed customers
    spark.sql("""
    INSERT INTO iceberg.recipes.dim_customer
    SELECT s.customer_id, s.full_name, s.country,
           current_date(), cast(null AS date), true
    FROM staged s
    LEFT JOIN iceberg.recipes.dim_customer c
           ON c.customer_id = s.customer_id AND c.is_current = true
    WHERE c.customer_id IS NULL
       OR md5(concat_ws('|', c.full_name, c.country))
       <> md5(concat_ws('|', s.full_name, s.country))
    """)
    ```

    **`concat_ws('|', …)`** glues the business columns into one string with a `|` separator (the
    separator stops `'A','BC'` colliding with `'AB','C'`), and **`md5(...)`** hashes it. Two rows
    with the same hash are identical across *every* listed column, so adding a tracked column means
    editing just the two `concat_ws` lists — not a growing pile of `OR … <> …` conditions.

    **Option B — handle deletes.** After the close/open steps, close anyone who vanished from the
    batch:

    ```python
    spark.sql("""
    MERGE INTO iceberg.recipes.dim_customer t
    USING staged s ON t.customer_id = s.customer_id AND t.is_current = true
    WHEN NOT MATCHED BY SOURCE AND t.is_current = true
         THEN UPDATE SET valid_to = current_date(), is_current = false
    """)
    ```

    **`WHEN NOT MATCHED BY SOURCE`** fires for current target rows that have **no** row in the
    batch — a soft delete: we stamp `valid_to` and clear `is_current`, but the history stays. (If
    your engine lacks that clause, do the same with a `MERGE`/`UPDATE` whose source is the set of
    `customer_id`s *missing* from `staged`.)

!!! tip "🎯 SCD Type 2 on Databricks, Snowflake & Fabric"
    **What you just did:** kept full history in a dimension by *closing* old versions and *opening*
    new ones — the standard warehouse technique, portable everywhere.

    - **Azure Databricks** — **Delta Live Tables** does this *declaratively*: a single
      `APPLY CHANGES INTO … STORED AS SCD TYPE 2` statement manages the close/open and the
      `valid_from`/`valid_to` bookkeeping for you. The manual two-step `MERGE` + `INSERT` here is
      exactly what it generates under the hood.
    - **Snowflake** — drive the incremental version with **Streams + Tasks**: a stream captures
      changed rows, a scheduled task runs the close-and-open `MERGE` pattern.
    - **Microsoft Fabric** — the identical Spark `MERGE` + `INSERT` against a Lakehouse table, or a
      **Dataflow Gen2** with a built-in SCD Type 2 pattern.

    The tooling differs, but the shape — close the old version, open the new one, keep the
    tracking columns — is the same on every platform.

## Key terms, at a glance
| Term | Plain meaning |
|---|---|
| **Dimension table** | A clean, deduplicated list of business entities (customers, products) |
| **SCD Type 1** | Overwrite the attribute in place — no history kept |
| **SCD Type 2** | Keep history: close the old version, open a new one |
| **`valid_from` / `valid_to`** | The date range a version of a row was true (`valid_to = null` = still true) |
| **`is_current`** | Boolean flag marking the one live version of a row |
| **Close a row** | Stamp `valid_to` and set `is_current = false` on the old version |
| **Open a row** | Insert a new version with `valid_from = today`, `valid_to = null`, `is_current = true` |
| **Why two statements** | A `MERGE` touches one target row per source; you close *then* insert |
| **`LEFT JOIN … WHERE c.… IS NULL`** | Detects brand-new keys (no current row matched) |
| **`<>` change test** | Fire only when an attribute actually changed (idempotent) |
| **`concat_ws` + `md5`** | Hash many columns into one fingerprint to detect any change |
| **Point-in-time query** | Ask what an attribute was on a past date, using the validity range |

## You can now…
- Explain SCD Type 1 (overwrite) vs Type 2 (keep history) and when each is right
- Track history with `valid_from` / `valid_to` / `is_current` on a dimension table
- Apply a Type 2 batch as a two-step **close-then-open** (`MERGE` to close, `INSERT` to open)
- Pick brand-new *and* changed rows with a `LEFT JOIN … WHERE key IS NULL OR changed` filter
- Scale change detection to many columns with a `concat_ws` + `md5` hash, and handle deletes
