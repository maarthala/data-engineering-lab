# Snapshots: keep history

## Concept
Bronze `sample_orders` holds each order's **current** status. When order 557 goes from
`shipped` to `delivered`, the old value is overwritten. Nobody can say any more *when* it was
delivered, or how many orders were `shipped` last Monday.

A **snapshot** keeps that history. On every run, dbt compares the source with the snapshot
table and, for each row that changed, **closes** the old version and **adds** a new one. It's a
**slowly changing dimension type 2** (SCD2), the pattern you built by hand with `MERGE` in
[Slowly changing dimensions (SCD2)](../recipes/scd2.md), declared in a few lines of YAML.

Each version of a row gets four extra columns:

| Column | Meaning |
|---|---|
| **`dbt_valid_from`** | when this version became true |
| **`dbt_valid_to`** | when it stopped being true; **NULL** for the current version |
| `dbt_updated_at` | the time dbt used to date the change |
| `dbt_scd_id` | a unique id for the version (key + time, hashed) |

How does dbt decide a row **changed**? You pick a **strategy**:

| Strategy | A row changed when… | Needs |
|---|---|---|
| **`check`** | one of the columns you list has a new value | `check_cols` |
| **`timestamp`** | its updated-at column is newer than last time | a reliable `updated_at` column |

`sample_orders` has no updated-at column, so this lesson uses **`check`** on `status`.

## Lab
Continue in `~/work/my_files/dbt/shop`.

### 1 · Declare the snapshot
Create a folder **`snapshots/`** in the project (next to `models/`) and a file
**`snapshots/orders_snapshot.yml`**:

```yaml
snapshots:
  - name: orders_snapshot
    relation: source('bronze', 'sample_orders')
    config:
      schema: snapshots
      file_format: iceberg
      unique_key: order_id
      strategy: check
      check_cols: [status]
```

- **`relation`**: what to take snapshots of: the source itself. Snapshot the **raw** data, not
  a model: history you didn't capture can't be rebuilt later.
- **`schema: snapshots`**: the snapshot table lands in its own schema, `snapshots.orders_snapshot`.
- **`file_format: iceberg`**: as for models (folder defaults in `dbt_project.yml` only apply to
  `models/`).
- **`unique_key: order_id`**: which row is "the same order" from one run to the next.
- **`strategy: check`** + **`check_cols: [status]`**: a new version whenever `status` changes.

### 2 · First snapshot

```bash
dbt snapshot
```
```text
16:07:54  Found 5 models, 6 data tests, 1 seed, 1 snapshot, 1 source, 633 macros
16:07:55  1 of 1 START snapshot snapshots.orders_snapshot ................................ [RUN]
16:07:57  1 of 1 OK snapshotted snapshots.orders_snapshot ................................ [OK in 1.12s]
16:07:57  Finished running 1 snapshot in 0 hours 0 minutes and 2.59 seconds (2.59s).
16:07:57  Completed successfully
16:07:57  Done. PASS=1 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=1
```

The first run copies every order, all valid from now. In a notebook:

```sql
%%sql
SELECT order_id, status, dbt_valid_from, dbt_valid_to FROM my.snapshots.orders_snapshot
WHERE order_id = 557
```

| | order_id | status | dbt_valid_from | dbt_valid_to |
|---|---|---|---|---|
| 0 | 557 | shipped | 2026-10-13 16:07:56.227221 | NaT |

**`NaT`** ("not a time") is how pandas shows a **NULL** timestamp: this version is current.

### 3 · The order changes, snapshot again
Order 557 is delivered:

```sql
%%sql
UPDATE my.bronze.sample_orders SET status = 'delivered' WHERE order_id = 557
```

```bash
dbt snapshot
```
```text
16:09:09  1 of 1 OK snapshotted snapshots.orders_snapshot ................................ [OK in 4.70s]
16:09:10  Done. PASS=1 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=1
```

```sql
%%sql
SELECT order_id, status, dbt_valid_from, dbt_valid_to FROM my.snapshots.orders_snapshot
WHERE order_id = 557 ORDER BY dbt_valid_from
```

| | order_id | status | dbt_valid_from | dbt_valid_to |
|---|---|---|---|---|
| 0 | 557 | shipped | 2026-10-13 16:07:56.227221 | 2026-10-13 16:09:06.357078 |
| 1 | 557 | delivered | 2026-10-13 16:09:06.357078 | NaT |

The `shipped` version is **closed**: its `dbt_valid_to` is the moment dbt saw the change. The
`delivered` version starts at that same moment and is current. The other 999 orders didn't
change, so they still have one version each:

```sql
%%sql
SELECT count(*) AS rows, count(dbt_valid_to) AS closed FROM my.snapshots.orders_snapshot
```

| | rows | closed |
|---|---|---|
| 0 | 1001 | 1 |

(**`count(column)`** counts only the non-NULL values, so `count(dbt_valid_to)` = closed versions.)

### 4 · Put your starter table back

```sql
%%sql
UPDATE my.bronze.sample_orders SET status = 'shipped' WHERE order_id = 557
```

and run `dbt snapshot` once more. The snapshot doesn't "undo"; it records a **third** version:

| | order_id | status | dbt_valid_from | dbt_valid_to |
|---|---|---|---|---|
| 0 | 557 | shipped | 2026-10-13 16:07:56.227221 | 2026-10-13 16:09:06.357078 |
| 1 | 557 | delivered | 2026-10-13 16:09:06.357078 | 2026-10-13 16:09:46.297421 |
| 2 | 557 | shipped | 2026-10-13 16:09:46.297421 | NaT |

History only grows. That's the point.

### 5 · Questions only a snapshot can answer
**Current state**: the versions with no end:

```sql
%%sql
SELECT order_id, status FROM my.snapshots.orders_snapshot
WHERE dbt_valid_to IS NULL AND order_id IN (557, 998) ORDER BY order_id
```

| | order_id | status |
|---|---|---|
| 0 | 557 | shipped |
| 1 | 998 | delivered |

**State at a moment in the past** ("as of"): the version that had started by then and hadn't
ended yet:

```sql
SELECT order_id, status FROM my.snapshots.orders_snapshot
WHERE dbt_valid_from <= TIMESTAMP '2026-10-13 16:09:30'
  AND (dbt_valid_to > TIMESTAMP '2026-10-13 16:09:30' OR dbt_valid_to IS NULL)
```

For order 557 that's the `delivered` version: delivered at 16:09:06, back to shipped at 16:09:46.
Use your own times from step 3.

!!! info "A snapshot only sees what it's shown"
    dbt compares the source **when the snapshot runs**. A status that changes twice between two
    runs only shows the last value. Run snapshots on a schedule (Airflow, later in the unit), as
    often as you need the detail. **`dbt build`** runs snapshots too, together with your models
    and tests.

## 🎯 This runs unchanged on Azure, Databricks, Snowflake & Fabric
Snapshots are core dbt, with the same YAML and the same four columns on every adapter. The
platforms also keep **table history** of their own (Delta and Iceberg *time travel*,
Snowflake *Time Travel*), but that's short-term and per table: snapshots keep business history
for as long as you like.

## You can now…
- Explain what a **snapshot** keeps, and why it's SCD type 2
- Declare a snapshot in YAML with `relation`, `unique_key` and a **strategy**
- Choose **`check`** (with `check_cols`) or **`timestamp`** (with an updated-at column)
- Read `dbt_valid_from` / `dbt_valid_to`, and query the **current** state and the state **as of** a time
