# Read the changes in the lake

## Concept
Your job wrote four Iceberg tables. They look like the shop's tables, plus a few columns OLake
adds. Those columns tell you **what happened** to each row and **when**. How the job writes
them matters for every query you'll build on top:

- **One row per key.** In *Full load + CDC* mode OLake **upserts**: when order 34 changes, its
  row in the lake is replaced. The lake holds each order's **latest** version, not every
  version.
- **Deletes stay as markers.** When order 524 is cancelled, its row doesn't disappear. It stays
  with `_op_type = 'd'` and its other columns empty. Such a marker is called a
  **tombstone** (or a *soft delete*).

So the lake's tables are "the shop as it is now, plus tombstones".

## Lab
In a notebook with the `spark` session and the `my` catalog
([Spark from a notebook](../unit3/spark.md)). Your counts and ids will differ: the shop is live.

### 1 · What landed
```python
spark.sql("SHOW TABLES IN my.shop_cdc").show()
spark.sql("DESCRIBE my.shop_cdc.orders").show(truncate=False)
```

```
+---------+-----------+-----------+
|namespace|tableName  |isTemporary|
+---------+-----------+-----------+
|shop_cdc |customers  |false      |
|shop_cdc |order_items|false      |
|shop_cdc |orders     |false      |
|shop_cdc |products   |false      |
+---------+-----------+-----------+

+----------------+---------+-------+
|col_name        |data_type|comment|
+----------------+---------+-------+
|_olake_id       |string   |NULL   |
|_op_type        |string   |NULL   |
|customer_id     |int      |NULL   |
|_olake_timestamp|timestamp|NULL   |
|status          |string   |NULL   |
|updated_at      |timestamp|NULL   |
|total           |double   |NULL   |
|_cdc_timestamp  |timestamp|NULL   |
|order_id        |int      |NULL   |
|_cdc_lsn        |string   |NULL   |
|created_at      |timestamp|NULL   |
+----------------+---------+-------+
```

The shop's six columns are there (in a different order: column order doesn't matter in SQL,
you name the columns you want). OLake adds five:

| Column | What it holds |
|---|---|
| `_olake_id` | the row's key, built from the source's primary key (`order_id` here; `order_id` + `line` for `order_items`) |
| `_op_type` | what happened last: `r` snapshot read · `c` insert · `u` update · `d` delete |
| `_cdc_timestamp` | when the change happened in the source. Snapshot rows show `1970-01-01`: there was no change, only a read. |
| `_cdc_lsn` | the change's position in Postgres's log (*log sequence number*). Empty for snapshot rows. |
| `_olake_timestamp` | when OLake wrote the row into the lake |

!!! note "With Debezium: one `_cdc` column instead of five"
    A Debezium job's table has the shop's columns with their **own types** (`total` stays
    `decimal(12,2)`, `created_at` a `timestamp with time zone`) plus a single column `_cdc`, a
    *row* (a struct) of what happened. In the SQL editor:

    ```sql
    SHOW COLUMNS FROM <you>_lake.shop_cdc.orders;
    ```

    ```text
     Column      | Type
    -------------+------------------------------------------------------------------------------------------------------
     order_id    | integer
     customer_id | integer
     status      | varchar
     total       | decimal(12,2)
     created_at  | timestamp(6) with time zone
     updated_at  | timestamp(6) with time zone
     _cdc        | row("op" varchar, "ts" timestamp(6) with time zone, "offset" bigint, "source" varchar, "target" varchar, "key" row("order_id" integer))
    ```

    | Field | What it holds |
    |---|---|
    | `_cdc.op` | what happened: `I` insert (also the full load's rows) · `U` update · `D` delete |
    | `_cdc.ts` | when the change happened in the source |
    | `_cdc.offset` | its position in the job's Kafka topic |
    | `_cdc.source` | the source table, e.g. `shop.public.orders` |
    | `_cdc.key` | the row's key (`order_id` here; `order_id` + `line` for `order_items`) |

    Read a field with a dot: `SELECT order_id, status, _cdc.op, _cdc.ts FROM … ORDER BY _cdc.ts DESC`:

    ```text
     order_id |  status   | op |               ts
    ----------+-----------+----+--------------------------------
        16454 | paid      | U  | 2026-11-03 10:07:01.872000 UTC
        16009 | paid      | U  | 2026-11-03 10:06:45.141000 UTC
        16244 | delivered | U  | 2026-11-03 10:06:30.945000 UTC
    ```

    In *Full load + CDC* mode a **deleted row is removed** (no tombstone): the table is exactly
    the shop as it is now — 1,822 rows for 1,822 orders. When you need to see deletes, use the
    history mode, where each delete is a row with `_cdc.op = 'D'`. The rest of this lesson uses
    OLake's columns; with Debezium read `_op_type` as `_cdc.op` and `_cdc_timestamp` as `_cdc.ts`.

### 2 · Right after the first run
```python
spark.sql("""SELECT _op_type, count(*) AS rows
             FROM my.shop_cdc.orders GROUP BY _op_type""").show()
```

```
+--------+----+
|_op_type|rows|
+--------+----+
|       u|   1|
|       r| 572|
+--------+----+
```

Almost everything is `r`: the snapshot. The one `u` is an order that changed **while** the
snapshot was being read. The job caught it from the log straight away.

### 3 · After a second run
Click **Run now** on your job (or wait a minute and run it), then:

```python
spark.sql("""SELECT _op_type, count(*) AS rows
             FROM my.shop_cdc.orders GROUP BY _op_type ORDER BY _op_type""").show()
spark.sql("""SELECT count(*) AS rows, count(DISTINCT order_id) AS orders
             FROM my.shop_cdc.orders""").show()
```

```
+--------+----+
|_op_type|rows|
+--------+----+
|       d|   1|
|       r| 567|
|       u|   9|
+--------+----+

+----+------+
|rows|orders|
+----+------+
| 577|   577|
+----+------+
```

- 9 rows changed from `r` to `u`, and 1 became a tombstone `d`.
- **rows = orders**: still one row per order. The updates replaced rows; they didn't add copies.

Now the changes themselves, oldest first:

```python
spark.sql("""SELECT order_id, status, total, _op_type, _cdc_timestamp
             FROM my.shop_cdc.orders
             WHERE _op_type <> 'r'
             ORDER BY _cdc_timestamp""").show(truncate=False)
```

```
+--------+---------+-------+--------+--------------------------+
|order_id|status   |total  |_op_type|_cdc_timestamp            |
+--------+---------+-------+--------+--------------------------+
|614     |paid     |1990.38|u       |2026-11-02 16:15:58.661927|
|524     |NULL     |NULL   |d       |2026-11-02 16:16:06.142462|
|501     |paid     |263.96 |u       |2026-11-02 16:16:26.978531|
|670     |placed   |1059.0 |u       |2026-11-02 16:16:31.169833|
|646     |paid     |127.02 |u       |2026-11-02 16:16:39.354122|
|289     |paid     |73.28  |u       |2026-11-02 16:16:47.722553|
|34      |delivered|117.0  |u       |2026-11-02 16:16:54.062743|
|671     |placed   |119.37 |u       |2026-11-02 16:16:56.129813|
|672     |placed   |17.49  |u       |2026-11-02 16:17:01.378299|
|673     |placed   |122.17 |u       |2026-11-02 16:17:12.950393|
+--------+---------+-------+--------+--------------------------+
```

**Read it step by step:**

- **Order 524** was cancelled: a tombstone, every column empty except the key and the markers.
- **Orders 34, 289, 501…** moved on in their life cycle: `u`.
- **Orders 670–673** are **new**, yet they show `u`, not `c`. The shop inserts an order and
  then updates its `total` a moment later. The lake keeps only the **last** thing that happened
  to each row. In `customers`, which are inserted and left alone, you'll find real `c` rows.

### 4 · Deletes in the other tables
When an order is cancelled, Postgres also deletes its items (`ON DELETE CASCADE`). The log
records each of those deletes too:

```python
spark.sql("""SELECT order_id, line, product_id, quantity, _op_type
             FROM my.shop_cdc.order_items WHERE order_id = 524""").show()
```

```
+--------+----+----------+--------+--------+
|order_id|line|product_id|quantity|_op_type|
+--------+----+----------+--------+--------+
|     524|   1|      NULL|    NULL|       d|
|     524|   2|      NULL|    NULL|       d|
+--------+----+----------+--------+--------+
```

!!! warning "Always filter out the tombstones"
    `count(*)` on a CDC table counts the deleted rows too. For "the shop as it is now" use
    `WHERE _op_type <> 'd'`. You'll make that a habit in the next lesson.

### 5 · The old values are still there: time travel
The lake keeps only the latest version of each row, but the **table** keeps every version of
itself. Each run is a new Iceberg **snapshot**
([table maintenance](../unit4/table-maintenance.md) showed snapshots and `VERSION AS OF`):

```python
spark.sql("""SELECT snapshot_id, operation,
                    summary['added-records']          AS added,
                    summary['added-equality-deletes'] AS eq_deletes
             FROM my.shop_cdc.orders.snapshots ORDER BY committed_at""").show()
```

```
+-------------------+---------+-----+----------+
|        snapshot_id|operation|added|eq_deletes|
+-------------------+---------+-----+----------+
|2903732596239262669|   append|  573|      NULL|
|2522315036876988445|overwrite|    1|         1|
| 965017472027462427|overwrite|   13|         5|
+-------------------+---------+-----+----------+
```

- **`summary['added-records']`**: `summary` is a **map** column (key → value pairs). `['key']`
  reads one value from it.
- The first snapshot is the full load: 573 rows appended.
- Later ones are **overwrite**s with **equality deletes**: Iceberg's way to upsert. It writes the
  new rows plus a small "delete the old row with this key" file instead of rewriting the data.

Read the table as it was after the first run (use **your own** first `snapshot_id`):

```python
spark.sql("""SELECT order_id, status, _op_type
             FROM my.shop_cdc.orders VERSION AS OF 2903732596239262669
             WHERE order_id IN (34, 524, 614)""").show()
spark.sql("""SELECT order_id, status, _op_type
             FROM my.shop_cdc.orders
             WHERE order_id IN (34, 524, 614) ORDER BY order_id""").show()
```

```
+--------+-------+--------+
|order_id| status|_op_type|
+--------+-------+--------+
|      34|shipped|       r|
|     524| placed|       r|
|     614|   paid|       r|
+--------+-------+--------+

+--------+---------+--------+
|order_id|   status|_op_type|
+--------+---------+--------+
|      34|delivered|       u|
|     524|     NULL|       d|
|     614|     paid|       u|
+--------+---------+--------+
```

Order 34 was *shipped* then and is *delivered* now. Order 524 existed then and is gone now.
Order 614 shows `u` with the same status: it changed in a column you didn't select.

!!! note "Between two runs, only the last change survives"
    If order 34 goes *paid → shipped → delivered* between two runs, the lake only ever sees
    *delivered*. Snapshots keep the state **at each run**, not every step in between. When
    every step matters (an audit trail, an SCD2 history), you need an **append** sink that
    keeps all change rows. Debezium + Kafka work that way. Our job keeps the current state.

## Check yourself
1. `SELECT count(*) FROM my.shop_cdc.orders` says 577, but the shop has 576 orders. Why?
2. A brand-new order shows `_op_type = 'u'`. Is that a bug?
3. Where would you look to see an order's status as it was yesterday morning?

??? success "Answers"
    1. One row is a tombstone (`d`) for a cancelled order. Count with `WHERE _op_type <> 'd'`.
    2. No. The shop inserts the order, then updates its total. Only the last operation is kept.
    3. Time travel: find yesterday morning's snapshot in `….orders.snapshots` and query
       `VERSION AS OF` it. Or use `TIMESTAMP AS OF '2026-11-02 09:00:00'`, which picks
       the snapshot that was current at that moment.

## You can now…
- explain OLake's five columns and the four values of `_op_type`;
- see that CDC mode keeps one row per key, with tombstones for deletes;
- use snapshots and `VERSION AS OF` to see a row's earlier state.
