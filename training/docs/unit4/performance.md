# Spark performance & tuning

## Concept
Spark is fast because it works in **parallel** — but *how* it splits, shuffles, and joins data
decides whether a job takes 10 seconds or 10 minutes. This page covers the levers you'll actually
reach for: **partitions**, `repartition` vs `coalesce`, **`spark.conf.set`**, **broadcast joins**
(`autoBroadcastJoinThreshold`), caching, and the ideas behind them.

`spark` is pre-created ([3.7](../unit3/spark.md)); every snippet runs as-is in a notebook.

## Partitions — the unit of parallelism
A Spark DataFrame is split into **partitions** — chunks processed **in parallel**, one per task, on
the cluster's cores. Partitions are *the* reason Spark scales.

```python
from pyspark.sql.functions import spark_partition_id

RAW = "s3a://demouser-lake/files/source/shopflow"    # exported in {lesson:unit4/read-bronze.md} (your bucket)
df = spark.read.parquet(f"{RAW}/history/orders")
# how many chunks this DataFrame is split into
print(df.select(spark_partition_id()).distinct().count())
```

- **Too few** partitions → cores sit idle, no parallelism, big tasks.
- **Too many** tiny partitions → scheduling overhead dominates ("small files problem").
- Rule of thumb: aim for partitions in the **~100 MB** range, and at least as many as you have cores.

**Where partitions come from:** the source (files/blocks), and **shuffles** (see below). Reading a
folder of Parquet gives roughly one partition per file/block.

## Narrow vs wide transformations (and the shuffle)
- **Narrow** (`select`, `filter`, `withColumn`) — each output partition depends on **one** input
  partition. No data movement — cheap.
- **Wide** (`groupBy`, `join`, `distinct`, `orderBy`) — output partitions need data from **many**
  input partitions, so Spark **shuffles**: writes data across the network and re-partitions by key.
  Shuffles are the **expensive** part of most jobs.

The number of partitions **after a shuffle** is controlled by:

```python
spark.conf.set("spark.sql.shuffle.partitions", 8)   # default is 200
```
200 is fine for big clusters but wasteful for small/learning data (200 near-empty tasks). Lower it
for this stack.

## `repartition` vs `coalesce`
Both change the partition count, but differently:

| | `repartition(n)` | `coalesce(n)` |
|---|---|---|
| Direction | up **or** down | **down** only |
| Shuffle? | **yes** (full reshuffle) | **no** (merges adjacent partitions) |
| Even sizes? | yes, balanced | can be uneven |
| Cost | higher | cheap |
| Can partition **by column** | yes: `repartition("country")` | no |

```python
# Fewer output files when writing (cheap — no shuffle):
df.coalesce(1).write.mode("overwrite").parquet("s3a://demouser-lake/files/tmp/orders_single")

# Rebalance / partition by a key before a heavy op (costs a shuffle, but evens skew):
df.repartition(8, "customer_id")
```

**Use `coalesce`** to reduce files before a write (avoid the small-files problem).
**Use `repartition`** to *increase* parallelism or to co-locate rows by a key (balances skew).

> Step by step, with every output: **[4.11 Partitions: `coalesce` & `repartition`](partitions.md)** —
> including why one file can be several partitions.

## `spark.conf.set` — tuning knobs at runtime
`spark.conf.set("key", value)` changes a Spark SQL setting for your session — no restart. The ones
worth knowing:

```python
spark.conf.set("spark.sql.shuffle.partitions", 8)          # post-shuffle partitions
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "50MB")  # broadcast-join cutoff (below)
spark.conf.set("spark.sql.adaptive.enabled", True)         # AQE — usually on by default in Spark 3+/4

# read one back
print(spark.conf.get("spark.sql.shuffle.partitions"))
```
(`spark.conf.set` = session/runtime settings. Cluster-level things — cores, memory — are set when
the cluster starts, e.g. in `spark-defaults.conf`, not here.)

## Broadcast joins & `autoBroadcastJoinThreshold`
Joining a **big** table to a **small** one? A normal join shuffles *both* — slow. A **broadcast
join** sends a **copy of the small table to every node**, so the big table never shuffles. Much
faster.

Spark does this **automatically** when the small side is under
**`spark.sql.autoBroadcastJoinThreshold`** (default **10 MB**):

```python
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "50MB")  # broadcast bigger dimensions
# -1 disables auto-broadcast entirely
```

Or force it explicitly:

```python
from pyspark.sql import functions as F

big   = spark.table("iceberg.silver.orders")
small = spark.table("iceberg.silver.customers")

joined = big.join(F.broadcast(small), "customer_id")   # broadcast the small side
```

Rule: **broadcast the small dimension**, keep the big fact table put. It turns a two-sided shuffle
into a one-sided copy.

## Caching / persist
If you reuse a DataFrame across several actions, **cache** it so Spark doesn't recompute the whole
plan each time:

```python
df = spark.table("iceberg.silver.orders").filter("status = 'delivered'")
df.cache()          # keep in memory after first action
df.count()          # first action → computes & caches
df.groupBy("channel").count().show()   # reuses the cache
df.unpersist()      # free it when done
```
Only cache what you **reuse** — caching a once-used DataFrame just wastes memory.

## Partition pruning & predicate pushdown (free speed)
Spark skips data it doesn't need — if you let it:

- **Partition pruning** — the history data is laid out by `dt=...` folders. `WHERE dt = '2023-08-05'`
  reads **only that folder**, not the whole dataset.
- **Predicate pushdown** — `filter`/`WHERE` and column `select` are pushed into the Parquet/Iceberg
  reader, so less data leaves disk.

```python
# reads just one day's files, only two columns:
spark.read.parquet(f"{RAW}/history/orders") \
     .where("dt = '2023-08-05'").select("order_id", "status").show()
```
**Filter early, select only the columns you need** — the cheapest optimisation there is.

## Adaptive Query Execution (AQE)
Spark 3+/4 re-optimises at **runtime** using real data sizes — coalescing shuffle partitions,
switching to broadcast joins, handling skew — when `spark.sql.adaptive.enabled` is on (default). It
means the `shuffle.partitions` number matters less than it used to, but setting a sane value still
helps on small clusters.

!!! tip "Go deeper"
    - **[4.9 How Spark runs](spark-architecture.md)** — driver vs executors, the query lifecycle,
      how executors read the source in parallel, and the *route* of a join (broadcast vs sort-merge).
    - **[4.10 Data skew & salting](skew.md)** — why one hot key stalls the whole job, and how to fix it.
    - **[4.11 Partitions](partitions.md)** — `coalesce` vs `repartition` in depth, files ↔ partitions.
    - **[4.7 Table maintenance](table-maintenance.md)** — fix the small-files problem at the table level
      (`OPTIMIZE`, Z-order, `VACUUM`).

## Quick checklist
- Set **`spark.sql.shuffle.partitions`** sensibly (low for this stack).
- **`coalesce`** to cut output files; **`repartition`** to add parallelism / balance skew.
- **Broadcast** the small side of a big↔small join (auto via `autoBroadcastJoinThreshold`, or `F.broadcast`).
- **Filter and select early** — lean on partition pruning + pushdown.
- **`cache`** only DataFrames you reuse; `unpersist` when done.
- Prefer **fewer wide transformations** — shuffles are the cost.

## 🎯 This runs unchanged on Azure, Databricks, Snowflake & Fabric
These are core **Apache Spark** concepts — identical on **Azure Databricks** and **Fabric Spark**
(same `spark.conf`, `repartition`/`coalesce`, broadcast joins, AQE). Databricks adds Photon +
auto-optimize on top, but the tuning model is exactly this.

## You can now…
- Explain **partitions** and narrow vs wide transformations (and why shuffles cost)
- Choose **`repartition`** vs **`coalesce`** correctly
- Tune a session with **`spark.conf.set`** (`shuffle.partitions`, `autoBroadcastJoinThreshold`)
- Speed up big↔small joins with **broadcast**, and reuse work with **cache**
- Lean on **partition pruning / pushdown** and know what **AQE** does for you
