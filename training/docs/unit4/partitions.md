# Partitions: `coalesce` & `repartition`

## Concept
Spark never handles a DataFrame as one big block. It cuts the rows into **partitions** —
chunks that different workers process **at the same time** ([4.8](performance.md)). Each
partition becomes **one task** when Spark computes, and **one file** when Spark writes.

Two methods change how many partitions a DataFrame has:

| | `coalesce(n)` | `repartition(n)` |
|---|---|---|
| What it does | **glues** existing partitions together | **deals all rows out again** into `n` new partitions |
| Fewer partitions | ✅ | ✅ |
| More partitions | ❌ — asking for more changes nothing | ✅ |
| Moves rows between workers (**shuffle**) | no → **cheap** | yes → **costs time** |
| Partition sizes afterwards | can be uneven | even |

Think of partitions as **boxes of rows**. `coalesce` pours whole boxes into each other — quick,
but some boxes end up fuller than others. `repartition` empties every box onto the table and
repacks the rows evenly into new boxes — fair, but it takes work.

<figure markdown>
<svg viewBox="0 0 760 250" width="100%" role="img" aria-label="Left: coalesce(2) merges four partitions of 2 rows into two partitions of 4 rows, each new partition made of two whole old ones, no shuffle. Right: repartition(3) spreads the rows of all four partitions over three new partitions of about equal size, with arrows crossing, a shuffle." style="max-width:760px;font-family:inherit;font-size:13px">
  <text x="170" y="22" fill="currentColor" text-anchor="middle" font-weight="bold">coalesce(2) — merge, no shuffle</text>
  <g fill="none" stroke="currentColor" stroke-width="1.5">
    <rect x="30"  y="45" width="60" height="34" rx="4"/>
    <rect x="110" y="45" width="60" height="34" rx="4"/>
    <rect x="190" y="45" width="60" height="34" rx="4"/>
    <rect x="270" y="45" width="60" height="34" rx="4"/>
    <rect x="60"  y="170" width="90" height="34" rx="4" stroke-width="2.5"/>
    <rect x="200" y="170" width="90" height="34" rx="4" stroke-width="2.5"/>
    <path d="M60 79 L95 168"/><path d="M220 79 L115 168"/>
    <path d="M140 79 L235 168"/><path d="M300 79 L255 168"/>
  </g>
  <g fill="currentColor" text-anchor="middle">
    <text x="60" y="67">P0 · 2</text><text x="140" y="67">P1 · 2</text>
    <text x="220" y="67">P2 · 2</text><text x="300" y="67">P3 · 2</text>
    <text x="105" y="192">P0 · 4</text><text x="245" y="192">P1 · 4</text>
    <text x="170" y="232" opacity="0.75">whole partitions glued together</text>
  </g>
  <line x1="380" y1="30" x2="380" y2="235" stroke="currentColor" opacity="0.25"/>
  <text x="575" y="22" fill="currentColor" text-anchor="middle" font-weight="bold">repartition(3) — reshuffle every row</text>
  <g fill="none" stroke="currentColor" stroke-width="1.5">
    <rect x="425" y="45" width="60" height="34" rx="4"/>
    <rect x="505" y="45" width="60" height="34" rx="4"/>
    <rect x="585" y="45" width="60" height="34" rx="4"/>
    <rect x="665" y="45" width="60" height="34" rx="4"/>
    <rect x="430" y="170" width="80" height="34" rx="4" stroke-width="2.5"/>
    <rect x="535" y="170" width="80" height="34" rx="4" stroke-width="2.5"/>
    <rect x="640" y="170" width="80" height="34" rx="4" stroke-width="2.5"/>
  </g>
  <g stroke="currentColor" stroke-width="1" opacity="0.6">
    <path d="M455 79 L470 168"/><path d="M455 79 L575 168"/><path d="M455 79 L680 168"/>
    <path d="M535 79 L470 168"/><path d="M535 79 L575 168"/><path d="M535 79 L680 168"/>
    <path d="M615 79 L470 168"/><path d="M615 79 L575 168"/><path d="M615 79 L680 168"/>
    <path d="M695 79 L470 168"/><path d="M695 79 L575 168"/><path d="M695 79 L680 168"/>
  </g>
  <g fill="currentColor" text-anchor="middle">
    <text x="455" y="67">P0 · 24</text><text x="535" y="67">P1 · 26</text>
    <text x="615" y="67">P2 · 26</text><text x="695" y="67">P3 · 24</text>
    <text x="470" y="192">P0 · 33</text><text x="575" y="192">P1 · 34</text><text x="680" y="192">P2 · 33</text>
    <text x="575" y="232" opacity="0.75">every row may move to any new partition</text>
  </g>
</svg>
<figcaption>Left: <code>coalesce</code> joins whole partitions — no rows move between workers. Right: <code>repartition</code> sends every row to a new partition — a <strong>shuffle</strong>, but the result is even.</figcaption>
</figure>

!!! warning "Not the same as `F.coalesce` / SQL `COALESCE`"
    **`F.coalesce(col1, col2)`** and SQL **`COALESCE(a, b)`** return the **first non-`NULL`
    value** in a row — about **missing values** ([2.6](../unit2/conditional.md),
    [4.2.1](data-cleaning.md)). **`df.coalesce(n)`** — a **DataFrame** method with a **number** —
    is about **partitions**. Same word, unrelated jobs.

## Lab
Work in a notebook ([3.7](../unit3/spark.md)). The examples write to your own bucket,
`demouser-lake` — signed in with your own account? Use **your** bucket (e.g. `ravi-lake`).

### 1 · See the partitions
First a small helper that counts the rows in each partition — you'll use it all through the lab:

```python
from pyspark.sql import functions as F

def show_partitions(df):
    (df.groupBy(F.spark_partition_id().alias("partition"))
       .count()
       .orderBy("partition")
       .show())

df = spark.range(1, 9).repartition(4)

df.withColumn("partition", F.spark_partition_id()).orderBy("partition", "id").show()
show_partitions(df)
```

```
+---+---------+
| id|partition|
+---+---------+
|  1|        0|
|  8|        0|
|  3|        1|
|  7|        1|
|  2|        2|
|  5|        2|
|  4|        3|
|  6|        3|
+---+---------+

+---------+-----+
|partition|count|
+---------+-----+
|        0|    2|
|        1|    2|
|        2|    2|
|        3|    2|
+---------+-----+
```

**Read it step by step:**

- **`spark.range(1, 9)`** — a DataFrame with one column, `id`, holding the numbers **1 to 8** (the
  end, 9, is not included).
- **`.repartition(4)`** — spread those rows over **4** partitions (explained in step 3).
- **`F.spark_partition_id()`** — the **number of the partition** a row sits in (0, 1, 2, …).
  Partitions are normally invisible; this function lets you look inside.
- **`def show_partitions(df):`** — a small function: **group** the rows by partition number and
  **`count()`** them, so you see how many rows each partition holds.
- Result: **4 partitions × 2 rows**. Spark picks *which* rows go where — the exact ids may differ
  on your run; the counts are what matter.

### 2 · `coalesce` — glue partitions together
```python
df.coalesce(2).withColumn("partition", F.spark_partition_id()).orderBy("partition", "id").show()
```

```
+---+---------+
| id|partition|
+---+---------+
|  1|        0|
|  2|        0|
|  5|        0|
|  8|        0|
|  3|        1|
|  4|        1|
|  6|        1|
|  7|        1|
+---+---------+
```

- **`.coalesce(2)`** — merge the 4 partitions into **2**. Compare with step 1: old partitions **0
  and 2** (ids 1, 8, 2, 5) became the new partition 0; old **1 and 3** became partition 1.
  **Whole partitions** were glued together — no row was moved on its own.
- Same 8 rows, now **2 partitions × 4 rows**.

Now ask for **more** partitions than there are:

```python
show_partitions(df.coalesce(10))
```

```
+---------+-----+
|partition|count|
+---------+-----+
|        0|    2|
|        1|    2|
|        2|    2|
|        3|    2|
+---------+-----+
```

**Still 4.** No error — `coalesce` simply can't **split** a partition, only merge them. To get
more partitions you need `repartition`.

### 3 · `repartition` — deal the rows out again
With 100 rows the difference shows clearly:

```python
big = spark.range(1, 101).repartition(4)        # 100 rows, 4 partitions (24 / 26 / 26 / 24)

show_partitions(big.repartition(8))             # MORE partitions
show_partitions(big.coalesce(3))                # fewer, by merging
show_partitions(big.repartition(3))             # fewer, by reshuffling
```

```
repartition(8)          coalesce(3)             repartition(3)
+---------+-----+       +---------+-----+       +---------+-----+
|partition|count|       |partition|count|       |partition|count|
+---------+-----+       +---------+-----+       +---------+-----+
|        0|   12|       |        0|   48|       |        0|   33|
|        1|   12|       |        1|   26|       |        1|   34|
|        2|   14|       |        2|   26|       |        2|   33|
|        3|   14|       +---------+-----+       +---------+-----+
|        4|   12|
|        5|   12|
|        6|   12|
|        7|   12|
+---------+-----+
```

(The three outputs are shown side by side here; in the notebook they appear one after the other.)

- **`repartition(8)`** — **more** partitions: 4 → 8, about 12 rows each. Only `repartition` can do
  this.
- **`coalesce(3)`** — 4 → 3 by gluing two old partitions into one: **48** / 26 / 26. Cheap, but
  **uneven** — the task that gets the 48 rows takes twice as long as the others.
- **`repartition(3)`** — 4 → 3 by dealing every row out again: **33 / 34 / 33**. Even, but every
  row had to **move** (a shuffle).

!!! info "Repartition by a column"
    `repartition` can also put rows with the **same value** together:
    `df.repartition(8, "customer_id")` → all orders of one customer land in the same partition.
    Useful before heavy joins or aggregations on that key ([4.8](performance.md),
    [4.10 Data skew](skew.md)). `coalesce` can't do this — it never looks at the rows.

### 4 · One file in — how many partitions?
A common question: *"I read **one** Parquet file — is that **one** partition?"* Not necessarily.
Create a single ~20 MB Parquet file in your bucket to find out:

```python
path = "s3a://demouser-lake/files/tmp/one_file"

(spark.range(0, 300_000)
      .withColumn("txt", F.sha2(F.col("id").cast("string"), 256))
      .coalesce(1)
      .write.mode("overwrite")
      .option("parquet.block.size", 1024 * 1024)
      .parquet(path))

one = spark.read.parquet(path)
print(len(one.inputFiles()))        # 1 — really just one file
show_partitions(one)
```

```
1
+---------+------+
|partition| count|
+---------+------+
|        0|179348|
|        1|120652|
+---------+------+
```

**Read it step by step:**

- **`spark.range(0, 300_000)`** — 300,000 rows (`_` in a number is just for readability).
- **`F.sha2(F.col("id").cast("string"), 256)`** — a 64-character code made from each id, only so
  the file has some size (~20 MB). **`cast("string")`** turns the number into text first.
- **`.coalesce(1)`** — one partition → Spark writes **one** file. (You'll see why in step 5.)
- **`.write.mode("overwrite")`** — write, replacing the folder if it already exists.
- **`.option("parquet.block.size", 1024 * 1024)`** — store the file in **row groups** (internal
  blocks) of 1 MB. Spark can only cut a Parquet file **between** row groups — more on that below.
- **`one.inputFiles()`** — the list of files the DataFrame reads; **`len(...)`** counts them → **1**.

**One file, yet 2 partitions.** When Spark reads files it cuts them into pieces, and it
doesn't only cut at 128 MB — it also wants **every core** to get a piece of work. It works out
the **piece size** like this (simplified):

```
per_core   = (total file size + 4 MB per file) / number of cores
piece size = min( 128 MB , max( 4 MB , per_core ) )
```

- **128 MB** — `spark.sql.files.maxPartitionBytes`: the **largest** piece.
- **4 MB** — `spark.sql.files.openCostInBytes`: what Spark counts for *opening* a file. It's
  added once per file, and it's also the **smallest** piece Spark aims for — tiny pieces cost more
  to schedule than they save.
- **number of cores** — how many tasks your session can run at once. In this lab your Spark
  session has **2 cores** (`spark.cores.max=2`, locally and on Kubernetes).

For our 20 MB file:

```
per_core   = (20 + 4) / 2              = 12 MB
piece size = min(128, max(4, 12))      = 12 MB
pieces     = 20 MB cut into 12 MB      → 2   (12 MB + 8 MB)
```

That's exactly what the output shows: **179,348 / 120,652 rows** — about 60 % / 40 %, the same
split as 12 MB / 8 MB.

The **same file** on other clusters:

| Cores | `per_core` | Piece size | Partitions |
|---|---|---|---|
| 1 | 24 MB | 24 MB | **1** |
| 2 (this lab) | 12 MB | 12 MB | **2** |
| 4 | 6 MB | 6 MB | **about 4** |
| 8 | 3 MB | 4 MB (the minimum) | **about 5** |

So the number of partitions depends on the **cluster**, not just the file. A big file is cut at
128 MB; a smaller file is cut so all cores get a share — but never into pieces under 4 MB.

You can choose smaller pieces yourself:

```python
spark.conf.set("spark.sql.files.maxPartitionBytes", "4MB")
small_chunks = spark.read.parquet(path)
show_partitions(small_chunks)
```

```
+---------+-----+
|partition|count|
+---------+-----+
|        0|55184|
|        1|68980|
|        2|55184|
|        3|55184|
|        4|65468|
+---------+-----+
```

- **`spark.conf.set("spark.sql.files.maxPartitionBytes", "4MB")`** — pieces of at most 4 MB → the
  same single file now reads as **5** partitions. (`spark.conf.set` changes a setting for **your
  session** only — [4.8](performance.md).) With the formula:

    ```
    piece size = min(4, max(4, 12))   = 4 MB
    pieces     = 20 MB cut into 4 MB  → 5
    ```

Now `coalesce` and `repartition` on this DataFrame:

```python
show_partitions(small_chunks.coalesce(2))                               # 5 → 2
print(small_chunks.coalesce(50).select(F.spark_partition_id()).distinct().count())     # 5
print(small_chunks.repartition(50).select(F.spark_partition_id()).distinct().count())  # 50

spark.conf.unset("spark.sql.files.maxPartitionBytes")   # back to the default (128 MB)
```

```
+---------+------+
|partition| count|
+---------+------+
|        0|179348|
|        1|120652|
+---------+------+
5
50
```

- **`coalesce(2)`** — 5 → 2 by gluing pieces together.
- **`coalesce(50)`** — still **5**: it can't split pieces.
- **`repartition(50)`** — **50**: every row dealt out again.
- **`.select(F.spark_partition_id()).distinct().count()`** — a quick way to count the partitions
  that hold rows: list each row's partition number, keep the **distinct** ones, count them.
- **`spark.conf.unset(...)`** — remove your setting again, so Spark uses its default.

| A single Parquet file of… | Partitions when read | `coalesce(n)` useful? |
|---|---|---|
| a few MB | 1 | no — already 1, and it can't add more |
| ~20 MB (this lab) | about 2 (depends on cores) | only to go down to 1 |
| ~1 GB | about 8 (128 MB pieces) | yes — e.g. `coalesce(2)` |
| any size, written as **one** row group | 1 | no — Spark can't cut inside a row group |

### 5 · Partitions → files when you write
Every partition is written as **its own file**:

```python
out = "s3a://demouser-lake/files/tmp/out"

one.repartition(4).write.mode("overwrite").parquet(out)
print(len(spark.read.parquet(out).inputFiles()))      # 4

one.repartition(4).coalesce(1).write.mode("overwrite").parquet(out)
print(len(spark.read.parquet(out).inputFiles()))      # 1
```

```
4
1
```

- 4 partitions → **4 files**; `coalesce(1)` → **1 file**. Open **📁 My files** →
  `files/tmp/out/` to see them (plus an empty `_SUCCESS` marker Spark writes when the job
  finished).
- That's the most common use of `coalesce`: **fewer, bigger files**. Thousands of tiny files make
  every later read slow (the *small-files problem*, [4.7](table-maintenance.md)).

!!! warning "`coalesce(1)` makes the work before it run on one worker"
    `coalesce` adds no shuffle, so Spark runs the steps **before** it with the **new, smaller**
    number of tasks. `df.filter(...).withColumn(...).coalesce(1).write…` does the filter and
    the `withColumn` on **one** worker too. Fine for small results (a report, a CSV export);
    for heavy work use **`repartition(1)`** instead: the heavy part still runs in parallel, and
    only the final rows are shuffled into one partition.

### 6 · Shuffles make their own partitions
`groupBy`, `join`, `distinct` and `orderBy` **shuffle** — and a shuffle decides the number of
partitions again:

```python
print(spark.conf.get("spark.sql.shuffle.partitions"), spark.conf.get("spark.sql.adaptive.enabled"))

counts = spark.range(0, 1000).groupBy((F.col("id") % 10).alias("k")).count()
show_partitions(counts)
```

```
200 true
+---------+-----+
|partition|count|
+---------+-----+
|        0|   10|
+---------+-----+
```

- **`spark.sql.shuffle.partitions`** = **200** — after a shuffle Spark *plans* 200 partitions.
- **`spark.sql.adaptive.enabled`** = **true** — **AQE** (Adaptive Query Execution, [4.8](performance.md))
  looks at the real data after the shuffle and **merges** tiny partitions automatically. The 10
  result rows end up in **1** partition, not 200 near-empty ones.
- **`F.col("id") % 10`** — the remainder after dividing by 10 (0–9), so `groupBy` makes 10 groups.

So after a `groupBy` or `join` you rarely need `coalesce` for performance — AQE already does it.
You still use it before a **write**, to control the number of files.

### 7 · Clean up
In **📁 My files**, delete the **`files/tmp/one_file`** and **`files/tmp/out`** folders — they take
about 40 MB of your bucket's space.

## When to use which

| You want… | Use | Why |
|---|---|---|
| fewer output files | **`coalesce(n)`** | cheap, no shuffle |
| one CSV / report file from a **small** result | **`coalesce(1)`** | one file, nothing to shuffle |
| one file from a **heavy** job | **`repartition(1)`** | heavy work stays parallel; only the end is shuffled |
| **more** parallelism (few huge partitions) | **`repartition(n)`** | `coalesce` can't increase |
| even partitions after a filter removed most rows | **`repartition(n)`** | `coalesce` keeps them uneven |
| rows with the same key together | **`repartition(n, "key")`** | `coalesce` doesn't look at values |
| fewer partitions after `groupBy` / `join` | nothing | AQE merges them for you |

## 🎯 This runs unchanged on Azure, Databricks, Snowflake & Fabric
`coalesce`, `repartition`, `spark.sql.files.maxPartitionBytes` and `spark.sql.shuffle.partitions`
are plain Spark: the same in **Databricks** and **Fabric** notebooks. Databricks and Fabric also
**auto-optimize** file sizes when writing Delta tables (*optimized writes*), so you need
`coalesce` less there — but you still use it for single-file exports. **Snowflake** has no
partitions to manage; it splits data into micro-partitions on its own.

## You can now…
- Look inside a DataFrame's partitions with `spark_partition_id()`
- **Merge** partitions cheaply with `coalesce(n)` — and know it can't add partitions
- **Reshuffle** into even partitions, more or fewer, with `repartition(n)` (or by a key)
- Explain why **one file** can still be **several** partitions (128 MB pieces, cores, row groups)
- Control the number of **output files**, and avoid running a heavy job on one worker with `coalesce(1)`
- Tell `df.coalesce(n)` (partitions) apart from `F.coalesce(...)` / `COALESCE(...)` (missing values)
