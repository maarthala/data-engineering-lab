# Data skew & salting

## Concept
A shuffle spreads rows across partitions **by a key**. If one key has far more rows than the
others, its partition becomes huge — and since a shuffle stage is a **barrier**, the whole job
**waits on that one slow task**. That's **data skew**, and it's the most common reason a Spark
job "runs for hours on the last 2 tasks."

```
Even:                          Skewed:
[P1][P2][P3][P4]  all ~equal   [P1][P2][P3][ P4 ..................... ]  ← one giant partition
→ finish together              → 3 tasks done in minutes, 1 runs for hours (straggler)
```

## Detecting it
In the data flow monitor (or Spark UI):
- One partition has **far more rows / much longer task time** than the rest.
- The job flies to ~95%, then **stalls on 1–2 tasks**.

## The usual causes
| Cause | Example |
|---|---|
| **Null / blank keys** (most common!) | thousands of rows with `customer_id = null` all hash to **one** partition |
| **Hot keys** | one `product_id = "UNKNOWN"` holds 40% of the rows |
| **Low-cardinality key** | grouping by a column with only a few distinct values |

## Fixes, in order of preference

### 1. Filter/handle null & garbage keys *before* the shuffle (easiest, biggest win)
Nulls all collapse into one monster partition. Drop or quarantine them first.
```
readOrders → Filter(!isNull(order_id)) → dedup/aggregate
```
For a dedup on a near-unique key, this is usually **the whole fix** — the skew was the nulls.

### 2. Broadcast the small side of a join (skew-proof for joins)
If one side is small, broadcasting it means **no shuffle** → no skew possible on the big side.
(See [4.9 — Join strategies](spark-architecture.md#join-strategies-the-route).)

### 3. More partitions / round-robin (mild skew)
Rebalances moderately uneven data — not a single extreme hot key.
```
Optimize tab → Set partitioning → Round robin, raise the partition count
```

### 4. Salting (for a genuine hot key)
Split the hot key across many partitions with a random **salt**, aggregate in two stages, then
combine.

**Salting an aggregation** — `sum(amount)` by `product_id`, where `"UNKNOWN"` dominates:
```
N = 16
# Stage 1: add salt → the hot key now spreads across N partitions
salted  = df.withColumn("salt", (rand() * N).cast("int"))
partial = salted.groupBy("product_id", "salt").agg(sum("amount"))   # partial sums, parallel
# Stage 2: drop the salt, combine the partials
final   = partial.groupBy("product_id").agg(sum(partial_sum))       # correct total
```
The hot key `UNKNOWN` is now computed by N tasks in parallel instead of one straggler.
**ADF:** Derived Column `salt = round(rand()*16)` → Aggregate `groupBy(key, salt)` →
second Aggregate `groupBy(key)`.

**Salting a join** — salt the **skewed (big) side** randomly, and **fan out** the small side
(replicate each row once per salt value) so matches still happen:
```
orders_s   = orders.withColumn("salt", (rand()*N).cast("int"))   # random salt on big side
products_x = products.crossJoin(salts(0..N-1))                    # each product × every salt
result     = orders_s.join(products_x, ["product_id", "salt"])   # join on composite key
```
The hot key's join work now spreads across N partitions; the small side just grew N× (cheap).

### 5. Pre-aggregate before the shuffle
`groupBy` already does a **partial (map-side) aggregate** before the shuffle, so far less data
crosses the network — one reason group-by tolerates skew better than a `Window` does. Always
**filter and pre-aggregate early**.

## Decision guide
```
Skew from null/blank keys?      → filter/quarantine them first        (fix 1)
Joining a big table to a small? → broadcast the small side            (fix 2)
Mild general imbalance?         → round-robin / more partitions       (fix 3)
One legitimately huge key?      → salting + two-stage aggregate/join  (fix 4)
Always:                         → filter & pre-aggregate before shuffle(fix 5)
```

## Key terms, at a glance
| Term | Plain meaning |
|---|---|
| **Data skew** | uneven partition sizes → one task does most of the work |
| **Straggler** | the slow task the whole stage waits on |
| **Salt** | a random `0..N-1` tag that splits a hot key across partitions |
| **Two-stage aggregation** | partial aggregate by (key, salt), then final by key |
| **Fan-out** | replicating the small join side once per salt so salted keys still match |

## You can now…
- **Recognize** skew (straggler tasks) and name its usual cause (null/hot keys)
- Pick the right fix: **filter nulls**, **broadcast**, **repartition**, or **salt**
- Explain **salting** for both an aggregation and a skewed join, in two stages
- Say why **filtering/pre-aggregating before the shuffle** always helps

## 🎯 This runs unchanged on Azure, Databricks, Snowflake & Fabric
Skew, salting, and partitioning are **Apache Spark** concepts — the same techniques apply in
Databricks, Synapse/Fabric Spark, and any Spark-backed engine; only the UI differs.
