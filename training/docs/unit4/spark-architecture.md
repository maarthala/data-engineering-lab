# How Spark runs under the hood (architecture & the query lifecycle)

## Concept
Every data flow / Spark job is really **one driver coordinating many executors**. Understanding
who does what — and *how your query becomes parallel work* — is what makes the performance
lessons (shuffle, broadcast, skew) click.

The one idea to hold onto: **the driver is the brain (it plans and schedules); the executors
are the muscle (they read data and do the work). Bulk data does NOT flow through the driver.**

## Driver vs executors
```
         ┌─────────────┐
         │   DRIVER    │   plans the query, splits it into tasks, schedules them,
         │  (the brain)│   tracks progress. Holds NO bulk data (one exception: broadcast).
         └──────┬──────┘
        assigns tasks ↓ (metadata only)
   ┌────────┬────────┬────────┐
   │  EXEC 1 │ EXEC 2 │ EXEC 3 │   read data slices DIRECTLY from the source, in parallel,
   │ (muscle)│(muscle)│(muscle)│   run the transforms, hold partitions in memory.
   └────────┴────────┴────────┘
```
- **Driver**: parses your code, optimizes the plan, computes how to split the read, creates
  **tasks**, assigns them to executors, and collects final status. It's air-traffic control.
- **Executors**: each reads its **slice** of the source directly and processes it. Data moves
  **executor↔source** and **executor↔executor** — **not** through the driver.

!!! warning "The one time data goes through the driver"
    A **broadcast join** collects the small table **to the driver** first, then ships a copy to
    every executor. That's the *only* time bulk data routes through the driver — and exactly why
    an oversized broadcast can OOM the driver (see [Join strategies](#join-strategies-the-route)).

## The query lifecycle (code → parallel tasks)
```
your data flow / SQL
   → LOGICAL plan            (what you asked for)
   → Catalyst OPTIMIZER      (predicate pushdown, column pruning, join strategy choice)
   → PHYSICAL plan           (how to actually run it)
   → DAG of STAGES           (split at every shuffle)
   → TASKS (1 per partition) → scheduled onto executors
```
Nothing runs until an **action** (write to a sink, `count`, `collect`). Transformations are
**lazy** — Spark builds the whole plan first so it can optimize across steps (e.g. push a filter
down into the source read).

## How executors read the source *in parallel*
The driver decides **what** each executor reads; executors do the reading. Two source types:

**Database (JDBC)** — with `partitionColumn` + bounds + `numPartitions`, the driver generates
**N bounded queries**, and each executor runs **one** directly against the DB:
```sql
-- Executor 1                          -- Executor 2
SELECT city, amount FROM orders        SELECT city, amount FROM orders
WHERE order_date >= '2024-01-01'       WHERE order_date >= '2024-01-01'
  AND order_id <  2000001              AND order_id >= 2000001 AND order_id < 4000001
```
Filter (`WHERE`) is **pushed down**; only needed columns are read (**pruning**).

!!! danger "The #1 JDBC mistake"
    No `partitionColumn`/`numPartitions` → Spark runs **one** query on **one** executor —
    the whole table single-threaded. Always partition large JDBC reads.

**Files (Parquet/CSV in the lake)** — the driver **lists the files**, splits them into chunks,
and hands one chunk per task. Parquet's per-row-group **min/max stats** let executors **skip**
chunks that can't match the filter (predicate pushdown).

## Stages & shuffles
A **stage** is a run of **narrow** transforms that pipeline together with no data movement.
A **shuffle** (any **wide** transform — join, groupBy, window, sort) **ends one stage and
starts the next**:
```
Stage 1: read → filter → (partial aggregate)   ══ SHUFFLE ══   Stage 2: final aggregate → write
                                              (network + disk,
                                               a barrier: Stage 2
                                               waits for all of Stage 1)
```
Count the shuffles in the monitor — each is a cost, and each is where things can go wrong
(skew, spill).

## Join strategies (the route)
A join must get **matching keys onto the same executor**. Two ways:

**Broadcast join** (small side): the small table is **collected to the driver**, then a **full
copy is shipped to every executor**. The big table **stays put** — each partition joins locally
against the in-memory copy. **No shuffle of the big table.**
```
products (small) → driver (collect) → copy to E1,E2,E3 → each joins its orders slice locally
orders (big)     → never moves
```
- Fast, but the copy must fit in **each executor's memory** (and the driver's). Too big →
  driver OOM / broadcast timeout. That's why the "is it *really* small?" check matters.

**Sort-merge join** (both large): **both** tables are hash-partitioned by the key (a shuffle),
sorted, then merged. The big table moves over the network — expensive, but works at any size.

| | Broadcast | Sort-merge |
|---|---|---|
| Big table shuffles? | **No** | **Yes** |
| Needs | small side fits in memory | nothing special |
| Cost | ship small side once | shuffle both sides |

Spark auto-broadcasts when a side is under `spark.sql.autoBroadcastJoinThreshold` (default
10 MB); otherwise it sort-merges. In a data flow you steer this on the **Join → Optimize →
Broadcast** setting.

## Key terms, at a glance
| Term | Plain meaning |
|---|---|
| **Driver** | plans + schedules the job; holds no bulk data (except a broadcast collect) |
| **Executor** | worker that reads a data slice and runs the tasks |
| **Task** | the work for **one partition** on one core |
| **Stage** | pipelined narrow transforms; boundaries are shuffles |
| **Catalyst** | the optimizer that pushes filters/columns down and picks the join strategy |
| **Predicate / projection pushdown** | filter + column selection done at the source read |

## You can now…
- Explain the **driver vs executor** split and that bulk data doesn't flow through the driver
- Describe the **query lifecycle** (lazy plan → optimizer → stages → tasks)
- Say **how executors read a source in parallel** (JDBC bounded queries; file splits)
- Trace the **route of a join** for both broadcast and sort-merge, and why broadcast avoids the big shuffle
