# Challenge: build a Gold mart

## Concept
You've walked the full medallion with Spark: read the lake into **Bronze** ({lesson:unit4/read-bronze.md}),
cleaned and joined into **Silver** ({lesson:unit4/transform-silver.md}), and aggregated into **Gold** ({lesson:unit4/spark-sql-gold.md}). Now you
own the pattern end to end. This unit's challenge asks you to design and ship a **new
Gold mart** yourself — the kind of ticket a data engineer picks up on any real team.

```mermaid
flowchart LR
  S["🥈 silver.orders"] --> Q["your aggregation<br/>(spark.sql)"] --> G["🥇 gold.category_daily_revenue"]
```

**What is the mart you're about to build?** `gold.category_daily_revenue` is a small,
purpose-built table that answers one business question directly: *"On any given day, how much
revenue did each product category bring in, and how big a slice of that day's total was it?"*
Instead of forcing an analyst to re-derive that number by joining and summing raw orders every
time they open a dashboard, you compute it **once**, store it, and let everyone read the answer
cheaply. That's the whole point of the Gold layer: it turns clean Silver detail into a
**pre-aggregated, decision-ready** table. One row = one category on one day, which is exactly
the shape a "revenue by category over time" chart needs.

**Why it matters.** Silver holds one row per order line — millions of rows, great for
flexibility but slow and repetitive to aggregate on every query. Gold trades that flexibility
for **speed and consistency**: the heavy `GROUP BY` runs in your pipeline, not in the
dashboard, and every consumer sees the *same* definition of "daily category revenue" (no two
analysts writing subtly different SQL and getting different totals). The extra `pct_of_day`
column — each category's share of that day's revenue — is a classic Gold move: bake the
*business ratio* into the table so the reader doesn't have to compute it. This is the everyday
work of a data engineer: take a business question, express it as an aggregation over the
governed Silver layer, and ship a named, reusable mart.

A good Gold mart is: built **only** from Silver (never re-reading raw sources),
**business-meaningful**, written to your own lakehouse **`my`** (so Trino can read it, as `demouser_lake`), and
**partitioned** if it's naturally sliced by date.

## Lab
### The brief
Build **`gold.category_daily_revenue`**: for each **product category** and **day**,
report revenue, units sold, and number of distinct orders — delivered orders only. Then add
one analytical column that requires a **window function** (Unit {unit:unit2/intro.md} skill): each row's
**share of that day's total revenue**, so the business can see which categories
dominate on any given day.

### Requirements
1. Source **only** from `my.silver.orders`.
2. Grain: one row per `(category, order_date)`.
3. Columns: `order_date`, `category`, `revenue`, `units`, `orders`,
   `pct_of_day` (this category's revenue ÷ that day's total revenue).
4. Write as an **Iceberg** table, **partitioned by month of `order_date`**.
5. Create it as `my.gold.category_daily_revenue` and verify with a query.

### Starter scaffold

```python
from pyspark.sql.functions import partitioning
spark.sql("CREATE SCHEMA IF NOT EXISTS my.gold")
# TODO: aggregate, add pct_of_day via a window, write the Iceberg table.
```

Think first: which Unit {unit:unit2/intro.md} tools apply? A `GROUP BY` for the grain, and a
`sum(...) OVER (PARTITION BY order_date)` window to get the daily total without a
second pass.

## Challenge
Ship `gold.category_daily_revenue` meeting all five requirements above. Bonus: after
building it, confirm the **same** table is readable from Trino (it should be — same catalog).

??? note "Solution"
    ```python
    from pyspark.sql.functions import partitioning

    cat_daily = spark.sql("""
        WITH agg AS (
            SELECT
                order_date,
                category,
                sum(line_amount)          AS revenue,
                sum(quantity)             AS units,
                count(DISTINCT order_id)  AS orders
            FROM my.silver.orders
            WHERE status = 'delivered'
            GROUP BY order_date, category
        )
        SELECT
            order_date,
            category,
            revenue,
            units,
            orders,
            round(
                revenue / sum(revenue) OVER (PARTITION BY order_date) * 100, 2
            ) AS pct_of_day
        FROM agg
        ORDER BY order_date, revenue DESC
    """)

    (cat_daily.writeTo("my.gold.category_daily_revenue")
        .using("iceberg")
        .partitionedBy(partitioning.months("order_date"))
        .createOrReplace())

    # verify
    spark.sql("""
        SELECT * FROM my.gold.category_daily_revenue
        ORDER BY order_date DESC, revenue DESC
        LIMIT 15
    """).show()
    ```
    The window `sum(revenue) OVER (PARTITION BY order_date)` computes each day's total
    alongside the per-category rows — no self-join needed. Your notebook's `my` is your own
    lakehouse, which Trino calls `demouser_lake` (own account? `<your username>_lake`) — the identical
    table opens there:
    ```sql
    SELECT order_date, category, pct_of_day
    FROM demouser_lake.gold.category_daily_revenue
    ORDER BY order_date DESC LIMIT 10;
    ```

    **Walk through the solution step by step.** The whole thing is one read → transform →
    aggregate → write pipeline, expressed as a single `spark.sql` query plus one `writeTo`.

    - **Read (the `FROM`).** `FROM my.silver.orders` reads *only* from Silver — the clean,
      governed detail table from lesson {lesson:unit4/transform-silver.md}. We never touch the raw lake here; requirement 1 is
      "source from Silver alone", and honouring it is what keeps Gold consistent (everyone
      aggregates the same trusted rows). `my.silver.orders` is already one row per order
      line, with `line_amount`, `quantity`, `order_date`, `category`, and `status` columns ready
      to roll up.
    - **Filter (`WHERE status = 'delivered'`).** Revenue only counts orders that actually
      shipped, so we drop everything else *before* aggregating. `WHERE` runs before `GROUP BY`,
      so cancelled or pending lines never enter any sum.
    - **Aggregate (the `agg` CTE).** `GROUP BY order_date, category` collapses the line-level
      rows into **one row per (day, category)** — that pairing *is* the grain of the mart.
      Inside each group we compute three business measures:
        - `sum(line_amount) AS revenue` — total money earned by that category that day.
        - `sum(quantity) AS units` — total items sold.
        - `count(DISTINCT order_id) AS orders` — how many *distinct* orders touched the
          category. `DISTINCT` matters because one order can have several lines in the same
          category; a plain `count(*)` would count lines, not orders, and inflate the number.
    - **The window (`pct_of_day`).** After grouping, each row already knows its own category
      revenue. To get its *share of the day*, we need that day's grand total sitting next to it —
      and `sum(revenue) OVER (PARTITION BY order_date)` does exactly that. A window function
      computes an aggregate **across a partition of rows without collapsing them**: it partitions
      the aggregated rows by `order_date` and sums `revenue` within each day, returning that
      total on *every* row of the day. Then `revenue / (that total) * 100` is the percentage, and
      `round(…, 2)` trims it to two decimals. The payoff: we get the ratio in a **single pass**,
      with no second query and no self-join back to a "daily totals" table.
    - **Order (`ORDER BY order_date, revenue DESC`).** Purely for readable output — biggest
      category first within each day. It doesn't change the stored data.
    - **Write (`writeTo(...).createOrReplace()`).** `writeTo("my.gold.category_daily_revenue")`
      targets the Gold schema in the shared catalog; `.using("iceberg")` writes it as an Iceberg
      table (so Trino and Spark read the identical files); `.partitionedBy(partitioning.months("order_date"))`
      satisfies requirement 4 — Iceberg buckets the data files by *month of* `order_date`, so a
      query filtered to one month prunes straight to the right files instead of scanning the whole
      table. `.createOrReplace()` makes the write **idempotent**: rerun the pipeline and it
      rebuilds the table cleanly rather than erroring or appending duplicates — the behaviour you
      want for a mart that's regenerated on a schedule.
    - **Verify.** The final `spark.sql(...).show()` reads the mart back and prints the newest
      days, so you can eyeball that revenue, units, orders, and `pct_of_day` all look sane.

    **Grain, restated:** one row per `(category, order_date)`. Everything above exists to produce
    that shape — the `GROUP BY` sets it, the window enriches it, and the partitioned Iceberg write
    persists it for both engines.

!!! tip "🎯 This runs unchanged on Azure, Databricks, Snowflake & Fabric"
    **What you just did:** designed and shipped a brand-new Gold mart from Silver alone,
    combining `GROUP BY` with a window function and writing a partitioned Iceberg table.

    - **Azure Databricks** — this exact PySpark/`spark.sql` runs unchanged, scheduled as a Job;
      you can also express Bronze→Silver→Gold declaratively with **Delta Live Tables (DLT)**,
      governed by **the Databricks catalog** (the cloud analogue of the **Apache Polaris** catalog you use here).
    - **Microsoft Fabric** — Fabric Spark notebooks build the same mart and store it as Delta in
      a Lakehouse on OneLake.
    - **Snowflake** — build the same `GROUP BY` + window mart in SQL/Snowpark, or as a declarative
      **Dynamic Table**, governed by **RBAC roles**.
    - **Azure Data Factory** — rebuild it no-code with a Mapping Data Flow (Aggregate + Window),
      scheduled in a pipeline.

    The medallion + governed-catalog pattern you just executed is the industry standard.

## Key terms, at a glance
| Term | Plain meaning |
|---|---|
| **Gold mart** | A focused, business-ready aggregate table |
| **Grain** | What one output row represents — here, one `(category, order_date)` |
| **Built from Silver only** | Never re-read raw sources for Gold |
| **`GROUP BY`** | Collapse detail rows into one summary row per group (sets the grain) |
| **`count(DISTINCT …)`** | Count unique values — count orders, not the lines they fan out to |
| **`… OVER (PARTITION BY …)`** | Window: per-group total alongside detail rows, no self-join |
| **`pct_of_day`** | A baked-in business ratio: a category's share of its day's revenue |
| **`writeTo(...).createOrReplace()`** | Idempotent write — rebuild the mart cleanly on every run |
| **`partitionBy` / `partitioning.months(...)`** | Split output files by a column (here, month) for pruned reads |
| **Shared catalog** | One Iceberg table, written by Spark and read by Trino |

## You can now…
- Design and build a new business-meaningful Gold mart from Silver alone
- Combine `GROUP BY` with a window function to compute shares in one pass
- Ship a partitioned Iceberg table readable by both Spark and Trino (same catalog)
