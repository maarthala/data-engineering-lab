# Capstone 1 (easy)

## Concept
You've now touched every layer of a real data platform: **Python** (Unit {unit:unit3/python-essentials.md}), **Spark** to build
(Unit {unit:unit4/fundamentals.md}), **Airflow** to schedule (Unit {unit:unit5/basics.md}), **Apache Polaris** to govern (Unit {unit:unit6/polaris.md}), and **Superset**
to visualise (Unit {unit:unit7/dashboards.md}). This capstone strings them together on your own — end to end — by shipping
**one new metric** for ShopFlow.

The business question: **"What fraction of each day's orders were cancelled?"** Right now nobody
knows. Your job: add an **order cancellation rate** and flow it from the lakehouse to the executive
dashboard.

## Lab
Warm up. Confirm the signal exists — ShopFlow orders carry a `status` (`delivered`, `shipped`,
`placed`, `cancelled`). Check it in your Silver table — in a notebook:

```sql
%%sql
SELECT status, COUNT(*) AS orders
FROM my.silver.orders
GROUP BY status
ORDER BY orders DESC
```

(or in SQLPad / Trino as `demouser_lake.silver.orders`). If `cancelled` shows up, you have
everything you need. No Silver yet? Run your [5.2 medallion DAG](../unit5/medallion-dag.md) first.

## Challenge
**Project brief — ship the ShopFlow cancellation rate, Silver → dashboard.**

Add a new Gold mart `gold.daily_cancellations` to your lakehouse and expose it in Superset.

### Requirements
1. **Gold step.** Add a task **`gold_cancellations`** to your `demouser_medallion` DAG
   ([5.2](../unit5/medallion-dag.md)) that reads `my.silver.orders`, aggregates by
   `order_date`, and writes `my.gold.daily_cancellations` with columns `order_date`,
   `total_orders`, `cancelled_orders`, `cancellation_rate` (0.0–1.0), using `createOrReplace`
   (idempotent). It runs after `silver`, beside the existing `gold` task.
2. **Governance.** Share the new table **read-only** with one other member on **🗂️ My catalogs**
   ([6.4](../unit6/grant-and-query.md)) — nothing more than that one table.
3. **BI.** Add a Superset **Dataset** on `demouser_lake` → `gold` → `daily_cancellations` and a
   **line chart** of `cancellation_rate` over `order_date`, on your executive dashboard
   ([7.1](../unit7/dashboards.md)).

### Acceptance criteria
- `SELECT * FROM demouser_lake.gold.daily_cancellations ORDER BY order_date DESC LIMIT 5;`
  (SQLPad / Trino) returns one row per day with `cancellation_rate` between 0 and 1.
- Re-running the DAG leaves row counts unchanged (idempotent).
- My catalogs → `demouser_lake` → **Current shares** lists the read share on
  `gold.daily_cancellations`.
- The chart appears on **ShopFlow — Executive Overview**.

### Hints
- `cancellation_rate` is a ratio — **cast to `double` before dividing** or you'll get integer `0`,
  and guard against divide-by-zero on empty days.
- Model the task on the `gold` task in [5.2](../unit5/medallion-dag.md) — `lab_spark()`, then Spark.
- "Idempotent" = `createOrReplace` — same Silver in, same Gold out, every run.

??? note "Solution"

    **1. Gold step — a new task in `my_files/dags/medallion.py`**

    ```python
    @task
    def gold_cancellations():
        """Cancellation rate per day."""
        from lab_spark import lab_spark
        from pyspark.sql import functions as F
        spark = lab_spark()
        orders = spark.table("my.silver.orders")
        daily = (
            orders.groupBy("order_date")
            .agg(
                F.countDistinct("order_id").alias("total_orders"),
                F.countDistinct(F.when(F.col("status") == "cancelled", F.col("order_id")))
                    .alias("cancelled_orders"),
            )
            .withColumn(                                   # cast to double; guard /0
                "cancellation_rate",
                F.when(F.col("total_orders") > 0,
                       F.col("cancelled_orders").cast("double") / F.col("total_orders"))
                 .otherwise(F.lit(0.0)),
            )
        )
        daily.writeTo("my.gold.daily_cancellations").createOrReplace()
    ```

    and wire it beside `gold` (Silver has one row per order *line*, so count **distinct orders**):

    ```python
    s = silver()
    bronze() >> s >> [gold(), gold_cancellations()]
    ```

    **2. Governance — share it read-only (My catalogs)**

    **🗂️ My catalogs** → `demouser_lake` → **🤝 Invite a member**: the other member's username →
    **read** → to **table gold.daily_cancellations** → **Invite**.
    Behind the scenes that's a Polaris catalog role with `TABLE_READ_DATA` on that one table, bound
    to their principal-role — the least-privilege grant chain from [6.4](../unit6/grant-and-query.md).

    **3. Superset**

        1. Datasets → + Dataset → ShopFlow Lakehouse / demouser_lake / gold / daily_cancellations.
        2. Line Chart: X-axis order_date, Metric MAX(cancellation_rate), time grain Day.
        3. Save as "Daily Cancellation Rate" and add it to "ShopFlow — Executive Overview".

    **Verify** (SQLPad / Trino):

    ```sql
    SELECT * FROM demouser_lake.gold.daily_cancellations ORDER BY order_date DESC LIMIT 5;
    -- one row per day, cancellation_rate in [0,1]; re-run the DAG → row count unchanged.
    ```

!!! tip "🎯 The same metric-to-dashboard loop on Azure, Databricks, Snowflake & Fabric"
    **What you just did:** shipped one new metric end to end — a Gold mart built in Spark,
    scheduled in Airflow, shared through Apache Polaris, charted in Superset.

    - **Transform:** the identical PySpark runs unchanged as an **Azure Databricks** or **Fabric**
      notebook; in pure **ADF** it's a Mapping Data Flow (Aggregate + Derived Column).
    - **Orchestrate:** the task becomes a **Databricks Workflow**, a **Fabric/ADF pipeline** activity.
    - **Govern:** the `SELECT`-on-Gold grant is the same **Databricks catalog `GRANT`** (Databricks) /
      role `GRANT` (Snowflake).
    - **BI:** the chart becomes a **Databricks AI/BI** tile or **Power BI (Direct Lake)** on Fabric.
    - **Snowflake:** one stack — Snowpark/SQL builds the mart, a **Task** schedules it, a role
      `GRANT` governs it, **Snowsight** charts it.

    Build in Spark → schedule → govern → chart from Gold is the universal daily rhythm of the job.

## You can now…
- Ship a brand-new metric end to end across Spark, Airflow, Apache Polaris, and Superset
- Write an idempotent Gold job a scheduler can safely re-run
- Define least-privilege grants and surface a metric on a dashboard
