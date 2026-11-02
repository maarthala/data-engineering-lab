# Milestone: run it, show it, trust it

## Goal
The platform runs by itself, the managers have a dashboard, and you know when something breaks.

## Tasks
### 1 · Schedule it
Ingestion copies every 15 minutes. Silver and Gold must follow.

- Turn your Silver and Gold notebook code into a job, and a DAG that runs it every 15 minutes
  ([the development workflow](../unit5/dev-workflow.md),
  [schedule ingestion & Spark jobs](../unit5/schedule.md)).
- Silver first, then Gold: one task each, Gold after Silver.

### 2 · Show it
A Superset dashboard "Tidewell today" on your Gold tables
([dashboards from the Gold layer](../unit7/dashboards.md)), with at least:

- revenue today, web against stores;
- revenue by store for the last 7 days;
- the top 10 customers by total revenue, and whether they have a card;
- points earned against points redeemed per day.

### 3 · Trust it
Make the self-checks from the earlier milestones part of the pipeline: a last task that runs them
and **fails** when one doesn't pass ([data quality checks](../recipes/data-quality.md)). Add a
freshness check: fail when the newest Bronze row is more than 30 minutes old.

??? tip "Hint"
    A check that fails a task is a Python `assert` (or `raise`) in the job:

    ```python
    dup = spark.sql("SELECT count(*) - count(DISTINCT customer_key) FROM my.tw_silver.customers").first()[0]
    assert dup == 0, f"{dup} duplicate customers"
    ```

    - **`.first()[0]`**: the first row of the result, and its first column: one number.
    - **`assert condition, message`**: stops with an error showing `message` when the condition
      is false. Airflow marks the task failed, and the run shows red.

## Done when
- [ ] three Ingestion jobs run every 15 minutes with CDC
- [ ] Silver and Gold rebuild by themselves after each copy (Airflow)
- [ ] all self-checks pass, inside the pipeline
- [ ] the "Tidewell today" dashboard shows today's numbers
- [ ] your notes answer the questions of each milestone

## Show your work
Write one page for Tidewell's managers. No code: what the platform does, how fresh the numbers
are, what the checks guard against, and the three most surprising things you found in the data.
That page is what a real data engineer would hand over.

## Clean up
When you're done: pause the schedules, then delete jobs, sources and destinations in Ingestion
(deleting a source drops its replication slot), and the `tw_*` namespaces in 🗂️ My catalogs
([Run it for real](../cdc/operate.md) has the order).
