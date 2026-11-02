# Milestone: land the three systems

## Goal
Every table of the three systems lands in your lakehouse as a Bronze table, and stays at most
15 minutes behind the source, with CDC.

## Tasks
1. **Look before you copy.** In the SQL editor, open each of the connections
   *Tidewell web shop*, *Tidewell stores* and *Tidewell loyalty*. For every table, write down its
   key, what one row means, and how it changes (inserts only? updates? deletes?).
2. **Three sources** in 📡 Ingestion, each with CDC on: host `lab-db`, port `5432`, user and password
   `reader`, databases `shop`, `store`, `loyalty`. You may already have *Lab shop (live)*: that is
   the web shop.
3. **Three destinations** in your lakehouse: namespaces `tw_web`, `tw_store`, `tw_loyalty`.
4. **Three jobs**, one per system, all tables, **Full load + CDC**, **Every 15 min**.

??? tip "Hints"
    - One CDC job per source: a source has one change stream. Pick all tables in that one job.
    - Run each job once by hand (**Run now**) so you have data before the schedule starts.
    - Lost? [Your first sync job](../cdc/first-job.md) walks through the same steps.

## Self-checks
In a notebook. Every query must return what's described.

**B1 · Every table landed.**

```python
for ns, n in [("tw_web", 4), ("tw_store", 4), ("tw_loyalty", 2)]:
    found = spark.sql(f"SHOW TABLES IN my.{ns}").count()
    print(ns, found, "✓" if found == n else f"✗ expected {n}")
```

**B2 · Fresh: the newest row is less than 15 minutes old** (after the schedule has run once).

```python
spark.sql("""
SELECT 'web' AS system, max(_olake_timestamp) AS newest FROM my.tw_web.orders
UNION ALL SELECT 'store', max(_olake_timestamp) FROM my.tw_store.sales
UNION ALL SELECT 'loyalty', max(_olake_timestamp) FROM my.tw_loyalty.points""").show(truncate=False)
```

**B3 · CDC, not reloads.** On each job's page, the runs after the first copy only a few records
(the changes), not the whole tables again.

## Questions to answer in your notes
1. Which tables only ever get inserts? Which get updates? Which get deletes?
2. A web order is cancelled. What does Bronze `tw_web.orders` show for it, and what happens
   in `tw_loyalty.points`?
3. Why does a store return **not** show up as a delete?
