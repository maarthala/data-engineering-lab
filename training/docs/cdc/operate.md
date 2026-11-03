# Run it for real: schedules, costs and clean-up

## Concept
A sync you click by hand is a demo. A sync that runs by itself, that you can trust and that
cleans up after itself is a pipeline. This lesson covers the decisions that make the difference.

### CDC or full reload?
| Use **Full load + CDC** when… | Use **Full reload** when… |
|---|---|
| tables are big and change a little at a time | tables are small (a few thousand rows) |
| you need deletes to reach the lake | the database can't do CDC (`wal_level` isn't `logical`, or the login can't replicate) |
| the source should feel as little load as possible | the source's CDC is already used by another job |
| you want the lake minutes behind the source | once a day is fresh enough |

### Current state or history?
Both CDC modes read the same change stream. They differ in what they keep:

| Mode | The table keeps | Use it for |
|---|---|---|
| **Full load + CDC (merge by key)** | one row per key: the **latest** version (deleted rows as tombstones — with Debezium they're removed) | "the shop as it is now" |
| **Full load + every change (history)** | **every** insert, update and delete as its own row (OLake: `_op_type` c / u / d, `_cdc_timestamp`; Debezium: `_cdc.op` I / U / D, `_cdc.ts`); nothing is overwritten | history: what an order looked like at 10:00, how often a price changed, SCD2 |

In history mode, order 15010 has one row as *shipped* (from the full load) and a second as
*delivered* (the update), where *Full load + CDC* keeps only *delivered*. Ingestion never builds
SCD2 tables itself: you build them in Silver from the history, with `MERGE`
([Slowly changing dimensions](../recipes/scd2.md)).

### What a replication slot costs
A slot makes Postgres **keep** the change log until the slot's reader has read it. That is
what makes CDC lossless, and also its main risk:

- A job that **runs** often keeps its slot near the end of the log: little is kept.
- A job that is **paused**, failing or deleted *without* its source keeps the slot standing
  still, and the log piles up on the database server's disk.
- The lab's shop caps this with `max_slot_wal_keep_size = 1GB`. Past that, Postgres drops the
  oldest log, and the slot can no longer continue where it stopped: the job has to start again
  with a full load.

On a real production database, a forgotten slot that fills the disk is a well-known way to
take the database down. That's why the database team watches slots, and why you tidy up.

## Lab
### 1 · Put the job on a schedule
On your job's page, in **How it runs**, choose **Cron** and pick the preset **Every 15 minutes**
→ **Save**. That's all: the lab runs it every 15 minutes from now on.

A **cron expression** is the classic way to write a schedule: five fields, *minute hour
day-of-month month day-of-week*, where `*` means "every" and `*/15` "every 15th":

| Expression | Runs |
|---|---|
| `*/15 * * * *` | every 15 minutes |
| `0 * * * *` | every hour, on the hour |
| `0 2 * * *` | every day at 02:00 |
| `30 6 * * 1-5` | weekdays (Mon–Fri) at 06:30 |
| `0 3 * * 0` | Sundays at 03:00 |

The times are **UTC**. The page writes the expression in words (*→ Every 15 minutes (UTC)*) and
the job's line shows when it runs next. **Manual** means only **Run now**; **Datastream** never
stops ([Live data](datastream.md)).

After half an hour, look at **Runs**: two scheduled runs (*Why*: schedule), each a few hundred
records at most. Rebuild Silver from the last lesson and the gap to the shop is never more
than one schedule interval.

- **Pause schedule** stops the scheduled runs; **Run now** still works. Remember the slot keeps
  the log while you pause. Pause for hours, not weeks.
- A run that's due while the previous one is still busy waits for it: one run at a time per job.
- Ingestion's schedule only copies. If Silver and Gold should be rebuilt after each copy,
  that's a job for [Airflow](../unit5/basics.md): one DAG that runs the notebook steps on its
  own schedule, after the copy.

### 2 · When a run fails
Open the run's **Log**. The usual causes:

| In the log | Cause | Fix |
|---|---|---|
| `password authentication failed` | the source's password changed | **Sources → Edit**, type the new password, **Save** |
| `wal_level is not 'logical'` | the database can't do CDC | make the job a **Full reload**, or ask for `wal_level = logical` |
| an error about the **replication slot** | the slot was dropped, or lost its log (the 1 GB cap) | **Check** the source (it makes a new slot), then recreate the job |
| `this job is running already` (on **Run now**) | a run is still busy | wait; one run at a time per job |
| `the Streaming unit is off` (Debezium) | Kafka + Kafka Connect aren't running | a lab manager switches on **Streaming** (`make status`; team stack: Settings → Services) |
| `… is there already, written by something else` (Debezium) | the destination's namespace holds a table another tool made (e.g. an OLake job's) | choose a destination with another namespace |

### 3 · Clean up in the right order
Each piece you made leaves something behind. Delete them in this order:

1. **The job** (**Delete** on its page). Its runs go with it (with Debezium also its connectors
   and topics). Its **tables stay** in your lakehouse: they're your data now.
2. **The source**, if you no longer need it. Deleting a CDC source **drops its replication
   slot** in the shop, so the shop stops keeping log for you.
3. **The destination**, if you no longer need it. Only the setting goes; it holds no data.
4. **The tables**, if you don't want them: **My catalogs → your lakehouse → `shop_cdc`** →
   🗑 next to each table (it deletes the table and its files), then 🗑 on the empty namespace.
   Or in SQL: `DROP TABLE my.shop_cdc.orders PURGE`.

!!! warning "Don't delete the files by hand"
    Deleting a table's folder in **My files** removes the files but leaves the table registered
    in the catalog, broken. Always drop tables through the catalog (🗑 in My catalogs, or
    `DROP TABLE`).

## Challenge
The shop's manager wants a small **"orders today"** report that is never more than 15 minutes
old.

1. Make sure your job runs **Cron `*/15 * * * *`**.
2. Build a Gold table `my.gold.shop_orders_today` with one row per status: the number of orders
   **created today** and their total value. Build it from Silver, not from Bronze.
3. Add a column `as_of`: the latest `_olake_timestamp` in Bronze `orders` (Debezium:
   `_cdc.ts`). That's the moment the data is from, which is not the same as the moment you ran
   the query.
4. Run the job, rebuild Silver and Gold, and compare `placed` with the shop's own count of
   today's placed orders. Explain the difference.
5. Bonus: find one order that was **cancelled today** using only the lake. (Hint: tombstones
   keep the key and `_cdc_timestamp`.)

??? tip "Hints"
    - `to_date(created_at) = current_date()` keeps today's orders.
    - `max(_olake_timestamp)` from `my.shop_cdc.orders` gives `as_of`. `CROSS JOIN` that one-row
      result onto your status counts.
    - Cancelled today: `_op_type = 'd' AND to_date(_cdc_timestamp) = current_date()`.

## You can now…
- choose between CDC and full reload for a source;
- explain what a replication slot costs and why paused or forgotten slots are dangerous;
- schedule a job with a cron expression, read a failed run, and clean up job, source,
  destination and tables in the right order.
