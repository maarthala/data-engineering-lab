# 📡 Ingestion

**📡 Ingestion** copies data into your lakehouse: tables from a database you can reach, or the events
of one of your [📨 Pub/Sub streams](pubsub.md). You set up three things once: a **source** (where the
data comes from), a **destination** (a lakehouse and a namespace) and a **job** (which tables, how, and
when). The lab does the work as you: there are no keys to type. Lessons:
[What is change data capture (CDC)?](../cdc/what-is-cdc.md) and the ones after it.

Open **Tools → Ingestion**. The page has three tabs, **Sources**, **Destinations** and **Jobs**, each
with its **+ New …** button on the right. Every member starts with a source **Lab shop (live)** (the
lab's live shop database, see [Meet the live shop](../cdc/live-shop.md)) and a destination
**My lakehouse** (your lakehouse, namespace `cdc`). You can have up to 10 of each.

## Sources
**Sources → + New source**, then pick **Source: 🗄 Database** or **📨 Pub/Sub stream**.

**🗄 Database** — fill in the form:

| Field | What to type |
|---|---|
| **Type** | *PostgreSQL*; with the OLake engine also *MySQL* and *SQL Server* |
| **Name** | your name for it, e.g. *Shop database* |
| **Host**, **Port**, **Database** | where the database is, e.g. host `lab-db` |
| **Schemas** | (PostgreSQL) empty for all, or e.g. `public, sales` |
| **User**, **Password** | the login; the password is kept in the lab's vault and never shown again |
| **CDC** | **On — inserts, updates, deletes** or **Off** (full reloads only). The hint under it says what the database needs for CDC |

**Test connection** tries the login, **Browse tables** lists what the job could copy, **Save source**
saves it and the lab checks it in the background. A Postgres source with CDC on gets a publication and
a replication slot at its first check.

**📨 Pub/Sub stream** — pick one of your streams in **Stream** and press **Save source**. (No stream
yet? Make one on 📨 Pub/Sub first.) You can also start from the stream's page:
**🏞 Lakehouse → + New Ingestion job**.

In the **Sources** list, each source has **Check** (test it again), **Tables**, **Edit** and **Delete**.
Deleting a database source with CDC on drops its replication slot too; removing a stream source leaves
the stream on 📨 Pub/Sub.

## Destinations
**Destinations → + New destination**:

| Field | What it means |
|---|---|
| **Name** | your name for it, e.g. *Shop Bronze* |
| **Lakehouse** | one of your catalogs |
| **Namespace** | one it already has, **+ New namespace…** (made when you save), or **— none** (a namespace `<database>_<schema>` per source; Pub/Sub: `events`) |

Each table lands as `<lakehouse>.<namespace>.<table>`. Two jobs can't write the same table into one
namespace. Deleting a destination leaves its tables in your lakehouse.

## Jobs
**Jobs → + New job** is one form:

| Field | What to choose |
|---|---|
| **Source** | a source, or **+ New** to add one on the way |
| **Destination** | a destination, or **+ New** |
| **Tables** | tick the tables (**All**, **None**, **↻ Reload**); each shows its columns and key. A stream has one **Table**, named after the stream |
| **Mode** | **Full load + CDC (merge by key)**: one row per key, the latest · **Full load + every change (history)**: every change as a new row · **Full reload every run** (OLake only) · **Every event as a new row** (streams) |
| **Runs** | **Manual** (when you press **Run now**) · **Cron** (a cron expression in UTC, or pick from **Presets…**) · **Datastream** (always on, changes ~10–30 s behind; Debezium and streams only) |
| **Job name** | filled in from the source and destination; change it if you like |

**Create & run** (or **Create & start stream** for a Datastream) saves the job and starts it. A
source's change stream feeds one job: a second CDC job on the same source isn't offered.

## The job page
Click a job's name (or **Open**). At the top: its status and the buttons.

| Button | What it does |
|---|---|
| **▶ Run now** | one run (Manual and Cron jobs) |
| **Pause schedule** / **Resume schedule** | stop or restart a Cron job's runs |
| **■ Stop stream** / **▶ Start stream** | a Datastream job |
| **Delete** | asks **Yes, delete** / **Cancel**; the job's tables stay in your lakehouse |

Below: **From**, **To**, **Tables**, **Mode**, **Runs** and **Engine**; **How it runs** (change
Manual / Cron / Datastream and **Save**); for a Datastream its **Log**, updated every few seconds; and
**Runs**: **Started**, **Took**, **Result**, **Rows**, **Why** (what started it) and a **Log** button per run.

The **Status** column (list and job page) shows **not run yet**, **… starting**, **running**,
**✓ ok** (with the rows), **✗ failed** (hover for the error), **streaming** or **■ stopped**.

## The engine
A job's page shows its **Engine**. Which one new jobs use is set for the whole lab:

| Engine | How a job runs |
|---|---|
| **Debezium + Kafka** (what the lab uses now) | two connectors in Kafka Connect, a reader and a writer; a run wakes them and puts them back to sleep, a Datastream keeps them running. PostgreSQL sources only |
| **OLake** | each run is one OLake container that reads, writes the Iceberg tables and stops. PostgreSQL, MySQL, SQL Server; no Datastream |
| **Kafka Connect (Iceberg sink)** | jobs on a 📨 Pub/Sub stream |

A job keeps the engine it was made with.

## "Streaming is off"
Debezium jobs need the **Streaming** unit (Kafka and Kafka Connect). While it's off, a yellow
**⏸ Streaming is off** card is on every screen: Datastream jobs wait, **Run now** and Cron runs fail.
Sources and destinations work as usual. A lab manager switches it on: on a laptop
`make status` → Streaming → **s**; on a team cluster **Manage → ⚙️ Settings → Services → Streaming**
(see [⚙️ Settings](settings.md)).

## Related lessons
- [What is change data capture (CDC)?](../cdc/what-is-cdc.md)
- [Meet the live shop](../cdc/live-shop.md)
- [Your first sync job](../cdc/first-job.md)
- [Read the changes in the lake](../cdc/read-changes.md)
- [From changes to Silver and Gold](../cdc/silver-gold.md)
- [Live data: Datastream (Debezium + Kafka)](../cdc/datastream.md)
- [Run it for real: schedules, costs and clean-up](../cdc/operate.md)
- [Land events in Bronze](../pubsub/bronze.md)
