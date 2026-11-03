# Your first sync job

## Concept
A sync in **📡 Ingestion** has three parts. You set up each one once:

| Part | What you tell it | Example |
|---|---|---|
| **Source** | the database to read from: type, host, port, database, login | Lab shop (live) |
| **Destination** | where the tables land: your lakehouse and a **namespace** | `<you>_lake` · `shop_cdc` |
| **Job** | which tables, the **mode**, and how it **runs** | 4 tables · Full load + CDC · Manual |

The work is done by one of two **engines**, depending on the stack:

- **OLake** — each run is one OLake container: it reads, writes the Iceberg tables as **you**, and
  stops.
- **Debezium + Kafka** (the laptop stack) — the job is two always-there *connectors* (a reader
  and a writer) that a run wakes up and puts back to sleep; they can also run all the time
  ([Live data](datastream.md)).

A job's page shows which one it uses (**Engine**). Either way there are no keys to type: the lab
gives the job your own lakehouse login.

## Lab
Open **Tools → 📡 Ingestion**. The page has three tabs, in the order you fill them:
**Sources**, **Destinations**, **Jobs** — each a table, with **+ New …** on the right.

### 1 · The source
Look under **Sources**. Every member starts with **Lab shop (live)**, already filled in with the
settings from [the previous lesson](live-shop.md). If you deleted it, add it again with
**+ New source**:

| Field | Value |
|---|---|
| Database type | PostgreSQL |
| Name | Lab shop (live) |
| Host · Port | `lab-db` · `5432` |
| Database | `shop` |
| User · Password | `reader` · `reader` |
| Schemas | `public` |
| Change data capture | on |

**Test connection** checks the login before you save. **Browse tables** lists what it can
read. Then **Save source**.

Now click **Check** on the source. The check looks at what CDC needs and fixes what it can:

- `wal_level` is `logical` ✓
- the login may replicate ✓
- the publication `olake_pub` exists ✓
- the replication slot: the first check **creates** it, e.g. *made replication slot
  demouser_bf4bfd71*

That slot is your bookmark from [What is CDC](what-is-cdc.md). Run the `pg_replication_slots`
query from the last lesson again and you'll see it.

!!! note "With Debezium"
    The check shows its two prerequisites as ✓ (`wal_level` logical, a login with REPLICATION)
    and makes the publication if it's missing. The **slot** is made by the job's first run, not
    by the check: Debezium creates it itself, at the exact point its full load starts.

!!! tip "Common source errors"
    | Message | Meaning |
    |---|---|
    | `password authentication failed for user "reader"` | wrong password, or your browser filled in your portal login. Type `reader` again. |
    | `database "lab-db" does not exist` | `lab-db` is the **host**. The database is `shop`. |
    | `wal_level is not 'logical'` | this database can't do CDC. Use **Full reload** for it, or ask its owner to turn it on. |

### 2 · The destination
Under **Destinations**, click **+ New destination**:

| Field | Value |
|---|---|
| Name | Shop CDC |
| Lakehouse | `<you>_lake` (your main lakehouse, `my` in notebooks) |
| Namespace | `shop_cdc` |

**Save destination**. The check makes sure the job can write there. Each table will land as
`<lakehouse>.<namespace>.<table>`, here `my.shop_cdc.orders` in a notebook.

### 3 · The job: one form
Go to **Jobs → + New job**. One form, top to bottom:

1. **Source**: pick **Lab shop (live)** in the dropdown (**+ New** next to it adds one and brings
   you back). The line under it says whether CDC is on and checked.
2. **Destination**: pick **Shop CDC**. The line under it shows where the tables will land.
3. **Tables**: the form lists the source's tables by itself, each with its **key** (primary key).
   Click **All**: `public.customers`, `public.order_items`, `public.orders`, `public.products`.
4. **Mode**: **Full load + CDC (merge by key)**. The first run copies everything, later runs only
   the inserts, updates and deletes since the run before, merged into the table by its key. (The
   other choice, *every change (history)*, comes in [Run it for real](operate.md).)
5. **Runs**: **Manual** for now — the other two, **Cron** and **Datastream**, come in the last
   lessons.
6. **Job name**: `Shop to lake` (filled in from the source and destination; change it if you like)
   → **Create & run**.

!!! note "One CDC job per source"
    A source has one change stream (one slot), so only **one** job can read it with CDC. If you
    pick a source that's already read, the form says *CDC on … is used by job …* and offers no
    CDC mode. So choose all the tables you need in that one job. To change its tables later,
    delete the job and create it again (its first run copies everything once more).

### 4 · Watch the first run
You land on the job's page: its status at the top with **▶ Run now** and **Delete**; **From** /
**To**, the tables, mode, how it runs and the engine; a **How it runs** card; and the **Runs**
table. The page keeps itself up to date — no need to reload. The first run takes about 15–30
seconds:

| Started | Status | Records | Trigger |
|---|---|---|---|
| 16:15 | ok | 1891 | first run |

**Records** counts the rows it copied: here every row of the four tables (the snapshot).
**Log** shows OLake's own output if something goes wrong.

Wait a minute, then click **Run now**:

| Started | Status | Records | Trigger |
|---|---|---|---|
| 16:17 | ok | 29 | run now |
| 16:15 | ok | 1891 | first run |

Only **29** this time: the changes the shop made in that minute, not the whole tables again.
That's CDC paying off. On a real database the difference is millions of rows against a few
thousand.

!!! note "With Debezium"
    The same two runs on the laptop stack (the shop had grown by then):

    | Started | Took | Result | Rows | Why |
    |---|---|---|---|---|
    | 10:06 | 49 s | ✓ ok | 15 | run now |
    | 10:04 | 36 s | ✓ ok | 7542 | first run |

    A Debezium run takes a little longer: it wakes the connectors, waits until the reader has
    caught up with where the shop was when the run began and the writer has saved it all to
    Iceberg, then puts them back to sleep. Its **Log** says exactly that:

    ```text
    10:06:14 source lab-db/shop at WAL 266840408; topics hold 7542 changes
    10:06:14 resumed the connectors
    10:06:45 read the source up to the run's start (13 changes)
    10:07:03 the sink has committed everything to Iceberg
    10:07:03 paused the connectors
    ```

    followed by Kafka Connect's own lines about the job's two connectors.

## Check yourself
1. Why can't you use `lab-db` as the database name?
2. The second run copied 29 records. What would a **Full reload** job have copied?
3. You want a second job that copies only `products` from the same shop with CDC. Why does the
   form refuse, and what can you do instead?

??? success "Answers"
    1. `lab-db` is the server (host). The database on that server is called `shop`.
    2. Every row of the chosen tables again, about 1,900 or more, on every run.
    3. The source's change stream is already read by `Shop to lake` (one slot, one reader).
       Recreate `Shop to lake` with every table you need, or make the second job a
       **Full reload**.

## You can now…
- add a source, check it is ready for CDC, and know what the check creates;
- add a destination and predict the full name of each landed table;
- create a job with the New job form and read its runs (and its log).
