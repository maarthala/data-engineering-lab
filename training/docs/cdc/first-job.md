# Your first sync job

## Concept
A sync in **📡 Ingestion** has three parts. You set up each one once:

| Part | What you tell it | Example |
|---|---|---|
| **Source** | the database to read from: type, host, port, database, login | Lab shop (live) |
| **Destination** | where the tables land: your lakehouse and a **namespace** | `<you>_lake` · `shop_cdc` |
| **Job** | which tables, the **mode**, the **schedule** | 4 tables · Full load + CDC · Every 15 min |

Each run of the job is one OLake container: it reads, writes the Iceberg tables as **you**, and
stops. There are no keys to type: the lab gives the job your own lakehouse login.

## Lab
Open **Tools → 📡 Ingestion**. The page has three tabs: **Jobs**, **Sources**,
**Destinations**.

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

### 3 · The job: a four-step guide
Go to **Jobs → + New job**. The guide walks you through four steps. **Next →** unlocks once
a step is filled in, and **← Back** (or a finished step at the top) takes you back.

1. **① Which database do you copy from?** Pick **Lab shop (live)**. The card shows whether
   CDC is on and ready.
2. **② Where do the tables land?** Pick **Shop CDC**.
3. **③ Which tables?** The guide lists the source's tables by itself. Click **Select all**:
   `public.customers`, `public.order_items`, `public.orders`, `public.products`.
4. **④ How often, and a last look:**
    - **Job name**: `Shop to lake`
    - **Mode**: **Full load + CDC** (the third choice, *every change (history)*, comes in
      [Run it for real](operate.md)). The first run copies everything, later runs only the
      inserts, updates and deletes since the run before.
    - **Schedule**: **Manual** for now (you'll switch it on in the last lesson).
    - Check the summary (From / To / Tables) and click **Create & run**.

!!! note "One CDC job per source"
    A source has one change stream (one slot), so only **one** job can read it with CDC. If you
    try a second one, step ① says *CDC used by job …: full reloads only*. So choose all the
    tables you need in that one job. To change its tables later, delete the job and create it
    again (its first run copies everything once more).

### 4 · Watch the first run
You land on the job's page: **Last run**, **From** / **To**, the tables, mode and schedule,
**Run now** / **Pause schedule** / **Delete job**, and the **Runs** table. The first run takes
about 15–30 seconds:

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

## Check yourself
1. Why can't you use `lab-db` as the database name?
2. The second run copied 29 records. What would a **Full reload** job have copied?
3. You want a second job that copies only `products` from the same shop with CDC. Why does the
   guide refuse, and what can you do instead?

??? success "Answers"
    1. `lab-db` is the server (host). The database on that server is called `shop`.
    2. Every row of the chosen tables again, about 1,900 or more, on every run.
    3. The source's change stream is already read by `Shop to lake` (one slot, one reader).
       Recreate `Shop to lake` with every table you need, or make the second job a
       **Full reload**.

## You can now…
- add a source, check it is ready for CDC, and know what the check creates;
- add a destination and predict the full name of each landed table;
- create a job with the four-step guide and read its runs.
