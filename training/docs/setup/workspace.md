# 0.3 Your lab account & workspace

You sign in **once** — with your **lab account** — and every tool knows who you are. On your first
sign-in the lab also builds you a private **workspace**: your own lakehouse, your own storage
bucket and your own Jupyter. This page shows what you get and where to find it.

## Your lab account

Open the landing page — [http://localhost:8000](http://localhost:8000) locally, `http://de.lan` on
Kubernetes. You land on the **Epireum's Data Engineering Lab** sign-in page:

- **On your own laptop (local stack):** sign in as **`demouser` / `demouser`** — the default lab
  account. **Every example in the course is written for `demouser`** (`demouser_lake`,
  `demouser-lake`, …), so you can copy and run them as they are. A second account,
  **`learner2` / `learner2`**, is there for the lessons that need *another user* (sharing in
  [6.1](../unit6/polaris.md)) — sign in with it in a private window.
- **Want your own account** (or on a shared/class stack, where everyone needs their own)? Click
  **Register** and choose a **username** (3–30 lower-case letters/digits) and a password — no email
  needed. Then, wherever an example says `demouser`, use your username instead (`ravi_lake`,
  `ravi-lake`, …). No **Register** link? Your manager has registration switched off; they turn it
  on while a new batch signs up, or create your account for you.
- **Managing the team?** Sign in as `manager` (managers are admins in every tool).

!!! note "Shared stack? Don't share `demouser`"
    On a stack the whole class uses (e.g. Kubernetes), everyone registers their own account —
    `demouser` would mean one shared lakehouse, bucket and Jupyter for everybody. That's why
    `demouser` only exists on the local stack.

That one account signs you in to **every** tool — Jupyter, Airflow, Superset, SQLPad, the Polaris
Console and the RustFS console. Open a tool from the landing page and you're straight in; you won't
be asked for your password again while you're signed in. (Opened some other way, SQLPad and the
RustFS console may show a **"Sign in with Epireum lab account"** button: click it.) **Log out** on the
landing page ends your lab-account session (Jupyter follows within a minute); a tool you still have
open — Superset, Airflow, SQLPad — keeps its own session until you log out there or it expires.

## What you get on first sign-in

| Yours | Name | What it is |
|---|---|---|
| 🏠 Lakehouse | `<you>_lake` | your own Polaris catalog with `bronze` / `silver` / `gold` — where *your* Spark tables go |
| 🧪 Sample table | `<you>_lake.bronze.sample_orders` | 1,000 ShopFlow orders, ready to query from day one (SQLPad, Trino, Superset, `%%sql`) |
| 📄 Practice files | `files/source/shopflow/*.csv` | small, messy customers / orders / products CSVs to load and clean |
| 🪣 Bucket | `<you>-lake` | your own object storage (RustFS, **100 MB** limit) — your files, notebooks and DAGs |
| 📓 Jupyter | your own server | your notebooks, with `spark` ready and `iceberg` = your lakehouse |
| 🧰 SQLPad user | `<you>` | your saved queries |

The top bar of the landing page shows it: **"your lakehouse: `<you>_lake` ✓ · bucket:
`<you>-lake`"**, plus **📁 My files**, **🗂️ My catalogs**, **🔑 My variables** and **Log out**. Below it, the
tools are grouped in four columns — *Build & explore* (Notebook, Compute, SQL workbench),
*Pipelines* (Airflow), *Visualize* (Superset) and *Lakehouse* (My files, My catalogs, Storage).

## Which lake am I querying?

There are two kinds of lake — **yours** and the course's **shared** one (read-only for members,
with the ready-made ShopFlow tables such as `gold.daily_sales`):

| Where you run it | `iceberg.…` means | The shared lake is |
|---|---|---|
| **Jupyter / Spark** (`spark.sql`, `%%sql`) | **your** lakehouse `<you>_lake` | `shared.…` — e.g. `shared.gold.daily_sales` |
| **Trino, SQLPad, Superset** | the **shared** lake | `iceberg.…` |

```sql
%%sql
SELECT * FROM shared.gold.daily_sales LIMIT 10   -- the course's ready-made Gold table
```

(*Table not found*? The shared lake hasn't been filled yet — whoever runs the stack starts the
`shopflow_medallion` DAG once, see [0.2](deploy.md#fill-the-shared-lake-once).)

So in Jupyter, the Unit 4 lessons build `iceberg.bronze/silver/gold` **in your own lakehouse** —
nobody else's tables get in your way, and you can't break theirs.

!!! tip "Another catalog in Spark"
    `use_catalog("<catalog>")` makes any other catalog you may use available in Spark — one you
    created on **My catalogs**, or one another member shared with you:
    `use_catalog("kiran_sales")` → `SELECT * FROM kiran_sales.sales.orders`.

## Your files — notebooks, DAGs, data

Your bucket is organised like an Azure / Fabric lakehouse — **Files** and **Tables**:

```
<you>-lake/                       (demouser-lake for the default account)
├── files/                        files you work with
│   ├── src/
│   │   ├── notebooks/            = Jupyter's notebooks/ folder (kept in sync)
│   │   ├── dags/                 = Jupyter's dags/ folder (kept in sync) → Airflow
│   │   └── dbt/                  = Jupyter's dbt/ folder (kept in sync)
│   └── source/                   raw files to ingest — lesson 4.2 exports ShopFlow here
└── tables/                       table storage of your catalogs — tables/<catalog>/<namespace>/<table>/
```

**Tables** is managed by the catalog: create and drop tables with SQL / Spark, don't edit files
there. **Files** is yours to use freely.

In Jupyter's **file browser** you start with three folders, **mirrored to `files/src/`**: every
save, rename and delete in them is copied to your bucket straight away.

| Jupyter folder | In your bucket | For |
|---|---|---|
| `notebooks/` | `files/src/notebooks/` | your notebooks (`.ipynb`) |
| `dags/` | `files/src/dags/` | your Airflow DAGs (`.py`) — **Airflow picks them up in ~30 s** |
| `dbt/` | `files/src/dbt/` | your dbt projects — dbt's generated `target/`, `logs/` and `dbt_packages/` stay in Jupyter only |

Files you upload elsewhere (My files, the RustFS console) appear in Jupyter the next time your
server starts (**File → Hub Control Panel → Stop My Server → Start**). Anything outside these three
folders stays in your Jupyter workspace only.

**📁 My files** (landing page) is a file manager for your bucket — browse, upload, download, new
folder, delete. The **RustFS console** (Storage tile) shows the same bucket. (Its bucket
**Settings** page is for managers — members get *Access Denied* there; everything about
your files works in the browser view and My files.)

!!! warning "100 MB per member"
    Your bucket holds at most **100 MB** — plenty for code and lesson data. If it's full, saving
    shows an error ("saved in Jupyter, but NOT in your bucket"); delete files you no longer need.

## Your own Airflow DAGs

Save a `.py` file in `dags/` — Airflow (one Airflow shared by the whole class) loads it within
about 30 seconds. One rule: the **`dag_id` must start with your username and `_`**, so names never
clash:

```python
from datetime import datetime
from airflow.sdk import dag, task

@dag(dag_id="ravi_hello", start_date=datetime(2026, 1, 1), schedule=None)   # ravi = your username
def hello():
    @task
    def hi():
        print("hello from my bucket")
    hi()

hello()
```

A DAG that breaks the rule (or has an error) shows up under Airflow's **import errors** with the
reason — e.g. *"DAG id 'hello' must start with 'ravi_'"*. Everyone can *see* all DAGs; the prefix
tells you whose is whose.

## My catalogs — create and share

**🗂️ My catalogs** (landing page) lists the catalogs you own and the ones shared with you:

- **＋ New** creates a catalog `<you>_<name>`, stored in your bucket under `tables/<you>_<name>/`.
- **Share** a namespace or a single table with another member — **read** or **write** — and
  **revoke** it again. Polaris enforces it: they see exactly what you shared.
- **Open in Polaris Console ↗** opens the catalog in the Console, signed in as you — browse
  it, add namespaces and tables. (The Console's own **Catalogs** list shows `0` for members:
  listing *all* catalogs is admin-only, so open yours from here.)

## My variables — keys and settings for your notebooks

Some code needs a value you must **not** write into a notebook: an API token, a password, the key
for an outside S3 bucket. Put those on **🔑 My variables** (top bar). Every notebook then finds them
in `os.environ`, the dictionary of environment variables Python gives every program.

1. Type a **name** (letters, digits and `_`, e.g. `API_TOKEN`) and a **value**.
2. Leave **secret** ticked for keys and passwords: the value is never shown again, only replaced.
   Untick it for plain settings (e.g. `REGION = eu-west`), and the page shows the value.
3. **Save.** The variables live in your own bucket (`.lab/env.json`), and only you can read them.

A notebook loads them when its kernel starts. The startup lines then include:

```text
✓ 2 variable(s) from My variables in os.environ — lab_env() reloads them
```

Read them with `os.environ`:

```python
import os
token = os.environ["API_TOKEN"]          # KeyError if the variable doesn't exist
region = os.environ.get("REGION", "eu-west")   # .get(name, default) — no error, default instead
```

- **`os.environ["API_TOKEN"]`** — the value as a string. It fails with `KeyError` when the name is
  missing, which is what you want for a key the code can't work without.
- **`os.environ.get("REGION", "eu-west")`** — returns the default when the variable isn't set.

Added or changed a variable while a notebook is open? Run **`lab_env()`** to reload, with no
kernel restart:

```python
lab_env()
```
```text
✓ 2 variable(s) from My variables: API_TOKEN, REGION
```

It prints the **names** only, never the values. Names the lab sets itself (`AWS_*`, `LAKE_*`,
`SPARK_*`, `GIT_*`, `PATH` …) are refused, e.g. *"AWS_REGION is set by the lab itself — choose
another name"*, so you can't break your own login by accident.

!!! note "On the cloud platforms"
    The same idea is **Databricks secrets** (`dbutils.secrets.get`), **Azure Key Vault**, and
    **Snowflake secrets**: the value lives in a store, and code asks for it by name.

## Starting over

Broke your lakehouse, or want a clean slate for the next unit? Ask your **platform manager**
(on your laptop stack, that's you: sign in as `manager`). On the landing page they have
**👥 Manage members**, with two resets:

| | What goes | What stays |
|---|---|---|
| **Reset lakehouse** | every table and view in your catalogs (bronze / silver / gold and the sample table come back) | your files, notebooks, DAGs, catalogs and shares |
| **Wipe** | everything: tables, extra catalogs, the shares you gave, your bucket, your Jupyter files | your account; you're back to a new member's setup (lakehouse, sample table, starter files) |

The same page lets managers create members, give someone a new password, delete an account, and
switch **self-registration** on or off (on while a new batch signs up, off otherwise).

## You can now…
- Register, sign in once and reach every tool with your lab account
- Tell your own lakehouse (`iceberg` in Jupyter) from the shared lake (`shared` in Jupyter, `iceberg` in Trino)
- Keep notebooks and DAGs in `notebooks/` and `dags/`, and name DAGs `<you>_…`
- Create a catalog and share a namespace or table with another member
- Keep keys and settings on My variables and read them with `os.environ`
- Know when to ask a manager for a lakehouse reset or a full wipe
