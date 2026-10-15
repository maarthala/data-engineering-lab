# Your lab account & workspace

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
| 👥 Team | `general` at first | the team you work in, and its lakehouse `team_<team>` — see [Your team](#your-team) |

The top bar of the landing page shows it: **"your lakehouse: `<you>_lake` ✓ · bucket:
`<you>-lake`"**, plus **📁 My files**, **🗂️ My catalogs**, **🔑 My secrets** and **Log out**. Below it, the
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

So in Jupyter, the Unit {unit:unit4/fundamentals.md} lessons build `iceberg.bronze/silver/gold` **in your own lakehouse** —
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
│   └── source/                   raw files to ingest — lesson {lesson:unit4/read-bronze.md} exports ShopFlow here
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

## Your team

Every member is in **one team**, picked by a manager (new members start in `general`). The team
has its own lakehouse, **`team_<team>`**, with `bronze` / `silver` / `gold`, and **everyone in the
team reads and writes it**. Your own lakehouse stays yours; the team's is where you put tables the
whole team works on. The top bar shows it: *"· team: sales (team_sales)"*.

| Where you run it | Your team's lakehouse is |
|---|---|
| **Jupyter / Spark** (`spark.sql`, `%%sql`) | `team.…`, e.g. `team.bronze.customers` |
| **SQLPad, Superset** | its real name, e.g. `team_sales.bronze.customers` |

Jupyter sets up `team` when your server starts and says so in the first cell's output:

```text
✓ team = your team's lakehouse (team_sales) — e.g. team.bronze.customers
```

Make a table there and your teammates see it straight away:

```python
df.writeTo("team.bronze.customers").createOrReplace()
spark.sql("SELECT * FROM team.bronze.customers ORDER BY id").show()
```
```text
+---+--------+-------+
| id|    name|country|
+---+--------+-------+
|  1|     Ada|     UK|
|  2|Surendra|     FI|
+---+--------+-------+
```

- **`team`** — like `iceberg` (your lakehouse) and `shared` (the course's lake), a name Jupyter
  gives a catalog; behind it is `team_<team>` in Polaris.

The team also has a **bucket**, `team-<team>-lake` (limit **500 MB**): files everyone in the team
sees, uploads and changes. Open it with **📁 My files → 👥 Team** (or **🪣 Mine** for your own
bucket), in Jupyter's file browser as *team bucket (team-sales-lake)*, and in the RustFS console
next to your own bucket. Put files the team needs in `files/source/`, and read them in Spark by
their path:

```python
spark.read.option("header", True).csv("s3a://team-sales-lake/files/source/customers.csv").show()
```
```text
+---+----+
| id|name|
+---+----+
|  1| Ada|
+---+----+
```
- If a manager **moves** you to another team, you lose the old team's lakehouse and bucket and get
  the new ones (in Jupyter after your next server start). The tables and files you made stay with
  the old team.
- A team lakehouse isn't shared from **My catalogs**: being in the team is the access.

## My catalogs — create and share

**🗂️ My catalogs** (landing page) lists the catalogs you own and the ones shared with you:

- **＋ New** creates a catalog `<you>_<name>`, stored in your bucket under `tables/<you>_<name>/`.
- **Share** a namespace or a single table with another member — **read** or **write** — and
  **revoke** it again. Polaris enforces it: they see exactly what you shared.
- **Open in Polaris Console ↗** opens the catalog in the Console, signed in as you — browse
  it, add namespaces and tables. (The Console's own **Catalogs** list shows `0` for members:
  listing *all* catalogs is admin-only, so open yours from here.)

## My secrets — keys and passwords in a key vault

Some code needs a value you must **not** write into a notebook: an API token, a password, the key
for an outside S3 bucket. A notebook gets shared and committed to git; whoever reads it reads the
key. Teams keep such values in a **key vault**: a service that stores them encrypted, gives each
person access to their own, and hands a value to code that asks for it **by name**. The lab runs
one (**OpenBao**, the open-source fork of HashiCorp Vault), and **🔑 My secrets** (top bar) is your
folder in it.

1. Type a **name** (letters, digits and `_`, e.g. `API_TOKEN`) and a **value**.
2. Leave **secret** ticked for keys and passwords: the value is never shown again, only replaced.
   Untick it for plain settings (e.g. `REGION = eu-west`), and the page shows the value.
3. **Save.** Only you can read it: your Jupyter server gets a vault login that reaches your folder
   and nothing else, and a new one at every start.

In a notebook, ask for a secret when you need it:

```python
token = key_vault("API_TOKEN")           # the value, as a string
key_vault_list()                         # the names of yours, never the values
```
```text
['API_TOKEN', 'REGION']
```

- **`key_vault(name)`** — reads the value from the vault **now**: change it on the page and the
  next call returns the new value, no restart. A name you don't have fails with
  `KeyError: "no secret 'NOPE' — add it on 🔑 My secrets (key_vault_list() lists yours)"`, which is
  what you want for a key the code can't work without.
- The value is never printed, and it isn't in the notebook file: only the **name** is. Share the
  notebook and nobody gets your key.
- The first names of these helpers still work: `lab_secret`, `lab_secrets` and `lab_secret_set`
  are the same functions.

You can also save one from code:

```python
key_vault_set("DB_PASSWORD", "pw-42")
```
```text
✓ secret DB_PASSWORD saved
```

In a **DAG** it's the same call, imported from the lab's helpers
([Airflow basics](../unit5/basics.md)); it reads the secrets of the member whose DAG it is:

```python
@task
def fetch():
    from lab_spark import key_vault
    key = key_vault("API_TOKEN")
```

### In the key vault's own UI

The bottom of **🔑 My secrets** has **Open the key vault ↗**: the vault's own web UI (OpenBao), at
[http://localhost:8200](http://localhost:8200) locally, `http://vault.de.lan` on the lab cluster.
Click **Sign in with OIDC Provider** and you're in with your lab account (the same Keycloak login),
straight in **your folder** `secret/<you>/`: one entry per secret, the same ones the page shows.

- Open a secret: its keys with the values hidden (`***********`), **Version 1**, and **Create new
  version** to change it (the vault keeps the old versions). One saved on the page has three keys:
  `value` (the secret itself) and the page's own `secret` and `updated`.
- **Create Secret** adds one. A secret in the vault is a set of named **fields** (the JSON toggle
  is the same thing as JSON), so one secret can hold a whole login, e.g. `db` with `username` =
  `demo` and `password` = `pw-42`. Read it by field, or all at once:

    ```python
    key_vault("db", "password")      # one field
    key_vault_dict("db")             # every field, as a dict
    ```
    ```text
    'pw-42'
    {'password': 'pw-42', 'username': 'demo'}
    ```

    A field it doesn't have fails with
    `KeyError: "secret 'db' has no field 'host' — its fields: password, username"`. A secret
    saved on the page has one field, `value`: `key_vault("NAME")` is its value,
    `key_vault_dict("NAME")` is `{'value': '...'}`. That's the usual vault practice: one secret
    per login or connection, and code asks for the field it needs.
- Only your folder: another member's folder, or the list of all folders, shows nothing (just a
  *View secret* box), because the vault refuses. The UI can't show an empty folder, so while yours
  is empty the link opens **Create secret** with `<you>/` filled in: type the name after the `/`.

!!! note "Coming from My variables?"
    The lab used to load these into `os.environ` ("My variables"). Your values moved into the
    vault on their own; read them with `key_vault("NAME")` instead of `os.environ["NAME"]`. Why
    the change: anything in `os.environ` is visible to every library in the notebook and shows up
    in a `print(os.environ)`; a vault hands out a value only when asked.

!!! note "On the cloud platforms"
    Same idea, other names: **Azure Key Vault** (`SecretClient(...).get_secret("api-token").value`),
    **Databricks secrets** (`dbutils.secrets.get("scope", "api_token")`), **AWS Secrets Manager**,
    **Snowflake secrets**. The value lives in the vault; code asks for it by name.

## Starting over

Broke your lakehouse, or want a clean slate for the next unit? Ask your **platform manager**
(on your laptop stack, that's you: sign in as `manager`). On the landing page they have
**🛠️ Manage**, whose **👥 Members** page has two resets:

| | What goes | What stays |
|---|---|---|
| **Reset lakehouse** | every table and view in your catalogs (bronze / silver / gold and the sample table come back) | your files, notebooks, DAGs, catalogs and shares |
| **Wipe** | everything: tables, extra catalogs, the shares you gave, your bucket, your Jupyter files, your secrets | your account; you're back to a new member's setup (lakehouse, sample table, starter files) |

The same page lets managers create members (in the team they pick), give someone a new password
and delete an account. **🧩 Teams** lists the teams on the left; pick one to see its members on the right,
add members (search by name, pick several; one who is in another team moves) or remove them (back
to the default team). It also creates teams and deletes empty ones. **⚙️ Settings** switches **self-registration** on or off (on while a new batch
signs up, off otherwise).

## You can now…
- Register, sign in once and reach every tool with your lab account
- Tell your own lakehouse (`iceberg` in Jupyter) from the shared lake (`shared` in Jupyter, `iceberg` in Trino)
- Keep notebooks and DAGs in `notebooks/` and `dags/`, and name DAGs `<you>_…`
- Work in your team's lakehouse (`team` in Jupyter, `team_<team>` in SQLPad)
- Create a catalog and share a namespace or table with another member
- Keep keys and passwords in **My secrets** (the key vault) and read them with `key_vault()`
- Know when to ask a manager for a lakehouse reset or a full wipe
