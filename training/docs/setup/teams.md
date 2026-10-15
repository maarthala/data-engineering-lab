# Working in a team

Every member is in **one team**. A manager picks it (new members start in `general`). The team has
its own files, lakehouse, secrets and DAGs. **Everyone in the team reads and writes them**, and
nobody outside the team sees them. Your own workspace stays yours: use the team's for work you
share.

The top bar shows your team, e.g. *"· team: sales (team_sales)"*. Below, replace `sales` with your
team's name.

| What | Yours | Your team's |
|---|---|---|
| Files (bucket) | `demouser-lake` | `team-sales-lake` (500 MB) |
| Jupyter folder | `my_files/` | `sales_team/` |
| Lakehouse (catalog) | `demouser_lake` (`iceberg`) | `team_sales` (`team`) |
| Secrets | `secret/demouser/` | `secret/sales_team/` |
| SQLPad connection | Lakehouse (Trino) — demouser | Team sales (Trino) — demouser |
| DAG names | `demouser_…` | `team_sales_…` |

## Files: mine and the team's

- **📁 My files** has a switch at the top: **🪣 Mine** is your bucket, **👥 Team** is the team's.
  Both have the same folders: `files/source/` for data files (CSV, Parquet …) and
  `files/src/{notebooks,dags,dbt}/` for code.
- **In Jupyter** you see two folders, each with `notebooks/`, `dags/` and `dbt/`:
    - `my_files/`: yours, saved to your bucket
    - `sales_team/`: the team's, saved to the team bucket. Opening a folder there brings in your
      teammates' latest changes. If two people edit the same file at once, the last save wins, so
      agree who edits what.
- **In Spark**, read a team file by its path:
  `spark.read.csv("s3a://team-sales-lake/files/source/customers.csv", header=True)`.
- The **RustFS console** shows the team bucket next to your own.

## The team's lakehouse

The team catalog **`team_sales`** has `bronze`, `silver` and `gold`, like your own.

- **Jupyter / Spark:** `team.…` or `team_sales.…`, e.g. `spark.table("team.bronze.customers")`.
  (`iceberg.…` is still your own lakehouse.)
- **SQLPad / Superset:** `team_sales.…`, e.g. `SELECT * FROM team_sales.bronze.customers`.
  In SQLPad, pick the connection **Team sales (Trino) — demouser** to see the team's tables in the
  schema browser on the left. A schema shows up there once it holds a table.
- A table one teammate makes, everyone in the team sees and can change straight away.

## The team's secrets

Keys and passwords the whole team uses (an API token, a database password) go in the team's
folder of the key vault, **`secret/sales_team/`**.

- **🔑 My secrets → 👥 Team** lists, adds and changes them (values stay hidden).
- **In a notebook**, add `team=True`:
  `key_vault_set("API_TOKEN", "…", team=True)`, `key_vault_list(team=True)`,
  `key_vault("API_TOKEN", team=True)`.
- **In the key vault's own UI**, the team folder sits next to yours.

## The team's DAGs

- Save a DAG in **`sales_team/dags/`**. It shows up in Airflow within about 30 seconds for
  **everyone in the team**: each of you can switch it on, trigger it and read its logs.
- Its `dag_id` must start with **`team_sales_`**, e.g. `team_sales_daily`.
- It runs **as the team**, not as whoever started it: in its tasks, `lab_spark()` gives `iceberg` =
  the team's lakehouse, and `key_vault("NAME")` reads the team's secrets. So it keeps working
  when people join or leave.

## Changing teams

- A manager moves you (**🛠️ Manage → 🧩 Teams**). You lose the old team's files, lakehouse,
  secrets and DAGs at once, and get the new team's.
- Restart your Jupyter server (**File → Hub Control Panel → Stop My Server**, then start it) to
  get the new team's folder and catalog.
- What you made stays with the old team.
