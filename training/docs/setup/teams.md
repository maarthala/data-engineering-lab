# Working in a team

A new member is in **no team**: they have their own lakehouse, files and secrets, and that's all.
A manager creates teams and adds members to them; a member is then in **one team**. The team has
its own files, lakehouse, secrets and DAGs. **Everyone in the team reads and writes them**, and
nobody outside the team sees them. Your own workspace stays yours: use the team's for work you
share.

Once you're in a team, the 👥 Team views below appear (e.g. **📁 My files → 👥 Team: sales**, and
`team_sales` on **🗂️ My catalogs**). Below, replace `sales` with your
team's name.

| What | Yours | Your team's |
|---|---|---|
| Files (bucket) | `demouser-lake` | `team-sales-lake` (500 MB) |
| Jupyter folder | `my_files/` | `sales_team/` |
| Lakehouse (catalog) | `demouser_lake` (`my` in Jupyter and DAGs) | `team_sales` (the same name everywhere) |
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

The team catalog **`team_sales`** has `bronze`, `silver` and `gold`, like your own. It goes by
that name in every tool (no short name like `my`, so it's always clear which team you mean):

- **Jupyter / Spark:** e.g. `spark.table("team_sales.bronze.customers")`. (`my.…` is still your
  own lakehouse.)
- **SQLPad / Superset:** e.g. `SELECT * FROM team_sales.bronze.customers`. In SQLPad, pick the connection **Team sales (Trino) — demouser** to see the team's tables in the
  schema browser on the left. A schema shows up there once it holds a table.
- A table one teammate makes, everyone in the team sees and can change straight away.

## The team's secrets

Keys and passwords the whole team uses (an API token, a database password) go in the team's
folder of the key vault, **`secret/sales_team/`**.

- **🔑 My secrets → 👥 Team** lists, adds and changes them (values stay hidden).
- **In a notebook or DAG**, add the team's name, `team="sales"`:
  `key_vault_set("API_TOKEN", "…", team="sales")`, `key_vault_list(team="sales")`,
  `key_vault("API_TOKEN", team="sales")`.
- **In the key vault's own UI**, the team folder sits next to yours.

## The team's DAGs

- Save a DAG in **`sales_team/dags/`**. It shows up in Airflow within about 30 seconds for
  **everyone in the team**: each of you can switch it on, trigger it and read its logs.
- Its `dag_id` must start with **`team_sales_`**, e.g. `team_sales_daily`.
- It runs **as the team**, not as whoever started it, so it keeps working when people join or
  leave. In its tasks, `lab_spark()` reaches `team_sales.…` and `shared.…` (there is no `my`: the
  DAG isn't anyone's), and `key_vault("NAME")` reads the team's secrets.

## Changing teams

- A manager adds, moves or removes you (**🛠️ Manage → 🧩 Teams**). You lose the old team's
  files, lakehouse, secrets and DAGs at once, and get the new team's (or, when removed, keep only
  your own).
- Restart your Jupyter server (**File → Hub Control Panel → Stop My Server**, then start it) to
  get the new team's folder and catalog.
- What you made stays with the old team.
