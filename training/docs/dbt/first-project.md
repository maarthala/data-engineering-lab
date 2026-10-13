# Your first dbt project

## Concept
A dbt **project** is a folder with one settings file (`dbt_project.yml`) and your models in
`models/`. dbt also needs to know **how to connect** to the engine; that's `profiles.yml`.
You already have a starter project: Jupyter's **`dbt/shop/`** folder.

```
dbt/shop/
├── dbt_project.yml        the project: its name, and how each folder of models is built
├── profiles.yml           the connection: Spark Connect, as you
├── macros/
│   └── generate_schema_name.sql
└── models/                your models (empty for now; the next lessons fill it)
```

Both settings files are **YAML**, a plain-text format for settings: `key: value` pairs, and
**indentation** (spaces, never tabs) shows what belongs inside what. `# …` is a comment.

```yaml
shop:              # "shop" contains everything indented below it
  target: dev      # a key with a value
```

## Lab

### 1 · Open a terminal in your project
In Jupyter: **File → New → Terminal**. A terminal is a window where you type commands; each
line you type runs when you press Enter. Go to the project and list its files:

```bash
cd ~/work/dbt/shop
ls
```
```text
dbt_project.yml  macros  models  profiles.yml
```

- **`cd`** (*change directory*) moves into a folder. `~` is your home folder, so
  `~/work/dbt/shop` is the `dbt/shop/` you see in Jupyter's file browser.
- **`ls`** lists what's in the folder.

!!! tip "Every dbt command runs in the project folder"
    dbt looks for `dbt_project.yml` and `profiles.yml` in the folder you're in. Run it anywhere
    else and it can't find them (the errors are in step 4).

### 2 · Read the project settings: `dbt_project.yml`
Open it from the file browser (double-click):

```yaml
name: shop
version: "1.0"
profile: shop                  # which connection in profiles.yml to use

models:
  shop:
    +file_format: iceberg      # every model is an Iceberg table/view in your lakehouse
    staging:                   # models/staging/ → views in the silver schema
      +materialized: view
      +schema: silver
    marts:                     # models/marts/ → tables in the gold schema
      +materialized: table
      +schema: gold
```

- **`name`**: the project's name. **`profile`**: which connection to use (next step).
- **`models:` → `shop:`**: settings for the models of this project. Below it, **folder names**
  (`staging`, `marts`) set defaults for every model in `models/staging/` and `models/marts/`.
- **`+`** in front of a key marks a **setting** (a *config*), not a folder name.
- **`+file_format: iceberg`**: create Iceberg tables, like everything else in your lakehouse.
- **`+materialized`**: *how* dbt builds the model: as a **`view`** or a **`table`** (more in
  [Models, ref() and materializations](models-ref.md)).
- **`+schema`**: the schema (namespace) the model lands in: `silver` or `gold`, the medallion
  layers from [Medallion architecture](../unit1/medallion.md).

### 3 · Read the connection: `profiles.yml`

```yaml
shop:
  target: dev
  outputs:
    dev:
      type: spark
      method: session          # use the Spark session of this server (Spark Connect)
      host: na                 # not used by the session method, but required
      schema: silver           # default schema for models without +schema
      server_side_parameters:
        spark.sql.defaultCatalog: iceberg
        spark.sql.catalog.iceberg.default-namespace: silver   # for dbt's helper views (snapshots)
        spark.sql.catalog.iceberg.warehouse: "{{ env_var('LAKE_WAREHOUSE') }}"
        spark.sql.catalog.iceberg.credential: "{{ env_var('LAKE_CREDENTIAL') }}"
```

- **`shop:`**: the profile named in `dbt_project.yml`. **`target: dev`** picks one of its
  **outputs** (connections). Teams often have `dev` and `prod`; you need one.
- **`type: spark`**: use the **dbt-spark adapter**, the plugin that lets dbt talk to Spark.
- **`method: session`**: run SQL through the Spark session of this Jupyter server, which
  means Spark Connect, exactly like your notebooks.
- **`server_side_parameters`**: Spark settings for dbt's session. `iceberg` becomes the default
  catalog, so `silver.stg_orders` means `iceberg.silver.stg_orders`, in **your** lakehouse.
  **`default-namespace: silver`** is the schema for anything dbt creates without naming one
  (a few helper views, like the snapshots' in a later lesson).
- **`{{ env_var('LAKE_WAREHOUSE') }}`**: `{{ … }}` is **Jinja**, the same template syntax as
  Airflow's `{{ ds }}` ([Airflow basics](../unit5/basics.md)): dbt fills in the value before
  using the file. **`env_var(name)`** reads an **environment variable** of your server:
  `LAKE_WAREHOUSE` is your lakehouse, `LAKE_CREDENTIAL` your login. So **no password is in this
  file**. It's safe in your bucket, and works unchanged for every member.

### 4 · Check the connection: `dbt debug`

```bash
dbt debug
```
```text
15:32:54  Running with dbt=1.12.5
15:32:54  dbt version: 1.12.5
15:32:54  python version: 3.10.21
15:32:54  python path: /opt/dbt/bin/python
15:32:54  os info: Linux-6.12.76-linuxkit-x86_64-with-glibc2.41
15:32:54  Using profiles dir at /home/jovyan/work/dbt/shop
15:32:54  Using profiles.yml file at /home/jovyan/work/dbt/shop/profiles.yml
15:32:54  Using dbt_project.yml file at /home/jovyan/work/dbt/shop/dbt_project.yml
15:32:54  adapter type: spark
15:32:54  adapter version: 1.11.0
15:32:54  Configuration:
15:32:54    profiles.yml file [OK found and valid]
15:32:54    dbt_project.yml file [OK found and valid]
15:32:54  Required dependencies:
15:32:54   - git [OK found]

15:32:54  Connection:
15:32:54    host: na
15:32:54    port: 443
15:32:54    cluster: None
15:32:54    endpoint: None
15:32:54    schema: silver
15:32:54    organization: 0
15:32:54  Registered adapter: spark=1.11.0
15:32:56    Connection test: [OK connection ok]

15:32:56  All checks passed!
```

**`dbt debug`** checks the setup without building anything: it finds both files, checks they're
valid YAML, and runs a test query through Spark. `All checks passed!` means you're ready.
(`host`, `port`, `cluster` and `endpoint` belong to other connection methods; ignore them.)

!!! note "What `dbt debug` doesn't check"
    Its test query doesn't touch your lakehouse, so it passes even if your lakehouse login were
    wrong. That login is checked by the first `dbt run` (next lesson).

If something's wrong instead:

| You see | What's wrong |
|---|---|
| `project path </home/jovyan/work/dbt_project.yml> not found` (`dbt debug`) | you're not in the project folder: `cd ~/work/dbt/shop` |
| `Invalid value for '--profiles-dir': Path '/home/jovyan/.dbt' does not exist` (`dbt run` …) | the same: wrong folder, so dbt can't find `profiles.yml` |
| `Runtime Error` … `Syntax error near line 4` | a YAML mistake on that line: usually a tab or a wrong indentation |

### 5 · The schema macro
`macros/generate_schema_name.sql` changes one dbt default. Without it dbt **prefixes** custom
schemas with the profile's `schema`: `+schema: gold` would become **`silver_gold`**. Teams use
that prefix so each developer gets their own copy of every schema. Your lakehouse is already
yours, so the macro keeps the names plain: `silver` and `gold`.

A **macro** is a reusable piece of Jinja, like a function. You'll write your own later in the
unit; for now, leave this one as it is.

!!! note "Where your files are kept"
    `dbt/` is mirrored to your bucket (`files/src/dbt/`) like `notebooks/` and `dags/`: files
    you save in Jupyter's editor are copied straight away. dbt's generated folders, `target/`
    and `logs/`, stay in Jupyter only: dbt rebuilds them on every run.
    ([Your files](../setup/workspace.md#your-files-notebooks-dags-data))

## 🎯 This runs unchanged on Azure, Databricks, Snowflake & Fabric
`dbt_project.yml` is identical on every platform. Only `profiles.yml` changes: `type:
databricks` with a workspace host and token, `type: snowflake` with an account and warehouse,
`type: fabric` with a workspace. dbt Cloud stores the connection in its web settings instead
of a file.

## You can now…
- Open a terminal in Jupyter and move to your project with `cd`
- Read `dbt_project.yml`: project name, folder defaults, `+materialized`, `+schema`
- Read `profiles.yml`: profile, target, the `spark` adapter, `env_var()` for your login
- Check the setup with `dbt debug`, and fix the common errors
- Explain why the schema macro keeps `silver` / `gold` names plain
