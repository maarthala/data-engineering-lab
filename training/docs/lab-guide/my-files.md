# 📁 My files

**My files** is a file manager for your own bucket, `<you>-lake` (`demouser-lake` for the laptop's
`demouser`). Your notebooks, DAGs and dbt projects are kept there, with the raw files you load and
the storage of your tables. Browse it, upload and download files, and open a file to read it without
downloading. The top line shows both names: **bucket** `<you>-lake` · **lakehouse** `<you>_lake`.
Spark reads files by the bucket name (hyphens) and tables by the catalog name (underscores).

## What's in your bucket

| Folder | What it holds |
|---|---|
| `files/src/notebooks/` | your notebooks: Jupyter's `my_files/notebooks/` |
| `files/src/dags/` | your Airflow DAGs: Jupyter's `my_files/dags/`. A `.py` here shows up in Airflow in about 30 seconds |
| `files/src/dbt/` | your dbt projects: Jupyter's `my_files/dbt/` |
| `files/source/` | raw files to load (CSV, Parquet, JSON …), e.g. the practice files in `files/source/shopflow/` |
| `tables/` | the storage of your catalogs: `tables/<catalog>/<namespace>/<table>/` |

Each folder has a `README.md` that says what it is for. Under `tables/`, a folder marked **catalog**
is one of your catalogs, and a folder marked **table** (🧊) is an Iceberg table. These are the tables'
files: change tables with SQL or Spark, not here (see [🗄 My catalogs](my-catalogs.md)).

## Mine, Team, Shared with me
The switch at the top picks whose files you see:

| Tab | What it shows |
|---|---|
| **🪣 Mine** | your own bucket. Only you can see it |
| **👥 Team: <team>** | your team's bucket, `team-<team>-lake`. Everyone in the team sees, adds and changes these files. Only there once a manager has put you in a team ([Working in a team](../setup/teams.md)) |
| **🤝 Shared with me** | lakes other members invited you to (**Lake**, **Shared by**, **Access**, **Files in Spark**). Open one for its `files` and `tables` folders |

A lake's files come with a **whole-lake** invite on [My catalogs](my-catalogs.md#share-a-catalog). With
**read + write** you can upload, add folders and delete in its `files/`. With **read** you can only look
and download. Its `tables` folder is always read-only here.

## Work with files

- **Folders**: the tree on the left, or click a folder in the list. **⬑ ..** goes up one level.
- **⬆ Upload**: pick one or more files. Or drag files onto the list. They go into the folder you
  have open.
- **＋ New folder**: type a name, then **Create** (or **Cancel**).
- **⬇ Download** a file, or **🗑** to delete it (**Delete** / **Cancel** to confirm).
- **🗑** on a folder deletes the folder and everything in it. Type the folder's name to confirm
  (**⧉ Copy name** copies it for you). If the folder is an Iceberg table, the page warns you first:
  deleting its files breaks the table. Drop a table with SQL (`DROP TABLE … PURGE`) or on My catalogs.
- **⧉** copies a file's Spark path, e.g. `s3a://demouser-lake/files/source/shopflow/orders.csv`.
- **↻** refreshes the list.

Read a copied path in a notebook:

```python
df = spark.read.csv("s3a://demouser-lake/files/source/shopflow/orders.csv", header=True)
```

## Open a file in place
Click a file name to open it on the page. **← Back** returns to the list; **⧉ Copy path** and
**⬇ Download** are in the viewer too.

| File | Shown as |
|---|---|
| `.csv`, `.tsv` | a table: the first 1,000 rows, with the row and column count |
| `.ipynb` | the notebook's cells (Markdown and code), without their output |
| `.txt`, `.md`, `.py`, `.sql`, `.json`, `.jsonl`, `.yaml`, `.log`, `.sh`, `.xml`, `.html` … | plain text |

Files over 2 MB, and other types (Parquet, images …), are not previewed: use **⬇ Download**.

## The size limit
Your bucket holds at most **100 MB**, and a team bucket **500 MB** (the page shows the current limit;
a lab manager can change both). Your tables count too, since they live in `tables/`. When the bucket is
full, an upload fails with *Your bucket is full*: delete files you no longer need. One upload can be at
most 512 MB.

## Jupyter and Airflow
- Saves, renames and deletes in Jupyter's `my_files/notebooks/`, `dags/` and `dbt/` are copied to
  `files/src/` straight away.
- Files you add here (or in **Storage**) reach Jupyter the next time your server starts
  (**File → Hub Control Panel → Stop My Server → Start**).
- A DAG in `files/src/dags/` shows up in Airflow in about 30 seconds, whether you saved it in Jupyter
  or uploaded it here. Its `dag_id` must start with your username and `_`.
- dbt's `target/`, `logs/` and `dbt_packages/` stay in Jupyter only.

## Related lessons
- [Your lab account & workspace](../setup/workspace.md#your-files-notebooks-dags-data)
- [Working in a team](../setup/teams.md)
- [Upload a file & register it as a table](../unit3/upload-register.md)
- [Read the lake (Bronze)](../unit4/read-bronze.md)
- [Airflow basics](../unit5/basics.md)
