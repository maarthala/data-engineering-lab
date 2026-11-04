# 📓 Notebooks

**Tools → Notebooks** opens JupyterLab, signed in with your lab account. Every member has a Jupyter
server of their own, already connected to Spark and to your lakehouse: a new notebook can read and
write your tables in its first cell. Lessons: [Python & notebooks for DE](../unit3/python-essentials.md)
and the ones after it.

## Your own server
- Your server is a container of your own (on a team cluster: a pod), with a work folder that stays
  when the server stops.
- It stops by itself after a while without activity; your files stay. Open **Notebooks** again to
  start it.
- To restart it (for example after you joined a team): **File → Hub Control Panel → Stop My Server**,
  then **Start My Server**.
- Its memory and CPU, and the idle time, are set by lab managers on
  [⚙️ Settings](settings.md). A new size applies when your server next starts.

## Your files: in sync with My files
The file browser shows these folders. Each one is kept in sync with a bucket's `files/src/`
(see [📁 My files](my-files.md)):

| Folder | In the bucket | Who writes it |
|---|---|---|
| `my_files/notebooks/`, `dags/`, `dbt/` | your bucket, `files/src/…` | you: every save, rename and delete goes up at once |
| `<team>_team/…` (e.g. `sales_team/`) | your team's bucket, `files/src/…` | the whole team: opening a folder first pulls your teammates' changes |
| `<owner>_lake/…` | another member's bucket, `files/…` | a lake shared with you; read only, unless it was shared with write |

At server start, files you uploaded with **My files** (or the Storage console) come down too. A `.py`
saved in `my_files/dags/` shows up in [🔀 Pipelines](pipelines.md) about 30 seconds later.

## `spark` and your catalogs
Every notebook starts with a `spark` session (Spark Connect to the lab's Spark cluster). The first
lines of output say who you are:

```text
✓ `spark` ready via Spark Connect (Spark …)
✓ you are demouser · my = your lakehouse (demouser_lake) — e.g. my.bronze.orders
✓ shared = the course's shared lake (read-only) — e.g. shared.gold.daily_sales
```

| Name | What it is |
|---|---|
| `my` | your own lakehouse, `<you>_lake` |
| `shared` | the course's shared lake, read only |
| `team_<team>` | your team's lakehouse, if you're in a team |
| `<you>_lake`, … | the same catalogs by their real names, as in SQLPad and Superset |

Another catalog (one you made on [🗄 My catalogs](my-catalogs.md), or one shared with you):
`use_catalog("kiran_sales")`, then `spark.sql("SELECT * FROM kiran_sales.sales.orders")`.

If the Spark unit is off, the notebook says **⚠ `spark` not created**. Once it's on, restart the kernel
(**Kernel → Restart Kernel**).

## `%%sql`
A cell that starts with `%%sql` runs Spark SQL and shows the result as a table (up to 1000 rows):

```sql
%%sql
SELECT * FROM my.bronze.sample_orders LIMIT 10
```

`${name}` is replaced by a notebook variable or an environment variable (e.g. `s3a://${LAKE_BUCKET}/…`,
your own bucket). `%%sql` also runs `CREATE`, `REFRESH` and `DROP MATERIALIZED VIEW` and
`SHOW MATERIALIZED VIEWS IN …` ([Materialized views](../unit4/materialized-views.md)).

## Helpers in every notebook
| Function | What it does |
|---|---|
| `use_catalog(catalog, alias=None)` | makes another Polaris catalog usable in Spark, with your login |
| `key_vault(name, field=None, team=False)` | one of your secrets from [🔑 My secrets](my-secrets.md), never printed |
| `key_vault_dict(name, team=False)` | every field of a secret, as a dict |
| `key_vault_list(team=False)` | the names of your (or your team's) secrets |
| `key_vault_set(name, value, secret=True, team=False)` | saves a secret from code |
| `lab_kafka(stream=None, python=False, beam=False)` | Kafka settings for one of your [📨 Pub/Sub streams](pubsub.md): for Spark, `python=True` for confluent-kafka, `beam=True` for Beam's `ReadFromKafka` |
| `lab_beam(runner="local", name="beam", parallelism=None, …)` | options for an Apache Beam pipeline: `"local"` runs it in Jupyter, `"flink"` on the Flink unit |
| `lab_beam_jobs(running=True)` | your Beam jobs on the Flink unit: name, state, seconds |
| `lab_beam_stop(name)` | stops your streaming Beam job of that name |

`team=` takes a team's name (`team="sales"`) or `True` for your only team.

## Related lessons
- [Python & notebooks for DE](../unit3/python-essentials.md)
- [SQL from Python (Postgres & lakehouse)](../unit3/databases.md)
- [Spark from a notebook](../unit3/spark.md)
- [Query tables with %%sql](../unit3/sql-magic.md)
- [Upload a file & register it as a table](../unit3/upload-register.md)
- [Stream processing in a notebook: watermarks and windows](../pubsub/stream-processing.md)
- [Your first pipeline: PCollections and transforms](../beam/first-pipeline.md)
