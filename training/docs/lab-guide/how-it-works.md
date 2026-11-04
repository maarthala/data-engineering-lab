# 🧭 How the lab works

The lab is a web **portal** over a set of open-source data tools. You sign in once, and the portal
opens every tool for you, already signed in as you. Each member gets a private workspace of their
own: a lakehouse, a storage bucket, a Jupyter server, logins and secrets. This page is the short
practical picture; the [Stack architecture](../unit0/stack.md) and
[Technical architecture](../unit0/architecture.md) lessons go deeper.

## The tools behind the portal

| In the portal | The tool | What it does |
|---|---|---|
| **My catalogs** | Apache Polaris | the catalog: which Iceberg tables exist and who may use them |
| **My files**, **Storage** | RustFS (object storage, S3) | the files and the table data, in buckets |
| **SQL editor** | SQLPad on Trino | SQL on your lakehouse and on the lab's databases |
| **Notebooks** | Jupyter, with Spark | Python, pandas, Spark and `%%sql` |
| **Spark UI** | Apache Spark | the Spark cluster your notebooks and DAGs use |
| **Pipelines** | Apache Airflow | your DAGs: scheduled and run |
| **Dashboards** | Apache Superset | charts and dashboards on your tables |
| **Pub/Sub**, **Ingestion** | Kafka and the lab's runners | event streams, and copies of databases and streams into your lakehouse |
| **Flink UI** | Apache Flink | Apache Beam jobs on a cluster with slots |
| **My databases** | PostgreSQL | the lab's sample and live databases, and your own |
| **My secrets** | OpenBao (a key vault) | your API keys and passwords |

Every engine reads the same tables through Polaris, so a table you write in Spark shows up in
SQLPad and Superset too: [Polaris: governed Iceberg for every engine](../unit6/polaris.md).

## What you get of your own
On your first sign-in the lab builds your workspace (`<you>` is your username):

| Yours | Name | What it is |
|---|---|---|
| Lakehouse | `<you>_lake` | your Polaris catalog with `bronze`, `silver` and `gold`, and a sample table `bronze.sample_orders` |
| Bucket | `<you>-lake` | your object storage, with a size limit; `files/` for your files, `tables/` for your tables' data |
| Starter files | in `files/` | practice CSVs in `files/source/`, starter notebooks, DAGs and a dbt project in `files/src/` |
| Jupyter | your own server | `my` = your lakehouse, `shared` = the course's read-only lake; it stops by itself when idle (your files stay) |
| Logins | per member | your Jupyter gets its own catalog login (a fresh secret at every start) and a key that reaches only your bucket; your own SQLPad user; a Postgres login for your own databases |
| Secrets | your own folder | **My secrets**; notebooks and DAGs read them with `key_vault("NAME")` |

A manager can also put you in a team; the team then has its own lakehouse and bucket:
[Accounts, members and managers](accounts.md). Details of every item:
[Your lab account & workspace](../setup/workspace.md).

## Units: parts you switch on and off
The services are grouped in **units**. A unit that is off uses no memory, and its tool in the menu
(**Notebooks**, **SQL editor**, **Pipelines**, **Dashboards**, **Spark UI**, **Flink UI**) is greyed out.

| Unit | What it runs |
|---|---|
| **Core** | sign-in, the portal, files, the catalog, storage, secrets: always on |
| **SQL** | Trino and SQLPad |
| **Notebooks** | Jupyter |
| **Spark** | the Spark cluster |
| **Airflow** | Airflow |
| **Superset** | Superset (it needs SQL) |
| **Streaming** | Kafka and the event simulator, for **Ingestion** and **Pub/Sub** |
| **Flink** | Flink and the Beam services |

Who switches them:

- **On your laptop**: you, in the terminal: `make status`, then **↑ ↓** to a unit, **s** to start it,
  **x** to stop it ([make status: how to use it](../setup/deploy/make-status.md)). `make up` starts
  everything except Streaming and Flink.
- **On a team cluster**: a manager, on **Manage → Settings → Services** (a switch per unit). Data is
  kept while a unit is off.

## Laptop or team cluster
The same lab runs in two ways; the lakehouse, the portal and the lessons are the same.

| | On your laptop | On a team cluster |
|---|---|---|
| Runs on | Docker Compose, in `local/` | Kubernetes |
| Start it | `make up`, or units one by one with `make status` | an operator deploys it |
| Portal | `http://lab.localhost:8000` | `http://de.lan` |
| A tool's address | `<app>.lab.localhost:8000` | `<app>.de.lan` |
| Accounts | `demouser` / `demouser` is ready to use | everyone registers or gets their own account |

Set it up: [Bring up the stack](../setup/deploy/index.md) · [Kubernetes setup](../setup/deploy/kubernetes.md).

## Related lessons
- [Stack architecture](../unit0/stack.md)
- [Technical architecture](../unit0/architecture.md)
- [Your lab account & workspace](../setup/workspace.md)
- [Polaris: governed Iceberg for every engine](../unit6/polaris.md)
- [make status: how to use it](../setup/deploy/make-status.md)
- [Features: what you can do with this stack](../features.md)
