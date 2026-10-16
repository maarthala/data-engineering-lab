# OSS ⇄ Databricks / Snowflake / Fabric / Azure

This course teaches Data Engineering on an **open-source** stack — deliberately, so you learn the
*concepts* without a cloud bill. Every concept maps directly to the commercial platforms you'll
learn next. This page is your dictionary.

## The big picture
> You're not learning "toy" tools. You're learning the **same building blocks** the platforms are
> made of — the governance **model** (catalog→schema→table + RBAC) and the **Medallion**
> terminology map 1:1 onto Databricks and Snowflake.

## Component map
| This stack (OSS) | Databricks | Snowflake | Microsoft Fabric | Azure (native) |
|---|---|---|---|---|
| **RustFS** (S3-compatible object storage) | cloud storage / catalog Volumes | Stages & external volumes | **OneLake** | ADLS Gen2 |
| **Iceberg** tables (via the `iceberg` REST catalog) | **Delta Lake** (native) | Iceberg & native tables | Delta on OneLake | Delta on ADLS |
| **Apache Polaris** (Iceberg REST catalog, governance) | **the Databricks catalog** | **Snowflake Open Catalog** / Database→Schema + RBAC roles | Fabric catalog + Purview | Microsoft Purview |
| **Medallion** Bronze/Silver/Gold | **Medallion** (Databricks' term) | raw/staging/marts (same idea) | Medallion on OneLake | same pattern |
| **Python + pandas / NumPy / PyArrow** (Jupyter) | Databricks notebooks (pandas / PySpark) | **Snowpark** (Python) | Fabric notebooks | Synapse notebooks |
| **`requests` / REST ingest** | notebook + Auto Loader | external access + COPY INTO | Dataflows Gen2 / Copy | Data Factory REST connector |
| **Spark** (Spark Connect) | Databricks Runtime (Spark) | Snowpark | Fabric Spark notebooks | Synapse/Databricks Spark |
| **Trino** (SQL engine over the lake) | Databricks **SQL Warehouse** | Virtual **Warehouse** | Fabric SQL endpoint / Warehouse | Synapse SQL |
| **Airflow** (DAGs) | Databricks **Workflows / Jobs** | Tasks & Streams (+ external orch.) | Fabric **Data Factory** pipelines | Azure **Data Factory** |
| **Superset** (BI) | Databricks AI/BI Dashboards | Snowsight | **Power BI** (Direct Lake) | Power BI |
| **Polaris client id/secret + RBAC** (roles/grants) | the Databricks catalog + SCIM/SSO (Entra/Okta) | Snowflake RBAC + SSO | Entra ID | Entra ID |
| **SQL** (joins, CTEs, window fns, MERGE) | identical | identical | identical | identical |
| **Materialized views** in `%%sql` (Spark Declarative Pipelines) | Materialized views / Lakeflow Declarative Pipelines | Materialized views / **Dynamic Tables** | **Materialized lake views** | Synapse materialized views |
| **Table maintenance**: Iceberg `rewrite_data_files` / `expire_snapshots`, Delta `OPTIMIZE` / `VACUUM` | `OPTIMIZE` / `ZORDER` / `VACUUM` (+ predictive optimization) | automatic (managed storage) | `OPTIMIZE` / `VACUUM` + **V-Order** | `OPTIMIZE` / `VACUUM` (Synapse Spark) |

## The three biggest transfers
1. **Polaris' governance model *is* the Databricks catalog / Snowflake model** — catalog→
   schema→table with RBAC roles and grants. You finish this course already knowing the cloud
   governance **model**, even though the local catalog is Apache Polaris.
2. **Medallion / Bronze–Silver–Gold is Databricks' own vocabulary** — 1:1.
3. **Your SQL is 100% portable** — joins, `GROUP BY`, CTEs, window functions, and `MERGE` run
   unchanged on Databricks SQL, Snowflake, and Fabric (only a few date-function *names* differ).

## How the lakehouse is wired here (and what changes on the cloud)
On this stack, **Spark builds** the medallion and **Trino/Superset read** it — both through one
shared **`iceberg`** catalog backed by **Apache Polaris** (Iceberg REST + RustFS). Polaris is *also*
the governance layer that models catalogs, schemas, and grants.

On **Databricks**, storage and governance are **one product**: the Databricks catalog both *stores* the tables (as Delta)
and *enforces* access — the query engines call it on every read. That unification is the main thing
the managed platform adds over this OSS stack.

## What's *different* on managed platforms (so there are no surprises)
The **concepts, SQL, Spark code, and governance model transfer directly.** What the platforms add
— and what you're *not* practicing here — is the **managed operational layer**:

- **Engine-level governance enforcement** — on the cloud, the query engines enforce the Databricks catalog's
  grants automatically. On this OSS stack you *define and inspect* the policy in Polaris (the
  transferable skill), but Trino/Spark aren't wired to enforce it — that wiring is the managed convenience.
- **Serverless / auto-scaling compute** — no clusters to run; pay per query/second.
- **Billing & cost governance** — the real day-2 skill on the platforms.
- **Proprietary performance** — Photon (Databricks), micro-partitions + result cache (Snowflake),
  query acceleration (Fabric).
- **Built-in lineage, monitoring, and data quality** — richer than the OSS equivalents.

The takeaway: **you'll arrive at Databricks/Snowflake/Fabric/Azure already fluent in the
architecture and the work** — the platform course then teaches the managed layer on top, not the
fundamentals.

!!! tip "Try Snowflake for real"
    Have (or want) a Snowflake trial account? The [Snowflake unit](../snowflake/setup.md) loads
    the same ShopFlow data into Snowflake with one SQL file and walks you through every SnowPro
    Core topic on it.
