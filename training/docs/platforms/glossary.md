# Glossary

Terms used throughout the course — deliberately the same words the cloud platforms use, so
they're already familiar when you get there.

## Foundations & storage
| Term | Meaning |
|---|---|
| **OLTP** | Online Transaction Processing — the live app database (ShopFlow's Postgres). |
| **OLAP** | Online Analytical Processing — analytics over large datasets (the lakehouse). |
| **ETL / ELT** | Transform-then-load vs. load-raw-then-transform (this course, and the lakehouse, are ELT). |
| **Object storage** | Cheap, scalable file storage addressed by keys (S3 / RustFS / ADLS / OneLake). |
| **Data lake** | Raw files in object storage — flexible but ungoverned. |
| **Data warehouse** | A managed analytical database with strong SQL + governance. |
| **Lakehouse** | Lake storage + warehouse table semantics (ACID, schema) + a catalog. |
| **Data swamp** | A lake gone bad — files nobody trusts or can find. |
| **Schema-on-write / -read** | Fix the schema before load (warehouse) vs. impose it at query time (lake). |
| **Storage/compute separation** | Scale (and pay for) storage and compute independently. |

## File & table formats
| Term | Meaning |
|---|---|
| **Parquet** | Columnar, compressed, typed file format — the analytics workhorse. |
| **Row vs columnar** | Store a whole record together vs. store a whole column together. |
| **Table format** | Metadata/log turning files into ACID tables — **Iceberg**, **Delta**. |
| **Iceberg** | The open table format this course's lakehouse uses (Iceberg REST catalog on RustFS). |
| **Delta Lake** | Table format native to Databricks/Fabric — the managed equivalent of the Iceberg tables you build here. |
| **ACID** | All-or-nothing, consistent, isolated, durable writes. |
| **Time travel** | Querying a previous version/snapshot of a table. |
| **Schema evolution** | Safely changing a table's columns over time. |
| **Partitioning / pruning** | Split a table into folders by a column; skip the ones a filter can't match. |
| **Pushdown** | Send filters/columns down to the source so less data is read (projection / predicate). |

## Catalog, medallion & governance
| Term | Meaning |
|---|---|
| **Catalog / governance** | Top level of the namespace + governance layer (**Apache Polaris** here; the **Databricks catalog** / Snowflake on the cloud). |
| **Namespace** | `catalog.schema.table` — the 3-level path to any table. |
| **Medallion** | Bronze (raw) → Silver (clean) → Gold (business) layering. |
| **Bronze / Silver / Gold** | The three medallion quality tiers. |
| **Conform** | Reshape many sources onto one shared column model. |
| **Grain** | What one row represents (e.g. one order line). |
| **Single source of truth** | One trusted table everyone agrees on (Silver). |
| **Data mart / star schema** | A focused Gold dataset / fact + dimension modelling. |
| **RBAC** | Role/rule-based access control — grants on securables. |
| **Securable / principal / privilege** | Object protected / identity granted / action allowed. |
| **Grant chain** | You need permission at *every* level above a table to read it. |
| **SSO / OIDC** | Single sign-on via an identity provider — used on the cloud platforms (Entra/Okta), and in this lab too: one **lab account** (Keycloak, OIDC) signs you in to every tool. |
| **Credential vending** | The catalog issuing short-lived storage credentials to engines. |

## Compute, SQL & Python
| Term | Meaning |
|---|---|
| **Spark** | Distributed compute engine for large-scale transforms. |
| **Spark Connect** | Lightweight client protocol to a remote Spark cluster (Jupyter → cluster). |
| **DataFrame** | A distributed (Spark) or in-memory (pandas) typed table. |
| **Trino** | Distributed SQL query engine over the lake. |
| **pandas** | Python's in-memory table library — the DE prototyping tool. |
| **NumPy** | Python's numeric-array library that underlies pandas (vectorised math). |
| **PyArrow** | Reads/writes **Parquet** and is the zero-copy bridge between engines. |
| **CTE** | Common Table Expression — a named subquery (`WITH …`). |
| **Window function** | A calculation across a set of rows related to the current row. |
| **MERGE / upsert** | Insert-or-update rows in one atomic operation (handles late/corrected data). |

## Orchestration & operations
| Term | Meaning |
|---|---|
| **DAG** | Directed Acyclic Graph — a pipeline of tasks (Airflow). |
| **Orchestration** | Scheduling + sequencing pipeline tasks reliably, with retries. |
| **Idempotent** | Re-running a job produces the same result (safe to retry). |
| **Incremental load** | Processing only new/changed data, not the whole dataset. |
| **Late-arriving data** | Records that show up after the period they belong to. |
| **CDC** | Change Data Capture — capturing inserts/updates/deletes from a source. |
| **Backfill / catchup** | Running missed or historical dates, in order. |
| **Sensor** | A task that waits for a condition (e.g. a file to arrive) before proceeding. |
| **High-water mark** | The latest key already loaded — the basis of incremental loads. |
