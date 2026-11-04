# Features: what you can do with this stack

The data engineering work this lab runs end to end, on your laptop or a team cluster, with the tools
that do it. Each task links to where the course does it.

## 1 · Ingest
| Task | With | In the course |
|---|---|---|
| Load files: CSV, JSON, Parquet, Excel from your bucket | 📁 My files, notebooks, Spark | [Upload a file & register it as a table](unit3/upload-register.md) · [Ingest Excel](recipes/excel.md) |
| Pull data from an API | Python (`requests`) | [Consuming APIs](unit3/apis.md) |
| Replicate a database: full copy, incremental, or every change (CDC) | 📡 Ingestion (OLake / Debezium) | [Your first sync job](cdc/first-job.md) · [Datastream](cdc/datastream.md) |
| Stream business events, some of them late, with your own schema | 📨 Pub/Sub (Kafka) | [Events, topics and partitions](pubsub/events.md) · [Lab guide](lab-guide/pubsub.md) |
| Land events in Bronze: Manual, Cron or Datastream | 📡 Ingestion | [Land events in Bronze](pubsub/bronze.md) |
| Load when a file arrives | Airflow | [Run on file arrival](recipes/file-trigger.md) |

## 2 · Store and model
| Task | With | In the course |
|---|---|---|
| A lakehouse: Iceberg tables in object storage, one governed catalog | Polaris, storage | [Lake vs Warehouse vs Lakehouse](unit1/lakehouse.md) · [File & table formats](unit1/formats.md) |
| Medallion layers: Bronze → Silver → Gold | Spark, SQL, dbt | [Medallion architecture](unit1/medallion.md) |
| Dimensional models: facts, dimensions, SCD Type 2 | SQL, Spark, dbt | [Dimensional modeling](unit1/dimensional-modeling.md) · [SCD2](recipes/scd2.md) |
| Data Vault 2.0: hubs, links, satellites, PIT tables, marts | Spark, SQL | [What is Data Vault 2.0?](datavault/what-is-data-vault.md) |
| Databases of your own | 🗄 My databases (Postgres) | [SQL from Python](unit3/databases.md) |

## 3 · Transform
| Task | With | In the course |
|---|---|---|
| SQL on the lake: joins, windows, CTEs, MERGE, JSON | Trino, SQLPad | [Query the lake with Trino](unit2/intro.md) |
| Python transformations | Jupyter, pandas, PyArrow | [pandas](unit3/pandas.md) · [PyArrow & Parquet](unit3/pyarrow.md) |
| Distributed batch processing | Spark (Spark Connect) | [Spark fundamentals](unit4/fundamentals.md) · [Transform to Silver](unit4/transform-silver.md) |
| Clean messy data; check quality, quarantine bad rows | Spark, dbt tests | [Data cleaning](unit4/data-cleaning.md) · [Data quality checks](recipes/data-quality.md) |
| SQL projects: models, tests, incremental models, snapshots, lineage | dbt | [Why dbt](dbt/why-dbt.md) |
| Tune jobs: skew, partitions, the query plan | Spark | [Spark performance & tuning](unit4/performance.md) · [Data skew & salting](unit4/skew.md) |

## 4 · Process streams
| Task | With | In the course |
|---|---|---|
| Watermarks, tumbling and session windows, checkpoints | Spark Structured Streaming | [Stream processing in a notebook](pubsub/stream-processing.md) |
| Late data in batch: Silver and Gold from Bronze events | Spark, SQL | [Silver and Gold from late events](pubsub/late-batch.md) |
| Dataflow-style pipelines on workers with slots | Apache Beam on Flink | [Your first pipeline](beam/first-pipeline.md) · [Run it on workers](beam/workers.md) |
| Event time, triggers, results corrected for late events | Apache Beam | [Event time and windows in Beam](beam/windows.md) · [A streaming pipeline](beam/kafka.md) |
| Schema drift: producers that change their events | 📨 Pub/Sub custom schemas, Iceberg | [Lab guide](lab-guide/pubsub.md) |

## 5 · Orchestrate
| Task | With | In the course |
|---|---|---|
| DAGs: schedules, retries, dependencies | Airflow | [Airflow basics](unit5/basics.md) · [Build the medallion DAG](unit5/medallion-dag.md) |
| From a notebook to a scheduled job | Airflow, Spark | [The development workflow](unit5/dev-workflow.md) |
| dbt runs in a DAG | Airflow, dbt | [Run dbt from Airflow](dbt/airflow.md) |

## 6 · Serve and analyse
| Task | With | In the course |
|---|---|---|
| Dashboards and charts from Gold | Superset | [Dashboards from the Gold layer](unit7/dashboards.md) |
| Ad-hoc SQL, saved queries | SQLPad | [Query the lake with Trino](unit2/intro.md) |
| Publish Gold out to other systems (reverse ETL) | Spark, Python | [Reverse-ETL](recipes/reverse-etl.md) |

## 7 · Govern and operate
| Task | With | In the course |
|---|---|---|
| Access control: roles, grants, who sees what | 🛡 Governance (Polaris RBAC) | [Governed Iceberg with Polaris](unit6/polaris.md) · [Grant table access](unit6/grant-and-query.md) |
| Share tables and lakes; work in a team | My catalogs, teams | [Working in a team](setup/teams.md) |
| Keep secrets out of code | 🔑 My secrets (Vault) | [Your lab account & workspace](setup/workspace.md) |
| Table maintenance: compaction, snapshot expiry | 🧹 maintenance, Spark | [Table maintenance](unit4/table-maintenance.md) |
| Undo a bad load: time travel, rollback | Iceberg | [Undo a bad load](recipes/rollback.md) |

## 8 · Practise
| Task | With | In the course |
|---|---|---|
| Real-world recipes | the whole stack | [Real-world recipes](recipes/excel.md) |
| Capstone projects, easy to hard | the whole stack | [Capstone 1](capstones/capstone-1.md) · [Capstone 3: Tidewell's data platform](capstones/capstone-3.md) |
| Mock exams | 📝 Mock exams | in the portal: **Extras → Mock exams** |
