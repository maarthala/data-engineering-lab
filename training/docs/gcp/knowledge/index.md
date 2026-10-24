# GCP & BigQuery knowledge

This lesson explains **how the Google Cloud data services work inside**, before you use them in
the [practical implementation](../hands-on.md). Each page goes from "what is it and why does it
exist" down to the low-level details: architecture, core concepts, what matters most in
practice, how it compares to Spark, Snowflake, Fabric and AWS, a small hands-on exercise and a
glossary.

The running example is a typical **enterprise data warehouse (EDW) modernization** on Google
Cloud: a near real-time sales model in BigQuery, Looker on top, and the load layer moved into
shared ingestion projects, with cost and runtime under control.

| Page | What you learn |
|---|---|
| [BigQuery from the inside](bigquery.md) | Dremel, Colossus, slots, shuffle; pricing; partitioning, clustering; MERGE; cost and performance |
| [Data warehouse modelling](dwh-modelling.md) | Kimball facts and dimensions, grain, SCD2, nested data, a near real-time sales model |
| [Pub/Sub and Dataflow](pubsub-dataflow.md) | Messaging, Apache Beam, windows, watermarks, exactly-once, autoscaling |
| [Datastream and CDC](datastream-cdc.md) | Log-based change data capture, ordering, merging changes into BigQuery |
| [Cloud Composer and Airflow](composer-airflow.md) | Airflow architecture, DAG design at scale, Composer environments |
| [Dataform and dbt](dataform-dbt.md) | SQL transformations as code: models, tests, incremental loads |
| [GCP platform, IAM and Terraform](platform-iam-terraform.md) | Projects and folders, IAM, service accounts, networking, Terraform, CI/CD |
| [Looker and the semantic layer](looker-semantic-layer.md) | LookML, explores, caching, aggregate awareness, conversational BI |
| [Migration and EDW modernization](migration-modernization.md) | Inventory, phases, parallel run, reconciliation, cutover |

!!! tip "How to study"
    Read BigQuery and modelling first: everything else builds on them. Do the small hands-on
    part of each page; the free [BigQuery sandbox](https://console.cloud.google.com/bigquery) is
    enough for most of it. For a short summary of a topic afterwards, use the
    [deep dives](../deep-dives/index.md).

!!! note "Check current docs"
    Cloud services change quickly. Details marked "check current docs" (limits, default
    settings, preview features) may differ from what you see in the console.
