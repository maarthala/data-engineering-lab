# Practical implementation: ShopFlow on GCP

Now you build it. In this lesson you run the **ShopFlow** Bronze → Silver → Gold pipeline in
your own Google Cloud trial, with Google's managed services:

| Step | Page | Google Cloud service |
|---|---|---|
| 1 | [Your Google Cloud trial & project](setup.md) | project, billing, `gcloud` |
| 2 | [Cloud Storage: land the ShopFlow files](storage.md) | Cloud Storage |
| 3 | [BigQuery: Bronze from your files](bigquery.md) | BigQuery |
| 4 | [Apache Beam basics](beam-basics.md) | Apache Beam |
| 5 | [Dataflow: clean orders into Silver](dataflow.md) | Dataflow |
| 6 | [Notebook: build Gold in BigQuery Studio](notebook.md) | BigQuery Studio |
| 7 | [Composer: orchestrate the pipeline](composer.md) | Cloud Composer (Airflow) |
| 8 | [A small report in Looker Studio](report.md) | Looker Studio |
| 9 | [Cost & clean up](cleanup.md) | billing, deleting resources |

Do the steps in order: each one uses what the step before created. If you want to know *why*
a service works the way it does, read the matching page in
[GCP & BigQuery knowledge](knowledge/index.md).

!!! warning "Clean up at the end"
    Dataflow jobs and Composer environments cost money while they run. Finish with
    [Cost & clean up](cleanup.md), even if you stop halfway.
