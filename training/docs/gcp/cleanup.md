# Cost & clean up

## What costs money in this unit

| Service | Billed for | Stops by itself? |
|---|---|---|
| Cloud Storage | GB stored per month (a few MB here) | no, but it's tiny |
| BigQuery | bytes queried (first 1 TiB/month free), storage (first 10 GiB free) | n/a |
| Dataflow | worker VMs while a job runs | **yes**: a batch job ends |
| Notebook runtime | the VM while it runs | only after being idle a long time, so **delete it** |
| Cloud Composer | the environment, **every hour it exists** | **no**: delete it |
| Looker Studio | nothing | n/a |

See what you've spent: Console → **Billing** → **Reports**, filtered to project `shopflow-de`.
Costs show up there with a delay of up to a day.

## Clean up: stop what runs
Do this at the end of each session:

```bash
gcloud dataflow jobs list --region=$REGION --status=active
gcloud composer environments list --locations=$REGION
```

- **`gcloud dataflow jobs list --status=active`**: Dataflow jobs still running. It should print
  nothing (`Listed 0 items.`).
- **`gcloud composer environments list`**: if `shopflow-airflow` is listed, delete it as shown
  in the [Composer lesson](composer.md).

Notebook runtime: BigQuery Studio → your notebook → **▾** next to *Connect* → **Disconnect and
delete runtime**. Or Console → **Colab Enterprise** → **Runtimes**, and delete any you see.

## Clean up: remove everything
**Easiest: delete the project.** It removes the bucket, the datasets, the notebook, everything,
and stops all billing for it:

```bash
gcloud projects delete $PROJECT
```

- **`gcloud projects delete`** asks you to confirm, then shuts the project down. For **30 days**
  you can still bring it back with `gcloud projects undelete PROJECT_ID`; after that, it's gone.

**Or keep the project and remove just the data:**

```bash
for layer in bronze silver gold; do bq rm -r -f -d $PROJECT:shopflow_$layer; done
gcloud storage rm --recursive gs://$BUCKET
```

- **`bq rm -r -f -d`**: remove a **d**ataset, **r**ecursively (with its tables), and **f**orce
  (don't ask).
- **`gcloud storage rm --recursive gs://BUCKET`**: deletes every object, then the bucket.

The free trial ends after 90 days or US$300, whichever comes first. Google won't charge your
card unless you click **Activate full account**.

## Checklist
| You can… | Lesson |
|---|---|
| create a project, set a budget alert, use Cloud Shell, enable APIs | [Trial & project](setup.md) |
| create a bucket, upload files (console and `gcloud storage cp`), lay out a landing zone | [Cloud Storage](storage.md) |
| create datasets, load CSVs with `bq load`, keep Bronze raw with a `STRING` schema, read query cost | [BigQuery](bigquery.md) |
| write Beam pipelines (`Create`, `Map`, `Filter`, `FlatMap`, `CombinePerKey`, `GroupByKey`, `DoFn`, file I/O) and run them locally | [Apache Beam basics](beam-basics.md) |
| write an Apache Beam pipeline, test it on DirectRunner, run it on Dataflow, route bad rows aside, grant a service account roles | [Dataflow](dataflow.md) |
| use a BigQuery Studio notebook: `%%bigquery`, BigQuery DataFrames, write Gold tables | [Notebook](notebook.md) |
| read and write a Composer DAG with the Google operators, and build the same DAG in the lab's Airflow | [Composer](composer.md) |
| build and share a Looker Studio report with scorecards, a time series, a bar chart, a table and filters | [Report](report.md) |

The same pipeline in the lab and on Google Cloud:

| Step | Lab | Google Cloud |
|---|---|---|
| land files | My files | Cloud Storage |
| Bronze | Spark → Iceberg | `bq load` → BigQuery |
| Silver | Spark job | Dataflow (Beam) |
| Gold | Spark SQL / notebook | BigQuery SQL / BigQuery DataFrames |
| orchestrate | Airflow | Cloud Composer (Airflow) |
| report | Superset | Looker Studio |

## Check yourself
1. Which two things in this unit keep billing until you act? *(The Composer environment and a
   notebook runtime.)*
2. What's the one command that removes everything this unit created? *(`gcloud projects delete
   $PROJECT`.)*
