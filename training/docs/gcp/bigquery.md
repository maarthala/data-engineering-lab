# BigQuery: Bronze from your files

## Concept

### What BigQuery is
**BigQuery** is Google's serverless data warehouse. You don't start or size a cluster: you write
SQL, and Google runs it on as many machines as it needs. It plays the role Trino + Iceberg play
in the lab.

### Projects, datasets and tables
BigQuery organises everything in three levels:

```
shopflow-de-4821                 project   — the bill, the permissions, the top of every name
├── shopflow_bronze              dataset   — a folder of tables, in one location
│   ├── customers                table
│   ├── orders                   table
│   └── …
├── shopflow_silver              dataset
└── shopflow_gold                dataset
```

A **dataset** is a **named container for tables** (and views, functions and ML models) inside a
project. It's the BigQuery word for what the lab calls a **schema** (`bronze`, `silver`, `gold`).
A table always belongs to exactly one dataset, so you name it `dataset.table`. A dataset is
also where several settings live, and they apply to every table in it:

- **Location**: the region where its tables are stored (`us-central1`). It's set when you create
  the dataset and **can't be changed** later. A query can only join tables from the same
  location, and a load can only read a bucket in that location. So all three datasets go in the
  same region as the bucket.
- **Access**: you can let a person or a service account read or write **one dataset**, e.g. let
  analysts see `shopflow_gold` but not `shopflow_bronze`. That's how a medallion layout is
  usually secured.
- **Default table expiration** (optional): tables in it delete themselves after N days. It's
  handy for a scratch dataset.

The full name of a table has all three levels:

| Lab | BigQuery |
|---|---|
| catalog (`my`, `shared`) | **project** (`shopflow-de-4821`) |
| schema (`bronze`, `silver`, `gold`) | **dataset** (`shopflow_bronze`, …) |
| table | table |
| `my.bronze.orders` | `` `shopflow-de-4821.shopflow_bronze.orders` ``, or just `shopflow_bronze.orders` inside the project |

- In SQL, wrap a name in **backticks** (`` ` ``) when it contains a `-`, as project IDs often
  do. Without them, BigQuery reads `shopflow-de-4821` as a subtraction.
- The `bq` tool writes **`project:dataset.table`**, with a colon after the project
  (`$PROJECT:shopflow_silver.orders`). SQL uses dots. Both mean the same table.
- Dataset names may use letters, digits and `_`, but **no `-`**. That's why it's
  `shopflow_bronze`, not `shopflow-bronze`.

### Kinds of tables
| Kind | Data lives in | Used in this unit for |
|---|---|---|
| **Table** (standard) | BigQuery's own storage | Bronze, Silver, Gold |
| **External table** | files in Cloud Storage, read at query time | (explained below) |
| **View** | nowhere: a saved query that runs each time you select from it | not used here; handy for a Gold "latest numbers" view |

### Jobs
Every piece of work BigQuery does is a **job** with an id (`bqjob_…`): a **query job**, a **load
job** (files → table), an **extract job** (table → files) or a **copy job**. Jobs run on
Google's machines, not in Cloud Shell or your browser, so closing the tab doesn't stop a query.
**Job history** in the console lists every job with its status, duration, bytes and errors.
When something goes wrong, look there first.

### What you pay for
- **Queries** (on-demand): by the bytes a query **reads**. The first **1 TiB a month is free**.
  ShopFlow is a few MB, so this unit stays far below that.
- **Storage**: the first **10 GiB a month is free**.
- **Loading files** from Cloud Storage with a load job: **free**.

BigQuery stores tables **by column**, so a query pays only for the columns it reads.
`select *` reads every column and costs the most. Before you run a query, the editor shows
*"This query will process N MB when run"* at the top right.

### Load or external table?
- **Load** (`bq load`) copies the file's rows into BigQuery's own storage. It's fast to query,
  and the file can change or go without affecting the table. This is how you build Bronze.
- An **external table** leaves the data in the bucket and reads the files on every query. It's
  handy for a quick look, but slower, and it changes whenever the files change.

### Bronze stays raw
Bronze is the file **as it arrived**. For `orders` that matters: if you let BigQuery guess types,
the load **fails** on `unknown` in `customer_id` and `not a date` in `order_ts`. So `orders` gets
a schema where every column is `STRING`. Nothing is lost and nothing fails, and the Dataflow
lesson cleans it into Silver. The three clean files can use **autodetect**, where BigQuery
guesses the types from the data.

## Lab

### 1 · Create the three datasets
In Cloud Shell:

```bash
for layer in bronze silver gold; do
  bq mk --location=$REGION --dataset $PROJECT:shopflow_$layer
done
bq ls
```

- **`for layer in a b c; do …; done`** runs the command once per word, with `$layer` set to it.
- **`bq`** is BigQuery's command-line tool (it comes with `gcloud`).
- **`bq mk --dataset PROJECT:NAME`** creates a dataset. **`--location`** fixes where it lives.
- **`bq ls`** lists the datasets in the project.

Output:

```
     datasetId
 -----------------
  shopflow_bronze
  shopflow_gold
  shopflow_silver
```

!!! tip "See the datasets in the console"
    Console → **BigQuery** → **BigQuery Studio** (left menu: **Studio**). The **Explorer** panel
    on the left lists your project: click **▸** next to your project ID to open it. You'll see
    `shopflow_bronze`, `shopflow_gold` and `shopflow_silver`.

    - Click a dataset to see its **Dataset info**: **Data location** says `us-central1`.
    - Don't see your project? Type `shopflow` in the Explorer's search box, or check the project
      picker at the top of the console.
    - The UI changes often. Newer accounts may show the Explorer as a tree of *Datasets* /
      *Notebooks* / *Queries* under the project, and some offer a **Classic Explorer** switch.
      The datasets are under your project ID either way.

### 2 · Load the three clean files
```bash
for t in customers products order_items; do
  bq load --source_format=CSV --skip_leading_rows=1 --autodetect --replace \
      shopflow_bronze.$t "gs://$BUCKET/shopflow/$t/*.csv"
done
```

- **`bq load TABLE SOURCE`** runs a load job: files → table. The table is created if it's missing.
- **`--source_format=CSV`** says how to read the file. Others are `NEWLINE_DELIMITED_JSON`,
  `PARQUET` and `AVRO`.
- **`--skip_leading_rows=1`** skips the header line.
- **`--autodetect`** lets BigQuery guess the column types from the data.
- **`--replace`** empties the table first, so running it again doesn't double the rows.
- **`"gs://…/*.csv"`**: every CSV in that folder. The quotes stop Cloud Shell from expanding `*`
  itself.

Each load prints `Waiting on bqjob_… (2s) Current status: DONE`.

### 3 · Load `orders` as all text
```bash
bq load --source_format=CSV --skip_leading_rows=1 --replace \
    shopflow_bronze.orders "gs://$BUCKET/shopflow/orders/*.csv" \
    order_id:STRING,customer_id:STRING,channel:STRING,order_ts:STRING,status:STRING,currency:STRING,promo_id:STRING
```

- The last argument is the **schema**: `name:TYPE` per column, in file order. Every column is
  `STRING`, so every line loads, whatever is in it.

!!! tip "See the tables in the console"
    In the Explorer: your project → **▸ `shopflow_bronze`** shows the four tables: `customers`,
    `order_items`, `orders`, `products`. (Refresh the page if a new one doesn't appear.) Click a
    table. It opens in a tab with these sub-tabs:

    | Tab | What it shows |
    |---|---|
    | **Schema** | the columns and their types: all `STRING` for `orders`, guessed types for the others |
    | **Details** | **Number of rows**, size (**Total logical bytes**), location, when it was created and last changed |
    | **Preview** | the first rows, like `select * … limit`. It's **free**: Preview doesn't run a query |
    | **Lineage** | where the table's data came from (it appears after a few minutes, if the API is on) |

    Check **Details → Number of rows**: `customers` 6,000, `products` 200, `order_items`
    100,000, `orders` 40,150.

    **The load jobs themselves:** the bottom panel → **Job history** → **Personal history**.
    Each `bq load` is a row of type *Load* with its status. If a load fails, click it: the
    error says which line and column broke it.

### 4 · Query Bronze in BigQuery Studio
In BigQuery Studio, click **+** (new SQL query; some accounts show **SQL query** or **Compose
new query**) at the top of the editor area, and run (**Ctrl/Cmd + Enter**, or **▶ Run**). The
result appears in the **Query results** panel under the editor:

```sql
select count(*)                  as lines,
       count(distinct order_id)  as orders
from shopflow_bronze.orders;
```

- **`count(distinct col)`** counts the different values. Fewer orders than lines means some
  lines are duplicates.

| lines | orders |
|---|---|
| 40150 | 40000 |

150 lines are duplicates. Now look at `status`:

```sql
select status, count(*) as lines
from shopflow_bronze.orders
group by status
order by lines desc;
```

| status | lines |
|---|---|
| delivered | 26440 |
| placed | 3860 |
| cancelled | 3690 |
| shipped | 3684 |
| ␣Delivered␣ | 909 |
| DELIVERED | 839 |
| ␣Placed␣ | 143 |
| … | … |

(`␣` marks a space.) Twelve spellings of four statuses. Count `delivered` here and you'd miss
about 1,750 orders. That's why Silver exists.

Look at a clean table too. Click `shopflow_bronze.products` in the Explorer → **Schema**:
autodetect made `product_id` an `INTEGER`, `price` and `cost` `FLOAT`, and `name` and `category`
`STRING`.

### 5 · What did it cost?
Bottom panel → **Job history** → **Personal history** → click a query. Each query shows **Bytes
processed**, around 1 MB here, and **Bytes billed**: BigQuery bills at least **10 MB per
table** a query reads. Both are far inside the free 1 TiB.

## Check yourself
1. Why is `orders` loaded with every column as `STRING`? *(So unreadable values like `unknown`
   or `not a date` don't fail the load. Bronze keeps the raw file and Silver cleans it.)*
2. What does `--replace` do, and why is it there? *(It empties the table before loading, so a
   rerun doesn't add the rows twice.)*
3. Which query costs more: `select *` or `select order_id, status`? *(`select *`. BigQuery stores
   by column and bills the bytes of the columns a query reads.)*
