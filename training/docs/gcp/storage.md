# Cloud Storage: land the ShopFlow files

## Concept

### Buckets and objects
**Cloud Storage** is Google's object storage, the same idea as the lab's **My files** (S3-compatible
storage). You make a **bucket** (a named container in one location) and put **objects**
(files) in it. An object's address is a URL:

```
gs://shopflow-de-4821-shopflow/shopflow/orders/orders_2024-06-01_2026-05-31.csv
     └──────── bucket ────────┘└──────────────── object name ────────────────┘
```

There are no real folders. `shopflow/orders/` is just the start of the object name. The console
draws it as folders anyway, and a command can read every object that starts with it
(`gs://…/shopflow/orders/*.csv`).

### The landing zone
The bucket is ShopFlow's **landing zone**: files arrive here exactly as the source system wrote
them, and nothing edits them. Every later step **reads** from here and writes somewhere else.
Keep one folder per table:

```
gs://<bucket>/
├── shopflow/customers/      customers.csv
├── shopflow/products/       products.csv
├── shopflow/orders/         orders_*.csv        ← messy on purpose
├── shopflow/order_items/    order_items_*.csv
├── code/                    the Dataflow pipeline (later)
├── rejected/                rows Dataflow can't read (later)
└── tmp/                     scratch space for Dataflow and BigQuery loads
```

A new file for a table simply goes into its folder. Loads read `folder/*.csv`, so they pick it up
with no code change. You'll see this with the new day's orders in the Composer lesson.

### Storage classes
A bucket has a default **storage class**: **Standard** for data you read often, and **Nearline**
/ **Coldline** / **Archive** for data you rarely read (cheaper to store, more expensive to read).
Landing files that pipelines read every day belong in **Standard**.

## Lab

### 1 · Download the ShopFlow files
[:material-download: **shopflow_gcp_files.zip**](../assets/gcp/shopflow_gcp_files.zip){: download="shopflow_gcp_files.zip" }
(1 MB) — four CSV files, ShopFlow's sales from 2024-06-01 to 2026-05-31.

| File | Rows | Notes |
|---|---|---|
| `customers/customers.csv` | 6,000 | clean |
| `products/products.csv` | 200 | clean, with `price` and `cost` |
| `orders/orders_2024-06-01_2026-05-31.csv` | 40,150 | **messy**: duplicate lines, `DELIVERED` / ` Delivered `, blank currency, unreadable customer ids and timestamps |
| `order_items/order_items_2024-06-01_2026-05-31.csv` | 100,000 | clean, one row per product in an order |

The tables and columns are the lab's [ShopFlow schema](../unit0/schema.md). These are files
instead of a database, and they hold only the four core tables.

### 2 · Create the bucket
**The bucket's name** is your project ID followed by `-shopflow`, e.g.
**`shopflow-de-4821-shopflow`**. You stored it in the variable **`$BUCKET`** in
[step 3 of the setup](setup.md), so the commands in this unit write `$BUCKET` and Cloud Shell
fills in your name. Check it:

```bash
echo $BUCKET
```

```
shopflow-de-4821-shopflow
```

If it prints an empty line, Cloud Shell was restarted before `~/.bashrc` had the variables: run
the setup's step 3 again. In the console, look for this name. Wherever a lesson writes
`<bucket>`, it means this name too.

Now create it in Cloud Shell:

```bash
gcloud storage buckets create gs://$BUCKET --location=$REGION \
    --default-storage-class=STANDARD --uniform-bucket-level-access
```

- **`gcloud storage buckets create gs://NAME`** makes a bucket. The name must be unique worldwide;
  yours starts with the project ID, so it is.
- **`--location=$REGION`** puts it in one region (`us-central1`), the same one as BigQuery later.
- **`--default-storage-class=STANDARD`** for data read often.
- **`--uniform-bucket-level-access`** means permissions are set for the whole bucket with IAM,
  never per file. That's simpler and is what Google recommends.

Example output:

```
Creating gs://shopflow-de-4821-shopflow/...
```

### 3 · Upload the files: two ways
**a) In the console** (the way a person would do it)

1. Unzip `shopflow_gcp_files.zip` on your computer. You get a folder `shopflow/`.
2. Console → **Cloud Storage** → **Buckets** → your bucket (`<project-id>-shopflow`, the name `echo $BUCKET` printed).
3. **Upload** → **Upload folder** → choose the `shopflow` folder.

**b) From Cloud Shell** (the way a script would do it)

```bash
curl -sLO https://maarthala.github.io/data-engineering-lab/assets/gcp/shopflow_gcp_files.zip
unzip -o shopflow_gcp_files.zip
gcloud storage cp --recursive shopflow gs://$BUCKET/
```

- **`curl -sLO URL`** downloads the file into the current folder (`-s` quiet, `-L` follow
  redirects, `-O` keep its name).
- **`unzip -o`** extracts the zip into the current folder (`-o` overwrites without asking).
- **`gcloud storage cp SOURCE gs://BUCKET/`** copies files into the bucket. **`--recursive`**
  copies a whole folder, keeping its sub-folders in the object names.

Doing both is fine: the second upload overwrites the same object names.

### 4 · Look at what landed
```bash
gcloud storage ls --recursive gs://$BUCKET/shopflow/
```

- **`gcloud storage ls`** lists objects. **`--recursive`** goes into every "folder".

Example output:

```
gs://shopflow-de-4821-shopflow/shopflow/:

gs://shopflow-de-4821-shopflow/shopflow/customers/:
gs://shopflow-de-4821-shopflow/shopflow/customers/customers.csv

gs://shopflow-de-4821-shopflow/shopflow/order_items/:
gs://shopflow-de-4821-shopflow/shopflow/order_items/order_items_2024-06-01_2026-05-31.csv

gs://shopflow-de-4821-shopflow/shopflow/orders/:
gs://shopflow-de-4821-shopflow/shopflow/orders/orders_2024-06-01_2026-05-31.csv

gs://shopflow-de-4821-shopflow/shopflow/products/:
gs://shopflow-de-4821-shopflow/shopflow/products/products.csv
```

Now read the start of the messy file without downloading it:

```bash
gcloud storage cat gs://$BUCKET/shopflow/orders/*.csv | head -8
```

- **`gcloud storage cat`** prints an object's content. **`| head -8`** keeps the first 8 lines.

Example output:

```
order_id,customer_id,channel,order_ts,status,currency,promo_id
11555,801,app,2025-08-01 01:44:07,delivered,GBP,
24226,350,web,2024-10-08 13:27:22,delivered,GBP,
23126,172,marketplace,2026-01-21 02:23:47,cancelled,USD,
8984,956,app,2024-08-07 07:20:39,delivered,GBP,
...
```

An empty last column means the order used no promotion.

Now look for the mess:

```bash
gcloud storage cat gs://$BUCKET/shopflow/orders/*.csv | grep -E "unknown|N/A|not a date| Delivered " | head -4
```

- **`grep -E "a|b|c"`** keeps the lines that contain any of the words.

Output:

```
32188,436,marketplace,not a date,delivered,USD,
3246,274,marketplace,2026-01-23 15:48:50, Delivered ,USD,
36773,217,marketplace,2025-04-13 21:36:03, Delivered ,USD,
988,301,app,2025-04-14 10:05:20, Delivered ,INR,1
```

Further down there are customer ids like `unknown` and `N/A`
(`24418,unknown,app,2025-09-18 07:04:52,delivered,EUR,`), `DELIVERED` in capitals, and some
lines that appear twice.

That's what Silver has to fix: the Dataflow lesson cleans these lines and sets aside the ones it
can't fix.

### 5 · Who may read the bucket?
Console → your bucket → **Permissions**. You'll see that you (the project **Owner**) can do
everything, and the bucket is **not public** (*Public access: Not public*). Dataflow, BigQuery
and Composer read it with their own **service accounts**, which get access in their lessons.

## Check yourself
1. Is `shopflow/orders/` a folder? *(No. It's the start of the object names. The console only
   draws it as one.)*
2. A new orders file arrives tomorrow. Where does it go, and what code changes?
   *(Into `shopflow/orders/`. Nothing changes, because loads read `shopflow/orders/*.csv`.)*
3. Which storage class for daily landing files, and why? *(Standard. They're read every day, and
   colder classes charge for every read.)*
