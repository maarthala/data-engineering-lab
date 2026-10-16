# JSON (VARIANT), stages & COPY INTO

## Concept

### VARIANT — JSON in a column
Snowflake stores semi-structured data (JSON, Avro, Parquet, ORC, XML) in a column of type
**`VARIANT`**. You don't parse it at query time like in Trino — Snowflake stores it in a
columnar way inside the micro-partitions, so querying `doc:status` is fast.

| Syntax | Means |
|---|---|
| `doc:status` | the `status` key of `doc` (still a VARIANT) |
| `doc:customer.id` | a nested key |
| `doc:items[0]` | the first element of an array |
| `doc:status::varchar` | cast to a real type — always cast before comparing or joining |
| `lateral flatten(input => doc:items)` | one row **per array element** |

You did the same in the lab with Trino's `json_extract_scalar` and `UNNEST` in
[Semi-structured data (JSON)](../unit2/json.md).

### Stages
A **stage** is a place for **files** that Snowflake can load from or unload to:

- **Internal stage** — storage inside Snowflake (`create stage …`). Upload with Snowsight or
  `PUT`.
- **External stage** — your own S3 / ADLS / GCS location, connected through a **storage
  integration** (the cloud-side permission).

A **file format** describes the files (CSV with header? JSON? Parquet?). **`COPY INTO`** loads
files into a table — or unloads a query into files. Snowflake remembers **which files it already
loaded** (load history, 64 days), so running the same `COPY` twice doesn't duplicate data.

It's the lab's ingestion flow — files in a bucket (RustFS), read into Bronze tables — as SQL
commands.

## Lab

```sql
use role accountadmin;
use warehouse PRACTICE_WH;
use schema SHOPFLOW_DEV.PRACTICE;
```

### 1 · Build JSON documents from ShopFlow
One JSON document per order, with its lines as an array — the shape an orders API would send:

```sql
create or replace table ORDERS_JSON as
select o.order_id,
       object_construct(
         'order_id', o.order_id,
         'order_ts', o.order_ts,
         'channel',  o.channel,
         'status',   o.status,
         'customer', object_construct('id', c.customer_id, 'country', c.country),
         'items',    array_agg(object_construct(
                       'product_id', i.product_id,
                       'qty',        i.quantity,
                       'price',      i.unit_price))
       ) as doc
from SHOPFLOW_DEV.BRONZE.ORDERS o
join SHOPFLOW_DEV.BRONZE.CUSTOMERS   c on c.customer_id = o.customer_id
join SHOPFLOW_DEV.BRONZE.ORDER_ITEMS i on i.order_id   = o.order_id
group by o.order_id, o.order_ts, o.channel, o.status, c.customer_id, c.country;

select doc from ORDERS_JSON limit 1;
```

- **`object_construct(key, value, …)`** — builds a JSON object.
- **`array_agg(x)`** — collects the group's values into a JSON array.

Example document:

```json
{
  "channel": "web",
  "customer": { "country": "FR", "id": 72 },
  "items": [
    { "price": 412.18, "product_id": 37, "qty": 2 },
    { "price": 18.5,   "product_id": 122, "qty": 1 }
  ],
  "order_id": 2,
  "order_ts": "2023-06-18 14:46:32.000",
  "status": "delivered"
}
```

### 2 · Read paths and cast

```sql
select doc:order_id::int            as order_id,
       doc:customer.country::varchar as country,
       doc:status::varchar           as status,
       array_size(doc:items)         as n_items
from ORDERS_JSON
limit 5;
```

- **`array_size(array)`** — number of elements.

Without `::varchar`, `doc:status` shows as `"delivered"` **with quotes** — it's still a VARIANT.

### 3 · One row per item: `lateral flatten`

```sql
select doc:order_id::int              as order_id,
       item.index                     as position,
       item.value:product_id::int     as product_id,
       item.value:qty::int            as qty,
       item.value:price::number(10,2) as price
from ORDERS_JSON,
     lateral flatten(input => doc:items) item
order by order_id, position
limit 6;
```

- **`lateral flatten(input => array)`** — a row per element. Each row has `value` (the element),
  `index` (its position) and more.

Count the rows: `100000` minus the cancelled orders' lines you deleted in the clone — back to the
grain of `ORDER_ITEMS`.

### 4 · Unload to a stage

```sql
create or replace file format JSON_FMT type = json;
create or replace stage ORDERS_STAGE file_format = JSON_FMT;

copy into @ORDERS_STAGE/orders/
from (select doc from ORDERS_JSON)
file_format = (type = json);

list @ORDERS_STAGE;
```

- **`@ORDERS_STAGE/orders/`** — a path inside the stage; `@` marks a stage.
- **`copy into @stage from (query)`** — **unload**: writes the query result as files
  (gzip-compressed by default).
- **`list @stage`** — the files in it, with size and checksum.

### 5 · Query files without loading them

```sql
select $1:order_id::int, $1:status::varchar
from @ORDERS_STAGE/orders/
limit 5;
```

- **`$1`** — the first column of each file row; for JSON, the whole document.

### 6 · Load from the stage — and see load history at work

```sql
create or replace table ORDERS_RAW (
  doc       variant,
  loaded_at timestamp_ltz default current_timestamp()
);

copy into ORDERS_RAW (doc)
from (select $1 from @ORDERS_STAGE/orders/)
file_format = (format_name = JSON_FMT);

-- the same COPY again: "0 files loaded" — each file is loaded only once
copy into ORDERS_RAW (doc)
from (select $1 from @ORDERS_STAGE/orders/)
file_format = (format_name = JSON_FMT);
```

The first `COPY` reports each file as `LOADED`. The second returns
`Copy executed with 0 files processed.` Add `force = true` to load them again anyway.

### 7 · CSV — upload a file through Snowsight
Export a CSV of ShopFlow customers into a stage, then load it into a typed table — the everyday
Bronze ingestion:

```sql
create or replace file format CSV_FMT
  type = csv  skip_header = 1  field_optionally_enclosed_by = '"';
create or replace stage CSV_STAGE file_format = CSV_FMT;

copy into @CSV_STAGE/customers.csv
from SHOPFLOW_DEV.BRONZE.CUSTOMERS
file_format = (type = csv  field_optionally_enclosed_by = '"'  compression = none)
header = true  single = true;

create or replace table CUSTOMERS_LOADED like SHOPFLOW_DEV.BRONZE.CUSTOMERS;

copy into CUSTOMERS_LOADED
from @CSV_STAGE/customers.csv
on_error = 'abort_statement';
```

- **`single = true`** — one file instead of several parallel ones.
- **`create table … like …`** — an empty table with the same columns.
- **`on_error`** — what to do with a bad row: `abort_statement` (default for `COPY`),
  `continue` (skip bad rows), `skip_file`.

Then try the UI way: download `customers.csv` from Snowsight (**Catalog → Database Explorer →
SHOPFLOW_DEV → PRACTICE → Stages → CSV_STAGE**), and upload it back as a new file with
**+ Files**. That's how you'd bring a local file into Snowflake.

!!! info "Learn next"
    **External stages** (S3 + storage integration), **`validation_mode`** (dry-run a `COPY`),
    **`infer_schema`** (columns from Parquet files), **Snowpipe** (auto-load when a file lands in
    cloud storage — like the lab's [file-arrival trigger](../recipes/file-trigger.md)) and
    **Snowpipe Streaming**.

## Check yourself
1. Why cast `doc:status::varchar` before comparing it? *(It's a VARIANT — a JSON string with
   quotes. Cast to a real type first.)*
2. A nightly job runs the same `COPY INTO` twice by mistake. Do you get duplicates? *(No — load
   history skips files already loaded.)*
