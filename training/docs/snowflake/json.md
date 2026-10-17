# JSON (VARIANT)

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

### 4 · Write the documents as JSON files
APIs and other systems hand JSON over as **files**. Write `ORDERS_JSON` out as files into a
**stage** — a place for files inside Snowflake (the [next lesson](stages.md) covers stages
in full):

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

### 5 · Query JSON files without loading them

```sql
select $1:order_id::int, $1:status::varchar
from @ORDERS_STAGE/orders/
limit 5;
```

- **`$1`** — the first column of each file row; for JSON, the whole document.

**What the file looks like.** The unload in step 4 wrote **one JSON document per line** (called
*NDJSON*; your values differ):

```
{"channel":"web","customer":{"country":"FR","id":72},"items":[…],"order_id":2,"order_ts":"2023-06-18 14:46:32.000","status":"delivered"}
{"channel":"app","customer":{"country":"US","id":4120},"items":[…],"order_id":3,"order_ts":"2023-06-18 15:02:11.000","status":"shipped"}
…
```

When Snowflake reads a file, it splits it into **rows** and **columns** using the file format:

| File format | One row is | Columns |
|---|---|---|
| `type = json` | one JSON document (here: one line) | **only one**: `$1` = the whole document, as a VARIANT |
| `type = csv` | one line | one per comma-separated field: `$1`, `$2`, `$3` … — as **text** |

So for a JSON file there's never a `$2` — the document **is** the one column, and
`$1:order_id` reaches inside it. See which file and line each row came from:

```sql
select metadata$filename, metadata$file_row_number, $1:order_id::int as order_id
from @ORDERS_STAGE/orders/
limit 3;
```

- **`metadata$filename`**, **`metadata$file_row_number`** — the file and the row number in it;
  handy to trace a bad row back to its file.

### 6 · Load JSON files into a VARIANT column

```sql
create or replace table ORDERS_RAW (
  doc       variant,
  loaded_at timestamp_ltz default current_timestamp()
);

copy into ORDERS_RAW (doc)
from (select $1 from @ORDERS_STAGE/orders/)
file_format = (format_name = JSON_FMT);

select count(*) from ORDERS_RAW;
```

- **`copy into table (doc) from (select $1 …)`** — load each file row's document into the
  `doc` column; `loaded_at` fills itself from its `default`.

One row per order again — the same documents as `ORDERS_JSON`, now loaded from files. What
`COPY` does when it meets the same file twice is in the [next lesson](stages.md).

### 7 · JSON inside a CSV column
Files from other systems often mix both: a CSV whose **2nd column holds a JSON object**, or
**two columns with two different JSON objects**. Make one from `ORDERS_JSON` — `order_id`, the
customer object and the items array, as three CSV columns:

```sql
create file format if not exists CSV_FMT
  type = csv  skip_header = 1  field_optionally_enclosed_by = '"';

copy into @ORDERS_STAGE/orders_mixed.csv
from (select doc:order_id::int        as order_id,
             to_json(doc:customer)    as customer,
             to_json(doc:items)       as items
      from ORDERS_JSON
      order by order_id
      limit 3)
file_format = (type = csv  field_optionally_enclosed_by = '"'  compression = none)
header = true  single = true;
```

- **`create file format … type = csv`** — how to read a CSV: `skip_header = 1` skips the
  header line, `field_optionally_enclosed_by = '"'` handles values wrapped in quotes.
- **`to_json(variant)`** — the JSON as **text**, so it can go into a CSV field.

The file looks like this — each JSON is wrapped in `"…"`, and every `"` inside it is doubled
(`""`) so the commas in the JSON don't split the field (your values differ):

```
ORDER_ID,CUSTOMER,ITEMS
2,"{""country"":""FR"",""id"":72}","[{""price"":412.18,""product_id"":37,""qty"":2},{""price"":18.5,""product_id"":122,""qty"":1}]"
3,"{""country"":""US"",""id"":4120}","[{""price"":99.9,""product_id"":5,""qty"":1}]"
…
```

Now read it **as CSV**, so the columns are `$1`, `$2`, `$3`:

```sql
select $1::int                                   as order_id,
       parse_json($2):country::varchar           as country,       -- JSON in column 2
       parse_json($2):id::int                    as customer_id,
       array_size(parse_json($3))                as n_items,       -- another JSON in column 3
       parse_json($3)[0]:product_id::int         as first_product
from @ORDERS_STAGE/orders_mixed.csv (file_format => 'CSV_FMT');
```

- **`(file_format => 'CSV_FMT')`** — read this file with `CSV_FMT` instead of the stage's own
  JSON format.

- **`$2`**, **`$3`** — the 2nd and 3rd CSV columns. In a CSV every column is **text**, so
  `$2:country` doesn't work yet.
- **`parse_json(text)`** — turns JSON text into a VARIANT; after that `:country`, `[0]` and
  `::int` work as on any VARIANT.
- **`[0]`** — the first element of an array (counting from 0).

To keep the JSON when loading, store it in VARIANT columns:

```sql
create or replace table ORDERS_MIXED (order_id int, customer variant, items variant);

copy into ORDERS_MIXED
from (select $1::int, parse_json($2), parse_json($3)
      from @ORDERS_STAGE/orders_mixed.csv)
file_format = (format_name = CSV_FMT);

select order_id, customer:country::varchar as country, array_size(items) as n_items
from ORDERS_MIXED;
```

| File | Where the JSON is | Read it with |
|---|---|---|
| JSON file (`type = json`) | the whole row | `$1:field` |
| CSV, JSON in the 2nd column | column 2, as text | `parse_json($2):field` |
| CSV, two JSON columns | columns 2 and 3 | `parse_json($2):…`, `parse_json($3):…` |

!!! info "Next"
    Uploading your own files and loading them into tables — from inside Snowflake and from AWS
    S3 — in [Stages: upload & load files](stages.md).

## Check yourself
1. Why cast `doc:status::varchar` before comparing it? *(It's a VARIANT — a JSON string with
   quotes. Cast to a real type first.)*
2. A JSON file is read with `$1`. When do you use `$2`? *(Never for a JSON file — the whole
   document is `$1`. `$2` is the 2nd column of a CSV; if it holds JSON text, use
   `parse_json($2)`.)*
