# Semi-structured data (JSON)

## Concept
Not all data arrives as neat rows and columns. APIs, application logs, and — most importantly
for ShopFlow — the **event stream** (the clickstream from
[the schema page](../unit0/schema.md#the-event-stream-clickstream)) arrive as **JSON**: nested,
flexible, and schema-light. A huge part of data engineering is **parsing (flattening)** that
JSON into typed columns you can join and aggregate.

### What JSON is (and why events arrive this way)
**JSON** (JavaScript Object Notation) is a text format that stores data as **key/value pairs**
inside `{ … }`, like `{"event":"add_to_cart","qty":3}`. Three properties matter here:

- **Nested** — a value can itself be another object or a list: `{"user":{"id":10}}` or
  `{"items":[{"sku":"A"},{"sku":"B"}]}`. Structure can go many levels deep.
- **Flexible** — every record can carry *different* keys. A `search` event has a `term`; a
  `page_view` doesn't. Nothing forces them to match.
- **Schema-light** — there's no table definition saying "these columns, these types." The
  shape lives inside each string.

That flexibility is exactly why **events, APIs, and logs** use JSON: the app that emits them can
add a new field tomorrow without anyone changing a database table first. The cost is pushed
**downstream, to you** — the JSON is just text until someone parses it into columns.

This is **schema-on-read** ([1.2](../unit1/lakehouse.md)) in action: the JSON has no fixed
shape until *you* impose one at query time. That's the opposite of a normal SQL table
(**schema-on-write**), where the columns and types are fixed the moment you insert a row. With
schema-on-read you store the raw text first and decide *later* which fields to pull out.

!!! info "The tools you'll use to impose that shape"
    Trino gives you a small toolkit of JSON functions. You'll meet each one in the Lab, but
    here's the map so the names aren't a surprise:

    - **`json_extract_scalar(json, path)`** — pull **one leaf value** out as **text**. A "leaf"
      is a single scalar: a string, number, or boolean sitting at the end of a path. The result
      always comes back as `varchar`, even for numbers — you type it yourself with `CAST`.
    - **`json_extract(json, path)`** — pull a **sub-object or array**, which is **still JSON**.
      Use this when the thing you want isn't a single value but a nested chunk you'll dig into
      further.
    - **`CAST(… AS <type>)` / `TRY_CAST(… AS <type>)`** — turn the extracted *text* into a real
      typed value (an `integer`, a `date`, …) so you can do maths and comparisons with it.
    - **JSONPath** — the little address language you hand these functions: `$.field`, nested
      `$.a.b`, array element `$.items[0].sku`. More on this below.

### Reading a path: JSONPath in 30 seconds
The second argument to every extract function is a **JSONPath** — an address that walks from the
top of the document down to the value you want:

- **`$`** means "the root" — the whole JSON document.
- **`$.field`** — the value under key `field` at the top level.
- **`$.a.b`** — go into object `a`, then read its `b` (a **nested** lookup).
- **`$.items[0].sku`** — go into the `items` **array**, take element **`[0]`** (arrays are
  zero-based, so `[0]` is the first, `[1]` the second), then read that element's `sku`.

If the path points at something that isn't there, the function returns **`NULL`** rather than
erroring — which is what makes flattening ragged data painless.

## Lab
> Run these in **SQLPad** on the **Lakehouse (Trino) — &lt;your username&gt;** connection (not *ShopFlow — OLTP*) (see [2.1](intro.md)) — the write steps need a
> tool that can write (Superset SQL Lab is read-only). They go to **your own** catalog
> `demouser_lake` (fully qualified).

!!! note "Signed in with your own account?"
    The examples use the default lab account **`demouser`** — its catalog is `demouser_lake`. If you
    registered your own account, replace `demouser` with your username (SQLPad: find & replace,
    `Ctrl/Cmd + H`), e.g. `ravi_lake`.


### 1 · Parse fields straight out of a JSON string
Before touching a table, let's prove the functions on a literal string so you can see input and
output side by side. This one JSON string holds three fields; we pull two of them:

```sql
SELECT
  json_extract_scalar('{"event":"add_to_cart","product_id":42,"qty":3}', '$.event')      AS event_type,
  CAST(json_extract('{"event":"add_to_cart","product_id":42,"qty":3}', '$.qty') AS integer) AS qty;
```

**Read it clause by clause:**

- **`json_extract_scalar('{…}', '$.event')`** — reach into the JSON, follow the path `$.event`
  to the leaf value `"add_to_cart"`, and return it as **text**. **`AS event_type`** just gives
  that output column a readable name (an **alias**).
- **`json_extract('{…}', '$.qty')`** — follow `$.qty` and return `3`, but as a **JSON fragment**,
  not a plain number. (`json_extract` is the "still-JSON" cousin; here the fragment happens to be
  a lone number.)
- **`CAST(… AS integer)`** — convert that fragment's text into a real **`integer`** so it behaves
  like a number in later maths. Without the cast you'd be stuck with text.
- The result is a single row: `event_type = add_to_cart`, `qty = 3`.

!!! note "`json_extract_scalar` vs `json_extract` — which do I use?"
    Reach for **`json_extract_scalar`** whenever you want a **single value** (a string, number,
    or boolean) — it hands you clean text you can `CAST`. Reach for **`json_extract`** when the
    thing at that path is an **object or array** you need to keep as JSON and dig into further.
    A common beginner surprise: `json_extract` on a leaf gives you JSON-flavoured text (a string
    stays wrapped in quotes), which is why `json_extract_scalar` is the everyday workhorse for
    flattening.

### 2 · Model the ShopFlow event stream
Real events don't arrive one string at a time — they land as **rows of raw JSON text** in a
landing (Bronze-ish) table. Here we build a tiny one to practise on. Notice the table has only
**two columns**: an `event_id` and a `payload` of type **`varchar`** (plain text). The entire
JSON document lives *inside* that one text column — the classic "store it raw first" shape:

```sql
CREATE SCHEMA IF NOT EXISTS demouser_lake.sandbox;

DROP TABLE IF EXISTS demouser_lake.sandbox.raw_events;
CREATE TABLE demouser_lake.sandbox.raw_events (event_id int, payload varchar);

INSERT INTO demouser_lake.sandbox.raw_events VALUES
  (1, '{"type":"page_view","customer_id":10,"product_id":42,"channel":"web"}'),
  (2, '{"type":"add_to_cart","customer_id":10,"product_id":42,"qty":2,"channel":"web"}'),
  (3, '{"type":"search","customer_id":null,"term":"keyboard","channel":"app"}');
```

**Read it clause by clause:**

- **`CREATE SCHEMA IF NOT EXISTS demouser_lake.sandbox`** — make a scratch schema (a namespace for
  tables) in the `iceberg` catalog, but only if it isn't already there.
- **`DROP TABLE IF EXISTS …`** — delete any previous version so this lab is repeatable; the
  `IF EXISTS` stops it erroring on the first run when there's nothing to drop.
- **`CREATE TABLE … (event_id int, payload varchar)`** — define the landing table: an integer id
  and the raw JSON as text.
- **`INSERT INTO … VALUES (…)`** — load three sample events. Look at how **ragged** they are:
  event 3 (`search`) has a `term` but no `product_id` or `qty`; the others have `product_id` but
  no `term`; event 3's `customer_id` is even an explicit `null`. That unevenness is normal — and
  it's exactly what the next query has to survive.

### 3 · Flatten JSON → typed columns
This is **the** move of the lesson: turn one raw JSON column into a wide table of clean, typed
columns — the raw → **Bronze/Silver** step for the event stream. One call to
`json_extract_scalar` per field we want, wrapped in `TRY_CAST` wherever we need a real number:

```sql
SELECT event_id,
       json_extract_scalar(payload, '$.type')                        AS event_type,
       TRY_CAST(json_extract_scalar(payload, '$.customer_id') AS integer) AS customer_id,
       TRY_CAST(json_extract_scalar(payload, '$.product_id')  AS integer) AS product_id,
       TRY_CAST(json_extract_scalar(payload, '$.qty')         AS integer) AS qty,
       json_extract_scalar(payload, '$.channel')                     AS channel,
       json_extract_scalar(payload, '$.term')                        AS search_term
FROM demouser_lake.sandbox.raw_events
ORDER BY event_id;
```

**Read it clause by clause:**

- **`json_extract_scalar(payload, '$.type') AS event_type`** — pull the `type` leaf from each
  row's `payload` as text. This one stays text (event types *are* strings), so no cast.
- **`TRY_CAST(json_extract_scalar(payload, '$.customer_id') AS integer) AS customer_id`** — pull
  `customer_id` as text, then convert it to an `integer`. The three `id`/`qty` fields all get this
  treatment so downstream maths and joins see real numbers, not strings.
- **`json_extract_scalar(payload, '$.channel')`** and **`… '$.term'`** — two more leaf pulls, left
  as text.
- **`FROM demouser_lake.sandbox.raw_events`** — do this for **every row** in the landing table.
- **`ORDER BY event_id`** — sort the output by id so the three events come back in order.

**What the flattened result looks like:** three rows, seven tidy columns. Where a field was
**absent**, the extract returns `NULL` — so event 1 (`page_view`) has `qty = NULL` and
`search_term = NULL`; event 3 (`search`) has `product_id = NULL` and `qty = NULL` but a filled-in
`search_term`. Event 3's `customer_id` was an explicit `null` in the JSON, which also lands as SQL
`NULL`. You've gone from one opaque text blob per row to a table you could hand to anyone.

!!! warning "Why `TRY_CAST`, not plain `CAST`?"
    A plain **`CAST`** *errors and aborts the whole query* the moment it hits a value it can't
    convert — one malformed `"qty":"lots"` in a million-row stream and your job dies.
    **`TRY_CAST`** does the same conversion but, on any value it can't parse (or a missing field),
    quietly returns **`NULL`** instead of throwing. With ragged, real-world semi-structured data
    that's almost always what you want: keep the good rows, null out the bad cells, finish the run.

Notice `search_term` is `NULL` for non-search events and `qty` is `NULL` where absent —
exactly the ragged shape semi-structured data produces, handled cleanly by `TRY_CAST`.

### 4 · Nested objects and arrays — reach deeper with the path

JSON isn't always flat key/value pairs — real payloads nest objects inside objects and hold
**arrays** (ordered lists) of them. The path syntax handles both: chain keys with dots, and index
into arrays with `[n]`.

```sql
SELECT
  json_extract_scalar('{"items":[{"sku":"A"},{"sku":"B"}]}', '$.items[1].sku') AS second_sku,
  json_array_length('[1,2,3,4]')                                                AS n_items;
```

**Read it clause by clause:**

- **`'$.items[1].sku'`** — walk into the `items` array, take element **`[1]`** (the **second**
  element, because indexing starts at `0`), then read that object's `sku`. Result: `B`. Swap to
  `[0]` and you'd get `A`.
- **`json_array_length('[1,2,3,4]')`** — count the elements in a JSON array. Result: `4`. This is
  handy for "how many items were in the cart?" without unpacking the array first.

!!! note "Arrays are zero-based"
    `[0]` is the first element, `[1]` the second, and so on. Ask for an index past the end
    (say `$.items[9]` on a two-item list) and you get `NULL` — no error — same forgiving
    behaviour as a missing key.

### 5 · Aggregate the flattened events
Because you can extract a field inline, you can also **group by** it — asking a real analytics
question straight over the raw stream. Here: how many events of each type?

```sql
SELECT json_extract_scalar(payload, '$.type') AS event_type,
       count(*) AS events
FROM demouser_lake.sandbox.raw_events
GROUP BY json_extract_scalar(payload, '$.type')
ORDER BY events DESC;
```

**Read it clause by clause:**

- **`json_extract_scalar(payload, '$.type') AS event_type`** — pull the event type out of each
  row's JSON, same as before.
- **`count(*) AS events`** — count how many rows fall into each group.
- **`GROUP BY json_extract_scalar(payload, '$.type')`** — bucket the rows by event type. Note we
  repeat the *whole expression* here, not the alias — grouping happens before the `SELECT` alias
  exists (see [2.2](joins-aggregations.md) on execution order).
- **`ORDER BY events DESC`** — most-frequent type first.

**The result:** one row per distinct `event_type` (`page_view`, `add_to_cart`, `search`), each
with a count of `1` in our tiny table. On the real stream this is the shape of every "top events"
dashboard.

!!! tip "Parse once, store typed"
    Extracting JSON on every query is slow and repetitive. In a real pipeline you **flatten
    once** — parse the raw JSON into a typed Silver table (`event_type`, `customer_id`, …) —
    then everyone queries clean columns. That's the [Medallion](../unit1/medallion.md) idea
    applied to the event stream.

## Challenge
From `demouser_lake.sandbox.raw_events`, produce one row per event with `event_id`, `event_type`,
`customer_id`, and a derived `is_anonymous` flag (`true` when `customer_id` is `NULL`). Then
count how many events are anonymous vs. known.

!!! tip "Which functions do you need?"
    Pull `type` and `customer_id` with **`json_extract_scalar`** (wrap `customer_id` in
    **`TRY_CAST … AS integer`** for the typed column). Build the flag with an **`IS NULL`** test —
    it returns `true`/`false` directly. For the summary, wrap that test in a **`CASE WHEN … THEN
    … ELSE … END`** to label each row `anonymous`/`known`, then **`GROUP BY`** the label and
    `count(*)`. (`GROUP BY 1` means "group by the first SELECT column" — a handy shorthand.)

??? note "Solution"
    ```sql
    -- per-event, with the anonymous flag
    SELECT event_id,
           json_extract_scalar(payload, '$.type') AS event_type,
           TRY_CAST(json_extract_scalar(payload, '$.customer_id') AS integer) AS customer_id,
           json_extract_scalar(payload, '$.customer_id') IS NULL AS is_anonymous
    FROM demouser_lake.sandbox.raw_events
    ORDER BY event_id;

    -- the summary
    SELECT CASE WHEN json_extract_scalar(payload, '$.customer_id') IS NULL
                THEN 'anonymous' ELSE 'known' END AS who,
           count(*) AS events
    FROM demouser_lake.sandbox.raw_events
    GROUP BY 1
    ORDER BY events DESC;
    ```

!!! tip "🎯 The same idea on Azure, Databricks, Snowflake & Fabric"
    Every platform parses JSON — but the **function names differ** (this is the one place SQL
    isn't copy-paste portable), so learn the *concept* here and swap the syntax there:

    - **Azure Databricks** — the `payload:type` colon syntax, or `get_json_object` / `from_json`
      with a schema; explode arrays with `explode()`.
    - **Snowflake** — load JSON into a `VARIANT` column, navigate with `payload:type`, and
      expand arrays with `FLATTEN`.
    - **Microsoft Fabric** — `JSON_VALUE` / `OPENJSON` (T-SQL) in the Warehouse; `from_json`
      in a Spark Lakehouse.
    - **Azure Data Factory** — the **Flatten** transformation in a Mapping Data Flow.

    The *skill* — flatten semi-structured data into typed columns — is identical everywhere.

## Key terms, at a glance
| Term | Plain meaning |
|---|---|
| **JSON** | Text format of nested `{key: value}` pairs; flexible, schema-light |
| **Semi-structured data** | Data with a flexible/nested shape (JSON, logs, events) |
| **Schema-on-read** | Impose structure at query time, not on ingest (vs schema-on-write) |
| **`json_extract_scalar`** | Pull one leaf value out of JSON **as text** |
| **`json_extract`** | Pull a sub-object/array — the result is **still JSON** |
| **JSONPath** | The address language: `$.field`, `$.a.b`, `$.items[0].sku` |
| **JSON path** | `$.a.b`, `$.items[0].sku` — how you address a field |
| **`CAST`** | Convert extracted text to a real type — **errors** on bad values |
| **`TRY_CAST`** | Same, but returns **`NULL`** instead of erroring on bad/ragged values |
| **`json_array_length`** | Count the elements in a JSON array |
| **Flatten** | Turn nested JSON into flat, typed columns (raw → Bronze/Silver) |
| **`VARIANT`** | (Snowflake) a column type that stores JSON natively |

## You can now…
- Explain semi-structured data and schema-on-read
- Extract leaf values, nested fields, and array elements from JSON
- Flatten a JSON event payload into typed columns with `json_extract_scalar` + `TRY_CAST`
- Aggregate over parsed JSON, and name the equivalent functions on each cloud platform
