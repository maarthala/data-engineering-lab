# PyArrow — Parquet & the columnar bridge

## Concept
**Apache Arrow** is a language-agnostic **columnar in-memory format**, and **PyArrow** is its
Python library. You rarely program *directly* against it, but it's quietly everywhere in data
engineering — and worth understanding because it does two jobs no other tool does as well:

1. **It reads and writes Parquet.** The [columnar file format from 1.3](../unit1/formats.md) —
   the one the lakehouse stores everything in — is read/written by Arrow. `pandas.read_parquet`
   is PyArrow under the hood.
2. **It's the zero-copy bridge between tools.** pandas, Spark (`toPandas()`), Trino, DuckDB, and
   Snowflake all speak Arrow to hand data to each other *without re-serializing* — it's the
   lingua franca that makes moving data between engines fast.

Think of it this way: **Parquet is Arrow written to disk; Arrow is Parquet loaded into memory.**

### Tying back to {lesson:unit1/formats.md}: row vs column, disk vs memory
In [1.3 (file formats)](../unit1/formats.md) you met **Parquet** as a *columnar file on disk* —
values of one column stored together, which is what makes "read only the `amount` column" cheap.
Arrow is the same **columnar** idea, but for data held *in memory* while a program is running.
Three words, one family:

- **Parquet** — columnar layout **on disk** (a file you can store, copy, and reopen later).
- **Arrow** — columnar layout **in memory** (a live table your program can read column-by-column).
- **PyArrow** — the Python **library** that reads/writes Parquet files, holds Arrow tables, and
  hands them to pandas with little or no copying.

!!! info "Why Arrow is the *columnar bridge* (and 'zero-copy')"
    pandas, Spark, Trino, DuckDB and Snowflake each store data in their own internal layout. To
    move a table between two of them, *something* has to agree on a shape. Arrow **is** that
    agreed shape: because they all lay bytes out the *same* columnar way, one tool can hand its
    Arrow buffers to another **without re-serializing** — often the receiver just *points* at the
    same memory instead of copying it. That "point, don't copy" trick is what **zero-copy** means,
    and it's why `spark.toPandas()` or a Trino Python fetch is fast rather than a slow row-by-row
    translation.

### The imports
Two modules, aliased the way the whole ecosystem writes them:

```python
import pyarrow as pa
import pyarrow.parquet as pq
```

**Read it step by step:**

- **`import pyarrow as pa`** — the core Arrow library. `pa` is where the in-memory table type
  lives; you'll use it as `pa.Table`.
- **`import pyarrow.parquet as pq`** — the Parquet submodule, conventionally nicknamed **`pq`**.
  Everything that touches a `.parquet` *file* (write, read, peek at metadata) comes from here.

Keeping these two names straight is the whole mental model: **`pa` = Arrow in memory**,
**`pq` = Parquet on disk**.

## Lab

### An Arrow Table
An **Arrow Table** is Arrow's in-memory table type (`pa.Table`). It's columnar and **strongly
typed** — every column has one explicit type (`int64`, `string`, `double`), unlike a loose Python
list where a column could secretly mix ints and strings. That declared type list is called the
table's **schema**.

```python
import pandas as pd

df = pd.DataFrame({
    "order_id": [1, 2, 3],
    "country":  ["US", "UK", "DE"],
    "amount":   [120.0, 40.0, 300.0],
})

table = pa.Table.from_pandas(df)   # pandas → Arrow
print(table.schema)                # typed columns: order_id: int64, country: string, amount: double
print(table.num_rows, table.num_columns)
```

**Read it step by step:**

- **`df = pd.DataFrame({...})`** — build a tiny 3-row pandas DataFrame with three columns. This is
  ordinary pandas; nothing Arrow yet.
- **`pa.Table.from_pandas(df)`** — the **pandas → Arrow** conversion. `from_pandas` is a
  *constructor* on `pa.Table`: it takes a DataFrame and returns a new **Arrow Table** holding the
  same data in Arrow's columnar layout. Along the way it **infers a schema** — it looks at each
  pandas column and picks an Arrow type (`int64`, `string`, `double`).
- **`table.schema`** — the table's **schema**: the ordered list of `(column name, type)` pairs.
  Printing it is how you confirm Arrow typed your data the way you expected.
- **`table.num_rows`, `table.num_columns`** — simple size attributes (here `3` and `3`).

!!! note "What you produced"
    `table` is now an **Arrow Table** living in memory — the same data as `df`, but in Arrow's
    columnar format and carrying an explicit schema. It is *not* a file yet; nothing has been
    written to disk.

### Write & read Parquet
This is the everyday use — persist a DataFrame as Parquet and read it back. This is the moment the
in-memory Arrow Table becomes an on-disk Parquet file, and vice versa:

```python
pq.write_table(table, "/tmp/orders.parquet")           # write Parquet
back = pq.read_table("/tmp/orders.parquet")            # read it back (Arrow Table)
back.to_pandas()                                       # Arrow → pandas

# pandas' own helpers use PyArrow underneath:
df.to_parquet("/tmp/orders2.parquet")
pd.read_parquet("/tmp/orders2.parquet")
```

**Read it step by step:**

- **`pq.write_table(table, "/tmp/orders.parquet")`** — takes an Arrow Table and **serializes it to
  a Parquet file** at that path. This is the "Arrow-in-memory → Parquet-on-disk" step. The
  schema is written into the file too, so a reader later knows the types without guessing.
- **`back = pq.read_table("/tmp/orders.parquet")`** — the reverse: **read the Parquet file back
  into a fresh Arrow Table**. `back` holds the same data and schema as `table` did.
- **`back.to_pandas()`** — the **Arrow → pandas** conversion. `to_pandas()` is a method *on an
  Arrow Table* that hands the columns back to pandas (often zero-copy for numeric columns). Use it
  when you want to keep working in pandas after loading Parquet.
- **`df.to_parquet(...)` / `pd.read_parquet(...)`** — pandas' own shortcuts. They do the exact same
  thing, but **call PyArrow underneath**. So `pandas.read_parquet` *is* `pq.read_table(...)
  .to_pandas()` with a nicer name — proving Arrow is quietly everywhere even when you never type
  `pa` or `pq`.

!!! note "What you produced"
    Two files on disk (`/tmp/orders.parquet`, `/tmp/orders2.parquet`) and, in memory, the
    round-tripped Arrow Table `back`. `pq.write_table` ↔ `pq.read_table` are the on-disk pair;
    `to_pandas` ↔ `from_pandas` are the pandas-bridge pair.

### Column projection — the columnar win
Because Parquet stores each column separately, you can read **only the columns you need** — far
less I/O than reading the whole file (the same idea as Trino's projection pushdown in Unit {unit:unit2/intro.md}):

```python
pq.read_table("/tmp/orders.parquet", columns=["order_id", "amount"]).to_pandas()
```

**Read it step by step:**

- **`columns=["order_id", "amount"]`** — the key argument. It tells `read_table` to fetch **only
  those two columns** from disk and skip `country` entirely. Because Parquet stores each column in
  its own contiguous block, the reader can jump straight to the blocks it wants and never touch the
  bytes of `country` — that's real I/O saved, not just filtering after loading.
- **`.to_pandas()`** — convert the 2-column Arrow Table result into a pandas DataFrame, as before.

!!! info "This is 'column projection'"
    Reading a subset of columns is called **projection**. On a 3-column toy file the saving is
    tiny, but on a real table with 200 columns and millions of rows, asking for 2 columns instead
    of 200 is the difference between seconds and minutes. It's the exact same idea as Trino's
    *projection pushdown* from Unit {unit:unit2/intro.md} — the engine pushes "I only need these columns" down to the
    storage layer.

### Inspect Parquet without loading it
Read just the metadata — schema, row counts, row groups — instantly, without scanning data:

```python
meta = pq.read_metadata("/tmp/orders.parquet")
print(meta.num_rows, meta.num_row_groups)
print(pq.read_schema("/tmp/orders.parquet"))
```

**Read it step by step:**

- **`pq.read_metadata(...)`** — every Parquet file has a small **footer** describing itself: how
  many rows, how it's chunked, per-column statistics. This reads *only that footer*, not the data.
  It's near-instant even on a huge file.
- **`meta.num_rows`** — total row count, read straight from the footer.
- **`meta.num_row_groups`** — how many **row groups** the file has. A *row group* is a horizontal
  slice of the file (a batch of rows) stored together; a big Parquet file is split into several so
  a reader can process, skip, or parallelize slice-by-slice. Our tiny file has just one.
- **`pq.read_schema(...)`** — pull just the **schema** (column names + types) from the footer,
  again without scanning the data.

!!! note "What you produced"
    `meta` is a metadata object, not the data. Reading metadata is how tools answer "what's in this
    file and how big is it?" instantly — and how they later decide which row groups they can skip.

### Partitioned datasets + predicate pushdown
Real lakehouse data is **partitioned** into folders (like the `dt=…` history in Unit {unit:unit4/fundamentals.md}). PyArrow's
`dataset` API reads a whole partitioned tree and pushes filters down so it skips folders it
doesn't need:

```python
import pyarrow.dataset as ds

# write a partitioned dataset (partition folders by country)
pq.write_to_dataset(table, root_path="/tmp/orders_ds", partition_cols=["country"])

dataset = ds.dataset("/tmp/orders_ds", format="parquet", partitioning="hive")
# read only US rows, only two columns — Arrow prunes partitions + columns
dataset.to_table(filter=ds.field("country") == "US",
                 columns=["order_id", "amount"]).to_pandas()
```

**Read it step by step:**

- **`import pyarrow.dataset as ds`** — the `dataset` submodule (aliased `ds`) works with a *tree of
  Parquet files* as if it were one table, instead of one file at a time.
- **`pq.write_to_dataset(table, root_path=..., partition_cols=["country"])`** — writes the table
  out as a **partitioned dataset**: it splits rows by the value of `country` and drops each group
  into its own folder (`country=US/`, `country=UK/`, `country=DE/`). The column value becomes the
  folder name — this is the standard "Hive-style" partition layout.
- **`ds.dataset(..., format="parquet", partitioning="hive")`** — opens that folder tree as one
  logical `dataset`. `partitioning="hive"` tells it to read the `country=US` folder names back into
  a real `country` column.
- **`dataset.to_table(filter=..., columns=...)`** — materialize the dataset into a single Arrow
  Table, applying two prunings at once:
    - **`filter=ds.field("country") == "US"`** — a **predicate**. `ds.field("country")` refers to
      the `country` column; the whole expression means "keep only US rows". Because `country` is
      the partition, Arrow can skip the `UK` and `DE` folders **without opening them at all** —
      this is **partition pruning / predicate pushdown**.
    - **`columns=["order_id", "amount"]`** — the same **column projection** as before, layered on
      top.
- **`.to_pandas()`** — hand the pruned result back to pandas.

!!! info "Two kinds of skipping"
    This one call skips work along both axes: **rows** (don't open non-US partition folders) and
    **columns** (don't read `country`'s data blocks). Reading less is the entire performance story
    of a columnar, partitioned lakehouse — and it's the same principle whether the engine is
    PyArrow here, Spark, or Trino.

!!! tip "Why this matters for the lakehouse"
    Every Parquet file in `iceberg.bronze/silver/gold` (Unit {unit:unit4/fundamentals.md}) is Arrow-on-disk. When Spark does
    `.toPandas()` or Trino returns rows to the Python client, Arrow is the wire format. Learning
    it explains *why* columnar + partition pruning make the whole lakehouse fast.

## Challenge
Write the 3-row `df` to `/tmp/shopflow.parquet`, then use PyArrow to (a) print the schema and row
count from the **metadata only** (without reading the data), and (b) read back **only** the
`country` and `amount` columns.

??? note "Solution"
    ```python
    import pyarrow as pa, pyarrow.parquet as pq
    pq.write_table(pa.Table.from_pandas(df), "/tmp/shopflow.parquet")

    # (a) metadata only — no data scan
    meta = pq.read_metadata("/tmp/shopflow.parquet")
    print("rows:", meta.num_rows)
    print(pq.read_schema("/tmp/shopflow.parquet"))

    # (b) column projection
    pq.read_table("/tmp/shopflow.parquet", columns=["country", "amount"]).to_pandas()
    ```

!!! tip "🎯 The same Arrow/Parquet on Azure, Databricks, Snowflake & Fabric"
    Arrow and Parquet are **industry standards**, not a local quirk:

    - **Azure Databricks / Microsoft Fabric** — Delta/Parquet on ADLS/OneLake *is* this format;
      Spark uses Arrow for `toPandas()` and Python UDF transport.
    - **Snowflake** — Iceberg tables store Parquet; the Python connector returns results as Arrow
      (`fetch_arrow_all` / `to_pandas`).
    - **Azure Data Factory** — reads/writes Parquet natively as a Copy source/sink.

    Parquet-on-storage + Arrow-in-transit is the same everywhere — this knowledge is fully portable.

## Key terms, at a glance
| Term | Plain meaning |
|---|---|
| **Apache Arrow** | Language-agnostic *columnar in-memory* format |
| **PyArrow** | Python library for Arrow + Parquet (`pa` = Arrow, `pq` = Parquet) |
| **Arrow Table** (`pa.Table`) | A typed, columnar in-memory table |
| **Parquet** | Columnar *on-disk* format (Arrow written to storage) |
| **Schema** | The ordered list of `(column name, type)` pairs a table carries |
| **`from_pandas` / `to_pandas`** | Convert pandas → Arrow / Arrow → pandas |
| **`pq.write_table` / `pq.read_table`** | Arrow Table → Parquet file / Parquet file → Arrow Table |
| **Metadata (footer)** | Self-description in a Parquet file — rows, row groups, schema, stats — read without scanning data |
| **Row group** | A horizontal slice (batch of rows) inside a Parquet file, stored together so readers can skip/parallelize it |
| **Column projection** | Read only the columns you need (`columns=[...]`) |
| **`dataset` + filter** | Read a partitioned tree with partition/predicate pruning |
| **Columnar bridge** | Arrow's shared layout that lets engines exchange tables directly |
| **Zero-copy** | Hand data between engines (pandas/Spark/Trino) by *pointing* at the same memory, without re-serializing |

## You can now…
- Convert between pandas and Arrow, and read/write **Parquet** with PyArrow
- Read only the columns you need (projection) and inspect metadata without a scan
- Read partitioned datasets with partition + predicate pruning
- Explain how Arrow/Parquet make pandas, Spark, and the lakehouse interoperate fast
