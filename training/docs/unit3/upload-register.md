# Upload a file & register it as a table

## Concept
A very common real task: someone hands you a file, you drop it in the **data lake** (RustFS), then
make it a **catalog table** so everyone can query it in SQL and Spark — governed, shared, and
addressable as `my.<namespace>.<table>`.

Three moves: **upload → read → register**. Then it's a first-class table in the `my` catalog —
your own lakehouse — visible to your notebooks (SQL and Spark) and the Polaris Console.

> This is the ad-hoc *"someone handed me a file"* version. Ingesting ShopFlow's own sources
> (Postgres + Parquet) into Bronze systematically — the pipeline version — is
> [4.2](../unit4/read-bronze.md).

## Lab

### 1 · Upload the file to the lake (RustFS)
Open the **RustFS console** at <http://localhost:9001/rustfs/console/> (sign in with your lab account):

1. Go to your own bucket, **`<username>-lake`** — the only one members see in the console
   (managers also see the shared **`demo-bucket`**).
2. Create/enter a folder, e.g. **`files/source/uploads/`**.
3. **Upload** your file — say `customers.csv`.

The object is now at **`s3a://demouser-lake/files/source/uploads/customers.csv`**. (**📁 My files** on the
landing page uploads to the same place. Any S3 client works too — RustFS is S3-compatible, reached
inside the stack at `http://storage:9000` — but the console needs no tooling.)

The examples use the default lab account's bucket, **`demouser-lake`** — signed in with your own
account? Use your bucket instead (shown at the top of the landing page, e.g. `ravi-lake`).

### 2 · Read it in the notebook
`spark` is already there ([3.7](spark.md)). Point it at the path:

```python
path = "s3a://demouser-lake/files/source/uploads/customers.csv"   # your own bucket

df = (spark.read
        .option("header", True)        # first row is column names
        .option("inferSchema", True)   # detect types (int/double/…)
        .csv(path))

df.printSchema()
df.show(5)
```

For Parquet it's just `spark.read.parquet("s3a://demouser-lake/files/source/uploads/…")` (schema is built in — no
options needed).

### 3 · Register it as a catalog table
Write the DataFrame into the governed **`my`** catalog. Pick (or create) a namespace, then
create the table:

```python
spark.sql("CREATE NAMESPACE IF NOT EXISTS my.bronze")

df.writeTo("my.bronze.customers_upload").createOrReplace()
```

- **`writeTo("my.bronze.customers_upload")`** — target table in `my` → namespace `bronze`.
- **`.createOrReplace()`** — create it (or replace if re-running). The data is written as Iceberg
  files in RustFS and the table is registered in Polaris.

#### SQL-only alternative — and getting the **column names** right
You can register the file without the DataFrame step, but there's a trap. The obvious one-liner
**does not work well for CSVs with a header**:

```sql
%%sql
-- DON'T: the csv.`path` shorthand accepts NO options, so it can't set header=true
CREATE TABLE my.bronze.customers_upload USING iceberg AS
SELECT * FROM csv.`s3a://demouser-lake/files/source/uploads/customers.csv`
```
Because that shorthand can't be told that row 1 is a header, you get generic columns
**`_c0, _c1, _c2`** *and* the header line (`id,name,country`) lands as a **data row**:

| _c0 | _c1  | _c2     |
|-----|------|---------|
| id  | name | country |  ← the header, now a row 😱
| 1   | Ada  | UK      |

**The fix: register a temp view with `USING csv OPTIONS (...)` first, then `CREATE TABLE … AS`.**
The options are where `header`/`inferSchema` live (`%%sql` is one statement per cell, so this is two
cells):

```sql
%%sql
CREATE TEMPORARY VIEW customers_raw USING csv
OPTIONS (path 's3a://demouser-lake/files/source/uploads/customers.csv', header 'true', inferSchema 'true')
```
```sql
%%sql
CREATE TABLE my.bronze.customers_upload USING iceberg AS
SELECT * FROM customers_raw
```
Now the columns take their **names from the header** (`id`, `name`, `country`) with inferred types —
no stray header row.

- **`header 'true'`** — treat row 1 as column names (and skip it from the data).
- **`inferSchema 'true'`** — detect types (`int`/`double`/…); omit it and every column is `string`.
- For Parquet there's no header problem — a Parquet file already carries names and types built in,
  so the plain `parquet.<path>` shorthand works fine (no options needed).

!!! tip "Define the columns yourself (explicit names + types)"
    Don't want inferred names/types? **Declare the schema on the view** and keep `header 'true'` so
    the first row is still skipped:
    ```sql
    %%sql
    CREATE TEMPORARY VIEW customers_raw (id INT, name STRING, country STRING) USING csv
    OPTIONS (path 's3a://demouser-lake/files/source/uploads/customers.csv', header 'true')
    ```
    This is the SQL twin of the DataFrame reader's `.option("header", True)` / `.schema(...)` — the
    header/options `spark.read` sets in Python become the `OPTIONS (...)` clause in SQL.

### 4 · Access it — SQL and Spark
It's now a normal catalog table. Query it any way:

```sql
%%sql
SELECT * FROM my.bronze.customers_upload LIMIT 10
```

```python
df = spark.table("my.bronze.customers_upload")   # or spark.sql("SELECT ...")
df.count()
```

The same table is now visible in the **Polaris Console** (your lakehouse `<username>_lake`) and to
every Spark session that signs in as you — one governed copy.

#### From SQLPad (Trino)
SQLPad reaches the same table by your lakehouse's **real name**, **`<username>_lake`**. `my`
is a short name only Jupyter knows — in SQLPad (and Superset) `iceberg` is the course's **shared**
lake ([0.3](../setup/workspace.md)). The real name works in Jupyter too, so a query written with it
runs in both:

| Where | The table is |
|---|---|
| Jupyter (`%%sql`, `spark.table`) | `my.bronze.customers_upload` or `demouser_lake.bronze.customers_upload` |
| SQLPad, Superset | `demouser_lake.bronze.customers_upload` |

Open SQLPad, pick the connection **Lakehouse (Trino) — demouser** (your own username), and run:

```sql
SELECT * FROM demouser_lake.bronze.customers_upload ORDER BY id LIMIT 10;
```
```text
 id | name  | country
----+-------+---------
  1 | Ada   | UK
  2 | Linus | FI
  3 | Grace | US
```

- **`ORDER BY id`** — sort by `id`, so the rows come back in the same order every time.
- `iceberg.bronze.customers_upload` here fails with *Table 'iceberg.bronze.customers_upload' does not
  exist*: that name looks in the shared lake.

SQLPad also **changes** the table — it's your table, so `UPDATE`, `INSERT` and `DELETE` work:

```sql
UPDATE demouser_lake.bronze.customers_upload SET name = 'Surendra' WHERE id = 2;
```
```text
UPDATE: 1 row
```

- **`UPDATE … SET name = 'Surendra'`** — set the column `name` to a new value…
- **`WHERE id = 2`** — …only in the rows that match (leave out `WHERE` and every row changes).

Run the `SELECT` again (in Jupyter too) and row 2 now says `Surendra` — one table, read and changed
from both tools.

!!! warning "Text values take 'single quotes'"
    In SQL, `'Surendra'` is a text value, but `"Surendra"` (double quotes) is a **column name**.
    `SET name = "Surendra"` fails with *Column 'surendra' cannot be resolved*: Trino looks for a
    column called `surendra`. Double quotes are only for column or table names with spaces or
    capitals, like `"Order Date"`.

### Just exploring? Use a temp view instead
If you only need to poke at a file for this session (no permanent table), skip the catalog and
register a **temp view** — no write, no namespace:

```python
spark.read.option("header", True).csv(path) \
     .createOrReplaceTempView("customers_tmp")
```
```sql
%%sql
SELECT count(*) FROM customers_tmp
```
The view disappears when the kernel stops; a catalog table persists and is shared.

> **Next:** the data you just registered is rarely clean — [4.2.1 Data cleaning](../unit4/data-cleaning.md)
> takes two messy Bronze tables to clean Silver.

## 🎯 This runs unchanged on Azure, Databricks, Snowflake & Fabric
Upload to **ADLS / S3**, then `CREATE TABLE … AS SELECT` or `writeTo(...).createOrReplace()` into
**the Databricks catalog** / **Snowflake** / **Fabric** — the exact same read-then-register
pattern; only the storage URL and catalog name change.

## You can now…
- Upload a file to the data lake via the RustFS console
- Read a CSV/Parquet from `s3a://…` in a notebook
- **Register** it as a governed `my` catalog table (`writeTo().createOrReplace()` or `CREATE TABLE AS`)
- In SQL-only mode, get **column names right** with `CREATE TEMPORARY VIEW … USING csv OPTIONS (header 'true', …)` — or declare an explicit schema — instead of the header-losing `csv.<path>` shorthand
- Query it from SQL and Spark, and from **SQLPad** as `<username>_lake.bronze.…` (select *and* update) — and know when a **temp view** is enough instead
