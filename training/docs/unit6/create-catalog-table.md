# Create your own catalog, namespace & table

You've *used* the governed catalog; now build the whole hierarchy yourself and see
exactly where each piece lives. The structure is three levels:

```
catalog  →  namespace (schema)  →  table
 (learn)      (demo)               (sales)   →  addressed as  learn.demo.sales
```

Key idea to hold onto:

| Level | Where it lives | Hits RustFS? |
|---|---|---|
| **Catalog** | Polaris (metadata) | ❌ no |
| **Namespace** | Polaris (metadata) | ❌ no |
| **Table (created)** | Polaris + a `metadata.json` in RustFS | ✅ first object |
| **Rows (inserted)** | Parquet data + manifests in RustFS | ✅ data files |

So a catalog/namespace is just *registration*; the **table** is the first thing that
writes to object storage.

## A · Create a catalog

**As a member — on 🗂️ My catalogs** (landing page): click **＋ New**, type `learn`,
**Create**. You get **`demouser_learn`** — e.g. `ravi_learn` — stored in your own bucket at
`s3://demouser-lake/tables/demouser_learn`, and you're its owner. Click **Open in Polaris Console ↗**
to see it in the Console.

!!! note "Why not *Catalogs → Create catalog* in the Console?"
    Creating catalogs there needs the Polaris **admin** — with your lab account it answers
    *403*. (For the same reason the Console's **Catalogs** list shows `0` for members:
    listing *all* catalogs is admin-only. Open yours from My catalogs.)

**As the admin — in the Console** (the platform manager, or your own laptop stack): open
<http://polaris-console.lab.localhost:8000/login?local=1> (k8s: `http://polaris-console.de.lan/login?local=1` — the
`?local=1` shows the **Client ID / Secret** form instead of signing you in with your lab account), sign in as `root` / `s3cr3t` (laptop stack; on a team stack this is the platform manager's job), then **Catalogs → Create catalog**:

- **Name:** `learn`
- **Storage type:** `S3`
- **Default base location:** `s3://demo-bucket/learn`
- **Endpoint:** `http://storage:9000` (RustFS — S3-compatible; the in-stack name is `storage`)
- **Region:** `us-east-1`
- **Path-style access:** **ON** ← required for RustFS
- **Create**

!!! warning "Path-style access is mandatory for RustFS"
    RustFS is addressed as `storage:9000/bucket` (path-style). The default S3 style is
    `bucket.storage:9000` (virtual-host), which doesn't resolve in this stack — table I/O then
    fails with an `UnknownHost` error. So **turn Path-style access ON**. (On real AWS S3
    you'd leave it off.)

Grant your admin write access so you can create tables in it: on the catalog →
**Catalog Roles** → create one → grant **`CATALOG_MANAGE_CONTENT`** → assign it to the
`service_admin` principal-role. *(The built-in `polaris_lake` catalog already has this.)*

## B · Create a namespace & table

A **namespace** (schema) is a folder for tables. A **table** has a schema and holds rows.

!!! note "Make the new catalog visible to Spark"
    The notebook's `spark` knows **`my`** (your own lakehouse `demouser_lake`) and
    **`shared`** out of the box. A brand-new catalog needs one line first —
    `use_catalog("demouser_learn")` — after which `spark.sql("… demouser_learn.demo.sales …")`
    works with your login. The steps below use **`my`** so they run as-is; swap in
    your `demouser_learn` catalog to build there instead.

In a notebook (`spark` is already there):

```python
# namespace (schema) — metadata only, nothing in RustFS yet
spark.sql("CREATE NAMESPACE IF NOT EXISTS my.demo")

# table — writes the first metadata.json to RustFS
spark.sql("""
CREATE TABLE IF NOT EXISTS my.demo.sales (
    id int, product string, amount double, sold_on date
) USING iceberg
""")

# rows — writes Parquet data + manifests
spark.sql("""
INSERT INTO my.demo.sales VALUES
    (1,'Widget',120.50, DATE'2026-01-05'),
    (2,'Gadget', 75.00, DATE'2026-01-06'),
    (3,'Widget', 60.25, DATE'2026-02-01')
""")
```

You can also create the namespace/table visually in the **Console** (Catalog →
**Create namespace** / **Create Iceberg Table**); the notebook is just quicker for a
table with data.

**Watch it land in your bucket** (📁 **My files**, or the RustFS console
<http://storage.lab.localhost:8000/rustfs/console/>): after `CREATE TABLE` you'll see
`tables/demouser_lake/demo/sales/metadata/00000-….metadata.json` in `demouser-lake`; after `INSERT`, a
`data/*.parquet` plus manifest/snapshot files appear.

## C · Query the table

All three work — pick by need:

```python
spark.table("my.demo.sales").show()                 # DataFrame API
```
```python
spark.sql("SELECT * FROM my.demo.sales ORDER BY id").show()
```
```sql
%%sql
SELECT product, sum(amount) AS revenue
FROM my.demo.sales
GROUP BY product
```

The same table is now visible in the **Polaris Console** (open your catalog from
**My catalogs**) and to every Spark session that signs in as you — one governed copy.
(Trino and Superset read the course's *shared* lake, not your own — see
[0.3](../setup/workspace.md).)

## 🎯 This runs unchanged on Azure, Databricks, Snowflake & Fabric
`catalog → schema → table` with `CREATE NAMESPACE` / `CREATE TABLE … USING iceberg` is
the same in **the Databricks catalog** and **Snowflake** (Snowflake calls it
database → schema → table). Only the catalog name and storage URL change.

## You can now…
- Create your own **catalog** on My catalogs (or, as admin, in the Console with **path-style** for RustFS)
- Create a **namespace** and a **table**, and say when each first touches object storage
- Query the table from the notebook (DataFrame, `spark.sql`, `%%sql`)
- Make a *new* catalog visible to Spark with `use_catalog(...)`
