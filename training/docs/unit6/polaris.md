# Polaris: governed Iceberg for *every* engine

## Concept
The lakehouse stores tables; a **catalog** governs them — who can see and touch what.
This stack uses **Apache Polaris** (Snowflake's, now Apache), an **Iceberg-native**
governed REST catalog. It gives you:

- **One catalog for every engine** — Spark, Trino and the Console all reach tables through the
  standard **Iceberg REST** protocol
- **Per-user reads *and* writes** — checked on the identity making each request
- **Credential vending** — after checking your grants, Polaris hands the engine short-lived
  storage credentials scoped to just those tables (read-only if you may only read)
- **Owners who share** — you own your lakehouse and decide who else may read or write it
- **A web Console** to browse catalogs, namespaces, tables and grants

The model is the universal one — **catalog → namespace → table**, with **principals,
roles, grants** — so the skills transfer straight to the cloud catalogs
([10.1](../platforms/rosetta.md)).

## What you already have
Every lab account is a Polaris **principal** with two catalogs:

| In your notebook | Polaris catalog | Stored in | You can |
|---|---|---|---|
| `iceberg` | **`demouser_lake`** — your lakehouse | `s3://demouser-lake/tables/demouser_lake` | everything — you're the **owner** |
| `shared` | **`polaris_lake`** — the course's shared lake | `s3://demo-bucket/warehouse` | read `bronze` / `silver` / `gold` |

```
you (lab account demouser)
   │  sign in once (Keycloak SSO) → Polaris knows you as principal demouser
   ▼
 Spark / Console ──(Iceberg REST)──► Apache Polaris
                                        │ checks demouser's roles + grants,
                                        │ vends scoped RustFS credentials
                                        ▼
                                RustFS: demouser-lake/tables/…  (your tables)
```

Behind "owner" and "read" is the same **RBAC chain** for everyone:

```
principal demouser ──holds──► principal-role demouser
                                   │ bound to
                         ┌─────────┴──────────┐
              catalog-role owner         catalog-role learner_reader
              on demouser_lake           on polaris_lake
              CATALOG_MANAGE_CONTENT     TABLE_LIST + TABLE_READ_DATA on bronze/silver/gold
```

A **principal** is *who*; a **principal-role** is the roles they hold; a **catalog-role** is a
bundle of **grants** inside one catalog. Change a role once and everyone holding it changes.

**Console:** open your lakehouse from 🗂️ **My catalogs → Open in Polaris Console ↗** (it signs
you in with your lab account). The Console's own catalog list is for admins only, so start from
My catalogs.

## Lab — share your lakehouse with another user
Sharing needs **two accounts**: the **owner** of the lakehouse and **another user** to share with.

| | Owner | Other user |
|---|---|---|
| **Local stack** | `demouser` / `demouser` | `learner2` / `learner2` — sign in in a **private window** |
| **Class stack** | your account | a classmate's account (or swap roles) |

Below the owner is `demouser` and the other user `learner2`.

### 1 · Owner: make something worth sharing
In a notebook, build one Gold and one Silver table from your sample table:

```python
spark.sql("""
CREATE OR REPLACE TABLE iceberg.gold.status_counts AS
SELECT status, count(*) AS n FROM iceberg.bronze.sample_orders GROUP BY status
""")
spark.sql("""
CREATE OR REPLACE TABLE iceberg.silver.orders_clean AS
SELECT * FROM iceberg.bronze.sample_orders WHERE status <> 'cancelled'
""")
```

### 2 · Owner: share `gold`, read-only
🗂️ **My catalogs** → `demouser_lake` → on the **`gold`** row click **🤝 Share** → type
`learner2` → **read** → **Share**. **Current shares** now lists it.

### 3 · Other user: use it
As **learner2** (private window): 🗂️ **My catalogs** → **Shared with me** → `demouser_lake`
shows exactly what you got (`gold` · all tables · read) and the notebook lines to use it.
In learner2's notebook (Jupyter, same private window):

```python
use_catalog("demouser_lake")                                  # the owner's catalog, with YOUR login
spark.table("demouser_lake.gold.status_counts").show()        # ✅ shared
```
```python
spark.table("demouser_lake.silver.orders_clean").show()       # ⛔ not shared
```
→ `ForbiddenException: Forbidden: Principal 'learner2' … is not authorized …`

```python
spark.sql("INSERT INTO demouser_lake.gold.status_counts VALUES ('test', 1)")   # ⛔ read-only
```
→ the job fails with `Access Denied (Service: S3, Status Code: 403)`. Polaris let you *read*
the table's metadata, but the storage credentials it vended you are **read-only** — the
storage itself refuses the write.

Open **Open in Polaris Console ↗** on the same page: you see the catalog and its namespace
**names**, but only `gold` lists tables. Seeing that `silver` exists isn't access to it.

### 4 · Owner: widen, then revoke
- Share **one table** with write: on `silver` → `orders_clean` → **🤝 Share** → `learner2` →
  **write**. learner2 can now `INSERT` / `DELETE` in that one table — and still can't read
  anything else in `silver`.
- **Revoke** both shares under **Current shares**. learner2's next query answers
  *Forbidden* — grants are checked on every request, there's nothing to log out of.

### 5 · What Share built
Each share is a **catalog-role** in *your* catalog (e.g. `share_learner2_read_n_gold`),
bound to the other user's principal-role:

| Shared | Grants in the catalog-role |
|---|---|
| namespace, **read** | `TABLE_LIST`, `TABLE_READ_DATA`, `TABLE_READ_PROPERTIES`, `NAMESPACE_READ_PROPERTIES` |
| namespace, **write** | the read set + `TABLE_WRITE_DATA`, `TABLE_WRITE_PROPERTIES`, `TABLE_CREATE` |
| table, **read** / **write** | the same, on that one table |
| always | `CATALOG_READ_PROPERTIES` + `NAMESPACE_LIST` (open the catalog, see namespace names) |

Revoke deletes that catalog-role. In [6.4](grant-and-query.md) you build the same chain by
hand in the Console, as the Polaris admin.

!!! info "Governed everywhere — Spark, the Console, SQLPad and Superset"
    Spark and the Console sign in to Polaris **as you**, so everything above is enforced there.
    SQLPad (your own connection) and Superset pass **your username** to Trino, whose access rules
    are generated from these same Polaris grants: your own catalogs, exactly what was shared with
    you, and the shared lake read-only. A new share or revoke reaches Trino within about half a
    minute (a minute or two on a team stack).

!!! info "Personas on the shared lake"
    `polaris_lake` also has three demo principals — **`analyst`**, **`engineer`**, **`lead`** —
    with graded access (gold / gold+silver / everything). They're client-id/secret logins for
    the admin lessons ([6.2](polaris-admin.md), [6.4](grant-and-query.md)), not lab accounts.

## Key terms, at a glance
| Term | Plain meaning |
|---|---|
| **Iceberg REST catalog** | the open protocol every engine speaks → one governed catalog |
| **principal / principal-role / catalog-role** | *who* / *the roles they hold* / *a grants bundle in one catalog* |
| **owner** | your catalog-role on your lakehouse: `CATALOG_MANAGE_CONTENT` (everything inside it) |
| **share** | a catalog-role in your catalog, bound to another member's principal-role |
| **credential vending** | Polaris hands the engine short-lived storage creds scoped to what you may do |

## 🎯 The same model on Azure, Databricks, Snowflake & Fabric
Sharing a schema read-only is `GRANT SELECT ON SCHEMA gold TO …` in **Databricks Unity
Catalog** and **Snowflake**, with the same owner / role / grant model. Databricks also vends
short-lived storage credentials for each query. **Fabric** shares a lakehouse or a single table
through workspace roles and OneLake security.

## You can now…
- Explain how **Polaris governs every engine** over one Iceberg lakehouse, per user
- Read your own RBAC chain: principal → principal-role → catalog-role → grants
- **Share** a namespace or a single table, read or write, and **revoke** it
- Tell *metadata* denial (`Forbidden`) from *storage* denial (`Access Denied` on vended creds)
