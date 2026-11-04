# 🗄 My catalogs

**My catalogs** is where you manage your lakehouse: its namespaces and tables, who you share them
with, and the maintenance that keeps the tables fast. The left side lists **My catalogs** (the ones
you own) and **Shared with me** (the ones other members invited you to). Click one to open it.
Polaris, the lab's catalog, enforces what everyone may do; this page is your view of it
([Polaris: governed Iceberg for every engine](../unit6/polaris.md)).

## Your catalogs

| Catalog | Tag | What it is |
|---|---|---|
| `<you>_lake` | **main lakehouse** | made at your first sign-in, with `bronze`, `silver`, `gold` and the sample table `bronze.sample_orders`. In notebooks it is also `my` |
| `<you>_<name>` | | a catalog you add with **＋ New** |
| `team_<team>` | **team lakehouse** | your team's: everyone in the team reads and writes it. Nothing to share; a manager decides who is in the team ([Working in a team](../setup/teams.md)) |

**＋ New** makes another catalog: type a name after `<you>_` (a–z, 0–9 and `_`, up to 24, not `lake`)
and **Create**. It is stored in your own bucket, under `tables/<you>_<name>/`, and counts towards your
bucket's limit.

## Namespaces & tables
The **Namespaces & tables** card lists the catalog's namespaces. Click one to see its tables.

- **＋ New namespace**: type a name, then **Create**.
- **🗑** on a namespace deletes it. It must be empty.
- **🗑** on a table drops it with its files (**Yes** / **Cancel**). **⧉** copies its full name,
  e.g. `demouser_lake.bronze.sample_orders`.
- Tables are made by code (e.g. `df.writeTo("my.bronze.orders").create()`) or by a **📡 Ingestion** job,
  not on this page.

Click a table's name for its panel: rows, data files, delete files, size on storage and snapshots, then
four tabs.

| Tab | What it shows |
|---|---|
| **Columns** | each column's type, `key` and `not null`, and the partitioning |
| **Snapshots** | every write: when, the operation, rows added and removed. **⧉ SQL** copies a time-travel query (`SELECT * FROM … FOR VERSION AS OF <id>`) |
| **🧹 Maintenance** | what maintenance applies to this table, and its last runs |
| **Properties** | the table's location and Iceberg properties |

## Share a catalog
In the **🤝 Sharing** card:

1. **＋ Invite a member**.
2. Type at least 3 letters of their username and pick them from the list.
3. Choose **read** or **read + write**.
4. Choose **the whole lake** or **namespace <name>**.
5. **Invite**.

| Scope | What they get |
|---|---|
| **the whole lake** | every namespace and table, also ones you make later. With **read + write** they can also create and drop tables. On your main lakehouse it also shares your files (`files/` in your bucket) with the same access: they find them under **📁 My files → 🤝 Shared with me** ([My files](my-files.md#mine-team-shared-with-me)) |
| **namespace <name>** | that namespace and all its tables. The other namespaces' names are visible, not their tables |

The list under it shows each share: **Member**, **What**, **Access**. **Revoke** takes it back.
**👥 Who has access** lists everyone who can reach the catalog and how: you (owner), your shares, and
roles a lab manager gave on **🛡 Governance**.

A catalog shared with you shows under **Shared with me**, tagged **read** or **read + write**. Its page
lists **What you can access** and the namespaces and tables you can see; click a table for its panel.
In a notebook, `use_catalog("<catalog>")` makes it usable in Spark.

## 🧹 Maintenance
Three jobs keep Iceberg tables healthy. Set them on **🧹 Maintenance — the whole catalog** (the card at
the bottom), on a namespace (**🧹** on its row), or on one table (its **🧹 Maintenance** tab). The most
specific setting wins.

| Kind | What it does | Value |
|---|---|---|
| **Compaction** | merges small files (and the delete files CDC updates leave) up to a target size | file size, 8–1,024 MB (128 suggested) |
| **Snapshot expiry** | forgets snapshots older than N days: time travel goes back that far, and their files are freed | 1–365 days (7 suggested) |
| **Orphan files** | deletes files no snapshot uses any more, older than N days | 1–365 days (7 suggested) |

Per kind, choose **on** with a value, **none** (on the catalog) or **as set higher up** / **off here**
(on a namespace or table), then **Save**. Every table a setting covers is maintained every night at
03:00 UTC. **▶ Run now** does it at once for this catalog, namespace or table. A run goes through SQL,
so the SQL unit must be running. Only the owner (or a lab manager) can change the settings; on a shared
table you see what applies.

## 🧹 Reset…
On your main lakehouse, **🧹 Reset…** starts it over. A warning lists what goes; type the lakehouse's
name and press **Delete and reset** (about a minute).

| Goes | Stays |
|---|---|
| every table, view and namespace in `<you>_lake`; `bronze` / `silver` / `gold` come back empty, with the sample table | your files (My files, notebooks, DAGs, dbt) |
| the tables' data, `tables/` in your bucket | your other catalogs, the shares you gave, 🔑 secrets |
| | 📡 Ingestion sources and destinations, your account |

📡 Ingestion jobs writing into the lakehouse are stopped, and your 📨 Pub/Sub streams paused.

## How to use it
**ⓘ How to use it** on a catalog's card shows the code for it:

```python
spark.sql("SHOW NAMESPACES IN my").show()            # your main lakehouse, in a notebook
use_catalog("demouser_sales")                        # another catalog: yours or shared with you
spark.sql("SELECT * FROM demouser_sales.<namespace>.<table>").show()
```

In the SQL editor and Superset, use the real name: `SELECT * FROM demouser_lake.<namespace>.<table>;`

## Related lessons
- [Your lab account & workspace](../setup/workspace.md#my-catalogs-create-and-share)
- [Create your own catalog, namespace & table](../unit6/create-catalog-table.md)
- [Grant table access to a user & query it](../unit6/grant-and-query.md)
- [Table maintenance: OPTIMIZE, Z-order, VACUUM](../unit4/table-maintenance.md)
- [Working in a team](../setup/teams.md)
