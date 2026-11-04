# ⌨️ SQL editor

**Tools → SQL editor** opens SQLPad, a web editor for SQL. It runs any SQL, reads as well as writes:
queries on your lakehouse go through Trino as **you**, and the lab's Postgres databases are there too.
Lessons: [Query the lake with Trino](../unit2/intro.md) and the ones after it.

## Signing in
SQLPad signs you in with your lab account: opening it from the portal goes straight to the lab's sign-in
(on SQLPad's own login page: **Sign in with Epireum lab account**). Your SQLPad account is made for you
when you first sign in to the lab. Lab managers are SQLPad admins.

## Your connections
Pick one in the connection list at the top. The schema browser on the left shows that connection's
tables.

| Connection | What it reaches |
|---|---|
| **Lakehouse (Trino) — `<you>`** | Trino as you; its schema browser shows your lakehouse `<you>_lake` |
| **Team `<team>` (Trino) — `<you>`** | the same Trino, browsing your team's lakehouse `team_<team>` (only when you're in a team) |
| **ShopFlow — OLTP (Postgres)**, **AdventureWorks — OLTP (Postgres)** | the sample databases, read only |
| **Tidewell web shop (Postgres)**, **Tidewell stores (Postgres)**, **Tidewell loyalty (Postgres)** | the live systems, read only |
| **`<database>` (Postgres) — `<you>`** | each database of your own from [🛢 My databases](my-databases.md): create tables, write, drop |

A Trino connection only sets the catalog the schema browser shows. In a query you can name any catalog
you may use, with three-part names: `SELECT * FROM demouser_lake.gold.daily_sales`.

## Running a query
1. Pick the connection.
2. Type the SQL. Several statements can sit in the editor; select one to run only that.
3. Press **Run** (or Ctrl+Enter / ⌘+Enter). The rows show below.
4. **Save** keeps the query under a name, in your list of queries.

## What you can query through Trino
Trino's rules are made from the catalog's own grants (Polaris), so they match Spark and Superset:

| Catalog | You may |
|---|---|
| your own lakehouses (`<you>_lake`, `<you>_<name>`) and your team's (`team_<team>`) | everything: create, write, drop |
| a lakehouse, namespace or table shared with you | what the share says: read, or read and write |
| the shared lake (`iceberg`, `polaris_lake`) | read |
| `shopflow`, `adventureworks` | read |
| another member's lakehouse, not shared | nothing |

A new share (or one taken back) reaches Trino after about 30 seconds on a laptop, 1–2 minutes on a team
cluster. `SHOW CATALOGS` lists what Trino has; `SHOW SCHEMAS FROM <catalog>` and
`SHOW TABLES FROM <catalog>.<schema>` look inside.

SQLPad needs the **SQL** unit (Trino and SQLPad). If its tile is greyed out, a lab manager switches it on:
`make status` on a laptop, **Manage → ⚙️ Settings → Services** on a team cluster.

## Related lessons
- [Query the lake with Trino](../unit2/intro.md)
- [Joins & aggregations](../unit2/joins-aggregations.md)
- [MERGE & upserts](../unit2/merge.md)
- [Grant table access to a user & query it](../unit6/grant-and-query.md)
