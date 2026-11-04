# 📊 Dashboards

**Tools → Dashboards** opens Superset, the lab's BI tool: charts and dashboards on your lakehouse
tables, usually the Gold ones. Superset doesn't read the files itself; it sends SQL to Trino, as you.
Lesson: [Dashboards from the Gold layer](../unit7/dashboards.md).

## Signing in
Superset signs you in with your lab account: opening it from the portal goes straight to the lab's
sign-in. Your Superset user is made at your first sign-in. Members can build datasets, charts and
dashboards; lab managers are Superset admins.

## The connections
Two Trino connections are set up for everyone (**Settings → Database Connections**):

| Connection | What it's for |
|---|---|
| **ShopFlow Lakehouse** | your lakehouse and every other catalog you may read: pick the catalog when you make a dataset or query in SQL Lab |
| **shopflow** | the raw ShopFlow source database, for SQL Lab |

Both query Trino **as the signed-in member**, so the same rules apply as in the
[⌨️ SQL editor](sql-editor.md): your own and your team's lakehouses, what was shared with you, and the
shared lake read only.

## From a table to a dashboard
1. **SQL → SQL Lab**: pick **ShopFlow Lakehouse**, run a query to check you get rows, e.g.
   `SELECT * FROM demouser_lake.gold.daily_sales LIMIT 10` (your own: `<you>_lake`).
2. **Datasets → + Dataset**: database **ShopFlow Lakehouse**, then the catalog (`<you>_lake`), the
   schema (`gold`) and the table. **Create dataset and create chart**.
3. Build the chart: a chart type, a metric, a dimension; **Save** it.
4. **Dashboards → + Dashboard**: drag your saved charts onto the grid, add a filter, **Save**.

## What's shared
Superset is one app for the whole lab: datasets, charts and dashboards others save can show up in your
lists too. The data is not shared that way: every chart runs its query as the member looking at it. A
chart on someone else's lakehouse shows an error for you, unless they shared that table (or their
lakehouse) with you on [🗄 My catalogs](my-catalogs.md).

Superset needs the **Superset** unit, and the **SQL** unit for its data. If its tile is greyed out, a lab
manager switches them on: `make status` on a laptop, **Manage → ⚙️ Settings → Services** on a team
cluster.

## Related lessons
- [Dashboards from the Gold layer](../unit7/dashboards.md)
- [Spark SQL & aggregates (Gold)](../unit4/spark-sql-gold.md)
- [Query the lake with Trino](../unit2/intro.md)
