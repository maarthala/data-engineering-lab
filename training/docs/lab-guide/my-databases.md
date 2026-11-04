# 🛢 My databases

**My databases** lists the PostgreSQL databases you can use, all on the lab's database server for
members, `lab-db` (port 5432): the lab's sample and live databases, which everyone reads, and up to
three databases of your own, where you create tables and write. Click one for its connection details,
its tables and how to use it. Use them to practise SQL on an operational database, and as sources
for **📡 Ingestion** into your lakehouse.

## What's listed

| Section | Database | What it is | Login |
|---|---|---|---|
| **Lab databases** (read only) | **ShopFlow** (`shopflow`) | the course's online shop (orders, customers, products …) | `learner` / `learner` |
| | **AdventureWorks** (`adventureworks`) | Microsoft's sample company: sales, production, HR | `learner` / `learner` |
| | **Tidewell web shop** (`shop`) | Tidewell's online shop: customers, products, orders (live) | `reader` / `reader` |
| | **Tidewell stores** (`store`) | Tidewell's store tills: sales in local time, cents, SKUs (live) | `reader` / `reader` |
| | **Tidewell loyalty** (`loyalty`) | Tidewell's loyalty card: members and their points (live) | `reader` / `reader` |
| **My databases** (full access) | `<you>_<name>` | yours: create tables, write, drop. Only you can connect | your own login |

The live Tidewell databases change every few seconds ([Meet the live shop](../cdc/live-shop.md)).

A database's page shows:

- **Host**, **Port**, **Database**, **User** and **Password**, each with **⧉** to copy it. On your
  own databases the password is hidden; **👁** shows it.
- **Open SQL editor**, and which connection to choose there.
- **Tables**: each table and view with its kind and an estimated row count. **↻ Refresh** reloads it.
- **ⓘ How to use it**: the steps and code below, filled in for this database.

## Make a database of your own
1. **＋ New** (next to **My databases**).
2. Type a name after `<you>_`: lower-case letters, digits and `_`, starting with a letter (e.g. `sales`).
3. **Create**. The database is `<you>_sales`, owned by your own login.

The login is named after you and made with your first database; its password is the same for all of
them. You can have up to 3 databases. The SQL editor gets a private connection to each one,
**`<you>_<name> (Postgres) — <you>`**.

To delete one: open it, **Delete database**, then **Yes, delete**. The database and every table in it
are gone; this can't be undone. Its SQL editor connection goes too.

If the page says *Own databases aren't set up on this lab*, the lab runs without them; the lab
databases still work.

## Use it in the SQL editor
1. **Open SQL editor**.
2. Choose the connection the page names: e.g. **ShopFlow — OLTP (Postgres)**, **Tidewell web shop
   (Postgres)**, or **`<you>_sales (Postgres) — <you>`** for your own.
3. Run SQL. On the lab databases that is `SELECT` only. On your own, e.g.:

```sql
CREATE TABLE customers (
  customer_id serial PRIMARY KEY,
  name        text NOT NULL,
  country     text
);
INSERT INTO customers (name, country) VALUES ('Asha', 'IN'), ('Ben', 'UK');
SELECT * FROM customers;
```

## Use it in a notebook
With pandas and SQLAlchemy, using the page's **User**, **Password**, **Host**, **Port** and **Database**:

```python
import pandas as pd
from sqlalchemy import create_engine

db = create_engine("postgresql+psycopg2://learner:learner@lab-db:5432/shopflow")
pd.read_sql("SELECT * FROM information_schema.tables WHERE table_schema = 'public'", db)
```

For your own database, keep the password out of the notebook: save it on
[🔑 My secrets](my-secrets.md) (e.g. a secret `mydb` with the fields `username` and `password`) and
read it with `key_vault("mydb", "password")`.

## Use it in Ingestion
**📡 Ingestion** can copy a database into your lakehouse. Make a source with the page's **Host**,
**Port**, **Database**, **User** and **Password**. The live Tidewell databases are the course's CDC
sources ([Your first sync job](../cdc/first-job.md)).

## Related lessons
- [SQL from Python — Postgres & the lakehouse](../unit3/databases.md)
- [Meet the live shop](../cdc/live-shop.md)
- [Your first sync job](../cdc/first-job.md)
