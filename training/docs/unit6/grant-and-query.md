# Grant table access to a user & query it

You have personas ([6.2](polaris-admin.md)) and tables ([6.3](create-catalog-table.md)).
Now the everyday governance task: **give a user read access to a specific table**, then
**query it as that user**.

## The one thing that trips everyone: read ≠ see

Two *different* capabilities, each its own privilege:

| Goal | Privilege | Scope |
|---|---|---|
| **Query** the table (read rows) | `TABLE_READ_DATA` | the table |
| **See / browse** it (`SHOW TABLES`, the table list) | `TABLE_LIST` | the **namespace** |
| See the namespace exists | `NAMESPACE_LIST` | the **catalog** |
| Write / insert / drop | `TABLE_WRITE_DATA`, `TABLE_CREATE`, … | the table/namespace |

A user with only `TABLE_READ_DATA` can query the table **if they know its exact name**,
but it won't show up when they browse — that needs the two `*_LIST` privileges. For
**read-only query + discovery**, grant: `TABLE_READ_DATA` + `TABLE_LIST` (namespace) +
`NAMESPACE_LIST` (catalog). No write privileges = read-only.

## The grant chain (always through a role)

You never grant a user directly — access flows **user → principal-role → catalog-role → grant**:

```
analyst (principal) ─► analyst_role ─► sales_reader (catalog-role) ─► TABLE_READ_DATA + LIST on demo.sales
```

!!! note "Permissions are set on the Governance page, not with SQL"
    Polaris manages its own RBAC — you grant access on the portal's **🛡 Governance** page (it
    calls Polaris for you), *not* with SQL. `GRANT` in Spark or Trino does **not** work against
    Polaris (the engines only read/write data; the catalog owns access control).

## Do it — grant analyst read-only on `demo.sales`

Open **Manage → 🛡 Governance** (a lab manager — on your laptop stack, your manager account) and
use the catalog from [6.3](create-catalog-table.md) — below it's `demouser_learn`, yours is
`<you>_learn`.

!!! tip "Members: the same grant, without a manager"
    Sharing *your own* tables with another member doesn't need the Governance page: on **🗂️ My
    catalogs** **🤝 Invite a member** → their username → **read** or **read + write** → the whole
    lake or a namespace → **Invite**. Behind the scenes it creates exactly the chain below (a
    catalog role with these privileges, given to their principal role) — and **Revoke** removes it.

**1. Create the catalog role:** tab **Catalog roles & grants** → choose your catalog → type
`sales_reader` → **Create catalog role**.

**2. Add these grants** — on the `sales_reader` card, **Grant on** … → *One privilege* … →
**Grant**, one row at a time:

| Grant on | Namespace / table | Privilege | Why |
|---|---|---|---|
| a table | `demo` / `sales` | **`TABLE_READ_DATA`** | read the rows |
| a table | `demo` / `sales` | **`TABLE_READ_PROPERTIES`** | open the table (its schema, its files) |
| a namespace | `demo` | **`TABLE_LIST`** | list the tables in `demo` |
| the whole catalog | — | **`NAMESPACE_LIST`** | discover the `demo` namespace |

**3. Give it to the persona:** on `sales_reader` → **+ give to a principal role → `analyst_role`**.

That's it — analyst now has read-only access, granted through the role.

## Verify — as analyst

Log in to Polaris as **`analyst` / `analyst`** (the persona's Client ID / Secret, laptop defaults)
from a notebook and try reading *and* writing:

```python
import requests

POLARIS, CAT = "http://polaris:8181/api/catalog", "demouser_learn"      # yours: <you>_learn
login = {"grant_type": "client_credentials", "client_id": "analyst", "client_secret": "analyst",
         "scope": "PRINCIPAL_ROLE:ALL"}
auth = {"Authorization": "Bearer " + requests.post(f"{POLARIS}/v1/oauth/tokens", data=login).json()["access_token"]}

print("namespaces", requests.get(f"{POLARIS}/v1/{CAT}/namespaces", headers=auth).json()["namespaces"])
print("tables", [t["name"] for t in requests.get(f"{POLARIS}/v1/{CAT}/namespaces/demo/tables", headers=auth).json()["identifiers"]])
print("open sales", requests.get(f"{POLARIS}/v1/{CAT}/namespaces/demo/tables/sales", headers=auth).status_code)
r = requests.delete(f"{POLARIS}/v1/{CAT}/namespaces/demo/tables/sales", headers=auth)
print("drop sales", r.status_code, r.json()["error"]["message"])
```

- `GET …/namespaces` — needs `NAMESPACE_LIST`; `GET …/demo/tables` — `TABLE_LIST`;
  `GET …/demo/tables/sales` — opening the table, `TABLE_READ_PROPERTIES` (reading its rows then
  needs `TABLE_READ_DATA`).
- `DELETE …/tables/sales` — dropping it, which analyst was **not** given.

Output:

```text
namespaces [['demo']]
tables ['sales']
open sales 200
drop sales 403 Principal 'analyst' with activated PrincipalRoles '[analyst_role]' and activated grants via '[analyst_role, sales_reader]' is not authorized for op DROP_TABLE_WITHOUT_PURGE
```

Read ✅, write ⛔ — enforced by the catalog, whatever engine asks.

## Query it

Reading through Polaris vends **short-lived, scoped credentials** — a user can only read
tables they're granted. An engine configured with analyst's client id/secret sees exactly
`demo.sales` and nothing they weren't granted. Your notebook's `spark` signs in as **you** (your
own principal), so it's enforced there too: a table another member shared with you is readable
after `use_catalog("<their catalog>")`, and anything not shared answers *forbidden*.

## Revoke

On the `sales_reader` card: **Revoke** a grant, the **×** next to `analyst_role` (take the role
away), or **Delete catalog role** — access disappears immediately (grants are checked per request).
The catalog's owner sees all of this on **🗂️ My catalogs → 👥 Who has access**.

## 🎯 This runs unchanged on Azure, Databricks, Snowflake & Fabric
This is `GRANT SELECT ON <table> TO <role>` — the Databricks catalog and Snowflake use
the exact same *grant-to-a-role, read-vs-usage* model; only the syntax differs.

## You can now…
- Grant a user **read-only** access to a specific table (through a role)
- Explain why **query** (`TABLE_READ_DATA`) and **see** (`TABLE_LIST`/`NAMESPACE_LIST`) are separate
- Verify enforcement (read ✅, write ⛔) as the persona, from code
- Query a table as a specific user via their credentials, and **revoke** access
