# Create users & assign access (🛡 Governance)

In {lesson:unit6/polaris.md} you shared your own tables through **My catalogs**. Here you'll build the same
chain by hand, as a lab manager — create a user, give it a role, grant that role
access — on the portal's **🛡 Governance** page (left menu → **Manage → Governance**).
No command line: Polaris manages its own users (*principals*), and the page talks to Polaris for you.

## How access works

Every user in Polaris is a **principal** that logs in with a **Client ID + Client
Secret** (people and apps alike). You never grant a user directly — access always
flows through a **role**:

```
user (principal) ─► principal-role ─► catalog-role ─► privilege on a namespace
   e.g. auditor      auditor_role       auditor_cr      gold : read
```

Grant once to the role, and every user who holds it gets that access. The Governance page has
one tab per link of that chain: **Logins** (principals), **Principal roles**, and **Catalog roles
& grants**.

!!! info "A manager's lesson"
    The Governance page is for **lab managers** (the Keycloak group `managers`). On your own laptop
    stack you are one — sign in with your manager account. On a shared stack this is the platform
    team's job; with a member account you can still share your own tables on **🗂️ My catalogs**
    ([0.3](../setup/workspace.md)), which builds exactly this chain for you.

!!! note "🔒 what the lab manages"
    The page lists every principal and role in Polaris, including the ones the lab makes itself:
    each member's own login and role, their job login (`<member>_jobs`), Trino's login, the
    admin `root`, catalog owners and shares. Those are shown **🔒 read-only** with the reason, so a
    click here can't break anyone's access. What you make on this page, and the course's personas
    (analyst, engineer, lead), can be changed.

---

## Create a user and grant it access

We'll make a read-only user **`auditor`** that can read the `gold` namespace of the shared lake
`polaris_lake`.

**1 · Create the user.** Tab **Logins** → **New login**: type `auditor`, **untick** *with a
principal role of the same name* (we build the role by hand below), then **Create login**. The
page shows its **Client ID** and **Secret** — **copy them now**, the secret is shown once (*New
secret* makes another). That pair *is* the user's login: hand it to the person or app that will
use it.

**2 · Create a role and give it to the user.** Tab **Principal roles** → **New principal role**:
`auditor_role` → **Create role**. In its row, **+ give to a login → `auditor`**.

**3 · Create the grant bundle.** Tab **Catalog roles & grants** → choose the catalog
**`polaris_lake`** → type `auditor_cr` → **Create catalog role**.

**4 · Attach the bundle to the role.** On the `auditor_cr` card: **+ give to a principal role →
`auditor_role`**.

**5 · Grant the access.** Still on `auditor_cr`: **Grant on → a namespace**, type `gold`, choose
the bundle **read (list + read data)** → **Grant**. The card now lists what it granted:

```text
On               Privilege
namespace gold   NAMESPACE_LIST
namespace gold   NAMESPACE_READ_PROPERTIES
namespace gold   TABLE_LIST
namespace gold   TABLE_READ_PROPERTIES
namespace gold   TABLE_READ_DATA
```

A **bundle** is a shortcut for the privileges a read (or write) usually needs — pick *One
privilege* in the same list to grant a single one instead.

**Done.** `auditor` now reads `gold` with Trino and Spark — and nothing else. To let a user also
*write* `silver`, grant the **write** bundle on namespace `silver` (it adds `TABLE_WRITE_DATA`,
`TABLE_CREATE`, `TABLE_DROP`, …).

## Test it: log in as the user

A principal logs in to Polaris with its **Client ID + Secret** and gets a short-lived **token**;
every call after that carries the token. In a notebook (**Tools → Notebooks**), with the values
you copied in step 1:

```python
import requests

POLARIS = "http://polaris:8181/api/catalog"
login = {"grant_type": "client_credentials", "client_id": "<auditor's Client ID>",
         "client_secret": "<auditor's Secret>", "scope": "PRINCIPAL_ROLE:ALL"}
token = requests.post(f"{POLARIS}/v1/oauth/tokens", data=login).json()["access_token"]
auth = {"Authorization": f"Bearer {token}"}

for ns in ("gold", "silver"):
    r = requests.get(f"{POLARIS}/v1/polaris_lake/namespaces/{ns}/tables", headers=auth)
    print(ns, r.status_code, r.json().get("identifiers") or r.json()["error"]["message"])
```

- `requests.post(…/oauth/tokens, data=login)` — the login: the Client ID / Secret go in as a
  form (OAuth2 *client credentials*); `scope: PRINCIPAL_ROLE:ALL` asks for all of its roles.
- `…/namespaces/<ns>/tables` — the Iceberg REST call that lists a namespace's tables, the same
  call Spark and Trino make.

Output:

```text
gold 200 [{'namespace': ['gold'], 'name': 'daily_sales'}, {'namespace': ['gold'], 'name': 'top_products'}, {'namespace': ['gold'], 'name': 'customer_ltv'}]
silver 403 Principal 'auditor' with activated PrincipalRoles '[auditor_role]' and activated grants via '[auditor_cr, auditor_role]' is not authorized for op LIST_TABLES
```

`gold` answers with its tables; `silver` is refused — and Polaris says exactly which roles it
looked at. That message is the first thing to read when someone reports "access denied".

!!! note "Memorable persona logins"
    The built-in personas use tidy credentials — `analyst`/`analyst`, `engineer`/`engineer`,
    `lead`/`lead` — because `common/polaris/seed-polaris.sh` pins them (Client ID = Secret = name).
    A login made on the Governance page gets random values instead.

!!! tip "Where the IdP (SSO) fits — your lab account"
    Human logins in this lab already go through an identity provider: your **lab account**
    (Keycloak). Polaris trusts its token and maps your username to your own principal +
    principal-role — exactly how production fronts people with Okta / Entra (+ MFA), while
    services and these demo principals keep using client secrets. The grants model is the same
    either way.

## Privileges you'll grant most

| Privilege | Lets the role… |
|---|---|
| `TABLE_LIST` | see that tables exist |
| `TABLE_READ_DATA` | read table data |
| `TABLE_WRITE_DATA` | insert / update / delete rows |
| `TABLE_CREATE` | create new tables |
| `NAMESPACE_CREATE` | create namespaces |
| `CATALOG_MANAGE_CONTENT` | full control of a catalog's content (the *manage* bundle) |

Grant on a whole **catalog**, a **namespace** (as above), or a single **table** —
narrower scope = tighter least-privilege.

## Managing access later

- **Change access** → grant or **Revoke** on the **catalog role**'s card (every holder updates
  at once).
- **A leaked secret** → **New secret** on the login's row (the old one stops working).
- **Remove access** → the **×** on a role in the login's row, or **Delete** the login.
- **Who can reach a catalog?** → its owner sees it on **🗂️ My catalogs → Sharing → 👥 Who has
  access**, with the roles you gave here marked *Governance*.

## You can now…

- Create a **user** (principal) on the Governance page and copy its login
- Build the chain **principal → principal-role → catalog-role → grant** by clicking
- Grant read vs write at namespace scope, and **revoke / rotate / delete** later
- Log in as the user from code and read Polaris's refusal when access is missing
