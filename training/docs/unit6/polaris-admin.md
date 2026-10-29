# Create users & assign access (in the Console)

In {lesson:unit6/polaris.md} you shared your own tables through **My catalogs**. Here you'll build the same
chain by hand, as the Polaris admin — create a user, give it a role, grant that role
access — entirely in the **Polaris Console**.
No command line: Polaris manages its own users (*principals*). (Your lab account is one
of them too — signing in to the Console with it maps you to your own principal.)

## How access works

Every user in Polaris is a **principal** that logs in with a **Client ID + Client
Secret** (people and apps alike). You never grant a user directly — access always
flows through a **role**:

```
user (principal) ─► principal-role ─► catalog-role ─► privilege on a namespace
   e.g. auditor      auditor_role       auditor_cr      gold : read
```

Grant once to the role, and every user who holds it gets that access.

Open the **Polaris Console** admin login — <http://polaris-console.lab.localhost:8000/login?local=1> (k8s: `http://polaris-console.de.lan/login?local=1` — the
`?local=1` shows the **Client ID / Secret** form instead of signing you in with your lab account) — and sign in as
**`root` / `s3cr3t`** (the admin, on your laptop stack — on a team stack only the platform
manager has the admin login).

!!! info "An admin lesson"
    Creating principals and roles needs the Polaris **admin** (`root`), so in a company this
    is the platform team's job (or done on your own laptop stack). With your lab account you can't
    create users — but you *can* share your own tables with other members on
    **🗂️ My catalogs** ([0.3](../setup/workspace.md)), which builds exactly this
    user → role → grant chain for you.

---

## Create a user and grant it access

We'll make a read-only user **`auditor`** with access to `gold`.

**1 · Create the user.** Left nav **Access Control → Principals → Create Principal**.
Name it `auditor`, choose **Create and generate credentials**. Polaris shows a
**Client ID** and **Client Secret** — **copy them now**, the secret is shown once
(you can **Rotate** it later). That pair *is* the user's login — hand it to the
person or app that will use it.

**2 · Create a role.** **Access Control → Principal Roles → Create Principal Role**,
name it `auditor_role`.

**3 · Give the user the role.** Open `auditor_role` → **Assigned Principals →
Grant to Principal → `auditor`**. (Same thing from the principal's *Assigned
Principal Roles* tab.)

**4 · Create the grant bundle.** Go to **Catalogs → `polaris_lake` → Catalog Roles
→ Create Catalog Role**, name it `auditor_cr`.

**5 · Attach the bundle to the role.** On `auditor_cr` → **manage principal roles →
Grant to Principal Role → `auditor_role`**.

**6 · Grant the access.** Still on `auditor_cr`, add grants: pick **Namespace →
`gold`**, privilege **`TABLE_READ_DATA`**; add another for **`TABLE_LIST`** (so it
can see the tables). Save.

**Done.** `auditor` now reads `gold` on both Trino and Spark. To let a user also
*write* `silver`, add grants on its catalog role for **`silver`**: `TABLE_READ_DATA`,
`TABLE_LIST`, `TABLE_WRITE_DATA`, `TABLE_CREATE`.

!!! tip "Test it"
    Sign out and sign back in with `auditor`'s Client ID/Secret (or point Trino/Spark
    at the catalog with them) — you see only `gold`; `silver` returns *not found*.

## Log in as the user

A principal signs into the Console (the `?local=1` form) or any engine with the
**Client ID + Secret** from step 1. Whoever holds those credentials *is*
that principal and sees exactly its grants.

!!! note "Memorable persona logins"
    The built-in personas use tidy credentials — `analyst`/`analyst`,
    `engineer`/`engineer`, `lead`/`lead` — because `common/polaris/seed-polaris.sh`
    pins them (Client ID = Secret = name). The Console's own *Create / Rotate* gives
    randomly generated values instead; use the seed (or the reset API) when you want
    a memorable pair.

!!! tip "Where the IdP (SSO) fits — your lab account"
    Human logins in this lab already go through an identity provider: your **lab account**
    (Keycloak). Polaris trusts its token and maps your username to your own principal +
    principal-role — exactly how production fronts people with Okta / Entra (+ MFA), while
    services and these demo principals keep using client secrets. The grants model below
    is the same either way.

## Privileges you'll grant most

| Privilege | Lets the role… |
|---|---|
| `TABLE_LIST` | see that tables exist |
| `TABLE_READ_DATA` | read table data |
| `TABLE_WRITE_DATA` | insert / update / delete rows |
| `TABLE_CREATE` | create new tables |
| `NAMESPACE_CREATE` | create namespaces |
| `CATALOG_MANAGE_CONTENT` | full control of a catalog's content |

Grant on a whole **catalog**, a **namespace** (as above), or a single **table** —
narrower scope = tighter least-privilege.

## Managing access later (all in the Console)

- **Change access** → add/remove grants on the **catalog role** (every holder
  updates at once).
- **Rotate a leaked secret** → the principal's **Rotate** action.
- **Remove access** → revoke the role, or **Delete Principal** to remove the user.

## You can now…

- Create a **user** (principal) in the Polaris Console and read back its login
- Build the chain **principal → principal-role → catalog-role → grant** by clicking
- Grant read vs write at namespace scope, and **rotate / revoke / delete** later
- Explain how the built-in personas log in, and where an IdP would slot in for SSO
