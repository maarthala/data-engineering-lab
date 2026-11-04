# 🛡 Governance

Only lab managers see this page (**Manage → Governance** in the sidebar). It shows who may do what in
the lakehouse, in Apache Polaris's own terms, and lets you change it without a command line. Access
always goes along one chain: a **login** holds **principal roles**, a principal role holds **catalog
roles**, and a catalog role holds **grants** (privileges on a catalog, a namespace or a table). The page
has one tab per link: **Logins**, **Principal roles**, **Catalog roles & grants**.

```
login (principal) ─► principal role ─► catalog role ─► grant
   report_bot          report_bot        gold_readers    gold : read
```

## What you can change
The lab makes most logins and roles itself: each member's login and role, their catalogs' owner role,
job logins, shares and Trino's login. They show 🔒 with why (for example *a member's own login (Manage
members)*), and can't be changed here, so an edit can't break anyone's access. You can change what is
made on this page (*✓ can be changed*) and the course's personas `analyst`, `engineer` and `lead`
([Personas & roles (governance demos)](../setup/personas.md)).

## Logins
**New login**: a name (a-z, 0-9, `_`, starting with a letter), tick **with a principal role of the same
name** if you want one, then **Create login**. A card shows its **Client ID** and **Secret** once, with
**Copy** buttons. Polaris doesn't show the secret again. Use them in a notebook, Spark or Trino as
credential `<client id>:<secret>`, scope `PRINCIPAL_ROLE:ALL`.

A new login can do nothing until it holds a principal role that holds catalog roles.

The **Logins** list shows each login (tag *persona*), its **Client ID** and its **Principal roles**:

- **+ give a role** gives it a principal role you can change; **×** on a role takes it away. This works on
  a member's login too.
- **New secret** (on logins you can change) makes a new secret; the old one stops working.
- **Delete** removes a login made here. Personas can't be deleted (change their roles instead).

## Principal roles
A principal role is what a login is given, like a group. Its rights come from the catalog roles it holds.

- **New principal role**: a name, then **Create role**.
- In the list, **Held by** shows its logins: **+ give to a login** adds one, **×** removes one.
- **Delete** removes a role made here (not a persona's role).

## Catalog roles & grants
Pick a catalog in **— choose a catalog —**. Every catalog role in it shows as a card:

- **Held by**: the principal roles that hold it. **+ give to a principal role** adds one, **×** removes
  one.
- The grants (**On**, **Privilege**), each with **Revoke**.
- **Grant on** *the whole catalog*, *a namespace* or *a table* (type the namespace and table), pick a
  privilege, then **Grant**.
- **Delete catalog role** removes it with its grants (not a persona's).

**Create catalog role** (a name at the top) adds a new one to the catalog.

| Privilege choice | What it gives |
|---|---|
| **read** | list namespaces and tables, read their properties and data |
| **write** | read, plus write data and properties, create and drop tables |
| **manage** | everything in the catalog (only on *the whole catalog*) |
| one privilege | a single Polaris privilege, e.g. `TABLE_READ_DATA`, `NAMESPACE_CREATE` |

## Governance and members' own sharing
Members share their own catalogs on **🗂️ My catalogs → 🤝 Sharing** ([My catalogs](my-catalogs.md)).
Each share is a catalog role named `share_…` in their catalog. Here it shows 🔒 *a share — the catalog's
owner changes it in 🗂 My catalogs → Sharing*.

The other way round: when you give a member's login a role here, the catalog's owner sees it on **👥 Who
has access**, marked *(🛡 Governance)*.

## Related lessons
- [Polaris: governed Iceberg for *every* engine](../unit6/polaris.md)
- [Create users & assign access (🛡 Governance)](../unit6/polaris-admin.md)
- [Grant table access to a user & query it](../unit6/grant-and-query.md)
- [Personas & roles (governance demos)](../setup/personas.md)
