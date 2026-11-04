# 👥 Members, teams and activity

Only lab managers see these pages (the **Manage** section of the sidebar). **Members** holds the lab
accounts, **Teams** groups members so they share a lakehouse, and **Activity** shows who still uses the
lab. On a laptop, sign in with the manager account to see them; on a team cluster, with the manager
account your platform team gives you.

## Members
**Manage → Members**, tab **Members**: every lab account, with **Search members…** (by name or team).

| Column | What it shows |
|---|---|
| **Account** | the username; tags *manager*, *you*, *disabled* |
| **Team** | the member's team (a link to **Teams**), or — |
| **Lakehouse** | their catalog `<user>_lake`, or *not signed in yet* |
| **Catalogs** | how many catalogs they own |
| **Bucket** | what their files and tables use, in MB |
| **Since** | when the account was made |

**Actions** on a row:

| Action | What it does | What stays |
|---|---|---|
| **New password** | shows a new password once; the member picks their own at the next sign-in | everything |
| **Reset lakehouse** | drops every table and view in their catalogs; bronze / silver / gold and the sample table come back | files, catalogs, shares |
| **Wipe** | back to a new member's setup: every table dropped, extra catalogs deleted, the shares they gave revoked, bucket emptied, secrets removed, Jupyter reset | the account, `<user>_lake` with its sample table, the starter files |
| **Delete** | removes the account and everything it owns | nothing (it can't be undone) |

**Reset lakehouse**, **Wipe** and **Delete** ask you to type the username first. On your own account
there is only **New password**; managers can't be wiped or deleted here. A member can also reset their
own lakehouse on **🗂️ My catalogs** ([My catalogs](my-catalogs.md)).

**Delete** removes: their catalogs and tables, bucket and Jupyter files, secrets, logins (their own and
their jobs'), Airflow DAG history, mock-exam attempts, 📡 Ingestion sources and jobs, 📨 Pub/Sub
streams, their own Postgres databases, folder shares, and their SQLPad user.

### Add a member
Tab **Add a member**: **Username** (a-z, 0-9, -), **Password** (optional: without one a random password
is shown once) and **Team** (or *no team*), then **Create member**. The account gets its lakehouse,
bucket and SQLPad user and can sign in at once. Members can also register themselves while
**Self-registration** is on ([⚙️ Settings](settings.md)).

### Managers and disabled accounts
The portal doesn't make a member a manager or disable an account. Both are done in the sign-in
service (Keycloak): a manager is in its group `managers`. The **Members** list only shows the tags.

## Teams
A member is in no team (only their own lakehouse) or in exactly one. Everyone in a team reads and writes
its lakehouse `team_<team>`, its bucket `team-<team>-lake`, its secrets and its Airflow DAGs. What
members see: [Working in a team](../setup/teams.md).

**Manage → Teams**:

- **All teams**: each team with its number of **Members**, its **Lakehouse** and **Bucket** use.
  Click one to manage it.
- **Create a team**: a **Team name** (a-z, 0-9, starting with a letter), then **Create team**. It
  makes the team's lakehouse (bronze / silver / gold) and bucket.

A team's page has three tabs:

| Tab | What you do there |
|---|---|
| **Members (n)** | filter, tick and **Remove selected**, or **Remove** one. They keep their own lakehouse; their tables in the team's lakehouse stay there |
| **Add members** | search, pick one or more, then **Add**. Someone in another team moves here and loses the old team's lakehouse, files, secrets and DAGs |
| **Delete team** | only when it has no members. It removes the team's lakehouse (every table) and its bucket; asks twice |

A moved member sees the new team's folder in Jupyter from their next server start.

## Activity
**Manage → Activity** shows when each member last used the lab, to find accounts nobody uses.

| Column | Newest… |
|---|---|
| **Last active** | of the columns after it (not **Created**) |
| **Sign-in** | sign-in (kept for 90 days) |
| **Jupyter** | Jupyter use (tag *Jupyter on* while a server runs) |
| **Files** · **Tables** | change in `files/` · table write (starter files count on day one) |
| **DAG run** | run of their DAGs |
| **Storage** | bucket use, in MB |

SQLPad and Superset use isn't counted. **Show** filters: *everyone*, or *inactive 7+ / 14+ / 30+ /
60+ / 90+ days*. Click a column to sort; hover a time for the date.

To remove members: tick them, **Delete selected (n)**, type `delete n`, then confirm. It does the same as
**Delete** on **Members**. Managers can't be ticked.

## Related lessons
- [Your lab account & workspace](../setup/workspace.md)
- [Working in a team](../setup/teams.md)
