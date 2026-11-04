# 👤 Accounts, members and managers

Everyone in the lab has one **lab account**. It signs you in to the portal and to every tool in it
(single sign-on), and it decides what you see: your own workspace, your team's, and what others
shared with you. Most accounts are **members**; a few are **managers**, who run the lab for the
others.

## Sign in
1. Open the portal: `http://lab.localhost:8000` on your laptop, `http://de.lan` on a team cluster.
2. The **Epireum's Data Engineering Lab** sign-in page opens. Enter your username and password.
3. You land on **Home**. The first time, the lab builds your workspace first
   ([what you get](how-it-works.md#what-you-get-of-your-own)).

Opening a tool from the portal needs no second sign-in: Jupyter, SQLPad, Airflow, Superset and the
storage console all know who you are. If the top bar says **lakehouse not ready**, reload the page a
little later.

## Your account

| Where | Account |
|---|---|
| On your laptop | **demouser** / **demouser** is ready. The course's examples are written for it (`demouser_lake`, `demouser-lake` …) |
| On a team cluster | your own account: register (below) or ask a manager. Don't share one account: it would be one lakehouse, bucket and Jupyter for everybody |

**Register** (when it's on): click **Register** on the sign-in page, pick a username and a password.
The username is 3–30 lowercase letters and digits; it also names your lakehouse and bucket
(`ravi` → `ravi_lake`, `ravi-lake`). No email is needed. No **Register** link? A manager has
switched it off: they turn it on while new members sign up, or create your account for you.

A password a manager sets for you is temporary: you choose your own at the next sign-in.

## Members and managers

| | Members | Managers |
|---|---|---|
| Own workspace (lakehouse, bucket, Jupyter, secrets) | ✓ | ✓ |
| **Workspace**, **Tools** and **Extras** in the menu | ✓ | ✓ |
| **Manage** in the menu | | ✓ |
| In the tools (SQLPad, Jupyter's hub, storage) | their own things | admin |

**Manage** has these pages:

| Page | What managers do there |
|---|---|
| **Members** | every account with its lakehouse, bucket use and team; **Create member**, **New password**, **Reset lakehouse**, **Wipe**, **Delete** |
| **Teams** | create teams, add members to one, move or remove them, delete an empty team |
| **Activity** | when each member last signed in, used Jupyter, changed files or tables, ran a DAG |
| **Governance** | Polaris users, roles and grants by hand: [Create users & assign access (🛡 Governance)](../unit6/polaris-admin.md) |
| **Exams** | the mock exams: add them, set who may take them, see results |
| **Settings** | **Self-registration** on or off, the **Members' limits** (Jupyter idle time, memory and CPU, bucket sizes, Pub/Sub streams), **Flink cluster**, **Services**, **Lab version** |

## Teams
A new member is in no team: only their own workspace. A manager can put a member in **one** team.
Everyone in the team reads and writes what the team has:

| The team's | Name |
|---|---|
| Lakehouse | `team_<team>` (bronze / silver / gold) |
| Bucket | `team-<team>-lake` |
| Secrets | the team's folder in the key vault |
| DAGs | named `team_<team>_…` |

Members come and go; the team's tables and files stay with the team. A mock exam can be opened to
chosen teams only. Details: [Working in a team](../setup/teams.md).

## Who sees what
A member sees only their own things, their team's, and what someone shared with them. Each member's
Jupyter gets logins that reach only their own lakehouse, bucket and secrets (and their team's);
another member's tables and files are not there unless they shared them.

To give someone access, the owner invites them on **My catalogs**: the whole lake or one namespace,
to read or to write, and can revoke it later. Everyone may read the course's shared lake
(`polaris_lake`, `shared` in Jupyter). The governance lessons use fixed logins with set roles:
[Personas & roles (governance demos)](../setup/personas.md).

## Sign out
**Log out**, top right in the portal, ends your lab-account session. Jupyter follows within a
minute. A tool you still have open in its own browser tab (Superset, Airflow, SQLPad) keeps its own
session until you log out there or it expires.

## Related lessons
- [Your lab account & workspace](../setup/workspace.md)
- [Working in a team](../setup/teams.md)
- [Personas & roles (governance demos)](../setup/personas.md)
- [Polaris: governed Iceberg for every engine](../unit6/polaris.md)
- [Create users & assign access (🛡 Governance)](../unit6/polaris-admin.md)
