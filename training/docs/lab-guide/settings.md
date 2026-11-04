# ⚙️ Settings

Only lab managers see this page (**Manage → Settings** in the sidebar). It holds the lab-wide switches:
who may register, the members' limits, which tools run, the size of the Flink cluster, and the lab's
version. Nothing needs a restart; each setting applies as its note on the page says. The page has three
tabs: **General**, **Services** and **Lab version**.

## General: self-registration
The **Self-registration** switch:

- **On — the sign-in page shows Register**: anyone who can reach the sign-in page can make an account.
- **Off — only managers create accounts** (on **Manage → Members**, see
  [👥 Members, teams and activity](members-teams.md)).

Turn it on while a new batch signs up, then off again. New members start in no team; add them to one on
**Teams**.

## General: members' limits
Change the numbers, then **Save**. A value out of range isn't saved (the page says which).

| Setting | Range | What it is | When it applies |
|---|---|---|---|
| **Jupyter stops when idle after** | 5–480 minutes | a member's Jupyter server stops after this long without activity; their files stay | within a minute |
| **Jupyter memory** | 0.5–16 GB | per member | when their server next starts |
| **Jupyter CPU** | 0.25–8 cores | per member | when their server next starts |
| **Member bucket size** | 20–10240 MB | each member's files and tables (`<user>-lake`) | at once, for every member; one already above it can only delete until under |
| **Team bucket size** | 50–51200 MB | each team's bucket (`team-<team>-lake`) | at once, for every team |
| **📨 Pub/Sub streams** | 0–20 per member | how many event streams a member may have (0 = none); 3 until you change it | when a member makes a stream |
| **📨 Pub/Sub rate** | 1–100 events / s | the most events a second one stream may send; 20 until you change it | for new rates; streams already above it keep theirs |

## Services
The tools run in **units**. **Core** (sign-in, files, catalog, storage) is always on. The others: SQL,
Notebooks, Spark, Airflow, Superset, Streaming and Flink. Each row shows its services and its state:
**● on**, **○ off**, **◐ starting…**, **◐ stopping…** or **◐ partly on**. Superset needs SQL.

**On a team cluster** each unit has a switch. Turn off what the class isn't using yet, to free the
server's memory and CPU:

1. Click the unit's switch. Turning off asks first (**Turn off** / **Cancel**) and names what goes with
   it: SQL takes Superset along; Notebooks stops the members' open Jupyter servers (their files stay).
2. Starting takes a minute or two. The state updates by itself.

Turning a unit off keeps all its data. Members see its tiles greyed on the home page: *Not running.
Contact admin.* A lab update turns every unit back on.

**On a laptop** the list only shows what runs. Switch units in the terminal: `make status`, then ↑ ↓
to a unit, **s** on, **x** off ([make status: how to use it](../setup/deploy/make-status.md)).

## Services: Flink cluster
The Flink unit runs Apache Beam jobs in **slots**: each TaskManager is one container, each slot one
parallel copy of a job's steps. Pick **TaskManagers** (1–4) **×** **slots each** (1–4), then **Save**
(2 × 2 until you change it). The line under it shows the total slots and the memory: 1.25 GB + 0.25 GB per
slot for each TaskManager, plus about 1.5 GB for the JobManager and Beam's job server. **Now:** shows
what runs at the moment.

| Where | When the new size applies |
|---|---|
| Team cluster, Flink on | at once: the TaskManagers restart with the new size, running jobs with them |
| Team cluster, Flink off | the next time the Flink unit starts |
| Laptop | `make status` → select Flink → **s** |

## Lab version
The release this lab runs, and whether a newer one is out on GitHub (checked at most once an hour):
*✓ up to date*, *vX.Y.Z available* with **What's new**, or *couldn't check*. To update a laptop:
`git pull` in the repo, then `make up` in `local/`. A team cluster: `git pull`, then deploy again.

## Related lessons
- [make status: how to use it](../setup/deploy/make-status.md)
- [Kubernetes setup](../setup/deploy/kubernetes.md)
- [Your lab account & workspace](../setup/workspace.md)
