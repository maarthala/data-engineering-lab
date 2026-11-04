# 🏠 Home

**Home** is your own overview: what you worked on last, what's in your lakehouse, how your
pipelines ran and how your mock exams went. It's the first tab after you sign in
(**Workspace → Home**). The tools themselves are in the menu on the left ([The portal](portal.md)).

## The top
- **Welcome, *your username***: who you are signed in as.
- **🎓 Open the course ↗**: this course, in a new browser tab.
- A banner when a newer release of the lab is out ([Release and updates](portal.md#release-and-updates)).

## Continue where you left off
Tiles to jump back in:

| Tile | Shows | Opens |
|---|---|---|
| 📝 **Resume** *exam* | a mock exam you paused: how many questions you answered of how many | **Mock exams** |
| 📓 **Open Jupyter** / **Jupyter is running** | whether your Jupyter server runs, and when it was last active | **Notebooks** |
| 📒 🐍 🧮 📄 your recent files | up to four files you changed last (notebook, Python, SQL, other), with their folder and when | **My files**, at that folder |

Recent files are the ones in your bucket's `files/`; task logs and dbt output are left out.

## My lakehouse
Your own lakehouse at a glance. **My catalogs →** opens **My catalogs**.

| Part | What it shows |
|---|---|
| **Storage** | how much of your bucket you use, of its limit (MB and %), and how many files; the bar turns red from 90 % |
| **bronze / silver / gold tables** | how many tables each layer of your lakehouse has |
| **Your lakehouse** | its name, `<you>_lake` |
| **Catalogs** | how many catalogs are yours, and how many others shared with you |
| **Team** | your team's lakehouse `team_<team>`, or *not in a team yet* |
| **Files** | **bucket `<you>-lake` →** opens **My files** |

A full bucket refuses new files: delete some on **My files**, or ask a manager for more room
(**Manage → Settings → Members' limits**).

## My pipelines
Your Airflow DAGs and your team's (marked **team**). **Open Airflow →** opens **Pipelines**.

| Column | What it shows |
|---|---|
| **DAG** | its name: yours start with `<you>_`, your team's with `team_<team>_` |
| **Last run** | the last run's state (*success*, *failed*, *running*, *queued* …) or *never run* |
| **When** | how long ago it ran |
| (last column) | its schedule, or *paused* |

No DAGs yet? Save a `.py` file into `dags/` in Jupyter (or **My files → files/src/dags/**) and it
shows up here. *Airflow is not running right now* means the Airflow unit is off
([greyed-out tools](portal.md#greyed-out-tools)).

## Mock exams
The mock exams open to you. **All exams →** opens **Mock exams**.

| Column | What it shows |
|---|---|
| **Exam** | its name (opens **Mock exams**) |
| **Attempts** | how often you finished it |
| **Passed** | how often you passed |
| **Best** | your best score |
| **Last** | your last score, green when you passed |

*No mock exams are open to you yet* until a manager opens one to everyone or to your team.

## The footer
The lab's release, a link to the **Data Engineering course** and **Docs & GitHub** (the lab's README).

## Related lessons
- [Your lab account & workspace](../setup/workspace.md)
- [Working in a team](../setup/teams.md)
- [Stack architecture](../unit0/stack.md)
