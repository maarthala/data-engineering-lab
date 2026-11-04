# 🗂 The portal

The portal is the lab's front page: one window with a menu on the left and the tools on the right.
Every page and tool opens there as a **tab**, so you can keep a notebook, the SQL editor and a
dashboard open side by side and switch between them without losing your place.

## The menu
The menu on the left has four groups:

| Group | Items | What they are |
|---|---|---|
| **Workspace** | **Home**, **My files**, **My catalogs**, **My databases**, **My secrets** | your own things: [Home](home.md), your bucket, your catalogs, your Postgres databases, your secrets |
| **Tools** | **Ingestion**, **Pub/Sub**, **Notebooks**, **SQL editor**, **Pipelines**, **Dashboards**, **Spark UI**, **Flink UI**, **Storage** | the lab's tools: Ingestion and Pub/Sub are portal pages ([Pub/Sub streams](pubsub.md)); the others are Jupyter, SQLPad, Airflow, Superset, Spark, Flink and the RustFS console |
| **Extras** | **Mock exams** | practice exams |
| **Manage** | **Members**, **Teams**, **Activity**, **Governance**, **Exams**, **Settings** | for managers only; members don't see this group ([Accounts, members and managers](accounts.md)) |

Hover over a tool to see what it is (e.g. *Jupyter*). On a narrow screen, **☰** at the top opens
and closes the menu.

The top bar shows where you are (e.g. *Workspace / Home*), who is signed in, and **Log out**.

## Tabs
1. Click an item in the menu: it opens in a new tab, or switches to its tab if it is open already.
2. Click a tab to switch to it. Each tab keeps its state: a running notebook, a half-written query,
   a filtered dashboard stay as they were.
3. **×** on a tab closes it.

The bar above the tool has:

| Button | What it does |
|---|---|
| **Reload** | loads the tab's page or tool again |
| **Open in new tab ↗** | the tool on its own, in a new browser tab |
| **Restart server** / **Stop server** | on the **Notebooks** tab, while your Jupyter runs; each asks first (**Yes** / **Cancel**); your files are kept |

**Notebooks** does not start Jupyter by itself: the tab shows **Start Jupyter** until you click it.

The open tabs are remembered for this browser tab: reload the page, or come back from another
page, and they are there again (the tools load again). A link in a portal page to another page
(e.g. **My catalogs →** on Home) opens that page's tab.

## Greyed-out tools
A tool shown greyed out in **Tools** is not running: its unit is switched off. Hover over it: it says
*not running*. Opening it shows *… is not running*:

- **Members**: *Ask your lab managers to switch it on.*
- **Managers**: start its unit. On your laptop: `make status`, then **s** on the unit
  ([make status: how to use it](../setup/deploy/make-status.md)). On a team cluster:
  **Manage → Settings → Services**.

Which unit holds which tool: [Units](how-it-works.md#units-parts-you-switch-on-and-off).

## Release and updates
The bottom of the menu shows the lab's release, e.g. **Release v1.11.0**. Home's footer shows it
too.

When a newer release is out, **Home** shows a banner: *A newer release of the lab is out*, with
**What's new** (the release notes) and **Hide** (hides it until the next release, in this browser).

| You are | The banner says |
|---|---|
| a member | *Your lab managers can update it.* |
| a manager, on your laptop | `git pull` in the repo, then `make up` in `local/` |
| a manager, on a team cluster | `git pull`, then deploy again |

Managers also see the release on **Manage → Settings → Lab version**: up to date, or which one is
available.

## Course links
- **Course ↗** at the bottom of the menu, and **🎓 Open the course ↗** on Home: this course.
- **Docs ↗**: the lab's README on GitHub.

## Related lessons
- [Your lab account & workspace](../setup/workspace.md)
- [Stack architecture](../unit0/stack.md)
- [make status: how to use it](../setup/deploy/make-status.md)
- [Bring up the stack](../setup/deploy/index.md)
