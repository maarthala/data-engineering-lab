# General information

What applies however you start the stack: checking it works, the one-time pipeline that fills the
shared lake, and the day-to-day commands. On a cluster, see [Kubernetes setup](kubernetes.md).

## Verify
```bash
make ps                       # the services you started should be "running"/"healthy"
```
Open the landing page at **<http://lab.localhost:8000>** and sign in as **`demouser` / `demouser`** —
the default lab account every course example is written for (or **Register** your own, or use
`manager` / `manager`). Every tool tiles off it. Then run the
[setup checklist](../prerequisites.md#verify-your-setup).

## Fill the shared lake (once)
Starting Core (`make up`, or **s** on Core in `make status`) creates the shared lake (`polaris_lake`) with **empty** bronze / silver /
gold namespaces. The ready-made ShopFlow tables the lessons read — `shared.gold.daily_sales`
in a notebook, `iceberg.gold.daily_sales` in Trino / SQLPad / Superset — are built by an
Airflow pipeline. Run it once:

1. With `make status`: switch on **Airflow** and **Spark** (the pipeline runs Spark jobs).
   Open **Airflow** (landing page → Orchestration), signed in as `manager`.
2. Find **`shopflow_medallion`**, switch it **on** (new DAGs start paused) and press
   **▶ Trigger**.
3. After about **2–3 minutes** all three tasks are green: Bronze → Silver → Gold.

Until then those queries answer *table not found*. The tables stay, so you can switch Airflow
and Spark off again afterwards. Re-run it after a reset (`make clean`, `make destroy`).

## Day-to-day
Run them from `local/`. The last column is what matters most:

| Command | What it does | Your data |
|---|---|---|
| `make init` | **first run only**: downloads the Spark/Iceberg jars (~2.5 GB, into `common/dockerfiles/tmp`) and prepares AdventureWorks. Starts nothing | — |
| `make up` | starts every service: see [Full stack](full-stack.md) | kept |
| `make status` | watch the stack and switch units on and off: see [make status](make-status.md) | kept |
| `make build` | rebuilds the images, then starts — after you changed a Dockerfile, or pulled changes that did (`make up` tells you). Almost everything in `common/dockerfiles/add/` is mounted into the containers, so editing those files only needs a restart, not a rebuild | kept |
| `make down` | brings everything down — the stack and the members' Jupyter servers — and removes the containers, **not** the volumes or images. Shows each service going running → stopping → removed (`make stop` is the same) | **kept** |
| `make restart` | `make down` then `make up` | kept |
| `make clean` | with the stack down: deletes **all its data** (every volume) — back to a fresh install. Images stay, so the next `make up` is quick. Lists what goes and asks you to type `yes` | ⚠️ **wiped** |
| `make destroy` | removes **everything the stack made** — containers, volumes, network and images (built and pulled) — a clean plate; the next `make up` rebuilds every image. Asks you to type `yes` | ⚠️ **wiped** |
| `make ps` | show running services | — |
| `make logs` (`make logs S=trino`) | tail logs (all, or one service) | — |
| `make docs` | rebuild the course site after editing `training/docs/**` | — |

**Kept** means members, their lakehouses, tables, buckets, notebooks and DAGs, the catalog
(no re-seeding needed) and the shared lake's tables — all of it is in Docker
volumes.

!!! warning "`make clean` and `make destroy` delete all the data"
    Both delete the stack's volumes, so the object store (RustFS), Postgres, the catalog and the
    **lab accounts** are wiped — registered accounts are gone (`demouser`, `learner2` and `manager`
    come back by themselves), and each member's lakehouse and bucket are recreated, empty, on their next
    sign-in. After either: `make up` (it recreates the catalog) and the `shopflow_medallion` DAG again.
    They only touch this stack — other projects' containers, volumes and images are left alone —
    and keep the `make init` downloads. To stop for the day, use **`make down`**.

## Updating the lab

New versions of the lab are published as **releases**: git tags named like `v1.4.0`. When a
newer release is out, the lab tells whoever runs it:

- **the home page**, signed in as a manager: a banner *A newer release of the lab is out*, with
  a link to what's new. **Hide** hides it in this browser until the next release. Members never
  see it.
- **the terminal**: `make up` (at the end) and `make status` (in the log) print
  `⚠ newer release v1.4.0 (you're on v1.3.0) — git pull, then make up`.

Which release runs is shown in two places: **Manage → ⚙️ Settings → Lab version** (with *✓ up
to date* or *v1.4.0 available*), and at the bottom of the home page for everyone (handy when you
report a problem).

To update, from the repo:

```bash
git pull          # get the new release
cd local
make up           # restarts what changed (or switch units on in make status)
```

If `make up` says an image changed since it was built, run `make build`. Your data stays.

To check, home-api asks GitHub for the repo's tags at most once an hour. Offline, nothing is
shown. To switch the check off, add `LAB_UPDATE_CHECK=0` to `local/.env`.

!!! note "For the lab's maintainers: `make release`"
    A release is made with `make release` in `local/`: it tags the next version and pushes the tag.
    The default is the next **patch** (`v1.0.0` → `v1.0.1`, fixes); `BUMP=minor` makes the next
    **minor** (→ `v1.1.0`, new features) and `BUMP=major` the next **major** (→ `v2.0.0`, needs a
    reset). It refuses with uncommitted changes or when `main` differs from GitHub's, lists the
    commits since the last release and asks y / N before tagging.

## After it's up
- **Register your lab account** on the landing page — see
  [0.3 Your lab account & workspace](../workspace.md) for what you get and where things are.
- For the governance lessons, sign in to the Polaris Console as one of the
  [personas](../personas.md) (`analyst` / `engineer` / `lead`).
- The training course is published on **GitHub Pages**
  (<https://maarthala.github.io/data-engineering-lab/>), auto-deployed on every push to
  `main` — and the stack's landing page links straight to it, so it's always the current version.
