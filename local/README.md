# Local setup (Docker Compose)

The **governed lakehouse** on a single machine with Docker Compose — the same stack
as [`../k8s/README.md`](../k8s/README.md), just on localhost instead of a cluster.

## What runs
RustFS object storage · **Apache Polaris** (governed Iceberg catalog) + web **Console** · Spark
(master + worker + Connect) · Trino · Superset · Airflow · Jupyter · Postgres · Redis ·
an nginx landing page. Postgres also hosts the **AdventureWorks** OLTP sample DB
(see below).

## Prerequisites
- **Docker** + **Docker Compose** v2 (≥ 8 GB RAM for Docker, ~25 GB free disk).
- `git`, `make`, `curl`, `unzip`, `ruby` (AdventureWorks prep in `make init`), `python3`
  (`make polaris-seed`). macOS: `xcode-select --install`; Debian/Ubuntu/WSL:
  `sudo apt-get install -y git make curl unzip ruby python3`.
- The custom images (spark, jupyter, superset, airflow, home) are **built by
  `make up`**; the rest (polaris, trino, rustfs, postgres, redis) are stock and pulled.

## Run
```bash
cp .env.example .env   # first time: create your env (gitignored); set GIT_TOKEN to enable notebook auto-push
make init   # first time: download base JARs (into ../common/dockerfiles/tmp)
make up     # build the compose images + start everything (first build: 15–30 min)
make polaris-seed   # create the iceberg catalog + personas; re-run after make down/restart
make ps     # status      make logs S=polaris     make down   # stop + remove volumes
```
Ports 5432 / 8001 taken by something else? Set `POSTGRES_HOST_PORT` / `AIRFLOW_HOST_PORT` in `.env`.

The **hands-on Data Engineering course** ([`../training`](../training)) is published to
**[GitHub Pages](https://maarthala.github.io/databrick-local-simulator/)** (auto-deployed on
every push to `main`), and the stack's landing page links straight there — one always-current
source. When editing lessons under `training/docs/`: `make docs-serve` live-previews at
**http://localhost:8010**, and `make docs` validates with `mkdocs build --strict` (the same
gate the Pages CI uses) before you push.

## Access
| Service | URL | Login |
|---|---|---|
| Landing page | http://localhost:8000 | your **lab account** — Register, or `manager` / `manager`; one login for every tool below |
| Training course | https://maarthala.github.io/databrick-local-simulator/ | — (linked from the landing page) |
| Keycloak (accounts) | http://localhost:8180/admin | admin / admin (realm `de-lab`) |
| RustFS console (S3) | http://localhost:9001/rustfs/console/ | lab account (own bucket) · root admin / admin123 |
| Polaris Console | http://localhost:8189 | lab account · `/login?local=1`: personas analyst/engineer/lead (id = secret = name), admin root/s3cr3t |
| Polaris API | http://localhost:8185 | OAuth2 client credentials (realm `POLARIS`) |
| Key vault (OpenBao) | http://localhost:8200 | **Sign in with OIDC Provider** = lab account (members: own folder; managers: all) · token `lab-vault-admin-local-token` = the lab's admin |
| Trino | http://localhost:8007/ui/ | any username, no password |
| Superset | http://localhost:8004 | lab account |
| SQLPad (SQL workbench) | http://localhost:8003 | lab account (local admin admin@de.local / admin1234) |
| Airflow | http://localhost:8001 (`AIRFLOW_HOST_PORT`) | lab account |
| Jupyter (JupyterHub) | http://localhost:8008 | your lab account (Keycloak) — own Jupyter + own lakehouse |
| Spark master UI | http://localhost:8002 | — |

## First steps
Seed the governed catalog (catalog, bronze/silver/gold namespaces, personas + RBAC):
```bash
make polaris-seed      # runs ../common/polaris/seed-polaris.sh against localhost:8185
```
Then:
- **Register your lab account** at http://localhost:8000 — it signs you in to every tool and
  creates your own lakehouse, bucket and Jupyter (course page 0.3).
- **Personas** (governance lessons): Polaris Console http://localhost:8189/login?local=1
  (`analyst`/`analyst`, `engineer`/`engineer`, `lead`/`lead`; admin `root`/`s3cr3t`).
- **Query the lake** with Trino (`iceberg` catalog) or Superset SQL Lab.
- **Governed Spark** — in Jupyter, `iceberg` is your own lakehouse and `shared` the course lake
  (`spark.sql("SELECT * FROM shared.gold.daily_sales")`); Spark Connect reads/writes through Polaris.

## Notebook auto-push (git commit + push on save)
Jupyter clones a repo into `/home/jovyan/work/repo` and, on every save, commits + pushes
the file. Save notebooks under the repo's `notebooks/` folder. Config is in
`local/jupyter.yaml` (`GIT_REPO_URL`, `GIT_BRANCH`, `GIT_AUTHOR_NAME/EMAIL`).

Config lives in **`local/.env`** (copy from `.env.example`; `.env` is gitignored, so the
token never lands in git):
```bash
cp .env.example .env         # first time
# edit .env → set GIT_TOKEN (the pushing account; needs Contents: Read+Write on the repo)
#   quick fill: GIT_TOKEN=$(gh auth token)
make up                      # or: docker compose up -d --force-recreate jupyter
```
Blank `GIT_TOKEN` = auto-push disabled. To switch account/repo: edit `GIT_REPO_URL` /
`GIT_TOKEN` in `.env`, delete the stale clone, and recreate:
```bash
docker exec jupyter rm -rf /home/jovyan/work/repo
docker compose up -d --force-recreate jupyter
```

## AdventureWorks sample database
The full **AdventureWorks OLTP** sample (68 tables across `person`, `sales`, `production`,
`purchasing`, `humanresources`) is loaded into the local Postgres as the `adventureworks`
database — handy for richer SQL practice than ShopFlow.

**How it's wired:**
- `make init` downloads the Microsoft OLTP CSVs + the community Postgres port and fixes the
  CSVs (needs **`ruby`** — ships with macOS). Data is staged under
  `init_scripts/postgres/adventureworks/` (gitignored, ~110 MB).
- On the **first** `make up`, `init_scripts/postgres/03_adventureworks.sh` creates the
  `adventureworks` DB and loads it.
- Query it from **Trino / Superset** via the `adventureworks` catalog, or directly:
  ```bash
  docker exec -it postgres psql -U postgres -d adventureworks -c \
    "select top 5 firstname, lastname from person.person;"    # (or a normal LIMIT query)
  # Trino:
  docker exec -it trino trino --execute "SELECT count(*) FROM adventureworks.sales.salesorderheader"
  ```

⚠️ It only loads on a **fresh Postgres volume** (init scripts run once). If Postgres already
has a volume, reload with: `make down` (removes volumes) → `make init` → `make up`.
If `ruby` is missing at `make init`, the prep is skipped and the stack still starts — just
without the `adventureworks` DB.

## Query tools — which to use for what
Two complementary tools (plus a desktop option):

- **Superset** (http://localhost:8004, `admin`/`admin`) — **BI / analytics**: dashboards and
  charts over the **Gold** layer. Read-only by design.
- **SQLPad** (http://localhost:8003, `admin@de.local`/`admin1234`) — **SQL workbench**: run
  *any* SQL, including `INSERT`/`UPDATE`/`DELETE`/`MERGE`/DDL, against **both** the OLTP source
  (Postgres) and the OLAP lakehouse (Trino/Iceberg). This is the tool for the write/DDL
  lessons. Three connections come pre-wired: **ShopFlow — OLTP**, **AdventureWorks — OLTP**,
  and **Lakehouse — OLAP (Trino/Iceberg)**.

> Superset is a read-only BI tool (it only runs `SELECT`); use **SQLPad** — or DBeaver
> below — whenever you need to write.

### DBeaver Desktop (optional, on your own machine)
Prefer a full desktop SQL IDE? [DBeaver Community](https://dbeaver.io/download/) connects
straight to the stack (both ports are published to your host):

| Connection | Driver | Host | Port | Database / Catalog | User / Pass |
|---|---|---|---|---|---|
| ShopFlow / AdventureWorks (OLTP) | PostgreSQL | `localhost` | `5432` | `shopflow` / `adventureworks` | course login `learner` / `learner` (admin: `postgres` / `postgres`) |
| Lakehouse (OLAP) | Trino | `localhost` | `8007` | catalog `iceberg` (or `shopflow`) | any user, no password |

## Notes
- Governance (Polaris per-persona RBAC + storage credential vending) is validated in
  compose and works the same as on k8s.
