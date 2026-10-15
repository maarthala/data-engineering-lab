# Data Engineering Lab

A hands-on Data Engineering learning environment — a **governed lakehouse**
(RustFS object storage, Apache Polaris governed Iceberg catalog, Spark, Trino, Superset, Airflow,
Jupyter) that you can run two ways with the **same stack**:

## 📚 Training course

Read the hands-on **Data Engineering course** online — build the ShopFlow
Bronze→Silver→Gold lakehouse and map every skill to Databricks, Snowflake & Fabric:

**→ [maarthala.github.io/data-engineering-lab](https://maarthala.github.io/data-engineering-lab/)**

Start with **Prerequisites & setup** and **Bring up the stack**, then work through the units.
The stack's landing page links straight to this course, so it's always the current version.

## 💬 Community & help

Join our **Discord** to discuss the stack, get help with setup or problems, and connect
with other learners:

**→ [discord.gg/sByPdSRFjf](https://discord.gg/sByPdSRFjf)**

- Instructor-led **DE training** for students & career-changers — **learning@epireum.com**
- **Companies** looking for DE talent or delivery — **contact@epireum.com**

## Setup — pick your environment

### 🖥️ [Local (Docker Compose)](./local/README.md)
Run the whole governed lakehouse on a single machine with Docker Compose.
Fastest to start; great for learning and offline use.
→ **[local/README.md](./local/README.md)**

### ☸️ [Kubernetes](./k8s/README.md)
The same stack on a MicroK8s/Kubernetes cluster via a Helm umbrella chart, with a
one-command Ansible bootstrap (build-and-import for locked-down nodes, or pull from
a registry for bigger clusters).
→ **[k8s/README.md](./k8s/README.md)**

## Repository layout
```
local/    Docker Compose stack + its setup, config, and challenges (Tasks.md)
k8s/      Helm chart (k8s/helm) + Ansible bootstrap (k8s/ansible)
common/   Shared by both: Dockerfiles for the custom images, the Polaris seed
          (common/polaris), and the Polaris Console build (common/polaris-console)
```

Both environments deploy the **same governed-lakehouse component set** and are
self-contained (each runs its own Postgres). What differs is only the orchestrator
(Compose vs Kubernetes) and the hostnames (localhost ports vs `*.de.lan` ingress).

## Major releases & integrations
Notable additions — a new tool, data source, or capability — are logged here so you
can see what's new at a glance. This is **not** a per-commit changelog; only
integration-level updates (like AdventureWorks or SQLPad) get an entry.

| Date | Release / integration | What it adds | Scope |
|---|---|---|---|
| 2026-10-15 | **Teams** | Every member is in one team (Keycloak group `team-<team>`, new members start in `general`); each team has its own lakehouse `team_<team>` (bucket `team-<team>-lake`) that every member reads and writes — `team.…` in Jupyter, `team_<team>.…` in SQLPad / Superset. Managers create and delete teams and add members by name on **🛠️ Manage → 🧩 Teams** (Manage is now three pages: Members, Teams, Settings); the team's bucket `team-<team>-lake` holds files for the whole team (My files → Team, Jupyter, RustFS console, Spark); a member who moves loses the old team's lakehouse, the tables stay with the team. Course 0.3 updated | local, course |
| 2026-10-14 | **Key vault (OpenBao)** | The lab's own key vault: OpenBao (open-source Vault), stock image, stored encrypted in the stack's Postgres and unlocked on its own at every start. **🔑 My secrets** (was My variables) keeps each member's keys and passwords in a folder only their login reaches; notebooks and DAGs read them with `key_vault("NAME")`, never via `os.environ`. The vault's own UI signs in with the lab account (Keycloak OIDC): members see only their folder, managers all. Each Jupyter start gets a fresh one-week vault token; old My variables moved in automatically; wipe / delete clear them. Course 0.3 + the API lesson updated | both, course |
| 2026-10-13 | **dbt (on Spark)** | dbt Core + dbt-spark in every member's Jupyter: SQL models run through Spark Connect **as the member** (own lakehouse only); a starter project in Jupyter's `dbt/` folder, mirrored to the bucket (`target/`, `logs/` stay local); `lab_dbt()` runs a member's project from their Airflow DAG, signed in as them; new course unit *Transform with dbt* (11 lessons). Course numbering now follows the nav order automatically | both, course |
| 2026-10-03 | **Manage members (manager console)** | Managers get 👥 **Manage members** on the landing page: create members, new password, reset lakehouse, wipe (back to a new member's setup), delete, and a **self-registration** on/off switch; Jupyter files reset with the member | local · k8s |
| 2026-10-03 | **Per-user SQL & production-like sources** | Trino only through SQLPad (a private connection per member) and Superset (runs as the signed-in user), with access rules generated from Polaris; the Postgres sources are **read-only** for members; Airflow shows each member only their own DAGs (+ platform DAGs read-only); fair Spark scheduling and resource limits | local · k8s |
| 2026-10-03 | **One sign-in, straight in** | Signed in to the lab, every tool opens directly (no "Sign in with…" click); home page regrouped as Build & explore · Pipelines · Visualize · Lakehouse; lab code from ConfigMaps on k8s (code changes without image rebuilds) | local · k8s |
| 2026-10-03 | **Learner isolation & private admin secrets** | Learners can reach only their own data: the Spark cluster holds no root keys, Polaris vends short-lived per-table credentials (STS), learners use their own storage key and a limited PostgreSQL login (`learner`); on a team stack every admin/service password is a random value in the `de-stack-secrets` Secret (`k8s/create-secrets.sh`) | local · k8s |
| 2026-10-02 | **Tool-neutral platform names & sharing** | Object storage is the service `storage` (`http://storage:9000`, root `admin` / `admin123`) — swap the S3 store without touching clients; admin account `manager` (group `managers`); second local account `learner2`; learners share a namespace or table read/write on **My catalogs** (6.1 rewritten around it); Postgres on a named volume; code-only lab images on dated tags | local · k8s |
| 2026-10-02 | **Learner workspace & course on it** | Default account `demouser`; bucket laid out like a Fabric lakehouse (`files/src`, `files/source`, `tables/`); starter kit (practice CSVs, `bronze.sample_orders`, a load notebook + DAG); `lab_spark()` / `run_notebook()` so learner DAGs write their own lakehouse; learner catalogs in Trino → SQLPad/Superset; every lesson rewritten and run as `demouser` | local |
| 2026-10-01 | **Single sign-on for every tool** | One lab account (Keycloak) for Jupyter, Airflow, Superset, SQLPad, Polaris Console and the RustFS console; instructors are admins everywhere | local · k8s |
| 2026-10-01 | **JupyterHub — one Jupyter per learner** | Per-learner Jupyter (DockerSpawner locally, KubeSpawner on k8s) with `iceberg` = own lakehouse and `shared` = the course lake; `notebooks/` + `dags/` mirrored to the learner's bucket | local · k8s |
| 2026-10-01 | **RustFS object store (replaces MinIO)** | S3-compatible, Apache-2.0; per-learner bucket `<user>-lake` (100 MB quota, own-bucket policy); **My files** page | local · k8s |
| 2026-10-01 | **My catalogs + learner DAGs** | Learners create catalogs and share namespaces/tables (read/write, revoke); Airflow loads every learner's `dags/` (dag_id prefix `<user>_`) | local · k8s |
| 2026-10-01 | **Learner front door (Keycloak)** | Landing page behind a Keycloak login / self-registration (oauth2-proxy); each learner gets their own Polaris lakehouse (`<name>_lake`, owner) + read-only shared lake on first login (home-api) | local · k8s |
| 2026-10-01 | **Delta SQL + table maintenance** | Delta SQL extension + DeltaCatalog enabled (`VACUUM`, `OPTIMIZE … ZORDER BY`, `DESCRIBE HISTORY`); Iceberg compaction / z-order / snapshot expiry / orphan cleanup verified from Spark + Trino; default Spark catalog now in-memory (no Hive Metastore) | local · k8s |
| 2026-09-30 | **Materialized views in `%%sql`** | `CREATE / REFRESH / DROP / SHOW MATERIALIZED VIEW` in notebooks, via Spark 4.1 Declarative Pipelines; results governed in Polaris (lesson 4.5) | local · k8s |
| 2026-09-29 | **Stack upgrade (Spark 4.1)** | Spark 4.1.3 + Iceberg 1.11 + Delta 4.4, Trino 483, Airflow 3.3.2, Superset 6.1, Polaris 1.8, Postgres 18; images pinned (only polaris-console on `:latest`); MinIO + mc mirrored to GHCR | local · k8s |
| 2026-09-29 | **Google Analytics (course site)** | GA4 page-view + site-search tracking on the published course pages | course |
| 2026-09-17 | **AdventureWorks SQL challenge** | Course unit 2.11 — 33 progressive SQL challenges (joins → CASE/COALESCE → windows → CTEs → subqueries/outer-joins → RFM capstone) with solutions | course |
| 2026-09-20 | **SQLPad (SQL workbench)** | Web SQL editor for read **+ write** (INSERT/UPDATE/DDL) on Postgres (OLTP) & Trino (OLAP); replaced Metabase | local · k8s |
| 2026-09-17 | **AdventureWorks OLTP** | Full 68-table AdventureWorks sample DB in Postgres, queryable via Trino/SQLPad/Superset | local · k8s |
| 2026-09-17 | **Spark internals lessons** | Course units 4.7 (architecture & query lifecycle) and 4.8 (data skew & salting) | course |
| 2026-09-13 | **Notebook auto-push** | Jupyter commits + pushes each notebook to git on every save | local · k8s |
| 2026-09-13 | **`%%sql` cell magic** | Databricks-style SQL cells in notebooks, backed by the governed Spark session | local · k8s |
| 2026-09-13 | **Pre-created Spark session** | A ready `spark` session on notebook open — no boilerplate | local · k8s |
| 2026-09-12 | **Azure AdventureWorks** | AdventureWorks sample on Azure SQL for the ADF learning track | azure |
| 2026-09-10 | **Azure ADF (Terraform)** | Provision Azure Data Factory + SQL as code (base-setup + sql stacks) | azure |
| 2026-09-08 | **Apache Polaris governance** | Single governed Iceberg REST catalog for Spark & Trino (replaced Unity Catalog) | local · k8s |
| 2026-09-08 | **Polaris on Kubernetes** | The governed catalog + Console added to the Helm chart | k8s |
| 2026-09-08 | **Medallion Airflow DAG** | Bronze→Silver→Gold pipeline orchestrated end-to-end | local · k8s |
| 2026-09-06 | **ShopFlow DE course** | Hands-on training site (units 0–10) served by the stack landing page | course |
| 2026-09-06 | **Kubernetes deployment** | Helm umbrella chart + one-command Ansible bootstrap | k8s |
| 2026-09-06 | **Governed lakehouse stack** | MinIO, Spark (+ Connect), Trino, Superset, Airflow, Jupyter on Compose | local |
| 2026-09-06 | **ShopFlow dataset** | 12-table e-commerce sample seeded into Postgres + the lake | local · k8s |
