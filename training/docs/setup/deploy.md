# Bring up the stack

This page is for **whoever runs the platform** — you on your laptop (Docker Compose) or an operator
on a cluster (Kubernetes). If someone already runs the stack for you and gave you URLs, you don't
need this — go straight to [Prerequisites](prerequisites.md) (just a browser) and start
learning.

!!! info "Two ways to run the *same* stack"
    The lakehouse is identical either way — same services, same course. **Docker Compose** runs it
    all on one machine (the normal choice for learning). **Kubernetes** runs it on a cluster (closer
    to production). Pick the tab that matches your setup.

=== "Local (Docker Compose)"

    ### Prerequisites
    - **Docker Desktop** (≥ 8 GB RAM and 4 CPUs allocated — 12–16 GB and 6 CPUs are comfortable) and **Git**
      — see [Prerequisites → run the stack yourself](prerequisites.md#3-running-the-stack-yourself-only-if-it-isnt-provided).

    ### Steps
    ```bash
    # 1. get the repo
    git clone https://github.com/maarthala/data-engineering-lab.git
    cd data-engineering-lab/local      # every command below runs from local/

    # 2. first run only
    cp .env.example .env   # REQUIRED: .env tells Docker Compose which services make up the stack
    make init              # download Spark/Iceberg jars (~2.5 GB) + prepare AdventureWorks

    # 3. start it
    make up                # build the images (first time) + start every service
                           # (also creates the lakehouse catalog `iceberg` + personas)

    # 4. done for the day
    make down              # bring it down, keep all data — `make up` starts it again
    ```

    !!! warning "Don't skip `cp .env.example .env`"
        Without `.env`, `make up` stops at once with **`no configuration file provided: not
        found`**. The file is gitignored, so each clone needs its own copy.

    !!! note "`make up` creates the `iceberg` lakehouse catalog when it's missing"
        Every `iceberg.…` query (Units {unit:unit2/intro.md}–{unit:unit6/polaris.md}) needs it. The
        catalog lives in Postgres's volume, so `make down` → `make up` keeps it; on the first start
        and after `make clean` / `make destroy` (which delete it), `make up` runs `make polaris-seed`
        for you and says so in its summary. If you still see **`Unable to find warehouse
        polaris_lake`** (e.g. you started with `make up-plain`), run `make polaris-seed` — it's
        safe to re-run.

    !!! note "The first `make up` is slow"
        It builds every image from scratch, which takes **15–30 minutes** depending on your machine
        and network. Later runs don't build at all — they start in a minute or two. After changing
        a Dockerfile, run `make build`.

    ### Optional settings in `.env`
    | Setting | When you need it |
    |---|---|
    | `POSTGRES_HOST_PORT=5433` | something else on your machine already uses port **5432** |
    | `AIRFLOW_HOST_PORT=8011` | something else already uses port **8001** (then open Airflow at `localhost:8011`) |
    | `GIT_TOKEN=<token>` | you want Jupyter to auto-push saved notebooks to a git repo. Leave it empty and Jupyter works normally, without auto-push |

    If a port is taken, `make up` stops before starting anything and lists each one — what needs
    it, what holds it, and the fix (for Postgres and Airflow, the `.env` line to add). Free it or set
    the port above, and run `make up` again.

    ### Verify
    ```bash
    make ps                       # all services should be "running"/"healthy"
    ```
    Open the landing page at **<http://localhost:8000>** and sign in as **`demouser` / `demouser`** —
    the default lab account every course example is written for (or **Register** your own, or use
    `manager` / `manager`). Every tool tiles off it. Then run the
    [setup checklist](prerequisites.md#verify-your-setup).

    ### Fill the shared lake (once)
    `make up` (via `make polaris-seed`) creates the shared lake (`polaris_lake`) with **empty** bronze / silver /
    gold namespaces. The ready-made ShopFlow tables the lessons read — `shared.gold.daily_sales`
    in a notebook, `iceberg.gold.daily_sales` in Trino / SQLPad / Superset — are built by an
    Airflow pipeline. Run it once:

    1. Open **Airflow** (landing page → Orchestration), signed in as `manager`.
    2. Find **`shopflow_medallion`**, switch it **on** (new DAGs start paused) and press
       **▶ Trigger**.
    3. After about **2–3 minutes** all three tasks are green: Bronze → Silver → Gold.

    Until then those queries answer *table not found*. Re-run it after a reset (`make clean`,
    `make destroy`).

    ### Day-to-day
    Run them from `local/`. The last column is what matters most:

    | Command | What it does | Your data |
    |---|---|---|
    | `make init` | **first run only**: downloads the Spark/Iceberg jars (~2.5 GB, into `common/dockerfiles/tmp`) and prepares AdventureWorks. Starts nothing | — |
    | `make up` | first checks Docker's CPUs and memory against the minimum (below it: says what's missing and asks before starting; `LAB_SKIP_CHECKS=1 make up` skips the question) and that every port it needs is free — if one is taken it starts nothing and says by what (another container, or a program and its pid) and how to free or change it; then starts every service; after `make down` it picks up where you left off. It builds an image only when it's missing (first run, after `make destroy`) and warns when a Dockerfile — or a file it copies in — changed since its image was built. Shows a live view — the log on the left, every service's state on the right — then the links to open (`make up-plain`: plain output) | kept |
    | `make status` | a live dashboard of the running stack, any time — see below | — |
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

    `make status` shows, live:

    - **left** — the logs of **all** containers, each line with its service's name in colour;
      errors in red, warnings in yellow;
    - **right** — every service with its state (● running/healthy, ✓ setup job done, ✗ failed,
      ↻ restarting, ○ stopped), **CPU**, **memory** and **port**, grouped in **units** (below),
      plus the members' Jupyter servers; below it, details of the selected unit;
    - **top** — total CPU and memory (of what Docker may use), how many services are ready, and
      how many are switched off.

    Keys: **↑ ↓** pick a **unit** · **s** turn it on · **x** turn it off (asks y / n) · **f** only
    its logs · **e** errors and warnings only · **a** all logs again · **space** pause the log ·
    **q** quit (the stack keeps running). Below the list: the selected unit's services, CPU,
    memory and ports.

    **Units: run only what you're learning.** On a laptop with little memory, stop the units you
    don't need. Each unit's header says ● on, ○ off or ◐ partly on, and how much memory it uses.

    | Unit | Services | Needed for |
    |---|---|---|
    | **Core** | postgres, redis, keycloak, vault, oauth2-proxy, nginx, home-api, storage, polaris | signing in, your files, the catalog — every unit needs it, so **x** on Core stops the **whole stack** (it asks first, in red) |
    | **SQL** | trino, sqlpad | SQL in SQLPad, and Superset's data |
    | **Notebooks** | jupyterhub, jupyter | Jupyter: pandas, SQL and Python practice (stopping it also removes the members' Jupyter servers; their files stay) |
    | **Spark** | spark-master, spark-worker-1, spark-connect | Spark code — in a notebook (`SparkSession`) or in a DAG (`lab_spark()`) |
    | **Airflow** | airflow-* | DAGs (they come from your bucket, so Notebooks can stay off; DAGs using Spark need **Spark**) |
    | **Superset** | superset | dashboards (starting it also starts **SQL**; stopping SQL also stops it) |
    | **Polaris console** | polaris-console, polaris-proxy | the catalog's web UI |

    Stopping a unit keeps all its data (`docker compose stop`). The next `make up` starts every
    unit again.

    **Start small:** with the stack down, `make status` shows **Core ○ off · s start**. Press
    **s** to start just Core, then **s** on the units you need. Pressing **s** on a unit while Core
    is off starts Core with it. For a SQL-only session that's Core + SQL; for pandas, Core + Notebooks; for a Spark
    pipeline in Airflow, Core + Airflow + Spark.

    !!! warning "`make clean` and `make destroy` delete all the data"
        Both delete the stack's volumes, so the object store (RustFS), Postgres, the catalog and the
        **lab accounts** are wiped — registered accounts are gone (`demouser`, `learner2` and `manager`
        come back by themselves), and each member's lakehouse and bucket are recreated, empty, on their next
        sign-in. After either: `make up` (it recreates the catalog) and the `shopflow_medallion` DAG again.
        They only touch this stack — other projects' containers, volumes and images are left alone —
        and keep the `make init` downloads. To stop for the day, use **`make down`**.

=== "Kubernetes"

    Deploying to a cluster is an **operator** task (more moving parts than Compose). The canonical
    guide is [`k8s/README.md`](https://github.com/maarthala/data-engineering-lab/blob/main/k8s/README.md)
    and [`k8s/ansible/README.md`](https://github.com/maarthala/data-engineering-lab/blob/main/k8s/ansible/README.md);
    the essentials are below.

    ### Prerequisites
    - A **Kubernetes cluster** (this repo targets MicroK8s) with the `ingress`,
      `hostpath-storage`, and `metrics-server` addons, plus an external **Postgres** reachable.
    - **Wildcard DNS** `*.de.lan → <node-ip>` so all the `*.de.lan` UIs resolve.
    - On your workstation: **`kubectl`**, **`helm`**, **`ansible`**, **`docker`**, and **`git`**, with
      `KUBECONFIG` pointed at the cluster.

    ### Steps — Ansible (recommended)
    ```bash
    cd k8s/ansible
    cp group_vars/vault.example.yml group_vars/vault.yml   # add a git PAT, then: ansible-vault encrypt group_vars/vault.yml
    ansible-playbook build-load.yml --ask-become-pass       # build the custom images + import them to the node
    ansible-playbook deploy.yml     --ask-vault-pass         # helm install (in waves) + seed the governed catalog
    ```

    ??? note "Manual (no Ansible) — deploy the whole chart at once"
        ```bash
        kubectl create namespace de-stack
        kubectl -n de-stack create secret generic de-stack-git-token --from-literal=token=<PAT>
        helm template de-stack k8s/helm/de-stack | kubectl -n de-stack apply -f -
        # then seed Polaris: run common/polaris/seed-polaris.sh inside the polaris pod
        # (POLARIS_URL=http://localhost:8181) — see common/polaris/seed-polaris.sh
        ```

    ### Verify
    ```bash
    kubectl -n de-stack get pods         # wait for everything to be Running/Ready
    ```
    Open the landing page at **`http://de.lan`**. Members **register / sign in** there (Keycloak at
    `http://auth.de.lan`, realm `de-lab`); each gets their own lakehouse, bucket and Jupyter on first
    login. All UIs live at `http(s)://<name>.de.lan` (jupyter, superset, sqlpad, airflow, spark,
    storage — the RustFS console, polaris-console) — the landing page links them.

    **First sign-in:** every admin password on Kubernetes is random (the laptop defaults don't
    work). Read the manager's with
    `kubectl -n de-stack get secret de-stack-secrets -o jsonpath='{.data.manager-password}' | base64 -d`
    and sign in as **`manager`**; the other admin logins and their keys are listed in
    [`k8s/README.md` → First sign-in after deploying](https://github.com/maarthala/data-engineering-lab/blob/main/k8s/README.md#first-sign-in-after-deploying).

    **Fill the shared lake once:** in Airflow (`http://airflow.de.lan`, as `manager`) switch
    **`shopflow_medallion`** on and trigger it — Bronze → Silver → Gold in under a minute. (Same
    DAG as locally; on k8s it runs the jobs through Spark Connect.)

    ### Teardown
    ```bash
    helm template de-stack k8s/helm/de-stack | kubectl -n de-stack delete -f - || true
    kubectl delete namespace de-stack
    ```

## After it's up
- **Register your lab account** on the landing page — see
  [0.3 Your lab account & workspace](workspace.md) for what you get and where things are.
- For the governance lessons, sign in to the Polaris Console as one of the
  [personas](personas.md) (`analyst` / `engineer` / `lead`).
- The training course is published on **GitHub Pages**
  (<https://maarthala.github.io/data-engineering-lab/>), auto-deployed on every push to
  `main` — and the stack's landing page links straight to it, so it's always the current version.

## You can now…
- Bring the whole stack up with Docker Compose (`cp .env.example .env`, `make init`, `make up`, then the `shopflow_medallion` DAG once) or on Kubernetes (Ansible)
- Verify every service is running and reach the landing page
- Tear it down / reset cleanly, and know which command wipes data
