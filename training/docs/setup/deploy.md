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
    - **Docker Desktop** (≥ 8 GB RAM allocated — 12–16 GB is comfortable) and **Git**
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
    make up                # build the images + start every service
    make polaris-seed      # create the lakehouse catalog (iceberg) + personas
    ```

    !!! warning "Don't skip `cp .env.example .env`"
        Without `.env`, `make up` stops at once with **`no configuration file provided: not
        found`**. The file is gitignored, so each clone needs its own copy.

    !!! warning "`make polaris-seed` is needed for everything in `iceberg`, and again after a reset"
        It creates the `iceberg` lakehouse catalog. Until it runs, every `iceberg.…` query
        (Units {unit:unit2/intro.md}–{unit:unit6/polaris.md}) fails with **`Unable to find warehouse polaris_lake`**.
        Postgres keeps no data volume, so the catalog is lost whenever the Postgres container is
        recreated: after `make down`, `make restart`, or a `make up` that rebuilds it. **Run
        `make polaris-seed` again** after any of those, or whenever you see that error. It's safe
        to re-run.

    !!! note "The first `make up` is slow"
        It builds every image from scratch, which takes **15–30 minutes** depending on your machine
        and network. Later runs reuse the cache and start in a minute or two.

    ### Optional settings in `.env`
    | Setting | When you need it |
    |---|---|
    | `POSTGRES_HOST_PORT=5433` | something else on your machine already uses port **5432** |
    | `AIRFLOW_HOST_PORT=8011` | something else already uses port **8001** (then open Airflow at `localhost:8011`) |
    | `GIT_TOKEN=<token>` | you want Jupyter to auto-push saved notebooks to a git repo. Leave it empty and Jupyter works normally, without auto-push |

    If `make up` fails with **`port is already allocated`**, set the matching port above and run
    `make up` again.

    ### Verify
    ```bash
    make ps                       # all services should be "running"/"healthy"
    ```
    Open the landing page at **<http://localhost:8000>** and sign in as **`demouser` / `demouser`** —
    the default lab account every course example is written for (or **Register** your own, or use
    `manager` / `manager`). Every tool tiles off it. Then run the
    [setup checklist](prerequisites.md#verify-your-setup).

    ### Fill the shared lake (once)
    `make polaris-seed` creates the shared lake (`polaris_lake`) with **empty** bronze / silver /
    gold namespaces. The ready-made ShopFlow tables the lessons read — `shared.gold.daily_sales`
    in a notebook, `iceberg.gold.daily_sales` in Trino / SQLPad / Superset — are built by an
    Airflow pipeline. Run it once:

    1. Open **Airflow** (landing page → Orchestration), signed in as `manager`.
    2. Find **`shopflow_medallion`**, switch it **on** (new DAGs start paused) and press
       **▶ Trigger**.
    3. After about **2–3 minutes** all three tasks are green: Bronze → Silver → Gold.

    Until then those queries answer *table not found*. Re-run it after a reset (`make down`,
    `make restart`).

    ### Day-to-day
    | Command | What it does |
    |---|---|
    | `make ps` | show running services |
    | `make logs` (`make logs S=trino`) | tail logs (all, or one service) |
    | `make docs` | rebuild the course site after editing `training/docs/**` |
    | `make restart` | `down` then `up`, so it **wipes data** too (then re-run `make polaris-seed` and the `shopflow_medallion` DAG) |
    | `make down` | **stop the stack and remove volumes** (wipes data) |
    | `docker compose stop` / `docker compose start` | pause and resume, **keeping** your data and catalog |
    | `make clean` | remove the images built for this stack |

    !!! warning "`make down` deletes the volumes"
        It runs `docker compose down -v`, so the object store (RustFS), Postgres, the catalog and the
        **lab accounts** are wiped — registered accounts are gone (`demouser`, `learner2` and `manager`
        come back by themselves), and each member's lakehouse and bucket are recreated, empty, on their next
        sign-in. Use it
        for a clean reset; use `docker compose stop` if you only want to pause and keep your data.

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
- Bring the whole stack up with Docker Compose (`cp .env.example .env`, `make init`, `make up`, `make polaris-seed`, then the `shopflow_medallion` DAG once) or on Kubernetes (Ansible)
- Verify every service is running and reach the landing page
- Tear it down / reset cleanly, and know which command wipes data
