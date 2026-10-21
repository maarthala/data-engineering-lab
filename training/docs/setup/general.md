# General information

What applies however you start the stack: checking it works, the one-time pipeline that fills the
shared lake, the day-to-day commands, and running it on Kubernetes instead.

## Verify
```bash
make ps                       # the services you started should be "running"/"healthy"
```
Open the landing page at **<http://localhost:8000>** and sign in as **`demouser` / `demouser`** —
the default lab account every course example is written for (or **Register** your own, or use
`manager` / `manager`). Every tool tiles off it. Then run the
[setup checklist](prerequisites.md#verify-your-setup).

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

## On Kubernetes

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
