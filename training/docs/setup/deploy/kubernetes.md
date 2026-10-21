# Kubernetes setup

Deploying to a cluster is an **operator** task (more moving parts than Compose). The canonical
guide is [`k8s/README.md`](https://github.com/maarthala/data-engineering-lab/blob/main/k8s/README.md)
and [`k8s/ansible/README.md`](https://github.com/maarthala/data-engineering-lab/blob/main/k8s/ansible/README.md);
the essentials are below.

## Prerequisites
- A **Kubernetes cluster** (this repo targets MicroK8s) with the `ingress`,
  `hostpath-storage`, and `metrics-server` addons, plus an external **Postgres** reachable.
- **Wildcard DNS** `*.de.lan → <node-ip>` so all the `*.de.lan` UIs resolve.
- On your workstation: **`kubectl`**, **`helm`**, **`ansible`**, **`docker`**, and **`git`**, with
  `KUBECONFIG` pointed at the cluster.

## Steps — Ansible (recommended)
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

## Verify
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

## Switch units off to free the node

The services are grouped in the same **units** as on a laptop
([make status](make-status.md#units)). A cluster has no `make status`: a manager switches units
in the lab's web page, **Manage → ⚙️ Settings → Services**. Off scales the unit's services to 0;
their data stays. Switching **Notebooks** off first stops the members' Jupyter servers (their
files stay). **Core** has no switch: it runs the page you switch from.

While a unit is off, its tile on the home page is grey: members see *Not running. Contact admin.*

!!! note "A lab update switches every unit back on"
    Deploying again (`deploy.yml` or `helm template … | kubectl apply`) sets every service back
    to 1 replica. Switch the units you don't need off again afterwards.

## Teardown
```bash
helm template de-stack k8s/helm/de-stack | kubectl -n de-stack delete -f - || true
kubectl delete namespace de-stack
```
