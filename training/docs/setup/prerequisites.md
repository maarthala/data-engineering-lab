# Prerequisites & setup

This course is **hands-on**: you drive a running data platform from **your own machine** — a
browser and a couple of command-line tools. You never open a shell *inside* a container; you use
each tool the way you would in a real job: through its web UI or its client.

!!! info "The golden rule — you are a *user* of the stack, not its operator"
    Every tool below runs as a service (in Docker locally, or on Kubernetes). Your job is to
    **connect to it** — open a URL, or point a client at a port. You will never be asked to
    `docker exec`, `kubectl exec`, or `ssh` into anything. If a step ever tells you to "run this
    inside the container", it's the wrong course.

```mermaid
flowchart LR
  YOU["💻 Your machine<br/>browser"] -->|http / SQL| STACK["🐳 The stack<br/>Trino · Superset · Airflow<br/>Jupyter · Polaris · RustFS"]
```

## What you need

| # | Thing | Required? | Why |
|---|---|---|---|
| 1 | A modern **web browser** | ✅ Always | Every tool (Superset, Airflow, Jupyter, Polaris Console, RustFS) is a web UI |
| 2 | The **Trino CLI** | Optional, laptop stack only | A terminal client for Trino. On a team stack you run SQL in SQLPad / Superset instead |
| 3 | **Docker Desktop** + Git | ⚙️ Only if *you* run the stack | Needed to bring the platform up on your own laptop |

Item 1 is all a member needs when the stack is provided for you (a shared server, or an
instructor's machine). Item 3 is only for running the whole thing yourself.

!!! note "One lab account for everything — and no hosts-file entry"
    You **register once** on the landing page (just a username + password) and that **lab account**
    signs you in to every tool — Jupyter, Airflow, Superset, SQLPad, the Polaris Console and the
    RustFS console (single sign-on via Keycloak). There's **nothing to add to your hosts file**
    locally — just open the URLs. (On Kubernetes the cluster admin points the `*.de.lan` wildcard at
    the ingress, as for every other service.)

---

## 1. A web browser

Any recent Chrome, Edge, Firefox, or Safari. That's the entire requirement for Superset, Airflow,
Jupyter, the Polaris Console, RustFS, and this course site. Bookmark the landing page — it links to
everything:

- **Local (Docker):** [http://localhost:8000](http://localhost:8000)
- **Kubernetes:** `http://de.lan`

---

## 2. The Trino CLI (optional)

[Unit 2](../unit2/intro.md) runs SQL against the lakehouse in **SQLPad** (or Superset SQL Lab),
in the browser. Nothing to install, so you can skip this section.

The Trino CLI only works when you run the stack **on your own laptop**. On a team stack, Trino
accepts queries only from SQLPad and Superset, because they tell it who you are. Once installed,
connect with:

```bash
trino --server http://localhost:8007 --user demouser
```

You'll get a `trino>` prompt — type SQL, end each statement with `;`, quit with `quit;`.

=== "macOS"

    Homebrew installs the client (and pulls in Java automatically):

    ```bash
    brew install trino
    trino --version
    ```

=== "Windows"

    The Trino CLI is a Java program, so install a Java runtime first, then the client.

    **Option A — Scoop** (easiest if you have [Scoop](https://scoop.sh)):

    ```powershell
    scoop install temurin-jre    # Java runtime
    scoop install trino-cli
    trino --version
    ```

    **Option B — the executable JAR** (no package manager):

    1. Install a Java runtime — e.g. [Adoptium Temurin JRE 17+](https://adoptium.net)
       (or `winget install EclipseAdoptium.Temurin.17.JRE`). Check with `java -version`.
    2. Download `trino-cli-<version>-executable.jar` from the
       [Trino releases](https://trino.io/download.html).
    3. Run it (from the folder you downloaded it to):

    ```powershell
    java -jar trino-cli-*-executable.jar --server http://localhost:8007
    ```

    !!! tip "Prefer a GUI on Windows?"
        [DBeaver](https://dbeaver.io) (free) has a built-in Trino driver — **New Connection →
        Trino**, host `localhost`, port `8007`, no password. Or just use **Superset SQL Lab** in
        the browser. Either avoids the Java setup entirely.

=== "Linux"

    The CLI is a Java program. Install a JRE, then grab the client JAR:

    ```bash
    # Debian/Ubuntu example — any JRE 17+ works
    sudo apt-get install -y default-jre

    # download the self-contained client and make it runnable
    curl -Lo trino https://repo1.maven.org/maven2/io/trino/trino-cli/latest/trino-cli-latest-executable.jar
    chmod +x trino
    ./trino --server http://localhost:8007
    ```

    (Move `trino` somewhere on your `PATH`, e.g. `/usr/local/bin`, to call it from anywhere.)

!!! note "Java version"
    The Trino CLI needs a reasonably recent Java (JRE **17+** is a safe choice). `brew install trino`
    and Scoop handle this for you; for the raw JAR, install a JRE first and confirm with
    `java -version`.

---

## 3. Running the stack yourself (only if it isn't provided)

If someone already runs the platform for you, skip this — you just need items 1–2 and their URLs.
To run it on your **own** laptop, you need Docker and the repo.

=== "macOS"

    1. Install [**Docker Desktop for Mac**](https://www.docker.com/products/docker-desktop/)
       (Apple Silicon or Intel). Give it plenty of memory in **Settings → Resources**
       (see the note below).
    2. Install the Xcode command-line tools. They bring **Git**, **make**, **curl**, **unzip**,
       **ruby** and **python3**, everything the setup scripts use:

        ```bash
        xcode-select --install
        ```

=== "Windows"

    1. Install [**Docker Desktop for Windows**](https://www.docker.com/products/docker-desktop/)
       — it uses the **WSL 2** backend, which Docker Desktop can set up for you.
    2. Install [**Git for Windows**](https://git-scm.com/download/win).
    3. Do the clone/edit inside your **WSL 2** home (e.g. `\\wsl$\Ubuntu\home\you`) — it's far
       faster than the Windows filesystem and avoids CRLF line-ending surprises in shell scripts.
    4. Inside the WSL shell, install the tools the setup scripts use:

        ```bash
        sudo apt-get update && sudo apt-get install -y git make curl unzip ruby python3
        ```

=== "Linux"

    1. Install **Docker Engine** + the **Compose plugin**
       ([docs.docker.com/engine/install](https://docs.docker.com/engine/install/)).
    2. Add yourself to the `docker` group so you don't need `sudo`:
       `sudo usermod -aG docker $USER` (then log out/in).
    3. Install **Git** and the tools the setup scripts use:

        ```bash
        sudo apt-get install -y git make curl unzip ruby python3
        ```

How much memory Docker needs depends on how much of the stack you run at once. You set it on
Docker Desktop in **Settings → Resources** (Memory limit, CPU limit). With too little,
containers get killed (`exit 137`).

### Incremental: Core + what you're learning (recommended)

You don't need every service at once. `make status` groups them in **units**: **Core** (sign-in,
your files, the catalog) is always on, and you switch on only the units the part of the course
you're on needs. **4 CPUs** are enough.

| You're learning | Units to switch on (with Core) | They use about | Give Docker |
|---|---|---|---|
| Foundations | none (reading) | — | — |
| SQL on big data | SQL | 4 GB | **6 GB** |
| Python — Notebook | Notebooks | 3.5 GB | **6 GB** |
| Spark, dbt, Data Vault | Notebooks + Spark | 5 GB, ~7.5 GB while a job runs | **8 GB** |
| Orchestration (Airflow) | Airflow + Spark | 5.5 GB, ~8 GB while a job runs | **10 GB** |
| Data governance (Polaris) | SQL + Notebooks + Polaris console | 5 GB | **8 GB** |
| Business intelligence (Superset) | Superset (starts SQL with it) | 4 GB | **6 GB** |

*They use about* is what the units measured on a laptop, Core's ~2 GB included. A notebook adds up
to 1 GB for each member signed in to Jupyter. *Give Docker* leaves room for Docker itself and
for your queries.

!!! tip "Once, before the SQL lessons: Airflow + Spark"
    The ready-made ShopFlow tables the lessons read are built by an Airflow pipeline that runs
    Spark. Switch on **Airflow + Spark** once, run it (see [Fill the shared lake](deploy/general.md#fill-the-shared-lake-once)), then switch them off again.

### Full stack

Every service at once (`make up`): it uses about **9 GB** with nothing running and **11–12 GB**
under load. Give Docker **12–16 GB** and **6 CPUs**. **8 GB** and **4 CPUs** is the bare minimum:
`make up` checks this first, and below it says what's missing and asks before it starts anything.

!!! warning "Disk space"
    Plan for about **25 GB free**: ~2.5 GB of downloads (`make init`) plus ~15–20 GB of Docker
    images. On Docker Desktop check **Settings → Resources → Disk usage limit**.

!!! info "Why `ruby`?"
    `make init` uses it to prepare the **AdventureWorks** sample database (lesson {lesson:unit2/adventureworks-challenge.md}). Without
    ruby that step is skipped with a warning, and everything else still works.

With Docker and Git installed, follow **[Bring up the stack](deploy/index.md)** for the exact `make`
commands: [`make status`](deploy/make-status.md) for the incremental way, [`make up`](deploy/full-stack.md) for
the full stack (Docker Compose), or the [Ansible steps](deploy/general.md#on-kubernetes) (Kubernetes).

---

## Verify your setup

Run through this checklist once — if all three pass, you're ready for Unit {unit:unit1/what-is-de.md}.

1. **Sign in to the landing page** — open [http://localhost:8000](http://localhost:8000). You're sent
   to the **DE Learning Lab** login page: sign in as **`demouser` / `demouser`** (the default account
   the course examples use), **Register** your own, or use `manager` / `manager`. After signing in you see the lab's tools in four columns and,
   on **📁 My files**, **"bucket <name>-lake · lakehouse <name>_lake"** — your own private
   lakehouse, created on first login.
2. **Single sign-on works** — open **🗂️ My catalogs**, then **Open in Polaris Console ↗** next to
   your lakehouse `<name>_lake`. The Console signs you in with your lab account without asking
   again and shows your lakehouse.
3. **SQL works**: in SQLPad (connection **Lakehouse (Trino) — demouser**) or Superset SQL Lab:

    ```sql
    SELECT 'ready!' AS status;
    SELECT count(*) FROM shopflow.public.orders;   -- expect 40000
    SHOW SCHEMAS FROM iceberg;                      -- expect bronze, silver, gold (…)
    ```

    If the last one fails with `Unable to find warehouse polaris_lake`, run `make polaris-seed`
    (see [Bring up the stack](deploy/index.md)).

---

## Access cheat-sheet

Everything in the stack, its URL, and how to sign in. (On Kubernetes swap `localhost:<port>` for the
`*.de.lan` host shown on the landing page.)

| Service | Local URL | Login |
|---|---|---|
| Landing page | [http://localhost:8000](http://localhost:8000) | **`demouser` / `demouser`** (default — the examples use it), `learner2` / `learner2` (a second user, for sharing in {lesson:unit6/polaris.md}), your own (**Register**), or `manager` / `manager` |
| Keycloak (logins) | http://localhost:8180/admin | admin console: `admin` / `admin` (realm **de-lab**) |
| Training course | https://maarthala.github.io/data-engineering-lab/ | — (linked from the landing page) |
| Jupyter (notebooks) | http://localhost:8008 | your lab account — your **own** Jupyter, and `my` = your own lakehouse |
| Spark master UI | http://127.0.0.1:8002 | — |
| Trino | through SQLPad / Superset (laptop stack: UI http://localhost:8007/ui/, CLI `:8007`) | lab account (via SQLPad / Superset) |
| SQLPad (SQL workbench) | http://localhost:8003 | lab account (from the landing page you go straight in) |
| Superset (BI) | http://localhost:8004 | lab account (managers = Admin) |
| Airflow | http://localhost:8001 (or the `AIRFLOW_HOST_PORT` you set) | lab account (managers = Admin) |
| Polaris Console (governance) | http://localhost:8189 | lab account · Client ID/Secret form at `/login?local=1` (admin `root` / `s3cr3t`, [personas](personas.md)) |
| RustFS console (S3) | http://localhost:9001/rustfs/console/ | lab account — you see **your own bucket** `<name>-lake` · root `admin` / `admin123` |
| My files · My catalogs | links at the top of the landing page | your bucket's files; create catalogs & share them ([details](workspace.md)) |

!!! warning "On a team stack these admin logins are private"
    The passwords above are the defaults of **your own laptop stack**, where you are the admin.
    On a shared team stack (Kubernetes) every admin and service password is a random value in
    the cluster's `de-stack-secrets` — only your **platform manager** has them. Members sign in
    with their own lab account, and use PostgreSQL as `learner` / `learner`.

## You can now…
- Set up your machine to use the stack (a browser; optionally the Trino CLI) on macOS, Windows, or Linux
- Sign in once with your lab account and reach every tool
- Run the whole platform yourself with Docker, if it isn't already provided
- Prove your setup works with the three-step verification checklist
