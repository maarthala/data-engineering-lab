# make status: how to use it

`make status` is one screen to watch the stack and switch parts of it on and off. Run it from
`local/` at any time, with the stack up or down. **q** quits the screen; the services keep
running.

The services are grouped in **units**. **Core** is always needed; you add only the units the
lesson needs. That way a laptop with 8 GB for Docker runs the course comfortably, one part at a
time ([memory per part of the course](prerequisites.md#incremental-core-what-youre-learning-recommended)).

## Your first session

1. Run `make status`. With the stack down it shows **Core ○ off · s start**.
2. Press **s**. Core starts: sign-in, your files and the catalog. The first time, its images
   are built first (several minutes), and the lakehouse catalog `iceberg` is created.
3. Press **↓** to the unit the lesson needs, e.g. **SQL**, and press **s**. Pressing **s** on a
   unit while Core is off starts Core with it.
4. When the unit's header shows **● on**, open <http://localhost:8000> and sign in.
5. Press **q** to leave the screen. Everything you started keeps running.

Next time, run `make status` again: it shows what's on. Moving to another part of the course,
switch on its units and switch off the ones you no longer need with **x** (it asks y / n).
Done for the day: **x** on Core stops the whole stack (it asks first, in red), or run
`make down`.

## The screen

- **left**: the logs of **all** containers, each line with its service's name in colour;
  errors in red, warnings in yellow;
- **right**: the units, each with a header (**● on**, **○ off** or **◐ partly on**) and its
  memory, then every service with its state (● running/healthy, ✓ setup job done, ✗ failed,
  ↻ restarting, ○ stopped), **CPU**, **memory** and **port**, plus the members' Jupyter
  servers. Below the list, the selected unit's services, memory and ports;
- **top**: total CPU and memory (of what Docker may use), how many services are ready, and
  how many are switched off.

With Core and SQL on, the right side starts like this (shortened):

```text
▸ CORE ● on · x stops all          healthy  2.0G
  ● postgres                        0.1%     75M   :5432
  ● keycloak                        0.2%    732M   :8180
  …
│ SQL ● on                          healthy  1.6G
  ● trino                           2.2%    1.5G   :8007
  ● sqlpad                          0.0%     95M   :8003
│ NOTEBOOKS ○ off                   stopped
  ○ jupyterhub                      stopped
  ○ jupyter                         stopped
```

`▸` marks the selected unit.

## Keys

| Key | What it does |
|---|---|
| **↑ ↓** | pick a unit |
| **s** | turn the unit on |
| **x** | turn the unit off (asks y / n); on Core it stops the whole stack |
| **f** | show only the selected unit's logs |
| **e** | show only errors and warnings |
| **a** | show all logs again |
| **space** | pause the log |
| **q** | quit the screen (the stack keeps running) |

## Units

| Unit | Services | Needed for |
|---|---|---|
| **Core** | postgres, redis, keycloak, vault, oauth2-proxy, nginx, home-api, storage, polaris | signing in, your files, the catalog. Every unit needs it, so **x** on Core stops the **whole stack** |
| **SQL** | trino, sqlpad | SQL in SQLPad, and Superset's data |
| **Notebooks** | jupyterhub, jupyter | Jupyter: pandas, SQL and Python practice (stopping it also removes the members' Jupyter servers; their files stay) |
| **Spark** | spark-master, spark-worker-1, spark-connect | Spark code: in a notebook (`SparkSession`) or in a DAG (`lab_spark()`) |
| **Airflow** | airflow-* | DAGs (they come from your bucket, so Notebooks can stay off; DAGs using Spark need **Spark**) |
| **Superset** | superset | dashboards (starting it also starts **SQL**; stopping SQL also stops it) |
| **Polaris console** | polaris-console, polaris-proxy | the catalog's web UI |

## Good to know

- **Stopping a unit keeps all its data** (`docker compose stop`): tables, files, notebooks and
  DAGs are there when you switch it on again.
- **A unit's first start builds its images**, so it takes several minutes once; later starts
  take a minute or two.
- **Starting and stopping carry on if you quit** the screen halfway. Their output is in
  `local/logs/make-status.log`.
- **`make up` starts every unit again.** Use `make status` to switch off what you don't need.
- **The home page greys the tiles** of tools whose unit is off.

Next, the first time: [check it works and fill the shared lake](general.md).
