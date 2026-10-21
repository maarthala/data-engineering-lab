# Full stack: make up

`make up` starts **every** service at once: SQL, Notebooks, Spark, Airflow, Superset and the
Polaris console on top of Core. It uses about **9 GB** before you run anything and 11–12 GB under
load, so give Docker **12–16 GB** and 6 CPUs. With less, use [`make status`](make-status.md) and
start only what you're learning.

```bash
make up
```

## What `make up` does

1. **Checks Docker's CPUs and memory** against the minimum (8 GB, 4 CPUs). Below it, it says
   what's missing and asks before starting anything. `LAB_SKIP_CHECKS=1 make up` skips the
   question.
2. **Checks that every port it needs is free.** If one is taken it starts nothing and says by
   what (another container, or a program and its pid) and how to free it or change it
   ([optional settings in `.env`](first-run.md#optional-settings-in-env)).
3. **Builds the images that are missing**: on the first run, and after `make destroy`. It warns
   when a Dockerfile, or a file it copies in, changed since its image was built.
4. **Starts every service.** After `make down` it picks up where you left off.
5. **Creates the lakehouse catalog** if it's missing (below).

While it runs you see a live view: the log on the left, every service's state on the right. At
the end it prints the links to open. `make up-plain` shows plain output instead.

!!! note "The first `make up` is slow"
    It builds every image from scratch, which takes **15–30 minutes** depending on your machine
    and network. Later runs don't build at all; they start in a minute or two.

!!! note "`make up` creates the `iceberg` lakehouse catalog when it's missing"
    Every `iceberg.…` query (Units {unit:unit2/intro.md}–{unit:unit6/polaris.md}) needs it. The
    catalog lives in Postgres's volume, so `make down` → `make up` keeps it; on the first start
    and after `make clean` / `make destroy` (which delete it), `make up` runs `make polaris-seed`
    for you and says so in its summary. If you still see **`Unable to find warehouse
    polaris_lake`** (e.g. you started with `make up-plain`), run `make polaris-seed` — it's
    safe to re-run.

## Stop it, restart it

| Command | What it does |
|---|---|
| `make down` | stops everything and removes the containers; your data stays. `make up` starts it again |
| `make restart` | `make down`, then `make up` |
| `make build` | rebuilds the images, then starts: after you changed a Dockerfile, or pulled changes that did (`make up` tells you) |

Running the full stack and want to free memory? `make status` works on a running stack too:
switch off the units you don't need with **x**.

Next, the first time: [check it works and fill the shared lake](general.md).
