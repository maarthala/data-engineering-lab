# First run: the mandatory make commands

Do these steps **once**, the first time you set up the lab on your laptop. Run them in a
terminal. After them you start the stack, the recommended way with
[`make status`](make-status.md) or everything at once with [`make up`](full-stack.md).

`make` runs a named recipe from the file `local/Makefile`: `make init`, for example, runs the
commands listed under `init:`. That's why every `make` command runs from the `local/` folder.

## What you need

**Docker Desktop** and **Git** (see
[Prerequisites → run the stack yourself](../prerequisites.md#3-running-the-stack-yourself-only-if-it-isnt-provided)).
Memory for Docker: **6–10 GB** if you run only what you're learning, **12–16 GB** for the full
stack.

## 1. Get the repo

```bash
git clone https://github.com/maarthala/data-engineering-lab.git
cd data-engineering-lab/local      # every command below runs from local/
```

## 2. Create your `.env`

```bash
cp .env.example .env
```

`.env` tells Docker Compose which services make up the stack and holds your local settings.
`.env.example` is the template that comes with the repo.

!!! warning "Don't skip `cp .env.example .env`"
    Without `.env`, `make up` stops at once with **`no configuration file provided: not
    found`**. The file is gitignored, so each clone needs its own copy.

### Optional settings in `.env`
| Setting | When you need it |
|---|---|
| `POSTGRES_HOST_PORT=5433` | something else on your machine already uses port **5432** |
| `AIRFLOW_HOST_PORT=8011` | something else already uses port **8001** (then open Airflow at `localhost:8011`) |
| `GIT_TOKEN=<token>` | you want Jupyter to auto-push saved notebooks to a git repo. Leave it empty and Jupyter works normally, without auto-push |

If a port is taken, `make up` stops before starting anything and lists each one — what needs
it, what holds it, and the fix (for Postgres and Airflow, the `.env` line to add). Free it or set
the port above, and run `make up` again.

## 3. Download the jars: `make init`

```bash
make init
```

It downloads the Spark and Iceberg jars (~2.5 GB, into `common/dockerfiles/tmp`) and prepares the
**AdventureWorks** sample database (it needs `ruby`; without it, that step is skipped with a
warning). It starts nothing. You run it once per clone; the downloads survive `make clean` and
`make destroy`.

## 4. Start the stack

Pick one:

| Command | What it starts | Docker memory | |
|---|---|---|---|
| `make status` | Core + only the units you switch on | 6–10 GB | **recommended**: [how to use it](make-status.md) |
| `make up` | every service at once | 12–16 GB | [full stack](full-stack.md) |

Then, the first time: [check it works and fill the shared lake](general.md).
