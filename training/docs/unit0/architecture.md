# Technical architecture

You've met the **business** ([the ShopFlow scenario](../scenario.md)) and the questions
it needs answered. This page is the **machine that answers them** — the tools in the
stack and, more importantly, **how data moves through them** from raw source to a
dashboard someone actually trusts.

!!! note "You're already inside it"
    You're reading this page *because the stack is already running* — this
    documentation is served by the same stack. So there's nothing to "install"
    here. Your job is to understand the pieces and follow the data.

## The big picture — how data moves

```mermaid
flowchart TB
  subgraph SRC["① Sources — where data is born"]
    direction LR
    PG[(Postgres<br/>live app DB)]
    HIST[RustFS / S3<br/>history as Parquet]
  end

  ING[Spark<br/>ingest & transform]

  subgraph LAKE["② Lakehouse — RustFS object storage"]
    direction LR
    BR[bronze<br/>raw copy] --> SI[silver<br/>clean & joined] --> GO[gold<br/>business marts]
  end

  subgraph GOV["③ Governance — every table, every user"]
    direction LR
    POL[Apache Polaris<br/>Iceberg catalog + RBAC]
  end

  subgraph USE["④ Consume — people & tools ask questions"]
    direction LR
    TR[Trino<br/>SQL engine]
    SUP[Superset<br/>dashboards]
    JUP[Jupyter<br/>notebooks]
  end

  AF[Airflow<br/>schedules the whole pipeline]

  PG --> ING
  HIST --> ING
  ING --> BR
  AF -.runs.-> ING
  POL -.governs.-> LAKE
  GO --> TR
  TR --> SUP
  GO --> JUP
```

Read it top to bottom: raw data on the left of ①, refined step by step through ②,
policed the whole way by ③, and finally turned into answers in ④ — with **Airflow**
running the pipeline on a schedule so it stays current.

## Follow the data, step by step

1. **Sources (①).** Two kinds of raw data feed the platform: the **live Postgres
   database** (today's orders, customers, products) and **years of history** sitting
   in **RustFS** object storage as Parquet files.
2. **Ingest & transform with Spark.** **Spark** reads both sources and writes them
   into the lakehouse, refining the data in three passes (the Medallion pattern):
   **Bronze** (a raw copy), **Silver** (cleaned, typed, deduplicated, joined), and
   **Gold** (the aggregated business marts that answer ShopFlow's questions).
3. **Store in the lakehouse (②).** All three layers live as tables in **RustFS** — one
   cheap object store holding raw files *and* analytics-ready tables. That's the whole
   idea of a lakehouse.
4. **Govern everything (③).** **Apache Polaris** is the single Iceberg catalog of every
   table and who may touch it — each persona logs in with a Polaris client id/secret.
   It enforces that analysts read Gold while engineers write Silver — the *same*
   rules no matter which tool does the asking.
5. **Consume (④).** The Gold layer is where value is extracted: **Trino** runs SQL over
   it, **Superset** turns that into dashboards, and **Jupyter** lets you explore with
   Spark. Each user only sees what they're granted.
6. **Orchestrate.** **Airflow** ties it together — it runs ingestion and the
   Bronze→Silver→Gold jobs on a schedule, so the answers reflect *today*, automatically.

## The components (what each tool is, and where to reach it)

| # | Tool | Its job in the flow | Local URL | Login |
|---|---|---|---|---|
| ① | **Postgres** | The live application database — a data *source* | (internal) | — |
| ① / ② | **RustFS** | S3-compatible object storage: raw history *and* the lakehouse tables (in-stack endpoint `http://storage:9000`) | http://localhost:9001/rustfs/console/ | your lab account (you see your own `<username>-lake` bucket) |
| ② | **Spark** | Ingests sources and builds Bronze→Silver→Gold | http://127.0.0.1:8002 | — |
| ②/③ | **Apache Polaris** | Iceberg REST catalog `iceberg` (warehouse `polaris_lake`) + per-user access (RBAC) | http://localhost:8189 | your lab account · admin `root` / `s3cr3t` at `/login?local=1` |
| ④ | **Trino** | Distributed SQL engine over the lakehouse | via SQLPad / Superset (laptop UI: http://localhost:8007/ui/) | your lab account (passed on by SQLPad / Superset) |
| ④ | **Superset** | BI dashboards on the Gold layer | http://localhost:8004 | your lab account |
| ④ | **Jupyter** | Notebooks (Spark) for exploration & labs | http://localhost:8008 | your lab account |
| — | **Airflow** | Orchestrates the whole pipeline on a schedule | http://localhost:8001 | your lab account |

*(On Kubernetes the URLs are `http://<tool>.de.lan` instead of `localhost` — the roles
are identical.)*

## Why it's built this way

Notice the shape: **cheap storage in the middle, many engines around it, one catalog
governing all of them.** That separation — storage decoupled from compute, governed
by a single catalog — is exactly how the big cloud platforms are built. Learning it
here means you already understand the architecture you'll meet on Databricks, Fabric,
and Snowflake later; only the logos change.

## You can now…
- Name every component in the stack and say what it does
- **Trace a single record** from a Postgres row all the way to a Superset chart
- Explain *where* data is stored, *what* moves it, and *who* is allowed to see it
