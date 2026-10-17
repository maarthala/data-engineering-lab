# Clean up & checklist

## Clean up — stop spending credits
Run this when you finish (or before a long break). It stops everything that runs on its own and
drops the practice objects. `SHOPFLOW` itself stays — re-run the
[setup file](setup.md) any time to start fresh.

```sql
use role accountadmin;

-- things that run on their own
alter task if exists SHOPFLOW_DEV.PRACTICE.MERGE_ORDERS_TASK suspend;
alter dynamic table if exists SHOPFLOW_DEV.PRACTICE.CHANNEL_REVENUE_DT suspend;

-- practice objects
drop database if exists SHOPFLOW_DEV;          -- the clone + stream, task, dynamic table, policies
drop schema   if exists SHOPFLOW.PRACTICE;     -- BIG_ORDERS tables
drop warehouse if exists SCRATCH_WH;

-- the reader role (or keep it)
drop role if exists REPORTING_READER;

alter warehouse PRACTICE_WH suspend;
```

Keep `PRACTICE_MONITOR` while the trial runs. To remove **everything**:

```sql
drop database if exists SHOPFLOW;
drop warehouse if exists PRACTICE_WH;
drop resource monitor if exists PRACTICE_MONITOR;
```

!!! note "Dropped isn't gone at once"
    Dropped databases stay in Time Travel (1 day) — `undrop database SHOPFLOW_DEV` still works
    tomorrow. Storage for it ends after that.

## Checklist
Every SnowPro Core topic, practised on ShopFlow:

| Topic | Lesson |
|---|---|
| Trial account, Snowsight, account identifier, loading ShopFlow | [Your Snowflake trial](setup.md) |
| `ACCOUNT_USAGE` vs `INFORMATION_SCHEMA` vs `SHOW`, query / login / grant / storage / load / task history | [The system views](system-views.md) |
| Database → schema → objects, CTAS, views, `DROP` / `UNDROP`, `INFORMATION_SCHEMA`, timestamp types, join pitfalls | [Snowflake SQL basics](sql-basics.md) |
| Storage / compute / services, warehouse sizes, auto-suspend, scale up vs out, multi-cluster | [Architecture & warehouses](warehouses.md) |
| Micro-partitions, pruning, clustering keys, Query Profile, result / warehouse / metadata cache | [Micro-partitions & caching](performance.md) |
| Zero-copy cloning, Time Travel, Fail-safe, permanent / transient / temporary tables | [Cloning & Time Travel](clone-time-travel.md) |
| `VARIANT`, paths, `FLATTEN`, JSON files, `parse_json` | [JSON (VARIANT)](json.md) |
| stages (internal, external S3), uploads, what a private bucket needs, file formats, `COPY INTO`, load history | [Stages](stages.md) |
| Streams, tasks, SCD Type 2, dynamic tables | [Streams, tasks & dynamic tables](pipelines.md) |
| RBAC, system roles, future grants, masking and row access policies | [Roles & governance](security.md) |
| `ACCOUNT_USAGE`, query history, storage, resource monitors | [Cost & monitoring](cost.md) |
| Snowpark DataFrames, Snowflake Notebooks | [Snowpark](snowpark.md) |
| SQL and Python UDFs, table functions, stored procedures, `session.udf.register`, `usage` grants | [Functions & procedures](functions.md) |

**Next goal:** the **SnowPro Core** certification covers all of these. Snowflake's own
"Hands-on Essentials" badges and the free *Snowflake documentation → Guides* are good next
steps.
