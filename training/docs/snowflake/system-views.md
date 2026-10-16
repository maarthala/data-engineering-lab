# The system views: Snowflake's own management tables

Before you build anything, meet the tables **Snowflake keeps about itself**: who logged in,
which queries ran and how long they took, what each warehouse cost, which tables exist and how
big they are, who was granted what. Nobody creates them — every account has them. Admins,
data engineers and support teams read them every day, and the rest of this unit keeps coming
back to them.

## Concept

### Where they live
| Place | What it is | Delay | History | Covers |
|---|---|---|---|---|
| **`SNOWFLAKE.ACCOUNT_USAGE`** | views in the shared `SNOWFLAKE` database | 45 min – 3 h | **365 days** | the whole account, **including dropped objects** |
| **`<database>.INFORMATION_SCHEMA`** | views + table functions inside **every** database | **real time** | 7 days – 6 months (per function) | that one database, existing objects only |
| **`SHOW …` commands** | metadata commands, no warehouse needed | real time | now | the objects you're allowed to see |
| `SNOWFLAKE.ORGANIZATION_USAGE` | like `ACCOUNT_USAGE`, across all accounts of an organisation | ~2 h | 365 days | many accounts (not in a trial) |

Rule of thumb: **`INFORMATION_SCHEMA` / `SHOW` for "right now"**, **`ACCOUNT_USAGE` for
trends, audits and anything that was dropped**.

!!! note "Who can read them"
    `SNOWFLAKE.ACCOUNT_USAGE` is readable by `ACCOUNTADMIN` — the role you use in this unit. In a
    real account an admin gives it to other roles with
    `grant imported privileges on database SNOWFLAKE to role …` (or the narrower
    `SNOWFLAKE.*` database roles such as `OBJECT_VIEWER`, `USAGE_VIEWER`, `GOVERNANCE_VIEWER`).

!!! warning "Your setup queries show up later"
    You loaded ShopFlow minutes ago. Most `ACCOUNT_USAGE` views won't show it for up to an hour
    or more — that's the delay, not an error. Read this page now, run the queries again
    tomorrow.

### How they compare to the lab
In the lab you looked for the same answers in many places: Trino's query page, Spark's UI,
Airflow's task log, Polaris' grants, the Activity page in Manage. Snowflake puts all of it in
**SQL views** — so monitoring, auditing and cost reports are just queries.

## The views you'll meet every day
All in `SNOWFLAKE.ACCOUNT_USAGE` unless noted. Times are `TIMESTAMP_LTZ`; durations are in
**milliseconds**.

### `QUERY_HISTORY` — every query that ran
**Answers:** Which queries are slow? Who ran what? Why did a query fail? Did it use the cache?
Is the warehouse too small?

| Column | Contains |
|---|---|
| `QUERY_ID` | unique id — open it in **Query History** for the profile |
| `QUERY_TEXT` | the SQL |
| `USER_NAME`, `ROLE_NAME` | who ran it, as which role |
| `WAREHOUSE_NAME`, `WAREHOUSE_SIZE` | where it ran |
| `DATABASE_NAME`, `SCHEMA_NAME` | the session's context |
| `QUERY_TYPE` | `SELECT`, `INSERT`, `MERGE`, `CREATE_TABLE_AS_SELECT`, `COPY` … |
| `EXECUTION_STATUS` | `SUCCESS`, `FAIL`, `INCIDENT` |
| `ERROR_CODE`, `ERROR_MESSAGE` | why it failed |
| `START_TIME`, `END_TIME` | when |
| `TOTAL_ELAPSED_TIME` | total time (ms) |
| `COMPILATION_TIME`, `EXECUTION_TIME` | time planning vs running (ms) |
| `QUEUED_OVERLOAD_TIME` | waited because the warehouse was busy → scale **out** |
| `BYTES_SCANNED`, `ROWS_PRODUCED` | how much it read / returned |
| `PARTITIONS_SCANNED`, `PARTITIONS_TOTAL` | pruning — scanned ≈ total means no pruning |
| `PERCENTAGE_SCANNED_FROM_CACHE` | warehouse cache hit (0–1) |
| `BYTES_SPILLED_TO_LOCAL_STORAGE`, `…_REMOTE_STORAGE` | ran out of memory → scale **up** |
| `QUERY_TAG` | a label the client set (`alter session set query_tag = 'dbt'`) |

```sql
-- the 10 slowest queries of the last 7 days
select query_id, user_name, warehouse_name,
       total_elapsed_time / 1000 as seconds,
       left(query_text, 80)      as query
from snowflake.account_usage.query_history
where start_time >= dateadd(day, -7, current_timestamp())
order by total_elapsed_time desc
limit 10;

-- failed queries and why
select start_time, user_name, error_code, error_message, left(query_text, 80) as query
from snowflake.account_usage.query_history
where execution_status = 'FAIL'
order by start_time desc
limit 20;
```

**Real-time version:** `table(information_schema.query_history())` — the last 7 days, no delay.

### `WAREHOUSE_METERING_HISTORY` — credits per warehouse per hour
**Answers:** What did each warehouse cost? Which one is burning credits at night?

| Column | Contains |
|---|---|
| `START_TIME`, `END_TIME` | the hour |
| `WAREHOUSE_NAME` | which warehouse |
| `CREDITS_USED` | total credits that hour |
| `CREDITS_USED_COMPUTE`, `CREDITS_USED_CLOUD_SERVICES` | split by layer |

```sql
select warehouse_name, sum(credits_used) as credits
from snowflake.account_usage.warehouse_metering_history
where start_time >= dateadd(day, -30, current_timestamp())
group by warehouse_name
order by credits desc;
```

Related: **`WAREHOUSE_LOAD_HISTORY`** (`AVG_RUNNING`, `AVG_QUEUED_LOAD` — busy or queuing?)
and **`METERING_DAILY_HISTORY`** (`USAGE_DATE`, `SERVICE_TYPE`, `CREDITS_USED`,
`CREDITS_BILLED` — every service, not only warehouses).

### `LOGIN_HISTORY` — every login attempt
**Answers:** Who logged in, from where, with which tool? Are there failed logins (wrong
password, attacks)? Who still logs in with a password instead of SSO / key-pair?

| Column | Contains |
|---|---|
| `EVENT_TIMESTAMP` | when |
| `USER_NAME` | who |
| `CLIENT_IP` | from where |
| `REPORTED_CLIENT_TYPE` | `SNOWFLAKE_UI`, `PYTHON_DRIVER`, `JDBC_DRIVER` … |
| `FIRST_AUTHENTICATION_FACTOR` | `PASSWORD`, `RSA_KEYPAIR`, `SAML2_ASSERTION`, `OAUTH_ACCESS_TOKEN` |
| `IS_SUCCESS` | `YES` / `NO` |
| `ERROR_MESSAGE` | why it failed |

```sql
select event_timestamp, user_name, client_ip, reported_client_type, error_message
from snowflake.account_usage.login_history
where is_success = 'NO'
order by event_timestamp desc;
```

### `USERS` and `ROLES` — who exists
**Answers:** Which users are inactive? Who has no default role? Which roles were created or
dropped?

| `USERS` column | Contains |
|---|---|
| `NAME`, `LOGIN_NAME`, `EMAIL` | the user |
| `DEFAULT_ROLE`, `DEFAULT_WAREHOUSE` | what a session starts with |
| `DISABLED` | locked out by an admin |
| `LAST_SUCCESS_LOGIN` | last time they got in |
| `CREATED_ON`, `DELETED_ON` | lifecycle (`DELETED_ON` set = dropped) |

`ROLES` has `NAME`, `OWNER`, `CREATED_ON`, `DELETED_ON`.

```sql
-- users who haven't logged in for 30 days
select name, email, last_success_login
from snowflake.account_usage.users
where deleted_on is null
  and (last_success_login is null
       or last_success_login < dateadd(day, -30, current_timestamp()));
```

### `GRANTS_TO_ROLES` and `GRANTS_TO_USERS` — who may do what
**Answers:** Who can read this table? What can this role do? Who has `ACCOUNTADMIN`?

| `GRANTS_TO_ROLES` column | Contains |
|---|---|
| `PRIVILEGE` | `SELECT`, `USAGE`, `OWNERSHIP`, `INSERT` … |
| `GRANTED_ON` | the object type: `TABLE`, `SCHEMA`, `DATABASE`, `WAREHOUSE`, `ROLE` |
| `TABLE_CATALOG`, `TABLE_SCHEMA`, `NAME` | the object (database, schema, name) |
| `GRANTEE_NAME` | the role that received it |
| `GRANTED_BY` | the role that granted it |
| `CREATED_ON`, `DELETED_ON` | when granted / revoked |

`GRANTS_TO_USERS` lists **roles given to users**: `ROLE`, `GRANTEE_NAME` (the user),
`GRANTED_BY`, `CREATED_ON`, `DELETED_ON`.

```sql
-- who has ACCOUNTADMIN?
select grantee_name, granted_by, created_on
from snowflake.account_usage.grants_to_users
where role = 'ACCOUNTADMIN' and deleted_on is null;

-- every privilege on SHOPFLOW objects
select privilege, granted_on, table_catalog, table_schema, name, grantee_name
from snowflake.account_usage.grants_to_roles
where table_catalog = 'SHOPFLOW' and deleted_on is null
order by grantee_name, granted_on;
```

**Real-time version:** `show grants to role <role>;`, `show grants on table <table>;`.

### `ACCESS_HISTORY` — which data each query read and wrote *[Enterprise]*
**Answers:** Who read the `EMAIL` column last month? Which tables are never used (safe to drop)?
Which job wrote this table?

| Column | Contains |
|---|---|
| `QUERY_ID`, `QUERY_START_TIME`, `USER_NAME` | the query |
| `DIRECT_OBJECTS_ACCESSED` | JSON array: objects named in the query, with columns |
| `BASE_OBJECTS_ACCESSED` | JSON array: the underlying tables (through views) |
| `OBJECTS_MODIFIED` | JSON array: tables and columns written |

The arrays are `VARIANT` — read them with `lateral flatten`, which you'll learn in
[JSON (VARIANT), stages & COPY INTO](json-stages.md):

```sql
select distinct ah.user_name, obj.value:objectName::varchar as table_name
from snowflake.account_usage.access_history ah,
     lateral flatten(input => ah.base_objects_accessed) obj
where ah.query_start_time >= dateadd(day, -7, current_timestamp());
```

### `TABLES` and `COLUMNS` — every table and column
**Answers:** Which tables are big? Which are transient? What changed recently? Which tables
were dropped and when?

| `TABLES` column | Contains |
|---|---|
| `TABLE_CATALOG`, `TABLE_SCHEMA`, `TABLE_NAME` | the table's full name |
| `TABLE_TYPE` | `BASE TABLE`, `VIEW`, `MATERIALIZED VIEW`, `EXTERNAL TABLE` … |
| `IS_TRANSIENT` | `YES` / `NO` |
| `TABLE_OWNER` | the owning role |
| `ROW_COUNT`, `BYTES` | size |
| `CLUSTERING_KEY` | if one is set |
| `RETENTION_TIME` | Time Travel days |
| `CREATED`, `LAST_ALTERED` | created / last changed |
| `DELETED` | when it was dropped (null = exists) |

`COLUMNS` has `TABLE_CATALOG`, `TABLE_SCHEMA`, `TABLE_NAME`, `COLUMN_NAME`, `DATA_TYPE`,
`IS_NULLABLE`, `ORDINAL_POSITION` — find every column called `EMAIL` in the account in one
query.

**Real-time version:** `SHOPFLOW.INFORMATION_SCHEMA.TABLES` / `.COLUMNS` — you'll use these in
the next lesson.

### `TABLE_STORAGE_METRICS` — what a table's storage really costs
**Answers:** Why is storage high when the tables are small? (Old versions in Time Travel and
Fail-safe, and data kept for clones.)

| Column | Contains |
|---|---|
| `TABLE_CATALOG`, `TABLE_SCHEMA`, `TABLE_NAME` | the table |
| `ACTIVE_BYTES` | the current data |
| `TIME_TRAVEL_BYTES` | old versions kept for Time Travel |
| `FAILSAFE_BYTES` | old versions kept for Fail-safe |
| `RETAINED_FOR_CLONE_BYTES` | data a clone still points at |
| `DELETED`, `TABLE_DROPPED` | dropped yet still billed? |

Related: **`DATABASE_STORAGE_USAGE_HISTORY`** (per database per day) and **`STORAGE_USAGE`**
(the account total per day: `STORAGE_BYTES`, `STAGE_BYTES`, `FAILSAFE_BYTES`).

### `COPY_HISTORY` — every file loaded
**Answers:** Did last night's files load? How many rows? Which file failed and why?

| Column | Contains |
|---|---|
| `FILE_NAME`, `STAGE_LOCATION` | which file, from where |
| `TABLE_NAME` | loaded into |
| `LAST_LOAD_TIME` | when |
| `ROW_COUNT`, `ROW_PARSED` | rows loaded / read |
| `ERROR_COUNT`, `FIRST_ERROR_MESSAGE` | what went wrong |
| `STATUS` | `Loaded`, `Load failed`, `Partially loaded` |
| `PIPE_NAME` | set when Snowpipe loaded it |

**Real-time version:** `table(information_schema.copy_history(table_name => '…',
start_time => …))`.

### `TASK_HISTORY` — every scheduled task run
**Answers:** Did the pipeline run? Which runs failed? How long do they take?

| Column | Contains |
|---|---|
| `NAME`, `DATABASE_NAME`, `SCHEMA_NAME` | the task |
| `STATE` | `SUCCEEDED`, `FAILED`, `SKIPPED`, `CANCELLED` |
| `SCHEDULED_TIME`, `QUERY_START_TIME`, `COMPLETED_TIME` | when |
| `ERROR_CODE`, `ERROR_MESSAGE` | why it failed |
| `QUERY_ID` | the run's query — open it in Query History |

Related: **`DYNAMIC_TABLE_REFRESH_HISTORY`** (in `INFORMATION_SCHEMA`) for dynamic tables,
**`AUTOMATIC_CLUSTERING_HISTORY`** (`CREDITS_USED`, `NUM_BYTES_RECLUSTERED` per table) and
**`SERVERLESS_TASK_HISTORY`** for background services that cost credits.

### Cheat sheet — which view answers which question
| Question | View |
|---|---|
| Why is this query slow? | `QUERY_HISTORY` |
| What did we spend this month? | `WAREHOUSE_METERING_HISTORY`, `METERING_DAILY_HISTORY` |
| Is the warehouse queuing? | `WAREHOUSE_LOAD_HISTORY` |
| Who logged in, did anyone fail? | `LOGIN_HISTORY` |
| Who has access to X? | `GRANTS_TO_ROLES`, `GRANTS_TO_USERS` |
| Who actually read X? | `ACCESS_HISTORY` *[Enterprise]* |
| Which tables exist / were dropped? | `TABLES`, `COLUMNS` |
| Why is storage so high? | `TABLE_STORAGE_METRICS`, `STORAGE_USAGE` |
| Did the files load? | `COPY_HISTORY` |
| Did the scheduled job run? | `TASK_HISTORY` |

## Lab

```sql
use role accountadmin;
use warehouse PRACTICE_WH;
```

### 1 · Browse what's there

```sql
show views in schema snowflake.account_usage;
```

About 100 views. Scroll through the names — the ones above are the ones you'll use most.

### 2 · `SHOW` output is a table too
`SHOW` commands run without a warehouse and are always current. To filter or sort their result
with SQL, read it back with `result_scan`:

```sql
show warehouses;

select "name", "state", "size", "auto_suspend"
from table(result_scan(last_query_id()));
```

- **`result_scan(query_id)`** — the result of an earlier query, as a table.
- **`last_query_id()`** — the id of your previous statement.
- `SHOW` output columns are **lower case**, so quote them: `"name"`.

### 3 · Your own setup, in the real-time views
The setup file ran a minute ago — `INFORMATION_SCHEMA` sees it immediately:

```sql
select query_type, execution_status,
       total_elapsed_time / 1000 as seconds,
       left(query_text, 60) as query
from table(SHOPFLOW.information_schema.query_history(result_limit => 50))
where query_type like 'INSERT%'
order by start_time;
```

You'll see the 12 inserts of the setup file — `ORDER_ITEMS` and the derived tables take the
longest.

### 4 · The same in `ACCOUNT_USAGE` — tomorrow
Run step 3 against `snowflake.account_usage.query_history` (with a `start_time` filter instead of
`result_limit`). Right after setup it's empty; an hour or two later it's all there — and stays
for a year.

### 5 · Your logins

```sql
select event_timestamp, reported_client_type, first_authentication_factor, is_success
from snowflake.account_usage.login_history
order by event_timestamp desc
limit 10;
```

## Check yourself
1. A table was dropped last week. Where can you still see when, and how big it was?
   *(`ACCOUNT_USAGE.TABLES` — its `DELETED` column. `INFORMATION_SCHEMA` only shows existing
   tables.)*
2. You need the queries from the last 5 minutes. Which source? *(`INFORMATION_SCHEMA`'s
   `query_history()` table function — `ACCOUNT_USAGE` is up to 45 minutes behind.)*
3. Queries wait before they start. Which column shows it, and what's the fix?
   *(`QUEUED_OVERLOAD_TIME` in `QUERY_HISTORY` — scale out with more clusters.)*
4. Storage is twice the size of your tables. Where do you look?
   *(`TABLE_STORAGE_METRICS` — `TIME_TRAVEL_BYTES`, `FAILSAFE_BYTES`,
   `RETAINED_FOR_CLONE_BYTES`.)*
