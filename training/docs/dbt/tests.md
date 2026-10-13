# Tests and seeds

## Concept
A model can build fine and still be **wrong**: duplicate orders, a missing customer, a channel
nobody expected. In Unit {unit:unit4/fundamentals.md} you'd spot that by looking. dbt lets you
**write the expectation down** next to the column, and checks it on every build.

A dbt **test** is a `SELECT` that returns the **bad rows**. **No rows = pass**; any row = fail.
You rarely write that SQL yourself. Four **generic tests** come with dbt, and you attach them to a
column in YAML:

| Test | Fails when |
|---|---|
| **`unique`** | a value appears more than once |
| **`not_null`** | a value is NULL |
| **`accepted_values`** | a value isn't in the list you give |
| **`relationships`** | a value has no match in another model (like a foreign key) |

A **seed** is a small CSV file in your project that dbt loads as a table: a lookup list, a
mapping, a calendar. Seeds are for small, hand-made data you'd otherwise type into SQL, not
for loading real data (that's Bronze's job).

## Lab
Continue in `~/work/dbt/shop` with the project from
[Models, ref() and materializations](models-ref.md).

### 1 · Describe what "correct" means: `schema.yml`
New file **`models/staging/schema.yml`**:

```yaml
version: 2

models:
  - name: stg_orders
    columns:
      - name: order_id
        data_tests:
          - unique
          - not_null
      - name: customer_id
        data_tests:
          - not_null
      - name: channel
        data_tests:
          - accepted_values:
              arguments:
                values: ['web', 'app', 'store', 'wholesale']
```

- **`models:`**: settings for models, by `name`. This file can sit next to the models it
  describes; dbt reads every `.yml` in `models/`.
- **`columns:`** → **`data_tests:`**: the tests for that column. `unique` and `not_null` need
  nothing else.
- **`accepted_values`** needs its list, under **`arguments:`** → **`values:`**. The list is
  the channels we *think* ShopFlow has.

### 2 · Run the tests: `dbt test`

```bash
dbt test
```
```text
15:59:37  Found 3 models, 4 data tests, 1 source, 516 macros
15:59:38  1 of 4 START test accepted_values_stg_orders_channel__web__app__store__wholesale  [RUN]
15:59:39  1 of 4 FAIL 1 accepted_values_stg_orders_channel__web__app__store__wholesale ... [FAIL 1 in 0.58s]
15:59:39  2 of 4 START test not_null_stg_orders_customer_id .............................. [RUN]
15:59:39  2 of 4 PASS not_null_stg_orders_customer_id .................................... [PASS in 0.25s]
15:59:39  3 of 4 START test not_null_stg_orders_order_id ................................. [RUN]
15:59:39  3 of 4 PASS not_null_stg_orders_order_id ....................................... [PASS in 0.21s]
15:59:39  4 of 4 START test unique_stg_orders_order_id ................................... [RUN]
15:59:40  4 of 4 PASS unique_stg_orders_order_id ......................................... [PASS in 0.53s]
15:59:40  Finished running 4 data tests in 0 hours 0 minutes and 2.75 seconds (2.75s).
15:59:40  Completed with 1 error, 0 partial successes, and 0 warnings:
15:59:40  [ERROR]: in test accepted_values_stg_orders_channel__web__app__store__wholesale (models/staging/schema.yml)
15:59:40    Got 1 result, configured to fail if != 0
15:59:40    compiled code at target/compiled/shop/models/staging/schema.yml/accepted_values_stg_orders_channel__web__app__store__wholesale.sql
15:59:40  Done. PASS=3 WARN=0 ERROR=1 SKIP=0 NO-OP=0 REUSED=0 TOTAL=4
```

**`dbt test`** runs every test. Three pass. `accepted_values` **fails** with *Got 1 result*:
its query returned one row, so one channel isn't in the list. Each test gets a long generated
name: test, model, column, arguments.

### 3 · Find out why it failed
The message points at the test's **compiled SQL**. Open it:

```sql
with all_values as (

    select
        channel as value_field,
        count(*) as n_records

    from silver.stg_orders
    group by channel

)

select *
from all_values
where value_field not in (
    'web','app','store','wholesale'
)
```

Quicker: **`dbt show`** runs a test's query and prints the bad rows:

```bash
dbt show -s accepted_values_stg_orders_channel__web__app__store__wholesale
```
```text
Previewing node 'accepted_values_stg_orders_channel__web__app__store__wholesale':
| value_field | n_records |
| ----------- | --------- |
| marketplace |       160 |
```

160 orders came through a **`marketplace`** channel we didn't know about, and there's no
`store`. The test did its job: the data didn't match our expectation. Here the *expectation*
was wrong, so fix the list in `schema.yml`:

```yaml
                values: ['web', 'app', 'marketplace', 'wholesale']
```

!!! tip "Fix the data or fix the test?"
    A failing test is a question, not a verdict. If the value is a real new case (a new sales
    channel), update the test. If it's bad data (a typo like `wbe`), fix it in staging, or
    upstream at the source.

### 4 · A lookup table from a CSV: seeds
Create a folder **`seeds/`** in the project (next to `models/`) and a file
**`seeds/channels.csv`**:

```text
channel,channel_group,is_online
web,direct,true
app,direct,true
marketplace,partner,true
wholesale,partner,false
```

```bash
dbt seed
```
```text
16:00:08  1 of 1 START seed file silver.channels ......................................... [RUN]
16:00:09  1 of 1 OK loaded seed file silver.channels ..................................... [INSERT 4 in 0.88s]
16:00:09  Done. PASS=1 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=1
```

```bash
dbt show -s channels
```
```text
| channel     | channel_group | is_online |
| ----------- | ------------- | --------- |
| web         | direct        |      True |
| app         | direct        |      True |
| marketplace | partner       |      True |
| wholesale   | partner       |     False |
```

- **`dbt seed`** loads every CSV in `seeds/` as a table named after the file:
  `silver.channels` (the profile's default schema, as no folder default applies to seeds).
- The first line is the **header** (column names). dbt guesses the types: `true`/`false`
  became a **boolean** column.
- A seed is a model like any other: other models and tests use it with **`ref('channels')`**.

### 5 · Every channel must exist in the lookup: `relationships`
Add a second test to `channel` in `schema.yml`:

```yaml
      - name: channel
        data_tests:
          - accepted_values:
              arguments:
                values: ['web', 'app', 'marketplace', 'wholesale']
          - relationships:
              arguments:
                to: ref('channels')
                field: channel
```

**`relationships`** checks that every `stg_orders.channel` exists in `channels.channel`, the
dbt version of a **foreign key** ([Dimensional modeling](../unit1/dimensional-modeling.md)).
A new channel that appears in the orders but not in the lookup now fails the build, before a
dashboard shows it without its group.

### 6 · Your own test: a singular test
Some rules don't fit a generic test. Write the "bad rows" query yourself, as a `.sql` file in
the project's **`tests/`** folder: **`tests/no_future_orders.sql`**:

```sql
-- fails if any order is dated in the future (returns the bad rows)
select order_id, order_date
from {{ ref('stg_orders') }}
where order_date > current_date()
```

A **singular test** is any `SELECT` in `tests/`. The same rule applies: rows = fail.
**`current_date()`** is today's date.

### 7 · Models and tests together: `dbt build`

```bash
dbt build
```
```text
16:00:27  Found 3 models, 1 seed, 6 data tests, 1 source, 516 macros
16:00:28  1 of 10 START sql view model silver.stg_orders ................................. [RUN]
16:00:29  1 of 10 OK created sql view model silver.stg_orders ............................ [OK in 0.20s]
16:00:29  2 of 10 START seed file silver.channels ........................................ [RUN]
16:00:29  2 of 10 OK loaded seed file silver.channels .................................... [INSERT 4 in 0.83s]
16:00:29  3 of 10 START test accepted_values_stg_orders_channel__web__app__marketplace__wholesale  [RUN]
16:00:30  3 of 10 PASS accepted_values_stg_orders_channel__web__app__marketplace__wholesale  [PASS in 0.50s]
16:00:30  4 of 10 START test no_future_orders ............................................ [RUN]
16:00:30  4 of 10 PASS no_future_orders .................................................. [PASS in 0.27s]
16:00:30  5 of 10 START test not_null_stg_orders_customer_id ............................. [RUN]
16:00:31  5 of 10 PASS not_null_stg_orders_customer_id ................................... [PASS in 0.26s]
16:00:31  6 of 10 START test not_null_stg_orders_order_id ................................ [RUN]
16:00:31  6 of 10 PASS not_null_stg_orders_order_id ...................................... [PASS in 0.29s]
16:00:31  7 of 10 START test unique_stg_orders_order_id .................................. [RUN]
16:00:31  7 of 10 PASS unique_stg_orders_order_id ........................................ [PASS in 0.68s]
16:00:31  8 of 10 START test relationships_stg_orders_channel__channel__ref_channels_ .... [RUN]
16:00:32  8 of 10 PASS relationships_stg_orders_channel__channel__ref_channels_ .......... [PASS in 0.55s]
16:00:32  9 of 10 START sql table model gold.daily_orders ................................ [RUN]
16:00:33  9 of 10 OK created sql table model gold.daily_orders ........................... [OK in 0.83s]
16:00:33  10 of 10 START sql table model gold.orders_by_channel .......................... [RUN]
16:00:34  10 of 10 OK created sql table model gold.orders_by_channel ..................... [OK in 0.89s]
16:00:34  Finished running 1 seed, 2 table models, 6 data tests, 1 view model in 0 hours 0 minutes and 6.77 seconds (6.77s).
16:00:34  Completed successfully
16:00:34  Done. PASS=10 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=10
```

**`dbt build`** = `seed` + `run` + `test`, **in DAG order**: `stg_orders` is built, its tests run,
and **only then** are the marts built from it. This is the command you'll use day to day, and
the one Airflow runs.

What happens when a test fails during a build? Remove `'marketplace'` from the list again and
run `dbt build`:

```text
16:00:53  3 of 10 FAIL 1 accepted_values_stg_orders_channel__web__app__wholesale ......... [FAIL 1 in 0.59s]
16:00:55  9 of 10 SKIP relation gold.daily_orders ........................................ [SKIP]
16:00:55  10 of 10 SKIP relation gold.orders_by_channel .................................. [SKIP]
16:00:55  [ERROR]: in test accepted_values_stg_orders_channel__web__app__wholesale (models/staging/schema.yml)
16:00:55    Got 1 result, configured to fail if != 0
16:00:55  Done. PASS=7 WARN=0 ERROR=1 SKIP=2 NO-OP=0 REUSED=0 TOTAL=10
```

The test on `stg_orders` failed, so dbt **skipped** both marts that depend on it: the Gold
tables keep yesterday's good data instead of being rebuilt from data that broke a rule. That's
the quality gate from [Data quality checks & quarantine](../recipes/data-quality.md), built in.

### 8 · Warn instead of fail: `severity`
Some rules should alert you without stopping the pipeline. Add a **`config:`** with
**`severity: warn`** to the test:

```yaml
          - accepted_values:
              arguments:
                values: ['web', 'app', 'wholesale']
              config:
                severity: warn
```
```text
16:01:07  3 of 10 WARN 1 accepted_values_stg_orders_channel__web__app__wholesale ......... [WARN 1 in 0.57s]
16:01:11  [WARNING]: in test accepted_values_stg_orders_channel__web__app__wholesale (models/staging/schema.yml)
16:01:11  [WARNING]: Got 1 result, configured to warn if != 0
16:01:11  Done. PASS=9 WARN=1 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=10
```

The test reports **WARN**, and the marts are built. Put `'marketplace'` back and remove the
`config:` lines before you continue.

## 🎯 This runs unchanged on Azure, Databricks, Snowflake & Fabric
Tests, seeds and `dbt build` are core dbt, the same on every platform. The platforms add their
own checks too (Databricks *expectations* in Declarative Pipelines, Snowflake *data metric
functions*), but dbt tests travel with your project.

## You can now…
- Attach **`unique`**, **`not_null`**, **`accepted_values`** and **`relationships`** tests to columns
- Run them with **`dbt test`**, read a failure, and see the bad rows with `dbt show -s <test>`
- Load a small lookup CSV with **`dbt seed`** and use it with `ref()`
- Write a **singular test** in `tests/`
- Use **`dbt build`**, and explain why a failed test **skips** the models after it
- Make a test **warn** instead of fail with `severity: warn`
