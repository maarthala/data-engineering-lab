# Jinja and macros

## Concept
You've used Jinja in small doses: `{{ ref() }}`, `{{ source() }}`, `{% if is_incremental() %}`.
Jinja is a **template language**: dbt runs it **first**, and the result is the SQL that Spark
runs. So Jinja can **write SQL for you**: repeat a column for every channel, fill in a setting,
reuse a snippet across models.

Three pieces of syntax cover almost everything:

| Syntax | What it does | Example |
|---|---|---|
| `{{ … }}` | **prints** a value into the SQL | `{{ ref('stg_orders') }}` → `silver.stg_orders` |
| `{% … %}` | a **statement**: `set`, `if`, `for`; prints nothing itself | `{% set n = 3 %}` |
| `{# … #}` | a **comment**, removed from the SQL | `{# why this filter #}` |

A **macro** is a Jinja function: you define it once in `macros/` and call it from any model.
A **package** is someone else's macros (and tests), installed into your project.

!!! tip "Always look at the compiled SQL"
    Jinja mistakes show up as strange SQL. `dbt compile -s <model>` prints what Jinja produced:
    check it whenever a model with Jinja behaves oddly.

## Lab
Continue in `~/work/my_files/dbt/shop`.

### 1 · A setting you can change per run: `var()`
In [Incremental models](incremental.md), the lookback window was a fixed `3`. Make it a
**variable**. In `models/marts/fct_orders.sql`, change the `where` line to:

```sql
where order_date >= (select date_sub(max(order_date), {{ var('lookback_days', 3) }}) from {{ this }})
```

**`var('lookback_days', 3)`** reads the variable `lookback_days`, or uses **3** if nobody set it.
Set it for one run with **`--vars`**:

```bash
dbt compile -s fct_orders --vars "{lookback_days: 7}"
```
```text
where order_date >= (select date_sub(max(order_date), 7) from gold.fct_orders)
```

(That's the line from the compiled SQL.) `--vars` takes YAML: `{name: value}`. To change the
default for the whole project, add it to `dbt_project.yml` instead:

```yaml
vars:
  lookback_days: 3
```

### 2 · Generate columns with a loop: `set` and `for`
One column per channel, orders per day: written by hand, that's the same line four times. New
file **`models/marts/daily_orders_by_channel.sql`**:

```sql
{% set channels = ['web', 'app', 'marketplace', 'wholesale'] %}

select
    order_date,
    {% for channel in channels %}
    sum(case when channel = '{{ channel }}' then 1 else 0 end) as {{ channel }}_orders,
    {% endfor %}
    count(*) as all_orders
from {{ ref('stg_orders') }}
group by order_date
```

- **`{% set channels = [ … ] %}`**: a Jinja variable holding a **list** (`[ ]`, items separated
  by commas), like a Python list ([Python & notebooks for DE](../unit3/python-essentials.md)).
- **`{% for channel in channels %} … {% endfor %}`**: repeats the lines in between once per item,
  with `channel` set to that item.
- **`'{{ channel }}'`** prints the item inside SQL quotes, so it becomes the string `'web'`;
  **`{{ channel }}_orders`** prints it into a column name, `web_orders`.

```bash
dbt compile -s daily_orders_by_channel
```
```text
Compiled node 'daily_orders_by_channel' is:


select
    order_date,

    sum(case when channel = 'web' then 1 else 0 end) as web_orders,

    sum(case when channel = 'app' then 1 else 0 end) as app_orders,

    sum(case when channel = 'marketplace' then 1 else 0 end) as marketplace_orders,

    sum(case when channel = 'wholesale' then 1 else 0 end) as wholesale_orders,

    count(*) as all_orders
from silver.stg_orders
group by order_date
```

The loop wrote four columns. (The empty lines are where the `{% %}` tags were; they don't matter
to SQL.) `count(*) as all_orders` comes after the loop, so every generated line can end with a
comma.

### 3 · Your own macro
`sum(case when … then 1 else 0 end)` now appears in two models. Make it a macro. New file
**`macros/count_if.sql`**:

```sql
{#- count_if(condition): how many rows match the condition -#}
{% macro count_if(condition) -%}
    sum(case when {{ condition }} then 1 else 0 end)
{%- endmacro %}
```

- **`{% macro name(arguments) %} … {% endmacro %}`** defines a macro. Whatever is between the
  tags is what it **returns**, with `{{ condition }}` replaced by the argument.
- The **`-`** in `{%-`, `-%}`, `{#-`, `-#}` removes the spaces and line breaks next to the tag,
  so the macro returns one tidy line.

Use it in the loop:

```sql
    {{ count_if("channel = '" ~ channel ~ "'") }} as {{ channel }}_orders,
```

**`~`** joins strings in Jinja: `"channel = '" ~ 'web' ~ "'"` is `channel = 'web'`. Compile again
and the SQL is **exactly the same** as in step 2: the macro only saves typing and keeps the
logic in one place. Use it in `daily_orders` too: `{{ count_if("status = 'cancelled'") }} as cancelled`.

### 4 · Other people's macros: the `dbt_utils` package
**dbt_utils** is the most used dbt package: dozens of tested macros. Install it with a
**`packages.yml`** file in the project folder (next to `dbt_project.yml`):

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: [">=1.3.0", "<2.0.0"]
```

`version` is a **range**: any 1.x from 1.3.0 on, never 2.0 (which may change how macros work).
Then:

```bash
dbt deps
```
```text
16:05:57  Updating lock file in file path: /home/jovyan/work/dbt/shop/package-lock.yml
16:05:57  Installing dbt-labs/dbt_utils
16:05:58  Installed from version 1.4.1
16:05:58  Up to date!
```

- **`dbt deps`** downloads the packages into **`dbt_packages/`**.
- **`package-lock.yml`** records the exact version installed (1.4.1), so the next `dbt deps`
  installs the same one. Keep it in your project.
- `dbt_packages/` isn't mirrored to your bucket (it's downloaded, not yours). After a **wipe**,
  run `dbt deps` again.

Now read the channel list **from the seed** instead of typing it. Replace the first line of
`daily_orders_by_channel.sql` with:

```sql
{% set channels = dbt_utils.get_column_values(ref('channels'), 'channel') %}
```

**`dbt_utils.get_column_values(model, column)`** runs a query **while dbt compiles** and returns
the column's distinct values as a list: here the four channels in `channels.csv`. A package's
macros are called as `package_name.macro_name`.

```bash
dbt build -s +daily_orders_by_channel
```
```text
16:06:11  1 of 9 OK created sql view model silver.stg_orders ............................. [OK in 0.22s]
16:06:12  2 of 9 OK loaded seed file silver.channels ..................................... [INSERT 4 in 0.87s]
16:06:12  3 of 9 PASS accepted_values_stg_orders_channel__web__app__marketplace__wholesale  [PASS in 0.55s]
16:06:12  4 of 9 PASS no_future_orders ................................................... [PASS in 0.25s]
16:06:13  5 of 9 PASS not_null_stg_orders_customer_id .................................... [PASS in 0.27s]
16:06:13  6 of 9 PASS not_null_stg_orders_order_id ....................................... [PASS in 0.28s]
16:06:14  7 of 9 PASS unique_stg_orders_order_id ......................................... [PASS in 0.55s]
16:06:14  8 of 9 PASS relationships_stg_orders_channel__channel__ref_channels_ ........... [PASS in 0.59s]
16:06:16  9 of 9 OK created sql table model gold.daily_orders_by_channel ................. [OK in 1.70s]
16:06:16  Done. PASS=9 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=9
```

`+daily_orders_by_channel` built its parents too, including the seed. `ref('channels')`
inside the macro counts as a dependency.

```bash
dbt show -s daily_orders_by_channel --limit 3
```
```text
| order_date | marketplace_orders | wholesale_orders | app_orders | web_orders | all_orders |
| ---------- | ------------------ | ---------------- | ---------- | ---------- | ---------- |
| 2023-07-15 |                  0 |                0 |          2 |          1 |          3 |
| 2025-02-16 |                  0 |                1 |          0 |          1 |          2 |
| 2023-11-08 |                  0 |                0 |          0 |          2 |          2 |
```

Add a channel to `channels.csv` and the next build adds its column, with no SQL change.
(`get_column_values` returns the values most frequent first; with one row per channel in the
seed, the order is arbitrary. Pass `order_by='channel'` for alphabetical.)

!!! warning "Keep Jinja small"
    Jinja makes SQL shorter, but harder to read. Use it to remove real repetition; if a
    colleague needs the compiled SQL to understand a model, simplify it.

## 🎯 This runs unchanged on Azure, Databricks, Snowflake & Fabric
Jinja, `var()`, macros and packages are core dbt. `dbt_utils` works on every major adapter, and
dbt's package hub (hub.getdbt.com) has adapter-specific packages too, such as
`dbt-labs/spark_utils`.

## You can now…
- Tell `{{ }}` (print), `{% %}` (statement) and `{# #}` (comment) apart
- Make a model configurable with **`var()`** and `--vars`, with a default
- Generate repeated SQL with **`{% set %}`** and **`{% for %}`**
- Write and call your own **macro**, with `-` to trim whitespace and `~` to join strings
- Install a **package** with `packages.yml` + `dbt deps`, and use `dbt_utils.get_column_values`
