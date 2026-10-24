# Looker, LookML and the semantic layer (for conversational BI)

> **Why this page matters.** In a typical EDW modernization on GCP, a key deliverable is a near real-time sales model in BigQuery, made available in Looker, with a **conversational BI** solution on top. You don't need to be a LookML expert to work on such a platform. You do need to understand how Looker works, how to expose a model correctly, and why the semantic layer decides whether AI answers are right or wrong.
>
> **Quick reference:** the cheat sheet is the [Looker and semantic layer deep dive](../deep-dives/looker-semantic-layer.md). The data model underneath is in [Data warehouse modelling](dwh-modelling.md).

---

## 1. What it is and why it exists

### What a semantic layer is

A **semantic layer** is a translation layer between database tables and business people. It says, in one central place:

- "**Net sales** means `SUM(net_amount)` where the line is not cancelled, excluding VAT, with returns as negative lines."
- "**Store** joins to **sales** on `store_sk`, many sales to one store."
- "**Region** is a field users can group by. `_loaded_at` is hidden."
- "Store managers see only their own region."

Without a semantic layer, each dashboard, each analyst's SQL file and each Excel export redefines "net sales". After a year you have five slightly different numbers and long meetings about which one is right.

### Why it exists

| Problem without it | What the semantic layer gives |
|---|---|
| Metrics defined differently in many places | **Consistency**: one definition, reused everywhere |
| Users must know join logic and table names | **Usability**: business names, pre-defined joins |
| No control over who sees what | **Governance**: row and field access rules in one place |
| Changes require editing every report | **Reuse and maintainability**: change once, all reports follow |
| AI tools guess SQL | **A safe vocabulary for AI**: the model picks known fields |

### An analogy

Think of a restaurant. The kitchen (BigQuery) has raw ingredients and equipment. Customers don't walk into the kitchen and cook. They read a **menu** (the semantic layer) with clear names and descriptions. When they order "Salmon soup", the kitchen follows a fixed **recipe** (LookML) so it tastes the same every time. A new waiter (an AI assistant) also works from the menu. They don't invent dishes from what's in the fridge.

### What Looker is

**Looker** is Google Cloud's enterprise BI platform built around a semantic modelling language called **LookML**. Its key property: **Looker does not store your data.** It generates SQL from LookML, sends it to the database (BigQuery), and shows the results. It's "in-database" BI.

---

## 2. Where it fits in a modern GCP data platform

| Platform goal | What you need from this page |
|---|---|
| "Near real-time sales data model on BigQuery, made available in Looker" | Views, explores, joins, measures, caching tied to data freshness |
| "Supports a conversational BI solution" | How LLMs use the semantic layer; how to make it AI-ready and test it |
| "Optimize BigQuery queries for runtime and cost" | Looker-generated queries are BigQuery cost; partition filters, aggregates, BI Engine |
| "Testing, documentation, handover" | LookML in git, validators, data tests, descriptions as documentation |
| Looker/LookML basics | Enough vocabulary to discuss design with the BI team |

If you know Power BI semantic models (for example on Fabric), Looker solves the same problem with a code-first approach in git. The concepts transfer directly: star schema, relationships, measures defined once, row-level security.

---

## 3. Architecture: how Looker works under the hood

### The big picture

```text
                    ┌──────────────────────── Looker instance ─────────────────────────┐
                    │                                                                  │
  Users ── browser ─►  Explore UI / Dashboards / Looks / API / Embedded / AI features  │
                    │                │                                                 │
                    │                ▼                                                 │
                    │   LookML project (from git)  ──►  SQL generator (BigQuery dialect)│
                    │   models, explores, views                │                      │
                    │                                          ▼                      │
                    │                                   Results cache  ◄── datagroups │
                    │                                          │ (miss)               │
                    │   PDT regenerator  ─────────┐            │                      │
                    └─────────────────────────────┼────────────┼──────────────────────┘
                                                  │            │ connection
                                                  ▼            ▼ (service account or per-user OAuth)
                                          ┌──────────────────────────────────┐
                                          │ BigQuery                         │
                                          │   mart_sales.* (your model)      │
                                          │   looker_scratch.* (PDTs)        │
                                          └──────────────────────────────────┘
```

### The parts

**Instance.** A Looker deployment (today usually "Looker (Google Cloud core)", hosted on Google Cloud). It has users, groups, roles, folders, schedules.

**Connection.** Configuration for reaching BigQuery: the billing project, the credentials (a **service account**, or **per-user OAuth** so each user queries with their own identity), and a **scratch dataset** where Looker may write persistent derived tables. It also has settings like the maximum number of concurrent queries and the PDT/datagroup check schedule.

**LookML project.** A **git repository** of LookML files. It contains:

- **Model files** (`.model.lkml`): which connection to use, which views to include, which explores to expose, datagroups.
- **View files** (`.view.lkml`): one per table or derived table, with dimensions and measures.
- Optionally LookML dashboards, manifest file, data tests.

**The hierarchy:**

```text
Project (git repo)
 └── Model  "sales"            connection: bq_sales
      ├── Explore "sales"       ◄── what users and AI can query; a starting view + joins
      │     ├── view fct_sales_line   (base)
      │     ├── join dim_store        many_to_one
      │     ├── join dim_product      many_to_one
      │     └── join dim_date         many_to_one
      └── Explore "returns"
            └── ...
 View = table (or derived table) → dimensions (group/filter by) + measures (aggregations)
```

### Query generation, step by step

1. A user (or an AI agent through the API) opens the **sales explore** and selects: `dim_store.region`, `fct_sales_line.order_week`, `fct_sales_line.net_sales`, filter `order_date: last 4 weeks`.
2. Looker reads the LookML and finds which views those fields belong to. It **only joins the views needed** for the selected fields (here: `dim_store`, not `dim_product`).
3. It writes SQL in the **BigQuery dialect**: `SELECT region, DATE_TRUNC(...), SUM(...) FROM fct LEFT JOIN dim_store ... WHERE order_date >= ... GROUP BY 1, 2`.
4. It checks the **results cache**. If the same SQL ran recently and the cache is still valid per the datagroup, it returns cached results. No BigQuery cost.
5. Otherwise it sends the query to BigQuery through the connection. BigQuery does all the computing.
6. Results come back, are cached, and are shown. Some calculations (table calculations, totals, pivots) can run in Looker after the SQL.

Because Looker generates SQL each time, **your BigQuery table design is your Looker performance**. Partitioning, clustering and aggregates in the mart matter directly.

### Caching layer

Looker caches query results (encrypted) on the instance. A cached result is reused when the same SQL is requested and the cache policy says it's still valid. The policy comes from a **datagroup** (section 4.9). Without an explicit policy there is a default cache age (historically one hour; check current docs).

### PDT scratch schema

When you define a **persistent derived table**, Looker runs `CREATE TABLE ... AS SELECT` in the connection's **scratch dataset** (e.g. `looker_scratch`) with a generated table name, and swaps to the new version when it's rebuilt. The Looker service account therefore needs write access to that one dataset, and read-only access to the marts.

---

## 4. Core concepts, one by one

### 4.1 Views and dimensions

**What.** A view maps to one table or derived table. **Dimensions** are the columns or expressions users group or filter by.

```text
view: dim_store {
  sql_table_name: `prj-dwh-sales-prod.mart_sales.dim_store` ;;

  dimension: store_sk {
    primary_key: yes
    hidden: yes
    type: number
    sql: ${TABLE}.store_sk ;;
  }

  dimension: store_name {
    label: "Store Name"
    description: "Name of the physical store or 'Web shop' for online. As of the time of sale."
    type: string
    sql: ${TABLE}.store_name ;;
  }

  dimension: region {
    description: "Sales region of the store at the time of sale (SCD2). Use 'Region (Current)' for today's structure."
    type: string
    sql: ${TABLE}.region ;;
  }

  dimension: is_large_format {
    type: yesno
    sql: ${TABLE}.size_m2 >= 2000 ;;
  }
}
```

**How it works.** `${TABLE}` is replaced by the table alias in the generated SQL. `${field_name}` references another field, so logic is defined once and reused. Common dimension types: `string`, `number`, `yesno`, `date`, `tier` (buckets), `location`.

**Why it matters.** Dimension names, labels and descriptions are what users and AI see. They are the "menu".

**Common mistake.** Exposing every raw column with its technical name (`str_rgn_cd`). Hide technical fields; label the rest in business language.

### 4.2 Dimension groups

**What.** One definition that creates many time fields.

```text
dimension_group: order {
  type: time
  timeframes: [raw, time, hour, date, week, month, quarter, year, day_of_week, hour_of_day]
  sql: ${TABLE}.order_ts ;;
  convert_tz: no        # the column is already in the business timezone, or set the model timezone
}
```
This creates `order_date`, `order_week`, `order_month`, and so on. Another type, `duration`, creates intervals between two timestamps (e.g. days from order to delivery).

**Why it matters.** Time is in almost every business question. Getting the timezone and week definition right (ISO weeks in Finland; `week_start_day` can be set in the model) prevents subtle errors. For fiscal calendars, join a proper `dim_date` view rather than relying only on generated timeframes.

**Common mistake.** Ignoring time zones. A sale at 00:30 Helsinki time is 22:30 the previous day in UTC. Decide once whether `order_date` is the local business date, and document it.

### 4.3 Measures

**What.** Aggregations, computed in SQL at query time.

| Type | What it does | Example |
|---|---|---|
| `count` | Counts rows (distinct primary keys when joins could fan out) | Number of order lines |
| `count_distinct` | `COUNT(DISTINCT ...)` | Number of orders, customers |
| `sum` | `SUM` (fan-out safe with a primary key) | Net sales |
| `average` | `AVG` (fan-out safe) | Average line value |
| `min`, `max`, `median`, `percentile` | As named | Largest basket |
| `sum_distinct`, `average_distinct` | Sum/average with an explicit `sql_distinct_key` | Header values on a non-PK grain |
| `number` | A calculation from other measures | Margin %, AOV |
| `list` | Lists distinct values | Product names in an order |
| `running_total`, `percent_of_total` | Computed by Looker after the SQL | Cumulative sales |

```text
measure: net_sales {
  label: "Net Sales (EUR)"
  description: "Sales after discounts, excluding VAT, in EUR. Returns are negative. Cancelled lines excluded. Also called revenue or turnover."
  type: sum
  sql: ${TABLE}.net_amount_eur ;;
  filters: [is_cancelled: "no"]
  value_format_name: eur
}

measure: orders {
  description: "Number of distinct orders (baskets)."
  type: count_distinct
  sql: ${TABLE}.order_id ;;
}

measure: average_order_value {
  label: "Average Order Value (AOV)"
  description: "Net Sales divided by number of orders."
  type: number
  sql: SAFE_DIVIDE(${net_sales}, NULLIF(${orders}, 0)) ;;
  value_format_name: eur
}
```

**Why it matters.** Ratios as `type: number` of other measures guarantee "ratio of sums", never "sum of ratios". Filtered measures (`filters:`) encode business rules once.

**Common mistake.** Writing `type: average` on a precomputed percentage column. Averages of ratios are wrong (see [Data warehouse modelling](dwh-modelling.md), §4.4).

### 4.4 Primary keys

Every view needs exactly one dimension with `primary_key: yes`, and it must be **truly unique** at the view's grain. For a composite grain, build one (`CONCAT(order_id, '-', CAST(line_no AS STRING))`) or use the hash key from the mart. Looker uses it for **symmetric aggregates** (4.7) and for `count`. A wrong primary key (not unique) produces wrong numbers silently. That's why grain tests in the warehouse matter for Looker.

### 4.5 Explores

**What.** An explore is a starting view plus the joins users can use. It's the unit users (and AI) query.

```text
explore: sales {
  label: "Sales"
  description: "Retail sales lines from stores and the web shop, near real-time (about 15 min delay). Use for revenue, margin, units, orders and basket questions."
  from: fct_sales_line
  view_name: sales
  always_filter: { filters: [sales.order_date: "28 days"] }

  join: dim_store {
    type: left_outer
    relationship: many_to_one
    sql_on: ${sales.store_sk} = ${dim_store.store_sk} ;;
  }
  join: dim_product {
    relationship: many_to_one
    sql_on: ${sales.product_sk} = ${dim_product.product_sk} ;;
  }
  join: order_date_dim {
    from: dim_date                       # role-playing dimension
    relationship: many_to_one
    sql_on: ${sales.order_date} = ${order_date_dim.calendar_date} ;;
  }
}
```

**Why it matters.** Explore design is where you control what is possible. Small, focused explores (sales, returns, inventory) are easier for users and for AI than one giant explore with 40 joins.

### 4.6 Joins: `relationship` and `sql_on`

- `sql_on` is the join condition, written with `${}` references.
- `type` is the join type: `left_outer` (default), `inner`, `full_outer`, `cross`.
- `relationship` tells Looker the **cardinality**: `many_to_one`, `one_to_one`, `one_to_many`, `many_to_many`. It does not change the SQL join itself. It tells Looker **whether rows can be duplicated**, and so whether it must use symmetric aggregates.

In a star schema, joins from the fact to dimensions are `many_to_one`. If you declare the relationship wrongly, Looker either does unnecessary work or, worse, double counts.

### 4.7 Fan-out and symmetric aggregates (with numbers)

**The problem.** Suppose an `orders` view (one row per order, with `shipping_fee`) is joined to `order_items` (one row per line).

```text
orders                     order_items
order_id | shipping_fee    item_id | order_id | price
O1       | 10              1       | O1       | 20
O2       | 5               2       | O1       | 30
                           3       | O1       | 15
                           4       | O2       | 40
```

After the join there are 4 rows. `shipping_fee` repeats on every O1 line:

```text
order_id | shipping_fee | price
O1       | 10           | 20
O1       | 10           | 30
O1       | 10           | 15
O2       | 5            | 40
```

- Naive `SUM(shipping_fee)` = 10 + 10 + 10 + 5 = **35**. Correct answer: **15**. That's **fan-out**.
- Naive `COUNT(*)` of orders = 4. Correct: 2.
- `SUM(price)` = 105, which is correct, because price lives at the joined (finer) grain.

**What Looker does.** Because `orders` has a primary key and the explore declares `orders` → `order_items` as `one_to_many`, Looker knows `orders` fields may be duplicated. For measures on `orders` it generates **symmetric aggregate** SQL. The idea (simplified):

```sql
-- Conceptual form. Looker's real SQL uses a large hash of the primary key.
SUM(DISTINCT hash(order_id) + shipping_fee) - SUM(DISTINCT hash(order_id))
-- Each order contributes its (hash + fee) only once, even if repeated.
-- Subtracting the sum of distinct hashes leaves 10 + 5 = 15.
```
For counts it uses `COUNT(DISTINCT primary_key)`. Looker only does this when needed (when a join could fan out the measure's view), because the SQL is heavier.

**Lessons:**

1. Define a correct `primary_key` on every view.
2. Declare the correct `relationship` on every join.
3. Prefer a fact at one clean grain with `many_to_one` joins to dimensions. Then fan-out never happens and SQL stays simple. (Allocate the shipping fee to lines in the mart, see [Data warehouse modelling](dwh-modelling.md) §4.2.)

### 4.8 Derived tables

| Type | Defined by | Stored? | Use |
|---|---|---|---|
| **Ephemeral (SQL) derived table** | `derived_table: { sql: ... }` | No, inlined as a CTE/subquery per query | Light reshaping |
| **Native derived table (NDT)** | `derived_table: { explore_source: ... }` using LookML fields | Optional | Reuse existing measures (e.g. customer lifetime value from the sales explore) |
| **Persistent derived table (PDT)** | A derived table with `datagroup_trigger` (or `persist_for`, `sql_trigger_value`) | Yes, in the scratch dataset | Expensive logic needed often |
| **Incremental PDT** | PDT with `increment_key` (+ `increment_offset` for late data) | Yes, appends/rebuilds recent increments | Large PDTs that grow over time |

```text
view: customer_first_order {
  derived_table: {
    explore_source: sales {
      column: customer_id { field: sales.customer_id }
      column: first_order_date { field: sales.min_order_date }
    }
    datagroup_trigger: sales_freshness
  }
  dimension: customer_id { primary_key: yes }
  dimension: first_order_date { type: date }
}
```

**Design recommendation.** Put core business transformations in **Dataform/dbt** in the warehouse: tested, version-controlled, reusable by tools other than Looker (including other AI tools). Use PDTs only for BI-specific needs. Otherwise logic is split between two places and hard to hand over.

### 4.9 Datagroups and caching

A **datagroup** is a named caching and rebuild policy.

```text
datagroup: sales_freshness {
  label: "Sales data freshness"
  sql_trigger: SELECT MAX(_loaded_at) FROM `prj-dwh-sales-prod.mart_sales.fct_sales_line` ;;
  max_cache_age: "30 minutes"
}

explore: sales {
  persist_with: sales_freshness
}
```

How it works:

- Looker runs the `sql_trigger` query on the connection's check schedule (configured on the connection; often every few minutes). If the result **changes**, the datagroup is "triggered": cached results for explores using it become invalid, and PDTs using `datagroup_trigger: sales_freshness` rebuild.
- `max_cache_age` is a safety net: cached results are never older than this, even if the trigger didn't fire.
- `interval_trigger` is an alternative for time-based triggers.
- Datagroups can also trigger schedules (send a dashboard when new data arrives).

**Why it matters for near real-time.** Tie the cache to **data arrival**, not a blind timer. The trigger query should be cheap: `MAX(_loaded_at)` with a partition filter, or read a tiny control table that the pipeline updates at the end of each successful run. The second is better: then the cache refreshes only when **tested** data is published.

**Common mistake.** A trigger query that scans the whole fact every 5 minutes, all day. Make it cheap.

### 4.10 Aggregate awareness

**What.** You define `aggregate_table` blocks in an explore. Looker builds them (as PDTs) and, when a user's query can be answered from a smaller aggregate, **rewrites the query to use it automatically**.

```text
explore: sales {
  aggregate_table: sales_daily_store_category {
    query: {
      dimensions: [sales.order_date, dim_store.region, dim_store.store_name, dim_product.category]
      measures: [sales.net_sales, sales.units, sales.gross_margin]
    }
    materialization: {
      datagroup_trigger: sales_freshness
    }
  }
}
```

- The user still selects `Net Sales` from the same explore. No new fields to learn. The AI also keeps using the same fields.
- It works best with additive measures (`sum`, `count`, `min`, `max`). Distinct counts and averages have restrictions (they can't simply be re-aggregated). Check the docs for exact rules.
- Combine with incremental PDTs for large aggregates.

### 4.11 `always_filter`, `conditionally_filter`, `sql_always_where`

| Parameter | Effect | Use |
|---|---|---|
| `always_filter` | Adds a filter the user **can change but not remove** | Force a date filter on the partition column |
| `conditionally_filter` | Requires a filter unless the user filters on one of the listed alternative fields | "Filter by date, unless you filter by order ID" |
| `sql_always_where` | Adds a hidden `WHERE` condition the user can't see or change | Exclude test stores, cancelled lines |
| `sql_always_having` | Hidden `HAVING` | Rare |

With `require_partition_filter = TRUE` on the BigQuery table, a query without a partition filter fails. `always_filter` on the partition column makes Looker always add one. Together they protect cost.

### 4.12 Access control

**Content and feature access:** roles = permission sets (what users can do) + model sets (which models they can see). Folders control dashboard access.

**Row-level security with user attributes and `access_filter`:**

```text
explore: sales {
  access_filter: {
    field: dim_store.region
    user_attribute: allowed_region      # set per user or group, e.g. 'South'
  }
}
```
Looker adds `WHERE region = 'South'` to every query from that user.

**Field-level security with `access_grant`:**

```text
access_grant: can_see_cost {
  user_attribute: department
  allowed_values: ["finance", "category_management"]
}

measure: gross_margin {
  required_access_grants: [can_see_cost]
  type: sum
  sql: ${TABLE}.net_amount_eur - ${TABLE}.cost_amount_eur ;;
}
```

**Looker security vs. BigQuery security.** With a single **service account** connection, BigQuery sees one identity, so rules must live in Looker. With **per-user OAuth**, BigQuery row access policies and column policy tags apply to each user's queries as well. OAuth has trade-offs (for example around PDTs and scheduled content; check current docs). Many teams use a service account plus Looker access filters, and keep sensitive columns out of the Looker-visible dataset entirely.

For conversational BI, the AI must run **as the user**, through the same explores and access rules. Otherwise it can leak data a user isn't allowed to see.

### 4.13 Refinements and extends (brief)

- **Extends** (`extends: [base_view]`): create a new object that inherits from another, like class inheritance. Good for a base view reused by several explores.
- **Refinements** (`view: +fct_sales_line { ... }`): modify an existing object in place, in a separate file, without copying it. Good for layering: generated base views from the table schema, then a refinement file that adds business labels, descriptions and measures.

### 4.14 Development workflow: dev mode → git → validation → production

```text
 Developer turns on Dev Mode ──► personal git branch of the LookML project
      │ edit LookML in the IDE (or locally, pushed to git)
      ▼
 LookML validator (syntax, references)  ──►  run explores in dev mode, check SQL
      ▼
 LookML data tests (assertions on query results)
      ▼
 Commit ──► pull request ──► review (and CI, e.g. Spectacles-style checks)
      ▼
 Merge / deploy to production ──► production users see the change
      ▼
 Content validator: finds dashboards and Looks that reference renamed or deleted fields
```

- **Dev mode** lets you see your changes without affecting anyone.
- The **content validator** matters because renaming a field breaks saved content. Prefer adding labels to renaming fields; validate content before deploying.
- **LookML data tests** (`test:` blocks) run a query and assert a result, e.g. "net sales for a known closed day equals the reconciled figure". Great for metric definitions.

---

## 5. Performance of Looker on BigQuery

Every Looker tile is a BigQuery query. Performance and cost are mostly warehouse design.

| Problem | Fix | Why it works |
|---|---|---|
| Queries scan years of data | Partitioned fact + `require_partition_filter` + `always_filter` on the partition date | Partition pruning cuts bytes scanned |
| Filters on store/product slow | Cluster the fact by `store_id, product_id` | Block pruning inside partitions |
| Same heavy aggregations repeated | Aggregate awareness, a Dataform aggregate table, or BigQuery materialized views | Read thousands of rows, not millions |
| Many users, dashboards must feel instant | **BI Engine** reservation (in-memory acceleration in BigQuery) | Sub-second for suitable queries |
| BI competes with ETL for slots | Separate **reservations** (BigQuery editions) for BI and for pipelines, assigned per project | Pipelines can't starve dashboards, and the reverse |
| Cache misses after every load | Datagroup triggered by a publish event, not every few minutes | Cache stays valid until new tested data |
| 25 tiles = 25 queries | Fewer tiles, merged queries, cross-filtering | Less concurrency pressure |
| Expensive distinct counts | Approximate counts where acceptable; HLL sketches in aggregates | Cheaper than exact `COUNT(DISTINCT)` |
| Fan-out symmetric aggregate SQL | Clean grain, `many_to_one` joins only | Simpler, faster SQL |

How to **measure**: BigQuery `INFORMATION_SCHEMA.JOBS` shows Looker queries (filter by the Looker service account or job labels, if configured), with bytes and slot time. Looker's own System Activity shows slow explores and dashboards. Find the top 10 expensive queries first; then fix.

---

## 6. Conversational BI on a semantic layer

### How it works

"Conversational BI" means users ask questions in natural language ("What were net sales in Helsinki stores last week compared to the same week last year?") and get an answer, often a chart and a short explanation.

There are two broad designs:

```text
 A) LLM writes raw SQL                       B) LLM uses the semantic layer
 ─────────────────────                       ──────────────────────────────
 question                                    question
   │                                           │
   ▼                                           ▼
 LLM reads table schemas,                    LLM reads explores, fields,
 guesses joins and formulas                  labels and descriptions
   │                                           │ chooses: explore = sales
   ▼                                           │ fields = dim_store.city, sales.net_sales,
 raw SQL ──► BigQuery                          │          order_date_dim.week
   │                                           │ filters = city: Helsinki, date: last week
   ▼                                           ▼
 answer (maybe wrong join,                   Looker generates the SQL (governed joins,
 different "net sales")                      symmetric aggregates, access filters) ──► BigQuery
                                               ▼
                                             answer consistent with dashboards
```

In design B, the LLM's job is smaller and safer: **map words to existing fields and filters**. It doesn't invent business logic. The semantic layer adds the correct joins, the official metric formula, access rules and partition filters. This is why a semantic layer matters so much for AI.

**Google's offering.** Google provides **Gemini in Looker**, including **conversational analytics** features that let users ask questions about Looker explores in natural language, and APIs for building agents on top. The exact features, names and availability change quickly, so check the current docs. The principle is stable: the AI works best through the LookML semantic layer. You could also build a custom agent that calls the Looker API to run queries on explores.

### Why descriptions and labels matter

The LLM can only choose fields by their **names, labels and descriptions**. Compare:

| Bad | Good |
|---|---|
| `amt_n` (no description) | `Net Sales (EUR)`: "Sales after discounts, excluding VAT, in EUR. Returns negative. Also called revenue or turnover." |
| `rgn` | `Region (at time of sale)` and `Region (current)`, each with a description |
| `dt` | `Order Date`: "Local Finnish business date of the sale." |
| Two fields both called "Sales" in different explores | One measure name, one meaning |

Descriptions are also **documentation** for humans and for handover.

### Making a model AI-ready: checklist

1. **Clean grain and correct primary keys** on every view. No fan-out.
2. **One measure per KPI**, with an explicit definition: VAT included or not, returns netted or not, cancelled excluded or not, currency.
3. **Business labels and descriptions** on every exposed field and explore. Add **synonyms** in the description ("revenue", "turnover").
4. **Hide** technical fields (`_loaded_at`, surrogate keys, hashes).
5. **Small, focused explores** with a clear description of what each one answers.
6. **A rich date dimension**: ISO week, fiscal periods, holidays, trading days, "same day last year". Comparisons like "vs. last year" are the most common questions. (Newer Looker versions offer period-over-period measures; check docs.)
7. **"As was" vs. "as is" fields** clearly named for SCD2 attributes.
8. **Guardrails**: `always_filter` on dates, access filters, sensible row limits.
9. **Freshness visible**: a "Data as of" field so the AI can say "as of 10:45".
10. **Consistent naming** across explores (`net_sales` means the same everywhere).

### Evaluating it: a test set of business questions

You can't say "the AI is good" without measuring. Build an **evaluation set**:

| # | Question | Expected explore | Expected fields and filters | Expected answer (on a fixed closed period) |
|---|---|---|---|---|
| 1 | "Net sales last week in the South region" | sales | `net_sales`, region = South, week = W41 | 1,234,567 EUR |
| 2 | "Which 5 categories grew most vs. last year in September?" | sales | `category`, `net_sales`, YoY | List of 5 |
| 3 | "Return rate for the web shop this month" | sales or returns | `return_rate`, channel = web | 7.2% |
| 4 | "Show me margin for store 123" (user without cost access) | — | Should refuse or hide margin | Access denied |

- 30–50 questions from real users, covering metrics, time comparisons, filters, rankings, ambiguous questions and access-restricted ones.
- Check **field selection** (did it pick the right explore, fields and filters?) and the **answer value** on a **closed, reconciled period**, so expected answers don't move.
- Run it **every time the LookML model changes** (like a regression test). Track the pass rate over time.
- Review failures: usually the fix is a better description, a missing measure, or an ambiguous field name, not "a better AI".

**In short:** conversational BI is mostly a data modelling and semantic layer problem. If the star schema is clean, every KPI is one well-described LookML measure, and access rules live in the model, the AI only has to pick fields. An evaluation set of real business questions, run on every model change, proves that it works.

---

## 7. What matters most in practice

### Critical 1: Looker is in-database
Looker stores no data. Each query is generated SQL in BigQuery. So the BigQuery model (partitions, clustering, aggregates) decides Looker speed and cost. Looker tuning and BigQuery optimization are the same task.

### Critical 2: Primary keys + relationships = correct numbers
Fan-out (the shipping fee example) is handled by symmetric aggregates. The warehouse-side fix is better: one clean grain per fact, `many_to_one` joins to dimensions, and grain tests in Dataform.

### Critical 3: Metrics defined once, in LookML
Ratios as `type: number` of sums; filtered measures for business rules. Every dashboard, API call and AI question uses the same definition. This is the core value of the semantic layer.

### Critical 4: Freshness-aware caching
A datagroup triggered by the pipeline's publish (a control table or `MAX(_loaded_at)`), with a `max_cache_age` safety net. Near real-time data with a stale cache is not near real-time.

### Critical 5: Cost guardrails
`always_filter` on the partition date + `require_partition_filter` in BigQuery + aggregate awareness + separate BI reservation + BI Engine where needed.

### Critical 6: Security in one place
Access filters with user attributes for rows, access grants for fields, or BigQuery policies with OAuth. The AI must use the same path and the user's identity.

### Critical 7: AI-readiness and evaluation
Descriptions, labels, synonyms, focused explores, hidden technical fields, and an evaluation set with expected answers. This is the "conversational BI" deliverable in practice.

### Critical 8: Software engineering for LookML
Git, dev mode, PRs, LookML validator, data tests, content validator. LookML gets the same CI/CD, git, testing and handover practices as the rest of the platform.

---

## 8. Mapping from Power BI / Fabric

| Power BI / Fabric | Looker | Key difference |
|---|---|---|
| Semantic model (dataset) | LookML model + views | LookML is **code in git**; Power BI models are usually edited in a GUI (with newer text formats like TMDL / PBIP for source control) |
| Tables + relationships (1:*, single direction) | Views + explore joins with `relationship: many_to_one` | In Looker, joins are defined **per explore**, not globally |
| DAX measures (evaluated in filter context, `CALCULATE`) | LookML measures (SQL aggregates generated per query) | DAX is a separate expression language over an engine; LookML compiles to the database's SQL |
| Calculated columns | Dimensions with `sql:` | Both row-level |
| **Import mode** (data copied into VertiPaq in memory) | No equivalent: Looker doesn't store data | Looker depends on the warehouse's speed |
| **DirectQuery** (queries the source live) | Looker's normal mode: always in-database | Looker is designed for this; DirectQuery is often a compromise |
| **Direct Lake** (Fabric: reads Delta files in OneLake directly) | Closest conceptually: query fresh warehouse data without an import step | Different mechanism, similar goal |
| Aggregations (user-defined agg tables) | Aggregate awareness (`aggregate_table`) | Same idea: automatic routing to smaller tables |
| Incremental refresh | Incremental PDTs (but mainly done in Dataform) | |
| Row-level security (DAX roles) | `access_filter` + user attributes | |
| Object-level security | `access_grant`, `hidden` | |
| Power BI report | Looker dashboard / Look | |
| Power BI Q&A / Copilot | Gemini in Looker / conversational analytics | Both depend on model quality, synonyms and descriptions |
| Deployment pipelines (dev/test/prod workspaces) | Dev mode, git branches, deploy to production | |

**Looker vs. Looker Studio.** **Looker** is the enterprise platform with LookML, governance, explores, the API and AI features. **Looker Studio** (formerly Data Studio) is a lighter, free dashboard tool (with a paid Pro edition) where each report can define its own data source and calculated fields. That's flexible but not governed. Looker Studio can connect to Looker models to reuse governed metrics; check current docs for the exact capabilities. For the sales model and conversational BI, the governed layer is Looker.

---

## 9. Worked design exercise: exposing the sales model in Looker

**Given:** the mart from [Data warehouse modelling](dwh-modelling.md): `fct_sales_line` (grain: order line, partitioned by `order_date`), `dim_store` (SCD2), `dim_product` (SCD2 + current columns), `dim_customer`, `dim_date`, `dim_channel`, `agg_sales_15min`, and a `publish_log` control table updated after each successful, tested run.

**Task:** design the LookML project. Try it yourself first, then compare.

**1. Project structure**

```text
sales_lookml/                     (git repo)
  manifest.lkml
  models/sales.model.lkml          connection, datagroups, explores
  views/base/                      generated from tables: fields only
    fct_sales_line.view.lkml
    dim_store.view.lkml ...
  views/refinements/               business labels, descriptions, measures
    fct_sales_line.refine.lkml
  tests/sales_tests.lkml           LookML data tests
```

**2. Datagroup**

```text
datagroup: sales_published {
  sql_trigger: SELECT MAX(published_at) FROM `prj-dwh-sales-prod.mart_sales.publish_log` ;;
  max_cache_age: "1 hour"
}
```
The pipeline writes to `publish_log` only after the Dataform assertions pass. So the cache refreshes on new **tested** data.

**3. Explore `sales`**

- Base `fct_sales_line`, joins `many_to_one` to `dim_store`, `dim_product`, `dim_customer`, `dim_channel`, `dim_date` (as order date).
- `always_filter: order_date: "28 days"`.
- `sql_always_where: ${sales.is_test_store} = FALSE` (or handle in the mart).
- `access_filter` on `dim_store.region` with a user attribute for store managers.
- `access_grant` for cost and margin.
- `persist_with: sales_published`.
- An `aggregate_table` at day × store × category for common dashboards.

**4. Measures:** `net_sales`, `gross_sales`, `discount_amount`, `units`, `orders` (count distinct order_id), `average_order_value` (number), `gross_margin`, `gross_margin_pct` (number, ratio of sums), `return_amount` (sum filtered on `line_type = RETURN`), `return_rate` (number), `data_as_of` (max of `_source_ts`).

**5. AI-readiness:** descriptions with definitions and synonyms on every exposed field, explore description, hidden keys, "(at time of sale)" vs. "(current)" for SCD2 attributes.

**6. Tests**

```text
test: net_sales_reconciles_for_closed_day {
  explore_source: sales {
    column: net_sales { field: sales.net_sales }
    filters: [sales.order_date: "2026-09-30"]
  }
  assert: matches_finance_figure {
    expression: round(${sales.net_sales}, 0) = 1234567 ;;
  }
}
```
Plus an evaluation set of 30–50 business questions for the conversational BI layer.

**7. Performance:** BI reservation separate from ETL; BI Engine if dashboards need sub-second; check `INFORMATION_SCHEMA.JOBS` weekly for the top expensive Looker queries.

**Hands-on option.** If you can't access Looker, practice by writing the LookML above in a text editor and, for each explore query you imagine, write the SQL Looker would generate. Then run that SQL in the BigQuery sandbox on your `my_sales` tables from [Data warehouse modelling](dwh-modelling.md). Reproduce the fan-out example (section 4.7) in BigQuery with two small tables and check both the naive and the symmetric results.

```sql
WITH orders AS (SELECT 'O1' order_id, 10 shipping_fee UNION ALL SELECT 'O2', 5),
items AS (SELECT 1 item_id, 'O1' order_id, 20 price UNION ALL SELECT 2,'O1',30
          UNION ALL SELECT 3,'O1',15 UNION ALL SELECT 4,'O2',40)
SELECT
  SUM(o.shipping_fee) AS naive_shipping,                           -- 35 (wrong)
  SUM(DISTINCT CAST(FARM_FINGERPRINT(o.order_id) AS NUMERIC) * 1000 + o.shipping_fee)
    - SUM(DISTINCT CAST(FARM_FINGERPRINT(o.order_id) AS NUMERIC) * 1000) AS symmetric_shipping,   -- 15
  SUM(i.price) AS items_total                                       -- 105 (correct)
FROM orders o JOIN items i USING (order_id);
```
(Simplified: the hash is multiplied by 1000 so it can't collide with small fee values. Looker's real SQL is similar in spirit but more robust. The point is the idea.)

---

## 10. Glossary

| Term | Meaning |
|---|---|
| **Access filter** | Explore-level row filter based on a user attribute |
| **Access grant** | Restricts fields, views or explores to users with certain attribute values |
| **Aggregate awareness** | Looker automatically uses a smaller aggregate table when it can answer the query |
| **Always filter** | Filter users can change but not remove |
| **BI Engine** | BigQuery's in-memory acceleration service for BI queries |
| **Connection** | Looker's configuration for accessing BigQuery (credentials, scratch dataset) |
| **Content validator** | Finds saved dashboards/Looks broken by LookML changes |
| **Conversational BI** | Asking business questions in natural language and getting data answers |
| **Datagroup** | Named policy for cache invalidation and PDT rebuilds (`sql_trigger`, `max_cache_age`) |
| **Derived table** | A view based on a query instead of a physical table |
| **Dev mode** | Personal git branch view of the LookML project |
| **Dimension** | Row-level field used to group or filter |
| **Dimension group** | One definition that creates many time or duration fields |
| **Explore** | A base view plus joins; the queryable unit |
| **Extends** | Create a new LookML object inheriting from another |
| **Fan-out** | Row duplication from a one-to-many join, causing double counting |
| **Gemini in Looker** | Google's generative AI features in Looker (check current docs) |
| **Incremental PDT** | A PDT that rebuilds only recent increments |
| **In-database** | BI that runs queries in the warehouse instead of storing data |
| **LookML** | Looker's modelling language |
| **Looker Studio** | Google's lighter, ungoverned dashboard tool (formerly Data Studio) |
| **Measure** | Aggregation computed at query time |
| **Model** | LookML file defining connection, includes and explores |
| **Native derived table (NDT)** | Derived table defined with LookML fields from an explore |
| **PDT** | Persistent derived table, stored in the scratch dataset |
| **Primary key** | Unique dimension of a view, needed for correct aggregation |
| **Refinement** | Modifies an existing LookML object in place (`view: +name`) |
| **Relationship** | Join cardinality declaration (many_to_one, etc.) |
| **Scratch dataset** | BigQuery dataset where Looker writes PDTs |
| **Semantic layer** | Central definition of metrics, joins and access rules |
| **sql_always_where** | Hidden WHERE condition on every explore query |
| **Symmetric aggregates** | Looker's SQL technique to aggregate correctly despite fan-out |
| **User attribute** | Per-user or per-group value used in filters and grants |
| **View** | LookML object mapped to a table, with dimensions and measures |
