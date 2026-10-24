# Looker, LookML and the semantic layer (for conversational BI)

> The sales model is **made available in Looker** and feeds **conversational BI**. These are the concepts you need to design for it.

---

## 1. What Looker is (and isn't)

- Looker is a **BI platform with a semantic modelling layer (LookML)**. It **doesn't store data**: it generates SQL and runs it **in BigQuery** at query time.
- **Looker** (the LookML platform) ≠ **Looker Studio** (formerly Data Studio, a free and lighter dashboard tool).
- Its value is **governed metrics**: define "net sales" once in LookML, and every dashboard, explore, API call and AI question uses the same definition.

```
BigQuery tables ──► LookML (views, explores, models) ──► generated SQL ──► BigQuery
                         │
                         ├─ Explores / dashboards (humans)
                         ├─ API / embedded analytics
                         └─ Conversational analytics / Gemini in Looker (AI)  ← relies on the semantic layer
```

---

## 2. LookML building blocks

| Object | Meaning |
|---|---|
| **Project** | A git repo of LookML files |
| **Connection** | Credentials to BigQuery (a service account) |
| **Model** | Which connection + which explores are exposed |
| **View** | Maps to a table (or derived table): **dimensions** and **measures** |
| **Dimension** | A column or expression to group or filter by (`store_name`, `order_date`) |
| **Dimension group** | Time dimensions in one go (date, week, month, quarter, year) |
| **Measure** | An aggregation (`sum`, `count_distinct`, `average`, or custom SQL) |
| **Explore** | The starting view + **joins** to other views: what users can query |
| **Derived table** | A SQL- or LookML-defined table; **PDT** = persisted in BigQuery |
| **Datagroup** | A caching and PDT-rebuild policy (triggered by SQL or a max cache age) |

### Example: sales views and explore

```lookml
# views/fct_sales_line.view.lkml
view: fct_sales_line {
  sql_table_name: `prj-dwh-sales-prod.mart_sales.fct_sales_line` ;;

  dimension: pk {
    primary_key: yes
    hidden: yes
    sql: CONCAT(${TABLE}.order_id, '-', CAST(${TABLE}.line_no AS STRING)) ;;
  }

  dimension_group: order {
    type: time
    timeframes: [raw, time, date, week, month, quarter, year]
    sql: ${TABLE}.order_ts ;;
  }

  dimension: store_id   { hidden: yes  sql: ${TABLE}.store_id ;; }
  dimension: product_id { hidden: yes  sql: ${TABLE}.product_id ;; }

  measure: net_sales {
    type: sum
    sql: ${TABLE}.net_amount ;;
    value_format_name: eur
    description: "Sales after discounts, excluding VAT. Returns are negative lines."
  }

  measure: gross_margin {
    type: sum
    sql: ${TABLE}.net_amount - ${TABLE}.cost_amount ;;
    value_format_name: eur
  }

  measure: gross_margin_pct {
    type: number
    sql: SAFE_DIVIDE(${gross_margin}, ${net_sales}) ;;     # ratio of sums, never a sum of ratios
    value_format_name: percent_1
  }

  measure: orders {
    type: count_distinct
    sql: ${TABLE}.order_id ;;
  }
}
```

```lookml
# models/sales.model.lkml
connection: "bq_sales"
include: "/views/*.view.lkml"

datagroup: sales_freshness {
  sql_trigger: SELECT MAX(_loaded_at) FROM `prj-dwh-sales-prod.mart_sales.fct_sales_line` ;;
  max_cache_age: "15 minutes"
}

explore: sales {
  from: fct_sales_line
  persist_with: sales_freshness
  always_filter: { filters: [sales.order_date: "7 days"] }   # ← forces partition pruning
  join: dim_store {
    sql_on: ${sales.store_id} = ${dim_store.store_id} ;;
    relationship: many_to_one
  }
  join: dim_product {
    sql_on: ${sales.product_id} = ${dim_product.product_id} ;;
    relationship: many_to_one
  }
}
```

---

## 3. Key concepts

### Symmetric aggregates (fan-out protection)

- Joining a one-to-many relationship can **duplicate** rows, so `SUM` double-counts.
- If **primary keys** are defined and the **relationship** is declared correctly, Looker generates "symmetric aggregate" SQL that sums distinct values correctly.
- Lesson: **always define `primary_key` and the correct `relationship`.** This is why grain matters (see [Data warehouse modelling](dwh-modelling.md)).

### Caching and freshness

- Looker caches query results. A **datagroup** decides when the cache is invalid: a `sql_trigger` (e.g. the max load timestamp changed) or `max_cache_age`.
- For near real-time, the trigger is the **fact's latest load timestamp**, so dashboards refresh when new data lands, not on a blind timer.

### Derived tables and PDTs

| Type | What |
|---|---|
| Ephemeral derived table | A subquery (CTE) at query time |
| **PDT** (persistent) | Looker builds it as a table in BigQuery (scratch dataset) and rebuilds it per datagroup |
| **Incremental PDT** | Appends or rebuilds only recent partitions |
| **Aggregate awareness** (`aggregate_table`) | Looker **automatically** routes a query to a smaller pre-aggregated table when it can answer it |

Design guideline: keep heavy transformations in Dataform/dbt (version-controlled and tested in the warehouse layer) and use Looker PDTs and aggregate awareness only for BI-specific performance.

### Access control

- **Model sets / permission sets / roles:** who can see which models and explores.
- **`access_filter` + user attributes:** row-level security in Looker (e.g. a store manager sees only their region).
- **`access_grant`:** hide fields (e.g. cost and margin) from some groups.
- Or enforce in BigQuery (row access policies, policy tags), and Looker inherits it if it uses per-user OAuth credentials. Using a single service account means security must be in Looker.

### Development workflow

- LookML is in **git**: dev mode (a personal branch), validate, then **content validator** (finds broken dashboards), then PR, then deploy to production.
- **Spectacles** / the LookML validator in CI for automated checks.

---

## 4. Conversational BI: what makes it work

The end goal: a near real-time sales data model that supports a conversational BI solution. (For example, Gemini in Looker / Looker conversational analytics, or a custom LLM agent querying BigQuery.)

**Why the semantic layer is crucial for AI:**

| Without a semantic layer | With LookML |
|---|---|
| The LLM writes raw SQL against tables: guesses joins and columns | The LLM picks **explores, dimensions and measures** that are already correct |
| "Net sales" computed differently per question | **One definition**, governed |
| Fan-out double counting | Symmetric aggregates with PKs and relationships |
| Ambiguous column names (`amt2`, `flag_x`) | **Labels and descriptions** the LLM reads |
| No guardrails | `always_filter`, access filters, hidden fields |

### Checklist for an AI-ready sales model

1. **Clear grain** and **primary keys** on every view.
2. **Business-friendly names, labels and `description`s** on every exposed field. *Write them as if explaining to a new colleague.*
3. **Measures for every KPI** (net sales, margin %, units, AOV, returns rate) with explicit definitions (VAT included or not? returns netted?).
4. **Date dimension** with fiscal calendar, week numbering (ISO weeks in Finland), holidays, "same period last year".
5. **Hide** technical columns (`_loaded_at`, keys).
6. **Small, focused explores** (sales, returns, inventory) rather than one giant explore.
7. **Synonyms** in descriptions ("revenue", "turnover" = net sales).
8. **Freshness** visible to users ("data as of 10:45").
9. **Test questions:** a set of 30–50 typical business questions with expected answers, checked whenever the model changes (an "evaluation set").

In short: for conversational BI, most of the work is upstream: a clean star schema, one definition per metric in LookML, and good descriptions. Then the AI has something reliable to reason over. A test set of typical business questions is used to evaluate the answers.

---

## 5. Performance: Looker on BigQuery

| Problem | Fix |
|---|---|
| Explore scans whole tables | `always_filter` / `sql_always_where` on the partition column; partitioned and clustered tables |
| Repeated heavy aggregations | Aggregate awareness / aggregate tables / BigQuery MVs |
| Many users, sub-second need | **BI Engine** reservation |
| Dashboard with 20 tiles = 20 queries | Merge tiles, cache via datagroups, reduce tiles |
| BI competing with ETL for slots | Separate **BI reservation** (Editions) |
| Expensive `count_distinct` | Approximate measures (`count_distinct` with `approximate: yes`) where acceptable |

---
