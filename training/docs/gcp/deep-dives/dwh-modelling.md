# Data warehouse modelling and the near real-time sales model

> Modelling approaches, Kimball essentials, SCDs, BigQuery-specific trade-offs and incremental loading, applied to a near real-time sales model on BigQuery that is served in Looker for **conversational BI**.

---

## 1. The three big modelling approaches

| Approach | Idea | Strengths | Weaknesses | Where it fits |
|---|---|---|---|---|
| **Kimball (dimensional)** | Business-process **star schemas**: facts + dimensions, bottom-up by process, integrated via **conformed dimensions** | Easy for BI and users, fast queries, natural for Looker | Hard to absorb many changing sources; history is in SCDs | **Marts / gold** layer. Most common in BigQuery + Looker |
| **Inmon (3NF EDW)** | Normalized enterprise model first, marts derived from it | Single integrated truth | Slow to build, many joins | Legacy enterprise EDWs |
| **Data Vault 2.0** | **Hubs** (business keys), **Links** (relationships), **Satellites** (attributes + history), insert-only | Auditable, absorbs source changes, parallel loading, good for many sources | More tables and complexity; needs a mart layer on top for BI | **Integration / silver** layer in large EDWs |

Typical BigQuery layering: **raw → cleaned/conformed → Kimball marts**. Joins are the expensive part, so keep marts as star schemas or even wide denormalized tables. With many volatile sources and strict audit needs, consider Data Vault in the integration layer, with marts on top.

---

## 2. Kimball essentials

### The 4-step design process

1. **Choose the business process**: e.g. "sales order lines", "returns", "inventory".
2. **Declare the grain**: *exactly what one row means*. **Always declare it first.** "One row per order line per change state" or "one row per order line."
3. **Identify the dimensions**: who, what, where, when (customer, product, store, date, channel, promotion).
4. **Identify the facts**: the numeric measures at that grain (qty, gross_amount, discount, net_amount, cost).

### Fact table types

| Type | Grain | Example | Behaviour |
|---|---|---|---|
| **Transaction** | One row per event | Order line | Insert-heavy, the most common |
| **Periodic snapshot** | One row per entity per period | Daily stock per product per store | Regular, dense |
| **Accumulating snapshot** | One row per process instance, **updated** as milestones happen | Order: placed → paid → shipped → delivered | Updates. Has multiple date columns |
| **Factless** | Event with no measure | Student attendance, promotion coverage | Counting events |

### Measure types

| Type | Can sum across… | Example |
|---|---|---|
| **Additive** | All dimensions | Sales amount, qty |
| **Semi-additive** | Some dimensions, **not time** | Account balance, stock level (use avg or last over time) |
| **Non-additive** | None | Ratios, percentages, unit price. Store the **components** and compute the ratio in BI |

### Dimension patterns

| Pattern | Meaning |
|---|---|
| **Conformed dimension** | The same `dim_product` and `dim_date` shared across facts, so you can "drill across" sales and returns |
| **Degenerate dimension** | A dimension key with no dimension table, e.g. `order_number` stored in the fact |
| **Role-playing dimension** | One dimension used for several roles: `dim_date` as order_date, ship_date, delivery_date |
| **Junk dimension** | Groups low-cardinality flags (is_gift, payment_type, channel) into one small dimension |
| **Mini-dimension** | Rapidly changing attributes (age band, income band) split out of a big dimension |
| **Bridge table** | Many-to-many, e.g. an account with multiple customers |
| **Unknown member** | A `-1` / `'UNKNOWN'` row, so facts never have null foreign keys |

### Bus matrix (shows "enterprise" thinking)

|  | Date | Product | Store | Customer | Promotion |
|---|---|---|---|---|---|
| Sales | ✅ | ✅ | ✅ | ✅ | ✅ |
| Returns | ✅ | ✅ | ✅ | ✅ | |
| Inventory | ✅ | ✅ | ✅ | | |

Rows = business processes (facts), columns = conformed dimensions. It's a planning tool for which marts share which dimensions.

---

## 3. Slowly changing dimensions (SCD)

| Type | Behaviour | Use |
|---|---|---|
| **0** | Never changes | Original signup date |
| **1** | **Overwrite**, no history | Fixing typos, attributes nobody analyses historically |
| **2** | **New row per change**, with `valid_from`, `valid_to`, `is_current` | History matters: customer segment, product category, store region |
| **3** | Previous value in an extra column | Rare: "current and previous region" |
| **4** | History in a separate table | Rapid changes |
| **6** | 1 + 2 + 3 hybrid: current value on every historical row | Report both "as was" and "as is" |

### SCD2 in BigQuery (MERGE pattern)

```sql
-- staging.customer: one row per customer (already deduplicated), with
-- attr_hash = TO_HEX(MD5(CONCAT(IFNULL(segment,''),'|',IFNULL(city,''))))
MERGE sales.dim_customer T
USING (
  -- (a) every staging row, keyed: matches the current version if one exists
  SELECT s.customer_id AS merge_key, s.* FROM staging.customer s
  UNION ALL
  -- (b) changed rows again with a NULL key: never matches, so the INSERT branch fires
  SELECT NULL AS merge_key, s.* FROM staging.customer s
  JOIN sales.dim_customer d
    ON d.customer_id = s.customer_id AND d.is_current AND d.attr_hash != s.attr_hash
) S
ON T.customer_id = S.merge_key AND T.is_current
-- changed → close the old version
WHEN MATCHED AND T.attr_hash != S.attr_hash THEN
  UPDATE SET is_current = FALSE, valid_to = CURRENT_TIMESTAMP()
-- brand-new customer (a) or new version of a changed one (b) → insert the current row
WHEN NOT MATCHED THEN
  INSERT (customer_sk, customer_id, segment, city, attr_hash, valid_from, valid_to, is_current)
  VALUES (FARM_FINGERPRINT(CONCAT(S.customer_id, '|', CAST(CURRENT_TIMESTAMP() AS STRING))),
          S.customer_id, S.segment, S.city, S.attr_hash,
          CURRENT_TIMESTAMP(), TIMESTAMP '9999-12-31', TRUE);
-- unchanged customers: (a) matches with the same hash → nothing happens
```

- In practice, **dbt snapshots** or **Dataform** do this for you. Prefer dbt snapshots or a Dataform incremental model over hand-written SCD2 MERGEs.
- Use the **source change timestamp** for `valid_from` when you have CDC. It's more accurate than load time.
- Joining a fact to an SCD2 dimension: `fact.order_ts >= dim.valid_from AND fact.order_ts < dim.valid_to`, or look up the surrogate key at load time and store it in the fact.

---

## 4. Keys in BigQuery

- There are no sequences or identity columns as in classic databases. Options:
    - **Natural/business key** (`order_id`): simplest, often fine
    - **Hash key**: `FARM_FINGERPRINT(CONCAT(source, '|', id))` (INT64) or `TO_HEX(MD5(...))`. It's deterministic, so **reloads produce the same key** and the load stays idempotent
    - `GENERATE_UUID()`: random, **not** deterministic, so avoid it for keys you must rebuild
- **Primary/foreign key constraints** exist as `NOT ENFORCED`. They're metadata that help the optimizer (join elimination) and document the model. They aren't validated, so test them yourself.
- Composite keys from different sources: include the source system in the hash to avoid collisions.

---

## 5. BigQuery-specific modelling choices

### Star schema vs. wide denormalized table vs. nested

| Option | Pros | Cons | Use when |
|---|---|---|---|
| **Star schema** | Flexible, conformed dims, SCD history, natural for Looker explores | Join cost at query time | The default mart design |
| **One Big Table (wide, denormalized)** | Fastest and cheapest to query, simple for BI and **AI/conversational BI** | Duplication, harder SCD handling, rebuild on dimension change | Hot dashboards, a specific use case on top of the star |
| **Nested/repeated (STRUCT/ARRAY)** | Parent-child without join (order + lines); stored together | Harder for some BI tools; `UNNEST` everywhere | Event payloads, orders with lines, IoT readings per device |

Rule: **storage is cheap, joins and shuffles are expensive.** Denormalize carefully, but keep the star as the source of truth.

### Layering (medallion, named the GCP way)

```
raw_<source>      (bronze)  as received (Datastream CDC tables, files); full history, append or CDC
stg_<domain>      (silver)  typed, deduplicated, renamed, conformed, data-quality-tested
mart_<domain>     (gold)    star schemas + aggregates, business logic, used by Looker
```

- One **dataset per layer per domain** in BigQuery makes IAM simple: analysts get `dataViewer` on `mart_*` only.
- **Staging = 1:1 with source tables** (clean, rename, cast). **Intermediate** = joins and business rules. **Marts** = facts and dims. That's the dbt-style convention.

---

## 6. Incremental and near real-time modelling

### Incremental strategies

| Strategy | How | Best for |
|---|---|---|
| **Append-only** | Insert new rows since the watermark | Immutable events (sensor readings, clicks) |
| **MERGE on key** | Upsert changed rows | Mutable entities (orders change status) |
| **Insert-overwrite partitions** | Recompute only affected **partitions** and replace them | Partitioned facts; usually the cheapest on BigQuery |
| **Rolling lookback window** | Each run reprocesses the last N days | Late-arriving data and updates (orders edited, returns) |

```sql
-- dbt incremental model (BigQuery), partition overwrite with a 3-day lookback
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = {'field': 'order_date', 'data_type': 'date'},
    cluster_by = ['store_id', 'product_id']
) }}
SELECT ...
FROM {{ ref('stg_pos__order_lines') }}
{% if is_incremental() %}
WHERE order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
{% endif %}
```

### Watermarks: track what's been processed

- Use **source commit time** (CDC metadata like `datastream_metadata.source_timestamp`), not load time, when you need correctness.
- Store the watermark in a control table or derive it with `MAX(...)` on the target, but **compute it once** (in a variable) so partition pruning still works.
- For late data, **overlap** the window (process `watermark - 30 min`) and rely on idempotent MERGE to absorb duplicates.

### Late-arriving data

| Case | Handling |
|---|---|
| **Late fact** (an order from yesterday arrives today) | Lookback window / partition overwrite of affected dates |
| **Late dimension** ("early fact": the fact references a product that doesn't exist in `dim_product` yet) | Insert an **inferred member** (placeholder dimension row) and update it when the real data arrives, or use the unknown member and reprocess |
| **Updates to closed periods** | Agree with finance when a period is "closed", and reprocess or adjust after that |

---

## 7. Designing a near real-time sales model

### Step 1: Questions to ask first

- **Sources?** POS, e-commerce, ERP? Which ones are CDC-capable databases?
- **Freshness need?** Per use case: operational "today's sales" every ~15 minutes vs. finance daily.
- **Key metrics?** Net sales, gross margin, units, basket size, returns. **Who owns the definitions?**
- **Dimensions?** Store, product hierarchy, customer, channel, promotion, date/time.
- **History needs?** Report by the product's category **then** or **now**? (That decides SCD1 vs. SCD2.)

### Step 2: Architecture

```
 POS DB / E-com DB / ERP
      │ Datastream (CDC, backfill + continuous)
      ▼
 raw_pos.*, raw_ecom.*           BigQuery CDC tables (PK + max_staleness ≈ 5 min)
      │ Dataform/dbt, triggered every 15 min by Composer (or Datasets)
      ▼
 stg_sales.*                      typed, deduplicated, unified across channels (channel column)
      ▼
 mart_sales.fct_sales_line        grain: one row per order line
     PARTITION BY order_date  CLUSTER BY store_id, product_id
 mart_sales.dim_product (SCD2)  dim_store (SCD1/2)  dim_customer (SCD2)  dim_date  dim_channel
 mart_sales.agg_sales_15min       pre-aggregated for "today" dashboards (MV or incremental table)
      ▼
 Looker: LookML views/explores, metric definitions (net_sales, margin), datagroups refresh on new data
      ▼
 Conversational BI (e.g. Gemini in Looker / conversational analytics) on top of the semantic layer
```

### Step 3: Fact table design

| Column | Notes |
|---|---|
| `order_id`, `line_no` | Degenerate dimension + line, together the natural key |
| `order_ts`, `order_date` | Partition on `order_date` |
| `store_sk`, `product_sk`, `customer_sk`, `channel_sk`, `promo_sk` | FKs (or natural keys + SCD2 join on time) |
| `qty`, `gross_amount`, `discount_amount`, `net_amount`, `cost_amount`, `tax_amount` | **Additive** components. Margin % is computed in Looker |
| `order_status`, `is_return` | Returns as **negative lines** or a separate returns fact. Decide and document |
| `currency`, `net_amount_eur` | Convert at the transaction-date FX rate |
| `_source_system`, `_source_ts`, `_loaded_at` | Lineage and freshness metadata |

### Step 4: Freshness and correctness

- **SLA:** "data in Looker is at most 15 minutes behind the source," **measured** by `CURRENT_TIMESTAMP() - MAX(_source_ts)`, with an alert on breach.
- The "today" dashboards read `agg_sales_15min`. Historical analysis reads the fact. Both use the **same LookML measure definitions**.
- **Reconciliation** against the source (counts and sums per store per day) runs daily as a blocking test.

### Step 5: Why the model matters for conversational BI

- An LLM answering "what were net sales in Helsinki stores last week vs. the same week last year?" needs:
    - **One unambiguous definition** of `net_sales` (in LookML, not duplicated in 5 SQL files)
    - **Clear names and descriptions** on fields (the LLM reads them)
    - **Correct joins and grain**, so there are no fan-out double counts
    - A **date dimension** with fiscal weeks, holidays and comparable periods
- Conversational BI is only as good as the semantic layer. Most of the work is a clean star schema plus well-described LookML metrics, not the AI.

---

## 8. Data quality as part of the model

| Test | Example |
|---|---|
| **Uniqueness** of the grain | `(order_id, line_no)` unique in the fact |
| **Not null** keys | FKs not null (use unknown members) |
| **Referential integrity** | Every `product_sk` exists in `dim_product` |
| **Accepted values** | `channel IN ('store','web','app')` |
| **Freshness** | `MAX(_source_ts) > NOW() - 30 min` |
| **Volume anomaly** | Today's row count within ±30% of the same weekday's average |
| **Reconciliation** | Sum of net_amount per store per day = source |

Implement them as **dbt tests** or **Dataform assertions**, run them in the pipeline as **blocking** checks for critical marts, and alert on the rest.

---
