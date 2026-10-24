# Data warehouse modelling (and the near real-time sales model)

> **Why this lesson matters.** Strong data warehouse modelling is the base of every enterprise data warehouse (EDW). A typical goal of an EDW modernization on GCP is "a near real-time sales data model on BigQuery, made available in Looker", which can also feed a **conversational BI** solution. If you understand grain, facts, dimensions, SCD2, incremental loads and the sales model, you understand the core of the warehouse.
>
> **Quick reference:** the cheat sheet is the [data warehouse modelling deep dive](../deep-dives/dwh-modelling.md). This lesson explains the *why* and *how* behind it. Related lessons: [BigQuery](bigquery.md), [Datastream and CDC](datastream-cdc.md), [Dataform and dbt](dataform-dbt.md), [Looker and the semantic layer](looker-semantic-layer.md).

---

## 1. What it is and why it exists

### Plain language

A **data model** is the shape you give your data so that people can answer questions with it. Source systems (a point-of-sale database, a web shop, an ERP) are shaped for *running the business*: they must save one sale very fast and very safely. A data warehouse is shaped for *understanding the business*: it must answer "how much did we sell, by region, by week, compared to last year?" over millions or billions of rows.

These two goals need different shapes. **Data warehouse modelling** is the discipline of choosing that second shape, on purpose, with clear rules.

### An analogy

Think of a large library.

- The **source system** is the returns desk. Books arrive one by one. The desk only needs to record "book X came back at 10:42". It is optimized for fast, small writes.
- The **data warehouse** is the catalogue. Books are organized by subject, author and year so a visitor can find "all books about energy from 2015–2020" quickly. It is optimized for reading and browsing.
- **Modelling** is designing the catalogue: which subjects exist, what one catalogue card represents, and how cards link to each other.

If the catalogue has no clear rule for "what one card means", visitors count the same book twice. That is exactly what happens in a warehouse with an unclear **grain**, and it is the most common modelling bug.

### OLTP vs. OLAP

| | OLTP (source systems) | OLAP (data warehouse) |
|---|---|---|
| Typical query | "Insert this order", "update order 123" | "Sum of net sales by region and month for 3 years" |
| Rows touched per query | 1–100 | Millions to billions |
| Columns touched | All columns of a few rows | A few columns of many rows |
| Shape | **Normalized** (3NF): no duplication, many small tables | **Dimensional**: facts + dimensions, some duplication on purpose |
| Storage | Row-oriented (PostgreSQL, MySQL, SQL Server) | Column-oriented (BigQuery, Fabric/Delta, Snowflake) |
| Priority | Correct and fast writes, consistency | Fast, simple, correct reads; history |
| History | Usually only the current state | Full history, often with versions |

### Normalized vs. dimensional

**Normalization** (3rd normal form) removes duplication. A product's category name lives in exactly one row of a `category` table. To update it, you change one row. This is perfect for writes.

For analytics, normalization hurts. A simple question like "sales by category by region" may need 8 joins (order_line → order → store → city → region, order_line → product → subcategory → category). Each join costs time. Users and BI tools also get confused by so many tables.

**Dimensional modelling** collapses those chains into a few wide, descriptive **dimension** tables around one central **fact** table. The product dimension contains `product_name`, `subcategory`, `category` and `brand` together. Category names are now repeated on many rows. That duplication is a deliberate trade: storage is cheap, but people's time and query joins are expensive.

```text
  Normalized (OLTP)                          Dimensional (star schema)

 region ─ city ─ store ─┐                        dim_store (store, city, region)
                        order ─ order_line              │
 category ─ subcat ─ product ─┘            dim_date ── fct_sales_line ── dim_product
                                                        │       (product, subcat, category, brand)
                                                   dim_customer
```

---

## 2. Where it fits in a modern GCP data platform

| Platform goal | What modelling skill it needs |
|---|---|
| "Strong DWH modelling and principles" | Kimball vocabulary, grain, SCD, keys, layering, trade-offs |
| "Near real-time sales data model on BigQuery" | Incremental models, CDC-based loads, late data, freshness SLAs |
| "Made available in Looker" | A star schema with clean keys and grain, so LookML joins and measures are correct |
| "Supports a conversational BI solution" | Unambiguous metric definitions, descriptive names, a good date dimension |
| "Simplify warehouse load chains" | A clear layered architecture (raw → staging → marts) instead of tangled chains |
| "Optimize BigQuery queries for runtime and cost" | Partitioning/clustering choices, denormalization, aggregates |
| "Testing, documentation, handover" | Data quality tests as part of the model; a documented bus matrix and grain |

Section 10 is a complete worked design of the sales model.

---

## 3. Architecture: how a modern warehouse is layered

### The three philosophies

#### Kimball (bottom-up, dimensional), 1990s onward

Ralph Kimball's idea: build the warehouse **one business process at a time** (sales, returns, inventory). Each process becomes a **star schema**. Stars are integrated through **conformed dimensions**: the same `dim_product` and `dim_date` are shared by every star. The "enterprise warehouse" is the collection of stars that share dimensions. This is called the **bus architecture**.

- Strength: fast to deliver value, easy for users and BI tools.
- Weakness: if sources change a lot, or many sources describe the same thing differently, you rebuild a lot.

#### Inmon (top-down, normalized EDW)

Bill Inmon's idea: first build a **normalized, integrated enterprise data warehouse** (3NF) that describes the whole company. Then build **data marts** (often dimensional) from it for each department.

- Strength: one integrated truth, low redundancy.
- Weakness: slow to build. The big enterprise model takes a long time before any user sees a dashboard. Many joins.

#### Data Vault 2.0 (Dan Linstedt)

Data Vault is designed for **integration and auditability** when there are many sources that change often. It splits everything into three table types:

| Table | Contains | Changes? |
|---|---|---|
| **Hub** | One row per unique **business key** (customer_number, product_code), plus a hash key, load timestamp, record source | Insert-only |
| **Link** | One row per unique **relationship** between hubs (order ↔ customer ↔ product) | Insert-only |
| **Satellite** | **Descriptive attributes and their history** for a hub or link, with a load timestamp and a `hashdiff` to detect changes | Insert-only (a new row when attributes change) |

**Example.** A customer, an order and the link between them:

```text
hub_customer
  customer_hk (hash of 'CRM|C-1001') | customer_bk 'C-1001' | load_dts | record_source 'CRM'

hub_order
  order_hk (hash of 'POS|O-77')      | order_bk 'O-77'       | load_dts | record_source 'POS'

link_order_customer
  order_customer_hk | order_hk | customer_hk | load_dts | record_source

sat_customer_crm   (attributes from CRM, history by load_dts)
  customer_hk | load_dts 2026-01-05 | hashdiff 'a1f..' | name 'Anna' | segment 'Silver' | city 'Espoo'
  customer_hk | load_dts 2026-06-10 | hashdiff '9c2..' | name 'Anna' | segment 'Gold'   | city 'Espoo'
```

Why it works:

- A new source system adds a **new satellite**. Existing tables don't change.
- Everything is insert-only, so loads can run **in parallel** and the full history is auditable.
- Hash keys mean every table can be loaded independently (no lookups for sequence keys).

Why it's not enough on its own: users can't query hubs, links and satellites directly. It needs many joins. You always build a **dimensional mart layer on top** (often with helper "point-in-time" and "bridge" tables in a "business vault").

#### When each fits

| Situation | Choice |
|---|---|
| A few sources, a clear BI goal, a small team, fast delivery (typical BigQuery + Looker) | **Kimball** marts on top of cleaned staging |
| Dozens of volatile sources, strict audit/regulation, a large EDW team | **Data Vault** in the integration layer + Kimball marts on top |
| A legacy enterprise EDW already normalized | Keep the **Inmon** core for now; build Kimball marts for new use cases |

For a typical EDW modernization (simplifying load chains, a sales model for Looker), the realistic default is **raw → staging → Kimball marts**. Consider Data Vault if the existing EDW already uses it. So first find out: "What does the current EDW use?"

### Medallion / layered architecture

If you know the medallion architecture (for example from Fabric or Databricks): it is a **layering** idea (how clean the data is). Kimball, Inmon and Data Vault are **modelling** ideas (what shape the data has). They combine:

```text
  Sources            Layer (medallion)          Modelling style inside the layer
  ───────            ─────────────────          ────────────────────────────────
  POS DB  ─┐
  Web DB  ─┼─CDC──►  raw_<source>   (bronze)    Source-shaped. As received. CDC tables, full history.
  ERP     ─┘              │
                          ▼
                     stg_<domain>   (silver)    Source-shaped but clean: typed, renamed, deduplicated.
                          │                     Optional: Data Vault or an integration (3NF) model here.
                          ▼
                     int_<domain>               Joins and business rules (dbt "intermediate" layer).
                          │
                          ▼
                     mart_<domain>  (gold)      Kimball: facts, dimensions, aggregates. Used by Looker.
```

In BigQuery, a **dataset per layer per domain** (`raw_pos`, `stg_sales`, `mart_sales`) makes IAM simple: analysts and the Looker service account read `mart_*` only. Transformations between layers live in **Dataform or dbt** (SQL, version-controlled, tested), orchestrated by **Composer** (see [Dataform and dbt](dataform-dbt.md)).

---

## 4. Core concepts, one by one

### 4.1 The Kimball 4-step process

1. **Choose the business process.** A process is something the business *does* that creates measurable events: taking orders, shipping, receiving stock, handling returns. Not a department ("marketing") and not a report ("the weekly dashboard").
2. **Declare the grain.** Say exactly what one row of the fact table means.
3. **Identify the dimensions.** The descriptive context of each row: who, what, where, when, how, why.
4. **Identify the facts.** The numeric measures that are true *at that grain*.

The order is important. You choose the grain **before** dimensions and facts. The grain decides which dimensions are possible and which measures make sense.

### 4.2 Grain (the most important concept)

**What.** The grain is a precise, business-language sentence: "one row in this table represents ___". For example: "one row per sales order line, in its current state".

**How it works.** The grain implies a **unique key**. If the grain is "one row per order line", then `(order_id, line_no)` must be unique. Every dimension must have exactly one value for that row. Every measure must be true for that row.

**Why it matters.**

- If two people disagree about the grain, they will write different SQL and get different numbers.
- Mixing grains in one table (some rows are order lines, some rows are order totals) creates **double counting**.
- Looker's join logic depends on knowing the primary key of every view (see [Looker and the semantic layer](looker-semantic-layer.md), symmetric aggregates).
- A conversational BI tool cannot "guess" that a table has mixed grain. It will sum and be wrong.

**Atomic grain.** Kimball recommends the **lowest, most detailed grain** the source can provide (the order line, not the daily total). You can always aggregate up; you can never break an aggregate down. Atomic data also survives new questions you didn't plan for.

**Good grain statements:**

| Good | Why |
|---|---|
| "One row per order line (order_id, line_no), current state" | Clear key, clear meaning |
| "One row per product per store per day, stock at end of day" | Periodic snapshot; the time unit and the moment are explicit |
| "One row per order, updated as it moves through placed → paid → shipped → delivered" | Accumulating snapshot; the lifecycle is explicit |
| "One row per store per 15-minute interval, net sales" | Time bucket and aggregation are explicit |

**Bad grain statements:**

| Bad | Problem |
|---|---|
| "Sales data" | Not a grain at all |
| "One row per order" when the table also has product_id | An order has many products. Which one? Either it's per line, or product doesn't belong here |
| "One row per transaction" | Is a transaction a basket, a line, or a payment? Say which |
| "Daily sales, but today's rows are per hour" | Mixed grain. Summing across days is now wrong |
| "Order lines plus a row for shipping fees" | Mixed grain unless shipping is modeled as a line type with a clear flag. Better: a separate fact or an order-level allocation |

**Example: the "header vs. line" trap.** The order header has `shipping_fee = 5.00`. The order has 3 lines. If you copy `shipping_fee` onto each line, `SUM(shipping_fee)` returns 15.00. Wrong. Options: (a) allocate it to lines (1.67 each, rounding to the last line), (b) put it on only one line, or (c) keep a separate order-level fact. Option (a) keeps the grain clean and is usually best for sales analysis.

**Common mistake.** Declaring the grain after building the table, from what the SQL happens to produce. Always write the grain first, put it in the table description, and **test** it with a uniqueness test.

### 4.3 Fact table types

**Transaction fact.** One row per event at the moment it happens. Example: `fct_sales_line`. Mostly inserts. Can be sparse (no row when nothing happens). The most common type.

**Periodic snapshot fact.** One row per entity per period, whether or not anything happened. Example: `fct_inventory_daily` (stock per product per store per day). It's dense and predictable. You use it when the measure is a *level* (balance, stock), not a *flow*. You can't easily get stock level from transactions every time someone asks, so you take a "photo" each day.

**Accumulating snapshot fact.** One row per **process instance** (one order, one claim), with **several date columns** for milestones, **updated** as the process progresses.

```text
fct_order_fulfilment
order_id | placed_date | paid_date  | shipped_date | delivered_date | days_to_ship | status
O-77     | 2026-10-01  | 2026-10-01 | 2026-10-03   | NULL           | 2            | SHIPPED
```

It answers "how long does each step take?" and "how many orders are stuck in 'paid'?". In BigQuery it's maintained with `MERGE` (updates are allowed, but each DML statement rewrites affected files, so batch them).

**Factless fact.** A row records that an event or coverage happened, with no numeric measure. Examples: "product X was on promotion in store Y on day Z" (coverage) or "customer attended an event". You count rows. The classic use: "which promoted products did **not** sell?" = promotion coverage factless fact minus sales fact.

| Type | Grain | Rows change after insert? | Example question |
|---|---|---|---|
| Transaction | One event | Rarely (corrections) | Net sales last week |
| Periodic snapshot | Entity × period | No (new photo each period) | Stock at month end |
| Accumulating snapshot | One process instance | Yes, at each milestone | Average days paid → shipped |
| Factless | Event / coverage | No | Promoted products with zero sales |

### 4.4 Measure additivity

**What.** Additivity says across which dimensions you can safely `SUM` a measure.

| Type | Sum across | Example | How to aggregate otherwise |
|---|---|---|---|
| **Additive** | All dimensions | qty, net_amount, cost_amount | `SUM` |
| **Semi-additive** | Some dimensions, **not time** | Stock level, account balance, number of open orders | `AVG` over time, or "last value in period" |
| **Non-additive** | None | Margin %, unit price, conversion rate, distinct customers | Store components; compute the ratio after summing |

**Worked example (non-additive).** Two stores:

| Store | Net sales | Margin | Margin % |
|---|---|---|---|
| A | 1,000 | 300 | 30% |
| B | 9,000 | 900 | 10% |

- Wrong: `SUM(margin_pct) = 40%` or `AVG(margin_pct) = 20%`.
- Right: `SUM(margin) / SUM(net_sales) = 1,200 / 10,000 = 12%`.

**Distinct counts are non-additive too.** 100 customers on Monday and 100 on Tuesday is not 200 customers for the two days if some came both days. Store `customer_id` at atomic grain and use `COUNT(DISTINCT)` at query time (or HyperLogLog sketches, `HLL_COUNT.*` in BigQuery, for pre-aggregated tables).

**Why it matters in practice.** Pre-aggregated tables (the 15-minute aggregate) must store **additive components** only (sums and counts, plus sketches for distinct counts). Ratios go in LookML as `type: number` measures.

**Common mistake.** Storing `margin_pct` in the fact or in an aggregate table, then letting a dashboard sum or average it.

### 4.5 Dimension patterns

**Conformed dimension.** One dimension shared, with the same keys and the same attribute values, by several facts. `dim_product` used by sales, returns and inventory. Why: you can "drill across" (compare sales and returns by category) and get consistent labels everywhere. Without conformed dimensions, every mart has its own product list and numbers never match.

**Degenerate dimension.** A dimension key stored in the fact with **no dimension table**, because all its attributes are already elsewhere. `order_id` on the order-line fact. It's useful for grouping lines into baskets ("average basket size") and for tracing back to the source.

**Role-playing dimension.** One physical dimension used in several roles. `dim_date` is used as `order_date`, `ship_date` and `delivery_date`. In SQL and LookML you join the same table several times under different aliases (`order_date`, `ship_date`).

**Junk dimension.** Several low-cardinality flags and codes (payment_type: 4 values, is_gift: 2, order_source: 3) combined into one small dimension with all combinations (4 × 2 × 3 = 24 rows). The fact gets one key instead of three columns. In BigQuery, columnar storage makes small flag columns cheap, so junk dimensions are less important; often you just keep the flags on the fact. Know the pattern, and know why you might skip it.

**Mini-dimension.** Some attributes of a big dimension change often (customer age band, income band, loyalty tier). Using SCD2 on the main customer table would create huge numbers of versions. Instead, move those attributes into a small `dim_customer_profile` (one row per combination of bands), and put its key on the fact. The main customer dimension stays stable.

**Bridge table.** For many-to-many relationships. A bank account with two owners; a sales line credited to two sales reps; a product in several categories. The bridge has (group_key, member_key, weight). The weight (e.g. 0.5 each) lets you allocate amounts without double counting. Without weights, sums over the bridge double count, by design, which is sometimes what you want ("impact" reporting) and sometimes not.

**Unknown member.** A special row in each dimension (key `-1` or a hash of `'UNKNOWN'`, name `'Unknown'`). Facts with missing or not-yet-known references point to it. Why: fact foreign keys are **never null**, inner joins never drop sales rows, and totals are always complete. You can also add `-2 = 'Not applicable'` (e.g. no promotion).

### 4.6 The bus matrix

**What.** A planning table. Rows are business processes (future fact tables). Columns are conformed dimensions. A tick means "this process uses this dimension".

|  | Date | Time of day | Product | Store | Customer | Channel | Promotion | Employee |
|---|---|---|---|---|---|---|---|---|
| Sales lines | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Returns | ✅ | | ✅ | ✅ | ✅ | ✅ | | ✅ |
| Inventory snapshot | ✅ | | ✅ | ✅ | | | | |
| Promotion coverage | ✅ | | ✅ | ✅ | | ✅ | ✅ | |

**Why it matters.** It gives an enterprise-level view. It tells you which dimensions to build first (the ones with the most ticks), and it prevents each team building its own `dim_product`.

---

## 5. Slowly changing dimensions (SCD) in depth

Dimension attributes change: a customer moves city, a product moves to a new category, a store changes region. The question is always: **when we report old sales, do we want the attribute value as it was then, or as it is now?** The answer is a business decision, attribute by attribute.

### 5.1 The types, with one running example

Customer C-1001, Anna, lives in **Espoo**, segment **Silver**. On 2026-06-10 she moves to **Tampere** and becomes **Gold**.

**Type 0: retain original.** The value never changes after the first load. Example: `first_signup_date`, `original_acquisition_channel`. Anna's signup date stays as is forever.

**Type 1: overwrite.** Replace the old value. No history.

```text
Before: customer_id C-1001 | city Espoo   | segment Silver
After:  customer_id C-1001 | city Tampere | segment Gold
```
All past sales now appear under Tampere. Use for corrections (typos) or attributes nobody analyses historically (email, phone).

**Type 2: add a new row.** Close the old version and insert a new one. Full history.

```text
customer_sk | customer_id | city    | segment | valid_from | valid_to   | is_current
101         | C-1001      | Espoo   | Silver  | 2026-01-05 | 2026-06-10 | FALSE
102         | C-1001      | Tampere | Gold    | 2026-06-10 | 9999-12-31 | TRUE
```
Sales before June stay in Espoo/Silver. Sales after June go to Tampere/Gold. Use when history matters for analysis: segment, region, product category.

**Type 3: add a column.** Keep the current and the previous value in two columns.

```text
customer_id | city_current | city_previous
C-1001      | Tampere      | Espoo
```
Only one step of history. Rare. Used for "compare under the old and new territory structure" during a reorganization.

**Type 4: separate history table / mini-dimension.** The term is used two ways, so say which you mean. (a) Kimball: move fast-changing attributes into a **mini-dimension** (section 4.5). (b) Common elsewhere: keep the main dimension as Type 1 (current only) and store all versions in a separate `dim_customer_history` table. Both reduce the size and churn of the main dimension.

**Type 5: mini-dimension + Type 1 outrigger.** The base dimension carries a Type 1 reference to the *current* mini-dimension row, so you can see the current profile without going through the fact. Rarely asked. Know the name.

**Type 6: 1 + 2 + 3 hybrid.** Type 2 rows, plus a "current value" column that is overwritten (Type 1) on **all** rows of that customer.

```text
customer_sk | customer_id | city (historical) | city_current | valid_from | valid_to   | is_current
101         | C-1001      | Espoo             | Tampere      | 2026-01-05 | 2026-06-10 | FALSE
102         | C-1001      | Tampere           | Tampere      | 2026-06-10 | 9999-12-31 | TRUE
```
Now one join gives both "as was" (`city`) and "as is" (`city_current`). This is very practical for Looker: expose two fields, "Customer City (at time of sale)" and "Customer City (current)".

| Type | History? | Storage | Typical use |
|---|---|---|---|
| 0 | Fixed at first value | 1 row | Original dates/channels |
| 1 | No | 1 row | Corrections, non-analysed attributes |
| 2 | Full | 1 row per version | Segment, category, region |
| 3 | One previous value | 1 row, extra column | Reorganizations |
| 4 | In another table / mini-dim | Split | Fast-changing attributes |
| 6 | Full + current | 1 row per version + current column | "As was" and "as is" side by side |

### 5.2 A full SCD2 walkthrough

**Setup.** `dim_product` tracks `category` as SCD2 and `product_name` as SCD1 (name fixes overwrite all versions).

**Day 1 (2026-03-01): initial load.**

```text
product_sk | product_id | product_name | category  | attr_hash | valid_from          | valid_to            | is_current
-1         | UNKNOWN    | Unknown      | Unknown   | -         | 1900-01-01 00:00:00 | 9999-12-31 00:00:00 | TRUE
5001       | P-10       | Oat drink 1L | Dairy     | h1        | 2026-03-01 00:00:00 | 9999-12-31 00:00:00 | TRUE
5002       | P-11       | Rye bread    | Bakery    | h2        | 2026-03-01 00:00:00 | 9999-12-31 00:00:00 | TRUE
```

**Day 2 (2026-05-15 08:30, source commit time): P-10 moves from Dairy to Plant-based.**

The incoming staging row has a different `attr_hash` (the hash of the SCD2 columns). The load does two things in one transaction:

1. **Close** the current row: `valid_to = 2026-05-15 08:30`, `is_current = FALSE`.
2. **Insert** a new row with a new surrogate key, `valid_from = 2026-05-15 08:30`.

```text
product_sk | product_id | product_name | category    | valid_from       | valid_to         | is_current
5001       | P-10       | Oat drink 1L | Dairy       | 2026-03-01 00:00 | 2026-05-15 08:30 | FALSE
5003       | P-10       | Oat drink 1L | Plant-based | 2026-05-15 08:30 | 9999-12-31 00:00 | TRUE
5002       | P-11       | Rye bread    | Bakery      | 2026-03-01 00:00 | 9999-12-31 00:00 | TRUE
```

**Day 3: the name is corrected to "Oat drink 1 L" (SCD1 attribute).** Update `product_name` on **both** P-10 rows. No new version, because the SCD2 hash didn't change.

**Rules that make SCD2 correct:**

- Use a **half-open interval**: `valid_from <= t < valid_to`. Then each moment matches exactly one version. Never use `BETWEEN` (both ends inclusive, so the boundary moment matches two rows).
- Use the **source change timestamp** from CDC (e.g. Datastream `source_timestamp`) for `valid_from`, not the load time. Load time is when *you* saw it, not when it changed.
- Use a far-future `valid_to` (`9999-12-31`) instead of `NULL`, so range joins are simple.
- Compare a **hash of the tracked columns** (`attr_hash`) instead of comparing columns one by one. Handle `NULL`s inside the hash (`IFNULL(col, '')` and a separator) so `NULL → 'x'` is detected.
- If one batch contains **several changes for the same key** (it moved twice in 15 minutes), process them in order (window functions with `LEAD` over `source_timestamp`), or you lose intermediate versions.
- Tests: exactly one `is_current = TRUE` per natural key; no overlapping intervals per key.

**In practice** you don't hand-write this MERGE. Use **dbt snapshots** or a Dataform incremental pattern. The full hand-written MERGE is in the [data warehouse modelling deep dive](../deep-dives/dwh-modelling.md) (section 3). Understand how it works: one branch closes changed rows, another inserts new versions and new keys.

A useful alternative when you have full CDC history in raw: **rebuild SCD2 from the change log** with window functions. It's idempotent and easy to reason about:

```sql
-- Build SCD2 versions from a change log (one row per source change)
WITH changes AS (
  SELECT
    product_id,
    product_name,
    category,
    TO_HEX(MD5(CONCAT(IFNULL(category, ''), '|'))) AS attr_hash,
    source_ts
  FROM stg_sales.product_changes
),
dedup AS (   -- keep only rows where the tracked attributes actually changed
  SELECT *
  FROM changes
  WHERE TRUE   -- older BigQuery versions required WHERE/GROUP BY/HAVING with QUALIFY
  QUALIFY attr_hash != IFNULL(LAG(attr_hash) OVER (PARTITION BY product_id ORDER BY source_ts), '#')
)
SELECT
  FARM_FINGERPRINT(CONCAT(product_id, '|', CAST(source_ts AS STRING))) AS product_sk,
  product_id,
  category,            -- SCD1 attributes like product_name: join the current value separately
  source_ts AS valid_from,
  IFNULL(LEAD(source_ts) OVER (PARTITION BY product_id ORDER BY source_ts),
         TIMESTAMP '9999-12-31') AS valid_to,
  LEAD(source_ts) OVER (PARTITION BY product_id ORDER BY source_ts) IS NULL AS is_current
FROM dedup;
```

For a few million product or customer versions, a full rebuild like this is cheap in BigQuery.

### 5.3 How facts join to SCD2 dimensions

There are two strategies. Know both and their trade-offs.

**Strategy A: look up the surrogate key at load time (classic Kimball).** When a sale is loaded, find the dimension version valid at `order_ts` and store its `product_sk` in the fact.

```sql
SELECT f.*, IFNULL(d.product_sk, -1) AS product_sk
FROM stg_sales.order_lines f
LEFT JOIN mart_sales.dim_product d
  ON  d.product_id = f.product_id
  AND f.order_ts >= d.valid_from
  AND f.order_ts <  d.valid_to;
```

- Pro: queries are a simple equi-join `fact.product_sk = dim.product_sk`. Fast and easy for Looker.
- Con: if the dimension history is corrected later (a late change, a backdated reorganization), you must **re-key** the affected fact rows. Late-arriving dimension rows need special handling.

**Strategy B: store the natural key and join on time range at query time.**

```sql
SELECT ...
FROM mart_sales.fct_sales_line f
JOIN mart_sales.dim_product d
  ON  d.product_id = f.product_id
  AND f.order_ts >= d.valid_from AND f.order_ts < d.valid_to
```

- Pro: the fact never needs re-keying. Corrections to dimension history apply automatically.
- Con: a range join on every query is more expensive and more complex in LookML. Always include the **equality on the natural key first** so BigQuery can hash-join on it and then filter the range.

**Common hybrid in BigQuery:** store **both** the natural key (`product_id`) and the surrogate key (`product_sk`) in the fact. Join "as was" with `product_sk`, join "as is" with `product_id` to the current row (`is_current = TRUE`). That gives Looker both views cheaply.

---

## 6. Keys in BigQuery

### Natural keys
The business identifier from the source (`order_id`, `product_code`). Simple, readable, traceable. Risks: they can be reused, change format, or **collide across sources** (POS order 123 and web order 123).

### Surrogate keys
A warehouse-owned key. In classic databases this is an auto-increment sequence. BigQuery has no classic sequences, and a `ROW_NUMBER()`-based key changes every time you rebuild. So in BigQuery we use **deterministic hash keys**:

```sql
-- INT64, compact, fast to join; deterministic
FARM_FINGERPRINT(CONCAT(source_system, '|', order_id, '|', CAST(line_no AS STRING))) AS sales_line_sk

-- STRING hex, portable across platforms (same MD5 everywhere), larger
TO_HEX(MD5(CONCAT(source_system, '|', order_id, '|', CAST(line_no AS STRING)))) AS sales_line_hk
```

**Why deterministic matters.** If you rebuild a table, reprocess a partition, or re-run a failed job, the same input must produce the **same key**. Otherwise facts point to keys that no longer exist, and reruns create duplicates. Deterministic keys make loads **idempotent**, which also makes Airflow retries and reruns safe.

**Why not `GENERATE_UUID()`.** It returns a new random value every call. Rebuild the dimension and every key changes, so every fact referencing it breaks. Use UUIDs only for things you never rebuild (e.g. a run ID).

**Practical rules:**

- Include the **source system** in the hash to avoid cross-source collisions.
- Use a **separator** (`'|'`) so `('ab','c')` and `('a','bc')` don't produce the same string.
- Wrap nullable parts in `IFNULL(..., '')` (CONCAT returns `NULL` if any part is `NULL`).
- `FARM_FINGERPRINT` is 64-bit. The collision risk is negligible for millions to low billions of keys, but it is not zero. MD5 (128-bit) is safer for huge key spaces and is the Data Vault convention.

### PK/FK constraints: `NOT ENFORCED`

```sql
ALTER TABLE mart_sales.dim_product ADD PRIMARY KEY (product_sk) NOT ENFORCED;
ALTER TABLE mart_sales.fct_sales_line
  ADD CONSTRAINT fk_product FOREIGN KEY (product_sk)
  REFERENCES mart_sales.dim_product (product_sk) NOT ENFORCED;
```

- BigQuery **does not check** these constraints. Duplicates and orphans are still possible.
- They are **metadata**: they document the model, and the optimizer can use them (for example, to remove a join that doesn't change the result). If the data violates them, results can be **wrong**, so you must test uniqueness and referential integrity yourself.
- Datastream-created CDC tables use a primary key so BigQuery can apply upserts.

---

## 7. How BigQuery changes modelling choices

### What's different about BigQuery

| Property | Consequence for modelling |
|---|---|
| **Columnar storage**: a query reads only the columns it names | Wide tables are fine. Unused columns cost nothing at query time |
| **Storage is cheap** (and older data gets the cheaper long-term rate) | Duplication (denormalization) is cheap |
| **Pricing by bytes scanned or slot time** | Partition pruning and clustering matter more than indexes (there are no traditional indexes) |
| **Joins and shuffles are expensive** at scale: big-to-big joins move data between workers | Avoid many large joins at query time; small dimension joins are cheap (broadcast) |
| **DML rewrites files** | Prefer append and partition overwrite; batch MERGEs; avoid row-by-row updates |
| **Nested and repeated fields** (STRUCT, ARRAY) are first-class | Parent-child data can be stored together without a join |

Important nuance: joining a big fact to **small** dimensions (a few thousand stores, a few hundred thousand products) is cheap. BigQuery broadcasts the small table to every worker. The expensive case is **big-to-big** joins (fact to fact, or fact to a huge customer dimension), which need a shuffle. So a star schema with modest dimensions is perfectly fine in BigQuery.

### Three options

**1. Star schema.** Fact + dimensions.

- Pros: flexible, conformed, SCD history works naturally, small storage, natural for Looker explores (one explore = fact + joined dims).
- Cons: joins at query time; more objects for users.
- Use as: the **source of truth** in the mart layer.

**2. One Big Table (OBT), wide and denormalized.** Fact columns plus all needed dimension attributes copied in.

```text
obt_sales_line: order_id, line_no, order_ts, store_name, store_region, product_name,
                category, brand, customer_segment, channel, qty, net_amount, ...
```

- Pros: no joins, fastest queries, very simple for users and for **LLM-based tools** (one table, obvious columns).
- Cons: when a dimension attribute changes (Type 1), you must rewrite many fact rows. History logic is frozen into the table. Duplication of strings increases bytes scanned when those columns are read.
- Use for: a specific hot use case on top of the star (a "today's sales" dashboard, a conversational BI explore), rebuilt from the star.

**3. Nested and repeated fields.** Parent with an array of children.

```sql
CREATE TABLE mart_sales.orders_nested (
  order_id STRING,
  order_ts TIMESTAMP,
  store_id STRING,
  lines ARRAY<STRUCT<line_no INT64, product_id STRING, qty INT64, net_amount NUMERIC>>
)
PARTITION BY DATE(order_ts);

-- basket-level: no join, no UNNEST
SELECT order_id, ARRAY_LENGTH(lines) AS line_count FROM mart_sales.orders_nested;

-- line-level: UNNEST
SELECT o.store_id, l.product_id, SUM(l.net_amount)
FROM mart_sales.orders_nested o, UNNEST(o.lines) l
GROUP BY 1, 2;
```

- Pros: header and lines are stored together, so no join between them. Great for basket analysis and for event payloads.
- Cons: every line-level query needs `UNNEST`. BI tools handle it with more effort (Looker can, with special join patterns). Updating one line rewrites the whole order row. Partitioning and clustering only work on top-level columns.
- Use for: raw/staging of JSON payloads, device or event readings, orders where you mostly analyse baskets.

| | Star | OBT | Nested |
|---|---|---|---|
| Query speed | Good (small dims broadcast) | Best | Good, `UNNEST` for children |
| Flexibility for new questions | Best | Low | Medium |
| SCD history | Natural | Frozen at build time | Hard |
| Maintenance on dim change | Change 1 dim row | Rewrite many fact rows | Rewrite parents |
| Looker friendliness | Native | Simple | Needs care |
| Conversational BI | Good with a semantic layer | Very simple | Harder |

**Summary:** *"I keep a Kimball star as the source of truth, because it's flexible and handles history. Small dimension joins are cheap in BigQuery because they're broadcast. For hot paths, like today's sales or a conversational BI explore, I build denormalized tables or aggregates from the star. Storage is cheap; repeated joins and wide scans are what cost money."*

### Partitioning and clustering as modelling decisions

- **Partition** the fact by the date users filter on most (`order_date`). Daily partitions are usual; it gives pruning and cheap partition overwrite.
- **Cluster** by the columns most used in filters and joins (`store_id`, `product_id`), up to four columns.
- Set `require_partition_filter = TRUE` on large facts so nobody scans 6 years by accident. In Looker, pair it with `always_filter` (see [Looker and the semantic layer](looker-semantic-layer.md)).
- See [BigQuery](bigquery.md) for the storage internals.

---

## 8. Incremental modelling

Rebuilding a 5-year fact every 15 minutes is slow and expensive. **Incremental** models process only new or changed data. The hard parts are: knowing what changed, handling data that arrives late, and staying **idempotent** (running twice gives the same result).

### 8.1 Strategies

**Append-only.** Insert rows newer than the last processed point. For immutable events (clickstream, device readings). Simple and cheap. Fails if events can change or arrive late, unless you deduplicate.

**MERGE on key (upsert).** Match on the grain key; update existing rows, insert new ones. For mutable entities (order lines change status, quantities are edited).

```sql
MERGE mart_sales.fct_sales_line T
USING (
  SELECT * FROM stg_sales.order_lines
  WHERE source_ts >= @watermark_start               -- only changed rows
  QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id, line_no ORDER BY source_ts DESC) = 1
) S
ON  T.order_id = S.order_id AND T.line_no = S.line_no
AND T.order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)   -- prune the target
WHEN MATCHED AND S.source_ts > T._source_ts THEN UPDATE SET
  qty = S.qty, net_amount = S.net_amount, order_status = S.order_status,
  _source_ts = S.source_ts, _loaded_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT ROW;
```
Notes: the `QUALIFY` makes the source unique per key (MERGE fails if several source rows match one target row). The `S.source_ts > T._source_ts` condition makes replays safe. The target date filter limits how much of the fact is scanned, but it assumes no update is older than 7 days; if that's not guaranteed, see 8.4. `INSERT ROW` needs matching column names and order.

**Insert-overwrite partitions.** Recompute the affected partitions completely and replace them. Often the cheapest and simplest on BigQuery: there's no row-by-row key matching, only the affected partitions are rewritten, and the result is idempotent by construction.

**Rolling lookback window.** Each run reprocesses the last N days (or hours), so late data and edits inside that window are absorbed. Combine it with insert-overwrite or MERGE.

| Strategy | Idempotent? | Handles updates? | Handles late data? | Cost |
|---|---|---|---|---|
| Append | Only with dedup | No | No (unless dedup) | Lowest |
| MERGE | Yes, if source deduped and ordered | Yes | Yes, within the scanned window | Medium |
| Insert-overwrite partitions | Yes | Yes (recompute) | Yes, within the window | Low–medium |
| Lookback window | Yes | Yes | Yes, up to N days | Grows with N |

### 8.2 Watermarks

A **watermark** remembers "how far have I processed?". Two important rules:

1. **Base it on source commit time, not load time.** Datastream writes `datastream_metadata.source_timestamp` (when the change happened in the source). Ingestion time can be out of order with source order during backfills or retries. For correctness, use the source time, and keep load time for monitoring.
2. **Overlap the window.** Process from `watermark - 30 minutes`, not exactly from the watermark. Rows can be committed with slightly older timestamps after you read (long transactions, replication delay). The overlap re-reads some rows; the idempotent MERGE absorbs them.

```sql
DECLARE watermark_start TIMESTAMP DEFAULT (
  SELECT TIMESTAMP_SUB(MAX(_source_ts), INTERVAL 30 MINUTE) FROM mart_sales.fct_sales_line
  WHERE order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
);
-- use watermark_start as a constant in the following statements
```
Compute the watermark **once into a variable**. Then later filters compare to a constant, and BigQuery can prune partitions reliably. A subquery inside the filter may not prune.

In Spark or Fabric this is the same idea as a checkpoint or a high-water mark in a control table.

### 8.3 Late-arriving facts and dimensions

**Late-arriving fact.** A sale from yesterday arrives today (a store was offline). With a lookback window that covers it, it's simply included when yesterday's partition is reprocessed. If it's later than the window, you need a backfill job for that date (parameterized by date, idempotent).

**Late-arriving dimension (early-arriving fact).** A sale references `product_id = P-99`, but P-99 isn't in `dim_product` yet (the product master is loaded later). Options:

1. **Unknown member.** Set `product_sk = -1`. The row is not lost; totals are right. Later, re-key the fact when P-99 arrives. Simple, but category reports show "Unknown" until reprocessed.
2. **Inferred member (preferred).** Insert a placeholder row into `dim_product`: `product_id = P-99`, attributes `'Unknown'`, `is_inferred = TRUE`. The fact gets a real key immediately. When the real product data arrives, **update that row in place** (treat the first real values as a Type 1 fill, not a new SCD2 version), and set `is_inferred = FALSE`.

**Never drop the fact row.** Dropping it makes totals wrong and hides the problem. Monitor the number of unknown/inferred members as a data quality metric.

### 8.4 Reprocessing closed periods

Finance "closes" a month, but sometimes a source still changes data in that month (a return booked against an old order, a correction). Decide with the business:

- **Restate**: reprocess the old partitions. Reports for that month change. Fine for operational reporting.
- **Adjust**: keep the closed month fixed; book the correction as an adjustment row in the current period. Needed for financial reporting.

Technically, make backfills easy: Dataform/dbt models with a date-range parameter, insert-overwrite by partition, and a Composer DAG that can run for any date range safely.

---

## 9. What matters most in practice

### Critical 1: Always define the grain first

In any design, the first decision after the requirements is the grain. "The core fact is `fct_sales_line`, one row per order line in its current state, unique on source system, order ID and line number." It prevents many downstream bugs.

### Critical 2: Additive facts + measures defined once
Store additive components (qty, gross, discount, net, cost, tax). Compute ratios (margin %, AOV, return rate) in **one place**: the LookML measure. This is what makes conversational BI give the same answer as the dashboard.

### Critical 3: SCD choices are business decisions
Ask: "Should last year's sales show under the store's region then or now?" Then choose per attribute. Default for the sales model: product category and customer segment as SCD2 (with a Type 6 "current" column), store region SCD2 if reorganizations happen, names SCD1.

### Critical 4: Deterministic keys and idempotent loads
Hash keys from business keys + source system. Every incremental model can be rerun for any window and give the same result. This directly improves the platform's "stability" metric.

### Critical 5: Incremental near real-time on CDC
Datastream CDC → BigQuery raw tables → Dataform incremental models every ~15 minutes with a lookback, watermark on `source_timestamp`, MERGE or partition overwrite. Know why each piece is there.

### Critical 6: BigQuery-aware physical design
Partition by `order_date`, cluster by `store_id, product_id`, `require_partition_filter`, small dims broadcast, OBT/aggregates for hot paths, MVs or incremental aggregate tables.

### Critical 7: Freshness is a measured SLA
"Near real-time" must become a number per use case ("today's sales: ≤ 15 minutes behind source, 99% of the time during opening hours"), measured as `now − MAX(source_ts)` in the mart, alerted on breach, and visible in Looker ("Data as of 10:45").

### Critical 8: Data quality is part of the model
Grain uniqueness, not-null keys, referential integrity, accepted values, freshness, volume anomaly, reconciliation to source. Blocking tests for critical marts.

### Critical 9: The model is the foundation for conversational BI
An LLM can only answer correctly if the model is unambiguous: clear grain, conformed dimensions, a rich date dimension (fiscal periods, ISO weeks, holidays, same-day-last-year), descriptive names, and one definition per metric.

---

## 10. Worked design: the near real-time retail sales model

This is a complete design, step by step.

### Step 1: Requirements questions (ask before drawing)

| Question | Why it matters |
|---|---|
| Which sources? POS per store, web shop, ERP? Which are databases with CDC support? | Ingestion design (Datastream vs. files vs. API) |
| How fresh, for which users? Store managers "today" vs. finance monthly | Freshness SLA per use case; cost |
| Which KPIs and who owns their definitions? Net sales incl./excl. VAT? Returns netted? | Measure definitions, semantic layer |
| Which dimensions and hierarchies? Store → region → country; product → subcategory → category → brand | Dimension design |
| History: report by category at the time of sale or now? | SCD1 vs. SCD2 |
| Multiple currencies? Which FX rate? | Conversion at transaction date |
| How late can data arrive? Can orders change after creation? For how long? | Lookback window size |
| Volume? Orders/day, lines/day, history years | Partitioning, cost estimate |
| Who will use conversational BI, and which questions? | The evaluation set, explore design |

Assume for the exercise: 400 stores + web, ~2 M order lines/day, 5 years of history, orders editable for 3 days, returns up to 30 days later, freshness target 15 minutes for "today".

### Step 2: Architecture

```text
 Sources                   Ingestion (shared ingestion project)       Warehouse (BigQuery)
 ───────                   ────────────────────────────────────       ───────────────────
 POS DB (PostgreSQL) ──┐
 Web shop DB (MySQL) ──┼──► Datastream (backfill + CDC) ─────────────► raw_pos.*, raw_web.*
 ERP product master  ──┘    writes with upsert, max_staleness ~ 5–15 min   (CDC tables with PK,
                                                                           datastream_metadata.source_timestamp)
 FX rates (API/file) ─────► Composer task / Cloud Run job ───────────► raw_fx.rates_daily
                                                                            │
          Composer (every 15 min) triggers Dataform ────────────────────────┤
                                                                            ▼
                                                     stg_sales.order_lines   typed, deduped, unified (channel)
                                                     stg_sales.products, stores, customers
                                                                            ▼
                                                     mart_sales.dim_date, dim_time_of_day (static)
                                                     mart_sales.dim_store (SCD2), dim_product (SCD2+6),
                                                     dim_customer (SCD2), dim_channel, dim_promotion
                                                     mart_sales.fct_sales_line   (incremental MERGE, 3-day lookback)
                                                     mart_sales.fct_returns_line (or negative lines, see below)
                                                     mart_sales.agg_sales_15min  (incremental, today + lookback)
                                                     mart_sales.freshness_log    (per run)
                                                                            ▼
                                                     Looker: views, explores, measures, datagroup on _loaded_at
                                                                            ▼
                                                     Dashboards + conversational BI on the semantic layer
```

Why these pieces:

- **Datastream** gives low-latency CDC without custom code, plus backfill. The BigQuery `max_staleness` setting trades freshness against merge cost (see [Datastream and CDC](datastream-cdc.md)).
- **Dataform** keeps all SQL transformations version-controlled and tested inside BigQuery. **Composer** handles scheduling, dependencies and alerts across systems.
- A 15-minute micro-batch is usually enough for "near real-time" sales and is far simpler than a streaming Dataflow pipeline. If someone needs seconds, discuss Dataflow and streaming inserts, and the cost.

### Step 3: The fact table

**Grain:** one row per order line, current state, unique on `(source_system, order_id, line_no)`.

| Column | Type | Notes |
|---|---|---|
| `sales_line_sk` | INT64 | `FARM_FINGERPRINT(source_system|order_id|line_no)` |
| `source_system` | STRING | 'POS', 'WEB' |
| `order_id` | STRING | Degenerate dimension (basket) |
| `line_no` | INT64 | |
| `order_ts` | TIMESTAMP | In UTC; local time derived via `dim_store.timezone` if needed |
| `order_date` | DATE | **Partition column** (local business date, agreed with the business) |
| `date_key`, `time_of_day_key` | INT64 | To `dim_date`, `dim_time_of_day` |
| `store_sk`, `product_sk`, `customer_sk`, `promotion_sk`, `channel_sk` | INT64 | SCD2 version keys at time of sale; `-1` unknown, `-2` not applicable |
| `store_id`, `product_id`, `customer_id` | STRING | Natural keys for "as is" joins and debugging |
| `qty` | INT64 | Additive. Negative for return lines |
| `gross_amount`, `discount_amount`, `net_amount`, `tax_amount`, `cost_amount` | NUMERIC | Additive, in transaction currency |
| `currency`, `net_amount_eur`, `cost_amount_eur` | | Converted at the order-date FX rate |
| `line_type` | STRING | 'SALE', 'RETURN', 'SHIPPING' (allocated) |
| `order_status` | STRING | Current status |
| `is_cancelled` | BOOL | Cancelled lines excluded by the standard measure |
| `_source_ts` | TIMESTAMP | Source commit time of the latest change (from CDC) |
| `_loaded_at` | TIMESTAMP | When this row was written |
| `_run_id` | STRING | Lineage to the pipeline run |

Physical: `PARTITION BY order_date CLUSTER BY store_id, product_id`, `require_partition_filter = TRUE`, PK `NOT ENFORCED` on `sales_line_sk`.

**Returns decision.** Two valid options. Choose one and document why.

- *Negative lines in the same fact* (a return is a line with negative qty and amount, `line_type = 'RETURN'`, dated on the return date). Net sales = simple sum. Simple for users and AI.
- *Separate `fct_returns_line`*, with a link to the original order line. Better for return analysis (reason codes, days to return).
- A practical choice: negative lines in the sales fact for net sales, **plus** a returns fact with return-specific attributes. Both share conformed dimensions.

### Step 4: Dimensions and SCD choices

| Dimension | Key attributes | SCD | Reason |
|---|---|---|---|
| `dim_date` | date, ISO week, fiscal week/month/quarter/year, weekday, holiday flag (Finnish holidays), same-day-last-year date, is_trading_day | Static (0) | Needed for period comparisons and conversational questions |
| `dim_time_of_day` | 15-min bucket, hour, day part (morning/lunch/evening) | Static | Intraday analysis |
| `dim_store` | name, city, region, country, format, opening date, timezone, size | SCD2 on region/format; SCD1 on name | Reorganizations change region; history matters |
| `dim_product` | name, brand, subcategory, category, department, is_private_label | SCD2 on category hierarchy + Type 6 `category_current`; SCD1 on name | Category moves are common |
| `dim_customer` | segment, loyalty tier, home city, age band (mini-dim?) | SCD2 on segment/tier; personal data minimized | GDPR: keep PII out of marts or protect with policy tags |
| `dim_channel` | store, web, app, click-and-collect | Type 1 | Small, stable |
| `dim_promotion` | promo name, type, start/end | Type 1 | Plus `-2 = No promotion` |

Every dimension has an **unknown member** row.

### Step 5: The 15-minute aggregate

**Purpose.** "Today so far" dashboards and many conversational questions hit the same small set of aggregates. Reading a pre-aggregated table is much faster and cheaper than scanning the fact for every tile.

**Grain:** one row per `order_date` × 15-min bucket × store × product category (current) × channel.

```sql
-- Dataform incremental: recompute today and yesterday (covers late edits during the day)
SELECT
  order_date,
  TIMESTAMP_SECONDS(DIV(UNIX_SECONDS(order_ts), 900) * 900) AS bucket_15min,   -- floor to 15 minutes
  store_id,
  p.category_current AS category,
  channel_sk,
  SUM(qty)                   AS qty,
  SUM(net_amount_eur)        AS net_sales_eur,
  SUM(cost_amount_eur)       AS cost_eur,
  COUNT(*)                   AS line_count,
  HLL_COUNT.INIT(order_id)   AS orders_sketch      -- re-aggregatable distinct count
FROM mart_sales.fct_sales_line f
JOIN mart_sales.dim_product p ON p.product_sk = f.product_sk
WHERE f.order_date >= DATE_SUB(CURRENT_DATE('Europe/Helsinki'), INTERVAL 1 DAY)
  AND NOT f.is_cancelled
GROUP BY 1, 2, 3, 4, 5;
```

- Only **additive** columns, plus an HLL sketch for distinct orders (`HLL_COUNT.MERGE` at query time). No ratios.
- Written with insert-overwrite of the last two partitions on every run.
- In Looker, expose it so users and the AI keep the **same measure names**. Two options (see [Looker and the semantic layer](looker-semantic-layer.md)): Looker **aggregate awareness**, where Looker builds and maintains its own aggregate table and routes matching queries to it automatically; or a LookML view on this Dataform-built table inside the same explore design. Check current Looker docs on whether aggregate awareness can point at an externally built table.
- Alternative: a BigQuery materialized view. Check the current MV limitations (supported joins and functions) before choosing; an incremental Dataform table is fully under your control.

### Step 6: Freshness SLA and how to measure it

Define the SLA in business terms and measure it in the data, not in the scheduler.

- **SLA:** "During store opening hours, `fct_sales_line` and `agg_sales_15min` are at most 15 minutes behind the source system, for 99% of 5-minute checks."
- **Measured as:** `freshness_lag = CURRENT_TIMESTAMP() - MAX(_source_ts)` per source system (a quiet store at 6 a.m. isn't late, so compare per source, or use a heartbeat table in the source that updates every minute).
- **End-to-end latency breakdown:** source commit → Datastream lands in raw (Datastream lag metrics) → `max_staleness` → Dataform run → Looker cache. Log each run's start, end, rows and watermark in `freshness_log`. This also produces the key platform metrics: load duration, throughput, stability.
- **Alerting:** Cloud Monitoring alert or a Composer check when lag > 20 minutes.
- **Visibility:** a Looker field "Data as of" so users (and the AI) can say "as of 10:45".

The latency budget for 15 minutes: Datastream ~1–2 min + staleness ≤ 5 min + Dataform run every 5–10 min (taking ~2–3 min) + Looker cache invalidated by datagroup. Note that a shorter target costs more (more frequent merges and runs).

### Step 7: Data quality tests

| Test | Example | Blocking? |
|---|---|---|
| Grain uniqueness | `sales_line_sk` unique in the fact | Yes |
| Not-null keys | All `*_sk` not null (unknown members used instead) | Yes |
| Referential integrity | Every `product_sk` exists in `dim_product` | Yes |
| SCD2 integrity | One current row per natural key; no overlapping intervals | Yes |
| Accepted values | `line_type IN ('SALE','RETURN','SHIPPING')` | Yes |
| Business rule | `net_amount = gross_amount - discount_amount` (within rounding) | Warn |
| Freshness | Lag < 20 min in opening hours | Alert |
| Volume anomaly | Lines this hour vs. the same hour last 4 same weekdays ± 40% | Alert |
| Unknown members | Share of `product_sk = -1` < 0.1% | Alert |
| Reconciliation | Daily net sales per store = source system totals | Yes, for the daily close |

Implement them as **Dataform assertions** or **dbt tests**. Blocking tests stop the downstream publish (the aggregate and the Looker datagroup don't refresh), so users see slightly old but correct data instead of fresh wrong data. Make that trade-off explicit to the business.

### Step 8: How it serves Looker and conversational BI

- One **Looker explore** `sales` based on `fct_sales_line` with many-to-one joins to the dimensions. Primary keys on every view.
- Measures defined once: `net_sales`, `gross_margin`, `gross_margin_pct`, `units`, `orders`, `average_order_value`, `return_rate`.
- `always_filter` on `order_date` so queries prune partitions.
- A datagroup triggered by `MAX(_loaded_at)` so the cache refreshes when new data lands.
- Aggregate awareness (or a view on `agg_sales_15min`) for fast "today" questions.
- Labels, descriptions and synonyms on every exposed field; technical columns hidden.
- An evaluation set of real business questions with expected answers.

Details in [Looker and the semantic layer](looker-semantic-layer.md).

---

## 11. Data quality as part of modelling

Quality is not a separate project after modelling. **Every modelling decision implies a test**:

| Modelling decision | Implied test |
|---|---|
| "The grain is one row per order line" | Uniqueness of the grain key |
| "Facts always point to a dimension row" (unknown members) | Not null + referential integrity |
| "Category is SCD2" | One current row per key, no overlaps, no gaps |
| "Net = gross − discount" | Row-level business rule |
| "Data is ≤ 15 min old" | Freshness |
| "Sales match the POS" | Reconciliation |

Two more principles:

- **Tests in the pipeline, not after it.** Run assertions as part of the Dataform/dbt run. Fail fast on critical tests; publish to the mart only after they pass (a "write-audit-publish" pattern).
- **Tests are documentation.** For handover, the tests tell the next team what the model promises.

---

## 12. Mapping from Fabric, Spark and AWS

| Fabric / Spark / AWS | In this topic (BigQuery / GCP) | Note |
|---|---|---|
| Medallion bronze/silver/gold in a Fabric lakehouse | `raw_*` / `stg_*` / `mart_*` datasets | Same idea; one dataset per layer per domain |
| Delta Lake `MERGE INTO` | BigQuery `MERGE` | Similar syntax. BigQuery has no Delta log or `VACUUM`; time travel is built in (7 days by default) |
| Delta `OPTIMIZE` / Z-ORDER | Clustering (automatic re-clustering) | No manual compaction jobs |
| Partitioned Delta tables | Partitioned BigQuery tables + `require_partition_filter` | BigQuery partitions on one column only |
| Spark Structured Streaming checkpoints | Watermark in a control table / derived from `MAX(_source_ts)` | Same concept of "processed up to" |
| Power BI semantic model star schema | Kimball mart + LookML model | Power BI also prefers star schemas; the same rules apply |
| Power BI relationships (1:* single direction) | Looker joins with `relationship: many_to_one` | |
| DAX measures | LookML measures | Ratios defined once, in the semantic layer |
| High-volume event table (millions of events per hour, years of history) | A transaction fact, append-only, plus periodic snapshots (15-min aggregates) | Same grain and aggregate rules apply |
| Fabric Direct Lake | Looker queries BigQuery directly (no import) | See [Looker and the semantic layer](looker-semantic-layer.md) |
| Kinesis/Firehose → S3 → Spark | Datastream (for databases) or Pub/Sub → Dataflow → BigQuery | CDC is not streaming windows |

**Example: the same principles on event data.** A ShopFlow clickstream table has a grain of one event per session per timestamp. Dashboards use a 15-minute aggregate per product and channel. Product attributes like category change rarely but need history for category KPIs, so `dim_product` is modelled as SCD2.

---

## 13. Hands-on exercise

Use the free BigQuery sandbox and the public dataset `bigquery-public-data.thelook_ecommerce` (tables `orders`, `order_items`, `products`, `users`). Create your own dataset, e.g. `my_sales`.

**Task 1: Declare the grain.** Look at `order_items`. Write the grain in one sentence. Check it:

```sql
SELECT id, COUNT(*) c FROM `bigquery-public-data.thelook_ecommerce.order_items`
GROUP BY id HAVING c > 1;   -- expect no rows
```

**Task 2: Build the fact.**

```sql
CREATE OR REPLACE TABLE my_sales.fct_sales_line
PARTITION BY order_date
CLUSTER BY product_id
AS
SELECT
  FARM_FINGERPRINT(CONCAT('THELOOK|', CAST(oi.id AS STRING))) AS sales_line_sk,
  oi.order_id,
  oi.id AS order_item_id,
  oi.product_id,
  oi.user_id,
  DATE(oi.created_at) AS order_date,
  oi.created_at AS order_ts,
  oi.status,
  oi.sale_price AS net_amount,
  p.cost AS cost_amount
FROM `bigquery-public-data.thelook_ecommerce.order_items` oi
LEFT JOIN `bigquery-public-data.thelook_ecommerce.products` p ON p.id = oi.product_id;
```

**Task 3: Build `dim_product` with an unknown member** (`UNION ALL` a row with `product_sk = -1`).

**Task 4: Simulate SCD2.** Create a small table of product category changes (3–4 rows, by hand with `INSERT`), then run the window-function SCD2 build from section 5.2. Join the fact to it with the half-open time range and check that sales before the change show the old category.

**Task 5: Prove the cost of no filter.** Dry-run two queries (the console shows "This query will process X"):

```sql
SELECT SUM(net_amount) FROM my_sales.fct_sales_line;                                   -- full scan
SELECT SUM(net_amount) FROM my_sales.fct_sales_line WHERE order_date = '2024-01-15';   -- one partition
```

**Task 6: Prove idempotency.** Write a MERGE from a small staging table into the fact. Run it twice. Check that row counts and `SUM(net_amount)` are the same after both runs.

**Task 7: Additivity trap.** Compute margin % per category two ways: `AVG(row-level margin %)` vs. `SUM(margin)/SUM(net)`. Note the difference and explain it.

**Design-only extension (no SQL).** Draw the bus matrix for thelook: sales, returns (status = 'Returned'), inventory (`inventory_items`). Which dimensions are conformed?

---

## 14. Glossary

| Term | Meaning |
|---|---|
| **Accumulating snapshot** | Fact with one row per process instance, updated at each milestone |
| **Additive measure** | Can be summed across all dimensions |
| **Atomic grain** | The most detailed level the source provides |
| **Bridge table** | Resolves many-to-many relationships, often with weights |
| **Bus matrix** | Grid of business processes × conformed dimensions |
| **CDC** | Change data capture: reading inserts/updates/deletes from a database log |
| **Conformed dimension** | Dimension shared consistently across multiple facts |
| **Data Vault** | Modelling method with hubs, links and satellites, insert-only |
| **Degenerate dimension** | Dimension key in the fact without its own table (e.g. order_id) |
| **Dimension** | Descriptive context table (product, store, date) |
| **Drill across** | Comparing measures from different facts via shared dimensions |
| **Fact** | Table of measurements/events at a declared grain |
| **Factless fact** | Fact with no measures; records events or coverage |
| **FARM_FINGERPRINT** | BigQuery function returning a deterministic INT64 hash |
| **Grain** | What exactly one row of a fact table represents |
| **Hashdiff / attr_hash** | Hash of attribute columns, used to detect changes |
| **Hub / Link / Satellite** | Data Vault tables: business keys / relationships / attributes with history |
| **Idempotent** | Running the same load twice gives the same result |
| **Inferred member** | Placeholder dimension row created when a fact arrives before its dimension |
| **Insert-overwrite** | Replace whole partitions with recomputed data |
| **Junk dimension** | Small dimension combining low-cardinality flags |
| **Late-arriving fact** | Fact row arriving after its event period was processed |
| **Lookback window** | Reprocessing the last N days on each incremental run |
| **max_staleness** | BigQuery table option for CDC tables: how stale results may be, trading freshness for merge cost |
| **Medallion** | Bronze/silver/gold layering by data quality |
| **Mini-dimension** | Separate small dimension for fast-changing attributes |
| **Natural key** | Business identifier from the source |
| **Non-additive measure** | Cannot be summed (ratios, distinct counts) |
| **NOT ENFORCED** | BigQuery PK/FK constraints: metadata only, not validated |
| **OBT** | One Big Table: wide, denormalized table |
| **Periodic snapshot** | Fact with one row per entity per period |
| **Role-playing dimension** | One dimension used in several roles (order date, ship date) |
| **SCD** | Slowly changing dimension; types 0–6 describe how history is kept |
| **Semi-additive measure** | Summable across some dimensions but not time |
| **Star schema** | A fact table surrounded by denormalized dimensions |
| **Surrogate key** | Warehouse-owned key, independent of source keys |
| **Unknown member** | Default dimension row (e.g. -1) so fact keys are never null |
| **Watermark** | Marker of how far data has been processed |
