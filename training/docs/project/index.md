# The brief: Tidewell's data platform

## The company
**Tidewell** sells electronics, home goods, office supplies, sportswear and snacks. It sells in
three ways, and each one has its own system, built by a different team in a different year:

| System | Database (on `lab-db`) | Tables | Who uses it |
|---|---|---|---|
| **Web shop** | `shop` | `customers`, `products`, `orders`, `order_items` | online customers |
| **Store tills** (point of sale) | `store` | `stores`, `items`, `sales`, `sale_lines` | five stores: Amsterdam, London, Bengaluru, New York, Singapore |
| **Loyalty card** | `loyalty` | `members`, `points` | card members, online and in store |

All three are **live**: something happens every few seconds. Read them with the login
`reader` / `reader` (read only). 🐘 **My databases** shows them with their tables.

So far nobody can answer simple questions: *How much did we sell yesterday, online and in
stores together? Who are our best customers across all channels? Is the loyalty card worth it?*
Each system has its own numbers, and they don't agree on anything.

**You are Tidewell's first data engineer.** Build the platform that answers those questions.

## The systems disagree
That's the real work. Before you write any code, look at the data and find these yourself:

| | Web shop | Store tills | Loyalty |
|---|---|---|---|
| Product | `product_id` (1, 2, …) | `sku` (`SKU-0001`, …) and its own name and price | — |
| Money | `numeric` in currency | **integer cents** | points (1 per currency unit spent) |
| Time | `timestamptz` (a moment in time) | `sold_at` in the **store's local time, no time zone** | `timestamptz` |
| Customer | `customer_id` + `email` | only a **card number**, and only on about half the sales | `member_id`, `card_number`, an **email typed differently** (case, spaces) |
| Returns, cancellations | a cancelled order is **deleted** | a return is a line with a **negative** `qty` | points come back as a negative transaction |

## What the business wants
1. **One sales view**: every web order line and every till line, in the same currency, in UTC,
   with the product and (where known) the customer.
2. **One customer view**: one row per real person, whether they shop online, in stores or both,
   with their card if they have one.
3. **Three Gold answers**, no more than **15 minutes** behind the systems:
    - **daily sales** by day, channel and store;
    - **customer 360**: per customer, what they spent online and in stores, their points, when
      they last bought;
    - **loyalty health** by day: new members, points earned, points redeemed.
4. A **dashboard** the managers can open, and a pipeline that runs **by itself**.

## The milestones
| # | Milestone | You'll use |
|---|---|---|
| 1 | [Land the three systems](ingest.md) in Bronze, with CDC | 📡 Ingestion |
| 2 | [One company in Silver](silver.md): match products, money, time and customers | Spark (notebook) |
| 3 | [Gold marts](gold.md) | Spark SQL |
| 4 | [Run it, show it, trust it](run.md): schedule, dashboard, quality checks | Airflow, Superset |

There is **no answer key**. The data keeps changing, so your numbers will never match anyone
else's. Instead, each milestone ends with **self-checks**: SQL queries that must return
`0`, or two numbers that must be equal. If every check passes, your platform is right.

## Names to use
Use these names, so the self-checks work as written:

| Layer | Namespace | Tables |
|---|---|---|
| Bronze | `my.tw_web`, `my.tw_store`, `my.tw_loyalty` | as in the systems |
| Silver | `my.tw_silver` | `customers`, `products`, `sales`, `points` |
| Gold | `my.tw_gold` | `daily_sales`, `customer_360`, `loyalty_health` |

!!! tip "Before you start"
    This project builds on [Change data capture](../cdc/what-is-cdc.md) (Ingestion, `_op_type`,
    tombstones), [Spark](../unit4/fundamentals.md), [Airflow](../unit5/basics.md) and
    [Superset](../unit7/dashboards.md). Look back at them when a milestone points there.
