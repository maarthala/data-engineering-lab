# Tidewell: the company and your assignment

## About Tidewell
**Tidewell** is a mid-sized retailer of everyday things: laptops and headphones, desk lamps and
coffee mugs, notebooks, running shoes, tea and chocolate. It opened its first store in New York in
2018, grew to five cities, sells online to customers in ten countries, and runs a loyalty card that
works both online and in its stores.

*(Tidewell is made up for this course. Its data is real, though: it lives in the lab and changes
every few seconds.)*

### At a glance
| | |
|---|---|
| **Sells** | 18 products in 6 categories: electronics, home, office, accessories, sports, food |
| **Online** | a web shop for customers in the US, UK, Germany, India, France, the Netherlands, Singapore, Brazil, Japan and Australia |
| **Stores** | 5 stores (below), paid by card, cash or mobile |
| **Loyalty card** | free to join, online or at a till · tiers blue, silver, gold |
| **Systems** | three, one per channel, built by different teams in different years |

### The stores
| Store | City | Country | Time zone | Opened |
|---|---|---|---|---|
| `NYC-01` | New York | US | America/New_York | 2018-09-01 |
| `AMS-01` | Amsterdam | NL | Europe/Amsterdam | 2019-03-01 |
| `LON-01` | London | UK | Europe/London | 2020-06-15 |
| `BLR-01` | Bengaluru | IN | Asia/Kolkata | 2021-01-10 |
| `SIN-01` | Singapore | SG | Asia/Singapore | 2022-04-20 |

### How the business works
- **Online orders** go *placed → paid → shipped → delivered*. A customer can cancel while the
  order is *placed* or *paid*; the order then disappears from the web shop.
- **In a store**, a customer pays at the till. About half of them show their loyalty card. A
  **return** is booked at the till as a new sale with the item at minus one.
- **Loyalty points:** 1 point per currency unit spent, online (when the card's email is the web
  account's) and in stores (when the card is shown). A cancelled web order takes its points
  back. New members joining at a till get 50 welcome points. Members redeem 100, 200 or 500
  points at a time.
- **Prices** are set separately online and in stores, and they drift apart over time.

### Who's asking
| Person | Wants to know |
|---|---|
| **The CEO** | How much did we sell today, online and in stores together? Are we growing? |
| **Head of Stores** | Which store sells what, and when? Do returns hurt one store more than the others? |
| **E-commerce lead** | How many online customers also shop in our stores? |
| **Loyalty manager** | Is the card worth it? Do members spend more? Are points piling up unused? |
| **Finance** | One revenue number that matches the systems, every day, with returns and cancellations netted out |

## Tidewell's three systems
All three are live databases on `lab-db`, read with the login `reader` / `reader` (read only, and
allowed to replicate for CDC). 🐘 **My databases** and the SQL editor show them as *Tidewell web
shop*, *Tidewell stores* and *Tidewell loyalty*.

| System | Database | Tables |
|---|---|---|
| **Web shop** | `shop` | `customers`, `products`, `orders`, `order_items` |
| **Store tills** (point of sale) | `store` | `stores`, `items`, `sales`, `sale_lines` |
| **Loyalty card** | `loyalty` | `members`, `points` |

They were never meant to work together, and it shows:

| | Web shop | Store tills | Loyalty |
|---|---|---|---|
| Product | `product_id` (1, 2, …) | `sku` (`SKU-0001`, …), its own name and price | — |
| Money | `numeric`, in currency | **integer cents** | points |
| Time | `timestamptz` (a moment in time) | `sold_at` in the **store's local time, no time zone** | `timestamptz` |
| Customer | `customer_id` + `email` | only a **card number**, on about half the sales | `member_id`, `card_number`, an **email typed differently** (case, spaces) |
| Undo | a cancelled order is **deleted** | a return is a line with a **negative** `qty` | a **negative** points transaction |

## The problem
Every team at Tidewell reports its own numbers from its own system. The web shop counts orders in
currency, the stores count tills in cents in five time zones, and the loyalty team can't say who
its members are online. When the CEO asks *"what did we sell yesterday?"*, three people give three
answers, and none of them includes the other two channels. Nobody knows how many customers shop
in both places, so nobody can say whether the loyalty card pays for itself.

Tidewell has hired you as its **first data engineer**. Your assignment:

> Build Tidewell's data platform: bring the three systems together into one trusted, always
> fresh picture of sales, customers and loyalty, at most **15 minutes** behind the tills and the
> web shop, and put it in front of the people who ask.

Concretely, when you're done:

1. **One sales view**: every web order line and every till line, in one currency, in UTC, with
   the product and, where known, the customer.
2. **One customer view**: one row per real person, whether they shop online, in stores or both,
   with their loyalty card if they have one.
3. **Three answers** (Gold): daily sales by channel and store, a 360° view per customer, and the
   loyalty programme's health per day.
4. **A dashboard** the managers open themselves, and a pipeline that runs **by itself** and
   **stops** when the numbers can't be trusted.

## Your tasks
Work through them in order. Each group links to a page with details, hints and **self-checks**.

### ① Land the three systems: [details & checks](ingest.md)
- [ ] **T1 · Explore.** In the SQL editor, open the three Tidewell connections. Note each table's
  key, what one row means, and how it changes (inserts, updates, deletes).
- [ ] **T2 · Sources.** In 📡 Ingestion, add the three systems as sources with CDC on.
- [ ] **T3 · Destinations.** Namespaces `tw_web`, `tw_store`, `tw_loyalty` in your lakehouse.
- [ ] **T4 · Jobs.** One job per system, all tables, *Full load + CDC*, every 15 minutes.
- [ ] **T5 · Check.** Self-checks B1–B3 pass: every table landed, fresh, CDC not reloads.

### ② One company in Silver: [details & checks](silver.md)
- [ ] **T6 · Products.** `tw_silver.products`: web id ↔ store SKU, both prices.
- [ ] **T7 · Customers.** `tw_silver.customers`: web customers and card members matched by
  cleaned email, one row per person, a `customer_key` you make.
- [ ] **T8 · Sales.** `tw_silver.sales`: web and till lines together, amounts in currency
  (returns negative), times in UTC, each with its product and customer.
- [ ] **T9 · Points.** `tw_silver.points`: every points transaction with its customer.
- [ ] **T10 · Check.** Self-checks S1–S7 pass.

### ③ The answers in Gold: [details & checks](gold.md)
- [ ] **T11 · Daily sales.** `tw_gold.daily_sales` by day, channel and store.
- [ ] **T12 · Customer 360.** `tw_gold.customer_360`: spend online and in stores, points, last
  purchase, channels.
- [ ] **T13 · Loyalty health.** `tw_gold.loyalty_health`: new members, points earned and redeemed,
  active members per day.
- [ ] **T14 · Check.** Self-checks G1–G4 pass.

### ④ Run it, show it, trust it: [details](run.md)
- [ ] **T15 · Schedule.** An Airflow DAG rebuilds Silver, then Gold, every 15 minutes.
- [ ] **T16 · Quality gate.** A last task runs the self-checks and a freshness check, and fails
  the run when one doesn't pass.
- [ ] **T17 · Dashboard.** "Tidewell today" in Superset: today's revenue web against stores,
  revenue by store over 7 days, the top 10 customers, points earned against redeemed.
- [ ] **T18 · Hand over.** One page for Tidewell's managers, without code: what the platform does,
  how fresh it is, what the checks protect, and the three most surprising things in the data.
  Answer each milestone's questions in your notes.
- [ ] **T19 · Clean up** when you're finished: pause, then delete jobs, sources, destinations and
  the `tw_*` namespaces.

## Names to use
Use these names so the self-checks work as written:

| Layer | Namespace | Tables |
|---|---|---|
| Bronze | `my.tw_web`, `my.tw_store`, `my.tw_loyalty` | as in the systems |
| Silver | `my.tw_silver` | `customers`, `products`, `sales`, `points` |
| Gold | `my.tw_gold` | `daily_sales`, `customer_360`, `loyalty_health` |

## How you know you're done
There is **no answer key**: the systems keep changing, so your numbers never match anyone else's.
Instead, every self-check passes inside your pipeline, the dashboard shows today's numbers, and
your hand-over page answers the questions in **Who's asking**.

!!! tip "Before you start"
    This project uses what you learned in [Change data capture](../cdc/what-is-cdc.md) (Ingestion,
    `_op_type`, tombstones), [Spark](../unit4/fundamentals.md), [Airflow](../unit5/basics.md) and
    [Superset](../unit7/dashboards.md). Look back at them when a task points there.
