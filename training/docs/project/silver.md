# Milestone: one company in Silver

## Goal
Turn three systems that disagree into one consistent model: one product list, one customer list,
one sales table, in one currency and one time zone. Silver is built from Bronze **without the
tombstones** (`_op_type <> 'd'`) and **without OLake's columns**
([From changes to Silver](../cdc/silver-gold.md) showed how).

## The tables to build
| Table | One row per | Columns (at least) |
|---|---|---|
| `my.tw_silver.products` | web product | `product_id`, `sku`, `name`, `category`, `web_price`, `store_price` |
| `my.tw_silver.customers` | real person | `customer_key`, `web_customer_id`, `member_id`, `card_number`, `email`, `name`, `country` |
| `my.tw_silver.sales` | sold line, web or store | `channel`, `sale_ref`, `line`, `product_id`, `customer_key`, `store_code`, `qty`, `amount`, `sold_at_utc` |
| `my.tw_silver.points` | points transaction | `member_id`, `customer_key`, `channel`, `points`, `created_at` |

## The four problems to solve
### 1 · Products: id against SKU
The tills say `SKU-0007`, the web shop says `7`. Build the SKU from the id, or the id from the SKU.

??? tip "Hint"
    - **`format_string('SKU-%04d', product_id)`** builds `SKU-0007` from `7`: `%04d` means "a
      whole number, 4 digits, padded with zeros".
    - **`CAST(substring(sku, 5) AS int)`** goes the other way: `substring(text, 5)` keeps the text
      from the 5th character on (`0007`), and the cast makes it `7`.

### 2 · Money: cents against currency
`unit_price_cents = 13545` is 135.45. Divide by 100 and store amounts as `decimal(12,2)`, the
same type for both channels. A return has a negative `qty`, so its amount must be negative too.

### 3 · Time: local time against UTC
`sold_at = 18:55` in Amsterdam is **17:55 UTC** in November (and 16:55 in summer). The store's
`timezone` column says which zone each store is in.

??? tip "Hint"
    **`to_utc_timestamp(sold_at, timezone)`** reads `sold_at` as a time in that zone and turns it
    into UTC, summer time included:

    ```
    +------+--------------------------+--------------------------+
    |code  |local_time                |utc                       |
    +------+--------------------------+--------------------------+
    |AMS-01|2026-11-02 18:55:40.885523|2026-11-02 17:55:40.885523|
    +------+--------------------------+--------------------------+
    ```

    Set your session to UTC first, so times print the same for everyone:
    `spark.conf.set("spark.sql.session.timeZone", "UTC")`.

### 4 · Customers: who is who?
A web customer and a loyalty member are the **same person** when their emails match, but only
after you clean them: the loyalty system has `MATEO.WEBER103@EXAMPLE.COM` and
` goran.okafor473@example.com` (with a space). On the live data, an exact match finds 23 people
and a cleaned match finds 53. Some members have no email at all, and some never shopped online.

- One row per person: web customers, card members, and people who are both.
- `customer_key`: a key **you** make, e.g. `web-12` for a web customer (with or without a card),
  `card-40` for a card member who never shopped online.
- A store sale knows only a card number: find its `customer_key` through the card.

??? tip "Hint"
    - Clean both sides the same way: **`lower(trim(email))`**.
    - **`FULL OUTER JOIN`** keeps rows from both sides, matched or not. `coalesce(a, b)` then
      picks whichever side has a value.
    - Members without an email can't match anyone. That's fine: they stay card-only customers.

## Self-checks
Each query must return **0**, or two equal numbers.

```python
checks = {
 "S1 one row per customer":
   "SELECT count(*) - count(DISTINCT customer_key) FROM my.tw_silver.customers",
 "S2 members matched by email":                                           # cleaned emails that still didn't match
   """SELECT count(*) FROM my.tw_silver.customers
      WHERE member_id IS NOT NULL AND web_customer_id IS NULL
        AND email IN (SELECT lower(trim(email)) FROM my.tw_web.customers WHERE _op_type <> 'd')""",
 "S3 every line has a product":
   """SELECT count(*) FROM my.tw_silver.sales s
      LEFT ANTI JOIN my.tw_silver.products p ON p.product_id = s.product_id""",
 "S4 card sales find a customer":
   """SELECT count(*) FROM my.tw_store.sales s
      LEFT ANTI JOIN my.tw_silver.customers c ON c.card_number = s.card_number
      WHERE s._op_type <> 'd' AND s.card_number IS NOT NULL""",
 "S5 no sale in the future":
   "SELECT count(*) FROM my.tw_silver.sales WHERE sold_at_utc > current_timestamp()",
 "S6 returns are negative":
   "SELECT count(*) FROM my.tw_silver.sales WHERE (qty < 0) <> (amount < 0)",
}
for name, q in checks.items():
    print(name, spark.sql(q).first()[0])

spark.sql("""SELECT (SELECT sum(qty * unit_price_cents) FROM my.tw_store.sale_lines WHERE _op_type <> 'd') / 100 AS bronze,
                    (SELECT sum(amount) FROM my.tw_silver.sales WHERE channel = 'store') AS silver""").show()   # S7: equal
```

- **`LEFT ANTI JOIN`** keeps the rows on the left that have **no** match on the right: exactly the
  ones that went wrong.
- **S2** catches an exact email match: members whose cleaned email *is* a web customer's, yet
  stayed unmatched.
- **S7** compares the store money before and after your conversion.

## Questions to answer in your notes
1. How many customers shop **only** online, **only** in stores, and **both**?
2. Why must the key be one *you* make, and not the web `customer_id` or the loyalty `member_id`?
3. Store prices and web prices drift apart. Which price does a sale's `amount` use, and why?
