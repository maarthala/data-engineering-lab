# Milestone: Gold marts

## Goal
The three answers the business asked for, built **only from Silver**.

## The marts
### `my.tw_gold.daily_sales`
One row per **day** (UTC), **channel** and **store** (empty for web):
`day`, `channel`, `store_code`, `sales` (distinct orders or till sales), `items`, `revenue`.

### `my.tw_gold.customer_360`
One row per **customer** in Silver:
`customer_key`, `name`, `has_card`, `web_revenue`, `store_revenue`, `total_revenue`,
`points_balance`, `last_purchase_utc`, `channels` (`web`, `store`, `both` or `none`).

### `my.tw_gold.loyalty_health`
One row per **day**: `new_members`, `points_earned` (web + store + bonus), `points_redeemed`,
`active_members` (members with any points transaction that day).

??? tip "Hints"
    - `to_date(sold_at_utc)` turns a timestamp into its day.
    - A customer without any sale still needs a row in `customer_360`: start from customers
      and `LEFT JOIN` the sales.
    - `channels`: compare the two revenues. `CASE WHEN … THEN 'both' …`.
    - Returns are negative amounts, so `sum(amount)` already nets them out.

## Self-checks
```python
spark.sql("""SELECT (SELECT sum(revenue) FROM my.tw_gold.daily_sales) AS gold,
                    (SELECT sum(amount)  FROM my.tw_silver.sales)    AS silver""").show()        # G1: equal

spark.sql("""SELECT (SELECT count(*) FROM my.tw_gold.customer_360) AS gold,
                    (SELECT count(*) FROM my.tw_silver.customers) AS silver""").show()           # G2: equal

spark.sql("""SELECT (SELECT sum(total_revenue) FROM my.tw_gold.customer_360) AS by_customer,
                    (SELECT sum(amount) FROM my.tw_silver.sales WHERE customer_key IS NOT NULL) AS known_sales
          """).show()                                                                             # G3: equal

spark.sql("""SELECT count(*) FROM my.tw_gold.daily_sales
             WHERE (channel = 'web') <> (store_code IS NULL)""").show()                         # G4: 0
```

- **G1**: every sale counts once. A join that doubles rows shows up here straight away.
- **G3**: anonymous store sales (no card) belong to no customer, so they're left out on the right.
- **G4**: web rows have no store; store rows always have one.

## Questions to answer in your notes
1. Which store sells the most electronics? Does the answer change if you use local days instead
   of UTC days?
2. Do card members spend more than other customers? How would you show that fairly?
3. Points earned grow every day, but the balance of many members goes down. Why?
