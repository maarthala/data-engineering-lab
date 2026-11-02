-- ShopFlow OLTP schema + a realistic generated dataset (the live application database,
-- i.e. the source system). Loaded into the stack's Postgres as database `shopflow`.
--
-- Seeds the FULL 12-table schema documented in the course (unit0/schema):
--   core     : customers (6k), products (200), orders (40k), order_items (~100k)
--   support  : suppliers, promotions, inventory, payments, returns,
--              marketing_campaigns, support_tickets, reviews
-- All foreign keys are real and the derived tables (payments, returns, reviews, …) are
-- generated FROM the actual orders/order_items, so joins, margins, refunds, fraud, and
-- review analysis all work against consistent data.
--
-- Re-runnable: it drops and recreates the database from scratch.

DROP DATABASE IF EXISTS shopflow;
CREATE DATABASE shopflow;
\connect shopflow

-- ===========================================================================
-- Schema
-- ===========================================================================

-- Reference tables first (products/orders reference these) -------------------
CREATE TABLE suppliers (
  supplier_id    SERIAL PRIMARY KEY,
  name           TEXT NOT NULL,
  country        TEXT NOT NULL,                     -- where they ship from
  lead_time_days INT  NOT NULL                      -- typical reorder -> restock
);

CREATE TABLE promotions (
  promo_id       SERIAL PRIMARY KEY,
  code           TEXT NOT NULL,                     -- discount code customers enter
  description    TEXT NOT NULL,
  discount_type  TEXT NOT NULL,                     -- percent | fixed
  discount_value NUMERIC(10,2) NOT NULL,            -- 15 (%) or 5.00 (fixed)
  starts_on      DATE NOT NULL,
  ends_on        DATE NOT NULL
);

-- Core tables ---------------------------------------------------------------
CREATE TABLE customers (
  customer_id      SERIAL PRIMARY KEY,
  full_name        TEXT NOT NULL,
  email            TEXT UNIQUE NOT NULL,
  country          TEXT NOT NULL,
  signup_date      DATE NOT NULL,
  loyalty_tier     TEXT NOT NULL DEFAULT 'none',   -- none | silver | gold
  marketing_opt_in BOOLEAN NOT NULL DEFAULT false
);

CREATE TABLE products (
  product_id   SERIAL PRIMARY KEY,
  name         TEXT NOT NULL,
  category     TEXT NOT NULL,
  price        NUMERIC(10,2) NOT NULL,             -- current list price
  cost         NUMERIC(10,2) NOT NULL,             -- what ShopFlow pays the supplier
  supplier_id  INT REFERENCES suppliers(supplier_id),
  updated_at   TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE orders (
  order_id     SERIAL PRIMARY KEY,
  customer_id  INT NOT NULL REFERENCES customers(customer_id),
  channel      TEXT NOT NULL,                      -- web | app | marketplace | wholesale
  order_ts     TIMESTAMP NOT NULL,
  status       TEXT NOT NULL,                      -- placed | shipped | delivered | cancelled
  currency     TEXT NOT NULL DEFAULT 'USD',
  promo_id     INT REFERENCES promotions(promo_id) -- nullable: most orders use no promo
);

CREATE TABLE order_items (
  order_id    INT NOT NULL REFERENCES orders(order_id),
  product_id  INT NOT NULL REFERENCES products(product_id),
  quantity    INT NOT NULL,
  unit_price  NUMERIC(10,2) NOT NULL,              -- price captured at order time
  PRIMARY KEY (order_id, product_id)
);

-- Supporting tables ---------------------------------------------------------
CREATE TABLE inventory (
  product_id       INT PRIMARY KEY REFERENCES products(product_id),
  warehouse        TEXT NOT NULL,
  quantity_on_hand INT NOT NULL,
  reorder_level    INT NOT NULL,
  updated_at       TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE payments (
  payment_id  SERIAL PRIMARY KEY,
  order_id    INT NOT NULL REFERENCES orders(order_id),
  method      TEXT NOT NULL,                       -- card | paypal | bank_transfer
  amount      NUMERIC(10,2) NOT NULL,
  currency    TEXT NOT NULL,
  status      TEXT NOT NULL,                       -- approved | declined | refunded
  fraud_score NUMERIC(3,2) NOT NULL,               -- risk 0..1 assigned at checkout
  paid_ts     TIMESTAMP NOT NULL
);

CREATE TABLE returns (
  return_id     SERIAL PRIMARY KEY,
  order_id      INT NOT NULL REFERENCES orders(order_id),
  product_id    INT NOT NULL REFERENCES products(product_id),
  quantity      INT NOT NULL,
  reason        TEXT NOT NULL,                      -- damaged | wrong_item | changed_mind | ...
  refund_amount NUMERIC(10,2) NOT NULL,
  return_ts     TIMESTAMP NOT NULL,
  status        TEXT NOT NULL                       -- requested | received | refunded
);

CREATE TABLE marketing_campaigns (
  campaign_id SERIAL PRIMARY KEY,
  name        TEXT NOT NULL,
  channel     TEXT NOT NULL,                        -- search | social | email | display
  spend       NUMERIC(12,2) NOT NULL,
  starts_on   DATE NOT NULL,
  ends_on     DATE NOT NULL
);

CREATE TABLE support_tickets (
  ticket_id   SERIAL PRIMARY KEY,
  customer_id INT NOT NULL REFERENCES customers(customer_id),
  order_id    INT REFERENCES orders(order_id),      -- nullable: not every ticket is about an order
  subject     TEXT NOT NULL,
  status      TEXT NOT NULL,                         -- open | pending | resolved
  opened_ts   TIMESTAMP NOT NULL,
  resolved_ts TIMESTAMP,                             -- nullable until resolved
  csat_score  INT                                    -- nullable: 1..5 after resolution
);

CREATE TABLE reviews (
  review_id   SERIAL PRIMARY KEY,
  customer_id INT NOT NULL REFERENCES customers(customer_id),
  product_id  INT NOT NULL REFERENCES products(product_id),
  rating      INT NOT NULL,                          -- 1..5 stars
  body        TEXT NOT NULL,
  created_ts  TIMESTAMP NOT NULL
);

-- ===========================================================================
-- Data generation
-- ===========================================================================

-- 20 suppliers.
INSERT INTO suppliers (name, country, lead_time_days)
SELECT 'Supplier ' || g,
       (ARRAY['CN','US','DE','IN','VN','MX'])[1 + floor(random()*6)::int],
       (ARRAY[3,5,7,10,14,21])[1 + floor(random()*6)::int]
FROM generate_series(1, 20) AS g;

-- 12 promotions (percent or fixed), each with an active window.
INSERT INTO promotions (code, description, discount_type, discount_value, starts_on, ends_on)
SELECT 'PROMO' || g,
       'Promotion ' || g,
       d.dt,
       CASE WHEN d.dt = 'percent' THEN (5 + floor(random()*25))::numeric
            ELSE round((5 + random()*20)::numeric, 2) END,
       s.start_on,
       s.start_on + (14 + floor(random()*31)::int)
FROM generate_series(1, 12) AS g
CROSS JOIN LATERAL (SELECT (ARRAY['percent','percent','fixed'])[1 + floor(random()*3)::int] AS dt) d
CROSS JOIN LATERAL (SELECT DATE '2023-06-01' + (random()*600)::int AS start_on) s;

-- 6,000 customers across several countries, signed up over ~2.5 years.
-- (Many will never place an order — like a real signup base — see the orders step.)
INSERT INTO customers (full_name, email, country, signup_date, loyalty_tier, marketing_opt_in)
SELECT
  'Customer ' || g,
  'customer' || g || '@example.com',
  (ARRAY['US','US','UK','UK','DE','FR','IN','CA','AU'])[1 + floor(random()*9)::int],
  DATE '2023-01-01' + (random()*900)::int,
  (ARRAY['none','none','none','none','silver','silver','gold'])[1 + floor(random()*7)::int],
  (random() < 0.6)
FROM generate_series(1, 6000) AS g;

-- 200 products across categories; cost is 40–70% of price (so margin is learnable);
-- each sourced from one of the 20 suppliers.
INSERT INTO products (name, category, price, cost, supplier_id, updated_at)
SELECT name, category, price,
       round((price * (0.4 + random()*0.3))::numeric, 2) AS cost,
       supplier_id, now()
FROM (
  SELECT 'Product ' || g AS name,
         (ARRAY['Electronics','Furniture','Stationery','Clothing','Home & Kitchen','Sports','Books','Toys'])[1 + floor(random()*8)::int] AS category,
         round((5 + random()*495)::numeric, 2) AS price,
         1 + floor(random()*20)::int AS supplier_id
  FROM generate_series(1, 200) AS g
) t;

-- 40,000 orders over two years; status weighted toward 'delivered'; ~20% use a promo.
-- Customer assignment is deliberately SKEWED (power curve) so the data has a realistic
-- long tail: a few high-volume "whale" customers, many one-time buyers, and some who
-- never order at all — which makes repeat-rate, LEFT JOIN, and cohort analysis meaningful.
INSERT INTO orders (customer_id, channel, order_ts, status, currency, promo_id)
SELECT
  1 + LEAST(floor(-ln(1 - random()) * 500)::int, 5999),
  (ARRAY['web','web','web','app','app','marketplace','wholesale'])[1 + floor(random()*7)::int],
  TIMESTAMP '2023-06-01 00:00:00'
    + (random()*730)::int * INTERVAL '1 day'
    + (random()*86400)::int * INTERVAL '1 second',
  (ARRAY['delivered','delivered','delivered','delivered','delivered','delivered',
         'delivered','shipped','placed','cancelled'])[1 + floor(random()*10)::int],
  (ARRAY['USD','USD','USD','GBP','EUR','INR'])[1 + floor(random()*6)::int],
  CASE WHEN random() < 0.2 THEN 1 + floor(random()*12)::int ELSE NULL END
FROM generate_series(1, 40000) AS g;

-- 1–4 distinct products per order (~100k line items); unit_price = price at order time
INSERT INTO order_items (order_id, product_id, quantity, unit_price)
SELECT o.order_id, p.product_id, 1 + floor(random()*5)::int, p.price
FROM orders o
CROSS JOIN LATERAL (
  SELECT product_id, price
  FROM products
  ORDER BY random()
  LIMIT (1 + (o.order_id % 4))          -- 1–4 distinct products, varies per order
) AS p;

-- Inventory: one stock row per product.
INSERT INTO inventory (product_id, warehouse, quantity_on_hand, reorder_level, updated_at)
SELECT product_id,
       (ARRAY['US-EAST','US-WEST','EU-CENTRAL','APAC'])[1 + floor(random()*4)::int],
       floor(random()*500)::int,
       (ARRAY[20,50,100])[1 + floor(random()*3)::int],
       now() - (random()*30)::int * INTERVAL '1 day'
FROM products;

-- Payments: one per non-cancelled order; amount = the order total; mostly approved,
-- with a fraud_score skewed toward 0 (random()*random()) and a few declined/refunded.
INSERT INTO payments (order_id, method, amount, currency, status, fraud_score, paid_ts)
SELECT o.order_id,
       (ARRAY['card','card','card','paypal','paypal','bank_transfer'])[1 + floor(random()*6)::int],
       t.total,
       o.currency,
       (ARRAY['approved','approved','approved','approved','approved','approved',
              'approved','approved','declined','refunded'])[1 + floor(random()*10)::int],
       round((random()*random())::numeric, 2),
       o.order_ts + (random()*180)::int * INTERVAL '1 minute'
FROM orders o
JOIN (SELECT order_id, sum(quantity * unit_price) AS total
      FROM order_items GROUP BY order_id) t ON t.order_id = o.order_id
WHERE o.status <> 'cancelled';

-- Returns: ~6% of delivered line items come back (whole line for simplicity).
INSERT INTO returns (order_id, product_id, quantity, reason, refund_amount, return_ts, status)
SELECT oi.order_id, oi.product_id, oi.quantity,
       (ARRAY['damaged','wrong_item','changed_mind','not_as_described','late'])[1 + floor(random()*5)::int],
       round((oi.unit_price * oi.quantity)::numeric, 2),
       o.order_ts + (5 + floor(random()*30)::int) * INTERVAL '1 day',
       (ARRAY['requested','received','refunded','refunded'])[1 + floor(random()*4)::int]
FROM order_items oi
JOIN orders o ON o.order_id = oi.order_id
WHERE o.status = 'delivered' AND random() < 0.06;

-- 15 marketing campaigns (no FK to orders — analytics joins them by channel + date).
INSERT INTO marketing_campaigns (name, channel, spend, starts_on, ends_on)
SELECT 'Campaign ' || g,
       (ARRAY['search','social','email','display'])[1 + floor(random()*4)::int],
       round((1000 + random()*49000)::numeric, 2),
       s.start_on,
       s.start_on + (7 + floor(random()*24)::int)
FROM generate_series(1, 15) AS g
CROSS JOIN LATERAL (SELECT DATE '2023-06-01' + (random()*600)::int AS start_on) s;

-- Support tickets: ~6% of orders raise one (customer taken from the order), plus a
-- batch of order-less "general" tickets. Resolved tickets get a resolved_ts + CSAT.
INSERT INTO support_tickets (customer_id, order_id, subject, status, opened_ts, resolved_ts, csat_score)
SELECT o.customer_id, o.order_id,
       (ARRAY['Where is my order?','Item damaged','Refund request','Wrong item received',
              'Change delivery address','Cancel my order'])[1 + floor(random()*6)::int],
       s.status,
       o.order_ts + (1 + floor(random()*10)::int) * INTERVAL '1 day',
       CASE WHEN s.status = 'resolved'
            THEN o.order_ts + (11 + floor(random()*10)::int) * INTERVAL '1 day' END,
       CASE WHEN s.status = 'resolved' THEN 1 + floor(random()*5)::int END
FROM orders o
CROSS JOIN LATERAL (SELECT (ARRAY['open','pending','resolved','resolved','resolved'])[1 + floor(random()*5)::int] AS status) s
WHERE random() < 0.06;

INSERT INTO support_tickets (customer_id, order_id, subject, status, opened_ts, resolved_ts, csat_score)
SELECT 1 + floor(random()*6000)::int, NULL,
       (ARRAY['General question','Account help','Website issue','Feedback'])[1 + floor(random()*4)::int],
       s.status,
       TIMESTAMP '2023-06-01 00:00:00' + (random()*730)::int * INTERVAL '1 day',
       CASE WHEN s.status = 'resolved'
            THEN TIMESTAMP '2023-06-01 00:00:00' + ((random()*730)::int + 5) * INTERVAL '1 day' END,
       CASE WHEN s.status = 'resolved' THEN 1 + floor(random()*5)::int END
FROM generate_series(1, 800) AS g
CROSS JOIN LATERAL (SELECT (ARRAY['open','pending','resolved'])[1 + floor(random()*3)::int] AS status) s;

-- Reviews: ~12% of delivered line items get a review, rating skewed high.
INSERT INTO reviews (customer_id, product_id, rating, body, created_ts)
SELECT o.customer_id, oi.product_id,
       (ARRAY[5,5,5,4,4,3,2,1])[1 + floor(random()*8)::int],
       (ARRAY['Great product, would buy again.','Exactly as described.','Good value for money.',
              'Arrived quickly.','Not what I expected.','Decent but could be better.',
              'Disappointed with the quality.'])[1 + floor(random()*7)::int],
       o.order_ts + (1 + floor(random()*20)::int) * INTERVAL '1 day'
FROM order_items oi
JOIN orders o ON o.order_id = oi.order_id
WHERE o.status = 'delivered' AND random() < 0.12;

-- ===========================================================================
-- Quick sanity output — every table's row count
-- ===========================================================================
SELECT 'suppliers'           AS table, count(*) FROM suppliers
UNION ALL SELECT 'promotions',          count(*) FROM promotions
UNION ALL SELECT 'customers',           count(*) FROM customers
UNION ALL SELECT 'products',            count(*) FROM products
UNION ALL SELECT 'orders',              count(*) FROM orders
UNION ALL SELECT 'order_items',         count(*) FROM order_items
UNION ALL SELECT 'inventory',           count(*) FROM inventory
UNION ALL SELECT 'payments',            count(*) FROM payments
UNION ALL SELECT 'returns',             count(*) FROM returns
UNION ALL SELECT 'marketing_campaigns', count(*) FROM marketing_campaigns
UNION ALL SELECT 'support_tickets',     count(*) FROM support_tickets
UNION ALL SELECT 'reviews',             count(*) FROM reviews
ORDER BY 1;
