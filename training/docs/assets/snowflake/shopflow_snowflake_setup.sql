-- ===========================================================================
-- ShopFlow for Snowflake — setup script
-- ===========================================================================
-- Creates the course's ShopFlow source data in YOUR Snowflake (trial) account:
--   database SHOPFLOW
--     BRONZE  the 12 ShopFlow source tables (same columns as the lab's Postgres `shopflow`)
--     SILVER  empty — you fill it in the lessons
--     GOLD    empty — you fill it in the lessons
--   warehouse PRACTICE_WH (X-Small, stops itself after 60 s idle)
--
-- The rows are GENERATED inside Snowflake (like the lab's seed.sql), so there is nothing to
-- upload. Same tables, columns, sizes and shape as the lab — not the same random values.
--
-- How to run: Snowsight → Projects → Worksheets → + SQL worksheet → paste this whole file →
-- run all (Ctrl/Cmd + Shift + Enter). Takes under a minute on X-Small. Re-runnable: it
-- replaces the SHOPFLOW database.
-- ===========================================================================

use role accountadmin;

-- The course's warehouse. (Lesson step 1 had you check the trial's default COMPUTE_WH and
-- drop it — this one replaces it: X-Small, stops after 60 s idle, starts on the next query.)
create warehouse if not exists PRACTICE_WH
    warehouse_size      = 'XSMALL'
    auto_suspend        = 60
    auto_resume         = true
    initially_suspended = true;
use warehouse PRACTICE_WH;

create or replace database SHOPFLOW;
create schema SHOPFLOW.BRONZE;
create schema SHOPFLOW.SILVER;
create schema SHOPFLOW.GOLD;
drop schema if exists SHOPFLOW.PUBLIC;
use schema SHOPFLOW.BRONZE;

-- ===========================================================================
-- Schema  (Snowflake records PRIMARY KEY / FOREIGN KEY but does not enforce them)
-- ===========================================================================

create table SUPPLIERS (
  supplier_id    int primary key,
  name           varchar not null,
  country        varchar not null,            -- where they ship from
  lead_time_days int     not null             -- typical reorder -> restock
);

create table PROMOTIONS (
  promo_id       int primary key,
  code           varchar not null,            -- discount code customers enter
  description    varchar not null,
  discount_type  varchar not null,            -- percent | fixed
  discount_value number(10,2) not null,       -- 15 (%) or 5.00 (fixed)
  starts_on      date not null,
  ends_on        date not null
);

create table CUSTOMERS (
  customer_id      int primary key,
  full_name        varchar not null,
  email            varchar not null unique,
  country          varchar not null,
  signup_date      date not null,
  loyalty_tier     varchar not null default 'none',   -- none | silver | gold
  marketing_opt_in boolean not null default false
);

create table PRODUCTS (
  product_id   int primary key,
  name         varchar not null,
  category     varchar not null,
  price        number(10,2) not null,         -- current list price
  cost         number(10,2) not null,         -- what ShopFlow pays the supplier
  supplier_id  int references SUPPLIERS(supplier_id),
  updated_at   timestamp_ntz not null
);

create table ORDERS (
  order_id     int primary key,
  customer_id  int not null references CUSTOMERS(customer_id),
  channel      varchar not null,              -- web | app | marketplace | wholesale
  order_ts     timestamp_ntz not null,
  status       varchar not null,              -- placed | shipped | delivered | cancelled
  currency     varchar not null default 'USD',
  promo_id     int references PROMOTIONS(promo_id)   -- nullable: most orders use no promo
);

create table ORDER_ITEMS (
  order_id    int not null references ORDERS(order_id),
  product_id  int not null references PRODUCTS(product_id),
  quantity    int not null,
  unit_price  number(10,2) not null,          -- price captured at order time
  primary key (order_id, product_id)
);

create table INVENTORY (
  product_id       int primary key references PRODUCTS(product_id),
  warehouse        varchar not null,
  quantity_on_hand int not null,
  reorder_level    int not null,
  updated_at       timestamp_ntz not null
);

create table PAYMENTS (
  payment_id  int primary key,
  order_id    int not null references ORDERS(order_id),
  method      varchar not null,               -- card | paypal | bank_transfer
  amount      number(10,2) not null,
  currency    varchar not null,
  status      varchar not null,               -- approved | declined | refunded
  fraud_score number(3,2) not null,           -- risk 0..1 assigned at checkout
  paid_ts     timestamp_ntz not null
);

create table RETURNS (
  return_id     int primary key,
  order_id      int not null references ORDERS(order_id),
  product_id    int not null references PRODUCTS(product_id),
  quantity      int not null,
  reason        varchar not null,             -- damaged | wrong_item | changed_mind | ...
  refund_amount number(10,2) not null,
  return_ts     timestamp_ntz not null,
  status        varchar not null              -- requested | received | refunded
);

create table MARKETING_CAMPAIGNS (
  campaign_id int primary key,
  name        varchar not null,
  channel     varchar not null,               -- search | social | email | display
  spend       number(12,2) not null,
  starts_on   date not null,
  ends_on     date not null
);

create table SUPPORT_TICKETS (
  ticket_id   int primary key,
  customer_id int not null references CUSTOMERS(customer_id),
  order_id    int references ORDERS(order_id),   -- nullable: not every ticket is about an order
  subject     varchar not null,
  status      varchar not null,                  -- open | pending | resolved
  opened_ts   timestamp_ntz not null,
  resolved_ts timestamp_ntz,                     -- nullable until resolved
  csat_score  int                                -- nullable: 1..5 after resolution
);

create table REVIEWS (
  review_id   int primary key,
  customer_id int not null references CUSTOMERS(customer_id),
  product_id  int not null references PRODUCTS(product_id),
  rating      int not null,                      -- 1..5 stars
  body        varchar not null,
  created_ts  timestamp_ntz not null
);

-- ===========================================================================
-- Data generation
--   table(generator(rowcount => n))   n empty rows to build from
--   row_number() over (order by …)    1, 2, 3 … without gaps (seq4() can have gaps)
--   uniform(a, b, random())           a random whole number from a to b (both included)
--   uniform(0, 99, random()) < 6       true for ~6% of rows
--   uniform(500, 50000, random()) / 100 a random amount 5.00–500.00
--   array_construct(…)[i]::varchar    pick the i-th value (0-based) from a list
-- ===========================================================================

-- 20 suppliers.
insert into SUPPLIERS
select g,
       'Supplier ' || g,
       array_construct('CN','US','DE','IN','VN','MX')[uniform(0, 5, random())]::varchar,
       array_construct(3,5,7,10,14,21)[uniform(0, 5, random())]::int
from (select row_number() over (order by seq4()) as g
      from table(generator(rowcount => 20)));

-- 12 promotions (percent or fixed), each with an active window.
insert into PROMOTIONS
select g, 'PROMO' || g, 'Promotion ' || g, dt,
       iff(dt = 'percent', uniform(5, 29, random()),
                           uniform(500, 2500, random()) / 100),
       start_on,
       dateadd(day, uniform(14, 44, random()), start_on)
from (select row_number() over (order by seq4()) as g,
             array_construct('percent','percent','fixed')[uniform(0, 2, random())]::varchar as dt,
             dateadd(day, uniform(0, 600, random()), '2023-06-01'::date) as start_on
      from table(generator(rowcount => 12)));

-- 6,000 customers across several countries, signed up over ~2.5 years.
-- (Many will never place an order — like a real signup base.)
insert into CUSTOMERS
select g,
       'Customer ' || g,
       'customer' || g || '@example.com',
       array_construct('US','US','UK','UK','DE','FR','IN','CA','AU')[uniform(0, 8, random())]::varchar,
       dateadd(day, uniform(0, 900, random()), '2023-01-01'::date),
       array_construct('none','none','none','none','silver','silver','gold')[uniform(0, 6, random())]::varchar,
       uniform(0, 99, random()) < 60
from (select row_number() over (order by seq4()) as g
      from table(generator(rowcount => 6000)));

-- 200 products across categories; cost is 40–70% of price (so margin is learnable);
-- each sourced from one of the 20 suppliers.
insert into PRODUCTS
select g, 'Product ' || g, category, price,
       round(price * uniform(40, 70, random()) / 100, 2),
       supplier_id,
       current_timestamp()::timestamp_ntz
from (select row_number() over (order by seq4()) as g,
             array_construct('Electronics','Furniture','Stationery','Clothing',
                             'Home & Kitchen','Sports','Books','Toys')[uniform(0, 7, random())]::varchar as category,
             uniform(500, 50000, random()) / 100 as price,
             uniform(1, 20, random()) as supplier_id
      from table(generator(rowcount => 200)));

-- 40,000 orders over two years; status weighted toward 'delivered'; ~20% use a promo.
-- Customers are deliberately SKEWED (power curve): a few "whale" customers, many one-time
-- buyers and some who never order — so repeat-rate, LEFT JOIN and cohort analysis mean something.
insert into ORDERS
select g,
       1 + least(floor(-ln(1 - uniform(0, 999999, random()) / 1000000) * 500), 5999),
       array_construct('web','web','web','app','app','marketplace','wholesale')[uniform(0, 6, random())]::varchar,
       dateadd(second, uniform(0, 86399, random()),
               dateadd(day, uniform(0, 730, random()), '2023-06-01'::timestamp_ntz)),
       array_construct('delivered','delivered','delivered','delivered','delivered','delivered',
                       'delivered','shipped','placed','cancelled')[uniform(0, 9, random())]::varchar,
       array_construct('USD','USD','USD','GBP','EUR','INR')[uniform(0, 5, random())]::varchar,
       iff(uniform(0, 99, random()) < 20, uniform(1, 12, random()), null)
from (select row_number() over (order by seq4()) as g
      from table(generator(rowcount => 40000)));

-- 1–4 distinct products per order (exactly 100,000 line items); unit_price = price at order time.
-- Each order gets a random start product and step; products start, start+step, … never repeat.
insert into ORDER_ITEMS
select o.order_id, p.product_id, uniform(1, 5, random()), p.price
from (select order_id,
             1 + mod(order_id, 4)        as n_items,
             uniform(0, 199, random())   as start_at,
             uniform(1, 49, random())    as step
      from ORDERS) o
join (select row_number() over (order by seq4()) - 1 as j
      from table(generator(rowcount => 4))) k
  on k.j < o.n_items
join PRODUCTS p
  on p.product_id = 1 + mod(o.start_at + k.j * o.step, 200);

-- Inventory: one stock row per product.
insert into INVENTORY
select product_id,
       array_construct('US-EAST','US-WEST','EU-CENTRAL','APAC')[uniform(0, 3, random())]::varchar,
       uniform(0, 499, random()),
       array_construct(20, 50, 100)[uniform(0, 2, random())]::int,
       dateadd(day, -uniform(0, 30, random()), current_timestamp())::timestamp_ntz
from PRODUCTS;

-- Payments: one per non-cancelled order; amount = the order total; mostly approved,
-- fraud_score skewed toward 0 (random × random), a few declined / refunded.
insert into PAYMENTS
select row_number() over (order by o.order_id),
       o.order_id,
       array_construct('card','card','card','paypal','paypal','bank_transfer')[uniform(0, 5, random())]::varchar,
       t.total,
       o.currency,
       array_construct('approved','approved','approved','approved','approved','approved',
                       'approved','approved','declined','refunded')[uniform(0, 9, random())]::varchar,
       round(uniform(0, 100, random()) * uniform(0, 100, random()) / 10000, 2),
       dateadd(minute, uniform(0, 180, random()), o.order_ts)
from ORDERS o
join (select order_id, sum(quantity * unit_price) as total
      from ORDER_ITEMS group by order_id) t on t.order_id = o.order_id
where o.status <> 'cancelled';

-- Returns: ~6% of delivered line items come back (whole line for simplicity).
insert into RETURNS
select row_number() over (order by order_id, product_id), *
from (select oi.order_id, oi.product_id, oi.quantity,
             array_construct('damaged','wrong_item','changed_mind','not_as_described','late')[uniform(0, 4, random())]::varchar as reason,
             round(oi.unit_price * oi.quantity, 2) as refund_amount,
             dateadd(day, uniform(5, 34, random()), o.order_ts) as return_ts,
             array_construct('requested','received','refunded','refunded')[uniform(0, 3, random())]::varchar as status
      from ORDER_ITEMS oi
      join ORDERS o on o.order_id = oi.order_id
      where o.status = 'delivered' and uniform(0, 99, random()) < 6);

-- 15 marketing campaigns (no FK to orders — analytics joins them by channel + date).
insert into MARKETING_CAMPAIGNS
select g, 'Campaign ' || g,
       array_construct('search','social','email','display')[uniform(0, 3, random())]::varchar,
       uniform(100000, 5000000, random()) / 100,
       start_on,
       dateadd(day, uniform(7, 30, random()), start_on)
from (select row_number() over (order by seq4()) as g,
             dateadd(day, uniform(0, 600, random()), '2023-06-01'::date) as start_on
      from table(generator(rowcount => 15)));

-- Support tickets: ~6% of orders raise one (customer taken from the order), plus 800
-- order-less "general" tickets. Resolved tickets get a resolved_ts + CSAT score.
insert into SUPPORT_TICKETS
select row_number() over (order by opened_ts), *
from (
  select o.customer_id, o.order_id,
         array_construct('Where is my order?','Item damaged','Refund request','Wrong item received',
                         'Change delivery address','Cancel my order')[uniform(0, 5, random())]::varchar as subject,
         o.ticket_status,
         dateadd(day, uniform(1, 10, random()), o.order_ts) as opened_ts,
         iff(o.ticket_status = 'resolved', dateadd(day, uniform(11, 20, random()), o.order_ts), null) as resolved_ts,
         iff(o.ticket_status = 'resolved', uniform(1, 5, random()), null) as csat_score
  from (select o.*,
               array_construct('open','pending','resolved','resolved','resolved')[uniform(0, 4, random())]::varchar as ticket_status
        from ORDERS o) o
  where uniform(0, 99, random()) < 6
  union all
  select uniform(1, 6000, random()), null,
         array_construct('General question','Account help','Website issue','Feedback')[uniform(0, 3, random())]::varchar,
         status,
         dateadd(day, opened_day, '2023-06-01'::timestamp_ntz),
         iff(status = 'resolved', dateadd(day, opened_day + uniform(1, 10, random()), '2023-06-01'::timestamp_ntz), null),
         iff(status = 'resolved', uniform(1, 5, random()), null)
  from (select array_construct('open','pending','resolved')[uniform(0, 2, random())]::varchar as status,
               uniform(0, 730, random()) as opened_day
        from table(generator(rowcount => 800)))
);

-- Reviews: ~12% of delivered line items get a review, rating skewed high.
insert into REVIEWS
select row_number() over (order by created_ts), *
from (select o.customer_id, oi.product_id,
             array_construct(5,5,5,4,4,3,2,1)[uniform(0, 7, random())]::int as rating,
             array_construct('Great product, would buy again.','Exactly as described.','Good value for money.',
                             'Arrived quickly.','Not what I expected.','Decent but could be better.',
                             'Disappointed with the quality.')[uniform(0, 6, random())]::varchar as body,
             dateadd(day, uniform(1, 20, random()), o.order_ts) as created_ts
      from ORDER_ITEMS oi
      join ORDERS o on o.order_id = oi.order_id
      where o.status = 'delivered' and uniform(0, 99, random()) < 12);

-- ===========================================================================
-- Check — every table's row count
-- ===========================================================================
select 'CUSTOMERS' as table_name, count(*) as row_count from CUSTOMERS
union all select 'INVENTORY',           count(*) from INVENTORY
union all select 'MARKETING_CAMPAIGNS', count(*) from MARKETING_CAMPAIGNS
union all select 'ORDERS',              count(*) from ORDERS
union all select 'ORDER_ITEMS',         count(*) from ORDER_ITEMS
union all select 'PAYMENTS',            count(*) from PAYMENTS
union all select 'PRODUCTS',            count(*) from PRODUCTS
union all select 'PROMOTIONS',          count(*) from PROMOTIONS
union all select 'RETURNS',             count(*) from RETURNS
union all select 'REVIEWS',             count(*) from REVIEWS
union all select 'SUPPLIERS',           count(*) from SUPPLIERS
union all select 'SUPPORT_TICKETS',     count(*) from SUPPORT_TICKETS
order by 1;
