"""ShopFlow's other sources — a CRM and a marketplace — for the Data Vault unit.

Made ONCE: its output was exported to docs/assets/datavault/*.csv, the files members download,
upload to My files and load into `my` (Data Vault → "Meet ShopFlow's three sources"). Those files
are the source of truth now: Spark's seeded rand() still varies with partitioning, so a rerun
gives slightly different rows — and every Data Vault example output was verified on the files.
Rerun only to make new files (then export, re-verify the unit's outputs and replace them).

They're generated from the shop's Bronze tables so the three sources overlap like real ones:

  crm.contacts            ~85% of shop customers + 500 leads; keyed by crm_id, e-mail as
                          typed (some upper-case), its own names, ~10% a different country
  marketplace.orders      8,000 orders 'MP-1xxxxx'; buyers by e-mail — ~60% are shop
                          customers (some typed with spaces / upper case), the rest new buyers;
                          its own status words (PAID, DISPATCHED, DELIVERED, REFUNDED)
  marketplace.order_lines 1–3 lines per order by SKU ('SKU-0005' = shop product 5), plus a
                          few marketplace-only SKUs (SKU-9001…9003)

Run by hand (it writes crm.* and marketplace.* into the catalog you give):
  spark-submit --master spark://spark-master:7077 \
    --conf spark.sql.catalogImplementation=in-memory \
    datavault_sources.py --catalog <catalog>
"""
import argparse
import os
from pyspark.sql import SparkSession

FIRST = "array('Ada','Ben','Chloe','Daniel','Emma','Felix','Grace','Hugo','Isla','Jonas'," \
        "'Kira','Liam','Maya','Noah','Olivia','Priya','Quinn','Ravi','Sofia','Tom')"
LAST = "array('Smith','Jones','Müller','Martin','Patel','Garcia','Brown','Rossi','Dubois'," \
       "'Kowalski','Nguyen','Silva','Taylor','Fischer','Khan','Moreau','Wilson','Costa')"
COUNTRIES = "array('US','UK','DE','FR','IN','CA','AU','NL','ES')"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", default="iceberg")
    ap.add_argument("--date", default=None)      # accepted for scheduling; full rebuild here
    args = ap.parse_args()
    cat = args.catalog

    spark = SparkSession.builder.appName("shopflow_land_sources").getOrCreate()
    # the cluster holds no Polaris login: this platform job uses the one Airflow gives it
    if os.environ.get("POLARIS_CREDENTIAL"):
        spark.conf.set(f"spark.sql.catalog.{cat}.credential", os.environ["POLARIS_CREDENTIAL"])
    q = spark.sql
    for ns in ("crm", "marketplace"):
        q(f"CREATE SCHEMA IF NOT EXISTS {cat}.{ns}")

    def pick(arr, seed):
        """One element of an array literal, chosen with a seeded random number."""
        return f"element_at({arr}, cast(1 + floor(rand({seed}) * size({arr})) AS int))"

    # --- CRM -------------------------------------------------------------------------------
    contacts = q(f"""
      WITH known AS (                     -- ~85% of the shop's customers are in the CRM
        SELECT customer_id, email, country FROM {cat}.bronze.customers WHERE rand(11) < 0.85
      ),
      leads AS (                          -- people the CRM knows who never bought in the shop
        SELECT CAST(NULL AS int) AS customer_id, concat('lead', id, '@example.com') AS email,
               {pick(COUNTRIES, 12)} AS country
        FROM range(1, 501) AS t(id)
      ),
      everyone AS (SELECT * FROM known UNION ALL SELECT * FROM leads)
      SELECT concat('C-', lpad(cast(row_number() OVER (ORDER BY coalesce(customer_id, 100000), email)
                                    AS string), 6, '0'))                      AS crm_id,
             CASE WHEN rand(13) < 0.10 THEN upper(email) ELSE email END       AS email,
             {pick(FIRST, 14)}                                                AS first_name,
             {pick(LAST, 15)}                                                 AS last_name,
             concat('+', cast(10000000000 + floor(rand(16) * 89999999999) AS bigint)) AS phone,
             CASE WHEN rand(17) < 0.10 THEN {pick(COUNTRIES, 18)} ELSE country END AS country,
             element_at(array('B2C','B2C','B2C','B2C','B2C','B2C','B2C','B2B','B2B','VIP'),
                        cast(1 + floor(rand(19) * 10) AS int))                AS segment,
             rand(20) < 0.55                                                  AS consent_email,
             timestamp_seconds(1704067200 + cast(rand(21) * 31536000 AS bigint)) AS updated_at
      FROM everyone
    """)
    contacts.writeTo(f"{cat}.crm.contacts").using("iceberg").createOrReplace()

    # --- Marketplace orders ----------------------------------------------------------------
    orders = q(f"""
      WITH o AS (
        SELECT id, rand(31) < 0.60 AS is_shop_customer,
               cast(1 + floor(rand(32) * 6000) AS int) AS shop_customer_id,
               cast(1 + floor(rand(33) * 2500) AS int) AS buyer_no
        FROM range(1, 8001) AS t(id)
      )
      SELECT concat('MP-', cast(100000 + o.id AS string))                     AS mp_order_ref,
             CASE WHEN NOT o.is_shop_customer
                    THEN concat('buyer', o.buyer_no, '@markethub.example')
                  WHEN rand(34) < 0.15 THEN concat(' ', upper(c.email))        -- typed by hand
                  ELSE c.email END                                            AS buyer_email,
             CASE WHEN o.is_shop_customer THEN c.full_name
                  ELSE concat('Buyer ', o.buyer_no) END                       AS buyer_name,
             CASE WHEN o.is_shop_customer THEN c.country                      -- one country per buyer
                  ELSE element_at({COUNTRIES}, 1 + pmod(o.buyer_no, size({COUNTRIES}))) END AS buyer_country,
             timestamp_seconds(1685577600 + cast(rand(36) * 63072000 AS bigint)) AS ordered_at,
             element_at(array('DELIVERED','DELIVERED','DELIVERED','DELIVERED','DELIVERED',
                              'DELIVERED','DISPATCHED','PAID','REFUNDED'),
                        cast(1 + floor(rand(37) * 9) AS int))                 AS order_status,
             element_at(array('EUR','EUR','GBP','USD'), cast(1 + floor(rand(38) * 4) AS int)) AS currency
      FROM o
      LEFT JOIN {cat}.bronze.customers c ON c.customer_id = o.shop_customer_id
    """)
    orders.writeTo(f"{cat}.marketplace.orders").using("iceberg").createOrReplace()

    # --- Marketplace order lines: 1–3 distinct SKUs per order -------------------------------
    lines = q(f"""
      WITH o AS (
        SELECT mp_order_ref,
               1 + pmod(cast(substr(mp_order_ref, 4) AS int), 3)    AS n_lines,
               cast(floor(rand(41) * 200) AS int)                   AS start_at,
               cast(1 + floor(rand(42) * 60) AS int)                AS step
        FROM {cat}.marketplace.orders
      ),
      l AS (
        SELECT o.mp_order_ref, k.j,
               1 + pmod(o.start_at + k.j * o.step, 200)             AS product_id,
               rand(43) < 0.02                                      AS marketplace_only
        FROM o JOIN (SELECT cast(id AS int) AS j FROM range(0, 3) AS t(id)) k ON k.j < o.n_lines
      )
      SELECT l.mp_order_ref,
             CASE WHEN l.marketplace_only THEN concat('SKU-', 9001 + l.j)          -- distinct per order
                  ELSE concat('SKU-', lpad(cast(l.product_id AS string), 4, '0')) END AS sku,
             cast(1 + floor(rand(44) * 3) AS int)                                      AS qty,
             CASE WHEN l.marketplace_only
                    THEN cast(round(10 + rand(45) * 90, 2) AS decimal(10,2))
                  ELSE cast(round(p.price * (0.90 + rand(46) * 0.15), 2) AS decimal(10,2)) END AS item_price
      FROM l
      JOIN {cat}.bronze.products p ON p.product_id = l.product_id
    """)
    lines.writeTo(f"{cat}.marketplace.order_lines").using("iceberg").createOrReplace()

    for t in ("crm.contacts", "marketplace.orders", "marketplace.order_lines"):
        print(f"wrote {cat}.{t}", spark.table(f"{cat}.{t}").count())
    spark.stop()


if __name__ == "__main__":
    main()
