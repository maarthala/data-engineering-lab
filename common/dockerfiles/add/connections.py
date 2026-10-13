"""Seed Superset's database connections for the training course.

Runs at container startup. Idempotent: it UPSERTS the wanted connections (so a fixed URI
takes effect on restart) and REMOVES stale/broken ones.

Both query Trino AS THE SIGNED-IN USER (impersonate_user): Trino's access rules (generated
from Polaris by home-api) then decide what each learner may read — their own lakehouse, what
was shared with them, the shared lake.

Two Trino connections, one engine, two data tiers:
  - "shopflow"          -> the raw OLTP source        (SQL lessons, SQL Lab)
  - "ShopFlow Lakehouse"-> the governed Gold lakehouse (dashboards lesson)
"""
import json

from superset.app import create_app
from superset import db

WANT = [
    {"database_name": "shopflow",
     "sqlalchemy_uri": "trino://trino@trino:8080/shopflow/public", "impersonate_user": True},
    # allow_multi_catalog: pick any Trino catalog in SQL Lab / datasets — `iceberg` (the shared
    # lake) or a learner's own (kiran_lake, kiran_sales …, added to Trino by home-api)
    {"database_name": "ShopFlow Lakehouse",
     "sqlalchemy_uri": "trino://trino@trino:8080/iceberg",
     "extra": {"allow_multi_catalog": True}, "impersonate_user": True},
]
# old defaults that are wrong/broken on this stack (dead hive catalog, disabled clickhouse)
REMOVE = ["trino", "clickhouse"]

app = create_app()
with app.app_context():
    from superset.models.core import Database

    for name in REMOVE:
        obj = db.session.query(Database).filter_by(database_name=name).first()
        if obj:
            db.session.delete(obj)
            print(f"removed stale connection: {name}")

    for c in WANT:
        obj = db.session.query(Database).filter_by(database_name=c["database_name"]).first()
        if obj:
            obj.sqlalchemy_uri = c["sqlalchemy_uri"]          # keep the URI current
            obj.impersonate_user = c.get("impersonate_user", False)
            if c.get("extra"):
                extra = json.loads(obj.extra or "{}")
                extra.update(c["extra"])
                obj.extra = json.dumps(extra)
            print(f"updated connection: {c['database_name']}")
        else:
            db.session.add(Database(database_name=c["database_name"], sqlalchemy_uri=c["sqlalchemy_uri"],
                                    impersonate_user=c.get("impersonate_user", False),
                                    extra=json.dumps(c.get("extra", {}))))
            print(f"added connection: {c['database_name']}")

    db.session.commit()
    print("Superset connections seeded.")
