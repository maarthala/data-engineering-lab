"""labdb — "🐘 My databases": the PostgreSQL databases a member can use, in one place.

  · the lab's databases (ShopFlow, AdventureWorks, the live shop): read only, a shared login each
  · the member's own databases: <member>_<name>, owned by a login named after the member — full
    access (create tables, write, drop), and only they can connect.
All of them live on the lab's database server lab-db (LABDB_ADMIN_URL), apart from the platform's
Postgres. The member's login password is made once and kept in the vault (admin only,
_lab/db/<member>); the page shows it to its owner.
Each own database gets a private SQLPad connection "<database> (Postgres) — <member>", so the SQL
editor can create tables in it. The database server itself is the record: no portal table.
"""
import os
import re
import secrets
from contextlib import contextmanager

import psycopg2
from psycopg2 import sql

ADMIN_URL = os.environ.get("LABDB_ADMIN_URL", "")          # owner login of the members' database server
HOST, PORT = os.environ.get("LABDB_HOST", "lab-db"), int(os.environ.get("LABDB_PORT", "5432"))
LIMIT = int(os.environ.get("LABDB_LIMIT", "3"))
VAULT_PATH = "_lab/db"
NAME = re.compile(r"^[a-z][a-z0-9_]{0,29}$")
RESERVED = {"postgres", "reader", "learner", "public"}
# the lab's own databases: everyone reads them (key → what the page shows; sqlpad = its connection id)
SHARED = [
    {"key": "shopflow", "title": "ShopFlow", "what": "the course's online shop (orders, customers, products…)",
     "host": HOST, "port": PORT, "database": "shopflow",
     "user": "learner", "password": "learner", "sqlpad": "ShopFlow — OLTP (Postgres)"},
    {"key": "adventureworks", "title": "AdventureWorks", "what": "Microsoft's sample company: sales, production, HR",
     "host": HOST, "port": PORT, "database": "adventureworks",
     "user": "learner", "password": "learner", "sqlpad": "AdventureWorks — OLTP (Postgres)"},
    # Tidewell, one company's three live systems (the course project), changing every few seconds — sources for 📡 Ingestion (CDC)
    {"key": "shop", "title": "Tidewell web shop", "what": "Tidewell's online shop: customers, products, orders (live)",
     "host": HOST, "port": PORT, "database": "shop", "user": "reader", "password": "reader",
     "sqlpad": "Tidewell web shop (Postgres)"},
    {"key": "store", "title": "Tidewell stores", "what": "Tidewell's store tills: sales in local time, cents, SKUs (live)",
     "host": HOST, "port": PORT, "database": "store", "user": "reader", "password": "reader",
     "sqlpad": "Tidewell stores (Postgres)"},
    {"key": "loyalty", "title": "Tidewell loyalty", "what": "Tidewell's loyalty card: members and their points (live)",
     "host": HOST, "port": PORT, "database": "loyalty", "user": "reader", "password": "reader",
     "sqlpad": "Tidewell loyalty (Postgres)"},
]


class DbError(ValueError):
    pass


def enabled():
    return bool(ADMIN_URL)


def _admin(database=None):
    c = psycopg2.connect(ADMIN_URL, connect_timeout=5, **({"dbname": database} if database else {}))
    c.autocommit = True                     # CREATE / DROP DATABASE can't run in a transaction
    return c


@contextmanager
def _cur(database=None):
    c = _admin(database)
    try:
        with c.cursor() as cur:
            yield cur
    finally:
        c.close()


def _role(member):
    if member in RESERVED:
        raise DbError("this member name can't own databases")
    return member


# ---- the member's login and its password (vault) -------------------------------------------
def _vault():
    import labvault
    if not labvault.enabled():
        raise DbError("the vault is off — own databases need it for their password")
    return labvault


def _password(member):
    v = _vault()
    st, body = v._call("GET", f"{v.MOUNT}/data/{VAULT_PATH}/{member}")
    return body["data"]["data"]["password"] if st == 200 else None


def _login(cur, member):
    """The member's login on the database server — made once; its password from the vault."""
    pw = _password(member)
    if not pw:
        pw = secrets.token_urlsafe(12)
        v = _vault()
        v._ok(v._call("POST", f"{v.MOUNT}/data/{VAULT_PATH}/{member}", {"data": {"password": pw}})[0], "database password")
    cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (member,))
    if cur.fetchone():
        cur.execute(sql.SQL("ALTER ROLE {} LOGIN PASSWORD %s").format(sql.Identifier(member)), (pw,))
    else:
        cur.execute(sql.SQL("CREATE ROLE {} LOGIN PASSWORD %s").format(sql.Identifier(member)), (pw,))
    return pw


# ---- what the page shows -------------------------------------------------------------------
def _mine(cur, member):
    cur.execute("SELECT d.datname FROM pg_database d JOIN pg_roles r ON r.oid = d.datdba "
                "WHERE r.rolname = %s ORDER BY 1", (member,))
    return [r[0] for r in cur.fetchall()]


def overview(member):
    out = {"shared": [{k: v for k, v in d.items()} for d in SHARED], "mine": [], "limit": LIMIT,
           "enabled": enabled(), "host": HOST, "port": PORT, "user": member, "password": None}
    if enabled() and member not in RESERVED:
        with _cur() as cur:
            out["mine"] = [{"key": db, "database": db, "host": HOST, "port": PORT, "user": member,
                            "sqlpad": _sqlpad_label(db, member)} for db in _mine(cur, member)]
        out["password"] = _password(member) if out["mine"] else None
    return out


def tables(member, key):
    """A database's tables with a row estimate — the lab's (with its shared login) or one of yours."""
    shared = next((d for d in SHARED if d["key"] == key), None)
    if shared:
        c = psycopg2.connect(host=shared["host"], port=shared["port"], dbname=shared["database"],
                             user=shared["user"], password=shared["password"], connect_timeout=5)
    else:
        with _cur() as cur:
            if key not in _mine(cur, _role(member)):
                raise PermissionError("not your database")
        c = _admin(key)
    try:
        with c.cursor() as cur:
            cur.execute("""
              SELECT n.nspname, cl.relname, CASE cl.relkind WHEN 'v' THEN 'view' WHEN 'm' THEN 'view' ELSE 'table' END,
                     CASE WHEN cl.reltuples >= 0 THEN cl.reltuples::bigint ELSE coalesce(s.n_live_tup, 0) END
              FROM pg_class cl JOIN pg_namespace n ON n.oid = cl.relnamespace
              LEFT JOIN pg_stat_all_tables s ON s.relid = cl.oid
              WHERE cl.relkind IN ('r', 'p', 'v', 'm') AND n.nspname NOT IN ('pg_catalog', 'information_schema')
                AND n.nspname NOT LIKE 'pg_toast%%' AND has_table_privilege(cl.oid, 'SELECT')
              ORDER BY 1, 2""")
            return [{"schema": s, "table": t, "kind": k, "rows": int(n)} for s, t, k, n in cur.fetchall()]
    finally:
        c.close()


# ---- create / delete -----------------------------------------------------------------------
def create(member, name):
    if not enabled():
        raise DbError("own databases aren't set up on this lab")
    member, name = _role(member), (name or "").strip().lower()
    if not NAME.fullmatch(name):
        raise DbError("name: lower-case letters, digits and _, starting with a letter (e.g. sales)")
    db = f"{member}_{name}"
    with _cur() as cur:
        mine = _mine(cur, member)
        if db in mine:
            raise DbError(f"you have {db} already")
        if len(mine) >= LIMIT:
            raise DbError(f"you have {len(mine)} databases — the most there can be; delete one first")
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db,))
        if cur.fetchone():
            raise DbError(f"{db} exists already")
        pw = _login(cur, member)
        cur.execute(sql.SQL("CREATE DATABASE {} OWNER {}").format(sql.Identifier(db), sql.Identifier(member)))
        cur.execute(sql.SQL("REVOKE CONNECT, TEMPORARY ON DATABASE {} FROM PUBLIC").format(sql.Identifier(db)))
    with _cur(db) as cur:          # the public schema is theirs too (PG 15+: owner only)
        cur.execute(sql.SQL("ALTER SCHEMA public OWNER TO {}").format(sql.Identifier(member)))
    label = None
    try:
        label = _sqlpad_connect(db, member, pw)
    except Exception as e:  # noqa: BLE001 — the database is there; SQLPad can catch up
        print(f"[labdb] sqlpad {db}: {e}", flush=True)
    return {"created": db, "sqlpad": label}


def delete(member, db):
    member = _role(member)
    with _cur() as cur:
        if db not in _mine(cur, member):
            raise PermissionError("not your database")
        cur.execute("SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
                    "WHERE database = %s AND NOT active", (db,))       # a slot would block DROP DATABASE
        cur.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(db)))
    try:
        _sqlpad_disconnect(db, member)
    except Exception as e:  # noqa: BLE001
        print(f"[labdb] sqlpad {db}: {e}", flush=True)
    return {"deleted": db}


def forget(member):
    """A member is deleted: their databases, their login, its password."""
    if not enabled() or member in RESERVED:
        return
    with _cur() as cur:
        dbs = _mine(cur, member)
    for db in dbs:
        delete(member, db)
    with _cur() as cur:
        cur.execute(sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(member)))
    try:
        v = _vault()
        v._call("DELETE", f"{v.MOUNT}/metadata/{VAULT_PATH}/{member}")
    except DbError:
        pass


# ---- SQLPad: a private connection per own database -----------------------------------------
def _sqlpad_label(db, member):
    return f"{db} (Postgres) — {member}"


def _sqlpad_connect(db, member, pw):
    import lakehouse as lh
    label = _sqlpad_label(db, member)
    body = {"name": label, "driver": "postgres", "data": {
        "host": HOST, "port": str(PORT), "database": db, "username": member, "password": pw}}
    conn = next((c for c in lh._sqlpad("GET", "/api/connections") if c["name"] == label), None)
    conn = lh._sqlpad("POST", "/api/connections", body) if conn is None else \
        lh._sqlpad("PUT", f"/api/connections/{conn['id']}", body)
    user = lh.storage_user(member)
    u = next((x for x in lh._sqlpad("GET", "/api/users") if x["email"] == user), None)
    if u and u["role"] != "admin":
        lh._sqlpad("POST", "/api/connection-accesses", {"connectionId": conn["id"], "userId": u["id"], "duration": 0})
    return label


def _sqlpad_disconnect(db, member):
    import lakehouse as lh
    for c in lh._sqlpad("GET", "/api/connections"):
        if c["name"] == _sqlpad_label(db, member):
            lh._sqlpad("DELETE", f"/api/connections/{c['id']}")
