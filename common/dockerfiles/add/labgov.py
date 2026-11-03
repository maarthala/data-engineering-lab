"""labgov — 🛡 Governance (managers): Polaris's logins, roles and grants, on the portal (what the
Polaris Console did). Polaris's model:
  principal        a login (client id + secret) — a member, a job login, a persona, or one made here
  principal role   what a principal is given; a principal can hold several
  catalog role     a set of grants (privileges) in one catalog, given to principal roles
  grant            a privilege on the catalog, a namespace or a table, e.g. TABLE_READ_DATA

The lab makes most of these itself (a member's login and role, their catalogs' owner role, job
logins, shares, Trino's login) — they're shown read-only, with why, so an edit here can't break
someone's access. Editable: what's made on this page (property lab.governance) and the course's
personas (analyst, engineer, lead — the governance lessons). A grant or link counts as the
catalog role's / principal role's: editable when that is.
"""
import os
import re

import lakehouse as lh

_M = "/api/management/v1"
TAG = "lab.governance"
PERSONAS = [p.strip() for p in os.environ.get("LAB_PERSONAS", "analyst,engineer,lead").split(",") if p.strip()]
NAME = re.compile(r"^[a-z][a-z0-9_]{1,40}$")
# what can be granted where (Polaris's privileges, by the kind of thing they're on)
TABLE_PRIVS = ["TABLE_READ_DATA", "TABLE_WRITE_DATA", "TABLE_READ_PROPERTIES", "TABLE_WRITE_PROPERTIES",
               "TABLE_FULL_METADATA", "TABLE_DROP"]
NAMESPACE_PRIVS = ["NAMESPACE_LIST", "NAMESPACE_READ_PROPERTIES", "NAMESPACE_WRITE_PROPERTIES", "NAMESPACE_CREATE",
                   "NAMESPACE_DROP", "NAMESPACE_FULL_METADATA", "TABLE_LIST", "TABLE_CREATE"] + TABLE_PRIVS + [
                   "VIEW_LIST", "VIEW_CREATE", "VIEW_DROP", "VIEW_READ_PROPERTIES", "VIEW_WRITE_PROPERTIES", "VIEW_FULL_METADATA"]
CATALOG_PRIVS = ["CATALOG_MANAGE_CONTENT", "CATALOG_MANAGE_METADATA", "CATALOG_MANAGE_ACCESS", "CATALOG_READ_PROPERTIES",
                 "CATALOG_WRITE_PROPERTIES"] + NAMESPACE_PRIVS
PRIVILEGES = {"catalog": CATALOG_PRIVS, "namespace": NAMESPACE_PRIVS, "table": TABLE_PRIVS}
# the usual bundles, so a lesson can say "give read on gold" without twelve clicks
BUNDLES = {"read": ["NAMESPACE_LIST", "NAMESPACE_READ_PROPERTIES", "TABLE_LIST", "TABLE_READ_PROPERTIES", "TABLE_READ_DATA"],
           "write": ["NAMESPACE_LIST", "NAMESPACE_READ_PROPERTIES", "TABLE_LIST", "TABLE_READ_PROPERTIES", "TABLE_READ_DATA",
                     "TABLE_WRITE_DATA", "TABLE_WRITE_PROPERTIES", "TABLE_CREATE", "TABLE_DROP"],
           "manage": ["CATALOG_MANAGE_CONTENT"]}


class GovError(ValueError):
    pass


def _t():
    return lh._admin_token()


def _get(path, t):
    st, body = lh._call("GET", path, token=t)
    if st != 200:
        raise RuntimeError(f"GET {path}: HTTP {st}")
    return body or {}


def _ok(st, what):
    if st not in (200, 201, 204):
        raise GovError(f"{what}: Polaris said HTTP {st}")


# ---- who may change what ---------------------------------------------------------------------
def _principal_why(p):
    """None (editable) or why the lab keeps it."""
    n, props = p["name"], p.get("properties") or {}
    if props.get(TAG) or n in PERSONAS:
        return None
    if n == "root":
        return "the lab's Polaris admin"
    if n == "trino_lab":
        return "Trino's login (SQL, Superset)"
    if n.endswith("_jobs"):
        return "a member's job login (Airflow, Ingestion)"
    return "a member's own login (Manage members)"


def _prole_why(r):
    n, props = r["name"], r.get("properties") or {}
    if props.get(TAG) or n in [f"{p}_role" for p in PERSONAS]:
        return None
    return {"service_admin": "Polaris's admin role", "trino_lab": "Trino's role"}.get(n, "a member's role (their lakehouses, shares, team)")


def _crole_why(cat, r):
    n, props = r["name"], r.get("properties") or {}
    if props.get(TAG) or (cat == "polaris_lake" and n in [f"{p}_role_cr" for p in PERSONAS]):
        return None
    if n.startswith("share_"):
        return "a share — the catalog's owner changes it in 🗂 My catalogs → Sharing"
    return {"owner": "the catalog's owner (its member or team)", "catalog_admin": "Polaris's own admin role of the catalog",
            "trino_lab": "Trino's access to the catalog", "learner_reader": "members reading the shared lake",
            "admin_cr": "the lab's admin access"}.get(n, "made by the lab")


def _principal(name, t):
    st, p = lh._call("GET", f"{_M}/principals/{name}", token=t)
    if st != 200:
        raise GovError(f"no login called {name}")
    return p


def _prole(name, t):
    st, r = lh._call("GET", f"{_M}/principal-roles/{name}", token=t)
    if st != 200:
        raise GovError(f"no principal role called {name}")
    return r


def _crole(cat, name, t):
    st, r = lh._call("GET", f"{_M}/catalogs/{cat}/catalog-roles/{name}", token=t)
    if st != 200:
        raise GovError(f"no catalog role {name} in {cat}")
    return r


def _editable(why, what):
    if why:
        raise GovError(f"{what} is {why} — the lab manages it, it can't be changed here")


# ---- what the page shows -----------------------------------------------------------------------
def overview():
    t = _t()
    principals = []
    for p in _get(f"{_M}/principals", t).get("principals", []):
        roles = [r["name"] for r in _get(f"{_M}/principals/{p['name']}/principal-roles", t).get("roles", [])]
        why = _principal_why(p)
        principals.append({"name": p["name"], "client_id": p.get("clientId"), "created": p.get("createTimestamp"),
                           "roles": roles, "editable": why is None, "why": why, "persona": p["name"] in PERSONAS,
                           "assignable": p["name"] not in ("root", "trino_lab")})
    proles = []
    for r in _get(f"{_M}/principal-roles", t).get("roles", []):
        why = _prole_why(r)
        proles.append({"name": r["name"], "created": r.get("createTimestamp"), "editable": why is None, "why": why,
                       "principals": [p["name"] for p in principals if r["name"] in p["roles"]]})
    cats = [{"name": c["name"], "type": c.get("type"), "location": (c.get("properties") or {}).get("default-base-location")}
            for c in _get(f"{_M}/catalogs", t).get("catalogs", [])]
    return {"principals": sorted(principals, key=lambda x: (x["why"] is not None, x["name"])),
            "principal_roles": sorted(proles, key=lambda x: (x["why"] is not None, x["name"])),
            "catalogs": cats, "privileges": PRIVILEGES, "bundles": BUNDLES, "personas": PERSONAS}


def catalog(cat):
    """One catalog's roles: their grants and which principal roles hold them."""
    t = _t()
    out = []
    for r in _get(f"{_M}/catalogs/{cat}/catalog-roles", t).get("roles", []):
        why = _crole_why(cat, r)
        grants = _get(f"{_M}/catalogs/{cat}/catalog-roles/{r['name']}/grants", t).get("grants", [])
        held = [x["name"] for x in _get(f"{_M}/catalogs/{cat}/catalog-roles/{r['name']}/principal-roles", t).get("roles", [])]
        out.append({"name": r["name"], "editable": why is None, "why": why, "principal_roles": held,
                    "grants": [{"type": g.get("type"), "privilege": g.get("privilege"),
                                "namespace": ".".join(g.get("namespace") or []) or None, "table": g.get("tableName")}
                               for g in grants]})
    return {"catalog": cat, "roles": sorted(out, key=lambda x: (x["why"] is not None, x["name"]))}


# ---- changes ----------------------------------------------------------------------------------
def _name(v, what):
    v = str(v or "").strip().lower()
    if not NAME.fullmatch(v):
        raise GovError(f"{what}: 2–41 of a–z, 0–9, _ (starting with a letter)")
    return v


def create_principal(name, role=None):
    """A new login (+ optionally a principal role of the same name held by it) → its client id and
    secret: shown once, here — Polaris doesn't show the secret again."""
    t, name = _t(), _name(name, "login name")
    st, body = lh._call("POST", f"{_M}/principals", {"principal": {"name": name, "properties": {TAG: "true"}}}, token=t)
    if st == 409:
        raise GovError(f"{name} exists already")
    _ok(st, "create login")
    if role:
        create_principal_role(name if role is True else role)
        assign(name, name if role is True else role)
    cred = (body or {}).get("credentials") or {}
    return {"name": name, "client_id": cred.get("clientId"), "client_secret": cred.get("clientSecret")}


def rotate(name):
    t = _t()
    _editable(_principal_why(_principal(name, t)), f"login {name}")
    st, body = lh._call("POST", f"{_M}/principals/{name}/reset", {}, token=t)    # (rotate: only the login itself)
    _ok(st, "new secret")
    cred = (body or {}).get("credentials") or {}
    return {"name": name, "client_id": cred.get("clientId"), "client_secret": cred.get("clientSecret")}


def delete_principal(name):
    t = _t()
    p = _principal(name, t)
    _editable(_principal_why(p), f"login {name}")
    if name in PERSONAS:
        raise GovError(f"{name} is one of the course's personas — keep it (change its roles instead)")
    _ok(lh._call("DELETE", f"{_M}/principals/{name}", token=t)[0], "delete login")
    st, r = lh._call("GET", f"{_M}/principal-roles/{name}", token=t)     # its own role (made with it), if no one else holds it
    if st == 200 and (r.get("properties") or {}).get(TAG):
        held = [p for p in _get(f"{_M}/principals", t).get("principals", [])
                if name in [x["name"] for x in _get(f"{_M}/principals/{p['name']}/principal-roles", t).get("roles", [])]]
        if not held:
            lh._call("DELETE", f"{_M}/principal-roles/{name}", token=t)
            return {"deleted": name, "and_role": name}
    return {"deleted": name}


def create_principal_role(name):
    t, name = _t(), _name(name, "role name")
    st, _ = lh._call("POST", f"{_M}/principal-roles", {"principalRole": {"name": name, "properties": {TAG: "true"}}}, token=t)
    if st == 409:
        raise GovError(f"principal role {name} exists already")
    _ok(st, "create principal role")
    return {"created": name}


def delete_principal_role(name):
    t = _t()
    _editable(_prole_why(_prole(name, t)), f"principal role {name}")
    if name in [f"{p}_role" for p in PERSONAS]:
        raise GovError(f"{name} is a persona's role — keep it")
    _ok(lh._call("DELETE", f"{_M}/principal-roles/{name}", token=t)[0], "delete principal role")
    return {"deleted": name}


def assign(principal, role):
    """Give a principal role to a login (any login — e.g. a member — when the role is editable)."""
    t = _t()
    _principal(principal, t)
    if principal in ("root", "trino_lab"):              # Trino's login serves every member's SQL: no extra rights
        raise GovError(f"{principal} keeps the roles the lab gives it")
    _editable(_prole_why(_prole(role, t)), f"principal role {role}")
    _ok(lh._call("PUT", f"{_M}/principals/{principal}/principal-roles", {"principalRole": {"name": role}}, token=t)[0], "assign")
    return {"assigned": role, "to": principal}


def unassign(principal, role):
    t = _t()
    _editable(_prole_why(_prole(role, t)), f"principal role {role}")
    _ok(lh._call("DELETE", f"{_M}/principals/{principal}/principal-roles/{role}", token=t)[0], "unassign")
    return {"unassigned": role, "from": principal}


def create_catalog_role(cat, name):
    t, name = _t(), _name(name, "role name")
    if name.startswith("share_") or name in ("owner", "catalog_admin", "trino_lab"):
        raise GovError(f"{name} is a name the lab uses — choose another")
    st, _ = lh._call("POST", f"{_M}/catalogs/{cat}/catalog-roles", {"catalogRole": {"name": name, "properties": {TAG: "true"}}}, token=t)
    if st == 409:
        raise GovError(f"catalog role {name} exists already in {cat}")
    _ok(st, "create catalog role")
    return {"created": name, "catalog": cat}


def delete_catalog_role(cat, name):
    t = _t()
    _editable(_crole_why(cat, _crole(cat, name, t)), f"catalog role {name}")
    if name in [f"{p}_role_cr" for p in PERSONAS]:
        raise GovError(f"{name} is a persona's catalog role — keep it (change its grants instead)")
    _ok(lh._call("DELETE", f"{_M}/catalogs/{cat}/catalog-roles/{name}", token=t)[0], "delete catalog role")
    return {"deleted": name}


def link(cat, crole, prole, on=True):
    """Give a catalog role to a principal role (or take it away)."""
    t = _t()
    _editable(_crole_why(cat, _crole(cat, crole, t)), f"catalog role {crole}")
    _prole(prole, t)
    if on:
        _ok(lh._call("PUT", f"{_M}/principal-roles/{prole}/catalog-roles/{cat}", {"catalogRole": {"name": crole}}, token=t)[0], "link")
    else:
        _ok(lh._call("DELETE", f"{_M}/principal-roles/{prole}/catalog-roles/{cat}/{crole}", token=t)[0], "unlink")
    return {"catalog": cat, "catalog_role": crole, "principal_role": prole, "linked": on}


def _grant_body(kind, privilege, namespace, table):
    if kind not in PRIVILEGES:
        raise GovError("on: catalog, namespace or table")
    if privilege not in PRIVILEGES[kind]:
        raise GovError(f"{privilege} can't be granted on a {kind}")
    g = {"type": kind, "privilege": privilege}
    if kind in ("namespace", "table"):
        if not namespace:
            raise GovError("which namespace?")
        g["namespace"] = namespace.split(".")
    if kind == "table":
        if not table:
            raise GovError("which table?")
        g["tableName"] = table
    return g


def grant(cat, crole, kind, privileges, namespace=None, table=None, on=True):
    """Add (or revoke) privileges of a catalog role — one, or a bundle (read / write / manage)."""
    t = _t()
    _editable(_crole_why(cat, _crole(cat, crole, t)), f"catalog role {crole}")
    privs = BUNDLES.get(privileges) if isinstance(privileges, str) and privileges in BUNDLES else (
        [privileges] if isinstance(privileges, str) else list(privileges or []))
    privs = [p for p in privs if p in PRIVILEGES.get(kind, [])] or privs
    done = []
    for p in privs:
        g = _grant_body(kind, p, namespace, table)
        st, _ = (lh._call("PUT", f"{_M}/catalogs/{cat}/catalog-roles/{crole}/grants", {"grant": g}, token=t) if on else
                 lh._call("POST", f"{_M}/catalogs/{cat}/catalog-roles/{crole}/grants", {"grant": g}, token=t))
        if st == 404:
            raise GovError(f"no {kind} {'.'.join(x for x in (namespace, table) if x)} in {cat}" if kind != "catalog" else f"no catalog {cat}")
        _ok(st, ("grant " if on else "revoke ") + p)
        done.append(p)
    return {"catalog": cat, "catalog_role": crole, "on": kind, "namespace": namespace, "table": table,
            ("granted" if on else "revoked"): done}


OPS = {"create_principal": lambda b: create_principal(b.get("name"), b.get("role") or None),
       "rotate": lambda b: rotate(b.get("name", "")),
       "delete_principal": lambda b: delete_principal(b.get("name", "")),
       "create_principal_role": lambda b: create_principal_role(b.get("name")),
       "delete_principal_role": lambda b: delete_principal_role(b.get("name", "")),
       "assign": lambda b: assign(b.get("principal", ""), b.get("role", "")),
       "unassign": lambda b: unassign(b.get("principal", ""), b.get("role", "")),
       "create_catalog_role": lambda b: create_catalog_role(b.get("catalog", ""), b.get("name")),
       "delete_catalog_role": lambda b: delete_catalog_role(b.get("catalog", ""), b.get("name", "")),
       "link": lambda b: link(b.get("catalog", ""), b.get("catalog_role", ""), b.get("principal_role", ""), True),
       "unlink": lambda b: link(b.get("catalog", ""), b.get("catalog_role", ""), b.get("principal_role", ""), False),
       "grant": lambda b: grant(b.get("catalog", ""), b.get("catalog_role", ""), b.get("on", ""), b.get("privileges"),
                                b.get("namespace") or None, b.get("table") or None, True),
       "revoke": lambda b: grant(b.get("catalog", ""), b.get("catalog_role", ""), b.get("on", ""), b.get("privileges"),
                                 b.get("namespace") or None, b.get("table") or None, False)}


def do(body):
    op = str(body.get("op", ""))
    if op not in OPS:
        raise GovError("unknown change")
    return OPS[op](body)
