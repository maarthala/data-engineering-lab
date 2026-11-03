"""labmaint — 🧹 table maintenance: what to do is kept in Polaris (its policy API), the doing is Trino.

Three kinds, each set on a whole catalog, a namespace or one table — the most specific one applies
(Polaris works that out: "applicable policies"):
  compaction   merge small data files (and the delete files of CDC upserts) up to a target size
  expiry       forget snapshots older than N days (time travel goes back that far, no further)
  orphans      delete files no snapshot uses any more, older than N days
"Off" on a namespace or table stops one set higher up. The policies live in each catalog's
namespace lab_maintenance (hidden on the portal's pages), one per kind and scope.

A run (Run now, or every night at NIGHTLY_HOUR UTC for every table some policy covers) is, per
table, Trino as the lab: ALTER TABLE … EXECUTE optimize / expire_snapshots / remove_orphan_files.
It needs the SQL unit. Each table's last run is kept in the portal database (maint_runs).
"""
import datetime
import json
import os
import re
import time
import urllib.parse
import urllib.request

import lakehouse as lh

POLICY_NS = "lab_maintenance"
NIGHTLY_HOUR = int(os.environ.get("MAINT_HOUR_UTC", "3"))
VERSION = "2025-02-03"                                   # Polaris's policy content version
KINDS = {"compaction": "system.data-compaction", "expiry": "system.snapshot-expiry", "orphans": "system.orphan-file-removal"}
DEFAULTS = {"compaction": {"target_mb": 128}, "expiry": {"days": 7}, "orphans": {"days": 7}}
LIMITS = {"target_mb": (8, 1024), "days": (1, 365)}
_P = "/api/catalog/polaris/v1"

SCHEMA = """
CREATE TABLE IF NOT EXISTS maint_runs (
  id bigserial PRIMARY KEY, catalog text NOT NULL, namespace text NOT NULL, tbl text NOT NULL,
  started_at timestamptz NOT NULL DEFAULT now(), finished_at timestamptz, ok boolean, trigger text,
  steps jsonb, error text);
CREATE INDEX IF NOT EXISTS maint_runs_tbl ON maint_runs (catalog, namespace, tbl, started_at DESC);
CREATE TABLE IF NOT EXISTS maint_nightly (day date PRIMARY KEY);            -- the nightly round: once a day
"""
_ready = [False]


class MaintError(ValueError):
    pass


def _q(sql, args=(), one=False, rows=True):
    import labcdc                                        # the portal database (its connection)
    if not _ready[0]:
        labcdc._q(SCHEMA, rows=False)
        _ready[0] = True
    return labcdc._q(sql, args, one=one, rows=rows)


# ---- Polaris: policies -----------------------------------------------------------------------
def _call(method, path, body=None, t=None):
    req = urllib.request.Request(lh.POLARIS + path, method=method, data=json.dumps(body).encode() if body is not None else None,
                                 headers={"Authorization": f"Bearer {t}", "Content-Type": "application/json", "Polaris-Realm": lh.REALM})
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            raw = r.read()
            return r.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            return e.code, json.loads(raw)
        except ValueError:
            return e.code, None


def _err(st, body, what):
    if st >= 300:
        msg = ((body or {}).get("error") or {}).get("message", "")
        raise RuntimeError(f"{what}: HTTP {st} {msg}".strip())


def _ns_path(namespace):
    return urllib.parse.quote(chr(31).join(namespace.split(".")))


def _policy_name(kind, namespace=None, table=None):
    tail = "all" if not namespace else f"ns__{namespace}" if not table else f"t__{namespace}__{table}"
    return re.sub(r"[^A-Za-z0-9_]", "_", f"{kind}__{tail}")[:200]


def _target(cat, namespace=None, table=None):
    if table:
        return {"type": "table-like", "path": namespace.split(".") + [table]}
    if namespace:
        return {"type": "namespace", "path": namespace.split(".")}
    return {"type": "catalog", "path": [cat]}


def _content(kind, v):
    """Our values → the policy's content (Polaris's documented shapes)."""
    if v is None or v.get("off"):
        return {"version": VERSION, "enable": False}
    if kind == "compaction":
        return {"version": VERSION, "enable": True, "config": {"target_file_size_bytes": int(v["target_mb"]) * 1024 * 1024}}
    if kind == "expiry":
        return {"version": VERSION, "enable": True, "config": {"max_snapshot_age_days": int(v["days"]), "min_snapshot_to_keep": 1}}
    return {"version": VERSION, "enable": True, "max_orphan_file_age_in_days": int(v["days"])}


def _values(kind, content):
    """A policy's content → our values ({"off": True} when it's switched off)."""
    try:
        c = json.loads(content) if isinstance(content, str) else (content or {})
    except ValueError:
        return None
    if not c.get("enable", True):
        return {"off": True}
    cfg = c.get("config") or {}
    if kind == "compaction":
        return {"target_mb": max(1, int(cfg.get("target_file_size_bytes", 128 * 2 ** 20)) // 2 ** 20)}
    if kind == "expiry":
        return {"days": int(cfg.get("max_snapshot_age_days", 7))}
    return {"days": int(c.get("max_orphan_file_age_in_days", cfg.get("max_orphan_file_age_in_days", 7)))}


def _scope_word(p, namespace, table):
    """Where an applicable policy was set, in words."""
    if not p.get("inherited"):
        return "here"
    n = p.get("name", "")
    if "__t__" in n:
        return "this table"
    if "__ns__" in n:
        return "namespace " + n.split("__ns__", 1)[1]
    return "the whole catalog"


def applicable(cat, namespace=None, table=None, t=None):
    """{kind: {values, from, name}} — what applies at that scope (the most specific wins)."""
    t = t or lh._admin_token()
    q = {}
    if namespace:
        q["namespace"] = chr(31).join(namespace.split("."))
    if table:
        q["target-name"] = table
    st, body = _call("GET", f"{_P}/{cat}/applicable-policies" + ("?" + urllib.parse.urlencode(q) if q else ""), t=t)
    _err(st, body, "applicable policies")
    out = {}
    for p in (body or {}).get("applicable-policies", []):
        kind = next((k for k, ty in KINDS.items() if ty == p.get("policy-type")), None)
        if kind:
            out[kind] = {"values": _values(kind, p.get("content")), "from": _scope_word(p, namespace, table), "name": p.get("name")}
    return out


def _check_owner(name, cat, t, manager):
    if not (manager or lh._owned(name, cat, t)):
        raise PermissionError("not your catalog")


def get(name, cat, namespace=None, table=None, manager=False):
    """The maintenance panel: per kind, what applies (and from where), whether this scope sets
    its own, and (a table) its last runs."""
    t = lh._admin_token()
    if table:
        if not lh._may_read(name, cat, namespace, table, t, manager):
            raise PermissionError("you can't see this table")
    elif not (manager or lh._owned(name, cat, t)):
        raise PermissionError("not your catalog")
    eff = applicable(cat, namespace, table, t)
    own = {k: (v["values"] if v["from"] == "here" else None) for k, v in eff.items()}
    out = {"kinds": list(KINDS), "defaults": DEFAULTS, "applies": eff, "own": own,
           "can_edit": bool(manager or lh._owned(name, cat, t)), "nightly_utc": f"{NIGHTLY_HOUR:02d}:00"}
    if table:
        out["runs"] = [dict(r, started_at=r["started_at"].isoformat(), finished_at=r["finished_at"].isoformat() if r["finished_at"] else None)
                       for r in _q("SELECT id, started_at, finished_at, ok, trigger, steps, error FROM maint_runs "
                                   "WHERE catalog = %s AND namespace = %s AND tbl = %s ORDER BY started_at DESC LIMIT 10",
                                   (cat, namespace, table))]
    return out


def set_policy(name, cat, namespace, table, kind, values, manager=False):
    """Set this scope's own policy of `kind`: values (on), {"off": True} (off — also stops one
    set higher up), or None (none of its own: what's set higher up applies)."""
    if kind not in KINDS:
        raise MaintError("kind: " + ", ".join(KINDS))
    if table and not namespace:
        raise MaintError("a table needs its namespace")
    t = lh._admin_token()
    _check_owner(name, cat, t, manager)
    if values is not None and not values.get("off"):
        clean = {}
        for k, dv in DEFAULTS[kind].items():
            try:
                v = int(values.get(k, dv))
            except (TypeError, ValueError):
                raise MaintError(f"{k}: a number") from None
            lo, hi = LIMITS[k]
            if not lo <= v <= hi:
                raise MaintError(f"{k}: {lo}–{hi}")
            clean[k] = v
        values = clean
    pname = _policy_name(kind, namespace, table)
    base = f"{_P}/{cat}/namespaces/{POLICY_NS}/policies"
    st, cur = _call("GET", f"{base}/{pname}", t=t)
    if values is None:                                    # none of its own: detach + drop
        if st == 200:
            st, body = _call("DELETE", f"{base}/{pname}?detach-all=true", t=t)
            _err(st, body, "remove policy")
        return get(name, cat, namespace, table, manager)
    lh._call("POST", f"/api/catalog/v1/{cat}/namespaces", {"namespace": [POLICY_NS]}, token=t)   # 409: there already
    content = json.dumps(_content(kind, values))
    where = f"{cat}" + (f".{namespace}" if namespace else "") + (f".{table}" if table else "")
    if st == 200:
        st, body = _call("PUT", f"{base}/{pname}", {"current-policy-version": cur["policy"]["version"],
                                                     "description": f"lab maintenance: {kind} on {where}", "content": content}, t=t)
        _err(st, body, "update policy")
    else:
        st, body = _call("POST", base, {"name": pname, "type": KINDS[kind], "description": f"lab maintenance: {kind} on {where}",
                                        "content": content}, t=t)
        _err(st, body, "create policy")
        st, body = _call("PUT", f"{base}/{pname}/mappings", {"target": _target(cat, namespace, table)}, t=t)
        _err(st, body, "attach policy")
    return get(name, cat, namespace, table, manager)


def forget_namespace(cat, namespace, t=None):
    """A namespace (or its tables) is going: drop the policies set on it, so they don't linger."""
    t = t or lh._admin_token()
    base = f"{_P}/{cat}/namespaces/{POLICY_NS}/policies"
    st, body = _call("GET", base, t=t)
    for p in (body or {}).get("identifiers", []) if st == 200 else []:
        n = p.get("name", "")
        if f"__ns__{namespace}" == n[n.find("__ns__"):] or f"__t__{namespace}__" in n:
            _call("DELETE", f"{base}/{n}?detach-all=true", t=t)


def forget_table(cat, namespace, table, t=None):
    t = t or lh._admin_token()
    for kind in KINDS:
        _call("DELETE", f"{_P}/{cat}/namespaces/{POLICY_NS}/policies/{_policy_name(kind, namespace, table)}?detach-all=true", t=t)


# ---- Trino: the doing --------------------------------------------------------------------------
def _trino(sql, cat):
    """One statement as the lab, with the catalog's minimum retentions lowered for this query only
    (Trino's default refuses anything under 7 days)."""
    session = f"{cat}.expire_snapshots_min_retention=1h,{cat}.remove_orphan_files_min_retention=1h"
    req = urllib.request.Request(f"{lh.TRINO}/v1/statement", data=sql.encode(), method="POST",
                                 headers={"X-Trino-User": "lab", "Content-Type": "text/plain", "X-Trino-Session": session})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            res = json.loads(r.read())
    except OSError as e:
        raise RuntimeError(f"Trino isn't reachable ({getattr(e, 'reason', e)}) — the SQL unit is off: switch it on") from None
    rows = []
    while True:
        if res.get("error"):
            raise RuntimeError(res["error"].get("message", str(res["error"]))[:400])
        rows += res.get("data") or []
        nxt = res.get("nextUri")
        if not nxt:
            return rows
        time.sleep(0.2)
        with urllib.request.urlopen(nxt, timeout=600) as r:
            res = json.loads(r.read())


def _q_ident(*parts):
    return ".".join('"' + p.replace('"', '""') + '"' for p in parts)


def maintain_table(cat, namespace, table, trigger="run now", t=None):
    """Apply what applies to one table → its maint_runs row."""
    t = t or lh._admin_token()
    eff = applicable(cat, namespace, table, t)
    todo = {k: v["values"] for k, v in eff.items() if v["values"] and not v["values"].get("off")}
    rid = _q("INSERT INTO maint_runs (catalog, namespace, tbl, trigger) VALUES (%s, %s, %s, %s) RETURNING id",
             (cat, namespace, table, trigger), one=True)["id"]
    steps, error = [], None
    full = _q_ident(cat, *namespace.split("."), table)
    try:
        if "compaction" in todo:
            mb = todo["compaction"]["target_mb"]
            t0 = time.time()
            rows = _trino(f"ALTER TABLE {full} EXECUTE optimize(file_size_threshold => '{mb}MB')", cat)
            steps.append({"kind": "compaction", "what": f"merged files under {mb} MB", "rows": rows, "s": round(time.time() - t0, 1)})
        if "expiry" in todo:
            d = todo["expiry"]["days"]
            t0 = time.time()
            rows = _trino(f"ALTER TABLE {full} EXECUTE expire_snapshots(retention_threshold => '{d}d')", cat)
            steps.append({"kind": "expiry", "what": f"expired snapshots older than {d} days", "rows": rows, "s": round(time.time() - t0, 1)})
        if "orphans" in todo:
            d = todo["orphans"]["days"]
            t0 = time.time()
            rows = _trino(f"ALTER TABLE {full} EXECUTE remove_orphan_files(retention_threshold => '{d}d')", cat)
            steps.append({"kind": "orphans", "what": f"removed unused files older than {d} days", "rows": rows, "s": round(time.time() - t0, 1)})
        if not todo:
            steps.append({"kind": None, "what": "no maintenance applies to this table"})
    except Exception as e:  # noqa: BLE001 — the run's row says what went wrong
        error = str(e)
    _q("UPDATE maint_runs SET finished_at = now(), ok = %s, steps = %s, error = %s WHERE id = %s",
       (error is None, json.dumps(steps), error, rid), rows=False)
    return {"run": rid, "ok": error is None, "steps": steps, "error": error}


def _tables(cat, namespace=None, t=None):
    """[(namespace, table)] of a catalog (or one namespace), the policy namespace left out."""
    t = t or lh._admin_token()
    if namespace:
        nss = [namespace]
    else:
        st, body = lh._call("GET", f"/api/catalog/v1/{cat}/namespaces", token=t)
        nss = [".".join(n) for n in (body or {}).get("namespaces", [])] if st == 200 else []
    out = []
    for ns in nss:
        if ns == POLICY_NS:
            continue
        st, body = lh._call("GET", f"/api/catalog/v1/{cat}/namespaces/{_ns_path(ns)}/tables", token=t)
        out += [(ns, i["name"]) for i in (body or {}).get("identifiers", [])] if st == 200 else []
    return out


def run(cat, namespace=None, table=None, trigger="run now"):
    """Run now (a table, a namespace, a catalog): every table in it that some policy covers."""
    t = lh._admin_token()
    targets = [(namespace, table)] if table else _tables(cat, namespace, t)
    done = []
    for ns, tb in targets:
        if table or any(v["values"] and not v["values"].get("off") for v in applicable(cat, ns, tb, t).values()):
            done.append(dict(maintain_table(cat, ns, tb, trigger, t), table=f"{ns}.{tb}"))
    return {"tables": len(done), "failed": sum(not d["ok"] for d in done), "runs": done}


def queue(name, cat, namespace=None, table=None, manager=False):
    """Run now from the page: a task for cdc-runner (it can take a while)."""
    import labcdc
    t = lh._admin_token()
    if table:
        if not (manager or lh._owned(name, cat, t)):
            raise PermissionError("not your catalog")
    else:
        _check_owner(name, cat, t, manager)
    return {"task": labcdc.queue(name, "maintain", None, {"catalog": cat, "namespace": namespace, "table": table})}


def nightly_due(now=None):
    """Once a day, from NIGHTLY_HOUR UTC: every catalog → a maintain task (if it has any policy)."""
    import labcdc
    now = now or datetime.datetime.now(datetime.timezone.utc)
    if now.hour < NIGHTLY_HOUR:
        return
    if not _q("INSERT INTO maint_nightly (day) VALUES (%s) ON CONFLICT DO NOTHING RETURNING day", (now.date(),), one=True):
        return                                           # today's round is done (or being done)
    t = lh._admin_token()
    st, body = lh._call("GET", f"{lh._M}/catalogs", token=t)
    for c in (body or {}).get("catalogs", []) if st == 200 else []:
        cat = c["name"]
        st, pol = _call("GET", f"{_P}/{cat}/namespaces/{POLICY_NS}/policies", t=t)
        if st == 200 and (pol or {}).get("identifiers"):
            labcdc.queue("lab", "maintain", None, {"catalog": cat, "trigger": "nightly"})
