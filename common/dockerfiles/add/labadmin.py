"""labadmin — the platform manager's member admin, behind the "Manage members" page
(home-api /api/admin/*, managers only). Uses home-api's admin logins: Polaris, storage, SQLPad,
plus the Keycloak admin (KEYCLOAK_ADMIN=user:password) and a JupyterHub service token
(HUB_ADMIN_TOKEN) to stop a learner's Jupyter and drop its volume.

  members()                  lab accounts, their lakehouse and bucket use
  activity()                 📊 Activity: when each member last signed in, used Jupyter, changed
                             files / tables, ran a DAG — to find members who don't use the lab
  ensure_login_events()      Keycloak keeps sign-ins (LOGIN events, 90 days) for activity()
  create(user, password)     new lab account + lakehouse, bucket, SQLPad user
  set_password(user, pw)     new password (the learner picks their own at the next sign-in)
  reset_lakehouse(user)      drop every table/view in their catalogs (bronze/silver/gold and the
                             sample table come back); files, catalogs and shares are kept
  wipe(user)                 back to a new member's setup: only <user>_lake (bronze/silver/gold +
                             sample table), starter files, fresh Jupyter, no secrets; extra
                             catalogs deleted, the shares they gave revoked (account kept)
  delete(user)               remove the account and everything it owns
  registration() / set_registration(on)   self-registration on the sign-in page (Keycloak realm)
  settings() / set_settings(**values)     ⚙️ Settings: the above + lakehouse.SETTINGS (idle minutes,
                                          bucket sizes, Jupyter memory / CPU)

Teams — a member is in no team (only their own lakehouse) until a manager adds them to one; then
in exactly one (Keycloak group team-<team>). The team's lakehouse team_<team>
(lakehouse.create_team_lake) is owned by all its members:
  teams()                    the teams, their members and bucket use
  create_team(team)          group + lakehouse · delete_team(team): only an empty team
  set_team(user, team)       move a member (they lose the old team's lake, gain the new one)
  add_to_team(team, users)   move several members into a team at once · people(): who is where
  remove_from_team(users)    out of their team, back to only their own lakehouse
  ensure_team(user)          re-attach a member's team lake (e.g. after their Polaris role was recreated)
"""
import json
import os
import re
import secrets
import time
import urllib.error
import urllib.parse
import urllib.request

import lakehouse as lh
import labvault

KEYCLOAK = os.environ.get("KEYCLOAK_URL", "http://keycloak:8080")
REALM = os.environ.get("KEYCLOAK_REALM", "de-lab")
HUB_API = os.environ.get("HUB_API_URL", "http://jupyterhub:8081/hub/api")
AIRFLOW = os.environ.get("AIRFLOW_URL", "http://airflow-apiserver:8080")
USERNAME = re.compile(r"^[a-z0-9][a-z0-9-]{1,30}$")
TEAM = re.compile(r"^[a-z][a-z0-9]{1,23}$")
TEAM_GROUP = "team-"
_C = "/api/catalog/v1"


class AdminError(Exception):
    """A problem to show the manager (bad input, unknown user …)."""


def _http(method, url, body=None, headers=None, form=None):
    headers, data = dict(headers or {}), None
    if form is not None:
        data = urllib.parse.urlencode(form).encode()
        headers["Content-Type"] = "application/x-www-form-urlencoded"
    elif body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            raw = r.read()
            return r.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as e:
        return e.code, e.read()[:300]


# ---- Keycloak (admin REST API) ----------------------------------------------------------
def _kc(method, path, body=None, token=None):
    return _http(method, KEYCLOAK + path, body, {"Authorization": f"Bearer {token}"})


def _kc_token():
    user, pw = os.environ.get("KEYCLOAK_ADMIN", "admin:admin").split(":", 1)
    st, body = _http("POST", f"{KEYCLOAK}/realms/master/protocol/openid-connect/token", form={
        "grant_type": "password", "client_id": "admin-cli", "username": user, "password": pw})
    if st != 200:
        raise RuntimeError(f"Keycloak admin login: HTTP {st}")
    return body["access_token"]


def _kc_user(user, t):
    st, body = _kc("GET", f"/admin/realms/{REALM}/users?" +
                   urllib.parse.urlencode({"username": user, "exact": "true"}), token=t)
    return body[0] if st == 200 and body else None


# ---- JupyterHub: stop the learner's server and forget them (the spawner drops the volume) --
def _hub(method, path):
    token = os.environ.get("HUB_ADMIN_TOKEN", "")
    return _http(method, HUB_API + path, headers={"Authorization": f"token {token}"}) if token else (0, None)


def _hub_forget(user):
    """Stop the learner's Jupyter and delete them from the Hub — with them goes their work
    volume (Docker volume / PVC), so old notebooks can't sync back into a wiped bucket. The
    Hub re-creates the user (fresh volume) at their next sign-in. False if it didn't work."""
    st, info = _hub("GET", f"/users/{user}")
    if st == 404:
        return True                                   # never opened Jupyter
    if st != 200:
        return False
    if (info or {}).get("servers"):
        _hub("DELETE", f"/users/{user}/server")
        for _ in range(60):                           # stopping is async (202)
            st, info = _hub("GET", f"/users/{user}")
            if st == 200 and not info.get("servers") and not info.get("pending"):
                break
            time.sleep(1)
    st, _ = _hub("DELETE", f"/users/{user}")
    return st in (204, 404)


# ---- Airflow: the member's role, account, job logins and DAG history (on delete) ----------
def _airflow_auth():
    """Headers for Airflow's API as AIRFLOW_ADMIN=user:password, or None if it can't be reached."""
    adm = os.environ.get("AIRFLOW_ADMIN", "")
    if ":" not in adm:
        return None
    u, p = adm.split(":", 1)
    st, tok = _http("POST", f"{AIRFLOW}/auth/token", {"username": u, "password": p})
    if st not in (200, 201):
        return None
    return {"Authorization": f"Bearer {tok['access_token']}"}


def _airflow_team(user, team):
    """The member's Airflow role for the team's DAGs: lab_team_<team> instead of any other team's,
    at once (the next sign-in would do the same, from the Keycloak group). Best effort — a member
    who never opened Airflow has no account yet."""
    h = _airflow_auth()
    if not h:
        return False
    q = urllib.parse.quote(user)
    st, u = _http("GET", f"{AIRFLOW}/auth/fab/v1/users/{q}", headers=h)
    if st != 200:
        return st == 404
    want = [f"lab_team_{team}"] if team else []                  # team=None: no team's DAGs
    names = [r["name"] for r in u.get("roles", []) if not r["name"].startswith("lab_team_")]
    if "Admin" in names:
        return True
    for w in want:
        _http("POST", f"{AIRFLOW}/auth/fab/v1/roles", {"name": w, "actions": []}, headers=h)   # 409 if there
    st, _ = _http("PATCH", f"{AIRFLOW}/auth/fab/v1/users/{q}?update_mask=roles",
                  {"roles": [{"name": n} for n in names + want]}, headers=h)
    return st == 200


def _airflow_forget(user, name, h=None):
    """Best effort; needs AIRFLOW_ADMIN=user:password. False if Airflow couldn't be reached.
    user=None: a team's job identity team_<team> (its DAGs, job logins, role lab_team_<team>)."""
    h = h or _airflow_auth()
    if not h:
        return False
    _, dags = _http("GET", f"{AIRFLOW}/api/v2/dags?" + urllib.parse.urlencode(
        {"dag_id_pattern": f"{name}_", "limit": 1000}), headers=h)
    for d in (dags or {}).get("dags", []) if isinstance(dags, dict) else []:
        if d["dag_id"].startswith(f"{name}_"):          # their DAG runs and task history
            _http("DELETE", f"{AIRFLOW}/api/v2/dags/{urllib.parse.quote(d['dag_id'])}", headers=h)
    for var in (f"lab_jobs_{name}", f"lab_jobs_s3_{name}"):   # their job logins (lab_spark)
        _http("DELETE", f"{AIRFLOW}/api/v2/variables/{var}", headers=h)
    if user:
        _http("DELETE", f"{AIRFLOW}/auth/fab/v1/users/{urllib.parse.quote(user)}", headers=h)
    for role in {f"lab_{user or name}", f"lab_{name}"}:
        _http("DELETE", f"{AIRFLOW}/auth/fab/v1/roles/{urllib.parse.quote(role)}", headers=h)
    return True


# ---- key vault: the member's secrets ("My secrets") ----------------------------------------
def _vault(fn, name):
    """Best effort: a vault that is down mustn't stop a wipe / delete. None = not reached."""
    if not labvault.enabled():
        return None
    try:
        return fn(name)
    except Exception as e:
        print(f"[labadmin] vault {name}: {e}", flush=True)
        return None


# ---- Polaris: tables of a catalog -------------------------------------------------------
def _ns_path(ns):
    return urllib.parse.quote("\x1f".join(ns), safe="")


def _namespaces(cat, t, parent=None):
    """Every namespace of `cat`, deepest first (so children are dropped before parents)."""
    q = f"?parent={_ns_path(parent)}" if parent else ""
    st, body = lh._call("GET", f"{_C}/{cat}/namespaces{q}", token=t)
    out = []
    for ns in (body or {}).get("namespaces", []) if st == 200 else []:
        out += _namespaces(cat, t, ns) + [ns]
    return out


def _clear_catalog(cat, t, keep=True):
    """Drop every table and view in `cat`, and every namespace but bronze/silver/gold (all of
    them if not keep). The files are removed from the bucket afterwards."""
    dropped = 0
    for ns in _namespaces(cat, t):
        p = f"{_C}/{cat}/namespaces/{_ns_path(ns)}"
        for kind in ("views", "tables"):
            st, body = lh._call("GET", f"{p}/{kind}", token=t)
            for ident in (body or {}).get("identifiers", []) if st == 200 else []:
                name = urllib.parse.quote(ident["name"], safe="")
                lh._ok(lh._call("DELETE", f"{p}/{kind}/{name}", token=t)[0],
                       f"drop {cat}.{'.'.join(ns)}.{ident['name']}")
                dropped += 1
        if not keep or ns not in [[n] for n in lh.NAMESPACES]:
            lh._ok(lh._call("DELETE", p, token=t)[0], f"drop namespace {cat}.{'.'.join(ns)}")
    return dropped


def _content_token():
    """Polaris token of trino_lab: every learner catalog grants it CATALOG_MANAGE_CONTENT
    (root manages catalogs and grants, but can't drop tables in them)."""
    cid, secret = lh.TRINO_LOGIN.split(":", 1)
    st, tok = lh._call("POST", f"{_C}/oauth/tokens", form={
        "grant_type": "client_credentials", "client_id": cid, "client_secret": secret,
        "scope": "PRINCIPAL_ROLE:ALL"})
    if st != 200:
        raise RuntimeError(f"Polaris login {cid}: HTTP {st}")
    return tok["access_token"]


def _owned_catalogs(name, t):
    """The member's own catalogs — not their team's lake (that one is the team's: a reset,
    wipe or delete of the member never touches it)."""
    return [c["name"] for c in lh._get(f"{lh._M}/catalogs", t)["catalogs"]
            if not lh.is_team_catalog(c["name"]) and lh._owned(name, c["name"], t)]


def _bucket_mb(bucket):
    import xml.etree.ElementTree as ET
    total, token = 0, None
    while True:
        q = {"list-type": "2"}
        if token:
            q["continuation-token"] = token
        with lh._s3("GET", bucket, query=q) as r:
            root = ET.fromstring(r.read())
        total += sum(int(c.findtext(f"{lh._NS}Size") or 0) for c in root.findall(f"{lh._NS}Contents"))
        token = root.findtext(f"{lh._NS}NextContinuationToken")
        if root.findtext(f"{lh._NS}IsTruncated") != "true":
            return round(total / 1e6, 1)


def _empty_prefix(bucket, prefix):
    keys = lh.list_all(bucket, prefix)
    for k in keys:
        lh.delete_object(bucket, k)
    return len(keys)


def _check(user, t=None):
    if not USERNAME.match(user or ""):
        raise AdminError("username: 2-31 characters of a-z, 0-9 and -")
    if t and not _kc_user(user, t):
        raise AdminError(f"no lab account {user}")


# ---- the actions ------------------------------------------------------------------------
def members():
    t, kc = lh._admin_token(), _kc_token()
    st, users = _kc("GET", f"/admin/realms/{REALM}/users?max=1000&briefRepresentation=true", token=kc)
    st, mgrs = _kc("GET", f"/admin/realms/{REALM}/groups?search=managers", token=kc)
    gid = next((g["id"] for g in mgrs or [] if g["name"] == "managers"), None)
    managers = set()
    if gid:
        _, members = _kc("GET", f"/admin/realms/{REALM}/groups/{gid}/members?max=1000", token=kc)
        managers = {m["username"] for m in members or []}
    team_of = {m: t_["team"] for t_ in teams(kc) for m in t_["members"]}
    out = []
    for u in sorted(users or [], key=lambda u: u["username"]):
        name = lh.lake_name(u["username"])
        try:
            cats = _owned_catalogs(name, t)
        except RuntimeError:
            cats = []
        try:
            mb = _bucket_mb(lh.bucket_name(name))
        except Exception:
            mb = None
        out.append({"user": u["username"], "manager": u["username"] in managers,
                    "enabled": u.get("enabled", True), "created": u.get("createdTimestamp"),
                    "lakehouse": f"{name}_lake" if f"{name}_lake" in cats else None,
                    "catalogs": cats, "bucket": lh.bucket_name(name), "bucket_mb": mb,
                    "team": team_of.get(u["username"])})
    return out


# ---- 📊 Activity: the latest sign of use, per member (nothing new is tracked but sign-ins) -----
LOGIN_DAYS = 90                                       # Keycloak keeps sign-ins this long
_SINCE_KEY = "login-events-since"                     # in lab-admin: when the sign-ins started


def _ms(iso):
    """'2026-10-16T07:13:17.967Z' → epoch ms (None stays None)."""
    import datetime
    if not iso:
        return None
    return int(datetime.datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp() * 1000)


def ensure_login_events(kc=None):
    """Turn on Keycloak's sign-in log (LOGIN events only, kept LOGIN_DAYS) once; returns since
    when it has been on (epoch ms)."""
    import io
    kc = kc or _kc_token()
    try:
        with lh._s3("GET", "lab-admin", _SINCE_KEY) as r:
            since = int(r.read())
    except urllib.error.HTTPError:
        since = None
    st, cfg = _kc("GET", f"/admin/realms/{REALM}/events/config", token=kc)
    if st == 200 and not (cfg.get("eventsEnabled") and "LOGIN" in cfg.get("enabledEventTypes", [])):
        st, body = _kc("PUT", f"/admin/realms/{REALM}/events/config", {
            "eventsEnabled": True, "enabledEventTypes": ["LOGIN"], "eventsExpiration": LOGIN_DAYS * 86400,
            "eventsListeners": cfg.get("eventsListeners") or ["jboss-logging"],
            "adminEventsEnabled": cfg.get("adminEventsEnabled", False),
            "adminEventsDetailsEnabled": cfg.get("adminEventsDetailsEnabled", False)}, token=kc)
        if st != 204:
            raise RuntimeError(f"Keycloak events: HTTP {st} {body!r}")
        since = None
    if since is None:
        since = int(time.time() * 1000)
        lh.create_bucket("lab-admin")
        lh.put_object("lab-admin", _SINCE_KEY, io.BytesIO(str(since).encode()), len(str(since)), "text/plain")
    return since


def _bucket_activity(bucket):
    """(MB used, newest change under files/, newest under tables/) — epoch ms or None."""
    import xml.etree.ElementTree as ET
    total, newest, token = 0, {"files/": None, "tables/": None}, None
    while True:
        q = {"list-type": "2"}
        if token:
            q["continuation-token"] = token
        try:
            with lh._s3("GET", bucket, query=q) as r:
                root = ET.fromstring(r.read())
        except urllib.error.HTTPError:
            return None, None, None                   # no bucket (yet)
        for c in root.findall(f"{lh._NS}Contents"):
            total += int(c.findtext(f"{lh._NS}Size") or 0)
            key, t = c.findtext(f"{lh._NS}Key") or "", _ms(c.findtext(f"{lh._NS}LastModified"))
            top = key[:key.find("/") + 1]
            if top in newest and not key.endswith("/") and (newest[top] or 0) < (t or 0):
                newest[top] = t
        token = root.findtext(f"{lh._NS}NextContinuationToken")
        if root.findtext(f"{lh._NS}IsTruncated") != "true":
            return round(total / 1e6, 1), newest["files/"], newest["tables/"]


def activity():
    """{members: [{user, manager, enabled, team, created, login, jupyter, jupyter_running, files,
    tables, dag, bucket_mb, last}], login_since} — times in epoch ms (None: never / unknown);
    `last` = the newest of them (not `created`). Starter files count as a file change on day one."""
    from concurrent.futures import ThreadPoolExecutor
    kc = _kc_token()
    try:
        since = ensure_login_events(kc)
    except Exception as e:  # noqa: BLE001 — the rest of the report still comes
        print(f"[labadmin] login events: {e}", flush=True)
        since = None
    people = _people_brief(kc)
    ids = {p.pop("id"): p["user"] for p in people}
    rows = {p["user"]: p for p in people}
    # sign-ins: newest first, so the first one seen per member is their last
    first = 0
    while True:
        st, evs = _kc("GET", f"/admin/realms/{REALM}/events?" + urllib.parse.urlencode(
            {"type": "LOGIN", "first": first, "max": 1000}), token=kc)
        if st != 200 or not evs:
            break
        for e in evs:
            u = rows.get(ids.get(e.get("userId")))
            if u is not None and not u.get("login"):
                u["login"] = e.get("time")
        if len(evs) < 1000:
            break
        first += 1000
    st, hub = _hub("GET", "/users?limit=1000")
    for h in hub or [] if st == 200 else []:
        u = rows.get(h["name"])
        if u is not None:
            u["jupyter"] = _ms(h.get("last_activity"))
            u["jupyter_running"] = bool(h.get("servers"))
    h = _airflow_auth()
    if h:
        names = sorted((lh.lake_name(n) for n in rows), key=len, reverse=True)
        by_name = {lh.lake_name(n): n for n in rows}
        st, runs = _http("GET", f"{AIRFLOW}/api/v2/dags/~/dagRuns?" + urllib.parse.urlencode(
            {"order_by": "-start_date", "limit": 1000}), headers=h)
        for r in (runs or {}).get("dag_runs", []) if st == 200 and isinstance(runs, dict) else []:
            owner = next((n for n in names if r["dag_id"].startswith(n + "_")), None)
            u = rows.get(by_name.get(owner))
            if u is not None and not u.get("dag"):
                u["dag"] = _ms(r.get("start_date") or r.get("logical_date"))
    with ThreadPoolExecutor(8) as pool:
        for u, (mb, files, tables) in zip(rows.values(), pool.map(
                lambda n: _bucket_activity(lh.bucket_name(lh.lake_name(n))), list(rows))):
            u.update(bucket_mb=mb, files=files, tables=tables)
    for u in rows.values():
        u["last"] = max((u.get(k) or 0 for k in ("login", "jupyter", "files", "tables", "dag")), default=0) or None
    return {"members": sorted(rows.values(), key=lambda u: u["user"]), "login_since": since}


def _people_brief(kc):
    """[{id, user, manager, enabled, created, team}] from Keycloak only (no lakehouse calls)."""
    _, users = _kc("GET", f"/admin/realms/{REALM}/users?max=1000&briefRepresentation=true", token=kc)
    _, mgrs = _kc("GET", f"/admin/realms/{REALM}/groups?search=managers", token=kc)
    gid = next((g["id"] for g in mgrs or [] if g["name"] == "managers"), None)
    managers = set()
    if gid:
        _, ms = _kc("GET", f"/admin/realms/{REALM}/groups/{gid}/members?max=1000", token=kc)
        managers = {m["username"] for m in ms or []}
    team_of = {m: t_["team"] for t_ in teams(kc) for m in t_["members"]}
    return [{"id": u["id"], "user": u["username"], "manager": u["username"] in managers, "enabled": u.get("enabled", True),
             "created": u.get("createdTimestamp"), "team": team_of.get(u["username"])}
            for u in users or []]


def create(user, password=None, team=None):
    _check(user)
    if user == "team" or user.startswith(TEAM_GROUP):
        raise AdminError("usernames team and team-… are kept for teams")
    kc, pw = _kc_token(), password or secrets.token_urlsafe(9)
    if team and not _kc_group(TEAM_GROUP + team, kc):
        raise AdminError(f"no team {team}")
    if _kc_user(user, kc):
        raise AdminError(f"{user} already exists")
    st, body = _kc("POST", f"/admin/realms/{REALM}/users", {
        "username": user, "enabled": True,
        "credentials": [{"type": "password", "value": pw, "temporary": True}]}, token=kc)
    if st != 201:
        raise RuntimeError(f"create {user} in Keycloak: HTTP {st} {body!r}")
    name = lh.lake_name(user)
    lh.provision(name)
    team = set_team(user, team)["team"] if team else None
    lh.publish_trino_rules()
    return {"user": user, "password": pw, "lakehouse": f"{name}_lake", "bucket": lh.bucket_name(name),
            "team": team}


def set_password(user, password=None):
    kc = _kc_token()
    _check(user, kc)
    pw = password or secrets.token_urlsafe(9)
    st, body = _kc("PUT", f"/admin/realms/{REALM}/users/{_kc_user(user, kc)['id']}/reset-password",
                   {"type": "password", "value": pw, "temporary": True}, token=kc)
    if st != 204:
        raise RuntimeError(f"reset password: HTTP {st} {body!r}")
    return {"user": user, "password": pw}


def _reset(user, prefix):
    """Drop every table/view in the learner's catalogs, empty `prefix` of their bucket, then
    re-provision (bronze/silver/gold, starter files, sample table)."""
    _check(user)
    name, t = lh.lake_name(user), lh._admin_token()
    cats = _owned_catalogs(name, t)
    if not cats:
        raise AdminError(f"{user} has no lakehouse yet (never signed in)")
    for c in cats:
        lh.trino_catalog(c, t)                    # makes sure trino_lab holds its grant
    ct = _content_token()
    dropped = sum(_clear_catalog(c, ct) for c in cats)
    files = _empty_prefix(lh.bucket_name(name), prefix)
    lh.provision(name)
    return {"user": user, "catalogs": cats, "dropped": dropped, "files_removed": files}


def reset_lakehouse(user):
    return _reset(user, "tables/")


def _drop_catalog(cat, t, ct):
    """A catalog and everything in it: tables, namespaces, its Trino catalog, its roles
    (owner, shares …). Returns how many tables/views were dropped."""
    lh.trino_catalog(cat, t)
    dropped = _clear_catalog(cat, ct, keep=False)
    try:
        lh._trino(f"DROP CATALOG IF EXISTS {cat}")
    except Exception as e:
        print(f"[labadmin] trino drop {cat}: {e}", flush=True)
    st, body = lh._call("GET", f"{lh._M}/catalogs/{cat}/catalog-roles", token=t)
    for r in (body or {}).get("roles", []) if st == 200 else []:
        if r["name"] != "catalog_admin":
            lh._call("DELETE", f"{lh._M}/catalogs/{cat}/catalog-roles/{r['name']}", token=t)
    lh._ok(lh._call("DELETE", f"{lh._M}/catalogs/{cat}", token=t)[0], f"delete catalog {cat}")
    return dropped


def wipe(user):
    """Back to a new member's setup: only <user>_lake (bronze/silver/gold + sample table), the
    starter files, a fresh Jupyter — extra catalogs deleted, the shares they gave revoked. The
    account stays; shares OTHER members gave them stay too (those are the other member's)."""
    _check(user)
    name, t = lh.lake_name(user), lh._admin_token()
    lake, cats = f"{name}_lake", _owned_catalogs(name, t)
    if lake not in cats:
        raise AdminError(f"{user} has no lakehouse yet (never signed in)")
    jupyter = _hub_forget(user)                   # first: a running Jupyter would sync files back
    ct = _content_token()
    extra = [c for c in cats if c != lake]
    dropped = sum(_drop_catalog(c, t, ct) for c in extra)
    lh.trino_catalog(lake, t)
    dropped += _clear_catalog(lake, ct)
    st, body = lh._call("GET", f"{lh._M}/catalogs/{lake}/catalog-roles", token=t)
    shares = [r["name"] for r in (body or {}).get("roles", []) if st == 200 and r["name"].startswith("share_")]
    for r in shares:
        lh._call("DELETE", f"{lh._M}/catalogs/{lake}/catalog-roles/{r}", token=t)
    files = _empty_prefix(lh.bucket_name(name), "")
    secrets_removed = _vault(labvault.clear, name)
    lh.provision(name)
    lh.publish_trino_rules()
    return {"user": user, "dropped": dropped, "catalogs_removed": extra, "shares_revoked": len(shares),
            "files_removed": files, "secrets_removed": secrets_removed, "jupyter_reset": jupyter}


def delete(user):
    _check(user)
    name, t, kc = lh.lake_name(user), lh._admin_token(), _kc_token()
    jupyter = _hub_forget(user)
    bucket, cats = lh.bucket_name(name), _owned_catalogs(name, t)
    ct = _content_token() if cats else None
    for cat in cats:                                     # tables, namespaces, roles, catalog
        _drop_catalog(cat, t, ct)
    _vault(labvault.forget, name)                        # their secrets, vault token and policy
    for pr in (name, f"{name}_jobs"):                    # their login + their Airflow jobs' login
        lh._call("DELETE", f"{lh._M}/principals/{pr}", token=t)
    lh._call("DELETE", f"{lh._M}/principal-roles/{name}", token=t)
    try:
        airflow = _airflow_forget(user, name)
    except Exception as e:
        airflow = False
        print(f"[labadmin] airflow {user}: {e}", flush=True)
    try:                                                 # folders they shared / were shared
        lh.forget_folder_shares(name)
    except Exception as e:  # noqa: BLE001
        print(f"[labadmin] folder shares {user}: {e}", flush=True)
    try:                                                 # bucket, its policy and keys
        lh._s3("HEAD", bucket).close()
        _empty_prefix(bucket, "")
        lh._s3("DELETE", bucket).close()
    except urllib.error.HTTPError:
        pass
    who = lh.storage_user(name)
    for key in (f"{who}-files", f"{who}-jobs"):         # only exist once Jupyter / a job used them
        try:
            lh._admin("DELETE", "remove-user", {"accessKey": key})
        except urllib.error.HTTPError:
            pass
    try:
        lh._admin("DELETE", "remove-canned-policy", {"name": who})
    except urllib.error.HTTPError:
        pass
    try:                                                 # SQLPad user + their Trino connection
        _sqlpad_delete(who)
    except Exception as e:
        print(f"[labadmin] sqlpad {who}: {e}", flush=True)
    u = _kc_user(user, kc)
    if u:
        _kc("DELETE", f"/admin/realms/{REALM}/users/{u['id']}", token=kc)
    lh.publish_trino_rules()
    return {"user": user, "catalogs": cats, "bucket": bucket, "jupyter_reset": jupyter, "airflow_cleaned": airflow}


def registration():
    """Is self-registration on? (Keycloak shows the Register link only when it is.)"""
    st, realm = _kc("GET", f"/admin/realms/{REALM}", token=_kc_token())
    if st != 200:
        raise RuntimeError(f"read realm: HTTP {st}")
    return {"registration": bool(realm.get("registrationAllowed"))}


def set_registration(on):
    st, body = _kc("PUT", f"/admin/realms/{REALM}", {"registrationAllowed": bool(on)}, token=_kc_token())
    if st != 204:
        raise RuntimeError(f"update realm: HTTP {st} {body!r}")
    return registration()


def settings():
    """Everything on ⚙️ Settings: {registration, idle_minutes, quota_mb, team_quota_mb, jupyter_mem_gb,
    jupyter_cpu, limits: {name: [lowest, highest]}}."""
    return {**registration(), **lh.lab_settings(),
            "limits": {k: [lo, hi] for k, (_, lo, hi, _) in lh.SETTINGS.items()}}


def set_settings(**values):
    """Store changed lakehouse.SETTINGS values (ValueError if one is out of range)."""
    lh.save_lab_settings(**values)
    return settings()


def _sqlpad_delete(who):
    import base64
    auth = {"Authorization": "Basic " + base64.b64encode(lh.SQLPAD_ADMIN.encode()).decode()}
    _, conns = _http("GET", f"{lh.SQLPAD}/api/connections", headers=auth)
    for c in conns or []:
        if c["name"] == f"Lakehouse (Trino) — {who}" or (
                c["name"].startswith("Team ") and c["name"].endswith(f" (Trino) — {who}")):
            _http("DELETE", f"{lh.SQLPAD}/api/connections/{c['id']}", headers=auth)
    _, users = _http("GET", f"{lh.SQLPAD}/api/users", headers=auth)
    for u in users or []:
        if u["email"] == who:
            _http("DELETE", f"{lh.SQLPAD}/api/users/{u['id']}", headers=auth)


# ---- teams ------------------------------------------------------------------------------
def _kc_group(group, kc):
    st, found = _kc("GET", f"/admin/realms/{REALM}/groups?" +
                    urllib.parse.urlencode({"search": group, "exact": "true"}), token=kc)
    return next((g for g in found or [] if st == 200 and g["name"] == group), None)


def _kc_user_teams(uid, kc):
    _, groups = _kc("GET", f"/admin/realms/{REALM}/users/{uid}/groups", token=kc)
    return [g for g in groups or [] if g["name"].startswith(TEAM_GROUP)]


def teams(kc=None):
    kc = kc or _kc_token()
    _, groups = _kc("GET", f"/admin/realms/{REALM}/groups?" +
                    urllib.parse.urlencode({"search": TEAM_GROUP, "max": 1000}), token=kc)
    out = []
    for g in sorted((g for g in groups or [] if g["name"].startswith(TEAM_GROUP)), key=lambda g: g["name"]):
        team = g["name"][len(TEAM_GROUP):]
        _, ms = _kc("GET", f"/admin/realms/{REALM}/groups/{g['id']}/members?max=1000&briefRepresentation=true", token=kc)
        try:
            mb = _bucket_mb(lh.team_bucket(team))
        except Exception:
            mb = None
        out.append({"team": team, "members": sorted(m["username"] for m in ms or []),
                    "catalog": lh.team_catalog(team), "bucket": lh.team_bucket(team), "bucket_mb": mb})
    return out


def create_team(team):
    if not TEAM.match(team or "") or team == "lake":
        raise AdminError("team name: 2-24 characters of a-z and 0-9, starting with a letter")
    kc = _kc_token()
    st, body = _kc("POST", f"/admin/realms/{REALM}/groups", {"name": TEAM_GROUP + team}, token=kc)
    if st not in (201, 409):
        raise RuntimeError(f"create group {TEAM_GROUP}{team}: HTTP {st} {body!r}")
    out = lh.create_team_lake(team)
    _vault(labvault.team_setup, team)                     # secret/<team>_team/ in the vault UI
    lh.publish_trino_rules()
    return {"team": team, **out}


def delete_team(team):
    """Only an empty team (move its members first): its lakehouse, bucket and group go."""
    kc = _kc_token()
    g = _kc_group(TEAM_GROUP + (team or ""), kc)
    if not g:
        raise AdminError(f"no team {team}")
    _, ms = _kc("GET", f"/admin/realms/{REALM}/groups/{g['id']}/members?max=1", token=kc)
    if ms:
        raise AdminError(f"{team} still has members — move them to another team first")
    t, cat, bucket = lh._admin_token(), lh.team_catalog(team), lh.team_bucket(team)
    dropped = 0
    if lh._call("GET", f"{lh._M}/catalogs/{cat}", token=t)[0] == 200:
        dropped = _drop_catalog(cat, t, _content_token())
    try:
        lh._s3("HEAD", bucket).close()
        _empty_prefix(bucket, "")
        lh._s3("DELETE", bucket).close()
    except urllib.error.HTTPError:
        pass
    secrets_removed = _vault(labvault.team_forget, team)  # its secrets, vault group, policy
    jobs = cat                                             # the team's DAGs ran as team_<team>
    lh._call("DELETE", f"{lh._M}/principals/{jobs}_jobs", token=t)
    lh._call("DELETE", f"{lh._M}/principal-roles/{jobs}", token=t)
    try:
        _airflow_forget(None, jobs)                        # its DAG history, job logins, role
    except Exception as e:  # noqa: BLE001
        print(f"[labadmin] airflow team {team}: {e}", flush=True)
    who = lh.storage_user(jobs)
    for op, q in (("remove-user", {"accessKey": f"{who}-jobs"}), ("remove-canned-policy", {"name": who})):
        try:
            lh._admin("DELETE", op, q)
        except urllib.error.HTTPError:
            pass
    _kc("DELETE", f"/admin/realms/{REALM}/groups/{g['id']}", token=kc)
    lh.publish_trino_rules()
    return {"team": team, "dropped": dropped, "bucket": bucket, "secrets_removed": secrets_removed}


def set_team(user, team, kc=None, g=None, publish=True):
    """Move <user> into <team> (out of any other) — team=None: out of their team, back to only
    their own lakehouse. The old team's lake is no longer theirs; what they made in it stays there."""
    kc = kc or _kc_token()
    _check(user, kc)
    if team:
        g = g or _kc_group(TEAM_GROUP + team, kc)
        if not g:
            raise AdminError(f"no team {team}")
    uid, name = _kc_user(user, kc)["id"], lh.lake_name(user)
    old = [x for x in _kc_user_teams(uid, kc) if not team or x["id"] != g["id"]]
    for x in old:
        _kc("DELETE", f"/admin/realms/{REALM}/users/{uid}/groups/{x['id']}", token=kc)
        lh.leave_team(name, x["name"][len(TEAM_GROUP):])
    if team:
        st, body = _kc("PUT", f"/admin/realms/{REALM}/users/{uid}/groups/{g['id']}", token=kc)
        if st != 204:
            raise RuntimeError(f"add {user} to {team}: HTTP {st} {body!r}")
        lh.join_team(name, team)
    _, groups = _kc("GET", f"/admin/realms/{REALM}/users/{uid}/groups", token=kc)
    manager = any(x["name"] == "managers" for x in groups or [])
    lh.put_storage_policy(name, admin=manager, team=None if manager else team)   # team bucket files
    _vault(lambda n: labvault.set_member_policy(n, team), name)                  # team secrets
    try:                                                                          # team DAGs in Airflow
        _airflow_team(user, team)
    except Exception as e:  # noqa: BLE001
        print(f"[labadmin] airflow team role {user}: {e}", flush=True)
    try:                                                                          # SQLPad: browse the team
        lh.sqlpad_team_connection(name, team)
    except Exception as e:  # noqa: BLE001
        print(f"[labadmin] sqlpad team connection {user}: {e}", flush=True)
    if publish:
        lh.publish_trino_rules()
    return {"user": user, "team": team, "from": [x["name"][len(TEAM_GROUP):] for x in old]}


def add_to_team(team, users):
    """Move each of `users` into <team> (one Keycloak login, Trino rules written once).
    Returns who moved from where, and who couldn't be moved (with why)."""
    kc = _kc_token()
    g = _kc_group(TEAM_GROUP + (team or ""), kc)
    if not g:
        raise AdminError(f"no team {team}")
    moved, failed = [], []
    for u in dict.fromkeys(x.strip().lower() for x in users or [] if x and x.strip()):
        try:
            moved.append(set_team(u, team, kc, g, publish=False))
        except Exception as e:  # noqa: BLE001 — the others still move
            failed.append({"user": u, "error": str(e)})
    lh.publish_trino_rules()
    return {"team": team, "moved": moved, "failed": failed}


def remove_from_team(users):
    """Take each of `users` out of their team: they keep only their own lakehouse."""
    kc, moved, failed = _kc_token(), [], []
    for u in dict.fromkeys(x.strip().lower() for x in users or [] if x and x.strip()):
        try:
            moved.append(set_team(u, None, kc, publish=False))
        except Exception as e:  # noqa: BLE001 — the others still go
            failed.append({"user": u, "error": str(e)})
    lh.publish_trino_rules()
    return {"moved": moved, "failed": failed}


def people():
    """Every lab account with its team (Keycloak only — quick, for the Teams page)."""
    kc = _kc_token()
    _, users = _kc("GET", f"/admin/realms/{REALM}/users?max=1000&briefRepresentation=true", token=kc)
    team_of = {m: t_["team"] for t_ in teams(kc) for m in t_["members"]}
    return [{"user": u["username"], "team": team_of.get(u["username"])}
            for u in sorted(users or [], key=lambda u: u["username"])]


def lookup(text, me=None, limit=8):
    """Usernames containing `text` (3+ characters), for the invite box — names only, the member
    themselves left out, names starting with `text` first."""
    text = (text or "").strip().lower()
    if len(text) < 3:
        return []
    _, users = _kc("GET", f"/admin/realms/{REALM}/users?" + urllib.parse.urlencode(
        {"username": text, "exact": "false", "briefRepresentation": "true", "max": 50}), token=_kc_token())
    names = [u["username"] for u in users or [] if u.get("enabled", True) and u["username"] != me
             and text in u["username"]]
    return sorted(names, key=lambda n: (not n.startswith(text), n))[:limit]


def ensure_team(user):
    """Re-attach the member's team lake, if they are in a team (e.g. after their Polaris role was
    recreated). A member in no team stays in none — managers add members to teams."""
    kc = _kc_token()
    u = _kc_user(user, kc)
    if not u:
        return None
    mine = _kc_user_teams(u["id"], kc)
    if not mine:
        return None
    team = mine[0]["name"][len(TEAM_GROUP):]
    lh.join_team(lh.lake_name(user), team)
    return team
