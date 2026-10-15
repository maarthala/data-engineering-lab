"""labadmin — the platform manager's member admin, behind the "Manage members" page
(home-api /api/admin/*, managers only). Uses home-api's admin logins: Polaris, storage, SQLPad,
plus the Keycloak admin (KEYCLOAK_ADMIN=user:password) and a JupyterHub service token
(HUB_ADMIN_TOKEN) to stop a learner's Jupyter and drop its volume.

  members()                  lab accounts, their lakehouse and bucket use
  create(user, password)     new lab account + lakehouse, bucket, SQLPad user
  set_password(user, pw)     new password (the learner picks their own at the next sign-in)
  reset_lakehouse(user)      drop every table/view in their catalogs (bronze/silver/gold and the
                             sample table come back); files, catalogs and shares are kept
  wipe(user)                 back to a new member's setup: only <user>_lake (bronze/silver/gold +
                             sample table), starter files, fresh Jupyter, no secrets; extra
                             catalogs deleted, the shares they gave revoked (account kept)
  delete(user)               remove the account and everything it owns
  registration() / set_registration(on)   self-registration on the sign-in page (Keycloak realm)

Teams — every member is in exactly one team (Keycloak group team-<team>); the team's lakehouse
team_<team> (lakehouse.create_team_lake) is owned by all its members:
  teams()                    the teams, their members and bucket use
  create_team(team)          group + lakehouse · delete_team(team): only an empty team
  set_team(user, team)       move a member (they lose the old team's lake, gain the new one)
  ensure_team(user)          a member without a team joins DEFAULT_TEAM ("general")
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
DEFAULT_TEAM = os.environ.get("DEFAULT_TEAM", "general")
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
def _airflow_forget(user, name):
    """Best effort; needs AIRFLOW_ADMIN=user:password. False if Airflow couldn't be reached."""
    adm = os.environ.get("AIRFLOW_ADMIN", "")
    if ":" not in adm:
        return False
    u, p = adm.split(":", 1)
    st, tok = _http("POST", f"{AIRFLOW}/auth/token", {"username": u, "password": p})
    if st not in (200, 201):
        return False
    h = {"Authorization": f"Bearer {tok['access_token']}"}
    _, dags = _http("GET", f"{AIRFLOW}/api/v2/dags?" + urllib.parse.urlencode(
        {"dag_id_pattern": f"{name}_", "limit": 1000}), headers=h)
    for d in (dags or {}).get("dags", []) if isinstance(dags, dict) else []:
        if d["dag_id"].startswith(f"{name}_"):          # their DAG runs and task history
            _http("DELETE", f"{AIRFLOW}/api/v2/dags/{urllib.parse.quote(d['dag_id'])}", headers=h)
    for var in (f"lab_jobs_{name}", f"lab_jobs_s3_{name}"):   # their job logins (lab_spark)
        _http("DELETE", f"{AIRFLOW}/api/v2/variables/{var}", headers=h)
    _http("DELETE", f"{AIRFLOW}/auth/fab/v1/users/{urllib.parse.quote(user)}", headers=h)
    for role in {f"lab_{user}", f"lab_{name}"}:
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
    for u in users or []:                     # every member is in a team: e.g. one who never
        if u["username"] not in team_of:      # signed in since teams came → the default team
            try:
                team_of[u["username"]] = ensure_team(u["username"])
            except Exception as e:  # noqa: BLE001 — listed without a team; tried again next time
                print(f"[labadmin] team for {u['username']}: {e}", flush=True)
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
    team = set_team(user, team)["team"] if team else ensure_team(user)
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


def _sqlpad_delete(who):
    import base64
    auth = {"Authorization": "Basic " + base64.b64encode(lh.SQLPAD_ADMIN.encode()).decode()}
    _, conns = _http("GET", f"{lh.SQLPAD}/api/connections", headers=auth)
    for c in conns or []:
        if c["name"] == f"Lakehouse (Trino) — {who}":
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
                    "catalog": lh.team_catalog(team), "bucket": lh.team_bucket(team), "bucket_mb": mb,
                    "default": team == DEFAULT_TEAM})
    return out


def create_team(team):
    if not TEAM.match(team or "") or team == "lake":
        raise AdminError("team name: 2-24 characters of a-z and 0-9, starting with a letter")
    kc = _kc_token()
    st, body = _kc("POST", f"/admin/realms/{REALM}/groups", {"name": TEAM_GROUP + team}, token=kc)
    if st not in (201, 409):
        raise RuntimeError(f"create group {TEAM_GROUP}{team}: HTTP {st} {body!r}")
    out = lh.create_team_lake(team)
    lh.publish_trino_rules()
    return {"team": team, **out}


def delete_team(team):
    """Only an empty team (move its members first): its lakehouse, bucket and group go."""
    kc = _kc_token()
    g = _kc_group(TEAM_GROUP + (team or ""), kc)
    if not g:
        raise AdminError(f"no team {team}")
    if team == DEFAULT_TEAM:
        raise AdminError(f"{team} is the default team — new members join it")
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
    _kc("DELETE", f"/admin/realms/{REALM}/groups/{g['id']}", token=kc)
    lh.publish_trino_rules()
    return {"team": team, "dropped": dropped, "bucket": bucket}


def set_team(user, team):
    """Move <user> into <team> (out of any other): the old team's lake is no longer theirs,
    what they made in it stays there."""
    kc = _kc_token()
    _check(user, kc)
    g = _kc_group(TEAM_GROUP + (team or ""), kc)
    if not g:
        raise AdminError(f"no team {team}")
    uid, name = _kc_user(user, kc)["id"], lh.lake_name(user)
    old = [x for x in _kc_user_teams(uid, kc) if x["id"] != g["id"]]
    for x in old:
        _kc("DELETE", f"/admin/realms/{REALM}/users/{uid}/groups/{x['id']}", token=kc)
        lh.leave_team(name, x["name"][len(TEAM_GROUP):])
    st, body = _kc("PUT", f"/admin/realms/{REALM}/users/{uid}/groups/{g['id']}", token=kc)
    if st != 204:
        raise RuntimeError(f"add {user} to {team}: HTTP {st} {body!r}")
    lh.join_team(name, team)
    lh.publish_trino_rules()
    return {"user": user, "team": team, "from": [x["name"][len(TEAM_GROUP):] for x in old]}


def ensure_team(user):
    """Every member is in one team: one without joins DEFAULT_TEAM (created if missing). Also
    re-attaches the team's lake (e.g. after the member's Polaris role was recreated)."""
    kc = _kc_token()
    u = _kc_user(user, kc)
    if not u:
        return None
    mine = _kc_user_teams(u["id"], kc)
    if mine:
        team = mine[0]["name"][len(TEAM_GROUP):]
        lh.join_team(lh.lake_name(user), team)
        return team
    if not _kc_group(TEAM_GROUP + DEFAULT_TEAM, kc):
        create_team(DEFAULT_TEAM)
    return set_team(user, DEFAULT_TEAM)["team"]
