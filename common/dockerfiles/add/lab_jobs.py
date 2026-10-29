"""lab_jobs — the logins a member's (or a team's) Airflow task runs with.

TRUSTED code: it uses the platform's admin logins (Polaris root, the object store's root key, the
vault's admin token), so member code must never run where it runs.
  k8s:   the scheduler calls job_env() when it starts the task's pod (airflow_local_settings.
         pod_mutation_hook) and puts the result into THAT pod's env only — the pod has no admin
         login, so a task can only ever act as its DAG's owner.
  local: lab_spark calls it inside the task (the local stack runs member tasks inside Airflow —
         a single-machine test stack, not isolated).

owner = the DAG's folder in the "learners" bundle: a member's bucket prefix (ravi, ravi-k) or
team-<team> for a team's DAGs (team bucket; runs as the team, whoever is in it).

job_env(owner) → {LAB_JOB_OWNER, LAB_JOB_NAME, LAB_JOB_CATALOG (own lakehouse), LAB_JOB_TEAM,
    LAB_JOB_BUCKET, LAB_JOB_CREDENTIAL (Polaris login <name>_jobs, holding the owner's role),
    LAB_JOB_S3_KEY / LAB_JOB_S3_SECRET and AWS_* (storage user <user>-jobs, the owner's storage
    policy), VAULT_TOKEN (12 h; the owner's secrets — a member's also reach their team's)}
The two long-lived logins are kept in the vault's _lab/jobs/<name>, which only the admin token
reaches (they used to be Airflow Variables — readable by every task through Airflow's API).
"""
import os
import re
import secrets
import time
import urllib.error

import lakehouse as lh

TEAM_DIR = "team-"                          # learners/team-<team>/ = a team's DAGs
JOBS = "_lab/jobs"                          # vault (admin only): name → {polaris, s3}
TOKEN_TTL = "12h"                           # a task's vault token
_cache = {}                                 # owner → (time, env): one pod start doesn't log in 4×
CACHE_S = 600


def identity(owner):
    """(name, own catalog, team) of a DAG folder: ravi → (ravi, ravi_lake, their team) ·
    team-sales → (team_sales, team_sales, sales)."""
    name = lh.lake_name(owner)
    if not re.fullmatch(r"[a-z0-9_]+", name) or not re.fullmatch(r"[a-z0-9-]+", owner):
        raise ValueError(f"bad DAG owner {owner!r}")
    if owner.startswith(TEAM_DIR):                            # (usernames team-… are reserved)
        team = owner[len(TEAM_DIR):]
        return name, lh.team_catalog(team), team
    return name, f"{name}_lake", lh.team_of(name)


def _load(name):
    import labvault
    if labvault.enabled():
        st, body = labvault._call("GET", f"{labvault.MOUNT}/data/{JOBS}/{name}")
        return body["data"]["data"] if st == 200 else {}
    from airflow.sdk import Variable                          # no vault: as before
    return Variable.get(f"lab_jobs_{name}", default_var={}, deserialize_json=True)


def _save(name, d):
    import labvault
    if labvault.enabled():
        labvault._ok(labvault._call("POST", f"{labvault.MOUNT}/data/{JOBS}/{name}", {"data": d})[0], "jobs logins")
        return
    from airflow.sdk import Variable
    Variable.set(f"lab_jobs_{name}", d, serialize_json=True)


def _works(cred):
    cid, secret = cred.split(":", 1)
    st, _ = lh._call("POST", "/api/catalog/v1/oauth/tokens", form={
        "grant_type": "client_credentials", "client_id": cid, "client_secret": secret,
        "scope": "PRINCIPAL_ROLE:ALL"})
    return st == 200


def _polaris_login(name, team=None):
    """client_id:secret of <name>_jobs, bound to the owner's principal-role (made on first use, or
    again when the stored one stopped working). team: <name> is that team's job identity."""
    if team and name == lh.team_catalog(team):
        lh.team_jobs_role(team)
    t, m, jobs = lh._admin_token(), "/api/management/v1", f"{name}_jobs"
    lh._ok(lh._call("POST", f"{m}/principals", {"principal": {"name": jobs}}, t)[0], "jobs principal")
    lh._ok(lh._call("PUT", f"{m}/principals/{jobs}/principal-roles", {"principalRole": {"name": name}}, t)[0],
           "jobs role")
    secret = secrets.token_urlsafe(24)
    st, _ = lh._call("POST", f"{m}/principals/{jobs}/reset", {"clientId": jobs, "clientSecret": secret}, t)
    if st != 200:
        raise RuntimeError(f"jobs login for {name}: HTTP {st}")
    return f"{jobs}:{secret}"


def _storage_ok(name, s3):
    key, secret = s3.split(":", 1)
    try:
        lh._s3_as(key, secret, "GET", lh.bucket_name(name), query={"list-type": "2", "max-keys": "1"}).close()
        return True
    except urllib.error.HTTPError:
        return False


def _vault_token(name, team, is_team):
    """A 12-hour token for the owner's vault folder (a member's: + their team's; a team's DAG:
    the team's). Policies member-* only (the token role's rule) → a team's is member-team_<team>."""
    import labvault
    if not labvault.enabled():
        return ""
    if is_team:
        labvault._ok(labvault._call("PUT", f"sys/policies/acl/member-{name}",
                                    {"policy": labvault._team_policy(team)})[0], "team jobs policy")
    else:
        labvault.set_member_policy(name, team)
    st, body = labvault._call("POST", "auth/token/create/member", {
        "policies": [f"member-{name}"], "ttl": TOKEN_TTL, "display_name": f"jobs-{name}",
        "meta": {"member": name, "jobs": "1"}})
    labvault._ok(st, "jobs token")
    return body["auth"]["client_token"]


def job_env(owner):
    """The env a task of `owner`'s DAG runs with (see the module doc). Cached 10 minutes."""
    hit = _cache.get(owner)
    if hit and time.time() - hit[0] < CACHE_S:
        return dict(hit[1])
    name, own, team = identity(owner)
    is_team = own == name
    saved = _load(name)
    d = dict(saved)
    if not (d.get("polaris") and _works(d["polaris"])):
        d["polaris"] = _polaris_login(name, team if is_team else None)
    if not (d.get("s3") and _storage_ok(name, d["s3"])):
        d["s3"] = ":".join(lh.storage_key(name, "jobs"))
    if d != saved:
        _save(name, d)
    key, secret = d["s3"].split(":", 1)
    env = {"LAB_JOB_OWNER": owner, "LAB_JOB_NAME": name, "LAB_JOB_CATALOG": own, "LAB_JOB_TEAM": team or "",
           "LAB_JOB_BUCKET": lh.bucket_name(name), "LAB_JOB_CREDENTIAL": d["polaris"],
           "LAB_JOB_S3_KEY": key, "LAB_JOB_S3_SECRET": secret,
           "AWS_ACCESS_KEY_ID": key, "AWS_SECRET_ACCESS_KEY": secret,
           "VAULT_TOKEN": _vault_token(name, team, is_team)}
    _cache[owner] = (time.time(), env)
    return dict(env)


def forget(name):
    """A member / team is deleted: drop their stored job logins (the principal and storage user
    go with the member's other logins)."""
    import labvault
    _cache.clear()
    if labvault.enabled():
        labvault._call("DELETE", f"{labvault.MOUNT}/metadata/{JOBS}/{name}")
