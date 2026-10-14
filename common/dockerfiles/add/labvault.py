"""labvault — every member's secrets in the lab's key vault (OpenBao, the "vault" service).
Shared by home-api (the "My secrets" page, Manage members), the JupyterHub spawn hook and
Airflow (key_vault() in member DAGs). Uses the admin token VAULT_ADMIN_TOKEN (vault-init.sh).

Layout: key/value store `secret/` (KV v2), one folder per member: secret/<name>/<key>, each
holding {"value", "secret": "1"|"0", "updated"}. Policy member-<name> reaches only that folder.

member_token(name)       a fresh token for the member's Jupyter (role member, one week); the
                         previous one is revoked, like the Polaris secret at every start
names / get / fields / put / delete   one member's secrets (home-api, Airflow)
clear(name)              delete all their secrets (wipe) · forget(name): + policy, token,
                         vault sign-in record (delete)
import_env_file(name)    move an old "My variables" file (.lab/env.json) into the vault, once
Standard library only.
"""
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request

VAULT = os.environ.get("VAULT_ADDR", "http://vault:8200").rstrip("/")
TOKEN = os.environ.get("VAULT_ADMIN_TOKEN", "")
MOUNT = "secret"
TOKENS = "_lab/tokens"           # admin-only: the accessor of each member's current token
MAX_SECRETS, MAX_VALUE = 50, 4096


def enabled():
    return bool(TOKEN)


def _call(method, path, body=None, token=None):
    headers = {"X-Vault-Token": token or TOKEN}
    data = None
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(f"{VAULT}/v1/{path}", data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            raw = r.read()
            return r.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as e:
        return e.code, None


def _ok(status, what):
    if status not in (200, 204):
        raise RuntimeError(f"vault {what}: HTTP {status}")


def _q(*parts):
    return "/".join(urllib.parse.quote(p, safe="") for p in parts)


def _policy(name):
    return (f'path "{MOUNT}/data/{name}/*" {{ capabilities = ["create", "read", "update", "delete"] }}\n'
            f'path "{MOUNT}/metadata/{name}/*" {{ capabilities = ["read", "list", "delete"] }}\n')


def member_token(name):
    """A new one-week token that reaches only secret/<name>/…; the previous one stops working."""
    _ok(_call("PUT", f"sys/policies/acl/member-{name}", {"policy": _policy(name)})[0], "policy")
    st, old = _call("GET", f"{MOUNT}/data/{TOKENS}/{name}")
    if st == 200:
        _call("POST", "auth/token/revoke-accessor", {"accessor": old["data"]["data"]["accessor"]})
    st, body = _call("POST", "auth/token/create/member", {
        "policies": [f"member-{name}"], "display_name": name, "meta": {"member": name}})
    _ok(st, "member token")
    auth = body["auth"]
    _ok(_call("POST", f"{MOUNT}/data/{TOKENS}/{name}", {"data": {"accessor": auth["accessor"]}})[0],
        "token record")
    return auth["client_token"]


def names(name):
    st, body = _call("GET", f"{MOUNT}/metadata/{_q(name)}/?list=true")
    if st == 404:
        return []
    _ok(st, "list")
    return sorted(k for k in body["data"]["keys"] if not k.endswith("/"))


def get(name, key):
    """{"value", "secret", "updated"} or None."""
    st, body = _call("GET", f"{MOUNT}/data/{_q(name, key)}")
    if st == 404:
        return None
    _ok(st, f"read {key}")
    d = body["data"]["data"]
    if "value" not in d:                 # made in the vault's own UI: any key name(s)
        rest = {k: v for k, v in d.items() if k not in ("secret", "updated")}
        d = {**d, "value": next(iter(rest.values())) if len(rest) == 1 else json.dumps(rest)}
    return {"value": d["value"], "secret": d.get("secret") != "0", "updated": d.get("updated")}


def fields(name, key):
    """Every field of the secret, without the page's own secret / updated; None if there's none."""
    st, body = _call("GET", f"{MOUNT}/data/{_q(name, key)}")
    if st == 404:
        return None
    _ok(st, f"read {key}")
    return {k: v for k, v in body["data"]["data"].items() if k not in ("secret", "updated")}


def put(name, key, value, secret=True, updated=None):
    data = {"value": value, "secret": "1" if secret else "0",
            "updated": updated or time.strftime("%Y-%m-%d %H:%M", time.gmtime())}
    _ok(_call("POST", f"{MOUNT}/data/{_q(name, key)}", {"data": data})[0], f"write {key}")


def delete(name, key):
    """Every version of the secret (metadata delete). False if there was none."""
    if get(name, key) is None:
        return False
    _ok(_call("DELETE", f"{MOUNT}/metadata/{_q(name, key)}")[0], f"delete {key}")
    return True


def clear(name):
    keys = names(name)
    for k in keys:
        _call("DELETE", f"{MOUNT}/metadata/{_q(name, k)}")
    return len(keys)


def forget(name):
    """The member is deleted: their secrets, their token and their policy."""
    n = clear(name)
    st, old = _call("GET", f"{MOUNT}/data/{TOKENS}/{name}")
    if st == 200:
        _call("POST", "auth/token/revoke-accessor", {"accessor": old["data"]["data"]["accessor"]})
    _call("DELETE", f"{MOUNT}/metadata/{TOKENS}/{name}")
    _call("DELETE", f"sys/policies/acl/member-{name}")
    st, auth = _call("GET", "sys/auth/oidc")                # their sign-in record (UI, Keycloak)
    if st == 200:
        st, ent = _call("POST", "identity/lookup/entity", {"alias_name": name, "alias_mount_accessor": auth["accessor"]})
        if st == 200 and ent:
            _call("DELETE", f"identity/entity/id/{ent['data']['id']}")
    return n


def import_env_file(name):
    """The old "My variables" kept values in plain text in the member's bucket (.lab/env.json).
    Move them into the vault (a name already in the vault wins) and delete the file."""
    import lakehouse as lh
    bucket = lh.bucket_name(name)
    try:
        with lh.get_object(bucket, ".lab/env.json") as r:
            data = json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return 0
        raise
    have, n = set(names(name)), 0
    for key, v in data.items():
        if key not in have and isinstance(v, dict):
            put(name, key, str(v.get("value", "")), bool(v.get("secret")), v.get("updated"))
            n += 1
    lh.delete_object(bucket, ".lab/env.json")
    print(f"[labvault] {name}: {n} variable(s) moved from .lab/env.json into the vault", flush=True)
    return n
