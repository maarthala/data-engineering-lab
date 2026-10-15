"""home-api — the landing page's tiny backend (behind oauth2-proxy).

GET /api/me  → who is logged in (from the headers oauth2-proxy sets after the Keycloak
               login) and, on first call, provisions that learner's own lakehouse in
               Polaris: catalog <name>_lake (bronze/silver/gold), principal <name>,
               principal-role <name> owning the catalog, plus read access to the shared
               lake. Idempotent — every step accepts "already exists" (409).

"My files" — the learner's OWN bucket <name>-lake, or with space=team their team's bucket
team-<team>-lake, or with space=shared&owner=<member> a folder that member shared with them.
The bucket always comes from the logged-in user (their team, their folder shares); nothing
from the browser can point at another bucket.
  GET    /api/files?prefix=a/b/            one folder level (folders, files, is-table)
  GET    /api/files/download?key=…          download a file
  PUT    /api/files/upload?key=…            upload (raw body, streamed)
  POST   /api/files/mkdir?prefix=a/new/     new folder
  DELETE /api/files?key=…                   delete a file
  DELETE /api/files/folder?prefix=…[&confirm_table=1]  delete a folder (tables need confirm)
  GET    /api/files/shares              files I share (by_me) / shared with me (with_me) — they
                                         come with a whole-lake invite on My catalogs

"My catalogs" — catalogs the learner owns or that are shared with them; create a catalog
<name>_<x> (stored in their bucket); invite another learner to it (the whole lake or a namespace)
(read / write) and revoke. Polaris enforces the access; ownership is checked here.
  GET    /api/catalogs                                   owned + shared-with-me
  POST   /api/catalogs?name=sales                        create <user>_sales
  GET    /api/catalogs/members?q=…                       usernames containing q (3+ characters), for invites
  GET    /api/catalogs/tree?catalog=…[&namespace=…]     namespaces, or one namespace's tables (owner)
  GET    /api/catalogs/shares?catalog=…                  who has what (owner)
  POST   /api/catalogs/shares?catalog&grantee&access[&namespace]  (no namespace: the whole lake)
  DELETE /api/catalogs/shares?catalog=…&role=…           revoke

"Manage members" — managers only (Keycloak group `managers`). Actions run in the background
(wipe / delete can take a minute); the page polls the job. JSON body {"user", "password"?}.
  GET    /api/admin/members                              every lab account + lakehouse + bucket use
  POST   /api/admin/{create|password|reset|wipe|delete}  → {"job": id}
  GET    /api/admin/jobs?id=…                            → {"state": running|done|error, "result"|"error"}
  GET    /api/admin/settings · POST {"registration": bool}  self-registration on the sign-in page
  GET    /api/admin/teams · /api/admin/people            the teams (members, bucket use) · who is where
  POST   /api/admin/{team-create|team-delete} {"team"} · /api/admin/move {"user", "team"}
         /api/admin/team-add {"team", "users": [...]} · /api/admin/team-remove {"users": [...]}  → {"job": id}

"My secrets" — the learner's own secrets (API keys, passwords…), kept in the lab's key vault
(OpenBao, labvault.py) under secret/<name>/. Notebooks read them with key_vault("NAME") with the
member's own vault token; DAGs the same, through Airflow. Secret values are never sent back.
Changes need the X-Lab-Request header (a plain cross-site form can't send it).
  GET    /api/env                                        names (+ values of non-secrets)
  POST   /api/env  {"name", "value"?, "secret"}          add / change (no value = keep it)
  DELETE /api/env?name=…                                 remove

Only reachable through oauth2-proxy (it is not published), so the identity headers can
be trusted. Standard library only.
"""
import json
import os
import re
import threading
import time
import urllib.error
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import labadmin
import lakehouse as lh
import labvault
from lakehouse import POLARIS, bucket_name, lake_name, provision

MAX_UPLOAD = int(os.environ.get("MAX_UPLOAD_MB", "512")) * 1024 * 1024


class _Body:
    """The request body as a file that ends after Content-Length bytes (the socket
    itself stays open for keep-alive, so reading it to EOF would hang)."""
    def __init__(self, f, length):
        self.f, self.left = f, length

    def read(self, n=-1):
        if self.left <= 0:
            return b""
        n = self.left if n is None or n < 0 else min(n, self.left)
        chunk = self.f.read(n)
        self.left -= len(chunk)
        return chunk

_ready = set()            # learners already provisioned since start (skip the API calls)
_lock = threading.Lock()
_jobs = {}                # manager actions: id → {"state", "result" | "error", …}
_ACTIONS = {"create": labadmin.create, "password": labadmin.set_password,
            "reset": labadmin.reset_lakehouse, "wipe": labadmin.wipe, "delete": labadmin.delete}
_TEAM_ACTIONS = {"team-create": labadmin.create_team, "team-delete": labadmin.delete_team,
                 "move": labadmin.set_team, "team-add": labadmin.add_to_team,
                 "team-remove": labadmin.remove_from_team}

# "My secrets": in the key vault (labvault.py), one folder per member. Names like variables:
# letters, digits and _ (the same names the old "My variables" allowed).
ENV_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,63}$")


class Handler(BaseHTTPRequestHandler):
    def _json(self, code, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    # ---- helpers ------------------------------------------------------------------
    def _user(self):
        return self.headers.get("X-Forwarded-Preferred-Username") or self.headers.get("X-Forwarded-User")

    def _route(self):
        u = urllib.parse.urlparse(self.path)
        return u.path, {k: v[0] for k, v in urllib.parse.parse_qs(u.query).items()}

    def _manager(self):
        groups = (self.headers.get("X-Forwarded-Groups") or "").split(",")
        return "managers" in groups

    def _ensure(self, name):
        with _lock:
            if name not in _ready:
                provision(name, manager=self._manager())
                try:                                 # their team's lake, if a manager put them in one
                    labadmin.ensure_team(self._user())
                except Exception as e:  # noqa: BLE001 — the own lake works without it
                    print(f"[home-api] team for {name}: {e}", flush=True)
                _ready.add(name)

    @staticmethod
    def _clean(key):
        key = (key or "").lstrip("/")
        if not key or any(part == ".." for part in key.split("/")):
            raise ValueError("bad path")
        return key

    def _files(self, method):
        """All /api/files* calls. Bucket = the logged-in learner's own, or (space=team) their team's."""
        path, q = self._route()
        user = self._user()
        if not user:
            return self._json(401, {"error": "not logged in"})
        name = lake_name(user)
        bucket, team, quota = bucket_name(name), None, lh.QUOTA_MB
        share = None                                 # space=shared: the folder share in use
        try:
            self._ensure(name)
            # files shared with me (they come with a whole-lake invite — see lh.share)
            if method == "GET" and path == "/api/files/shares":
                rows = lh.folder_shares()
                return self._json(200, {
                    "by_me": [r for r in rows if r["owner"] == name],
                    "with_me": [dict(r, lake=f"{r['owner']}_lake", path=f"s3a://{bucket_name(r['owner'])}/{r['folder']}")
                                for r in rows if r["grantee"] == name and r["folder"] == "files/"]})
            if q.get("space") == "team":
                team = lh.team_of(name)
                if not team:
                    return self._json(404, {"error": "you are not in a team yet"})
                bucket, quota = lh.team_bucket(team), lh.TEAM_QUOTA_MB
            if q.get("space") == "shared":           # another member's lake, shared with me
                owner = lake_name(q.get("owner", ""))
                where = (q.get("prefix") or q.get("key") or "").lstrip("/")
                share = lh.shared_lake(name, owner, where)
                if not share:
                    return self._json(403, {"error": "that isn't shared with you"})
                if method == "GET" and path == "/api/files" and not where:   # the lake: files + tables
                    return self._json(200, {
                        "bucket": bucket_name(owner), "prefix": "", "team": None, "quota_mb": lh.QUOTA_MB,
                        "share": share, "is_table": False, "files": [],
                        "folders": [{"name": "files", "prefix": "files/"},
                                    {"name": "tables", "prefix": f"tables/{share['lake']}/"}]})
                if method != "GET" and share["access"] != "write":
                    return self._json(403, {"error": "shared with you read-only"})
                if method == "DELETE" and path == "/api/files/folder" and \
                        where.rstrip("/") + "/" == share["folder"]:
                    return self._json(403, {"error": "the files folder itself stays — its owner deletes it"})
                bucket = bucket_name(owner)
            if method == "GET" and path == "/api/files":
                prefix = (q.get("prefix") or "").lstrip("/")
                folders, files = lh.list_dir(bucket, prefix)
                if not prefix:                       # "My variables" file: managed on its own page
                    folders = [f for f in folders if f != ".lab/"]
                kids = {f[len(prefix):] for f in folders}
                return self._json(200, {
                    "bucket": bucket, "prefix": prefix, "team": team, "quota_mb": quota,
                    "share": share,
                    "is_table": {"metadata/", "data/"} <= kids or "metadata/" in kids,
                    "folders": [{"name": f[len(prefix):].rstrip("/"), "prefix": f} for f in folders],
                    "files": [dict(f, name=f["key"][len(prefix):]) for f in files]})
            if method == "GET" and path == "/api/files/download":
                key = self._clean(q.get("key"))
                with lh.get_object(bucket, key) as r:
                    self.send_response(200)
                    self.send_header("Content-Type", r.headers.get("Content-Type", "application/octet-stream"))
                    self.send_header("Content-Length", r.headers.get("Content-Length", "0"))
                    fname = urllib.parse.quote(key.rsplit("/", 1)[-1])
                    self.send_header("Content-Disposition", f"attachment; filename*=UTF-8''{fname}")
                    self.end_headers()
                    while chunk := r.read(1 << 16):
                        self.wfile.write(chunk)
                return
            if method == "PUT" and path == "/api/files/upload":
                key = self._clean(q.get("key"))
                length = int(self.headers.get("Content-Length") or 0)
                if length > MAX_UPLOAD:
                    return self._json(413, {"error": f"file larger than {MAX_UPLOAD >> 20} MB"})
                try:
                    lh.put_object(bucket, key, _Body(self.rfile, length), length,
                                  self.headers.get("Content-Type") or "application/octet-stream")
                except urllib.error.HTTPError as e:
                    msg = e.read().lower()
                    if b"quota exceeded" in msg:
                        return self._json(413, {"error": f"{'The team bucket' if team else 'Your bucket'} is full — "
                                                         f"the limit is {quota} MB. Delete files you no longer need."})
                    if b"quota check temporarily unavailable" in msg:   # brand-new bucket, not scanned yet
                        return self._json(503, {"error": "Your storage is still being prepared — "
                                                         "try the upload again in a few seconds."})
                    raise
                return self._json(200, {"uploaded": key, "path": f"s3a://{bucket}/{key}"})
            if method == "POST" and path == "/api/files/mkdir":
                prefix = self._clean(q.get("prefix")).rstrip("/") + "/"
                import io
                lh.put_object(bucket, prefix, io.BytesIO(b""), 0)
                return self._json(200, {"created": prefix})
            if method == "DELETE" and path == "/api/files":
                key = self._clean(q.get("key"))
                lh.delete_object(bucket, key)
                return self._json(200, {"deleted": key})
            if method == "DELETE" and path == "/api/files/folder":
                prefix = self._clean(q.get("prefix")).rstrip("/") + "/"
                keys = lh.list_all(bucket, prefix)
                if lh.is_table(keys) and q.get("confirm_table") != "1":
                    return self._json(409, {"table": True, "error":
                        "This folder is an Iceberg table — drop it with SQL (DROP TABLE … PURGE) instead."})
                for k in keys:
                    lh.delete_object(bucket, k)
                return self._json(200, {"deleted": len(keys), "prefix": prefix})
            return self._json(404, {"error": "not found"})
        except ValueError as e:
            return self._json(400, {"error": str(e)})
        except PermissionError as e:
            return self._json(403, {"error": str(e)})
        except urllib.error.HTTPError as e:
            return self._json(404 if e.code == 404 else 502, {"error": f"storage: HTTP {e.code}"})
        except Exception as e:                       # show it on the page, don't crash
            return self._json(500, {"error": str(e)})

    def _catalogs(self, method):
        """All /api/catalogs* calls — always on behalf of the logged-in learner."""
        path, q = self._route()
        user = self._user()
        if not user:
            return self._json(401, {"error": "not logged in"})
        name = lake_name(user)
        try:
            self._ensure(name)
            if method == "GET" and path == "/api/catalogs":
                return self._json(200, lh.my_catalogs(name))
            if method == "POST" and path == "/api/catalogs":
                out = lh.create_catalog(name, (q.get("name") or "").strip().lower())
                _rules_now()
                return self._json(200, out)
            if method == "GET" and path == "/api/catalogs/members":
                return self._json(200, labadmin.lookup(q.get("q", ""), user))
            if method == "GET" and path == "/api/catalogs/tree":
                return self._json(200, lh.catalog_tree(name, q.get("catalog", ""), q.get("namespace") or None))
            if method == "GET" and path == "/api/catalogs/shares":
                return self._json(200, lh.list_shares(name, q.get("catalog", "")))
            if method == "POST" and path == "/api/catalogs/shares":
                out = lh.share(name, q.get("catalog", ""), q.get("grantee", "").strip(),
                               q.get("access", "read"), q.get("namespace") or None)
                _rules_now()
                return self._json(200, out)
            if method == "DELETE" and path == "/api/catalogs/shares":
                lh.revoke(name, q.get("catalog", ""), q.get("role", ""))
                _rules_now()
                return self._json(200, {"revoked": q.get("role")})
            return self._json(404, {"error": "not found"})
        except PermissionError as e:
            return self._json(403, {"error": str(e)})
        except ValueError as e:
            return self._json(400, {"error": str(e)})
        except Exception as e:
            return self._json(500, {"error": str(e)})

    def _env(self, method):
        """All /api/env calls — the logged-in learner's own secrets (or with space=team their
        team's), in their vault folder secret/<name>/ (secret/<team>_team/)."""
        path, q = self._route()
        user = self._user()
        if not user:
            return self._json(401, {"error": "not logged in"})
        name = lake_name(user)
        try:
            self._ensure(name)
            if path != "/api/env":
                return self._json(404, {"error": "not found"})
            team = None
            if q.get("space") == "team":
                team = lh.team_of(name)
                if not team:
                    return self._json(404, {"error": "you are not in a team yet"})
                name = labvault.team_folder(team)        # every call below: the team's folder
            if method == "GET":
                if not team:
                    labvault.import_env_file(name)      # once: the old My variables file
                rows = [(k, labvault.get(name, k)) for k in labvault.names(name)]
                return self._json(200, {"team": team, "folder": name, "vars": [
                    {"name": k, "secret": v["secret"], "updated": v["updated"],
                     **({} if v["secret"] else {"value": v["value"]})}
                    for k, v in rows if v]})
            if self.headers.get("X-Lab-Request") != "1":
                return self._json(403, {"error": "missing X-Lab-Request header"})
            with _lock:                                  # one change at a time (count + write)
                if method == "DELETE":
                    key = q.get("name", "")
                    if not ENV_NAME.match(key) or not labvault.delete(name, key):
                        return self._json(404, {"error": f"no secret {key}"})
                    print(f"[home-api] {user}: secret {key} deleted", flush=True)
                    return self._json(200, {"deleted": key})
                if method != "POST":
                    return self._json(404, {"error": "not found"})
                length = int(self.headers.get("Content-Length") or 0)
                body = json.loads(self.rfile.read(length) or b"{}") if length else {}
                key = (body.get("name") or "").strip()
                value = body.get("value")
                if not ENV_NAME.match(key):
                    raise ValueError("name: letters, digits and _ only, not starting with a digit (max 64)")
                old = labvault.get(name, key)
                if value is None:
                    if old is None:
                        raise ValueError("value is required")
                    value = old["value"]                 # edit without retyping a secret
                if not isinstance(value, str) or len(value) > labvault.MAX_VALUE:
                    raise ValueError(f"value: text up to {labvault.MAX_VALUE} characters")
                if old is None and len(labvault.names(name)) >= labvault.MAX_SECRETS:
                    raise ValueError(f"at most {labvault.MAX_SECRETS} secrets")
                labvault.put(name, key, value, bool(body.get("secret")))
            print(f"[home-api] {user}: secret {key} saved", flush=True)   # never the value
            return self._json(200, {"saved": key})
        except ValueError as e:
            return self._json(400, {"error": str(e)})
        except urllib.error.HTTPError as e:              # the bucket (old My variables file)
            return self._json(502, {"error": f"storage: HTTP {e.code}"})
        except urllib.error.URLError as e:
            return self._json(503, {"error": f"The key vault is not reachable — try again shortly ({e.reason})."})
        except Exception as e:
            return self._json(500, {"error": str(e)})

    def _admin(self, method):
        """All /api/admin* calls — managers only. Changes need the X-Lab-Admin header (a plain
        cross-site form can't send it) and act on other accounts, never your own."""
        path, q = self._route()
        user = self._user()
        if not user:
            return self._json(401, {"error": "not logged in"})
        if not self._manager():
            return self._json(403, {"error": "managers only"})
        try:
            if method == "GET" and path == "/api/admin/members":
                return self._json(200, {"me": user, "members": labadmin.members()})
            if method == "GET" and path == "/api/admin/jobs":
                job = _jobs.get(q.get("id", ""))
                if job and job["state"] != "running":
                    _jobs.pop(q["id"], None)          # read once (a new password isn't kept around)
                return self._json(200, job) if job else self._json(404, {"error": "no such job"})
            if method == "GET" and path == "/api/admin/settings":
                return self._json(200, labadmin.registration())
            if method == "GET" and path == "/api/admin/teams":
                return self._json(200, {"teams": labadmin.teams()})
            if method == "GET" and path == "/api/admin/people":
                return self._json(200, {"people": labadmin.people()})
            if method == "POST" and path == "/api/admin/settings":
                if self.headers.get("X-Lab-Admin") != "1":
                    return self._json(403, {"error": "missing X-Lab-Admin header"})
                length = int(self.headers.get("Content-Length") or 0)
                body = json.loads(self.rfile.read(length) or b"{}") if length else {}
                out = labadmin.set_registration(bool(body.get("registration")))
                print(f"[home-api] manager {user}: self-registration {'on' if out['registration'] else 'off'}", flush=True)
                return self._json(200, out)
            action = path.rsplit("/", 1)[-1]
            if method != "POST" or action not in {**_ACTIONS, **_TEAM_ACTIONS}:
                return self._json(404, {"error": "not found"})
            if self.headers.get("X-Lab-Admin") != "1":
                return self._json(403, {"error": "missing X-Lab-Admin header"})
            length = int(self.headers.get("Content-Length") or 0)
            body = json.loads(self.rfile.read(length) or b"{}") if length else {}
            target = (body.get("user") or "").strip().lower()
            team = (body.get("team") or "").strip().lower()
            if action in _TEAM_ACTIONS:
                users = [str(u) for u in body.get("users") or []][:500]
                args = ([target, team or None] if action == "move" else [team, users] if action == "team-add"
                        else [users] if action == "team-remove" else [team])
                jid = __import__("secrets").token_hex(8)
                _jobs[jid] = {"state": "running", "action": action, "user": target or team, "by": user}
                threading.Thread(target=_run_job, args=(jid, _TEAM_ACTIONS[action], args), daemon=True).start()
                print(f"[home-api] manager {user}: {action} {team} {target or ', '.join(users)}", flush=True)
                return self._json(202, {"job": jid})
            if target == user and action in ("wipe", "delete", "reset"):
                return self._json(400, {"error": "that's your own account — ask another manager"})
            if action in ("wipe", "delete") and any(
                    l["user"] == target and l["manager"] for l in labadmin.members()):
                return self._json(400, {"error": f"{target} is a manager — remove them from the "
                                                 "managers group in Keycloak first"})
            args = [target] + ([body["password"]] if body.get("password") and action in ("create", "password") else [])
            if action == "create":
                args = [target, body.get("password") or None, team or None]
            jid = __import__("secrets").token_hex(8)
            _jobs[jid] = {"state": "running", "action": action, "user": target, "by": user}
            threading.Thread(target=_run_job, args=(jid, _ACTIONS[action], args), daemon=True).start()
            print(f"[home-api] manager {user}: {action} {target}", flush=True)
            return self._json(202, {"job": jid})
        except ValueError as e:
            return self._json(400, {"error": str(e)})
        except Exception as e:
            return self._json(500, {"error": str(e)})

    def _dispatch(self, method):
        if self.path.startswith("/api/admin"):
            return self._admin(method)
        if self.path.startswith("/api/catalogs"):
            return self._catalogs(method)
        if self.path.startswith("/api/env"):
            return self._env(method)
        return self._files(method)

    def do_PUT(self):
        return self._dispatch("PUT")

    def do_POST(self):
        return self._dispatch("POST")

    def do_DELETE(self):
        return self._dispatch("DELETE")

    def do_GET(self):
        if self.path == "/api/health":
            return self._json(200, {"ok": True})
        if self.path.startswith("/api/files"):
            return self._files("GET")
        if self.path.startswith("/api/catalogs"):
            return self._catalogs("GET")
        if self.path.startswith("/api/admin"):
            return self._admin("GET")
        if self.path.startswith("/api/env"):
            return self._env("GET")
        if self.path != "/api/me":
            return self._json(404, {"error": "not found"})
        user = self._user()
        if not user:
            return self._json(401, {"error": "not logged in"})
        h = self.headers
        name = lake_name(user)
        me = {"user": user, "email": h.get("X-Forwarded-Email", ""),
              # oauth2-proxy's keycloak provider adds roles as "role:…" — keep real groups only
              "groups": [g for g in (h.get("X-Forwarded-Groups") or "").split(",") if g and not g.startswith("role:")],
              "lakehouse": f"{name}_lake", "bucket": bucket_name(name), "principal": name,
              "quota_mb": lh.QUOTA_MB,
              "airflow_port": os.environ.get("AIRFLOW_HOST_PORT", "")}
        try:
            self._ensure(name)
            me["status"] = "ready"
            me["team"] = lh.team_of(name)
            me["team_lakehouse"] = me["team"] and lh.team_catalog(me["team"])
        except Exception as e:                       # show it on the page, don't crash
            me["status"], me["detail"] = "error", str(e)
        return self._json(200, me)

    def log_message(self, fmt, *args):               # quieter logs: one line per request
        print(f"[home-api] {self.address_string()} {fmt % args}", flush=True)


def _run_job(jid, fn, args):
    try:
        result = fn(*args)
        with _lock:
            _ready.discard(lh.lake_name(args[0]))     # re-provision on their next visit
        _jobs[jid].update(state="done", result=result)
    except labadmin.AdminError as e:
        _jobs[jid].update(state="error", error=str(e))
    except Exception as e:  # noqa: BLE001 — shown on the page
        print(f"[home-api] {_jobs[jid]['action']} {args[0]}: {e}", flush=True)
        _jobs[jid].update(state="error", error=str(e))


def _rules_now():
    """A share / revoke / new catalog shows up in Trino right away (not on the next loop)."""
    try:
        lh.publish_trino_rules()
    except Exception as e:  # noqa: BLE001 — the loop retries
        print(f"[home-api] trino rules: {e}", flush=True)


def _trino_sync_loop():
    """Learner catalogs live in Trino's memory — re-add them (Trino restarts); and keep Trino's
    access rules in step with Polaris (owners, shares) — every 30 s."""
    import time
    try:                                    # teams made before a folder (or team secrets) existed
        for t in labadmin.teams():
            lh.make_team_folders(t["bucket"])
            if labvault.enabled():
                labvault.team_setup(t["team"])
                for m in t["members"]:
                    labvault.set_member_policy(lh.lake_name(m), t["team"])
            for m in t["members"]:
                lh.sqlpad_team_connection(lh.lake_name(m), t["team"])     # SQLPad: browse the team
                try:                       # Airflow: the team's DAGs (else at their next Airflow sign-in)
                    labadmin._airflow_team(m, t["team"])
                except Exception as e:  # noqa: BLE001 — Airflow still starting
                    print(f"[home-api] airflow team role {m}: {e}", flush=True)
    except Exception as e:  # noqa: BLE001
        print(f"[home-api] team folders: {e}", flush=True)
    n = 0
    while True:
        try:
            if n % 2 == 0:
                lh.sync_trino()
            if lh.publish_trino_rules():
                print("[home-api] trino access rules updated", flush=True)
        except Exception as e:
            print(f"[home-api] trino sync: {e}", flush=True)
        n += 1
        time.sleep(30)


if __name__ == "__main__":
    threading.Thread(target=_trino_sync_loop, daemon=True).start()
    port = int(os.environ.get("PORT", "8000"))
    print(f"[home-api] listening on :{port}, Polaris {POLARIS}", flush=True)
    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()
