"""lakehouse — one learner = one Polaris lakehouse. Shared by home-api (first login)
and the JupyterHub spawn hook (every server start).

provision(name)     the learner's own bucket <name>-lake on the object store (RustFS) with a
                    hard quota and a storage policy named after them (their bucket only —
                    managers: everything), and catalog <name>_lake stored in it
                    (bronze/silver/gold), principal <name>, principal-role <name> owning the
                    catalog, read-only on the shared lake. Idempotent (also moves an older
                    catalog's default location into the learner's bucket).
storage_key(name)   a per-learner object-store key (<user>-files, their own policy) with a
                    fresh secret — the Hub hands it to the learner's Jupyter for the
                    "my bucket" drive. Needs the `mc` client (in the Hub image).
reset_secret(name)  give principal <name> a fresh random client secret; returns it. The Hub
                    calls this on each spawn and hands the secret only to that learner's server.
Teams: create_team_lake(team) — catalog team_<team> (bronze/silver/gold) in bucket
                    team-<team>-lake; join_team / leave_team(name, team) — the member owns it
                    (or no longer); team_of(name). Members change, the tables stay.
Standard library only.
"""
import json
import os
import datetime
import hashlib
import hmac
import re
import secrets
import urllib.error
import urllib.parse
import urllib.request

POLARIS = os.environ.get("POLARIS_URL", "http://polaris:8181")
ADMIN = os.environ.get("POLARIS_ADMIN", "root:s3cr3t")
REALM = os.environ.get("POLARIS_REALM", "POLARIS")
SHARED = os.environ.get("SHARED_CATALOG", "polaris_lake")
S3_ENDPOINT = os.environ.get("LAKE_S3_ENDPOINT", "http://storage:9000")
S3_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "admin")
S3_SECRET = os.environ.get("AWS_SECRET_ACCESS_KEY", "admin123")
S3_REGION = os.environ.get("AWS_REGION", "us-east-1")
QUOTA_MB = int(os.environ.get("LEARNER_QUOTA_MB", "100"))      # per-learner bucket, hard limit
TEAM_QUOTA_MB = int(os.environ.get("TEAM_QUOTA_MB", "500"))    # per-team bucket, hard limit
SQLPAD = os.environ.get("SQLPAD_URL", "http://sqlpad:3000")
SQLPAD_ADMIN = os.environ.get("SQLPAD_ADMIN", "admin@de.local:admin1234")
SQLPAD_SHARED_CONNECTIONS = ("shopflow", "adventureworks")         # OLTP, PostgreSQL login `learner`
TRINO_HOST = os.environ.get("TRINO_HOST", "trino")
# k8s injects TRINO_PORT=tcp://<ip>:8080 for the `trino` Service — keep only the number
TRINO_PORT = os.environ.get("TRINO_PORT", "8080").rsplit(":", 1)[-1]
NAMESPACES = ("bronze", "silver", "gold")
TRINO = os.environ.get("TRINO_URL", "http://trino:8080")
TRINO_LOGIN = os.environ.get("TRINO_POLARIS_LOGIN", "trino_lab:trino-lab-secret")   # Trino → learner catalogs
# Credential vending: Polaris hands engines short-lived keys scoped to one table, minted by the
# object store's STS (AssumeRole + a session policy for the table's path). The role ARN is a
# label — RustFS takes any; on AWS it's the IAM role Polaris assumes.
VEND_ROLE = os.environ.get("STORAGE_VENDING_ROLE", "arn:aws:iam::000000000000:role/lab-vending")
# Bucket layout (like a Fabric / Azure lakehouse): files/ = files you work with, tables/ = catalog
# storage. files/src/{notebooks,dags,dbt} mirror Jupyter's notebooks/ + dags/ + dbt/ (dags → Airflow);
# files/source/ = raw files to ingest; tables/<catalog>/… = Iceberg (and Delta) table data.
SRC = "files/src/"
FOLDERS = ("files/src/notebooks/", "files/src/dags/", "files/src/dbt/", "files/source/", "tables/")



def lake_name(username):
    """Keycloak username → a safe Polaris/Iceberg identifier (ravi.k@x → ravi_k_x)."""
    return re.sub(r"[^a-z0-9]+", "_", username.lower()).strip("_") or "learner"


def bucket_name(name):
    """Learner's own bucket. S3 bucket names allow no '_' → ravi_k → ravi-k-lake."""
    return name.replace("_", "-") + "-lake"


def _s3(method, bucket, key="", query=None, body=b"", headers=None, stream=None, length=None):
    """One S3 call to the object store (path-style)."""
    path = "/" + bucket + ("/" + urllib.parse.quote(key, safe="/~") if key else "")
    return _signed(method, path, query, body, headers, stream, length)


def _admin(method, op, query=None, body=b""):
    """Object-store admin API (RustFS speaks the MinIO-compatible /minio/admin/v3/… API), same SigV4 signing."""
    with _signed(method, f"/minio/admin/v3/{op}", query, body, {"content-type": "application/json"}) as r:
        return r.status


def _s3_as(key, secret, method, bucket, k="", query=None):
    """One S3 call signed with another key (e.g. to check a learner's own key still works)."""
    path = "/" + bucket + ("/" + urllib.parse.quote(k, safe="/~") if k else "")
    return _signed(method, path, query, access=(key, secret))


def _signed(method, path, query=None, body=b"", headers=None, stream=None, length=None, access=None):
    """Sign with AWS SigV4 (stdlib only) and send. `stream` + `length` send a file-like
    body unsigned (UNSIGNED-PAYLOAD) without buffering it. `access` = (key, secret) to sign
    as someone else (default: the platform key). Returns the open response."""
    key_id, key_secret = access or (S3_KEY, S3_SECRET)
    host = urllib.parse.urlparse(S3_ENDPOINT).netloc
    now = datetime.datetime.now(datetime.timezone.utc)
    amz_date, day = now.strftime("%Y%m%dT%H%M%SZ"), now.strftime("%Y%m%d")
    qs = "&".join(f"{urllib.parse.quote(k, safe='-_.~')}={urllib.parse.quote(str(v), safe='-_.~')}"
                  for k, v in sorted((query or {}).items()))
    payload = "UNSIGNED-PAYLOAD" if stream is not None else hashlib.sha256(body).hexdigest()
    hdrs = {"host": host, "x-amz-content-sha256": payload, "x-amz-date": amz_date}
    hdrs.update({k.lower(): v for k, v in (headers or {}).items()})
    signed = ";".join(sorted(hdrs))
    canonical = "\n".join([method, path, qs, *(f"{k}:{hdrs[k]}" for k in sorted(hdrs)), "", signed, payload])
    scope = f"{day}/{S3_REGION}/s3/aws4_request"
    to_sign = "\n".join(["AWS4-HMAC-SHA256", amz_date, scope, hashlib.sha256(canonical.encode()).hexdigest()])
    k = f"AWS4{key_secret}".encode()
    for part in (day, S3_REGION, "s3", "aws4_request"):
        k = hmac.new(k, part.encode(), hashlib.sha256).digest()
    sig = hmac.new(k, to_sign.encode(), hashlib.sha256).hexdigest()
    out = {k: v for k, v in hdrs.items() if k != "host"}
    out["Authorization"] = (f"AWS4-HMAC-SHA256 Credential={key_id}/{scope}, "
                            f"SignedHeaders={signed}, Signature={sig}")
    data = stream if stream is not None else (body if method in ("PUT", "POST") else None)
    if stream is not None:
        out["Content-Length"] = str(length)
    req = urllib.request.Request(f"{S3_ENDPOINT}{path}" + (f"?{qs}" if qs else ""),
                                 data=data, method=method, headers=out)
    return urllib.request.urlopen(req, timeout=60)


def create_bucket(bucket):
    """True if created, False if it already exists (ours)."""
    try:
        _s3("PUT", bucket).close()
        return True
    except urllib.error.HTTPError as e:
        if e.code == 409:            # BucketAlreadyOwnedByYou
            return False
        raise RuntimeError(f"create bucket {bucket}: HTTP {e.code} {e.read()[:200]!r}")


def storage_user(name):
    """Keycloak username for a lake name (usernames are a-z0-9- only; lake names use _)."""
    return name.replace("_", "-")


def put_storage_policy(name, admin=False, team=None):
    """Policy named after the learner (Keycloak sends the username as the `policy` claim on
    the object-store console login): their own bucket and their team's bucket — or everything
    for managers. Their Jupyter and job keys carry the same policy, so a move to another team
    takes effect at once. (RustFS doesn't expand ${jwt:…} in resource names, so it's one policy
    per learner.)"""
    buckets = [bucket_name(name)] + ([team_bucket(team)] if team else [])
    doc = {"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Action": ["admin:*"]},
        {"Effect": "Allow", "Action": ["s3:*"], "Resource": ["arn:aws:s3:::*"]}]} if admin else {
        "Version": "2012-10-17", "Statement": [
            {"Effect": "Allow", "Action": ["s3:ListBucket", "s3:GetBucketLocation", "s3:ListBucketMultipartUploads"],
             "Resource": [f"arn:aws:s3:::{b}" for b in buckets]},
            # the console's bucket Settings page — read-only. (RustFS action names; its only quota
            # permission is admin:SetBucketQuota, which would let learners raise their own limit —
            # so the quota card stays hidden for learners.)
            {"Effect": "Allow", "Action": [
                "s3:GetBucketVersioning", "s3:GetBucketTagging", "s3:GetBucketLifecycle",
                "s3:GetBucketEncryption", "s3:GetBucketPolicy", "s3:GetBucketPolicyStatus",
                "s3:GetReplicationConfiguration", "s3:GetBucketNotification", "s3:GetBucketCors",
                "s3:GetBucketObjectLockConfiguration"],
             "Resource": [f"arn:aws:s3:::{b}" for b in buckets]},
            {"Effect": "Allow", "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject",
                                           "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"],
             "Resource": [f"arn:aws:s3:::{b}/*" for b in buckets]}]}
    if not admin:
        doc["Statement"] += _shared_folder_statements(name)
    return _admin("PUT", "add-canned-policy", {"name": storage_user(name)}, json.dumps(doc).encode())


# ---- Shared files: a whole-lake invite (share()) also gives read / write on the owner's files/ ----
# Kept in one JSON file in the platform bucket (members can't reach it); the grantee's storage
# policy above gets a rule per folder, so their Jupyter, DAG and console keys all see it at once.
SHARES_BUCKET, SHARES_KEY = "lab-admin", "folder-shares.json"
_READ = ["s3:GetObject"]
_WRITE = _READ + ["s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"]


def folder_shares():
    """Every folder share: [{owner, folder, grantee, access}]."""
    try:
        with _s3("GET", SHARES_BUCKET, SHARES_KEY) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        if e.code == 404:                          # nothing shared yet (or no bucket yet)
            return []
        raise


def _save_folder_shares(rows):
    import io
    create_bucket(SHARES_BUCKET)
    body = json.dumps(rows, indent=1).encode()
    put_object(SHARES_BUCKET, SHARES_KEY, io.BytesIO(body), len(body), "application/json")


def _shared_folder_statements(name):
    out = []
    for s in folder_shares():
        if s["grantee"] != name:
            continue
        b, f = bucket_name(s["owner"]), s["folder"]           # f ends with /
        out += [{"Effect": "Allow", "Action": ["s3:ListBucket"], "Resource": [f"arn:aws:s3:::{b}"],
                 "Condition": {"StringLike": {"s3:prefix": [f.rstrip("/"), f, f + "*"]}}},
                {"Effect": "Allow", "Action": ["s3:GetBucketLocation"], "Resource": [f"arn:aws:s3:::{b}"]},
                {"Effect": "Allow", "Action": _WRITE if s["access"] == "write" else _READ,
                 "Resource": [f"arn:aws:s3:::{b}/{f}*", f"arn:aws:s3:::{b}/{f.rstrip('/')}"]}]
    return out


def _refresh_policy(name):
    """Rebuild a member's storage policy (after their folder shares changed). Managers reach
    every bucket anyway — their policy stays."""
    try:
        with _signed("GET", "/minio/admin/v3/info-canned-policy", {"name": storage_user(name)}) as r:
            if "admin:*" in r.read().decode():
                return
    except urllib.error.HTTPError:
        pass
    try:
        team = team_of(name)
    except Exception:  # noqa: BLE001 — Polaris down: the own bucket + shares still
        team = None
    put_storage_policy(name, team=team)


def share_folder(name, folder, grantee, access):
    """Give another member read / write on one folder under files/ of your bucket (and everything
    in it, also files added later). Sharing it again changes the access."""
    if access not in ("read", "write"):
        raise ValueError("access must be read or write")
    folder = (folder or "").strip("/") + "/"
    if not folder.startswith("files/") or ".." in folder.split("/"):
        raise ValueError("pick a folder under files/")
    grantee = lake_name(grantee or "")
    if grantee == name:
        raise ValueError("that's you")
    if not grantee or _call("GET", f"{_M}/principal-roles/{grantee}", token=_admin_token())[0] != 200:
        raise ValueError(f"no member called {grantee}")
    rows = [s for s in folder_shares() if (s["owner"], s["folder"], s["grantee"]) != (name, folder, grantee)]
    rows.append({"owner": name, "folder": folder, "grantee": grantee, "access": access})
    _save_folder_shares(rows)
    _refresh_policy(grantee)
    return {"folder": folder, "grantee": grantee, "access": access, "path": f"s3a://{bucket_name(name)}/{folder}"}


def unshare_folder(name, folder, grantee):
    folder = (folder or "").strip("/") + "/"
    rows = folder_shares()
    keep = [s for s in rows if (s["owner"], s["folder"], s["grantee"]) != (name, folder, grantee)]
    if len(keep) == len(rows):
        raise ValueError("not shared")
    _save_folder_shares(keep)
    _refresh_policy(grantee)


def folder_share_of(name, owner, key):
    """The share that lets <name> reach <key> in <owner>'s bucket, or None."""
    best = None
    for s in folder_shares():
        if s["grantee"] == name and s["owner"] == owner and (key + "/").startswith(s["folder"]):
            if not best or len(s["folder"]) > len(best["folder"]):
                best = s
    return best


def forget_folder_shares(name):
    """A member is deleted: drop the folders they shared and the ones shared with them."""
    rows = folder_shares()
    gone = [s for s in rows if name in (s["owner"], s["grantee"])]
    if gone:
        _save_folder_shares([s for s in rows if s not in gone])
        for g in {s["grantee"] for s in gone} - {name}:
            _refresh_policy(g)


def storage_key(name, purpose="files"):
    """(access key, secret) for the learner's own bucket: storage user <user>-<purpose> with the
    learner's storage policy, given a new random secret on every call — one user per purpose
    (files = their Jupyter, jobs = their Airflow tasks) so renewing one never breaks the other.
    (RustFS's admin API wants the MinIO-style encrypted payload for users, so this uses `mc`.)"""
    import subprocess
    user = storage_user(name)
    key, secret = f"{user}-{purpose}", secrets.token_urlsafe(24)
    u = urllib.parse.urlparse(S3_ENDPOINT)
    env = dict(os.environ, MC_HOST_lab=f"{u.scheme}://{S3_KEY}:{S3_SECRET}@{u.netloc}")
    def mc(*args, ok=()):
        r = subprocess.run(["mc", "--json", *args], env=env, capture_output=True, text=True, timeout=30)
        if r.returncode and not any(o in r.stdout + r.stderr for o in ok):
            raise RuntimeError(f"mc {args[1]} {args[2]}: {(r.stdout + r.stderr).strip()[:200]}")
    mc("admin", "user", "add", "lab", key, secret)
    mc("admin", "policy", "attach", "lab", user, "--user", key, ok=("already",))
    return key, secret


_README = {
    "files/src/notebooks/": "Your notebooks — the my_files/notebooks/ folder in Jupyter, kept in sync.\n",
    "files/src/dags/": "Airflow DAGs — the my_files/dags/ folder in Jupyter, kept in sync. Any .py here shows up\n"
                       "in Airflow within ~30 seconds. The dag_id must start with your username + '_',\n"
                       "e.g. dag_id=\"ravi_daily_sales\".\n",
    "files/src/dbt/": "Your dbt projects — the my_files/dbt/ folder in Jupyter, kept in sync (dbt's target/, logs/\n"
                      "and dbt_packages/ are rebuilt on every run and stay in Jupyter only).\n",
    "files/source/": "Raw files to ingest (CSV, Parquet, JSON …) — the 'Read the lake' lesson exports ShopFlow here.\n",
    "tables/": "Table storage for your catalogs (tables/<catalog>/<namespace>/<table>/). Managed by\n"
               "the catalog — create and drop tables with SQL / Spark, don't edit files here.\n",
}


def make_folders(bucket):
    """The layout folders (FOLDERS), each with a README (a real file — an empty "folder marker"
    object shows up as an endless folder-inside-itself in s3fs-based browsers). Best
    effort: a brand-new bucket refuses writes until the quota scanner has seen it — the
    next provision call creates them."""
    import io
    for f in FOLDERS:
        key = f + "README.md"
        try:
            _s3("HEAD", bucket, key).close()
        except urllib.error.HTTPError:
            try:
                body = _README[f].encode()
                put_object(bucket, key, io.BytesIO(body), len(body), "text/markdown")
                delete_object(bucket, f)              # old empty marker, if any
            except urllib.error.HTTPError:
                pass


# Starter files every learner gets, copied from <this dir>/seed/ to the same path in their bucket:
#   seed/files/source/shopflow/*.csv        practice data
#   seed/files/src/notebooks/*.ipynb        starter notebooks (Jupyter's notebooks/ folder)
#   seed/files/src/dags/*.py                starter DAGs (Jupyter's dags/ folder → Airflow)
#   seed/files/src/dbt/shop/                starter dbt project (Jupyter's dbt/ folder)
# Copied once — a learner's own edits or deletions are left alone. In notebooks and DAGs,
# __BUCKET__ / __LAKE__ / __USER__ become the learner's bucket / lakehouse / name, so their code
# shows real names (a DAG's dag_id must start with <user>_).
SEED_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "seed")


def seed_files(bucket):
    import io
    if not os.path.isdir(SEED_DIR):
        return
    lake = bucket[:-len("-lake")].replace("-", "_") + "_lake"
    # On Kubernetes the seed is a ConfigMap mount: `files` is a link into the hidden
    # ..<timestamp>/ folder (with a ..data link beside it) — follow links, skip hidden entries.
    for root, dirs, names in os.walk(SEED_DIR, followlinks=True):
        dirs[:] = [d for d in dirs if not d.startswith(".")]
        for n in (n for n in names if not n.startswith(".")):
            path = os.path.join(root, n)
            key = os.path.relpath(path, SEED_DIR).replace(os.sep, "/")
            try:
                _s3("HEAD", bucket, key).close()
                continue                                  # already there
            except urllib.error.HTTPError:
                pass
            body = open(path, "rb").read()
            if n.endswith((".ipynb", ".py")):
                body = (body.replace(b"__BUCKET__", bucket.encode()).replace(b"__LAKE__", lake.encode())
                        .replace(b"__USER__", lake[:-len("_lake")].encode()))
            ctype = {"csv": "text/csv", "ipynb": "application/x-ipynb+json", "py": "text/x-python",
                     "yml": "text/yaml", "sql": "text/plain", "md": "text/markdown"}.get(n.rsplit(".", 1)[-1],
                                                                                         "application/octet-stream")
            try:
                put_object(bucket, key, io.BytesIO(body), len(body), ctype)
            except urllib.error.HTTPError:                # new bucket not scanned yet — next call
                pass


def set_quota(bucket, mb=QUOTA_MB):
    """Hard quota: uploads that would exceed it are rejected."""
    return _admin("PUT", "set-bucket-quota", {"bucket": bucket},
                  json.dumps({"quota": mb * 1024 * 1024, "quotatype": "hard"}).encode())


# ---- "My files": the learner's own bucket only (callers pass bucket_name(user)) ----
_NS = "{http://s3.amazonaws.com/doc/2006-03-01/}"


def list_dir(bucket, prefix=""):
    """One folder level: ({folders}, [{key,size,modified}]) under `prefix`."""
    import xml.etree.ElementTree as ET
    folders, files, token = [], [], None
    while True:
        q = {"list-type": "2", "delimiter": "/", "prefix": prefix}
        if token:
            q["continuation-token"] = token
        with _s3("GET", bucket, query=q) as r:
            root = ET.fromstring(r.read())
        folders += [p.findtext(f"{_NS}Prefix") for p in root.findall(f"{_NS}CommonPrefixes")]
        for c in root.findall(f"{_NS}Contents"):
            key = c.findtext(f"{_NS}Key")
            if key == prefix:                     # the folder marker itself
                continue
            files.append({"key": key, "size": int(c.findtext(f"{_NS}Size")),
                          "modified": c.findtext(f"{_NS}LastModified")})
        token = root.findtext(f"{_NS}NextContinuationToken")
        if root.findtext(f"{_NS}IsTruncated") != "true":
            return folders, files


def list_all(bucket, prefix):
    """Every key under `prefix` (recursive)."""
    import xml.etree.ElementTree as ET
    keys, token = [], None
    while True:
        q = {"list-type": "2", "prefix": prefix}
        if token:
            q["continuation-token"] = token
        with _s3("GET", bucket, query=q) as r:
            root = ET.fromstring(r.read())
        keys += [c.findtext(f"{_NS}Key") for c in root.findall(f"{_NS}Contents")]
        token = root.findtext(f"{_NS}NextContinuationToken")
        if root.findtext(f"{_NS}IsTruncated") != "true":
            return keys


def is_table(keys):
    """An Iceberg table folder holds metadata/*.metadata.json."""
    return any("/metadata/" in k and k.endswith(".metadata.json") for k in keys)


def put_object(bucket, key, stream, length, content_type="application/octet-stream"):
    _s3("PUT", bucket, key, stream=stream, length=length, headers={"content-type": content_type}).close()


def get_object(bucket, key):
    return _s3("GET", bucket, key)


def delete_object(bucket, key):
    _s3("DELETE", bucket, key).close()


def _call(method, path, body=None, token=None, form=None):
    headers = {"Polaris-Realm": REALM}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    data = None
    if form is not None:
        data = urllib.parse.urlencode(form).encode()
        headers["Content-Type"] = "application/x-www-form-urlencoded"
    elif body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(POLARIS + path, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            raw = r.read()
            return r.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as e:
        return e.code, None


def _ok(status, what):
    if status not in (200, 201, 204, 409):          # 409 = already exists → fine
        raise RuntimeError(f"{what}: HTTP {status}")


def _admin_token():
    cid, secret = ADMIN.split(":", 1)
    st, tok = _call("POST", "/api/catalog/v1/oauth/tokens", form={
        "grant_type": "client_credentials", "client_id": cid, "client_secret": secret,
        "scope": "PRINCIPAL_ROLE:ALL"})
    if st != 200:
        raise RuntimeError(f"Polaris admin login: HTTP {st}")
    return tok["access_token"]


def _sqlpad(method, path, body=None):
    auth = "Basic " + __import__("base64").b64encode(SQLPAD_ADMIN.encode()).decode()
    r = urllib.request.Request(SQLPAD + path, method=method, headers={
        "Authorization": auth, "Content-Type": "application/json"},
        data=None if body is None else json.dumps(body).encode())
    with urllib.request.urlopen(r, timeout=10) as resp:
        return json.loads(resp.read() or b"null")


def sqlpad_team_connection(name, team):
    """The member's second SQLPad connection, "Team <team> (Trino) — <user>": Trino as them, default
    catalog team_<team>, so SQLPad's schema browser shows the team's tables (it browses one catalog
    per connection). Another team's (the member moved) goes; team=None: none."""
    user = storage_user(name)
    label = f"Team {team} (Trino) — {user}" if team else None
    for c in _sqlpad("GET", "/api/connections"):
        if c["name"].startswith("Team ") and c["name"].endswith(f" (Trino) — {user}") and c["name"] != label:
            _sqlpad("DELETE", f"/api/connections/{c['id']}")
    if not team:
        return None
    body = {"name": label, "driver": "trino", "data": {
        "host": TRINO_HOST, "port": TRINO_PORT, "username": name, "catalog": team_catalog(team)}}
    conn = next((c for c in _sqlpad("GET", "/api/connections") if c["name"] == label), None)
    if conn is None:
        conn = _sqlpad("POST", "/api/connections", body)
    elif {k: (conn.get("data") or {}).get(k) for k in body["data"]} != body["data"]:
        conn = _sqlpad("PUT", f"/api/connections/{conn['id']}", body)
    users = {u["email"]: u for u in _sqlpad("GET", "/api/users")}
    u = users.get(user)
    if u and u["role"] != "admin" and not any(
            a["connectionId"] == conn["id"] and a["userId"] == u["id"] for a in _sqlpad("GET", "/api/connection-accesses")):
        _sqlpad("POST", "/api/connection-accesses", {"connectionId": conn["id"], "userId": u["id"], "duration": 0})
    return label


def invite_sqlpad(name, admin=False):
    """Pre-create the learner in SQLPad (user id = Keycloak username). SQLPad's OIDC login
    only auto-creates users whose EMAIL domain is allowed — our learners have no email —
    but it signs in any user that already exists. Managers are SQLPad admins."""
    user, role = storage_user(name), "admin" if admin else "editor"
    auth = "Basic " + __import__("base64").b64encode(SQLPAD_ADMIN.encode()).decode()
    def req(method, path, body=None):
        r = urllib.request.Request(SQLPAD + path, method=method, headers={
            "Authorization": auth, "Content-Type": "application/json"},
            data=None if body is None else json.dumps(body).encode())
        with urllib.request.urlopen(r, timeout=10) as resp:
            return json.loads(resp.read() or b"null")
    users = {u["email"]: u for u in req("GET", "/api/users")}
    if user not in users:
        req("POST", "/api/users", {"email": user, "name": user, "role": role})
        users = {u["email"]: u for u in req("GET", "/api/users")}
    elif users[user]["role"] != role:
        req("PUT", f"/api/users/{users[user]['id']}", {"role": role})
    # Lakehouse SQL: a private Trino connection under the learner's own name (Trino's rules then
    # apply to them); everyone shares only the OLTP connections (PostgreSQL login `learner`).
    accesses = req("GET", "/api/connection-accesses")
    for a in accesses:
        if a["connectionId"] == "__EVERY_CONNECTION__" and a["userId"] == "__EVERYONE__":
            req("PUT", f"/api/connection-accesses/{a['id']}/expire")
    for conn in SQLPAD_SHARED_CONNECTIONS:
        if not any(a["connectionId"] == conn and a["userId"] == "__EVERYONE__" for a in accesses):
            req("POST", "/api/connection-accesses", {"connectionId": conn, "userId": "__EVERYONE__", "duration": 0})
    label = f"Lakehouse (Trino) — {user}"
    # SQLPad keeps driver settings only under "data"; top-level fields are silently dropped
    # (the connection then fails with "Invalid URL"), so send them there and repair old ones.
    body = {"name": label, "driver": "trino", "data": {
        "host": TRINO_HOST, "port": TRINO_PORT, "username": name, "catalog": f"{name}_lake"}}
    conns = {c["name"]: c for c in req("GET", "/api/connections")}
    conn = conns.get(label)
    if conn is None:
        conn = req("POST", "/api/connections", body)
    elif {k: (conn.get("data") or {}).get(k) for k in body["data"]} != body["data"]:
        conn = req("PUT", f"/api/connections/{conn['id']}", body)
    uid = users[user]["id"]
    if role != "admin" and not any(a["connectionId"] == conn["id"] and a["userId"] == uid for a in accesses):
        req("POST", "/api/connection-accesses", {"connectionId": conn["id"], "userId": uid, "duration": 0})


def provision(name, manager=False):
    """Create (or confirm) the learner's bucket, storage policy, lakehouse + Polaris identity,
    and their SQLPad account."""
    bucket = bucket_name(name)
    create_bucket(bucket)
    set_quota(bucket)
    make_folders(bucket)
    seed_files(bucket)
    try:
        team = None if manager else team_of(name)
    except RuntimeError:
        team = None
    put_storage_policy(name, admin=manager, team=team)
    invite_sqlpad(name, admin=manager)
    try:
        sqlpad_team_connection(name, team_of(name))      # (managers too: browse their team)
    except Exception as e:  # noqa: BLE001 — SQLPad down: the next provision call
        print(f"[lakehouse] sqlpad team connection {name}: {e}", flush=True)
    t = _admin_token()
    m, cat = "/api/management/v1", f"{name}_lake"
    loc = f"s3://{bucket}/tables/{cat}"
    _ok(_call("POST", f"{m}/catalogs", {"catalog": {
        "name": cat, "type": "INTERNAL",
        "properties": {"default-base-location": loc, "polaris.config.drop-with-purge.enabled": "true"},
        "storageConfigInfo": storage_config([loc])}}, t)[0], "create catalog")
    _move_into_bucket(cat, loc, t)
    enable_vending(cat, t)
    for ns in NAMESPACES:
        _ok(_call("POST", f"/api/catalog/v1/{cat}/namespaces", {"namespace": [ns]}, t)[0], f"namespace {ns}")
        _relocate_namespace(cat, ns, f"{loc}/{ns}", t)
    _ok(_call("POST", f"{m}/principals", {"principal": {"name": name}}, t)[0], "principal")
    _ok(_call("POST", f"{m}/principal-roles", {"principalRole": {"name": name}}, t)[0], "principal-role")
    _ok(_call("PUT", f"{m}/principals/{name}/principal-roles", {"principalRole": {"name": name}}, t)[0], "assign role")
    # owner of their own lake
    _ok(_call("POST", f"{m}/catalogs/{cat}/catalog-roles", {"catalogRole": {"name": "owner"}}, t)[0], "owner role")
    _ok(_call("PUT", f"{m}/catalogs/{cat}/catalog-roles/owner/grants",
              {"grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}}, t)[0], "owner grant")
    _ok(_call("PUT", f"{m}/principal-roles/{name}/catalog-roles/{cat}", {"catalogRole": {"name": "owner"}}, t)[0],
        "attach owner")
    # read-only on the shared lake (skipped if it hasn't been seeded yet)
    if _call("GET", f"{m}/catalogs/{SHARED}", token=t)[0] == 200:
        enable_vending(SHARED, t)
        _ok(_call("POST", f"{m}/catalogs/{SHARED}/catalog-roles", {"catalogRole": {"name": "learner_reader"}}, t)[0],
            "reader role")
        for ns in NAMESPACES:
            for priv in ("TABLE_LIST", "TABLE_READ_DATA"):
                _ok(_call("PUT", f"{m}/catalogs/{SHARED}/catalog-roles/learner_reader/grants",
                          {"grant": {"type": "namespace", "namespace": [ns], "privilege": priv}}, t)[0], "reader grant")
        _ok(_call("PUT", f"{m}/principal-roles/{name}/catalog-roles/{SHARED}",
                  {"catalogRole": {"name": "learner_reader"}}, t)[0], "attach reader")
    trino_catalog(cat, t)
    sample_table(cat)


def storage_config(locations):
    """Polaris S3 storage config with credential vending (no KMS — RustFS has none)."""
    return {"storageType": "S3", "allowedLocations": list(locations), "endpoint": S3_ENDPOINT,
            "stsEndpoint": S3_ENDPOINT, "roleArn": VEND_ROLE, "kmsUnavailable": True,
            "pathStyleAccess": True, "region": S3_REGION}


def enable_vending(cat, t):
    """Catalogs created before vending: add the STS endpoint / no-KMS flag (idempotent, never
    fails provisioning). Polaris can't ADD a roleArn to an existing catalog — such catalogs are
    recreated once with migrate-vending (k8s/…/migrate_vending.py); until then they're logged."""
    st, c = _call("GET", f"/api/management/v1/catalogs/{cat}", token=t)
    if st != 200:
        return
    storage = c["storageConfigInfo"]
    want = storage_config(storage.get("allowedLocations", []))
    fields = ("stsEndpoint", "kmsUnavailable", "endpoint")
    if any(storage.get(k) != want[k] for k in fields):
        storage.update({k: want[k] for k in fields})
        st = _call("PUT", f"/api/management/v1/catalogs/{cat}", {
            "currentEntityVersion": c["entityVersion"], "properties": c["properties"],
            "storageConfigInfo": storage}, t)[0]
        if st != 200:
            print(f"[lakehouse] vending settings on {cat}: HTTP {st}", flush=True)
    if not storage.get("roleArn"):
        print(f"[lakehouse] {cat} has no vending role yet — run the vending migration", flush=True)


def _move_into_bucket(cat, loc, t):
    """Catalogs created before per-learner buckets stored under s3://demo-bucket/lakes/…:
    make the learner's bucket the default location (old tables keep theirs — still allowed)."""
    st, c = _call("GET", f"/api/management/v1/catalogs/{cat}", token=t)
    if st != 200 or c["properties"].get("default-base-location") == loc:
        return
    storage = c["storageConfigInfo"]
    storage["allowedLocations"] = sorted(set(storage.get("allowedLocations", [])) | {loc})
    props = dict(c["properties"], **{"default-base-location": loc})
    _ok(_call("PUT", f"/api/management/v1/catalogs/{cat}", {
        "currentEntityVersion": c["entityVersion"], "properties": props,
        "storageConfigInfo": storage}, t)[0], f"move {cat} into {loc}")


def _relocate_namespace(cat, ns, want, t):
    """Namespaces remember the location they were created with — point one created under the
    old layout (bucket root) at tables/<catalog>/<ns> (tables already in it keep theirs)."""
    st, b = _call("GET", f"/api/catalog/v1/{cat}/namespaces/{ns}", token=t)
    if st == 200 and (b.get("properties") or {}).get("location", "").rstrip("/") != want:
        _ok(_call("POST", f"/api/catalog/v1/{cat}/namespaces/{ns}/properties",
                  {"updates": {"location": want}, "removals": []}, t)[0], f"relocate {ns}")


def reset_secret(name):
    """New random client secret for principal <name> (client id = name). Returns it."""
    new = secrets.token_urlsafe(24)
    st, _ = _call("POST", f"/api/management/v1/principals/{name}/reset",
                  {"clientId": name, "clientSecret": new}, _admin_token())
    if st != 200:
        raise RuntimeError(f"reset secret for {name}: HTTP {st}")
    return new


# ---- "My catalogs": extra catalogs + sharing (run with the admin token, on behalf of a
# learner — every call checks that the learner OWNS the catalog it changes) ------------
_M = "/api/management/v1"
_NAME = re.compile(r"^[a-z0-9_]{1,24}$")
_SHARE_PRIVS = {
    ("namespace", "read"): ["NAMESPACE_READ_PROPERTIES", "TABLE_LIST", "TABLE_READ_PROPERTIES", "TABLE_READ_DATA"],
    ("namespace", "write"): ["NAMESPACE_READ_PROPERTIES", "TABLE_LIST", "TABLE_READ_PROPERTIES", "TABLE_READ_DATA",
                             "TABLE_WRITE_DATA", "TABLE_WRITE_PROPERTIES", "TABLE_CREATE"],
    ("table", "read"): ["TABLE_READ_PROPERTIES", "TABLE_READ_DATA"],
    ("table", "write"): ["TABLE_READ_PROPERTIES", "TABLE_READ_DATA", "TABLE_WRITE_DATA", "TABLE_WRITE_PROPERTIES"],
    # the whole lake (granted on the catalog: every namespace and table in it, also ones made later)
    ("catalog", "read"): ["NAMESPACE_READ_PROPERTIES", "TABLE_LIST", "TABLE_READ_PROPERTIES", "TABLE_READ_DATA"],
    ("catalog", "write"): ["NAMESPACE_READ_PROPERTIES", "TABLE_LIST", "TABLE_READ_PROPERTIES", "TABLE_READ_DATA",
                           "TABLE_WRITE_DATA", "TABLE_WRITE_PROPERTIES", "TABLE_CREATE", "TABLE_DROP"],
}


def _get(path, t):
    st, body = _call("GET", path, token=t)
    if st != 200:
        raise RuntimeError(f"GET {path}: HTTP {st}")
    return body


def _owned(name, cat, t):
    """The learner owns `cat` if their principal-role holds its `owner` catalog role."""
    st, body = _call("GET", f"{_M}/principal-roles/{name}/catalog-roles/{cat}", token=t)
    return st == 200 and any(r["name"] == "owner" for r in body.get("roles", []))


def my_catalogs(name):
    """Catalogs the learner owns, and catalogs shared with them (with what's shared)."""
    t = _admin_token()
    owned, shared = [], []
    for c in _get(f"{_M}/catalogs", t)["catalogs"]:
        cat = c["name"]
        st, body = _call("GET", f"{_M}/principal-roles/{name}/catalog-roles/{cat}", token=t)
        roles = [r["name"] for r in (body or {}).get("roles", [])] if st == 200 else []
        if "owner" in roles:
            owned.append({"name": cat, "location": c["properties"].get("default-base-location"),
                          "default": cat == f"{name}_lake", "team": is_team_catalog(cat)})
        elif any(r.startswith("share_") for r in roles):
            shared.append({"name": cat, "shares": [_describe_share(cat, r, t) for r in roles if r.startswith("share_")]})
    return {"owned": owned, "shared": shared}


def create_catalog(name, short):
    """New catalog <name>_<short>, stored in the learner's bucket, owned by them."""
    if not _NAME.match(short or "") or short == "lake":
        raise ValueError("catalog name: 1-24 of a-z, 0-9, _ (and not 'lake')")
    if name == "team":
        raise ValueError("team_… catalogs are team lakehouses — a manager creates teams")
    t = _admin_token()
    cat = f"{name}_{short}"
    loc = f"s3://{bucket_name(name)}/tables/{cat}"
    st, _ = _call("POST", f"{_M}/catalogs", {"catalog": {
        "name": cat, "type": "INTERNAL",
        "properties": {"default-base-location": loc, "polaris.config.drop-with-purge.enabled": "true"},
        "storageConfigInfo": storage_config([loc])}}, t)
    if st == 409:
        raise ValueError(f"{cat} already exists")
    _ok(st, "create catalog")
    _ok(_call("POST", f"{_M}/catalogs/{cat}/catalog-roles", {"catalogRole": {"name": "owner"}}, t)[0], "owner role")
    _ok(_call("PUT", f"{_M}/catalogs/{cat}/catalog-roles/owner/grants",
              {"grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}}, t)[0], "owner grant")
    _ok(_call("PUT", f"{_M}/principal-roles/{name}/catalog-roles/{cat}", {"catalogRole": {"name": "owner"}}, t)[0],
        "attach owner")
    trino_catalog(cat, t)
    return {"name": cat, "location": loc}


def catalog_tree(name, cat, namespace=None):
    """Namespaces of a catalog the learner owns, or (namespace given) that namespace's tables —
    the page lists namespaces and loads a namespace's tables when it's opened."""
    t = _admin_token()
    if not _owned(name, cat, t):
        raise PermissionError("not your catalog")
    if namespace:
        ns = urllib.parse.quote(chr(31).join(namespace.split(".")))
        return [i["name"] for i in _get(f"/api/catalog/v1/{cat}/namespaces/{ns}/tables", t).get("identifiers", [])]
    return [".".join(ns) for ns in _get(f"/api/catalog/v1/{cat}/namespaces", t)["namespaces"]]


def _share_role(grantee, access, namespace, table=None):
    if not namespace:                                   # the whole lake
        return f"share_{grantee}_{access}_c_all"
    tail = re.sub(r"[^a-z0-9_]", "_", f"{namespace}__{table}" if table else namespace)
    return f"share_{grantee}_{access}_{'t' if table else 'n'}_{tail}"[:120]


def _describe_share(cat, role, t):
    grants = _get(f"{_M}/catalogs/{cat}/catalog-roles/{role}/grants", t).get("grants", [])
    g = next((x for x in grants if x.get("namespace")), {})      # skip the catalog-level browse grants
    privs = {x["privilege"] for x in grants}
    return {"role": role, "namespace": ".".join(g.get("namespace", [])), "table": g.get("tableName"),
            "access": "write" if "TABLE_WRITE_DATA" in privs else "read",
            "scope": "table" if g.get("tableName") else "namespace" if g else "lake"}


def share(name, cat, grantee, access, namespace=None, table=None):
    """Give another learner read / write on a catalog you own — all of it (namespace empty: every
    namespace and table, also ones made later), or only one namespace or table. The whole of
    your main lake <name>_lake also shares your files (files/ in your bucket), the same access."""
    if access not in ("read", "write"):
        raise ValueError("access must be read or write")
    grantee = lake_name(grantee or "")
    if grantee == name:
        raise ValueError("that's you")
    if is_team_catalog(cat):
        raise ValueError("a team lakehouse is shared with its team — a manager moves members")
    t = _admin_token()
    if not _owned(name, cat, t):
        raise PermissionError("not your catalog")
    if _call("GET", f"{_M}/principal-roles/{grantee}", token=t)[0] != 200:
        raise ValueError(f"no learner called {grantee}")
    ns = namespace.split(".") if namespace else None
    role = _share_role(grantee, access, namespace, table)
    _ok(_call("POST", f"{_M}/catalogs/{cat}/catalog-roles", {"catalogRole": {"name": role}}, t)[0], "share role")
    kind = "table" if table else "namespace" if ns else "catalog"
    for priv in _SHARE_PRIVS[(kind, access)]:
        g = {"type": kind, "privilege": priv}
        if ns:
            g["namespace"] = ns
        if table:
            g["tableName"] = table
        _ok(_call("PUT", f"{_M}/catalogs/{cat}/catalog-roles/{role}/grants", {"grant": g}, t)[0], f"grant {priv}")
    # browse: open the catalog in the Polaris Console + see its namespace names (tables in
    # namespaces that aren't shared stay hidden)
    for priv in ("CATALOG_READ_PROPERTIES", "NAMESPACE_LIST"):
        _ok(_call("PUT", f"{_M}/catalogs/{cat}/catalog-roles/{role}/grants",
                  {"grant": {"type": "catalog", "privilege": priv}}, t)[0], f"grant {priv}")
    _ok(_call("PUT", f"{_M}/principal-roles/{grantee}/catalog-roles/{cat}", {"catalogRole": {"name": role}}, t)[0],
        "assign share")
    files = None
    if not ns and cat == f"{name}_lake":               # the lake = its tables + your files
        files = share_folder(name, "files/", grantee, access)["path"]
    return {"role": role, "grantee": grantee, "access": access, "namespace": namespace, "table": table,
            "files": files}


def list_shares(name, cat):
    """Shares on a catalog you own: who has what."""
    t = _admin_token()
    if not _owned(name, cat, t):
        raise PermissionError("not your catalog")
    out = []
    for r in _get(f"{_M}/catalogs/{cat}/catalog-roles", t).get("roles", []):
        if not r["name"].startswith("share_"):
            continue
        d = _describe_share(cat, r["name"], t)
        who = _get(f"{_M}/catalogs/{cat}/catalog-roles/{r['name']}/principal-roles", t).get("roles", [])
        d["grantees"] = [p["name"] for p in who]
        out.append(d)
    return out


def revoke(name, cat, role):
    """Remove a share (the catalog role) from a catalog you own."""
    if not role.startswith("share_"):
        raise ValueError("not a share")
    t = _admin_token()
    if not _owned(name, cat, t):
        raise PermissionError("not your catalog")
    for p in _get(f"{_M}/catalogs/{cat}/catalog-roles/{role}/principal-roles", t).get("roles", []):
        _ok(_call("DELETE", f"{_M}/principal-roles/{p['name']}/catalog-roles/{cat}/{role}", token=t)[0], "unassign")
        if role.endswith("_c_all") and cat == f"{name}_lake":   # the lake's files went with it
            other = {"read": "write", "write": "read"}[role.split("_")[-3]]
            if _call("GET", f"{_M}/catalogs/{cat}/catalog-roles/share_{p['name']}_{other}_c_all", token=t)[0] == 200:
                share_folder(name, "files/", p["name"], other)        # still invited the other way
                continue
            try:
                unshare_folder(name, "files/", p["name"])
            except ValueError:                         # already stopped on My files
                pass
    _ok(_call("DELETE", f"{_M}/catalogs/{cat}/catalog-roles/{role}", token=t)[0], "delete share")


# ---- Teams: one lakehouse per team, owned by every member of the team -------------------
# A team's catalog team_<team> holds the same `owner` role a member has on their own lake; that
# role is attached to each member's principal-role, so Spark, Trino (SQLPad, Superset) and My
# catalogs treat it like their own. A member who leaves loses the attachment — the catalog,
# its tables and the bucket belong to the team and stay. (Who is in which team: Keycloak
# groups team-<team>, see labadmin.py.)
TEAM_PREFIX = "team_"


def is_team_catalog(cat):
    return cat.startswith(TEAM_PREFIX) and cat != "team_lake"     # (team_lake: a member called team)


def team_catalog(team):
    return TEAM_PREFIX + team


def team_bucket(team):
    """team-<team>-lake (the -lake buckets are what Airflow later reads DAGs from)."""
    return bucket_name(TEAM_PREFIX + team)


_TEAM_README = {
    "files/source/": "Files for the whole team — everyone in the team sees, uploads and reads them\n"
                     "(My files → Team, s3a://<this bucket>/files/source/… in Spark).\n",
    "files/src/notebooks/": "The team's notebooks — the {folder}/notebooks/ folder in everyone's Jupyter.\n",
    "files/src/dags/": "The team's Airflow DAGs — the {folder}/dags/ folder in everyone's Jupyter. They show up\n"
                       "in Airflow for everyone in the team; the dag_id must start with {prefix}. In their\n"
                       "tasks lab_spark() works as the team: iceberg = the team's lakehouse.\n",
    "files/src/dbt/": "The team's dbt projects — the {folder}/dbt/ folder in everyone's Jupyter.\n",
    "tables/": "Table storage of the team's lakehouse (tables/team_<team>/<namespace>/<table>/). Managed\n"
               "by the catalog — create and drop tables with SQL / Spark, don't edit files here.\n",
}


def make_team_folders(bucket, wait=0):
    """The team bucket's folders, each with a README. A brand-new bucket refuses writes until the
    object store's quota scan has seen it — `wait` seconds to keep trying (else best effort, like
    make_folders). Returns how many folders are still missing."""
    import time
    end = time.time() + wait
    while True:
        missing = _team_folders_once(bucket)
        if not missing or time.time() >= end:
            return missing
        time.sleep(2)


def _team_folders_once(bucket):
    import io
    try:                                        # files/shared/ of the first team buckets: only its README → gone
        if list_all(bucket, "files/shared/") == ["files/shared/README.md"]:
            delete_object(bucket, "files/shared/README.md")
    except Exception:  # noqa: BLE001
        pass
    missing = 0
    folder = bucket[len("team-"):-len("-lake")].replace("-", "_") + "_team"     # its Jupyter folder
    for f, text in _TEAM_README.items():
        text = text.replace("{folder}", folder).replace("{prefix}", "team_" + folder[:-len("_team")] + "_")
        try:
            with get_object(bucket, f + "README.md") as r:
                old = r.read()                           # (the folder was called my_team at first;
                if b"my_team/" not in old and (f != "files/src/dags/" or b"dag_id" in old):
                    continue                             #  the dags README had no dag_id rule)
            raise urllib.error.HTTPError(None, 404, "old text", None, None)
        except urllib.error.HTTPError:
            try:
                put_object(bucket, f + "README.md", io.BytesIO(text.encode()), len(text.encode()), "text/markdown")
            except urllib.error.HTTPError:
                missing += 1
    return missing


def create_team_lake(team):
    """Bucket team-<team>-lake + catalog team_<team> with bronze/silver/gold (idempotent)."""
    t, cat, bucket = _admin_token(), team_catalog(team), team_bucket(team)
    create_bucket(bucket)
    set_quota(bucket, TEAM_QUOTA_MB)
    make_team_folders(bucket, wait=60)                 # notebooks / dags / dbt / source, like a member's
    loc = f"s3://{bucket}/tables/{cat}"
    _ok(_call("POST", f"{_M}/catalogs", {"catalog": {
        "name": cat, "type": "INTERNAL",
        "properties": {"default-base-location": loc, "polaris.config.drop-with-purge.enabled": "true"},
        "storageConfigInfo": storage_config([loc])}}, t)[0], "create team catalog")
    for ns in NAMESPACES:
        _ok(_call("POST", f"/api/catalog/v1/{cat}/namespaces", {"namespace": [ns]}, t)[0], f"namespace {ns}")
        _relocate_namespace(cat, ns, f"{loc}/{ns}", t)
    _ok(_call("POST", f"{_M}/catalogs/{cat}/catalog-roles", {"catalogRole": {"name": "owner"}}, t)[0], "owner role")
    _ok(_call("PUT", f"{_M}/catalogs/{cat}/catalog-roles/owner/grants",
              {"grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}}, t)[0], "owner grant")
    trino_catalog(cat, t)
    return {"catalog": cat, "bucket": bucket}


def join_team(name, team):
    """Member <name> owns team_<team> (read and write, like their own lake)."""
    t, path = _admin_token(), f"{_M}/principal-roles/{name}/catalog-roles/{team_catalog(team)}"
    st = _call("PUT", path, {"catalogRole": {"name": "owner"}}, t)[0]
    if st == 404 and _call("GET", f"{_M}/catalogs/{team_catalog(team)}", token=t)[0] == 404:
        create_team_lake(team)                       # the group is there, its lake isn't (yet)
        st = _call("PUT", path, {"catalogRole": {"name": "owner"}}, t)[0]
    _ok(st, f"join {team}")
    make_team_folders(team_bucket(team))           # (teams made before team files had none)


def leave_team(name, team):
    """No more access for <name>; what they made stays in the team's lake."""
    st, _ = _call("DELETE", f"{_M}/principal-roles/{name}/catalog-roles/{team_catalog(team)}/owner",
                  token=_admin_token())
    if st not in (204, 404):
        raise RuntimeError(f"leave {team}: HTTP {st}")


def team_jobs_role(team):
    """What the team's DAGs run as (lab_spark in a team DAG): the Polaris principal-role team_<team>
    — owner of the team's lake, reader of the shared lake — and the storage policy team-<team>
    on the team bucket. Its login team_<team>_jobs is made by lab_spark. Idempotent. (Usernames
    team-… are reserved, so these names never clash with a member's.)"""
    t, cat, role = _admin_token(), team_catalog(team), team_catalog(team)
    _ok(_call("POST", f"{_M}/principal-roles", {"principalRole": {"name": role}}, t)[0], "team principal-role")
    _ok(_call("PUT", f"{_M}/principal-roles/{role}/catalog-roles/{cat}", {"catalogRole": {"name": "owner"}}, t)[0],
        "team owner")
    if _call("GET", f"{_M}/catalogs/{SHARED}", token=t)[0] == 200:
        _ok(_call("PUT", f"{_M}/principal-roles/{role}/catalog-roles/{SHARED}",
                  {"catalogRole": {"name": "learner_reader"}}, t)[0], "team reader")
    put_storage_policy(role)                          # team_sales → policy team-sales on team-sales-lake


def team_of(name):
    """The team whose lake <name> owns, or None (Polaris, so the Hub needn't ask Keycloak)."""
    t = _admin_token()
    for c in _get(f"{_M}/catalogs", t)["catalogs"]:
        if is_team_catalog(c["name"]) and _owned(name, c["name"], t):
            return c["name"][len(TEAM_PREFIX):]
    return None


# ---- Trino (→ SQLPad, Superset): one Trino catalog per learner catalog --------------------
# Trino's own `iceberg` catalog is the shared lake (as root, who has no rights INSIDE learner
# catalogs). Each learner catalog gets a Trino catalog of the same name (kiran_lake,
# kiran_sales …), signed in as the principal trino_lab, which every learner catalog grants
# CATALOG_MANAGE_CONTENT — light isolation: through Trino, learners can reach each other's
# lakes. Trino keeps dynamic catalogs in memory, so home-api re-adds them (sync_trino) after
# a Trino restart.
_trino_ready = set()


def _trino(sql):
    """Run one statement on Trino (REST protocol, stdlib); returns the rows."""
    req = urllib.request.Request(f"{TRINO}/v1/statement", data=sql.encode(), method="POST",
                                 headers={"X-Trino-User": "lab", "Content-Type": "text/plain"})
    with urllib.request.urlopen(req, timeout=30) as r:
        res = json.loads(r.read())
    rows = []
    while True:
        if res.get("error"):
            raise RuntimeError(f"trino: {res['error'].get('message', res['error'])[:200]}")
        rows += res.get("data") or []
        nxt = res.get("nextUri")
        if not nxt:
            return rows
        with urllib.request.urlopen(nxt, timeout=30) as r:
            res = json.loads(r.read())


def _trino_principal(t):
    """trino_lab principal + principal-role, pinned secret (idempotent)."""
    if "principal" in _trino_ready:
        return
    m, (cid, secret) = "/api/management/v1", TRINO_LOGIN.split(":", 1)
    _ok(_call("POST", f"{m}/principals", {"principal": {"name": cid}}, t)[0], "trino principal")
    _ok(_call("POST", f"{m}/principal-roles", {"principalRole": {"name": cid}}, t)[0], "trino role")
    _ok(_call("PUT", f"{m}/principals/{cid}/principal-roles", {"principalRole": {"name": cid}}, t)[0], "trino assign")
    _ok(_call("POST", f"{m}/principals/{cid}/reset", {"clientId": cid, "clientSecret": secret}, t)[0], "trino secret")
    _trino_ready.add("principal")


def trino_catalog(cat, t=None):
    """Make Polaris catalog `cat` queryable in Trino as `cat` (grant + CREATE CATALOG). The shared
    lake too: Trino's `iceberg` is it, and `polaris_lake` its real name (read-only for members)."""
    if not re.fullmatch(r"[a-z0-9_]+", cat):
        return
    t = t or _admin_token()
    m, cid = "/api/management/v1", TRINO_LOGIN.split(":", 1)[0]
    try:
        _trino_principal(t)
        _ok(_call("POST", f"{m}/catalogs/{cat}/catalog-roles", {"catalogRole": {"name": cid}}, t)[0], "trino c-role")
        _ok(_call("PUT", f"{m}/catalogs/{cat}/catalog-roles/{cid}/grants",
                  {"grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}}, t)[0], "trino grant")
        _ok(_call("PUT", f"{m}/principal-roles/{cid}/catalog-roles/{cat}", {"catalogRole": {"name": cid}}, t)[0],
            "trino attach")
        if [cat] in _trino("SHOW CATALOGS"):
            return
        u = urllib.parse.urlparse(S3_ENDPOINT)
        props = {
            "iceberg.catalog.type": "rest",
            "iceberg.rest-catalog.uri": f"{POLARIS}/api/catalog",
            "iceberg.rest-catalog.warehouse": cat,
            "iceberg.rest-catalog.security": "OAUTH2",
            "iceberg.rest-catalog.oauth2.credential": TRINO_LOGIN,
            "iceberg.rest-catalog.oauth2.scope": "PRINCIPAL_ROLE:ALL",
            "iceberg.unique-table-location": "false",      # tables/<cat>/<ns>/<table>/ like Spark (no -uuid)
            "fs.native-s3.enabled": "true",
            "s3.endpoint": f"{u.scheme}://{u.netloc}",
            "s3.path-style-access": "true",
            "s3.region": S3_REGION,
            "s3.aws-access-key": S3_KEY,
            "s3.aws-secret-key": S3_SECRET,
        }
        _trino(f"CREATE CATALOG {cat} USING iceberg WITH (" +
               ", ".join(f'"{k}" = \'{v}\'' for k, v in props.items()) + ")")
    except Exception as e:                      # Trino down / not dynamic: tables still work in Spark
        if "already exists" not in str(e):       # (two syncs racing — harmless)
            print(f"[lakehouse] trino catalog {cat}: {e}", flush=True)


def sample_table(cat):
    """One ready-made table to practise SQL on from day one: <cat>.bronze.sample_orders — the
    first 1,000 ShopFlow orders, created through Trino once (never overwritten)."""
    try:
        _trino(f"CREATE TABLE IF NOT EXISTS {cat}.bronze.sample_orders AS "
               "SELECT * FROM shopflow.public.orders ORDER BY order_id LIMIT 1000")
    except Exception as e:                      # Trino not up yet — the next provision call
        print(f"[lakehouse] sample table {cat}: {e}", flush=True)


def sync_trino():
    """Re-add every learner catalog to Trino (after a Trino restart they're gone)."""
    t = _admin_token()
    for c in _get(f"/api/management/v1/catalogs", t)["catalogs"]:
        trino_catalog(c["name"], t)


# ---- Trino access rules (who may query what) --------------------------------------------
# Learners reach Trino only through SQLPad (a private connection under their own name) and
# Superset (queries as the signed-in user); on k8s a NetworkPolicy keeps everyone else out. The
# rules below mirror Polaris: a learner gets their own catalogs, exactly what was shared with
# them, the shared lake read-only and the OLTP sources read-only (the PostgreSQL `learner` login
# is read-only too: the sources play production). Rewritten by home-api whenever Polaris changes (Trino re-reads
# the file every 30 s): a file locally, the ConfigMap trino-access on Kubernetes.
TRINO_ADMINS = os.environ.get("TRINO_ADMIN_USERS", "lab|manager|admin")
TRINO_RULES_PATH = os.environ.get("TRINO_RULES_PATH", "")          # local: a bind-mounted file
TRINO_RULES_CONFIGMAP = os.environ.get("TRINO_RULES_CONFIGMAP", "")  # k8s: ConfigMap name
_ALL = ["SELECT", "INSERT", "DELETE", "UPDATE", "OWNERSHIP", "GRANT_SELECT"]


def trino_rules():
    """Trino file-based access-control rules, built from Polaris (owners + shares)."""
    t = _admin_token()
    owned, shares = [], []                  # (user, catalog) / (user, catalog, ns, table, access)
    for c in _get(f"{_M}/catalogs", t)["catalogs"]:
        cat = c["name"]
        if cat == SHARED:
            continue
        for r in _get(f"{_M}/catalogs/{cat}/catalog-roles", t).get("roles", []):
            holders = [p["name"] for p in _get(f"{_M}/catalogs/{cat}/catalog-roles/{r['name']}/principal-roles", t).get("roles", [])]
            if r["name"] == "owner":
                owned += [(h, cat) for h in holders]
            elif r["name"].startswith("share_"):
                d = _describe_share(cat, r["name"], t)
                shares += [(h, cat, d["namespace"], d["table"], d["access"]) for h in holders]
    q = re.escape
    catalogs = [{"user": TRINO_ADMINS, "allow": "owner"}]       # owner: may also (re)create catalogs
    catalogs += [{"user": q(u), "catalog": q(c), "allow": "all"} for u, c in owned]
    write = {(u, c) for u, c, _, _, a in shares if a == "write"}
    catalogs += [{"user": q(u), "catalog": q(c), "allow": "all" if (u, c) in write else "read-only"}
                 for u, c in sorted({(u, c) for u, c, *_ in shares})]
    catalogs += [{"catalog": f"iceberg|{re.escape(SHARED)}|shopflow|adventureworks|system", "allow": "read-only"}]
    schemas = [{"user": TRINO_ADMINS, "owner": True}]
    schemas += [{"user": q(u), "catalog": q(c), "owner": True} for u, c in owned]
    schemas += [{"owner": False}]
    tables = [{"user": TRINO_ADMINS, "privileges": _ALL}]
    tables += [{"user": q(u), "catalog": q(c), "privileges": _ALL} for u, c in owned]
    for u, c, ns, tbl, access in shares:
        tables.append({"user": q(u), "catalog": q(c), "schema": q(ns) if ns else ".*", "table": q(tbl) if tbl else ".*",
                       "privileges": (["SELECT"] if access != "write" else       # whole lake, write: also
                                      ["SELECT", "INSERT", "DELETE", "UPDATE"] + (["OWNERSHIP"] if not ns else []))})
                       # create / drop tables, as in Spark (Polaris TABLE_CREATE / TABLE_DROP)
    tables += [{"catalog": f"iceberg|{re.escape(SHARED)}|shopflow|adventureworks|system", "privileges": ["SELECT"]}]
    functions = [{"user": TRINO_ADMINS, "privileges": ["EXECUTE", "GRANT_EXECUTE", "OWNERSHIP"]}]
    functions += [{"user": q(u), "catalog": q(c), "privileges": ["EXECUTE", "GRANT_EXECUTE", "OWNERSHIP"]} for u, c in owned]
    functions += [{"privileges": ["EXECUTE"]}]
    return {
        "catalogs": catalogs, "schemas": schemas, "tables": tables, "functions": functions,
        "procedures": [{"user": TRINO_ADMINS, "privileges": ["EXECUTE", "GRANT_EXECUTE"]},
                       {"privileges": ["EXECUTE"]}],
        # everyone sees and kills their own queries; only admins see other people's SQL
        "queries": [{"user": TRINO_ADMINS, "allow": ["execute", "kill", "view"]}, {"allow": ["execute"]}],
        "system_information": [{"user": TRINO_ADMINS, "allow": ["read", "write"]}],
    }


def _k8s(method, path, body=None):
    """Kubernetes API call with the pod's service account (in-cluster only)."""
    import ssl
    sa = "/var/run/secrets/kubernetes.io/serviceaccount"
    req = urllib.request.Request(
        f"https://kubernetes.default.svc{path}", method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Authorization": "Bearer " + open(f"{sa}/token").read().strip(),
                 "Content-Type": "application/merge-patch+json"})
    ctx = ssl.create_default_context(cafile=f"{sa}/ca.crt")
    with urllib.request.urlopen(req, context=ctx, timeout=30) as r:
        return json.loads(r.read() or b"{}")


_last_rules = None


def publish_trino_rules():
    """Write the current rules where Trino reads them (only when they changed)."""
    global _last_rules
    if not (TRINO_RULES_PATH or TRINO_RULES_CONFIGMAP):
        return False
    text = json.dumps(trino_rules(), indent=1, sort_keys=True)
    if TRINO_RULES_CONFIGMAP:
        # compare with the LIVE ConfigMap: a chart apply resets it to its start value
        ns = open("/var/run/secrets/kubernetes.io/serviceaccount/namespace").read().strip()
        path = f"/api/v1/namespaces/{ns}/configmaps/{TRINO_RULES_CONFIGMAP}"
        if (_k8s("GET", path).get("data") or {}).get("rules.json") == text:
            return False
        _k8s("PATCH", path, {"data": {"rules.json": text}})
        return True
    if text == _last_rules:
        return False
    tmp = TRINO_RULES_PATH + ".tmp"
    with open(tmp, "w") as f:
        f.write(text)
    os.replace(tmp, TRINO_RULES_PATH)
    _last_rules = text
    return True
