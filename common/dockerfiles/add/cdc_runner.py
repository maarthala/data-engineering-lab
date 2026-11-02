"""cdc-runner — does the work of "📡 Ingestion (CDC)" (labcdc.py): picks up cdc_tasks rows and starts
the due scheduled jobs, each as one OLake engine container (olakego/source-<postgres|mysql|mssql>):
  check_source       the connection (+ what CDC still needs); Postgres with CDC on: first makes the
                     publication + replication slot
  check_destination  the member's lakehouse catalog, signed in as their job login
  discover           the source's tables → the task's result (the page lists them)
  test_source / browse   Test connection / Browse tables on a source form, saved or not
  run                a sync: full load / changes since the saved state; the run's status, rows, log
TRUSTED code: it holds the platform's admin logins (lab_jobs mints each member's job login), so
member input only ever becomes OLake's JSON config, never a command line. A container gets the
config as files (copied in before it starts), the stack's network and a memory limit; state and
discovered tables are copied out after it ends.
  local: Docker, through the mounted socket (CDC_EXECUTOR=docker)
  k8s:   a Pod in the runner's namespace (CDC_EXECUTOR=k8s; its service account may make pods and
         secrets): the config files come in as a Secret, the files to keep come back in the pod's log
"""
import http.client
import io
import json
import os
import re
import socket
import ssl
import tarfile
import threading
import time
import traceback
import urllib.parse
import urllib.error
import urllib.request

import labcdc
from labcdc import _q

VERSION = os.environ.get("CDC_VERSION", "v1.1.1")           # OLake's engine: olakego/source-<kind>:<version>
NETWORK = os.environ.get("CDC_NETWORK", "local_sparknet")
POLARIS = os.environ.get("POLARIS_URL", "http://polaris:8181").rstrip("/")
S3_ENDPOINT = os.environ.get("LAKE_S3_ENDPOINT", "http://storage:9000")
MEM = int(float(os.environ.get("CDC_MEM_GB", "1.5")) * 1024 ** 3)
WORKERS = int(os.environ.get("CDC_WORKERS", "2"))
TIMEOUT = int(os.environ.get("CDC_TIMEOUT_S", "1800"))       # a sync longer than this is stopped
CDC_WAIT = 120                                                # OLake: idle seconds before a CDC run ends
CFG = "/mnt/config"
EXECUTOR = os.environ.get("CDC_EXECUTOR", "docker")
SA = "/var/run/secrets/kubernetes.io/serviceaccount"
SYSTEM_TABLES = {("dbo", "systranschemas")}
LOG_LINES = 400


# ---- Docker Engine API over the socket ------------------------------------------------------
class _Sock(http.client.HTTPConnection):
    def __init__(self, timeout=60):
        super().__init__("docker", timeout=timeout)

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect("/var/run/docker.sock")


def _docker(method, path, body=None, raw=None, timeout=60, ctype="application/json"):
    c = _Sock(timeout)
    data = raw if raw is not None else (json.dumps(body).encode() if body is not None else None)
    c.request(method, "/v1.43" + path, body=data, headers={"Content-Type": ctype} if data is not None else {})
    r = c.getresponse()
    out = r.read()
    c.close()
    if r.status >= 400:
        raise RuntimeError(f"docker {method} {path.split('?')[0]}: HTTP {r.status} {out[:200]!r}")
    return out


def _image(kind):
    return f"olakego/source-{kind}:{VERSION}"


def _pull(image):
    name, tag = image.rsplit(":", 1)
    try:
        _docker("GET", f"/images/{urllib.parse.quote(image, safe='')}/json")
    except RuntimeError:
        _docker("POST", f"/images/create?fromImage={urllib.parse.quote(name)}&tag={tag}", timeout=900)


def _tar(files):
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as t:
        d = tarfile.TarInfo(CFG.lstrip("/"))
        d.type, d.mode = tarfile.DIRTYPE, 0o777
        t.addfile(d)
        for name, content in files.items():
            raw = json.dumps(content).encode()
            i = tarfile.TarInfo(f"{CFG.lstrip('/')}/{name}")
            i.size, i.mode = len(raw), 0o666
            t.addfile(i, io.BytesIO(raw))
    return buf.getvalue()


def _logs(cid):
    raw, out, i = _docker("GET", f"/containers/{cid}/logs?stdout=1&stderr=1&timestamps=0"), [], 0
    while i + 8 <= len(raw):                                   # the stream's 8-byte frame headers
        n = int.from_bytes(raw[i + 4:i + 8], "big")
        out.append(raw[i + 8:i + 8 + n])
        i += 8 + n
    return b"".join(out).decode("utf-8", "replace")


def _read(cid, name):
    try:
        raw = _docker("GET", f"/containers/{cid}/archive?path={urllib.parse.quote(f'{CFG}/{name}')}")
    except RuntimeError:
        return None
    with tarfile.open(fileobj=io.BytesIO(raw)) as t:
        m = t.getmembers()[0]
        return json.loads(t.extractfile(m).read() or b"null")


def olake(kind, label, args, files, read=(), timeout=TIMEOUT):
    """One OLake engine run for a `kind` of source: files → /mnt/config, run `args`, wait.
    → (exit code, log, {read file})."""
    if EXECUTOR == "k8s":
        return _olake_pod(kind, label, args, files, read, timeout)
    image = _image(kind)
    _pull(image)
    cid = json.loads(_docker("POST", f"/containers/create?name=cdc-{label}-{int(time.time() * 1000)}", {
        "Image": image, "Cmd": args, "Labels": {"lab.cdc": label},
        "HostConfig": {"NetworkMode": NETWORK, "Memory": MEM, "AutoRemove": False}}))["Id"]
    try:
        _docker("PUT", f"/containers/{cid}/archive?path=/", raw=_tar(files), ctype="application/x-tar")
        _docker("POST", f"/containers/{cid}/start")
        try:
            code = json.loads(_docker("POST", f"/containers/{cid}/wait", timeout=timeout))["StatusCode"]
        except (TimeoutError, socket.timeout):
            _docker("POST", f"/containers/{cid}/kill")
            code = -1
        return code, _logs(cid), {n: _read(cid, n) for n in read}
    finally:
        try:
            _docker("DELETE", f"/containers/{cid}?force=1")
        except RuntimeError:
            pass


# ---- Kubernetes: one Pod per run ------------------------------------------------------------
def _k8s(method, path, body=None, timeout=60, text=False):
    token = open(f"{SA}/token").read().strip()
    req = urllib.request.Request("https://kubernetes.default.svc" + path, method=method,
                                 data=json.dumps(body).encode() if body is not None else None,
                                 headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"})
    ctx = ssl.create_default_context(cafile=f"{SA}/ca.crt")
    try:
        with urllib.request.urlopen(req, context=ctx, timeout=timeout) as r:
            raw = r.read()
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"k8s {method} {path.split('?')[0]}: HTTP {e.code} {e.read()[:200]!r}") from None
    return raw.decode("utf-8", "replace") if text else (json.loads(raw) if raw else None)


MARK = "@@CDC-FILE "


def _olake_pod(kind, label, args, files, read, timeout):
    ns = os.environ.get("CDC_NAMESPACE") or open(f"{SA}/namespace").read().strip()
    name = re.sub(r"[^a-z0-9-]", "-", f"cdc-{label}-{int(time.time() * 1000)}".lower())[:63].rstrip("-")
    # OLake runs as usual; then the files to keep are printed (base64) after a marker each
    keep = " ".join(read)
    script = (f'cp /mnt/in/* {CFG}/ && cd /home && ./olake "$@"; code=$?; '
              f'for f in {keep}; do echo; echo "{MARK}$f"; [ -f {CFG}/$f ] && base64 -w0 {CFG}/$f; echo; done; exit $code')
    _k8s("POST", f"/api/v1/namespaces/{ns}/secrets", {
        "metadata": {"name": name, "labels": {"lab.cdc": "run"}},
        "stringData": {n: json.dumps(c) for n, c in files.items()}})
    try:
        _k8s("POST", f"/api/v1/namespaces/{ns}/pods", {
            "metadata": {"name": name, "labels": {"lab.cdc": "run"}},
            "spec": {"restartPolicy": "Never", "automountServiceAccountToken": False,
                     "containers": [{"name": "olake", "image": _image(kind), "imagePullPolicy": "IfNotPresent",
                                     "command": ["sh", "-c", script, "olake"], "args": args,
                                     "resources": {"limits": {"memory": str(MEM)}, "requests": {"memory": "256Mi"}},
                                     "volumeMounts": [{"name": "in", "mountPath": "/mnt/in", "readOnly": True},
                                                      {"name": "config", "mountPath": CFG}]}],
                     "volumes": [{"name": "in", "secret": {"secretName": name}}, {"name": "config", "emptyDir": {}}]}})
        code, end = None, time.time() + timeout
        while time.time() < end:
            st = _k8s("GET", f"/api/v1/namespaces/{ns}/pods/{name}")["status"]
            for c in st.get("containerStatuses") or []:
                w = (c.get("state") or {}).get("waiting") or {}
                if w.get("reason") in ("ErrImagePull", "ImagePullBackOff", "InvalidImageName"):
                    return 1, f"can't start OLake ({_image(kind)}): {w.get('reason')} {w.get('message', '')}", {}
                t = (c.get("state") or {}).get("terminated")
                if t:
                    code = t.get("exitCode", 1)
            if code is not None:
                break
            time.sleep(2)
        if code is None:
            return -1, "stopped: it ran longer than the time limit", {}
        log = _k8s("GET", f"/api/v1/namespaces/{ns}/pods/{name}/log?container=olake", text=True, timeout=120)
        out, parts = {}, log.split("\n" + MARK)
        for part in parts[1:]:
            fname, _, body = part.partition("\n")
            body = body.strip().split("\n")[0]
            try:
                out[fname.strip()] = json.loads(__import__("base64").b64decode(body) or b"null") if body else None
            except ValueError:
                out[fname.strip()] = None
        return code, parts[0], out
    finally:
        for kind_ in ("pods", "secrets"):
            try:
                _k8s("DELETE", f"/api/v1/namespaces/{ns}/{kind_}/{name}")
            except RuntimeError:
                pass


# ---- config files: member input → OLake's JSON ---------------------------------------------
def _clean(log):
    """OLake's log without colours, Java stack frames and its noisy debug lines — the last lines."""
    log = re.sub(r"\x1b\[[0-9;]*m", "", log)
    keep = [l for l in log.splitlines() if l.strip() and not re.search(r"\]\s+at |\.\.\. \d+ more|\bDEBUG\b", l)]
    return "\n".join(keep[-LOG_LINES:])


def _status(log):
    """The CONNECTION_STATUS line of `check` → (ok, message, prerequisites)."""
    for line in reversed(log.splitlines()):
        m = re.search(r'(\{"connectionStatus".*\})\s*$', line)
        if m:
            s = json.loads(m.group(1))["connectionStatus"]
            return s.get("status") == "SUCCEEDED", s.get("message") or "", s.get("prerequisites") or []
    m = re.findall(r"FATAL\s+(.*)", re.sub(r"\x1b\[[0-9;]*m", "", log))
    return False, (m[-1] if m else "OLake stopped without an answer"), []


def source_config(s):
    """OLake's source config for the kind of database (field names differ a little between them)."""
    cfg = {"port": s["port"], "database": s["dbname"], "username": s["username"],
           "password": s.get("password") or labcdc.password(s["member"], s["id"]), "max_threads": 2}
    if s["kind"] == "postgres":
        cfg.update(host=s["host"], ssl={"mode": "disable"})
        if s["schemas"]:
            cfg["schemas"] = s["schemas"]
        if s["cdc"] and s.get("slot"):
            cfg["update_method"] = {"type": "CDC", "replication_slot": s["slot"], "publication": s["publication"],
                                    "initial_wait_time": CDC_WAIT}
    elif s["kind"] == "mysql":
        cfg["hosts"] = s["host"]
        if s["cdc"]:
            cfg["update_method"] = {"type": "CDC", "initial_wait_time": CDC_WAIT}
    elif s["kind"] == "mssql":
        cfg["host"] = s["host"]
        cfg["update_method"] = {"type": "CDC" if s["cdc"] else "Standalone"}
    return cfg


def dest_config(member, d):
    """The member's catalog, written as their job login (<member>_jobs + own-bucket key, lab_jobs)."""
    import lab_jobs
    env = lab_jobs.job_env(re.sub(r"_", "-", member))
    return {"type": "ICEBERG", "writer": {
        "catalog_type": "polaris", "rest_catalog_url": f"{POLARIS}/api/catalog", "iceberg_s3_path": d["catalog"],
        "rest_auth_type": "OAuth2", "credential": env["LAB_JOB_CREDENTIAL"], "scope": "PRINCIPAL_ROLE:ALL",
        "oauth2_uri": f"{POLARIS}/api/catalog/v1/oauth/tokens", "aws_region": "us-east-1",
        "s3_endpoint": S3_ENDPOINT, "aws_access_key": env["LAB_JOB_S3_KEY"], "aws_secret_key": env["LAB_JOB_S3_SECRET"],
        "s3_path_style": True, "s3_use_ssl": False}}


def prepare_cdc(s):
    """Postgres: CDC needs a publication and a logical replication slot in the source — made with the
    source's own login if missing (it needs the rights to; otherwise the message says what to run)."""
    import psycopg2
    try:
        c = psycopg2.connect(host=s["host"], port=s["port"], dbname=s["dbname"], user=s["username"],
                             password=labcdc.password(s["member"], s["id"]), connect_timeout=10)
    except psycopg2.Error as e:
        return [f"can't connect: {str(e).strip().splitlines()[0]}"]
    notes = []
    c.autocommit = True
    with c, c.cursor() as cur:
        cur.execute("SHOW wal_level")
        if cur.fetchone()[0] != "logical":
            notes.append("wal_level is not 'logical' — set wal_level = logical in postgresql.conf and restart Postgres")
            return notes
        cur.execute("SELECT 1 FROM pg_publication WHERE pubname = %s", (s["publication"],))
        if not cur.fetchone():
            try:
                tables = [f'"{x}".*' for x in s["schemas"]] if s["schemas"] else None
                cur.execute(f'CREATE PUBLICATION "{s["publication"]}" ' +
                            ("FOR TABLES IN SCHEMA " + ", ".join(f'"{x}"' for x in s["schemas"]) if tables else "FOR ALL TABLES"))
                notes.append(f"made publication {s['publication']}")
            except psycopg2.Error as e:
                notes.append(f"couldn't make publication {s['publication']}: {str(e).strip().splitlines()[0]} — "
                             f"run: CREATE PUBLICATION {s['publication']} FOR ALL TABLES;")
        cur.execute("SELECT 1 FROM pg_replication_slots WHERE slot_name = %s", (s["slot"],))
        if not cur.fetchone():
            try:
                cur.execute("SELECT pg_create_logical_replication_slot(%s, 'pgoutput')", (s["slot"],))
                notes.append(f"made replication slot {s['slot']}")
            except psycopg2.Error as e:
                notes.append(f"couldn't make replication slot {s['slot']}: {str(e).strip().splitlines()[0]} — "
                             f"run: SELECT pg_create_logical_replication_slot('{s['slot']}', 'pgoutput');")
    c.close()
    return notes


# ---- the tasks -----------------------------------------------------------------------------
def check_source(t):
    s = _q("SELECT * FROM cdc_sources WHERE id = %s", (t["ref"],), one=True)
    notes = prepare_cdc(s) if s["cdc"] and s["kind"] == "postgres" else []
    code, log, _ = olake(s["kind"], f"check-src-{s['id']}", ["check", "--config", f"{CFG}/source.json"],
                         {"source.json": source_config(s)}, timeout=180)
    ok, msg, pre = _status(log)
    checked = {"ok": ok, "message": msg, "notes": notes, "at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
               "prerequisites": [{"name": p.get("name"), "passed": p.get("passed"), "required": p.get("required"),
                                  "current": p.get("current_value"), "recommended": p.get("recommended_value")} for p in pre]}
    _q("UPDATE cdc_sources SET checked = %s WHERE id = %s", (json.dumps(checked), s["id"]), rows=False)
    return checked


def check_destination(t):
    d = _q("SELECT * FROM cdc_destinations WHERE id = %s", (t["ref"],), one=True)
    code, log, _ = olake("postgres", f"check-dst-{d['id']}", ["check", "--destination", f"{CFG}/destination.json"],
                         {"destination.json": dest_config(d["member"], d)}, timeout=180)
    ok, msg, _ = _status(log)
    checked = {"ok": ok, "message": msg, "at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    _q("UPDATE cdc_destinations SET checked = %s WHERE id = %s", (json.dumps(checked), d["id"]), rows=False)
    return checked


def _form(t):
    """Test connection / Browse tables: the form in the task (a saved source's password when it's left
    empty; CDC isn't set up — that's the saved source's check)."""
    f = dict(t["payload"], member=t["member"], id=t["ref"], slot=None)
    if not f["password"]:
        f["password"] = labcdc.password(t["member"], t["ref"])
    return f


def test_source(t):
    s = _form(t)
    code, log, _ = olake(s["kind"], f"test-src-{t['id']}", ["check", "--config", f"{CFG}/source.json"],
                         {"source.json": source_config(dict(s, cdc=False))}, timeout=180)
    ok, msg, _ = _status(log)
    return {"ok": ok, "message": msg}


def browse(t):
    return {"tables": _discover(_form(t), f"browse-{t['id']}")["tables"]}


def discover(t):
    s = _q("SELECT * FROM cdc_sources WHERE id = %s", (t["ref"],), one=True)
    return _discover(s, f"discover-{s['id']}")


def _discover(s, label):
    code, log, out = olake(s["kind"], label, ["discover", "--config", f"{CFG}/source.json"],
                           {"source.json": source_config(s)}, read=("streams.json",), timeout=300)
    streams = out.get("streams.json")
    if code or not streams:
        raise RuntimeError(_status(log)[1])
    # SQL Server's own CDC bookkeeping table isn't data (and has no change stream of its own)
    streams["streams"] = [x for x in streams["streams"] if (x["stream"]["namespace"], x["stream"]["name"]) not in SYSTEM_TABLES]
    tables = [{"table": f"{x['stream']['namespace']}.{x['stream']['name']}",
               "columns": len([c for c, v in (x["stream"].get("type_schema", {}).get("properties") or {}).items()
                               if not v.get("olake_column")]),
               "modes": x["stream"].get("supported_sync_modes", [])} for x in streams["streams"]]
    return {"tables": sorted(tables, key=lambda x: x["table"]), "streams": streams}


def run(t):
    j = _q("SELECT * FROM cdc_jobs WHERE id = %s", (t["ref"],), one=True)
    if not j:
        raise RuntimeError("the job was deleted")
    s = _q("SELECT * FROM cdc_sources WHERE id = %s", (j["source_id"],), one=True)
    d = _q("SELECT * FROM cdc_destinations WHERE id = %s", (j["dest_id"],), one=True)
    rid = _q("INSERT INTO cdc_runs (job_id, member, trigger) VALUES (%s, %s, %s) RETURNING id",
             (j["id"], j["member"], (t["payload"] or {}).get("trigger", "schedule")), one=True)["id"]
    _q("UPDATE cdc_jobs SET last_run_at = now() WHERE id = %s", (j["id"],), rows=False)
    status, records, err, log = "failed", None, None, ""
    try:
        if s["cdc"] and j["mode"] in labcdc.STREAMING and s["kind"] == "postgres":
            notes = [n for n in prepare_cdc(s) if n.startswith(("can't", "couldn't", "wal_level"))]
            if notes:
                raise RuntimeError("; ".join(notes))
        files = {"source.json": source_config(s), "destination.json": dest_config(j["member"], d),
                 "streams.json": j["streams"], "state.json": j["state"] or {}}
        code, log, out = olake(s["kind"], f"run-{j['id']}", ["sync", "--config", f"{CFG}/source.json", "--destination",
                                                  f"{CFG}/destination.json", "--catalog", f"{CFG}/streams.json",
                                                  "--state", f"{CFG}/state.json"], files, read=("state.json",))
        m = re.findall(r"Total records read: (\d+)", log)
        records = int(m[-1]) if m else None
        if code == 0 and "Sync completed" in log:
            status = "ok"
            if out.get("state.json"):
                _q("UPDATE cdc_jobs SET state = %s WHERE id = %s", (json.dumps(out["state.json"]), j["id"]), rows=False)
        else:
            err = "stopped after the time limit" if code == -1 else _status(log)[1]
    except Exception as e:  # noqa: BLE001 — the run's row says what went wrong
        err = str(e)
    log = _clean(log)
    _q("UPDATE cdc_runs SET finished_at = now(), status = %s, records = %s, error = %s, log = %s WHERE id = %s",
       (status, records, err, log, rid), rows=False)
    if status != "ok":
        raise RuntimeError(err or "the sync failed")
    return {"run": rid, "records": records}


def drop_source(t):
    """A CDC source is deleted: drop its replication slot (with its own login), then the source."""
    s = _q("SELECT * FROM cdc_sources WHERE id = %s", (t["ref"],), one=True)
    if not s:
        return {}
    if not s["slot"]:
        labcdc.drop_source(s)
        return {}
    note = None
    try:
        import psycopg2
        c = psycopg2.connect(host=s["host"], port=s["port"], dbname=s["dbname"], user=s["username"],
                             password=labcdc.password(s["member"], s["id"]), connect_timeout=10)
        c.autocommit = True
        with c, c.cursor() as cur:
            cur.execute("SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = %s",
                        (s["slot"],))
            note = f"dropped replication slot {s['slot']}" if cur.rowcount else None
        c.close()
    except Exception as e:  # noqa: BLE001 — the source goes anyway; the note says what to do
        note = (f"couldn't drop replication slot {s['slot']} ({str(e).strip().splitlines()[0]}) — "
                f"run in that database: SELECT pg_drop_replication_slot('{s['slot']}');")
    labcdc.drop_source(s)
    return {"note": note}


KINDS = {"test_source": test_source, "browse": browse, "drop_source": drop_source, "check_source": check_source, "check_destination": check_destination, "discover": discover, "run": run}


# ---- the loop ------------------------------------------------------------------------------
def _claim():
    return _q("UPDATE cdc_tasks SET status = 'running' WHERE id = (SELECT id FROM cdc_tasks WHERE status = 'queued' "
              "ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1) RETURNING *", one=True)


def _work(t):
    try:
        result = KINDS[t["kind"]](t)
        _q("UPDATE cdc_tasks SET status = 'done', result = %s, finished_at = now() WHERE id = %s",
           (json.dumps(result), t["id"]), rows=False)
    except Exception as e:  # noqa: BLE001
        print(f"[cdc] task {t['id']} {t['kind']} failed: {e}", flush=True)
        if not isinstance(e, RuntimeError):
            traceback.print_exc()
        _q("UPDATE cdc_tasks SET status = 'failed', error = %s, finished_at = now() WHERE id = %s",
           (str(e)[:2000], t["id"]), rows=False)
    finally:                                             # a form's password isn't kept
        _q("UPDATE cdc_tasks SET payload = payload - 'password' WHERE id = %s AND payload ? 'password'", (t["id"],), rows=False)


def _due():
    """Scheduled jobs whose time has come (and that aren't queued or running) → a run task."""
    for sched, secs in labcdc.SCHEDULES.items():
        if not secs:
            continue
        for j in _q("SELECT id, member FROM cdc_jobs j WHERE enabled AND schedule = %s "
                    "AND (last_run_at IS NULL OR last_run_at <= now() - make_interval(secs => %s)) "
                    "AND NOT EXISTS (SELECT 1 FROM cdc_tasks t WHERE t.kind = 'run' AND t.ref = j.id "
                    "AND t.status IN ('queued', 'running'))", (sched, secs)):
            labcdc.queue(j["member"], "run", j["id"], {"trigger": "schedule"})


def main():
    labcdc._conn().close()
    # a restart: what was running then is lost
    _q("UPDATE cdc_tasks SET status = 'failed', error = 'cdc-runner restarted', finished_at = now() WHERE status = 'running'",
       rows=False)
    _q("UPDATE cdc_runs SET status = 'failed', error = 'cdc-runner restarted', finished_at = now() WHERE status = 'running'",
       rows=False)
    print(f"[cdc] runner up: OLake {VERSION}, {EXECUTOR} ({NETWORK if EXECUTOR == 'docker' else 'pods'}), {WORKERS} at a time", flush=True)
    busy, last_due = [], 0
    while True:
        try:
            busy = [w for w in busy if w.is_alive()]
            if time.time() - last_due > 30:
                _due()
                last_due = time.time()
            while len(busy) < WORKERS:
                t = _claim()
                if not t:
                    break
                w = threading.Thread(target=_work, args=(t,), daemon=True)
                w.start()
                busy.append(w)
        except Exception as e:  # noqa: BLE001 — the database may be restarting
            print(f"[cdc] loop: {e}", flush=True)
        time.sleep(2)


if __name__ == "__main__":
    main()
