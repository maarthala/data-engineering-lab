"""Learner code ↔ their own bucket.

~/work/notebooks, ~/work/dags and ~/work/dbt (JupyterLab's normal file browser) are mirrored to
s3://<user>-lake/files/src/notebooks/, files/src/dags/ and files/src/dbt/ — Airflow's "learners"
bundle reads the dags. dbt's generated folders (target/, logs/, dbt_packages/) stay local.

  server start   two-way: the newer copy of each file wins, nothing is deleted
                 (picks up files uploaded via My files / the RustFS console)
  save           upload            delete  remove in the bucket
  rename / move  remove old + upload new

Uses the learner's own key (LAKE_S3_KEY, own bucket only — handed out by the Hub).
Inert outside JupyterHub (no LAKE_BUCKET). Hidden paths (.ipynb_checkpoints …) are skipped.
"""
import logging
import os
from pathlib import Path

import s3fs
from jupyter_server.services.contents.largefilemanager import LargeFileManager
from tornado import web

FOLDERS = ("notebooks", "dags", "dbt")
# dbt rebuilds these on every run — mirroring them would only fill the 100 MB bucket
DBT_GENERATED = {"target", "logs", "dbt_packages"}
PREFIX = "files/src/"                          # their place in the bucket: files/src/notebooks, …
ROOT = "/home/jovyan/work"                     # the learner's persistent volume
BUCKET = os.environ.get("LAKE_BUCKET", "")
ENABLED = bool(BUCKET and os.environ.get("LAKE_S3_KEY"))
log = logging.getLogger("lab_sync")
_fs = None


def fs():
    global _fs
    if _fs is None:
        _fs = s3fs.S3FileSystem(
            key=os.environ["LAKE_S3_KEY"], secret=os.environ["LAKE_S3_SECRET"],
            client_kwargs={"endpoint_url": os.environ.get("AWS_S3_ENDPOINT", "http://storage:9000")},
            use_listings_cache=False)
    return _fs


def synced(path):
    """Path relative to ROOT, like 'dags/etl/a.py' → is it inside a synced folder?"""
    parts = (path or "").strip("/").split("/")
    if parts[0] == "dbt" and DBT_GENERATED.intersection(parts[1:-1]):
        return False
    return ENABLED and len(parts) > 1 and parts[0] in FOLDERS and not any(p.startswith(".") for p in parts)


def _remote(rel):
    return f"{BUCKET}/{PREFIX}{rel.strip('/')}"


def _upload(local, rel):
    """Upload one file; local mtime := the object's time, so the next start sees them equal."""
    try:
        fs().put_file(str(local), _remote(rel))
    except Exception as e:
        if "quota" in str(e).lower():
            raise web.HTTPError(507, "Saved in Jupyter, but NOT in your bucket: it is full. "
                                     "Delete files you no longer need (My files).")
        log.warning("[lab_sync] upload %s failed: %s", rel, e)
        return
    t = fs().info(_remote(rel))["LastModified"].timestamp()
    os.utime(local, (t, t))


def _upload_tree(root, rel):
    p = Path(root, rel)
    files = [p] if p.is_file() else [f for f in p.rglob("*") if f.is_file()]
    for f in files:
        r = f.relative_to(root).as_posix()
        if synced(r):
            _upload(f, r)


def _remove(rel):
    try:
        fs().rm(_remote(rel), recursive=True)
    except FileNotFoundError:
        pass
    except Exception as e:
        log.warning("[lab_sync] delete %s failed: %s", rel, e)


def initial_sync(root):
    """Server start: newest copy of every file wins, in both directions."""
    if not ENABLED:
        return
    for top in FOLDERS:
        Path(root, top).mkdir(parents=True, exist_ok=True)
        try:
            found = fs().find(f"{BUCKET}/{PREFIX}{top}", detail=True)
        except Exception as e:
            log.warning("[lab_sync] cannot list %s/%s: %s", BUCKET, top, e)
            continue
        remote = {}
        for name, info in found.items():
            rel = name[len(BUCKET) + 1 + len(PREFIX):]
            if info["type"] == "file" and not name.endswith("/") and synced(rel):
                remote[rel] = info["LastModified"].timestamp()
        for rel, t in remote.items():
            local = Path(root, rel)
            if not local.exists() or local.stat().st_mtime < t - 1:
                local.parent.mkdir(parents=True, exist_ok=True)
                fs().get_file(_remote(rel), str(local))
                os.utime(local, (t, t))
            elif local.stat().st_mtime > t + 1:
                _upload(local, rel)
        for local in Path(root, top).rglob("*"):
            rel = local.relative_to(root).as_posix()
            if local.is_file() and synced(rel) and rel not in remote:
                _upload(local, rel)
    log.info("[lab_sync] %s ↔ s3://%s synced", ", ".join(FOLDERS), BUCKET)


class SyncedFileManager(LargeFileManager):
    """The normal file manager + mirroring of notebooks/, dags/ and dbt/ to the learner's bucket.
    Works on real file paths (relative to ROOT), whatever the server's root_dir is."""

    def _rel(self, path):
        return os.path.relpath(self._get_os_path(path), ROOT)

    def save(self, model, path=""):
        out = super().save(model, path)
        rel = self._rel(path)
        if synced(rel) and model.get("type") != "directory" and model.get("chunk", -1) == -1:
            _upload(self._get_os_path(path), rel)
        return out

    def delete_file(self, path):
        rel = self._rel(path)
        super().delete_file(path)
        if synced(rel):
            _remove(rel)

    def rename_file(self, old_path, new_path):
        old, new = self._rel(old_path), self._rel(new_path)
        super().rename_file(old_path, new_path)
        if synced(old):
            _remove(old)
        if synced(new):
            _upload_tree(ROOT, new)
