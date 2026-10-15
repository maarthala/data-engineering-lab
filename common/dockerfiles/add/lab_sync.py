"""Learner code ↔ their own bucket, and the team folder ↔ their team's bucket.

~/work/notebooks, ~/work/dags and ~/work/dbt (JupyterLab's normal file browser) are mirrored to
s3://<user>-lake/files/src/notebooks/, files/src/dags/ and files/src/dbt/ — Airflow's "learners"
bundle reads the dags. dbt's generated folders (target/, logs/, dbt_packages/) stay local.

  server start   two-way: the newer copy of each file wins, nothing is deleted
                 (picks up files uploaded via My files / the RustFS console)
  save           upload            delete  remove in the bucket
  rename / move  remove old + upload new

~/work/team is the team's bucket: s3://team-<team>-lake/files/ (TEAM_BUCKET, from the Hub).
Several people write it, so the bucket is the truth: opening the folder (or a file in it)
first pulls teammates' changes — new and newer files come down, files a teammate deleted go.
Saves, deletes and renames go up at once, like the folders above. Moved to another team →
the old team's copies are cleared at the next server start.

Uses the learner's own key (LAKE_S3_KEY, own bucket + their team's — handed out by the Hub).
Inert outside JupyterHub (no LAKE_BUCKET). Hidden paths (.ipynb_checkpoints …) are skipped.
"""
import logging
import os
import shutil
import time
from pathlib import Path

import s3fs
from jupyter_server.services.contents.largefilemanager import LargeFileManager
from tornado import web

FOLDERS = ("notebooks", "dags", "dbt")
TEAM = "team"                                  # ~/work/team ↔ s3://<TEAM_BUCKET>/files/
# dbt rebuilds these on every run — mirroring them would only fill the 100 MB bucket
DBT_GENERATED = {"target", "logs", "dbt_packages"}
PREFIX = "files/src/"                          # their place in the bucket: files/src/notebooks, …
TEAM_PREFIX = "files/"                         # the team bucket's files (its tables/ stay out)
ROOT = "/home/jovyan/work"                     # the learner's persistent volume
BUCKET = os.environ.get("LAKE_BUCKET", "")
TEAM_BUCKET = os.environ.get("TEAM_BUCKET", "")
ENABLED = bool(BUCKET and os.environ.get("LAKE_S3_KEY"))
MARK = ".team-bucket"                          # in ~/work/team: whose copies these are
log = logging.getLogger("lab_sync")
_fs = None
_known = set()                                 # team files seen in the bucket (or put there)
_pulled = {}                                   # team dir → when it was last pulled


def fs():
    global _fs
    if _fs is None:
        _fs = s3fs.S3FileSystem(
            key=os.environ["LAKE_S3_KEY"], secret=os.environ["LAKE_S3_SECRET"],
            client_kwargs={"endpoint_url": os.environ.get("AWS_S3_ENDPOINT", "http://storage:9000")},
            use_listings_cache=False)
    return _fs


def _team(rel):
    parts = (rel or "").strip("/").split("/")
    return bool(ENABLED and TEAM_BUCKET and parts[0] == TEAM)


def synced(path):
    """Path relative to ROOT, like 'dags/etl/a.py' → is it inside a synced folder?"""
    parts = (path or "").strip("/").split("/")
    if any(p.startswith(".") for p in parts):
        return False
    if _team(path):
        return len(parts) > 1
    if parts[0] == "dbt" and DBT_GENERATED.intersection(parts[1:-1]):
        return False
    return ENABLED and len(parts) > 1 and parts[0] in FOLDERS


def _remote(rel):
    rel = rel.strip("/")
    if _team(rel):
        return f"{TEAM_BUCKET}/{TEAM_PREFIX}{rel[len(TEAM) + 1:]}"
    return f"{BUCKET}/{PREFIX}{rel}"


def _upload(local, rel):
    """Upload one file; local mtime := the object's time, so the next start sees them equal."""
    try:
        fs().put_file(str(local), _remote(rel))
    except Exception as e:
        if "quota" in str(e).lower():
            where = "the team bucket" if _team(rel) else "your bucket"
            raise web.HTTPError(507, f"Saved in Jupyter, but NOT in {where}: it is full. "
                                     "Delete files you no longer need (My files).")
        log.warning("[lab_sync] upload %s failed: %s", rel, e)
        return
    if _team(rel):
        _known.add(rel)
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
    if _team(rel):
        _known.difference_update({k for k in _known if k == rel or k.startswith(rel.rstrip("/") + "/")})


# ---- the team folder: the bucket is the truth ---------------------------------------------
def _team_remote(rel_dir, recursive):
    """{rel: mtime} of the team bucket's files under team/<rel_dir>, and its sub-folders."""
    base = _remote(rel_dir).rstrip("/")
    try:
        found = (fs().find(base, detail=True, withdirs=True) if recursive
                 else {i["name"]: i for i in fs().ls(base, detail=True)})
    except FileNotFoundError:
        return {}, set()
    files, dirs = {}, set()
    for name, info in found.items():
        rel = TEAM + "/" + name[len(TEAM_BUCKET) + 1 + len(TEAM_PREFIX):].strip("/")
        if not synced(rel):
            continue
        if info["type"] == "directory":
            dirs.add(rel)
        elif not name.endswith("/"):
            files[rel] = info["LastModified"].timestamp()
    return files, dirs


def pull_team(rel_dir=TEAM, recursive=False, every=3.0):
    """Bring team/<rel_dir> up to date with the team bucket (at most every `every` seconds)."""
    if not (ENABLED and TEAM_BUCKET):
        return
    rel_dir = rel_dir.strip("/") or TEAM
    if time.time() - _pulled.get(rel_dir, 0) < every:
        return
    _pulled[rel_dir] = time.time()
    try:
        remote, dirs = _team_remote(rel_dir, recursive)
    except Exception as e:  # noqa: BLE001 — show what's local; try again next time
        log.warning("[lab_sync] cannot list %s: %s", _remote(rel_dir), e)
        return
    here = Path(ROOT, rel_dir)
    here.mkdir(parents=True, exist_ok=True)
    for d in dirs:
        Path(ROOT, d).mkdir(parents=True, exist_ok=True)
        _known.add(d)
    for rel, t in remote.items():
        local = Path(ROOT, rel)
        if not local.exists() or local.stat().st_mtime < t - 1:
            local.parent.mkdir(parents=True, exist_ok=True)
            fs().get_file(_remote(rel), str(local))
            os.utime(local, (t, t))
        _known.add(rel)
    # what's here but not in the bucket: deleted by a teammate (we saw it before) → remove it;
    # never seen there (an upload that failed) → try the upload again
    kids = here.rglob("*") if recursive else here.iterdir()
    for local in sorted(kids, key=lambda p: -len(p.parts)):
        rel = local.relative_to(ROOT).as_posix()
        if not synced(rel) or rel in remote or rel in dirs:
            continue
        if local.is_file():
            if rel in _known:
                local.unlink()
                _known.discard(rel)
            else:
                _upload(local, rel)
        elif local.is_dir() and rel in _known and not any(local.iterdir()):
            local.rmdir()                             # a folder a teammate emptied and removed
            _known.discard(rel)


def _team_start(root):
    """Server start: copies from another team (the member moved) go; then a full pull."""
    team = Path(root, TEAM)
    mark = team / MARK
    owner = mark.read_text().strip() if mark.exists() else ""
    if team.exists() and owner != TEAM_BUCKET:
        shutil.rmtree(team, ignore_errors=True)       # not this team's files (or no team now)
    if not TEAM_BUCKET:
        return
    team.mkdir(parents=True, exist_ok=True)
    mark.write_text(TEAM_BUCKET + "\n")
    pull_team(TEAM, recursive=True, every=0)


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
    try:
        _team_start(root)
        if TEAM_BUCKET:
            log.info("[lab_sync] team ↔ s3://%s/%s synced", TEAM_BUCKET, TEAM_PREFIX)
    except Exception as e:  # noqa: BLE001 — the server still starts
        log.warning("[lab_sync] team folder: %s", e)


class SyncedFileManager(LargeFileManager):
    """The normal file manager + mirroring of notebooks/, dags/, dbt/ (own bucket) and team/
    (team bucket). Works on real file paths (relative to ROOT), whatever the server's root_dir is."""

    def _rel(self, path):
        return os.path.relpath(self._get_os_path(path), ROOT)

    def get(self, path, content=True, type=None, format=None, **kwargs):
        rel = self._rel(path)
        if _team(rel) and not rel.split("/")[-1].startswith("."):
            try:                                      # teammates' changes first
                if os.path.isdir(self._get_os_path(path)) or rel == TEAM:
                    pull_team(rel)
                else:
                    pull_team(os.path.dirname(rel) or TEAM)
            except Exception as e:  # noqa: BLE001
                log.warning("[lab_sync] pull %s: %s", rel, e)
        return super().get(path, content=content, type=type, format=format, **kwargs)

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
