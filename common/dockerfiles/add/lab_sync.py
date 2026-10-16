"""Jupyter's top folders ↔ the buckets a learner works in.

  my_files/{notebooks,dags,dbt}  ↔  s3://<user>-lake/files/src/{notebooks,dags,dbt}/
  <team>_team/{notebooks,dags,dbt} ↔ s3://team-<team>-lake/files/src/{notebooks,dags,dbt}/  (e.g. sales_team)
  <owner>_lake/…                 ↔  s3://<owner>-lake/files/…  a lake another member shared with
                                    them (whole-lake invite; SHARED_LAKES from the Hub)

(JupyterLab's normal file browser, root = the learner's volume ~/work.) Airflow's "learners"
bundle reads the dags. dbt's generated folders (target/, logs/, dbt_packages/) stay local.

my_files — only the learner writes it:
  server start   two-way: the newer copy of each file wins, nothing is deleted
                 (picks up files uploaded via My files / the RustFS console)
  save           upload            delete  remove in the bucket
  rename / move  remove old + upload new

<team>_team — the whole team writes it (TEAM_BUCKET, from the Hub), so the bucket is the truth:
opening a folder (or a file) in it first pulls teammates' changes — new and newer files come
down, files a teammate deleted go. Saves, deletes and renames go up at once, as above. Moved to
another team (or none) → the old team's copies are cleared at the next server start.

<owner>_lake — like the team folder (the owner's bucket is the truth, pulled folder by folder as
it is opened). read + write: saves go up. read: saving, deleting or renaming in it is refused, and
local changes are replaced by the owner's copy. Revoked → cleared at the next server start.

Uses the learner's own key (LAKE_S3_KEY: own bucket + their team's + shared lakes' files/ — from the Hub).
Inert outside JupyterHub (no LAKE_BUCKET). Hidden paths (.ipynb_checkpoints …) are skipped.
Older servers had notebooks/, dags/ and dbt/ right under ~/work: moved into my_files/ once.
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
TEAM_BUCKET = os.environ.get("TEAM_BUCKET", "")
MINE = "my_files"
# the team's folder is named after it: team-sales-lake → sales_team
TEAM = (TEAM_BUCKET[len("team-"):-len("-lake")].replace("-", "_") + "_team") if TEAM_BUCKET else "_no_team"
# dbt rebuilds these on every run — mirroring them would only fill the bucket
DBT_GENERATED = {"target", "logs", "dbt_packages"}
PREFIX = "files/src/"                          # their place in own + team bucket: files/src/notebooks, …
ROOT = "/home/jovyan/work"                     # the learner's persistent volume
BUCKET = os.environ.get("LAKE_BUCKET", "")
ENABLED = bool(BUCKET and os.environ.get("LAKE_S3_KEY"))
MARK = ".team-bucket"                          # in ~/work/<team>_team (or <owner>_lake): whose copies these are
# lakes shared with them: <owner>_lake → (owner's bucket, "read" | "write")
SHARED = {}
for _s in filter(None, os.environ.get("SHARED_LAKES", "").split(",")):
    _owner, _, _access = _s.partition(":")
    if _owner and _owner + "_lake" not in (MINE, TEAM):
        SHARED[_owner + "_lake"] = (_owner.replace("_", "-") + "-lake", _access or "read")
log = logging.getLogger("lab_sync")
_fs = None
_known = set()                                 # team folder paths seen in the bucket (or put there)
_pulled = {}                                   # team folder dir → when it was last pulled


def fs():
    global _fs
    if _fs is None:
        _fs = s3fs.S3FileSystem(
            key=os.environ["LAKE_S3_KEY"], secret=os.environ["LAKE_S3_SECRET"],
            client_kwargs={"endpoint_url": os.environ.get("AWS_S3_ENDPOINT", "http://storage:9000")},
            use_listings_cache=False)
    return _fs


def _parts(rel):
    return (rel or "").strip("/").split("/")


def _shared(rel):
    return ENABLED and _parts(rel)[0] in SHARED


def _team(rel):
    """In a folder whose bucket is the truth: the team folder or a shared lake."""
    return bool(ENABLED and ((TEAM_BUCKET and _parts(rel)[0] == TEAM) or _shared(rel)))


def read_only(rel):
    """In a lake shared with them read-only (its folder itself included)?"""
    return _shared(rel) and SHARED[_parts(rel)[0]][1] != "write"


def _bucket(rel):
    top = _parts(rel)[0]
    return {MINE: BUCKET, TEAM: TEAM_BUCKET}.get(top) or SHARED.get(top, ("",))[0]


def _prefix(top):
    return "files/" if top in SHARED else PREFIX


def synced(path):
    """Path relative to ROOT, like 'my_files/dags/etl/a.py' → is it inside a synced folder?"""
    parts = _parts(path)
    if not (ENABLED and _bucket(path)) or any(p.startswith(".") for p in parts):
        return False
    if parts[0] in SHARED:                    # a shared lake: all of its files/
        return len(parts) >= 2
    if len(parts) < 3 or parts[1] not in FOLDERS:
        return False
    return not (parts[1] == "dbt" and DBT_GENERATED.intersection(parts[2:-1]))


def _remote(rel):
    """my_files/dags/a.py → <bucket>/files/src/dags/a.py (also folders: sales_team → …/files/src;
    demouser_lake/source/x.csv → demouser-lake/files/source/x.csv)."""
    parts = _parts(rel)
    return f"{_bucket(rel)}/{_prefix(parts[0])}{'/'.join(parts[1:])}".rstrip("/")


def _local_rel(space, name):
    """An object name from s3fs (bucket/files/src/…) → its path relative to ROOT."""
    bucket = _bucket(space)
    return f"{space}/{name[len(bucket) + 1 + len(_prefix(space)):]}".rstrip("/")


def _upload(local, rel):
    """Upload one file; local mtime := the object's time, so the next start sees them equal."""
    try:
        fs().put_file(str(local), _remote(rel))
    except Exception as e:
        if "quota" in str(e).lower():
            where = ("the team bucket" if _parts(rel)[0] == TEAM else
                     "the shared lake" if _shared(rel) else "your bucket")
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


# ---- the team folder: the bucket is the truth --------------------------------------------
def _team_remote(rel_dir, recursive):
    """{rel: mtime} of the bucket's files under rel_dir (<team>_team/… or <owner>_lake/…), and its sub-folders."""
    base = _remote(rel_dir)
    top = _parts(rel_dir)[0]
    try:
        found = (fs().find(base, detail=True, withdirs=True) if recursive
                 else {i["name"]: i for i in fs().ls(base, detail=True)})
    except FileNotFoundError:
        return {}, set()
    files, dirs = {}, set()
    for name, info in found.items():
        rel = _local_rel(top, name.rstrip("/"))
        if info["type"] == "directory":
            if (len(_parts(rel)) >= 2 and (top in SHARED or _parts(rel)[1] in FOLDERS)
                    and not any(p.startswith(".") for p in _parts(rel))):
                dirs.add(rel)
        elif synced(rel) and not name.endswith("/"):
            files[rel] = info["LastModified"].timestamp()
    return files, dirs


def pull_team(rel_dir=TEAM, recursive=False, every=3.0):
    """Bring rel_dir (the team folder, a shared lake, or below) up to date with its bucket (at most every `every` s)."""
    rel_dir = rel_dir.strip("/") or TEAM
    if not _team(rel_dir):
        return
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
        if not local.exists() or local.stat().st_mtime < t - 1 or (read_only(rel) and local.stat().st_mtime > t + 1):
            local.parent.mkdir(parents=True, exist_ok=True)
            if local.exists():
                local.chmod(0o644)
            fs().get_file(_remote(rel), str(local))
            os.utime(local, (t, t))
        if read_only(rel):
            local.chmod(0o444)                        # JupyterLab opens it read-only
        _known.add(rel)
    # here but not in the bucket: deleted by a teammate (we saw it before) → remove it;
    # never seen there (an upload that failed) → try the upload again
    kids = here.rglob("*") if recursive else here.iterdir()
    for local in sorted(kids, key=lambda p: -len(p.parts)):
        rel = local.relative_to(ROOT).as_posix()
        if rel in remote or rel in dirs or _parts(rel)[-1].startswith("."):
            continue
        if local.is_file() and synced(rel):
            if rel in _known or read_only(rel):       # read-only: the owner's copy is the truth
                local.unlink()
                _known.discard(rel)
            else:
                _upload(local, rel)
        elif local.is_dir() and rel in _known and not any(local.iterdir()):
            local.rmdir()                             # a folder a teammate emptied and removed
            _known.discard(rel)


def _team_start(root):
    """Server start: copies of another team (the member moved, or the folder had another name) or
    of a lake no longer shared go — every marked folder that isn't this team's or a shared lake;
    each shared lake's top folder is pulled (the rest as it is opened); then the team's full pull."""
    keep = {TEAM: TEAM_BUCKET} if TEAM_BUCKET else {}
    keep.update({top: b for top, (b, _) in SHARED.items()})
    for d in Path(root).iterdir():
        mark = d / MARK
        if d.is_dir() and mark.exists() and keep.get(d.name) != mark.read_text().strip():
            shutil.rmtree(d, ignore_errors=True)
    for top, (bucket, _) in SHARED.items():
        try:
            Path(root, top).mkdir(parents=True, exist_ok=True)
            Path(root, top, MARK).write_text(bucket + "\n")
            pull_team(top, every=0)
        except Exception as e:  # noqa: BLE001 — the others still come
            log.warning("[lab_sync] %s: %s", top, e)
    if not TEAM_BUCKET:
        return
    team = Path(root, TEAM)
    mark = team / MARK
    for top in FOLDERS:
        Path(team, top).mkdir(parents=True, exist_ok=True)
    mark.write_text(TEAM_BUCKET + "\n")
    pull_team(TEAM, recursive=True, every=0)


def _move_old_layout(root):
    """notebooks/, dags/, dbt/ right under ~/work (older servers) → my_files/; team/ goes."""
    for top in FOLDERS:
        old, new = Path(root, top), Path(root, MINE, top)
        if not old.is_dir():
            continue
        new.mkdir(parents=True, exist_ok=True)
        for item in list(old.iterdir()):
            if not Path(new, item.name).exists():
                shutil.move(str(item), str(Path(new, item.name)))
        shutil.rmtree(old, ignore_errors=True)        # what's left was already in my_files/
    shutil.rmtree(Path(root, "team"), ignore_errors=True)   # the first team folder: comes back as <team>_team/


def initial_sync(root):
    """Server start: my_files — newest copy of every file wins, in both directions; <team>_team —
    the team bucket's copy."""
    if not ENABLED:
        return
    _move_old_layout(root)
    for top in FOLDERS:
        Path(root, MINE, top).mkdir(parents=True, exist_ok=True)
        try:
            found = fs().find(f"{BUCKET}/{PREFIX}{top}", detail=True)
        except Exception as e:
            log.warning("[lab_sync] cannot list %s/%s: %s", BUCKET, top, e)
            continue
        remote = {}
        for name, info in found.items():
            rel = _local_rel(MINE, name)
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
        for local in Path(root, MINE, top).rglob("*"):
            rel = local.relative_to(root).as_posix()
            if local.is_file() and synced(rel) and rel not in remote:
                _upload(local, rel)
    log.info("[lab_sync] %s ↔ s3://%s/%s synced", MINE, BUCKET, PREFIX)
    try:
        _team_start(root)
        if TEAM_BUCKET:
            log.info("[lab_sync] %s ↔ s3://%s/%s synced", TEAM, TEAM_BUCKET, PREFIX)
        for top, (bucket, access) in SHARED.items():
            log.info("[lab_sync] %s ↔ s3://%s/files/ (%s)", top, bucket, access)
    except Exception as e:  # noqa: BLE001 — the server still starts
        log.warning("[lab_sync] %s: %s", TEAM, e)


class SyncedFileManager(LargeFileManager):
    """The normal file manager + mirroring of my_files/ (own bucket), <team>_team/ (team bucket) and
    <owner>_lake/ (lakes shared with them).
    Works on real file paths (relative to ROOT), whatever the server's root_dir is."""

    def _rel(self, path):
        return os.path.relpath(self._get_os_path(path), ROOT)

    def get(self, path, content=True, type=None, format=None, **kwargs):
        rel = self._rel(path)
        if _team(rel) and not _parts(rel)[-1].startswith("."):
            try:                                      # teammates' changes first
                folder = rel if os.path.isdir(self._get_os_path(path)) else os.path.dirname(rel)
                pull_team(folder or TEAM)
            except Exception as e:  # noqa: BLE001
                log.warning("[lab_sync] pull %s: %s", rel, e)
        return super().get(path, content=content, type=type, format=format, **kwargs)

    def _refuse(self, rel):
        if read_only(rel):
            raise web.HTTPError(403, f"{_parts(rel)[0]} is shared with you read-only: save a copy in "
                                     "my_files/ to change it.")

    def save(self, model, path=""):
        rel = self._rel(path)
        self._refuse(rel)
        out = super().save(model, path)
        if synced(rel) and model.get("type") != "directory" and model.get("chunk", -1) == -1:
            _upload(self._get_os_path(path), rel)
        return out

    def delete_file(self, path):
        rel = self._rel(path)
        self._refuse(rel)
        super().delete_file(path)
        if synced(rel):
            _remove(rel)

    def rename_file(self, old_path, new_path):
        old, new = self._rel(old_path), self._rel(new_path)
        self._refuse(old)
        self._refuse(new)
        super().rename_file(old_path, new_path)
        if synced(old):
            _remove(old)
        if synced(new):
            _upload_tree(ROOT, new)
