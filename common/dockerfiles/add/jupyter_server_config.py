# Auto-push on save: after Jupyter writes a notebook/file that lives under the
# cloned git repo, commit it and push to the remote. Enabled only when
# GIT_AUTOPUSH=1 (the entrypoint clones + configures the repo first). The push
# runs on a background thread so saving in the UI never blocks on the network;
# a lock serialises pushes so rapid saves don't race.
import datetime
import os
import subprocess
import threading

REPO = os.environ.get("GIT_AUTOPUSH_DIR", "/home/jovyan/work/repo")
ENABLED = os.environ.get("GIT_AUTOPUSH", "0") == "1"
_lock = threading.Lock()


def _run(args):
    return subprocess.run(args, cwd=REPO, capture_output=True, text=True)


def _autopush(os_path, log):
    with _lock:
        try:
            if not os.path.isdir(os.path.join(REPO, ".git")):
                return
            rel = os.path.relpath(os_path, REPO)
            if rel.startswith(".."):
                return  # saved outside the repo clone → nothing to push
            _run(["git", "add", "--", rel])
            # nothing staged (unchanged content) → skip an empty commit
            if not _run(["git", "status", "--porcelain"]).stdout.strip():
                return
            ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            _run(["git", "commit", "-m", f"auto-save: {rel} @ {ts}"])
            # rebase on top of remote first so a push isn't rejected non-ff
            _run(["git", "pull", "--rebase", "--autostash"])
            push = _run(["git", "push"])
            if push.returncode != 0:
                log.warning("[autopush] push failed for %s: %s", rel, push.stderr.strip())
            else:
                log.info("[autopush] pushed %s", rel)
        except Exception as exc:  # never let a hook break saving
            log.warning("[autopush] error: %s", exc)


def post_save_hook(model, os_path, contents_manager, **kwargs):
    if not ENABLED or model.get("type") not in ("notebook", "file"):
        return
    threading.Thread(
        target=_autopush, args=(os_path, contents_manager.log), daemon=True
    ).start()


c = get_config()  # noqa: F821  (provided by Jupyter's config loader)
c.FileContentsManager.post_save_hook = post_save_hook

# ---------------------------------------------------------------------------
# jupyter-fs: browse the object-store data lake as a drive in the left file browser
# (no code). MetaManager wraps the default local file manager and adds the
# S3/fsspec resources below as extra "drives". Because MetaManager builds its
# local drive from root_manager_class(**kwargs) with this same config, the
# post_save_hook above still fires for notebooks saved under the local repo.
# Creds + endpoint come from the pod env (same object store the Spark jobs use).
# ---------------------------------------------------------------------------
# jupyter-fs ships no auto-enable config.d entry, so enable its server extension
# explicitly — it installs the /jupyterfs/resources handler the left-panel browser
# calls (without it the UI 404s and no drive appears).
c.ServerApp.jpserver_extensions = {"jupyterfs.extension": True}


# An empty "folder marker" object (key "dags/") is listed by s3fs as a child of the
# folder itself, so the drive shows dags/dags/dags… forever. Hide the self-entry.
import fsspec  # noqa: E402
import s3fs  # noqa: E402


class _LabS3(s3fs.S3FileSystem):
    async def _ls(self, path, detail=False, **kwargs):
        out = await super()._ls(path, detail=True, **kwargs)
        here = self._strip_protocol(path).rstrip("/")
        out = [o for o in out if o["name"].rstrip("/") != here]
        return out if detail else [o["name"] for o in out]


fsspec.register_implementation("s3", _LabS3, clobber=True)
c.ServerApp.contents_manager_class = "jupyterfs.metamanager.MetaManager"
# Under JupyterHub the drive is the learner's OWN bucket (<user>-lake: files/ + tables/), with
# their own-bucket-only key from the Hub — never the root keys, so learners can't touch the
# shared lake or anyone else's bucket. The standalone Jupyter (no learner) browses demo-bucket.
_BUCKET = os.environ.get("LAKE_BUCKET")
_OWN = bool(_BUCKET and os.environ.get("LAKE_S3_KEY"))
c.JupyterFs.resources = [
    {
        "name": f"my bucket ({_BUCKET})" if _OWN else "lake (storage)",
        "url": f"s3://{_BUCKET}" if _OWN else "s3://demo-bucket",
        "type": "fsspec",
        "auth": "none",
        "kwargs": {
            "key": os.environ["LAKE_S3_KEY"] if _OWN else os.environ.get("AWS_ACCESS_KEY_ID", "admin"),
            "secret": os.environ["LAKE_S3_SECRET"] if _OWN else os.environ.get("AWS_SECRET_ACCESS_KEY", "admin123"),
            "client_kwargs": {
                "endpoint_url": os.environ.get("AWS_S3_ENDPOINT", "http://storage:9000"),
            },
        },
    }
]
# JupyterHub learners: my_files/{notebooks,dags,dbt} in the normal file browser are mirrored to their
# own bucket (dags → Airflow), my_team/{notebooks,dags,dbt} to their team's bucket. See lab_sync.py.
import sys  # noqa: E402
sys.path.insert(0, "/etc/jupyter")
import lab_sync  # noqa: E402

c.JupyterFs.root_manager_class = lab_sync.SyncedFileManager
try:                                    # never block the server from starting
    lab_sync.initial_sync(lab_sync.ROOT)
except Exception as exc:
    print(f"[lab_sync] initial sync failed: {exc}", flush=True)
