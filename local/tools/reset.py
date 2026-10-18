#!/usr/bin/env python3
"""`make clean` and `make destroy` — only ever this stack (compose project of local/), never other
projects' containers, volumes or images on the machine.

    python3 tools/reset.py clean     the stack must be down (`make down`): delete its data — every
                                     volume (Postgres, object storage, catalog, members, their
                                     Jupyter work …). Images stay, so `make up` starts fresh, fast.
    python3 tools/reset.py destroy   everything the stack made: containers, volumes, network and
                                     images (built and pulled). `make up` then rebuilds from scratch.

Both list what goes and ask you to type "yes" (LAB_YES=1 skips the question). The downloads from
`make init` (common/dockerfiles/tmp/) are kept — delete that folder yourself to re-download.
"""
import json
import os
import shutil
import subprocess
import sys

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))        # local/
G, Y, R, D, B, X = "\033[32m", "\033[33m", "\033[31m", "\033[2m", "\033[1m", "\033[0m"
if not sys.stdout.isatty():
    G = Y = R = D = B = X = ""


def run(*cmd):
    return subprocess.run(cmd, cwd=HERE, capture_output=True, text=True)


def lines(*cmd):
    return [x for x in run(*cmd).stdout.splitlines() if x.strip()]


def project():
    cfg = json.loads(run("docker", "compose", "config", "--format", "json").stdout or "{}")
    return cfg.get("name", "local"), cfg


def stack_containers(name, running_only=False):
    flt = ["--filter", f"label=com.docker.compose.project={name}"]
    ours = lines("docker", "ps", *([] if running_only else ["-a"]), *flt, "--format", "{{.Names}}")
    members = lines("docker", "ps", *([] if running_only else ["-a"]), "--filter", "name=^jupyter-",
                    "--format", "{{.Names}}")
    return ours, members


def stack_volumes(name, cfg):
    """The stack's volumes: those labelled with the compose project, plus every volume the compose
    files declare (some were created before compose labelled them, e.g. local_postgres-data)."""
    labelled = lines("docker", "volume", "ls", "--filter", f"label=com.docker.compose.project={name}",
                     "--format", "{{.Name}}")
    declared = [v.get("name") or f"{name}_{k}" for k, v in (cfg.get("volumes") or {}).items()
                if not v.get("external")]
    exists = set(lines("docker", "volume", "ls", "--format", "{{.Name}}"))
    ours = sorted((set(labelled) | set(declared)) & exists)
    members = lines("docker", "volume", "ls", "--filter", "name=^jupyterhub-user-", "--format", "{{.Name}}")
    return ours, members


def stack_images(name, cfg):
    built = [conf.get("image") or f"{name}-{svc}" for svc, conf in cfg.get("services", {}).items()
             if conf.get("build")]
    pulled = sorted({conf["image"] for conf in cfg.get("services", {}).values()
                     if conf.get("image") and not conf.get("build")})
    have = set(lines("docker", "image", "ls", "--format", "{{.Repository}}:{{.Tag}}")) | \
        set(lines("docker", "image", "ls", "--format", "{{.Repository}}"))
    return [i for i in built if i in have or f"{i}:latest" in have], [i for i in pulled if i in have]


def size_of(volumes):
    """Rough total of the volumes (docker system df -v); '' when unknown."""
    out = run("docker", "system", "df", "-v", "--format", "{{json .Volumes}}").stdout
    try:
        vols = json.loads(out)
    except ValueError:
        return ""
    from_units = {"B": 1, "kB": 1e3, "KB": 1e3, "MB": 1e6, "GB": 1e9, "TB": 1e12}
    total = 0.0
    for v in vols or []:
        if v.get("Name") in volumes:
            s = v.get("Size", "0B")
            num = "".join(c for c in s if c.isdigit() or c == ".")
            unit = s[len(num):].strip()
            total += float(num or 0) * from_units.get(unit, 1)
    return f" (~{total / 1e9:.1f} GB)" if total >= 1e8 else f" (~{total / 1e6:.0f} MB)" if total else ""


def confirm(what):
    if os.environ.get("LAB_YES") == "1":
        return True
    if not sys.stdin.isatty():
        print(f"{R}Not a terminal — set LAB_YES=1 to {what} without the question.{X}")
        return False
    try:
        return input(f"\n  Type {B}yes{X} to {what}: ").strip().lower() == "yes"
    except EOFError:
        return False


def show(title, items, colour=""):
    if items:
        print(f"  {B}{title}{X} ({len(items)}): {colour}{', '.join(items)}{X}")


def clean():
    name, cfg = project()
    running, member_running = stack_containers(name, running_only=True)
    if running or member_running:
        print(f"\n{Y}{B}The stack is still running{X} ({len(running) + len(member_running)} containers).")
        print(f"  Bring it down first: {B}make down{X} — then {B}make clean{X}.\n")
        return 1
    vols, member_vols = stack_volumes(name, cfg)
    leftover, member_left = stack_containers(name)
    if not (vols or member_vols or leftover or member_left):
        print(f"\n{G}✓ Nothing to clean{X} — the stack has no data yet. {D}make up starts it.{X}\n")
        return 0
    print(f"\n{R}{B}make clean deletes ALL the lab's data{X} — back to a fresh install:")
    show("volumes", vols, R)
    show("members' Jupyter work", member_vols, R)
    show("stopped containers", leftover + member_left)
    print(f"  {D}= every member and account, their lakehouses, tables, buckets, notebooks and DAGs, "
          f"the catalog, Airflow / Superset / SQLPad history{size_of(vols + member_vols)}.{X}")
    print(f"  {G}Kept:{X} the images (so the next start is fast) and the make init downloads.")
    if not confirm("delete it"):
        print("  Nothing deleted.\n")
        return 1
    run("docker", "compose", "down", "-v", "--remove-orphans")
    if member_left:
        run("docker", "rm", "-f", *member_left)
    gone = [v for v in vols + member_vols if run("docker", "volume", "rm", "-f", v).returncode == 0]
    left = set(vols + member_vols) - set(gone)
    print(f"\n{G}{B}✓ Clean{X} — {len(gone)} volumes deleted" +
          (f"; {R}not deleted: {', '.join(sorted(left))}{X}" if left else "") + ".")
    print(f"  Next: {B}make up{X} (it recreates the catalog), then the {B}shopflow_medallion{X} DAG once "
          f"{D}(the shared lake starts empty){X}.\n")
    return 1 if left else 0


def destroy():
    name, cfg = project()
    cont, member_cont = stack_containers(name)
    vols, member_vols = stack_volumes(name, cfg)
    built, pulled = stack_images(name, cfg)
    if not (cont or member_cont or vols or member_vols or built or pulled):
        print(f"\n{G}✓ Nothing to destroy{X} — no containers, volumes or images of this stack.\n")
        return 0
    print(f"\n{R}{B}make destroy removes everything this stack made{X} — a clean plate:")
    show("containers", cont + member_cont)
    show("volumes (all data)", vols + member_vols, R)
    show("images built here", built)
    show("images pulled", pulled)
    print(f"  {D}Data{size_of(vols + member_vols)} and images are gone; the next make up rebuilds every image "
          f"(15–30 min).{X}")
    print(f"  {G}Kept:{X} the make init downloads (common/dockerfiles/tmp/) and other projects' Docker things.")
    if not confirm("destroy it"):
        print("  Nothing removed.\n")
        return 1
    if member_cont:
        run("docker", "rm", "-f", *member_cont)
    run("docker", "compose", "down", "-v", "--remove-orphans", "--rmi", "all")
    for v in vols + member_vols:
        run("docker", "volume", "rm", "-f", v)
    left_img = [i for i in built + pulled if run("docker", "image", "rm", i).returncode != 0
                and run("docker", "image", "inspect", i).returncode == 0]
    shutil.rmtree(os.path.join(HERE, "logs"), ignore_errors=True)
    print(f"\n{G}{B}✓ Destroyed{X} — containers, volumes, network and images of the lab are gone.")
    if left_img:
        print(f"  {Y}Still there (another container uses them): {', '.join(left_img)}{X}")
    print(f"  {D}Builder cache is shared with your other projects and was left alone "
          f"(docker builder prune frees it).{X}")
    print(f"  Start over: {B}make up{X} (builds everything and recreates the catalog).\n")
    return 0


if __name__ == "__main__":
    what = sys.argv[1] if len(sys.argv) > 1 else ""
    if what not in ("clean", "destroy"):
        sys.exit(__doc__)
    if run("docker", "info").returncode != 0:
        sys.exit(f"{R}Docker isn't running.{X}")
    sys.exit(clean() if what == "clean" else destroy())
