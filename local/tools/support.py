"""make support — one zip with everything needed to look into a problem on this machine.

The user runs `make support` (from local/) and sends the zip it writes. Inside:
  README.txt          what's in the zip + the user's own words (asked for, optional)
  FINDINGS.txt        first findings: what looks wrong and what to try (also printed at the end)
  system.txt          OS, Docker (version, CPUs, memory), lab version, git commit
  status.txt          every service: state, health, restarts, exit code, killed for lack of memory
  health.txt          why Docker calls a service unhealthy (its last health-check results)
  resources.txt       CPU / memory per container, the stack's total vs Docker's memory, disk space
  problems.log        every failed request through the gateway (4xx / 5xx): time, status, app, request
  airflow-failed/     the log of each failed Airflow task (last 7 days)
  containers/<x>.log  the last lines of each container's log (members' Jupyter servers too)
  env.txt             .env with passwords, secrets, tokens, keys and emails masked

Secrets never leave the machine: every value of a secret-looking .env key is replaced by *** in
all files, and so are passwords in URLs, Bearer tokens and password=… pairs. Reads Docker only,
so it works when the portal is down or nobody can sign in. Needs nothing but python3 + docker.
Read a zip with: python3 tools/analyse_support.py <zip>
"""
import datetime
import json
import os
import platform
import re
import subprocess
import sys
import zipfile

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))     # local/
TAIL = os.environ.get("SUPPORT_TAIL", "2000")
SECRET_KEY = re.compile(r"PASS|SECRET|TOKEN|KEY|CREDENTIAL|EMAIL|PRIVATE", re.I)
PATTERNS = [
    (re.compile(r"(://[^/\s:@]+:)[^@\s/]+@"), r"\1***@"),                       # user:pass@host
    (re.compile(r"(Bearer\s+)[A-Za-z0-9._~+/=-]{8,}", re.I), r"\1***"),
    (re.compile(r"((?:password|passwd|secret|token|credential|client_secret)[\"']?\s*[=:]\s*[\"']?)[^\s\"'&]+", re.I), r"\1***"),
    (re.compile(r"\b(?:ghp|github_pat|glpat)_[A-Za-z0-9_]{10,}"), "***"),
    (re.compile(r"\beyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]+"), "***jwt***"),
]


def run(*cmd, timeout=60):
    try:
        r = subprocess.run(cmd, cwd=HERE, capture_output=True, text=True, timeout=timeout,
                           encoding="utf-8", errors="replace")
        return r.stdout + (r.stderr if r.returncode else "")
    except Exception as e:  # noqa: BLE001
        return f"({' '.join(cmd[:3])}: {e})"


def read_env():
    env = {}
    path = os.path.join(HERE, ".env")
    if os.path.exists(path):
        for line in open(path, encoding="utf-8", errors="replace"):
            if "=" in line and not line.lstrip().startswith("#"):
                k, v = line.rstrip("\n").split("=", 1)
                env[k.strip()] = v.strip().strip("'\"")
    return env


class Masker:
    def __init__(self, env):
        vals = {v for k, v in env.items() if SECRET_KEY.search(k) and len(v) >= 4}
        self.values = sorted(vals, key=len, reverse=True)

    def __call__(self, text):
        for v in self.values:
            text = text.replace(v, "***")
        for p, r in PATTERNS:
            text = p.sub(r, text)
        return text


def containers():
    """[(name, service)] of this stack's containers (running or not) + members' Jupyter servers."""
    out = []
    for line in run("docker", "compose", "--profile", "*", "ps", "-a", "--format", "json").splitlines():
        try:
            rows = json.loads(line)
        except ValueError:
            continue
        for c in rows if isinstance(rows, list) else [rows]:
            out.append((c.get("Name"), c.get("Service")))
    for name in run("docker", "ps", "-a", "--filter", "name=^jupyter-", "--format", "{{.Names}}").split():
        out.append((name, "members' Jupyter"))
    return [c for c in out if c[0]]


def status(names):
    lines = [f"{'container':34} {'state':10} {'health':10} {'restarts':>8} {'exit':>5} {'oom-killed':>10}  started"]
    if names:
        fmt = "{{.Name}}|{{.State.Status}}|{{if .State.Health}}{{.State.Health.Status}}{{end}}|{{.RestartCount}}|{{.State.ExitCode}}|{{.State.OOMKilled}}|{{.State.StartedAt}}"
        for line in run("docker", "inspect", "--format", fmt, *names).splitlines():
            p = line.lstrip("/").split("|")
            if len(p) == 7:
                oom = "YES" if p[5] == "true" else "-"
                lines.append(f"{p[0]:34} {p[1]:10} {p[2] or '-':10} {p[3]:>8} {p[4]:>5} {oom:>10}  {p[6][:19]}")
    return "\n".join(lines) + "\n"


SIZE = {"B": 1, "KB": 1e3, "MB": 1e6, "GB": 1e9, "TB": 1e12, "KIB": 2**10, "MIB": 2**20, "GIB": 2**30, "TIB": 2**40}


def to_bytes(text):
    m = re.match(r"\s*([\d.]+)\s*([KMGT]?i?B)", text, re.I)
    return float(m.group(1)) * SIZE.get(m.group(2).upper(), 1) if m else 0.0


def resources(names):
    """CPU / memory per running container (a snapshot), the stack's total against Docker's memory,
    and disk: Docker's own use + free space where the containers live. `summary:` lines are for
    analyse_support.py."""
    out, used = [], 0.0
    running = run("docker", "ps", "--format", "{{.Names}}").split()
    mine = [n for n in names if n in running]
    total = to_bytes(run("docker", "info", "--format", "{{.MemTotal}}").strip() + "B")
    if mine:
        rows = []
        for line in run("docker", "stats", "--no-stream", "--format",
                        "{{.Name}}|{{.CPUPerc}}|{{.MemUsage}}|{{.MemPerc}}", *mine, timeout=120).splitlines():
            p = line.split("|")
            if len(p) == 4:
                mem = to_bytes(p[2].split("/")[0])
                used += mem
                rows.append((mem, f"{p[0]:34} {p[1]:>8} {p[2]:>24} {p[3]:>8}"))
        out.append(f"{'container (biggest first)':34} {'CPU':>8} {'memory used / limit':>24} {'mem %':>8}")
        out += [r for _, r in sorted(rows, reverse=True)]
    if total:
        out.insert(0, f"summary: memory {used / 2**30:.1f} GB used by the stack of {total / 2**30:.1f} GB Docker has "
                      f"({100 * used / total:.0f}%) · {len(mine)} containers running\n")
    box = "gateway" if "gateway" in running else (mine[0] if mine else None)
    if box:                                       # the disk Docker's containers and volumes live on
        df = run("docker", "exec", box, "df", "-Pk", "/").splitlines()
        p = df[-1].split() if len(df) > 1 else []
        if len(p) >= 5 and p[1].isdigit():
            size, free = int(p[1]) * 1024, int(p[3]) * 1024
            out.append(f"\nsummary: disk {free / 2**30:.1f} GB free of {size / 2**30:.1f} GB "
                       f"({100 - 100 * free / size:.0f}% used) — where Docker keeps images, containers and volumes")
    out.append("\nDocker disk use (docker system df):\n" + run("docker", "system", "df"))
    return "\n".join(out) + "\n"


def health(names):
    """The last health-check results of each container that isn't healthy — why Docker calls it unhealthy."""
    out = []
    for name in names:
        h = run("docker", "inspect", "--format", "{{json .State.Health}}", name).strip()
        try:
            h = json.loads(h)
        except ValueError:
            continue
        if not h or h.get("Status") == "healthy":
            continue
        out.append(f"===== {name}: {h.get('Status')} (failed {h.get('FailingStreak', 0)}× in a row)")
        for r in (h.get("Log") or [])[-2:]:
            out.append(f"  exit {r.get('ExitCode')}: {(r.get('Output') or '').strip()[:600]}")
    return "\n".join(out) + "\n" if out else "all health checks pass\n"


def system(env):
    info = run("docker", "info", "--format", "{{.ServerVersion}}|{{.NCPU}}|{{.MemTotal}}|{{.OperatingSystem}}")
    p = info.strip().split("|")
    docker = (f"Docker {p[0]} · {p[1]} CPUs · {int(p[2]) / 2**30:.1f} GB memory · {p[3]}"
              if len(p) == 4 and p[2].isdigit() else f"Docker: {info.strip()[:300]}")
    changed = [x for x in run("git", "status", "--porcelain").splitlines() if x.strip()]
    return "\n".join([
        f"collected   {datetime.datetime.now().astimezone():%Y-%m-%d %H:%M %Z}",
        f"machine     {platform.system()} {platform.release()} ({platform.machine()}) · python {platform.python_version()}",
        f"docker      {docker}",
        f"compose     {run('docker', 'compose', 'version', '--short').strip()}",
        f"lab version {run('git', 'describe', '--tags', '--always').strip()}",
        f"git commit  {run('git', 'log', '-1', '--format=%h %cs %s').strip()}",
        f"local edits {len(changed)} file(s) changed" + "".join(f"\n              {c}" for c in changed[:30]),
        f"units on    {env.get('COMPOSE_PROFILES', '(default)')}",
    ]) + "\n"


def problems():
    """Failed requests (4xx / 5xx) through the gateway, from its log (the `lab` format)."""
    log = run("docker", "logs", "gateway", timeout=120)
    keep = [l for l in log.splitlines() if re.match(r"^\S+T\S+ [45]\d\d ", l)]
    head = "time status app request seconds referer   (gateway: every web app goes through it)\n"
    if not re.search(r"^\d{4}-\d\d-\d\dT\S+ \d{3} ", log, re.M):     # no line in the `lab` format yet
        head += "(the gateway log has no app column yet — restart the gateway once: docker restart gateway)\n"
    return head + "\n".join(keep) + "\n"


AIRFLOW_FAILED = r'''
import glob, os, datetime
from airflow.models import TaskInstance
from airflow.utils.session import create_session
since = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=7)
with create_session() as s:
    tis = s.query(TaskInstance).filter(TaskInstance.state.in_(["failed", "upstream_failed"]),
                                       TaskInstance.start_date >= since).order_by(TaskInstance.start_date.desc()).limit(40).all()
    for t in tis:
        d = f"/opt/airflow/logs/dag_id={t.dag_id}/run_id={t.run_id}/task_id={t.task_id}"
        files = sorted(glob.glob(d + "/**/attempt=*.log", recursive=True), key=os.path.getmtime)
        print(f"=====TASK {t.dag_id} {t.task_id} {t.state} {t.start_date:%Y-%m-%d_%H%M%S} {t.run_id}")
        if files:
            print(open(files[-1], errors="replace").read()[-60000:])
        else:
            print("(no log file — the task never started)")
'''


def airflow_failed():
    """{filename: text} — the latest attempt's log of each failed task (last 7 days)."""
    names = run("docker", "ps", "--format", "{{.Names}}").split()
    box = next((n for n in ("airflow-scheduler", "airflow-apiserver") if n in names), None)
    if not box:
        return {"README.txt": "Airflow wasn't running — no task logs collected.\n"}
    out, cur, buf = {}, None, []
    for line in run("docker", "exec", box, "python", "-c", AIRFLOW_FAILED, timeout=180).splitlines():
        if line.startswith("=====TASK "):
            if cur:
                out[cur] = "\n".join(buf) + "\n"
            dag, task, state, when = line.split()[1:5]
            cur, buf = f"{when}_{dag}.{task}.log", [line[10:]]
        elif cur:
            buf.append(line)
        else:
            buf.append(line)
    if cur:
        out[cur] = "\n".join(buf) + "\n"
    elif buf:
        out["error.txt"] = "\n".join(buf) + "\n"
    return out or {"README.txt": "No failed Airflow tasks in the last 7 days.\n"}


def ask():
    if not sys.stdin.isatty():
        return ""
    print("What went wrong? One or two lines — which page / tool, what you did, what you saw.")
    print("(optional — press Enter to skip)")
    try:
        return input("> ").strip()
    except EOFError:
        return ""


def main():
    if run("docker", "info", "--format", "ok").strip() != "ok":
        print("Docker isn't running — start Docker Desktop (or the docker service) and try again.")
        sys.exit(1)
    story = ask()
    env = read_env()
    mask = Masker(env)
    stamp = datetime.datetime.now().strftime("%Y%m%d-%H%M")
    path = os.path.join(HERE, f"support-{stamp}.zip")
    print("Collecting …")
    names = containers()
    files = {
        "README.txt": (f"What went wrong (the user's words):\n  {story or '(not given)'}\n\n" + __doc__.split("Secrets never")[0]),
        "system.txt": system(env),
        "status.txt": status([n for n, _ in names]),
        "resources.txt": resources([n for n, _ in names]),
        "health.txt": health([n for n, _ in names]),
        "problems.log": problems(),
        "env.txt": "".join(f"{k}={'***' if SECRET_KEY.search(k) and v else v}\n" for k, v in env.items()),
    }
    for name, text in airflow_failed().items():
        files[f"airflow-failed/{name}"] = text
    for i, (name, svc) in enumerate(names, 1):
        print(f"\r  container logs {i}/{len(names)}", end="", flush=True)
        files[f"containers/{name}.log"] = run("docker", "logs", "--timestamps", "--tail", TAIL, name, timeout=60)
    print()
    files = {name: mask(text) for name, text in files.items()}
    found = first_findings(files)
    if found:
        files["FINDINGS.txt"] = "First findings (automatic — a starting point, not a verdict):\n\n" + found[0]
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as z:
        for name, text in files.items():
            z.writestr(f"support-{stamp}/{name}", text)
    size = os.path.getsize(path) / 2**20
    if found:
        print("\nFirst findings — what looks wrong, and what to try:\n")
        print(found[1].rstrip())
    print(f"\n✅ {path}  ({size:.1f} MB)")
    print("If the steps above don't fix it, send this file to whoever helps you. Passwords, secrets and")
    print("tokens are masked (***); it does hold usernames, app names and the stack's logs.")


def first_findings(files):
    """(plain text for the zip, text for the terminal) — from analyse_support.py; never fails make support."""
    try:
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        import analyse_support as a
        read = lambda n: files.get(n, "")                                   # noqa: E731
        return a.findings_text(read, list(files)), a.findings_text(read, list(files), color=sys.stdout.isatty())
    except Exception as e:  # noqa: BLE001
        return f"(couldn't work out findings: {e})\n", ""


if __name__ == "__main__":
    main()
