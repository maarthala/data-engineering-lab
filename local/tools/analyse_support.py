"""Read a `make support` zip and print what matters first.

  python3 tools/analyse_support.py support-20261122-1310.zip [--all]

Prints: the user's words, the machine, services that aren't running / healthy / keep restarting,
failed requests through the gateway (grouped by app + status + path), failed Airflow tasks with
their error, and each container's most frequent error lines. --all lists every group, not the top.
"""
import collections
import json
import re
import sys
import zipfile

ERR = re.compile(r"error|exception|traceback|denied|forbidden|unauthori[sz]ed|refused|fatal|failed|"
                 r"not found|no such|timed? ?out|oomkilled|killed", re.I)
NOISE = re.compile(r"\b(?:INFO|DEBUG)\b|GET /health|healthcheck|0 errors?|errors?=0|failed=0|# Errors|"
                   r"timeout policy|coordinator timeout|query timeout|bf-error-rate", re.I)
B, D, R, Y, G, X = "\033[1m", "\033[2m", "\033[31m", "\033[33m", "\033[32m", "\033[0m"
if not sys.stdout.isatty():
    B = D = R = Y = G = X = ""


def section(title):
    print(f"\n{B}── {title} {'─' * max(0, 70 - len(title))}{X}")


def readable(line):
    """An Airflow 3 JSON log line → 'level: event' (other lines as they are)."""
    if line.startswith("{"):
        try:
            d = json.loads(line)
            lvl = d.get("level", "")                    # a task's own output comes as level info: show it bare
            return (f"{lvl}: " if lvl not in ("info", "debug") else "") + str(d.get("event", "")) + (
                f" — {d['error_detail']}" if d.get("error_detail") else "")
        except ValueError:
            pass
    return line


def norm(line):
    """A log line with what changes between repeats (time, ids, numbers, hex) taken out."""
    line = re.sub(r"^\S*\d{4}-\d\d-\d\dT\S+\s+", "", line)                         # docker --timestamps
    line = re.sub(r"\d{4}-\d\d-\d\d[T ][\d:.,]+\S*", "<time>", line)
    line = re.sub(r"\b[0-9a-f]{8}-[0-9a-f-]{27,}\b|\b[0-9a-f]{12,}\b", "<id>", line)
    line = re.sub(r"\b\d+\b", "<n>", line)
    return line.strip()[:220]


# ── First findings: the evidence in plain words, each with what to do ─────────────────────────────
MIN_CPUS, MIN_MEM_GB = 4, 8                       # tools/up.py: the lab's minimum for Docker
UNIT_OF = [("spark-", "Spark"), ("airflow-", "Airflow"), ("trino", "SQL"), ("sqlpad", "SQL"),
           ("superset", "Superset"), ("jupyter", "Notebooks"), ("kafka", "Streaming"), ("event-sim", "Streaming"),
           ("flink-", "Flink"), ("beam-", "Flink")]
APP_UNIT = {"airflow": "Airflow", "superset": "Superset", "sqlpad": "SQL", "trino": "SQL", "jupyter": "Notebooks",
            "spark": "Spark", "flink": "Flink"}
DENIED = re.compile(r"access denied|forbidden|not authori[sz]ed|permission denied|\b403\b|unauthori[sz]ed|\b401\b", re.I)


def one_shot(name):
    """Setup steps that run once and exit 0 (compose services with restart: "no")."""
    return name.endswith(("-init", "-bootstrap", "-seed")) or name in ("portal-db", "vault-db")


STUCK_MIN = 10


def stuck_minutes(read, p):
    """Minutes between a container's start (last status.txt column, UTC) and the newest log line in the bundle."""
    started = p[-1] if re.match(r"\d{4}-\d\d-\d\dT\d\d:\d\d", p[-1]) else None
    newest = max(re.findall(r"^(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)", read("containers/gateway.log") +
                            "\n" + read("containers/postgres.log"), re.M) or [""])
    if not started or not newest:
        return 0
    from datetime import datetime
    f = "%Y-%m-%dT%H:%M:%S"
    return int((datetime.strptime(newest, f) - datetime.strptime(started[:19], f)).total_seconds() // 60)


def unit_of(name):
    return next((u for pre, u in UNIT_OF if name.startswith(pre)), "Core")


def last_error(text, n=1):
    lines = [readable(l) for l in text.splitlines()]
    lines = [l for l in lines if len(re.findall(r"[A-Za-z]", l)) >= 8 and not re.search(r"DeprecationWarning|has been moved|has been renamed", l)]
    hits = [l for l in lines if ERR.search(l) and not NOISE.search(l)] or lines
    return [re.sub(r"^\S*\d{4}-\d\d-\d\dT\S+\s+", "", l).strip()[:200] for l in hits[-n:]]


def findings(read, names):
    """[(severity, finding, what to do)] — severity 1 = most likely the cause."""
    out = []
    # Docker itself
    m = re.search(r"Docker \S+ · (\d+) CPUs · ([\d.]+) GB memory", read("system.txt"))
    if m and (int(m.group(1)) < MIN_CPUS or float(m.group(2)) < MIN_MEM_GB):
        out.append((1, f"Docker has only {m.group(1)} CPUs / {m.group(2)} GB memory — the lab needs at least "
                       f"{MIN_CPUS} CPUs / {MIN_MEM_GB:g} GB.",
                    "Docker Desktop → Settings → Resources: raise CPUs and memory, Apply & restart, then make up."))
    res = read("resources.txt")
    m = re.search(r"summary: memory ([\d.]+) GB used by the stack of ([\d.]+) GB Docker has \((\d+)%\)", res)
    if m and int(m.group(3)) >= 85:
        out.append((1, f"Memory is nearly full: the stack uses {m.group(1)} of {m.group(2)} GB ({m.group(3)}%).",
                    "Turn off units you don't need now (make status → select a unit → x), or give Docker more memory."))
    m = re.search(r"summary: disk ([\d.]+) GB free of ([\d.]+) GB \((\d+)% used\)", res)
    if m and (float(m.group(1)) < 10 or int(m.group(3)) >= 90):
        out.append((1, f"Disk is nearly full: {m.group(1)} GB free of {m.group(2)} GB.",
                    "docker system prune (removes unused images and build cache — your data stays), "
                    "or give Docker's disk more space."))
    # services
    down, hc = [], {}
    for block in read("health.txt").split("===== ")[1:]:
        hc[block.split(":", 1)[0]] = block
    for line in read("status.txt").splitlines()[1:]:
        p = line.split()
        if len(p) < 5:
            continue
        name, state, health, restarts, code = p[:5]
        oom = len(p) > 5 and p[5] == "YES"
        log = read(f"containers/{name}.log")
        if oom or (code == "137" and state != "running"):
            out.append((1, f"{name} was killed for lack of memory.",
                        f"Give Docker more memory, or run fewer units at once ({unit_of(name)} needs it)."))
        elif state == "exited" and code == "0" and one_shot(name):
            continue                                          # a setup step that finished
        elif state == "running" and one_shot(name) and stuck_minutes(read, p) >= STUCK_MIN:
            err = last_error(log)
            out.append((1, f"Setup step {name} is still running {stuck_minutes(read, p)} min after it started"
                           + (f" — last line: {err[0].rstrip('. ')}" if err else "") + ". What it sets up is missing until it ends.",
                        f"docker restart {name}, wait until it exits (make status), then try again."))
        elif state not in ("running",):
            down.append(name)
            err = last_error(log)
            out.append((1 if unit_of(name) == "Core" else 2,
                        f"{name} is not running ({state}, exit {code})" + (f": {err[0]}" if err else "."),
                        f"make status → {unit_of(name)} → s to start it again" +
                        ("; if it stops again, the line above is why." if err else ".")))
        elif health == "unhealthy" and "exceeded timeout" in hc.get(name, ""):
            t = re.search(r"exceeded timeout \((\w+)\)", hc[name])
            out.append((3, f"{name} is marked unhealthy only because its health check took longer than "
                           f"{t.group(1) if t else 'allowed'} — the service itself may be fine.",
                        "Usually a busy machine. Pull the latest lab (longer check), or run fewer units at once."))
        elif health == "unhealthy" or (restarts.isdigit() and int(restarts) >= 3):
            err = last_error(log)
            out.append((2, f"{name} is {'unhealthy' if health == 'unhealthy' else f'restarting ({restarts}×)'}"
                           + (f": {err[0]}" if err else "."),
                        f"docker restart {name}" + ("; if it keeps failing, the line above is why." if err else ".")))
    # failed Airflow tasks
    for n in sorted(x for x in names if x.startswith("airflow-failed/") and x.endswith(".log")):
        text = read(n)
        head = text.splitlines()[0].split() if text.strip() else []
        task = f"{head[0]}.{head[1]}" if len(head) > 1 else n
        err = last_error(text, 3)
        why = " | ".join(err)
        if re.search(r"connection refused|UNAVAILABLE|spark-connect|spark-master", why, re.I):
            todo = "Spark isn't running: make status → Spark → s, then run the task again."
        elif DENIED.search(why):
            todo = ("A login was refused inside the task (storage / catalog / database). Check the DAG's "
                    "credentials; platform DAGs use the lab's own login — was .env changed?")
        else:
            todo = "Open the task's log in Airflow (red task → Logs) — the line above is the error."
        out.append((2, f"Airflow task {task} failed: {err[-1] if err else '(no log)'}", todo))
    # failed requests through the gateway
    seen = set()
    for line in read("problems.log").splitlines()[1:]:
        m = re.match(r'^\S+ (\d{3}) ([^.\s:]+)\S* "(\S+) (\S+)', line)
        if not m:
            continue
        status, app, method, url = m.groups()
        key = (status, app, method if status in ("401", "403") else "")
        if key in seen:
            continue
        seen.add(key)
        unit = APP_UNIT.get(app)
        if status in ("502", "503", "504") and unit:
            out.append((2, f"{app} didn't answer ({status}) — it was off or still starting.",
                        f"make status → {unit} → s, wait until it's healthy, then reload the page."))
        elif status == "403" and app == "airflow" and "dagRuns" in url:
            out.append((1, "Airflow refused to run a DAG (403): the account signed in to Airflow may not run it.",
                        "Platform DAGs such as shopflow_medallion run only as a manager: Log out of the portal, "
                        "sign in as manager, open Airflow again (top-right must say manager)."))
        elif status == "403" and method != "GET" and app != "flink":
            out.append((3, f"{app} refused a change (403 on {method} {url.split('?')[0][:60]}).",
                        "That account has no right to do this — try as a manager, or ask one for access."))
    if down and any(unit_of(d) == "Core" for d in down):
        out.append((1, "Part of Core is down — nothing else works without it.", "make down, then make up."))
    order = {}
    for sev, f, todo in sorted(out, key=lambda x: x[0]):
        order.setdefault(f, (sev, todo))
    return [(sev, f, todo) for f, (sev, todo) in order.items()]


def findings_text(read, names, color=False):
    r, y, d, x = (R, Y, D, X) if color else ("", "", "", "")
    items = findings(read, names)
    if not items:
        return "No obvious cause found — the logs in this zip will tell more.\n"
    lines = []
    for i, (sev, f, todo) in enumerate(items, 1):
        lines.append(f"{(r if sev == 1 else y)}{i}. {f}{x}\n   {d}→{x} {todo}")
    return "\n".join(lines) + "\n"


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    top = None if "--all" in sys.argv else 10
    if not args:
        print(__doc__)
        sys.exit(1)
    z = zipfile.ZipFile(args[0])
    files = {n.split("/", 1)[1]: n for n in z.namelist() if "/" in n}

    def read(name):
        return z.read(files[name]).decode("utf-8", "replace") if name in files else ""

    section("The user's words")
    readme = read("README.txt")
    print(readme.split("\n\n", 1)[0].replace("What went wrong (the user's words):\n", "").strip() or "(none)")

    section("First findings")
    print(findings_text(read, list(files), color=True).rstrip())

    section("Machine")
    print(read("system.txt").strip())

    section("Services that need a look")
    bad, oom = [], []
    for line in read("status.txt").splitlines()[1:]:
        p = line.split()
        if len(p) < 5:
            continue
        name, state, health, restarts, code = p[:5]
        if state == "exited" and code == "0" and one_shot(name):     # a setup step that finished
            continue
        why = []
        if state != "running":
            why.append(f"{state} (exit {code})")
        if health == "unhealthy":
            why.append("unhealthy")
        if restarts.isdigit() and int(restarts) > 0:
            why.append(f"restarted {restarts}×")
        if len(p) > 5 and p[5] == "YES":
            why.append("KILLED FOR LACK OF MEMORY")
            oom.append(name)
        elif code == "137" and state != "running":
            why.append("exit 137: killed — often lack of memory")
            oom.append(name)
        if why:
            bad.append(f"{R}{name:34}{X} {', '.join(why)}")
    print("\n".join(bad) if bad else f"{G}all running and healthy{X}")
    running = {l.split()[0] for l in read("status.txt").splitlines()[1:] if " running " in l}
    units = {"Spark": "spark-master", "Airflow": "airflow-scheduler", "SQL": "trino", "Superset": "superset",
             "Notebooks": "jupyterhub", "Streaming": "kafka", "Flink": "flink-jobmanager"}
    print(f"{D}units on: {', '.join(u for u, c in units.items() if c in running) or 'Core only'}{X}")

    section("Memory, CPU and disk")
    res = read("resources.txt")
    if not res:
        print("(not in this zip — made by an older make support)")
    for line in res.splitlines():
        if not line.startswith("summary:"):
            continue
        text = line[len("summary: "):]
        m = re.search(r"\((\d+)% used\)|\((\d+)%\)", text)
        pct = int(next(g for g in m.groups() if g)) if m else 0
        free = re.search(r"disk ([\d.]+) GB free", text)
        warn = pct >= 85 or (free and float(free.group(1)) < 10)
        print(f"{R if warn else G}{text}{X}")
        if warn and text.startswith("memory"):
            print(f"  {Y}→ Docker is short of memory: give it more (Docker Desktop → Settings → Resources) or turn "
                  f"units off in make status{X}")
        if warn and text.startswith("disk"):
            print(f"  {Y}→ disk nearly full: docker system prune (unused images / build cache), or give Docker's disk more space{X}")
    if oom:
        print(f"{R}killed for lack of memory: {', '.join(oom)}{X} — give Docker more memory or run fewer units")
    top3 = [l for l in res.splitlines() if re.match(r"^\S+\s+[\d.]+%", l)][:5]
    if top3:
        print(f"{D}biggest:{X}")
        for l in top3:
            print(f"  {l}")

    section("Failed requests through the gateway")
    groups, users = collections.Counter(), collections.defaultdict(set)
    for line in read("problems.log").splitlines()[1:]:
        m = re.match(r'^(\S+) (\d{3}) (\S+) "(\S+) (\S+)', line)
        if m:
            when, status, host, method, url = m.groups()
            path = re.sub(r"\?.*", "", url)
            path = re.sub(r"/[0-9a-f-]{16,}|/\d+(?=/|$)", "/<id>", path)
            key = (status, host.split(":")[0], f"{method} {path}")
            groups[key] += 1
            users[key].add(when[:16])
        elif line.startswith("("):
            print(f"{Y}{line}{X}")
    if not groups:
        print(f"{G}none{X}")
    for (status, host, req), n in groups.most_common(top):
        col = R if status.startswith("5") or status == "403" else Y
        times = sorted(users[(status, host, req)])
        print(f"{col}{status}{X} {n:>5}×  {host:28} {req[:90]}  {D}{times[0]} … {times[-1]}{X}")

    section("Failed Airflow tasks (last 7 days)")
    tasks = sorted(n for n in files if n.startswith("airflow-failed/") and n.endswith(".log"))
    if not tasks:
        print(read("airflow-failed/README.txt").strip() or read("airflow-failed/error.txt").strip()[:2000] or "(none)")
    for n in tasks[:top or None]:
        text = read(n)
        lines = [readable(l) for l in text.splitlines() if l.strip()]
        print(f"{B}{lines[0] if lines else n}{X}")
        hits = [l for l in lines[1:] if ERR.search(l) and not NOISE.search(l)]
        for l in (hits[-8:] or lines[-8:]):
            print(f"   {l.strip()[:240]}")

    section("Most frequent error lines per container")
    for n in sorted(f for f in files if f.startswith("containers/")):
        c = collections.Counter(norm(l) for l in read(n).splitlines() if ERR.search(l) and not NOISE.search(l))
        if not c:
            continue
        print(f"{B}{n[len('containers/'):-4]}{X}  {D}({sum(c.values())} lines){X}")
        for line, k in c.most_common(3 if top else None):
            print(f"  {k:>5}×  {line}")


if __name__ == "__main__":
    main()
