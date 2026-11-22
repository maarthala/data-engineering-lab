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

    section("Machine")
    print(read("system.txt").strip())

    section("Services that need a look")
    bad = []
    for line in read("status.txt").splitlines()[1:]:
        p = line.split()
        if len(p) < 5:
            continue
        name, state, health, restarts, code = p[:5]
        if state == "exited" and code == "0" and restarts == "0":     # a setup step that finished
            continue
        why = []
        if state != "running":
            why.append(f"{state} (exit {code})")
        if health == "unhealthy":
            why.append("unhealthy")
        if restarts.isdigit() and int(restarts) > 0:
            why.append(f"restarted {restarts}×")
        if why:
            bad.append(f"{R}{name:34}{X} {', '.join(why)}")
    print("\n".join(bad) if bad else f"{G}all running and healthy{X}")
    running = {l.split()[0] for l in read("status.txt").splitlines()[1:] if " running " in l}
    units = {"Spark": "spark-master", "Airflow": "airflow-scheduler", "SQL": "trino", "Superset": "superset",
             "Notebooks": "jupyterhub", "Streaming": "kafka", "Flink": "flink-jobmanager"}
    print(f"{D}units on: {', '.join(u for u, c in units.items() if c in running) or 'Core only'}{X}")

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
