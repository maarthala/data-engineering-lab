#!/usr/bin/env python3
"""Live views of the local stack, in the terminal.

    python3 tools/up.py            `make up`: start (docker compose up -d) — the compose log on the
                                   left, every service's state on the right. Images are built only
                                   when missing; it warns when one is older than its Dockerfile
    python3 tools/up.py --build    `make build`: rebuild the images first (docker compose up -d --build)
    python3 tools/up.py --watch    `make status`: the running stack — all containers' logs on the
                                   left; state, CPU, memory and port of every service on the right
    python3 tools/up.py --stop     `make down`: members' Jupyter servers, then docker compose down
                                   (volumes kept) — each service going running → stopping → removed
                                   (--dry-run: show it without stopping anything)

Standard library only (curses). Not a terminal (CI, a pipe) → plain `docker compose` output.
`make up` keeps its full log in logs/make-up.log.
"""
import collections
import curses
import json
import locale
import shutil
import os
import re
import subprocess
import sys
import threading
import time

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))        # local/
LOG = os.path.join(HERE, "logs", "make-up.log")
LOG_SHOWN = "local/logs/make-up.log"
UNIT_LOG = os.path.join(HERE, "logs", "make-status.log")   # make status: starting / stopping units
BUILD = "--build" in sys.argv or os.environ.get("BUILD") == "1"
UP = ["docker", "compose", "--progress", "plain", "up", "-d"] + (["--build"] if BUILD else [])
# What Docker must be given (Docker Desktop: Settings → Resources). Memory in GB as Docker Desktop
# shows it: a "12 GB" setting reports ~11.9 GiB, so compare with a little slack.
# LAB_MIN_CPUS / LAB_MIN_MEM_GB override the minimum for one run (e.g. to try the check).
MIN_CPUS = int(os.environ.get("LAB_MIN_CPUS", 4))
MIN_MEM_GB = float(os.environ.get("LAB_MIN_MEM_GB", 8))
REC_CPUS, REC_MEM_GB = max(6, MIN_CPUS), max(12, MIN_MEM_GB)
SETTLE_TIMEOUT = 300           # after compose is done: wait this long for health checks
MEMBERS = "members' Jupyter"   # group of the JupyterHub-started servers (jupyter-<user>, not compose)

# Units: what a member switches on for what they're learning. Core (sign-in, the lakehouse's catalog
# and storage, secrets) is what every unit needs: stopping it stops the whole stack, like make down.
GROUPS = [
    ("Core", ["postgres", "redis", "keycloak", "vault-db", "vault", "vault-init", "oauth2-proxy", "nginx",
              "home-api", "storage", "storage-init", "polaris-bootstrap", "polaris"]),
    ("SQL", ["trino", "sqlpad"]),
    ("Notebooks", ["jupyterhub", "jupyter"]),           # pandas, SQL, Python: no Spark needed
    ("Spark", ["spark-master", "spark-worker-1", "spark-connect"]),
    ("Airflow", ["airflow-init", "airflow-apiserver", "airflow-scheduler", "airflow-dag-processor", "airflow-dag-processor-learners"]),
    ("Superset", ["superset"]),
    ("Polaris console", ["polaris-console", "polaris-proxy"]),
]
LOCKED = {"Core"}
NEEDS = {"Superset": ["SQL"]}          # starting Superset starts SQL; stopping SQL stops Superset
USES = {"Notebooks": "Spark", "Airflow": "Spark"}  # only some work needs it: a hint, not started for you
LINKS = [("Landing page", "oauth2-proxy"), ("Airflow", "airflow-apiserver"), ("Superset", "superset"),
         ("SQLPad", "sqlpad"), ("Keycloak", "keycloak")]
SPIN = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"
LOG_COLOURS = ["cyan", "magenta", "blue", "yellow", "green"]

#            symbol, colour, text
LOOK = {
    "waiting":    ("·", "dim", "waiting"),
    "pulling":    (None, "blue", "pulling"),
    "building":   (None, "cyan", "building"),
    "built":      ("◆", "cyan", "built"),
    "creating":   (None, "yellow", "creating"),
    "starting":   (None, "yellow", "starting"),
    "health":     (None, "yellow", "health check"),
    "running":    ("●", "green", "running"),
    "healthy":    ("●", "green", "healthy"),
    "done":       ("✓", "green", "done"),
    "stopped":    ("○", "dim", "stopped"),
    "stopping":   (None, "yellow", "stopping"),
    "removed":    ("○", "dim", "removed"),
    "restarting": ("↻", "red", "restarting"),
    "unhealthy":  ("✗", "red", "unhealthy"),
    "failed":     ("✗", "red", "failed"),
}
READY = {"running", "healthy", "done"}
BAD = {"failed", "unhealthy", "restarting"}
UNITS = {"B": 1, "KiB": 2**10, "MiB": 2**20, "GiB": 2**30, "TiB": 2**40, "kB": 1e3, "MB": 1e6, "GB": 1e9}


def compose(*args):
    return subprocess.run(["docker", "compose", *args], cwd=HERE, capture_output=True, text=True)


def size(text):
    """'79.61MiB' → bytes."""
    m = re.match(r"([\d.]+)\s*([A-Za-z]+)", text.strip())
    return float(m.group(1)) * UNITS.get(m.group(2), 1) if m else 0.0


def human(n):
    for u, f in (("G", 2**30), ("M", 2**20), ("K", 2**10)):
        if n >= f:
            return f"{n / f:.1f}{u}" if n < 10 * f else f"{n / f:.0f}{u}"
    return f"{n:.0f}B"


class Stack:
    """What we know about each service — from the compose log, `docker compose ps` and `docker stats`."""

    def __init__(self, mode="up"):
        self.mode = mode                        # up · watch · stop
        cfg = json.loads(compose("config", "--format", "json").stdout or "{}").get("services", {})
        self.names = list(cfg) or [s for _, g in GROUPS for s in g]
        self.container = {s.get("container_name", f"local-{n}-1"): n for n, s in cfg.items()}
        # setup jobs (run once, exit 0): restart "no", or what others wait on to complete
        self.oneshot = {n for n, s in cfg.items() if s.get("restart") == "no"} | {
            d for s in cfg.values() for d, c in (s.get("depends_on") or {}).items()
            if c.get("condition") == "service_completed_successfully"}
        self.pending = {}                       # status: service → starting/stopping while a unit changes
        self.busy = None                        # status: the units being started / stopped
        self.message = None                     # status: (text, when) — shown in the bottom line
        self.state = {n: "waiting" for n in self.names}
        self.ports, self.stats, self.info = {}, {}, {}
        self.members = []                       # jupyter-<user> containers (started by JupyterHub)
        self.mem_limit = 0
        self.lines = collections.deque(maxlen=5000)    # make up: compose log · status: (service, line)
        self.lock = threading.Lock()
        self.compose_rc = None
        self.started = time.time()

    # --- the compose log (make up) -------------------------------------------------------
    BUILD = re.compile(r"^(?:#\d+ \[([\w.-]+)[ \]]|\s*([\w.-]+) ==> )")   # BuildKit / compose 5 build steps
    EVENT = re.compile(r"^\s*(?:Container|Image|Service|Network|Volume)?\s*([\w.-]+)\s+"
                       r"(Pulling|Pulled|Building|Built|Creating|Created|Recreate|Recreated|Starting|"
                       r"Started|Waiting|Healthy|Running|Exited|Error|Stopping|Stopped|Removing|Removed)\b")
    PHASE = {"Pulling": "pulling", "Pulled": "waiting", "Building": "building", "Built": "built",
             "Creating": "creating", "Created": "creating", "Recreate": "creating", "Recreated": "creating",
             "Starting": "starting", "Started": "starting", "Healthy": "healthy", "Running": "running",
             "Error": "failed"}
    PHASE_STOP = {"Stopping": "stopping", "Stopped": "stopping", "Removing": "stopping", "Removed": "removed",
                  "Error": "failed"}

    def svc(self, name):
        if name in self.state:
            return name
        if name in self.container:
            return self.container[name]
        if name.startswith("local-"):                                 # image local-<svc>, container local-<svc>-1
            for cand in (name[6:], re.sub(r"-\d+$", "", name[6:])):
                if cand in self.state:
                    return cand
        return None

    def log(self, line):
        with self.lock:
            self.lines.append(line)
            m = self.BUILD.match(line)
            if m and (s := self.svc(m.group(1) or m.group(2))) and self.state[s] in ("waiting", "pulling"):
                self.state[s] = "building"
            m = self.EVENT.match(line)
            if m and self.mode == "stop":
                s = self.svc(m.group(1)) or (m.group(1) if m.group(1) in self.state else None)
                if s and m.group(2) in self.PHASE_STOP:
                    self.state[s] = self.PHASE_STOP[m.group(2)]
                return
            if m and (s := self.svc(m.group(1))) and m.group(2) in self.PHASE:
                if self.state[s] not in READY or m.group(2) in ("Error", "Healthy"):
                    self.state[s] = self.PHASE[m.group(2)]

    # --- docker compose ps: the truth once a container exists ------------------------------
    def poll(self, watch=False):
        out = compose("ps", "-a", "--format", "json").stdout.strip()
        rows = json.loads(out) if out.startswith("[") else [json.loads(x) for x in out.splitlines() if x.strip()]
        seen = set()
        with self.lock:
            for r in rows:
                s = r.get("Service")
                if s not in self.state:
                    continue
                seen.add(s)
                st, hl, code = r.get("State"), r.get("Health"), r.get("ExitCode", 0)
                if s in self.pending:
                    self.state[s] = self.pending[s]
                elif st == "exited" and watch and s not in self.oneshot and code in (0, 137, 143):
                    self.state[s] = "stopped"           # docker compose stop (SIGTERM 143 / SIGKILL 137)
                elif st == "running":
                    self.state[s] = {"healthy": "healthy", "unhealthy": "unhealthy",
                                     "starting": "health"}.get(hl, "running")
                elif st == "restarting":
                    self.state[s] = "restarting"
                elif st == "exited":
                    self.state[s] = "done" if code == 0 else "failed"
                elif st == "created" and self.state[s] in READY:
                    self.state[s] = "creating"
                pub = sorted({p["PublishedPort"] for p in (r.get("Publishers") or []) if p.get("PublishedPort")})
                if pub:
                    self.ports[s] = pub
                self.info[s] = {"image": r.get("Image", ""), "up": r.get("RunningFor", ""),
                                "status": r.get("Status", "")}
            if watch:                                   # status: no container at all = stopped
                for s in self.names:
                    if s not in seen:
                        self.state[s] = self.pending.get(s, "stopped")

    def poll_stats(self):
        out = subprocess.run(["docker", "stats", "--no-stream", "--format", "json"],
                             capture_output=True, text=True).stdout
        stats, members, limit = {}, [], 0
        for line in out.splitlines():
            try:
                r = json.loads(line)
            except json.JSONDecodeError:
                continue
            name = r.get("Name", "")
            key = self.svc(name) or (name if name.startswith("jupyter-") else None)
            if not key:
                continue                                  # another project's container
            used, _, lim = r.get("MemUsage", "0B / 0B").partition("/")
            stats[key] = {"cpu": float(r.get("CPUPerc", "0%").rstrip("%") or 0), "mem": size(used),
                          "net": r.get("NetIO", ""), "io": r.get("BlockIO", ""), "pids": r.get("PIDs", "")}
            limit = max(limit, size(lim))
            if key.startswith("jupyter-"):
                members.append(key)
        with self.lock:
            self.stats, self.members, self.mem_limit = stats, sorted(members), limit

    def counts(self, skip_done=False):
        with self.lock:
            v = [x for x in self.state.values() if not (skip_done and x == "done")]
        return sum(x in READY for x in v), sum(x in BAD for x in v), len(v)

    def settled(self):
        with self.lock:
            return all(x in READY or x in BAD for x in self.state.values())


# --- drawing ----------------------------------------------------------------------------------
class View:
    def __init__(self, scr, stack, watch):
        self.scr, self.stack, self.watch = scr, stack, watch
        curses.curs_set(0)
        scr.nodelay(True)
        scr.keypad(True)
        curses.start_color()
        curses.use_default_colors()
        self.c = {}
        for i, (k, fg) in enumerate([("green", curses.COLOR_GREEN), ("yellow", curses.COLOR_YELLOW),
                                     ("red", curses.COLOR_RED), ("cyan", curses.COLOR_CYAN),
                                     ("blue", curses.COLOR_BLUE), ("magenta", curses.COLOR_MAGENTA)], 1):
            curses.init_pair(i, fg, -1)
            self.c[k] = curses.color_pair(i)
        self.c["dim"] = curses.A_DIM
        self.c["bold"] = curses.A_BOLD
        self.tick = 0
        self.sel = 0              # status: selected unit (↑ ↓ move between units, not containers)
        self.only = None          # status: show only this unit's logs
        self.errors = False       # status: only lines that look like errors / warnings
        self.paused = None        # status: frozen copy of the log while paused
        self.rows = []            # status: the units in the order they're drawn (for ↑ ↓)
        self.confirm = None       # status: the units x will stop, waiting for y / n

    def put(self, y, x, text, attr=0, width=None):
        h, w = self.scr.getmaxyx()
        if y < 0 or y >= h or x >= w:
            return
        width = min(width or w - x, w - x - (1 if y == h - 1 else 0))
        try:
            self.scr.addstr(y, x, text[:max(width, 0)].replace("\0", ""), attr)   # logs can hold NUL bytes
        except (curses.error, ValueError):
            pass

    ERR = re.compile(r"\b(error|exception|fatal|failed|traceback|unhealthy|panic)\b|exited \(", re.I)
    WARN = re.compile(r"\b(warn|warning)\b", re.I)

    def line_attr(self, line):
        if self.ERR.search(line):
            return self.c["red"]
        if self.WARN.search(line):
            return self.c["yellow"]
        if not self.watch and re.search(r"\b(started|healthy|built|running|pulled)\b", line.lower()):
            return self.c["green"]
        if line.startswith("#"):
            return self.c["dim"]
        return 0

    def say(self, text):
        with self.stack.lock:
            self.stack.message = (text, time.time())

    def unit_key(self, action):
        """s / x on the selected unit: start / stop it (x on Core: the whole stack, asks first)."""
        if not self.rows:
            return
        unit = self.rows[self.sel]
        if self.stack.busy:
            self.say(f"wait: still changing {' + '.join(self.stack.busy)}")
        elif unit in LOCKED and action == "start" and core_on(self.stack):
            self.say(f"{unit} is already on")
        elif unit in LOCKED:                    # every unit needs Core: stopping it = the whole stack
            with self.stack.lock:
                up = any(self.stack.state.get(m) in READY | {"health", "starting", "creating"}
                         for m in self.stack.names if m not in self.stack.oneshot)
            if up:
                self.confirm = [t for t, _ in GROUPS]
            else:
                self.say("The stack is already stopped")
        elif action == "start":                 # the stack not running: Core comes first
            plan = ([] if core_on(self.stack) else sorted(LOCKED)) + unit_plan(unit, "start")
            threading.Thread(target=run_unit, args=(self.stack, "start", plan), daemon=True).start()
        else:
            self.confirm = unit_plan(unit, "stop")

    def key(self, k):
        """status keys: ↑ ↓ pick a unit · s on / x off · f its logs only · a all logs · e errors only
        · space pause."""
        if self.confirm:
            units, self.confirm = self.confirm, None
            if k in (ord("y"), ord("Y")):
                threading.Thread(target=run_unit, args=(self.stack, "stop", units), daemon=True).start()
            else:
                self.say("not stopped")
            return
        if k == ord("s"):
            self.unit_key("start")
        elif k == ord("x"):
            self.unit_key("stop")
        elif k in (curses.KEY_UP, ord("k")):
            self.sel = max(0, self.sel - 1)
        elif k in (curses.KEY_DOWN, ord("j")):
            self.sel = min(len(self.rows) - 1, self.sel + 1)
        elif k in (ord("f"), 10, 13, curses.KEY_ENTER) and self.rows:
            self.only = self.rows[self.sel]
        elif k == ord("a"):
            self.only, self.errors = None, False
        elif k == ord("e"):
            self.errors = not self.errors
        elif k == ord(" "):
            self.paused = None if self.paused is not None else list(self.stack.lines)

    def header(self, phase, w):
        s = self.stack
        ready, bad, total = s.counts(skip_done=self.watch)
        if s.mode == "stop":
            with s.lock:
                v = list(s.state.values())
            ready, total = sum(x in ("removed", "stopped") for x in v), len(v)
        off = 0
        if self.watch:                          # units switched off aren't "not ready"
            with s.lock:
                off = sum(x == "stopped" for x in s.state.values())
            total -= off
        self.put(0, 1, "Epireum DE Lab", self.c["bold"] | self.c["magenta"])
        self.put(0, 16, f"· {phase}", self.c["bold"])
        label = f" {ready:>2}/{total} " + ("stopped" if s.mode == "stop" else "ready") + (f" · {off} off" if off else "")
        lx = w - len(label) - 1
        if self.watch:
            with s.lock:
                cpu = sum(v["cpu"] for v in s.stats.values())
                mem = sum(v["mem"] for v in s.stats.values())
                lim, members = s.mem_limit, len(s.members)
            pct = 100 * mem / lim if lim else 0
            info = (f"CPU {cpu:5.1f}%   MEM {human(mem)} / {human(lim)} ({pct:.0f}%)   "
                    f"{members} member server{'s' if members != 1 else ''}   ")
            self.put(0, max(32, lx - len(info)), info,
                     self.c["red"] if pct > 85 else self.c["yellow"] if pct > 70 else self.c["dim"])
        else:
            el = int(time.time() - s.started)
            bar_w = max(10, min(30, w - 70))
            fill = int(bar_w * ready / max(total, 1))
            right = f"⏱ {el // 60:02d}:{el % 60:02d}   "
            self.put(0, lx - bar_w - len(right), right, self.c["dim"])
            self.put(0, lx - bar_w, "█" * fill, self.c["green"])
            self.put(0, lx - bar_w + fill, "░" * (bar_w - fill), self.c["dim"])
        self.put(0, lx, label, self.c["red"] if bad else self.c["green"])
        self.put(1, 0, "─" * w, self.c["dim"])

    def services(self, px, pw, h, w, spin):
        """The right-hand list; returns the services in drawing order."""
        s = self.stack
        with s.lock:
            state, ports, stats = dict(s.state), dict(s.ports), dict(s.stats)
            members = list(s.members)
        metrics = self.watch and pw >= 50
        groups = GROUPS + [("Other", [n for n in s.names if not any(n in g for _, g in GROUPS)])]
        groups = [(t, [m for m in g if m in state and not (self.watch and state[m] == "done")])
                  for t, g in groups]                     # status: finished setup jobs aren't shown
        if self.watch and members:
            groups.append((MEMBERS, members))
            for m in members:
                state[m] = "running"
        if s.mode == "stop":
            groups.append((MEMBERS, sorted(m for m in state if m.startswith("jupyter-"))))
        groups = [(t, g) for t, g in groups if g]
        rows_free = h - 3 - (4 if self.watch else 0)          # status: room for the detail lines
        order, hidden = None, 0
        settled = READY if s.mode != "stop" else {"removed", "stopped"}
        folded = False
        if self.watch and rows_free < sum(len(g) for _, g in groups) + 2 * len(groups) - 1:
            # too short: the units, with only the selected one's containers (↑↓ opens the next)
            groups = [(t, g) for t, g in groups if t != MEMBERS]
            names = [t for t, _ in groups if any(t == u for u, _ in GROUPS)]
            pick = names[max(0, min(self.sel, len(names) - 1))] if names else None
            groups = [(t, g if t == pick else []) for t, g in groups]
            folded = "tight" if sum(len(g) for _, g in groups) + 2 * len(groups) > rows_free else True
        elif rows_free < sum(len(g) for _, g in groups) + 2 * len(groups) - 1:   # too short: busy ones first
            flat = sorted([m for _, g in groups for m in g], key=lambda m: state[m] in settled)
            if len(flat) > rows_free:
                hidden = len(flat) - rows_free + 1
                flat = flat[:rows_free - 1]
            groups = [(None, flat)]
        if metrics:
            self.put(2, w - 27, "  CPU    MEM   PORT", self.c["dim"])
        y, drawn, units = 2, [], []
        unit_names = [t for t, _ in groups if any(t == u for u, _ in GROUPS)]
        if self.watch and unit_names:
            self.sel = max(0, min(self.sel, len(unit_names) - 1))
        for title, rows in groups:
            if title:
                full = dict(GROUPS).get(title, rows)
                sel = self.watch and title in unit_names and unit_names.index(title) == self.sel
                base = curses.A_REVERSE if sel else 0
                if sel:
                    self.put(y, px, " " * (pw - 1), base)
                self.put(y, px + 1, ("▸ " if sel else "") + title.upper(),
                         base | self.c["bold"] | (0 if sel else self.c["dim"]))
                title_w = len(title) + (2 if sel else 0)
                if title in unit_names:
                    units.append(title)
                if self.watch and title in LOCKED and not core_on(s):
                    self.put(y, px + 2 + title_w, "○ off · s start", base | self.c["yellow"] | self.c["bold"])
                elif self.watch and title in LOCKED:
                    self.put(y, px + 2 + title_w, "● on · x stops all", base | self.c["yellow"] | self.c["bold"])
                elif self.watch and title in unit_names:
                    live = [state[m] for m in full if m in state and m not in s.oneshot]
                    on = sum(x in READY for x in live)
                    word, col = (("◐ changing", "yellow") if any(x in ("starting", "stopping") for x in live)
                                 else ("● on", "green") if on == len(live)
                                 else ("○ off", "dim") if on == 0 else ("◐ partly on", "yellow"))
                    self.put(y, px + 2 + title_w, word, base | self.c[col])
                if self.watch and metrics and title in unit_names:
                    mem = sum(stats.get(m, {}).get("mem", 0) for m in full)
                    if mem:
                        self.put(y, w - 20, f"{human(mem):>6}", base | self.c["dim"])
                y += 1
            for m in rows:
                if y >= 2 + rows_free:
                    break
                sym, col, txt = LOOK[state[m]]
                base = 0                                    # (status selects units, not containers)
                self.put(y, px + 1, sym or spin, self.c[col] | base | (self.c["bold"] if col != "dim" else 0))
                name = m[len("jupyter-"):] if m.startswith("jupyter-") else m
                self.put(y, px + 3, name, base | (self.c["dim"] if state[m] in ("waiting", "stopped") else 0),
                         pw - (31 if metrics else 16))
                port = f":{ports[m][0]}" if m in ports and state[m] in READY else ""
                if metrics:
                    st = stats.get(m)
                    if st and state[m] in ("running", "healthy"):
                        self.put(y, w - 27, f"{st['cpu']:5.1f}% {human(st['mem']):>6}",
                                 base | (self.c["yellow"] if st["cpu"] > 80 else 0))
                    else:
                        self.put(y, w - 27, f"{txt:<13}", self.c[col] | base)
                    self.put(y, w - 7, f"{port:>6}", self.c[col] | base)
                else:
                    tail = f"{txt}  {port}" if port else txt
                    self.put(y, w - len(tail) - 1, tail, self.c[col] | base)
                drawn.append(m)
                y += 1
            if title and folded != "tight":       # tight: no blank line between units
                y += 1
        if hidden:
            self.put(y, px + 1, f"… {hidden} more, all " + ("stopped" if s.mode == "stop" else "ready"),
                     self.c["dim"])
        if folded and y < 2 + rows_free:
            self.put(y, px + 1, "↑↓ opens a unit · taller window: all", self.c["dim"],
                     pw - 2)
        if self.watch and units:                   # details of the selected unit
            unit = units[min(self.sel, len(units) - 1)]
            svcs = [m for m in dict(GROUPS)[unit] if m in state and m not in s.oneshot]
            with s.lock:
                cpu = sum(s.stats.get(m, {}).get("cpu", 0) for m in svcs)
                mem = sum(s.stats.get(m, {}).get("mem", 0) for m in svcs)
                ports = sorted({p for m in svcs for p in s.ports.get(m, []) if state.get(m) in READY})
            y = h - 5
            self.put(y, px, "─" * pw, self.c["dim"])
            self.put(y + 1, px + 1, unit, self.c["bold"])
            tag = (("off · s starts it" if not core_on(s) else "x stops the whole stack") if unit in LOCKED
                   else "s on · x off")
            self.put(y + 1, w - len(tag) - 1, tag, self.c["yellow"] if unit in LOCKED else self.c["cyan"])
            self.put(y + 2, px + 1, " · ".join(svcs), self.c["dim"])
            self.put(y + 3, px + 1, f"CPU {cpu:.1f}% · memory {human(mem)}" +
                     (f" · ports {', '.join(map(str, ports))}" if ports else ""), self.c["dim"])
        return units if self.watch else drawn

    def logs(self, lw, rows):
        s = self.stack
        with s.lock:
            lines = self.paused if self.paused is not None else list(s.lines)
        if not self.watch:
            for i, line in enumerate(lines[-rows:]):
                self.put(2 + i, 1, line.replace("\t", "  "), self.line_attr(line), lw)
            return
        if self.only:
            mine = set(dict(GROUPS).get(self.only, [self.only]))
            lines = [x for x in lines if x[0] in mine]
        if self.errors:
            lines = [x for x in lines if self.ERR.search(x[1]) or self.WARN.search(x[1])]
        names = sorted({x[0] for x in lines})
        colour = {n: self.c[LOG_COLOURS[hash(n) % len(LOG_COLOURS)]] for n in names}
        pw = 0 if self.only else min(18, max((len(n) for n in names), default=0)) + 1
        for i, (svc, text) in enumerate(lines[-rows:]):
            if pw:
                self.put(2 + i, 1, svc[:pw - 1].ljust(pw), colour[svc])
            self.put(2 + i, 1 + pw, text.replace("\t", "  "), self.line_attr(text), lw - pw)
        if not lines:
            self.put(3, 2, "no log lines yet" + (" — e or a shows more" if self.only or self.errors else ""),
                     self.c["dim"])

    def draw(self, phase):
        scr = self.scr
        scr.erase()
        h, w = scr.getmaxyx()
        self.tick += 1
        spin = SPIN[self.tick % len(SPIN)]
        self.header(phase, w)
        pw = (58 if w >= 140 else 50 if w >= 120 else 40 if w >= 100 else 32) if self.watch else \
             (40 if w >= 100 else 32)
        px = w - pw
        for y in range(2, h - 1):
            self.put(y, px - 1, "│", self.c["dim"])
        self.rows = self.services(px, pw, h, w, spin)
        self.logs(px - 2, h - 3)
        if self.watch:
            what = (f"logs: {self.only}" if self.only else "logs: all") + (" · errors" if self.errors else "") + \
                   (" · PAUSED" if self.paused is not None else "")
            hint = f"{what}   ↑↓ pick a unit · s on · x off · f its logs · a all · e errors · space pause · q quit"
            with self.stack.lock:
                msg, busy = self.stack.message, self.stack.busy
            if self.confirm:
                n = sum(len(g) for t, g in GROUPS if t in self.confirm)
                if any(t in LOCKED for t in self.confirm):
                    self.put(h - 1, 1, f"Stop the WHOLE stack — Core and every unit ({n} services, members' "
                             "Jupyter servers too; data is kept)? y / n", self.c["red"] | self.c["bold"])
                else:
                    self.put(h - 1, 1, f"Stop {' + '.join(self.confirm)} ({n} services; data is kept)? y / n",
                             self.c["yellow"] | self.c["bold"])
                scr.refresh()
                return
            if busy:
                hint = f"{spin} changing {' + '.join(busy)} …   " + hint
            elif not core_on(self.stack) and not (msg and time.time() - msg[1] < 8):
                with self.stack.lock:
                    some = any(self.stack.state.get(m) in READY for t, g in GROUPS if t in LOCKED for m in g
                               if m not in self.stack.oneshot)
                self.put(h - 1, 1, ("Core is still starting (or partly stopped) — wait, or s to start the rest"
                                    if some else "The stack isn't running — s starts Core (sign-in, catalog, storage)")
                         + ", then s on a unit adds it · make up starts everything · q quit",
                         self.c["yellow"] | self.c["bold"])
                scr.refresh()
                return
            elif msg and time.time() - msg[1] < 8:
                self.put(h - 1, 1, msg[0], self.c["cyan"] | self.c["bold"])
                scr.refresh()
                return
        elif self.stack.mode == "stop":
            hint = "containers and network go · volumes (all your data) stay · make up starts it again"
        else:
            hint = ("q quit" if self.stack.compose_rc is not None else "Ctrl-C stop") + f"  ·  full log: {LOG_SHOWN}"
        self.put(h - 1, 1, hint, self.c["dim"])
        scr.refresh()


# --- background work -----------------------------------------------------------------------
def run_compose(stack):
    os.makedirs(os.path.dirname(LOG), exist_ok=True)
    with open(LOG, "w") as f:
        p = subprocess.Popen(UP, cwd=HERE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                             bufsize=1, env={**os.environ, "BUILDKIT_PROGRESS": "plain"})
        for line in p.stdout:
            f.write(line)
            stack.log(line.rstrip("\n"))
        stack.compose_rc = p.wait()


def run_stop(stack, dry=False):
    """Members' Jupyter servers first (JupyterHub's, not compose's — they'd keep the network busy),
    then `docker compose down` — containers and network go, volumes stay."""
    servers = sorted(m for m in stack.state if m.startswith("jupyter-"))
    for m in servers:
        stack.log(f" Container {m} Removing   (a member's Jupyter server — their files stay in its volume)")
        if not dry:
            subprocess.run(["docker", "rm", "-f", m], capture_output=True)
        stack.log(f" Container {m} Removed")
    if dry:                                 # what `down` would print, without stopping anything
        by_svc = {v: k for k, v in stack.container.items()}
        for n in reversed(stack.names):
            if stack.state.get(n) not in ("stopped", "removed"):
                c = by_svc.get(n, f"local-{n}-1")
                for ev in ("Stopping", "Stopped", "Removing", "Removed"):
                    stack.log(f" Container {c} {ev} ")
                    time.sleep(0.04)
        stack.log(" Network local_sparknet Removing ")
        stack.log(" Network local_sparknet Removed ")
        stack.compose_rc = 0
        return
    p = subprocess.Popen(["docker", "compose", "--progress", "plain", "down"], cwd=HERE, stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT, text=True, bufsize=1)
    for line in p.stdout:
        stack.log(line.rstrip("\n"))
    stack.compose_rc = p.wait()


def follow_logs(stack, stop):
    """All compose containers' logs, as (service, line). Restarts when containers come and go."""
    prefix = re.compile(r"^([\w.-]+)\s+\| ?(.*)$")
    while not stop.is_set():
        p = subprocess.Popen(["docker", "compose", "logs", "-f", "--tail", "15", "--no-color"], cwd=HERE,
                             stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1,
                             errors="replace")
        for line in p.stdout:
            if stop.is_set():
                break
            m = prefix.match(line.rstrip("\n"))
            if m:
                with stack.lock:
                    stack.lines.append((stack.svc(m.group(1)) or m.group(1), m.group(2)))
        p.kill()
        stop.wait(3)


def poll_loop(stack, stop, watch):
    n = 0
    while not stop.is_set():
        try:
            stack.poll(watch)
            if watch and n % 2 == 0:
                stack.poll_stats()                 # takes ~2 s itself
        except Exception:  # noqa: BLE001 — a failed poll just waits for the next one
            pass
        n += 1
        stop.wait(2)


def core_on(stack):
    """Is every always-on service up? (No: the stack isn't running — make status offers to start it.)"""
    with stack.lock:
        return all(stack.state.get(m) in READY for t, g in GROUPS if t in LOCKED for m in g
                   if m in stack.state and m not in stack.oneshot)


def unit_of(svc):
    return next((t for t, g in GROUPS if svc in g), None)


def unit_plan(unit, action):
    """The units a start or stop touches: start brings what the unit needs, stop what needs it."""
    if action == "start":
        todo, out = [unit], []
        while todo:
            u = todo.pop()
            if u not in out:
                out.append(u)
                todo += NEEDS.get(u, [])
        return [u for u in reversed(out) if u not in LOCKED]
    out = [unit]
    for u in out:
        out += [d for d, needs in NEEDS.items() if u in needs and d not in out]
    return out


def run_unit(stack, action, units):
    """status s / x: docker compose up -d / stop for every service of the units (data stays)."""
    svcs = [s for t, g in GROUPS if t in units for s in g if s in stack.names]
    tag = "make status"
    with stack.lock:
        stack.busy = units
        stack.pending = {s: "starting" if action == "start" else "stopping" for s in svcs}
        stack.lines.append((tag, f"{action} {' + '.join(units)}: {' '.join(svcs)}"))
    if action == "stop" and "Notebooks" in units:
        for m in list(stack.members):           # members' servers need the Hub — their files stay
            subprocess.run(["docker", "rm", "-f", m], capture_output=True)
            with stack.lock:
                stack.lines.append((tag, f"removed {m} (a member's Jupyter server — files stay in its volume)"))
    cmd = ["up", "-d"] if action == "start" else ["stop"]
    # its own session, output to a file: quitting make status (or it crashing) mustn't kill compose
    # halfway — `up -d` waits for health checks, and services after them would stay "Created"
    os.makedirs(os.path.dirname(UNIT_LOG), exist_ok=True)
    with open(UNIT_LOG, "a") as out:
        out.write(f"\n== {time.strftime('%H:%M:%S')} {action} {' + '.join(units)} ==\n")
        out.flush()
        start = out.tell()
        p = subprocess.Popen(["docker", "compose", "--progress", "plain", *cmd, *svcs], cwd=HERE,
                             stdout=out, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL, start_new_session=True)
    with open(UNIT_LOG, errors="replace") as f:
        f.seek(start)
        while True:
            done = p.poll() is not None
            for line in f.readlines():
                with stack.lock:
                    stack.lines.append((tag, line.rstrip("\n")))
            if done:
                break
            time.sleep(0.3)
    rc = p.returncode
    if rc == 0 and action == "start" and "Core" in units:     # as make up: a fresh install has no shared lake
        line = ensure_shared_lake()
        if line:
            with stack.lock:
                stack.lines.append((tag, line))
    with stack.lock:
        stack.pending, stack.busy = {}, None
        done = "started" if action == "start" else "stopped"
        extra = [USES[u] for u in units if u in USES and USES[u] not in units] if action == "start" else []
        hint = f" · Spark code (SparkSession, lab_spark) also needs the {extra[0]} unit: s on it" \
            if extra and any(stack.state.get(m) not in READY for t, g in GROUPS if t == extra[0] for m in g) else ""
        what = "The whole stack" if action == "stop" and any(u in LOCKED for u in units) else " + ".join(units)
        stack.message = (f"{what} {done}{hint}" if rc == 0 else
                         f"docker compose {cmd[-1]} failed (exit {rc}) — see the log, f to filter", time.time())


def ui(scr, stack, watch):
    view, stop = View(scr, stack, watch), threading.Event()
    if stack.mode == "stop":
        threading.Thread(target=run_stop, args=(stack, "--dry-run" in sys.argv), daemon=True).start()
    else:
        threading.Thread(target=poll_loop, args=(stack, stop, watch), daemon=True).start()
        threading.Thread(target=follow_logs if watch else run_compose,
                         args=(stack, stop) if watch else (stack,), daemon=True).start()
    done_at = None
    try:
        while True:
            if stack.mode == "stop":
                phase = "stopping the stack" if stack.compose_rc is None else \
                        "stopped" if stack.compose_rc == 0 else "docker compose down failed"
                view.draw(phase)
                if stack.compose_rc is not None:
                    time.sleep(1.2)
                    return "done" if stack.compose_rc == 0 else "failed"
                time.sleep(0.12)
                continue
            if watch:
                phase = "status"
            elif stack.compose_rc is None:
                building = any(v in ("building", "pulling") for v in stack.state.values())
                phase = "building images" if building else "starting services"
            elif stack.compose_rc != 0:
                phase = "docker compose failed"
            else:
                phase = "waiting for health checks"
            view.draw(phase)
            k = scr.getch()
            while k != -1:
                if k in (ord("q"), ord("Q")) and (watch or stack.compose_rc is not None) and not view.confirm:
                    return "quit"
                if watch:
                    view.key(k)
                k = scr.getch()
            if not watch and stack.compose_rc is not None:
                if stack.compose_rc != 0:
                    time.sleep(1.5)
                    return "failed"
                done_at = done_at or time.time()
                if stack.settled() or time.time() - done_at > SETTLE_TIMEOUT:
                    stack.poll()
                    view.draw("done")
                    time.sleep(1)
                    return "done"
            time.sleep(0.15)
    finally:
        stop.set()


SOURCES = os.path.join(HERE, "logs", "image-sources.json")   # fingerprints of what the images were built from


def image_sources():
    """{Dockerfile: fingerprint of it + every local file it COPYs/ADDs} for the stack's built images,
    and {service: Dockerfile}. Big files (the jars in tmp/) count by size + date, small ones by content."""
    import hashlib
    cfg = json.loads(compose("config", "--format", "json").stdout or "{}")
    by_svc, prints = {}, {}
    for svc, conf in cfg.get("services", {}).items():
        b = conf.get("build")
        if not b:
            continue
        ctx, name = b.get("context", HERE), b.get("dockerfile", "Dockerfile")
        df = os.path.join(ctx, name)
        by_svc[svc] = name
        if name in prints:
            continue
        files = [df]
        try:
            for line in open(df):
                m = re.match(r"\s*(?:COPY|ADD)\s+(.*)", line)
                if m and "--from" not in m.group(1):
                    parts = [x for x in m.group(1).split() if not x.startswith("--")]
                    files += [os.path.join(ctx, x) for x in parts[:-1]]
        except OSError:
            pass
        h = hashlib.sha256()
        for f in files:
            walk = os.walk(f) if os.path.isdir(f) else [(os.path.dirname(f), [], [os.path.basename(f)])]
            for root, _, names in walk:
                for n in sorted(names):
                    path = os.path.join(root, n)
                    if not os.path.isfile(path):
                        continue
                    st = os.stat(path)
                    h.update(os.path.relpath(path, ctx).encode())
                    if st.st_size > 5 * 2**20:
                        h.update(f"{st.st_size}:{int(st.st_mtime)}".encode())
                    else:
                        with open(path, "rb") as fh:
                            h.update(fh.read())
        prints[name] = h.hexdigest()
    return prints, by_svc


def stale_images():
    """Services whose image was built from different sources than what's on disk now (a changed
    Dockerfile or copied-in file, e.g. after a `git pull`). `make up` doesn't rebuild them —
    `make build` does. The first run only records what's there."""
    prints, by_svc = image_sources()
    try:
        saved = json.load(open(SOURCES))
    except (OSError, ValueError):
        record_sources(prints)
        return []
    changed = {df for df, fp in prints.items() if saved.get(df) not in (None, fp)}
    return [f"{svc} ({df})" for svc, df in by_svc.items() if df in changed]


def record_sources(prints=None):
    """After a successful build: what the images are now built from."""
    if prints is None:
        prints, _ = image_sources()
    os.makedirs(os.path.dirname(SOURCES), exist_ok=True)
    with open(SOURCES, "w") as f:
        json.dump(prints, f, indent=1)


PORT_VARS = {"postgres": "POSTGRES_HOST_PORT", "airflow-apiserver": "AIRFLOW_HOST_PORT"}   # settable in .env


def port_busy(port):
    """True if something on this machine already listens on the port."""
    import socket
    for host in ("0.0.0.0", "127.0.0.1"):
        sk = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sk.bind((host, port))
        except OSError:
            return True
        finally:
            sk.close()
    return False


def check_ports():
    """Before `make up`: every host port the stack publishes must be free — or already be ours
    (the stack is up). Returns (ok, lines)."""
    R, Y, D, B, X = "\033[31m", "\033[33m", "\033[2m", "\033[1m", "\033[0m"
    cfg = json.loads(compose("config", "--format", "json").stdout or "{}")
    project = cfg.get("name", "local")
    wanted = {}                                            # port → service
    for svc, conf in cfg.get("services", {}).items():
        for p in conf.get("ports", []):
            if str(p.get("published", "")).isdigit():
                wanted[int(p["published"])] = svc
    held = {}                                              # port → (container, project)
    out = subprocess.run(["docker", "ps", "--format", "json"], capture_output=True, text=True).stdout
    for line in out.splitlines():
        try:
            c = json.loads(line)
        except ValueError:
            continue
        proj = re.search(r"com\.docker\.compose\.project=([^,]+)", c.get("Labels", ""))
        for m in re.finditer(r":(\d+)->", c.get("Ports", "")):
            held[int(m.group(1))] = (c.get("Names", "?"), proj.group(1) if proj else "")
    clashes = []
    for port, svc in sorted(wanted.items()):
        if port in held:
            name, proj = held[port]
            if proj == project:
                continue                                   # our own container — the stack is up
            clashes.append((port, svc, f"Docker container {B}{name}{X}" + (f" (project {proj})" if proj else ""),
                            f"docker stop {name}"))
        elif port_busy(port):
            who = subprocess.run(["lsof", "-nP", f"-iTCP:{port}", "-sTCP:LISTEN", "-Fpc"],
                                 capture_output=True, text=True).stdout if shutil.which("lsof") else ""
            pid = re.search(r"^p(\d+)", who, re.M)
            cmd = re.search(r"^c(.+)", who, re.M)
            what = f"program {B}{cmd.group(1)}{X} (pid {pid.group(1)})" if cmd and pid else "another program"
            clashes.append((port, svc, what, f"kill {pid.group(1)}" if pid else f"lsof -nP -iTCP:{port}"))
    if not clashes:
        return True, [f"✓ all {len(wanted)} ports free (or already the lab's)"]
    print(f"\n{R}{B}✗ {len(clashes)} port{'s are' if len(clashes) > 1 else ' is'} already in use{X} — "
          f"nothing was started.\n")
    suggested = set()
    for port, svc, what, stop in clashes:
        print(f"  {R}:{port}{X}  needed by {B}{svc}{X} — taken by {what}")
        if svc in PORT_VARS:
            free = next(p for p in range(port + 1, port + 200)
                        if p not in wanted and p not in suggested and p not in held and not port_busy(p))
            suggested.add(free)
            print(f"         {D}fix: stop it ({stop}), or use another port — in local/.env:{X} "
                  f"{PORT_VARS[svc]}={free}")
        else:
            files = [f for f in sorted(os.listdir(HERE)) if f.endswith(".yaml")
                     and f'"{port}:' in open(os.path.join(HERE, f)).read()]
            print(f"         {D}fix: stop it ({stop})" +
                  (f", or change the left-hand port in local/{files[0]}" if files else "") + f"{X}")
    print(f"\n  {D}Then run make up again.{X}\n")
    return False, []


def check_docker():
    """Before `make up`: is Docker running, and does it have enough CPU and memory?
    Returns (ok_to_go, lines for the log/summary). LAB_SKIP_CHECKS=1 skips the questions."""
    G, Y, R, D, B, X = "\033[32m", "\033[33m", "\033[31m", "\033[2m", "\033[1m", "\033[0m"
    p = subprocess.run(["docker", "info", "--format", "{{.NCPU}} {{.MemTotal}} {{.OperatingSystem}}"],
                       capture_output=True, text=True)
    if p.returncode != 0 or not p.stdout.strip():
        print(f"\n{R}{B}✗ Docker isn't running{X} (or this user can't reach it).")
        print(f"  Start Docker Desktop (or the Docker service) and run {B}make up{X} again.\n")
        return False, []
    cpus, mem, osname = p.stdout.split(" ", 2)
    cpus, gb = int(cpus), int(mem) / 2**30
    have = f"{cpus} CPUs, {gb:.1f} GB memory"
    low = [f"CPUs: {cpus} — at least {MIN_CPUS} needed" for _ in [1] if cpus < MIN_CPUS] + \
          [f"memory: {gb:.1f} GB — at least {MIN_MEM_GB:g} GB needed" for _ in [1] if gb < MIN_MEM_GB * 0.95]
    tight = [f"{REC_CPUS} CPUs" for _ in [1] if MIN_CPUS <= cpus < REC_CPUS] + \
            [f"{REC_MEM_GB:g} GB memory" for _ in [1] if MIN_MEM_GB * 0.95 <= gb < REC_MEM_GB * 0.95]
    where = ("Docker Desktop → Settings → Resources (CPU limit, Memory limit) → Apply & restart"
             if "Docker Desktop" in osname else "give the machine / VM running Docker more CPU and memory")
    if low:
        print(f"\n{R}{B}✗ This system doesn't meet the lab's minimum requirements{X}")
        print(f"  Docker has {have}. The lab needs at least {MIN_CPUS} CPUs and {MIN_MEM_GB:g} GB memory "
              f"({REC_CPUS} CPUs and {REC_MEM_GB:g}+ GB are comfortable).")
        for x in low:
            print(f"  {R}• {x}{X}")
        print(f"  Fix: {where}.")
        print(f"  {D}With less, containers get killed (exit 137) or start very slowly.{X}")
        if os.environ.get("LAB_SKIP_CHECKS") == "1":
            print(f"  {Y}LAB_SKIP_CHECKS=1 — starting anyway.{X}\n")
        elif sys.stdin.isatty():
            try:
                ans = input(f"\n  Start anyway? [y/N] ").strip().lower()
            except EOFError:
                ans = ""
            if ans not in ("y", "yes"):
                print("  Not started.\n")
                return False, []
        else:
            print(f"  {Y}Not a terminal — starting anyway.{X}\n")
        return True, [f"✗ below minimum: Docker has {have} (needs {MIN_CPUS} CPUs, {MIN_MEM_GB:g} GB)"]
    if tight:
        return True, [f"✓ Docker has {have} — meets the minimum; {' and '.join(tight)} would be more comfortable"]
    return True, [f"✓ Docker has {have} — meets the requirements ({MIN_CPUS}+ CPUs, {MIN_MEM_GB:g}+ GB)"]


def ensure_shared_lake():
    """After a fresh start (first install, make clean, make destroy) Polaris has no shared lake
    (polaris_lake, Trino's `iceberg`) — Superset and SQL Lab then fail with "Unable to find
    warehouse polaris_lake". Seed it when it is missing; a normal restart keeps it (Postgres)."""
    import base64
    import urllib.parse
    import urllib.request
    base = os.environ.get("POLARIS_URL", "http://localhost:8185")
    cred = os.environ.get("POLARIS_ROOT_CREDENTIAL", "root:s3cr3t")
    for _ in range(12):                                  # Polaris may still be finishing its start
        try:
            req = urllib.request.Request(
                f"{base}/api/catalog/v1/oauth/tokens", method="POST",
                data=urllib.parse.urlencode({"grant_type": "client_credentials",
                                             "scope": "PRINCIPAL_ROLE:ALL"}).encode(),
                headers={"Authorization": "Basic " + base64.b64encode(cred.encode()).decode(),
                         "Polaris-Realm": "POLARIS"})
            with urllib.request.urlopen(req, timeout=10) as r:
                token = json.load(r)["access_token"]
            req = urllib.request.Request(f"{base}/api/management/v1/catalogs",
                                         headers={"Authorization": f"Bearer {token}", "Polaris-Realm": "POLARIS"})
            with urllib.request.urlopen(req, timeout=10) as r:
                names = [c["name"] for c in json.load(r)["catalogs"]]
            break
        except Exception:  # noqa: BLE001
            time.sleep(5)
    else:
        return "⚠ couldn't reach Polaris to check the shared lake — if Superset says 'Unable to find " \
               "warehouse polaris_lake', run make polaris-seed"
    if "polaris_lake" in names:
        return None
    out = subprocess.run(["bash", os.path.join(HERE, "..", "common", "polaris", "seed-polaris.sh")],
                         capture_output=True, text=True, env={**os.environ, "POLARIS_URL": base})
    os.makedirs(os.path.dirname(LOG), exist_ok=True)
    with open(LOG, "a") as f:
        f.write("\n== make polaris-seed (shared lake was missing) ==\n" + out.stdout + out.stderr)
    if out.returncode:
        return "⚠ the shared lake (polaris_lake) is missing and seeding it failed — run make polaris-seed"
    return "✓ shared lake (polaris_lake) was missing — created it (make polaris-seed)"


def newer_release():
    """"⚠ newer release …" when GitHub has a vX.Y.Z tag above this checkout's, else None. Fetches the
    tags (10 s at most; offline → None). LAB_UPDATE_CHECK=0 in .env turns it off."""
    try:
        with open(os.path.join(HERE, ".env")) as f:
            if re.search(r"^LAB_UPDATE_CHECK=0\s*$", f.read(), re.M):
                return None
    except OSError:
        pass
    git = ["git", "-C", HERE]
    try:
        subprocess.run(git + ["fetch", "--tags", "--quiet", "origin"], capture_output=True, timeout=10)
        tags = subprocess.run(git + ["tag", "-l", "v[0-9]*"], capture_output=True, text=True, timeout=5).stdout.split()
        mine = subprocess.run(git + ["describe", "--tags", "--abbrev=0", "--match", "v[0-9]*"],
                              capture_output=True, text=True, timeout=5).stdout.strip()
    except (OSError, subprocess.TimeoutExpired):
        return None

    def ver(t):
        m = re.fullmatch(r"v(\d+)\.(\d+)(?:\.(\d+))?", t)
        return m and (int(m[1]), int(m[2]), int(m[3] or 0))
    tags = [t for t in tags if ver(t)]
    if not tags:
        return None
    latest = max(tags, key=ver)
    if mine and ver(mine) and ver(mine) >= ver(latest):
        return None
    return f"⚠ newer release {latest} (you're on {mine or 'an older version'}) — git pull, then make up"


def summary(stack, how):
    G, Y, R, D, B, X = "\033[32m", "\033[33m", "\033[31m", "\033[2m", "\033[1m", "\033[0m"
    stack.poll()
    ready, bad, total = stack.counts()
    el = int(time.time() - stack.started)
    jobs = sum(v == "done" for v in stack.state.values())
    failed = [n for n, v in stack.state.items() if v in BAD]
    pending = [n for n, v in stack.state.items() if v not in READY and v not in BAD]
    if how == "failed":
        print(f"\n{R}{B}✗ docker compose failed{X} (exit {stack.compose_rc}) after {el // 60}m{el % 60:02d}s — last lines:\n")
        for line in list(stack.lines)[-25:]:
            print("  " + line)
    elif failed:
        print(f"\n{Y}{B}⚠ The stack is up, but {len(failed)} service(s) are not well:{X} "
              f"{', '.join(f'{n} ({stack.state[n]})' for n in failed)}")
        for n in failed:
            print(f"\n{B}{n}{X}  {D}(make logs S={n}){X}")
            for line in compose("logs", "--tail", "12", "--no-log-prefix", n).stdout.splitlines():
                print("  " + line)
    else:
        print(f"\n{G}{B}✓ The lab is up{X} in {el // 60}m{el % 60:02d}s — "
              f"{ready - jobs} services running, {jobs} setup jobs done"
              + (f" {D}(still starting: {', '.join(pending)}){X}" if pending else ""))
    for line in getattr(stack, "checks", []):
        print(f"  {G if line.startswith('✓') else Y}{line}{X}")
    if not BUILD and not any(x.startswith("⚠") for x in getattr(stack, "checks", [])):
        print(f"  {D}Changed a Dockerfile (or pulled changes)? make build rebuilds the images.{X}")
    if how != "failed":
        print()
        for label, svc in LINKS:
            if svc in stack.ports:
                print(f"  {label:<13} {B}http://localhost:{stack.ports[svc][0]}/{X}")
        print(f"\n  {D}Sign in as demouser / demouser · live status: make status · bring down: make down{X}")
    print(f"  {D}Full log: {LOG_SHOWN}{X}\n")
    return 1 if how == "failed" or failed else 0


def summary_stop(stack, how):
    G, R, D, B, X = "\033[32m", "\033[31m", "\033[2m", "\033[1m", "\033[0m"
    el = time.time() - stack.started
    if how != "done":
        print(f"\n{R}{B}✗ docker compose down failed{X} (exit {stack.compose_rc}) — last lines:\n")
        for line in list(stack.lines)[-20:]:
            print("  " + line)
        return 1
    gone = sum(v == "removed" for v in stack.state.values())
    servers = sum(1 for m, v in stack.state.items() if m.startswith("jupyter-") and v == "removed")
    dry = " (dry run — nothing was stopped)" if "--dry-run" in sys.argv else ""
    print(f"\n{G}{B}✓ The lab is stopped{X} in {el:.0f}s — {gone} containers removed"
          + (f", including {servers} member Jupyter server{'s' if servers != 1 else ''}" if servers else "") + dry)
    print(f"  {D}All data is kept in Docker volumes — members, lakehouses, tables, files, DAGs.{X}")
    print(f"  {D}Start again: make up   ·   delete all data: make clean   ·   remove everything: make destroy{X}\n")
    return 0


def stop_plain():
    ids = subprocess.run(["docker", "ps", "-aq", "--filter", "name=^jupyter-"], capture_output=True,
                         text=True).stdout.split()
    if ids:
        print("Removing members' Jupyter servers...")
        subprocess.run(["docker", "rm", "-f", *ids], capture_output=True)
    return subprocess.call(["docker", "compose", "down"], cwd=HERE)


def main():
    watch, stopping = "--watch" in sys.argv, "--stop" in sys.argv
    checks = []
    if not watch and not stopping:
        ok, checks = check_docker()
        if not ok:
            sys.exit(1)
        ok, port_lines = check_ports()
        if not ok:
            sys.exit(1)
        checks += port_lines
        if not BUILD:
            try:
                old = stale_images()
            except Exception:  # noqa: BLE001 — the hint is optional
                old = []
            if old:
                checks.append(f"⚠ changed since built: {', '.join(old)} — run make build to rebuild")
    if not sys.stdout.isatty() or os.environ.get("TERM") in (None, "", "dumb"):
        if stopping:
            sys.exit(stop_plain() if "--dry-run" not in sys.argv else 0)
        if watch:
            sys.exit(subprocess.call(["docker", "compose", "ps"], cwd=HERE))
        rc = subprocess.call(["docker", "compose", "up", "-d"] + (["--build"] if BUILD else []), cwd=HERE)
        if rc == 0:
            line = ensure_shared_lake()
            if line:
                print(line)
        sys.exit(rc)
    locale.setlocale(locale.LC_ALL, "")
    stack = Stack("stop" if stopping else "watch" if watch else "up")
    stack.checks = checks
    for line in checks:
        stack.lines.append(line)
    if watch:
        def release_hint():                 # in the background: git fetch can take a few seconds
            line = newer_release()
            if line:
                with stack.lock:
                    stack.lines.append(("make status", line))
        threading.Thread(target=release_hint, daemon=True).start()
        try:
            stack.poll(watch=True)          # the first frame shows what runs (not all "waiting")
        except Exception:  # noqa: BLE001 — Docker not answering: the loop keeps trying
            pass
    if stopping:
        stack.poll(watch=True)              # what runs now (no container = already stopped)
        for name in subprocess.run(["docker", "ps", "-a", "--format", "{{.Names}}", "--filter", "name=^jupyter-"],
                                   capture_output=True, text=True).stdout.split():
            stack.state[name] = "running"
    try:
        how = curses.wrapper(ui, stack, watch)
    except KeyboardInterrupt:
        how = "quit"
        if not watch:
            print("\nInterrupted (Ctrl-C) — `make status` shows what is running now.")
            sys.exit(130)
    if watch:
        if stack.busy:
            print(f"Still {' + '.join(stack.busy)}: it carries on in the background (log: local/logs/make-status.log)"
                  " — make status shows it.")
        return
    if stopping:
        sys.exit(summary_stop(stack, how))
    if not watch:
        built = any(" Built" in x for x in stack.lines if isinstance(x, str))
        if how != "failed" and (BUILD or built):
            try:
                record_sources()
            except Exception:  # noqa: BLE001
                pass
        if how != "failed":
            print("Checking the shared lake in Polaris...", flush=True)
            line = ensure_shared_lake()
            if line:
                stack.checks.append(line)
            line = newer_release()
            if line:
                stack.checks.append(line)
        sys.exit(summary(stack, how))


if __name__ == "__main__":
    main()
