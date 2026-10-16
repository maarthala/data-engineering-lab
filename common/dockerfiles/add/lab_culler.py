"""The Hub's idle-culler (a Hub service), with the timeout managers set on ⚙️ Settings.

Reads idle_minutes (lakehouse.lab_settings: lab-admin/settings.json, else HUB_IDLE_TIMEOUT) every
minute and runs jupyterhub_idle_culler with it — restarted when the value changes, so a new
setting applies within a minute, no Hub restart. Extra arguments go to the culler (--url=…).
"""
import subprocess
import sys
import time

import lakehouse

proc, minutes = None, None
while True:
    try:
        want = int(lakehouse.lab_settings()["idle_minutes"])
    except Exception as e:  # noqa: BLE001 — storage away: keep what runs (or the default)
        print(f"[lab_culler] settings: {e}", flush=True)
        want = minutes or lakehouse.IDLE_MINUTES
    if want != minutes or proc is None or proc.poll() is not None:
        if proc and proc.poll() is None:
            proc.terminate()
            proc.wait()
        minutes, timeout = want, want * 60
        print(f"[lab_culler] idle servers stop after {minutes} min", flush=True)
        proc = subprocess.Popen([sys.executable, "-m", "jupyterhub_idle_culler", f"--timeout={timeout}",
                                 f"--cull-every={min(600, max(60, timeout // 5))}", *sys.argv[1:]])
    time.sleep(60)
