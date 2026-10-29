#!/usr/bin/env python3
"""make release — tag the next lab release (vX.Y.Z) and push the tag. For the lab's maintainers.

  make release               next patch: v1.0.0 → v1.0.1 (fixes)
  make release BUMP=minor    next minor: v1.0.1 → v1.1.0 (new features)
  make release BUMP=major    next major: v1.1.0 → v2.0.0 (needs a reset, e.g. make clean)

Refuses with uncommitted changes, or when main isn't the same as GitHub's (the tag must point to a
commit everyone can pull). Shows the commits since the last release and asks before tagging. Managers
on an older release then see the "newer release" banner (home page) and hint (make up / make status).
"""
import re
import subprocess
import sys

BUMPS = ("patch", "minor", "major")


def git(*args, check=True):
    out = subprocess.run(["git", *args], capture_output=True, text=True)
    if check and out.returncode:
        sys.exit(f"✗ git {' '.join(args)}: {out.stderr.strip()}")
    return out.stdout.strip()


def ver(tag):
    m = re.fullmatch(r"v(\d+)\.(\d+)(?:\.(\d+))?", tag)
    return m and (int(m[1]), int(m[2]), int(m[3] or 0))


def main():
    bump = (sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] else "patch").lower()
    if bump not in BUMPS:
        sys.exit(f"✗ BUMP={bump}: use one of {', '.join(BUMPS)}")
    if git("rev-parse", "--abbrev-ref", "HEAD") != "main":
        sys.exit("✗ releases are tagged on main — git switch main first")
    if git("status", "--porcelain", "--untracked-files=no"):     # untracked files aren't in the tag
        sys.exit("✗ uncommitted changes — commit (and push) them first")
    print("Fetching from GitHub...", flush=True)
    git("fetch", "--tags", "--quiet", "origin")
    head, remote = git("rev-parse", "HEAD"), git("rev-parse", "origin/main")
    if head != remote:
        ahead = git("rev-list", "--count", "origin/main..HEAD")
        behind = git("rev-list", "--count", "HEAD..origin/main")
        sys.exit(f"✗ main differs from GitHub ({ahead} ahead, {behind} behind) — git pull / git push first")

    tags = [t for t in git("tag", "-l", "v[0-9]*").split() if ver(t)]
    last = max(tags, key=ver) if tags else None
    major, minor, patch = ver(last) if last else (1, 0, -1)
    new = {"patch": f"v{major}.{minor}.{patch + 1}", "minor": f"v{major}.{minor + 1}.0",
           "major": f"v{major + 1}.0.0"}[bump] if last else "v1.0.0"

    if last:
        log = git("log", "--oneline", "--no-decorate", f"{last}..HEAD")
        if not log:
            sys.exit(f"✗ nothing new since {last}")
        lines = log.splitlines()
        print(f"\n{len(lines)} commit(s) since {last}:")
        for line in lines[:30]:
            print("  " + (line if len(line) <= 110 else line[:107] + "..."))
        if len(lines) > 30:
            print(f"  … and {len(lines) - 30} more")
    print(f"\nRelease: {last or '(none yet)'} → \033[1m{new}\033[0m ({bump})")
    try:
        if input(f"Tag {new} on {head[:7]} and push it to GitHub? [y/N] ").strip().lower() != "y":
            sys.exit("Nothing done.")
    except (EOFError, KeyboardInterrupt):
        sys.exit("\nNothing done.")
    git("tag", "-a", new, "-m", f"{new} — lab release")
    out = subprocess.run(["git", "push", "--quiet", "origin", new], capture_output=True, text=True)
    if out.returncode:
        git("tag", "-d", new)
        sys.exit(f"✗ push failed, local tag removed: {out.stderr.strip()}")
    print(f"\033[32m✓ {new} released.\033[0m Managers on an older release see the banner within the hour.")
    print("  k8s: roll it out with deploy.yml (it sets labVersion), or add "
          f"--set-string labVersion={new} to a manual helm template.")


if __name__ == "__main__":
    main()
