#!/usr/bin/env python3
"""One-way public mirror: forgejo SSOT -> github.com/atbrace/hermes-jetstream.

Every publish is a fresh orphan commit (no internal history is ever projected)
built from a deterministic whitelist + scrub of origin/main, then gated:
  1. estate grep-gate over the exact tree being published
  2. gitleaks over the fresh bundle history
  3. the hermetic test suite run INSIDE the scrubbed tree (deletions must not
     break what the public repo ships)
Then (with --push) force-pushes main + tag to the public remote. Force-push is
the design: the public repo is a release projection, not a development fork —
issues/PRs/commits happen on forgejo, never here.

Usage:
  scripts/publish-mirror.py                  # build bundle + all gates, no push
  scripts/publish-mirror.py --push --tag v0.11.0   # plus publish (needs GITHUB PAT)
  scripts/publish-mirror.py --remote <url>   # public remote for --push
"""
import argparse
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BUNDLE = ROOT / ".mirror-bundle"

# Everything public, from the current repo. Deleted entries are estate-coupled
# or agent scaffolding; scrubbed entries rewrite specific lines.
EXCLUDE_DIRS = {".beads", ".agents", ".claude", ".codex", "research", ".git"}
EXCLUDE_FILES = {
    "AGENTS.md", "CLAUDE.md", "IDEA.md", ".beads", "plugin/link_self",
    "tests/x6b_crash_drill.py",   # one-shot kill-mid-run drive: systemd unit + estate paths
    "tests/bu4_drive.py",         # one-shot watch drive: state.db + uv cache hash
    "tests/ncz_drive.py",         # one-shot tool drive: state.db + gateway log path
}
SCRUB = {
    "docs/BATTLE-TEST.md": [
        (r"nats\.service\.consul:4222", "<your-bus>:4222"),
        (r"`nomad job stop nats` mid-stream", "the bus bounced mid-stream"),
        (r"after restore \(`nomad job run specs/nats\.nomad\.hcl`, ~4\.5 min window\)",
         "after restore (~4.5 min window)"),
        (r"prwatch-cadence poller", "a poller"),
        (r" \(`scripts/parity-join\.py` in sysadmin repo\)", ""),
        (r"Findings filed as beads, linked by their discoveries\.",
         "Findings filed as issues against the repo, linked by their discoveries."),
    ],
    "scripts/ci.sh": [
        (r"this repo's own path contains gate tokens \(sysadmin/\.\.\.\)",
         "the checkout path itself can contain gate tokens"),
    ],
    ".gitignore": [
        (r"# Beads / Dolt files \(added by bd init\)\n\.dolt/\n\*\.db\n\.beads-credential-key\n"
         r"\.beads/proxieddb/\n", "# local dbs\n*.db\n"),
    ],
    ".github/workflows/ci.yml": [
        (r"runs-on: pool-runner", "runs-on: ubuntu-latest"),
        (r"# Community-repo gate\. Forgejo Actions: runs-on targets the estate CI lane \(`pool-runner`\)\.\n"
         r"# This instance registers no ubuntu-latest runner — jobs asking for it queue forever —\n"
         r"# so a fork moving to GitHub-hosted flips every runs-on back to ubuntu-latest, one line each\.\n"
         r"# The live job boots its own nats-server binary \(no docker service container\), then runs\n"
         r"# smoke \+ auth matrix \+ cluster drill as one sequence on it\.\n"
         r"# Forgejo instances without runners: run scripts/ci\.sh locally — it's the same hermetic gate\.\n",
         "# Community-repo gate: core = hermetic scripts/ci.sh; live = scratch nats-server +\n"
         "# smoke, auth matrix, cluster drill. No runner? scripts/ci.sh is the same gate.\n"),
    ],
    "tests/reconnect_drill.py": [
        (r'default="nats://nats\.service\.consul:4222"', 'default="nats://localhost:4222"'),
        (r'default="forgejo"', 'default="hermes"'),
        (r'sys\.path\.insert\(0, "/home/claude/[^"]+"\).*\n', ""),
        (r"nomad job stop nats ; <restore via your deploy path>",
         "<bounce your nats-server ; restore via your deploy path>"),
    ],
}
# Gate patterns: real USES of estate surface. The repo's own grep-gate token list
# (docs/V1-READINESS.md, scripts/ci.sh) enumerates these words as banned tokens —
# that is the mechanism, not a use, so patterns match usage shapes only.
BAN_PATTERNS = [
    r"service\.consul", r"initzero\.co", r"forgejo\.service",
    r"192\.168\.\d", r"/home/claude", r"sysadmin[/@.]", r"sysadmin repo",
    r"pool-runner", r"beads-dolt", r"nomad job", r"1[0-9]{18}",  # discord snowflakes
]


def run(cmd, cwd=None, **kw):
    r = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, **kw)
    if r.returncode != 0:
        sys.exit(f"FAIL: {' '.join(map(str, cmd))}\n{r.stdout[-2000:]}\n{r.stderr[-2000:]}")
    return r.stdout


def build_tree():
    ref = run(["git", "-C", str(ROOT), "rev-parse", "origin/main"]).strip()
    run(["git", "-C", str(ROOT), "fetch", "-q", "origin"])
    ref = run(["git", "-C", str(ROOT), "rev-parse", "origin/main"]).strip()
    if BUNDLE.exists():
        shutil.rmtree(BUNDLE)
    BUNDLE.mkdir()
    for rel in run(["git", "-C", str(ROOT), "ls-tree", "-r", "--name-only", ref]).splitlines():
        top = rel.split("/")[0]
        if top in EXCLUDE_DIRS or rel in EXCLUDE_FILES:
            continue
        dest = BUNDLE / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(subprocess.run(
            ["git", "-C", str(ROOT), "show", f"{ref}:{rel}"],
            capture_output=True).stdout)
    for f, rules in SCRUB.items():
        p = BUNDLE / f
        if not p.exists():
            continue
        text = p.read_text()
        for pat, repl in rules:
            text = re.sub(pat, repl, text)
        p.write_text(text)
    return ref


def gate_estate():
    hits = []
    for path in BUNDLE.rglob("*"):
        if not path.is_file() or "__pycache__" in str(path):
            continue
        try:
            text = path.read_text(errors="ignore")
        except OSError:
            continue
        for pat in BAN_PATTERNS:
            for m in re.finditer(pat, text):
                line = text[:m.start()].count("\n") + 1
                hits.append(f"{path.relative_to(BUNDLE)}:{line}: {pat}")
    if hits:
        sys.exit("FAIL estate gate:\n" + "\n".join(hits[:40]))
    print("gate 1 estate: clean")


def gate_commit_and_gitleaks():
    env = {**os.environ,
           "GIT_AUTHOR_NAME": "atbrace-hermes", "GIT_AUTHOR_EMAIL": "bot@atbrace.dev",
           "GIT_COMMITTER_NAME": "atbrace-hermes", "GIT_COMMITTER_EMAIL": "bot@atbrace.dev"}
    run(["git", "init", "-q", "-b", "main"], cwd=BUNDLE, env=env)
    run(["git", "add", "-A"], cwd=BUNDLE, env=env)
    run(["git", "-c", "core.hooksPath=/dev/null", "commit", "-q", "-m",
         "hermes-jetstream: public release projection (scrubbed from forgejo main)"],
        cwd=BUNDLE, env=env)
    gitleaks = shutil.which("gitleaks") or Path.home() / ".local/bin/gitleaks"
    if not Path(gitleaks).exists():
        sys.exit("FAIL: gitleaks not found — publishing without the secret gate is not allowed")
    proc = subprocess.run([str(gitleaks), "detect", "--no-banner"], cwd=BUNDLE,
                          capture_output=True, text=True)
    if proc.returncode != 0 or "no leaks found" not in (proc.stdout + proc.stderr):
        sys.exit("FAIL gitleaks:\n" + (proc.stdout + proc.stderr)[-3000:])
    print("gate 2 gitleaks: no leaks (fresh orphan history)")


def gate_suite():
    env = {k: v for k, v in os.environ.items() if k not in ("NATS_BIN", "HJS_SMOKE_SERVERS")}
    r = subprocess.run(["bash", "./scripts/ci.sh"], cwd=BUNDLE, capture_output=True, text=True, env=env)
    if "RESULT\nPASS" not in r.stdout and not r.stdout.rstrip().endswith("PASS"):
        sys.exit("FAIL suite in scrubbed tree:\n" + r.stdout[-3000:] + r.stderr[-2000:])
    print("gate 3 suite: PASS inside the scrubbed tree")


def push(tag, remote):
    if not remote:
        sys.exit("FAIL --push needs --remote (https://x-access-token:$PAT@github.com/atbrace/hermes-jetstream.git)")
    run(["git", "push", "-f", remote, "main"], cwd=BUNDLE)
    if tag:
        run(["git", "tag", "-f", "-a", tag, "-m", f"public projection {tag}"], cwd=BUNDLE)
        run(["git", "push", "-f", remote, tag], cwd=BUNDLE)
    print(f"pushed orphan main" + (f" + {tag}" if tag else "") + " to the public remote")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--push", action="store_true")
    ap.add_argument("--tag", default="")
    ap.add_argument("--remote", default="")
    a = ap.parse_args()
    ref = build_tree()
    nfiles = sum(1 for _ in BUNDLE.rglob("*") if _.is_file())
    print(f"bundle built from origin/main {ref[:9]} ({nfiles} files)")
    gate_estate()
    gate_commit_and_gitleaks()
    gate_suite()
    if a.push:
        push(a.tag, a.remote)
    else:
        print(f"gates green; bundle at {BUNDLE} — rerun with --push --tag vX.Y.Z --remote <url> to publish")


if __name__ == "__main__":
    main()
