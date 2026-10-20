#!/usr/bin/env python3
"""Read-only wake-route authz digest (hjs-oes).

Default-deny gateway authz drops every wake route whose `jetstream:<name>`
identity is not in the profile's allowlist, and the drop is a gateway WARNING
line only — rediscovery costs log archaeology. This digest answers, from
on-disk state alone (no gateway import, no network, nothing written):

  - the effective allowlist identities (JETSTREAM_ALLOWED_USERS +
    GATEWAY_ALLOWED_USERS from the profile .env, boot-read semantics),
  - every route the plugin knows about (config routes via --settings JSON,
    runtime routes from wake-routes.json, and every route name present in
    the wake ledger = ever-delivered),
  - a per-route AUTHORIZED | DROPPED verdict.

Exit 0 = every known route is authorized. Exit 1 = at least one would be
silently dropped. A route that has ledger rows but reads DROPPED fired under
an earlier allowlist and is now dead — fix or unregister it.

    python3 scripts/wake_authz_digest.py --home ~/.hermes/profiles/homelab
    python3 scripts/wake_authz_digest.py --settings settings.json

The pairing store is NOT consulted (approvals are not how wake routes get
authorized; env is the supported rail).
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import sys
from pathlib import Path

_ENV_LINE = re.compile(r"^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*?)\s*$")


def _dotenv_values(env_path: Path) -> dict:
    """Raw .env reads (no shell expansion — values here are plain lists)."""
    vals = {}
    if not env_path.exists():
        return vals
    for line in env_path.read_text(encoding="utf-8", errors="replace").splitlines():
        m = _ENV_LINE.match(line)
        if m:
            vals[m.group(1)] = m.group(2).strip().strip('"').strip("'")
    return vals


def _split_users(raw: str) -> set:
    return {u.strip() for u in raw.split(",") if u.strip()}


def digest(home: Path, settings_path: Path | None = None) -> tuple:
    """(rows, allowlists). Rows: (route, identity, AUTHORIZED|DROPPED, source)."""
    vals = _dotenv_values(Path(home) / ".env")
    jet = _split_users(vals.get("JETSTREAM_ALLOWED_USERS", ""))
    glob = _split_users(vals.get("GATEWAY_ALLOWED_USERS", ""))
    allow = jet | glob
    allow_all = "*" in allow

    routes = {}  # name -> set of source labels

    def note(name, source):
        if name:
            routes.setdefault(name, set()).add(source)

    rt_dir = Path(home) / "plugin-data" / "hermes-jetstream"
    rows_file = rt_dir / "wake-routes.json"
    if rows_file.exists():
        try:
            for row in json.loads(rows_file.read_text()).values():
                note(row.get("name"), "runtime")
        except Exception as exc:
            print(f"WARN: cannot parse {rows_file}: {exc}", file=sys.stderr)
    ledger = rt_dir / "wake-claims.db"
    if ledger.exists():
        try:
            db = sqlite3.connect(f"file:{ledger}?mode=ro", uri=True)
            for (name,) in db.execute("SELECT DISTINCT route FROM wake_claims"):
                note(name, "ledger")
            db.close()
        except Exception as exc:
            print(f"WARN: cannot read {ledger}: {exc}", file=sys.stderr)
    if settings_path is not None:
        try:
            settings = json.loads(Path(settings_path).read_text())
            for r in (settings.get("wake") or []):
                note(str(r.get("name", "")).strip(), "config")
        except Exception as exc:
            print(f"WARN: cannot parse {settings_path}: {exc}", file=sys.stderr)

    out = []
    for name in sorted(routes):
        identity = f"jetstream:{name}"
        ok = allow_all or identity in allow or name in allow
        out.append((name, identity, "AUTHORIZED" if ok else "DROPPED",
                    "+".join(sorted(routes[name]))))
    return out, {"JETSTREAM_ALLOWED_USERS": sorted(jet),
                 "GATEWAY_ALLOWED_USERS": sorted(glob)}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--home", default=os.environ.get("HERMES_HOME")
                    or os.path.expanduser("~/.hermes"),
                    help="profile home holding .env and plugin-data (default $HERMES_HOME or ~/.hermes)")
    ap.add_argument("--settings", default=None,
                    help="optional JSON of the plugin settings block (adds config-declared routes)")
    a = ap.parse_args(argv)
    rows, allow = digest(Path(a.home).expanduser(),
                         Path(a.settings) if a.settings else None)
    print(f"allowlist: JETSTREAM_ALLOWED_USERS={allow['JETSTREAM_ALLOWED_USERS'] or '(unset)'} "
          f"GATEWAY_ALLOWED_USERS={allow['GATEWAY_ALLOWED_USERS'] or '(unset)'}")
    if not rows:
        print("no wake routes known to on-disk state (pass --settings to include config routes)")
        return 0
    width = max(len(r[0]) for r in rows)
    dropped = 0
    for name, identity, verdict, source in rows:
        dropped += verdict == "DROPPED"
        print(f"  {name:<{width}}  {identity:<40}  {verdict:<10} source={source}")
    print(f"verdict: {len(rows) - dropped} authorized, {dropped} would be dropped")
    return 1 if dropped else 0


if __name__ == "__main__":
    sys.exit(main())
