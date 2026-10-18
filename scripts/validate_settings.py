#!/usr/bin/env python3
"""Pre-restart dry-run validator for hermes-jetstream settings (hjs-y4t).

Loads the settings JSON block (the object that goes under
plugins.entries.hermes-jetstream.settings) through the plugin's own
Settings.from_plugin_ctx — no second parser, no nats-py, no network — and
names every problem instead of only the first. Exit 0 = the loader accepts
the block; exit 1 prints one ERROR: line per problem naming its key/path.
Route item shape: docs/wake-routes.schema.json.

    python3 scripts/validate_settings.py settings.json
"""
from __future__ import annotations

import json
import sys
from dataclasses import fields
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "plugin"))

from hermes_jetstream.config import Settings, WakeRoute  # noqa: E402
from hermes_jetstream.subject import SubjectPolicy  # noqa: E402

# parser truth, read off the model rather than duplicated (mirrors docs/wake-routes.schema.json)
_WAKE_TYPES = {"name": str, "subject": str, "durable": str, "prompt": str, "job": str,
               "profile": str, "max_payload_chars": int, "rate_per_min": int,
               "min_interval_s": (int, float)}
_WAKE_KEYS = {f.name for f in fields(WakeRoute)}


def validate_settings_dict(settings: dict) -> list:
    """Every problem in a raw settings block, as 'wake[i] ...' paths. Empty list = good."""
    errors = []
    if not isinstance(settings, dict):
        return ["settings block must be a JSON object"]
    wake = settings.get("wake") or []
    if not isinstance(wake, list):
        return ["wake must be a list of route objects"]
    for i, r in enumerate(wake):
        if not isinstance(r, dict):
            errors.append(f"wake[{i}] must be an object, got {type(r).__name__}")
            continue
        for key in sorted(set(r) - _WAKE_KEYS):
            errors.append(f"wake[{i}] unknown key {key!r} (parser ignores it; allowed: {sorted(_WAKE_KEYS)})")
        for key, typ in _WAKE_TYPES.items():
            if key in r and r[key] is not None and not isinstance(r[key], typ):
                errors.append(f"wake[{i}] {key} must be {'|'.join(t.__name__ for t in typ) if isinstance(typ, tuple) else typ.__name__}, got {type(r[key]).__name__}")
        try:
            WakeRoute.from_dict(r)  # the plugin's own per-route validation
        except ValueError as exc:
            errors.append(f"wake[{i}] {exc}")
    # Semantic checks: the silent-runtime-failure shapes the parser cannot see.
    routes = [(i, str(r["name"]).strip(), str(r.get("subject", "")).strip())
              for i, r in enumerate(wake) if isinstance(r, dict) and str(r.get("name", "")).strip()]
    seen = set()
    for i, name, subject in routes:
        if name in seen:
            errors.append(f"wake[{i}] duplicate route name {name!r} (names key rate/coalesce buckets and the jetstream:<route> allowlist)")
        seen.add(name)
    subs = SubjectPolicy(settings.get("subscribe_allowed") or [])
    for i, name, subject in routes:
        if subject and not subs.allows(subject):
            errors.append(f"wake[{i}] {name!r} subject {subject!r} matches no subscribe_allowed pattern — wakes would be acked and dropped at runtime")
    hb_subject = str(settings.get("heartbeat_subject") or "").strip()
    hb_s = settings.get("heartbeat_s")
    if isinstance(hb_s, (int, float)) and hb_s > 0 and not (hb_subject and
            SubjectPolicy(settings.get("publish_allowed") or []).allows(hb_subject)):
        errors.append(f"heartbeat_subject {hb_subject!r} missing or not publish-allowlisted — heartbeat would be disabled with a warning at startup")
    return errors


class _Ctx:
    """Minimal get_config surface: exactly what Settings.from_plugin_ctx calls."""

    def __init__(self, settings):
        self._s = settings
        self.key = None  # last requested key: names the key in loader refusals

    def get_config(self, key, default=None):
        self.key = key
        return self._s.get(key, default)


def main(argv):
    if len(argv) != 2:
        print("usage: validate_settings.py <settings.json>", file=sys.stderr)
        return 2
    path = Path(argv[1])
    try:
        settings = json.loads(path.read_text())
    except (OSError, ValueError) as exc:
        print(f"ERROR: {path}: {exc}", file=sys.stderr)
        return 2
    errors = validate_settings_dict(settings if isinstance(settings, dict) else {})
    if not errors:  # ground truth: the real loader, end-to-end (it reports one error per raise; per-route checks already named every wake problem above)
        ctx = _Ctx(settings if isinstance(settings, dict) else {})
        try:
            Settings.from_plugin_ctx(ctx)
        except Exception as exc:
            errors.append(f"loader refused {ctx.key}: {type(exc).__name__}: {exc}")
    for e in errors:
        print(f"ERROR: {path}: {e}")
    if errors:
        return 1
    n = len(settings.get("wake") or []) if isinstance(settings, dict) else 0
    print(f"OK: {path}: settings load cleanly ({n} wake route(s))")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
