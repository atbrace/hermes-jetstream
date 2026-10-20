"""hjs-y4t: pre-restart settings validator (scripts/validate_settings.py).

Behavior contract: a bad wake-route settings block exits nonzero naming the
offending path; a good one exits zero. Stdlib only, no NATS, no nats-py.
"""
from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
SCRIPT = REPO / "scripts" / "validate_settings.py"
sys.path.insert(0, str(REPO / "plugin"))


def _run(settings):
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
        json.dump(settings, f)
        path = f.name
    p = subprocess.run([sys.executable, str(SCRIPT), path],
                       capture_output=True, text=True, timeout=30)
    return p.returncode, p.stdout + p.stderr


GOOD = {
    "servers": ["localhost:4222"],
    "stream": "hermes",
    "subscribe_allowed": ["ci.>"],
    "wake": [
        {"name": "ci-red", "subject": "ci.result.red", "rate_per_min": 3},
        {"name": "post-deploy", "subject": "ci.deploy.finished", "job": "nightly-audit", "profile": "ops"},
    ],
}


class ValidateCli(unittest.TestCase):
    def test_good_settings_exit_zero(self):
        rc, out = _run(GOOD)
        self.assertEqual(rc, 0, out)
        self.assertIn("OK", out)

    def test_bad_route_exit_nonzero_names_path(self):
        bad = json.loads(json.dumps(GOOD))
        bad["wake"][1].pop("subject")
        bad["wake"][1]["subjet"] = "ci.deploy.finished"  # the peer incident: typo'd key
        rc, out = _run(bad)
        self.assertEqual(rc, 1)
        self.assertIn("wake[1]", out)
        self.assertIn("subjet", out)          # unknown key named
        self.assertIn("subject", out)         # missing required named (from the parser itself)
        self.assertNotIn("wake[0]", out)      # only the offender is named

    def test_route_subject_not_allowlisted_is_refused(self):
        bad = json.loads(json.dumps(GOOD))
        bad["wake"][0]["subject"] = "other.thing"
        rc, out = _run(bad)
        self.assertEqual(rc, 1)
        self.assertIn("wake[0]", out)
        self.assertIn("subscribe_allowed", out)

    def test_hash_subject_refused_even_when_allowlisted(self):
        # 'ci.#' DOES match the ci.> allowlist — only the '#' literal rule
        # can catch it. This is the exact shape of the dead haus.gh.# route.
        bad = json.loads(json.dumps(GOOD))
        bad["wake"][0]["subject"] = "ci.#"
        rc, out = _run(bad)
        self.assertEqual(rc, 1)
        self.assertIn("wake[0]", out)
        self.assertIn("#", out)

    def test_duplicate_route_name_is_refused(self):
        bad = json.loads(json.dumps(GOOD))
        bad["wake"][1]["name"] = "ci-red"
        rc, out = _run(bad)
        self.assertEqual(rc, 1)
        self.assertIn("wake[1]", out)
        self.assertIn("duplicate", out)

    def test_bad_durable_named_by_route(self):
        bad = json.loads(json.dumps(GOOD))
        bad["wake"][0]["durable"] = "bad wildcard *"
        rc, out = _run(bad)
        self.assertEqual(rc, 1)
        self.assertIn("wake[0]", out)
        self.assertIn("durable", out)

    def test_bad_top_level_scalar_names_key(self):
        bad = json.loads(json.dumps(GOOD))
        bad["heartbeat_s"] = "abc"  # loader raises on int(); refusal must name the key
        rc, out = _run(bad)
        self.assertEqual(rc, 1)
        self.assertIn("heartbeat_s", out)

    def test_missing_file_exit_two(self):
        p = subprocess.run([sys.executable, str(SCRIPT), "/nonexistent/x.json"],
                           capture_output=True, text=True, timeout=30)
        self.assertEqual(p.returncode, 2)


class SchemaMirrorsParser(unittest.TestCase):
    """The doc schema must not drift from the model it claims to mirror."""

    def test_schema_keys_equal_model_keys(self):
        from dataclasses import fields
        from hermes_jetstream.config import WakeRoute
        schema = json.loads((REPO / "docs" / "wake-routes.schema.json").read_text())
        self.assertEqual(set(schema["properties"]), {f.name for f in fields(WakeRoute)})
        self.assertEqual(set(schema["required"]), {"name", "subject"})


class WakeAuthzDigest(unittest.TestCase):
    """hjs-oes: the digest must name allowlist, routes, and verdicts from disk."""

    def _home(self, env_lines, ledger_routes=(), runtime_rows=None):
        import sqlite3
        home = Path(tempfile.mkdtemp())
        (home / ".env").write_text("\n".join(env_lines) + "\n")
        data = home / "plugin-data" / "hermes-jetstream"
        data.mkdir(parents=True)
        if runtime_rows is not None:
            (data / "wake-routes.json").write_text(json.dumps(runtime_rows))
        if ledger_routes:
            db = sqlite3.connect(data / "wake-claims.db")
            db.execute("CREATE TABLE wake_claims (dedupe_key TEXT PRIMARY KEY, route TEXT, "
                       "status TEXT, claimed_at REAL, done_at REAL)")
            for i, r in enumerate(ledger_routes):
                db.execute("INSERT INTO wake_claims VALUES(?,?,?,?,?)", (f"k{i}", r, "done", 1.0, None))
            db.commit()
        return home

    def _run(self, home, settings=None):
        p = subprocess.run([sys.executable, str(REPO / "scripts" / "wake_authz_digest.py"),
                            "--home", str(home)] + (["--settings", str(settings)] if settings else []),
                           capture_output=True, text=True, timeout=30)
        return p

    def test_dropped_route_exit_one_and_named(self):
        home = self._home(["JETSTREAM_ALLOWED_USERS=jetstream:alive"],
                          ledger_routes=["alive", "ghost"])
        p = self._run(home)
        self.assertEqual(p.returncode, 1)
        self.assertRegex(p.stdout, r"alive\s+jetstream:alive\s+AUTHORIZED")
        self.assertRegex(p.stdout, r"ghost\s+jetstream:ghost\s+DROPPED")
        self.assertIn("1 authorized, 1 would be dropped", p.stdout)

    def test_global_allowlist_and_wildcard_authorize(self):
        home = self._home(["GATEWAY_ALLOWED_USERS=me", "JETSTREAM_ALLOWED_USERS=*"],
                          runtime_rows={"rt-x": {"name": "rt-x"}})
        p = self._run(home)
        self.assertEqual(p.returncode, 0)
        self.assertIn("AUTHORIZED", p.stdout)

    def test_config_routes_included_via_settings(self):
        home = self._home(["JETSTREAM_ALLOWED_USERS=jetstream:cfg"])
        settings = Path(tempfile.mkdtemp()) / "s.json"
        settings.write_text(json.dumps({"wake": [{"name": "cfg", "subject": "a.b"}]}))
        p = self._run(home, settings)
        self.assertEqual(p.returncode, 0)
        self.assertIn("source=config", p.stdout)

    def test_empty_state_clean_exit(self):
        p = self._run(self._home(["FOO=bar"]))
        self.assertEqual(p.returncode, 0)
        self.assertIn("no wake routes", p.stdout)


if __name__ == "__main__":
    unittest.main()
