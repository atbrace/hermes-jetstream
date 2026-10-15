"""Unit tests: pure core (subject policy, envelope, ledger, wake pipeline, config).
stdlib unittest only — runs anywhere, no nats/gateway imports (LAN-hermetic gate)."""
from __future__ import annotations

import asyncio
import os
import sys
import tempfile
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "plugin"))

from hermes_jetstream.subject import subject_matches, matches_any, SubjectPolicy, validate_subject_literal
from hermes_jetstream.envelope import (build_envelope, is_self_origin, render_prompt,
                                       find_secret_like, parse_envelope, GATEWAY_NONCE)
from hermes_jetstream.ledger import WakeLedger
from hermes_jetstream.wake import WakePipeline
from hermes_jetstream.config import Settings, WakeRoute


class SubjectMatches(unittest.TestCase):
    def test_exact(self):
        self.assertTrue(subject_matches("a.b.c", "a.b.c"))
        self.assertFalse(subject_matches("a.b.c", "a.b"))

    def test_star(self):
        self.assertTrue(subject_matches("a.b.c", "a.*.c"))
        self.assertFalse(subject_matches("a.b.c.d", "a.*.c"))
        self.assertFalse(subject_matches("a.b", "a.*.c"))

    def test_gt(self):
        self.assertTrue(subject_matches("a.b.c", "a.>"))
        self.assertTrue(subject_matches("a.b", "a.>"))
        self.assertFalse(subject_matches("a", "a.>"))      # > requires at least one token after
        self.assertFalse(subject_matches("x.a.b", "a.>"))

    def test_policy_default_deny(self):
        p = SubjectPolicy([])
        self.assertFalse(p.allows("anything"))
        self.assertFalse(p.allows(""))
        q = SubjectPolicy(["hermes.>", "ci.result"])
        self.assertTrue(q.allows("hermes.session.start"))
        self.assertTrue(q.allows("ci.result"))
        self.assertFalse(q.allows("forgejo.webhook.push"))

    def test_no_wildcard_publish(self):
        with self.assertRaises(ValueError):
            validate_subject_literal("hermes.>", "publish")
        with self.assertRaises(ValueError):
            validate_subject_literal("a.*.c", "publish")
        validate_subject_literal("a.b.c", "publish")


class Envelope(unittest.TestCase):
    def test_self_origin(self):
        env = build_envelope("test", {"x": 1}, profile="homelab", session_id="s1")
        self.assertTrue(is_self_origin(env, GATEWAY_NONCE, "homelab"))
        self.assertFalse(is_self_origin(env, GATEWAY_NONCE, "other-profile"))
        self.assertFalse(is_self_origin(env, "deadbeef00", "homelab"))
        self.assertFalse(is_self_origin({"no": "origin"}, GATEWAY_NONCE, "homelab"))

    def test_render_single_brace_only(self):
        out = render_prompt("on {subject}: {payload} seq {seq}", subject="a.b",
                            payload={"k": [1, 2]}, seq=7)
        self.assertEqual(out, 'on a.b: {"k":[1,2]} seq 7')
        # attribute traversal not honored: {payload.__class__} stays literal
        out2 = render_prompt("{payload.__class__}", subject="s", payload={}, seq=1)
        self.assertEqual(out2, "{payload.__class__}")

    def test_truncation(self):
        out = render_prompt("{payload}", subject="s", payload="x" * 5000, seq=1, max_payload_chars=100)
        self.assertLess(len(out), 130)
        self.assertTrue(out.endswith("…[truncated]"))

    def test_secret_detector(self):
        self.assertIsNotNone(find_secret_like({"note": "key sk-abcdefghijklmnopqrstuvwx"}))
        self.assertIsNotNone(find_secret_like("-----BEGIN RSA PRIVATE KEY-----"))
        self.assertIsNone(find_secret_like({"status": "green", "run": 42}))

    def test_parse_envelope_garbage(self):
        self.assertIsNone(parse_envelope(b"not json"))
        self.assertIsNone(parse_envelope(b"[1,2]"))  # list not dict
        self.assertEqual(parse_envelope(b'{"a": 1}'), {"a": 1})


class Ledger(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.ledger = WakeLedger(Path(self.tmp) / "t.db", reclaim_after_s=10)

    def test_claim_complete_duplicate(self):
        self.assertTrue(self.ledger.claim("k1", "r"))
        self.ledger.complete("k1")
        self.assertFalse(self.ledger.claim("k1"))  # done -> deduped

    def test_inflight_not_reclaimable_until_age(self):
        now = time.time()
        self.assertTrue(self.ledger.claim("k2", "r", now=now))
        self.assertFalse(self.ledger.claim("k2", "r", now=now + 5))   # in flight
        self.assertTrue(self.ledger.claim("k2", "r", now=now + 11))   # reclaimed

    def test_prune(self):
        now = time.time()
        self.ledger.claim("k3", "r", now=now)
        self.ledger.complete("k3", now=now)
        self.assertEqual(self.ledger.prune(older_than_s=1, now=now + 2), 1)


class FakeMsg:
    def __init__(self, data: bytes, seq=1, stream="hermes", consumer="c", attempt=1):
        self.data = data
        self._md = (f"{stream}:{consumer}:{seq}", seq, attempt)
        self.acked = False
        self.naked = False

    def metadata(self):
        class MD:
            class sequence:
                stream = self._md[1]
            stream, consumer, deliver_attempt = self._md[0].split(":")[0], self._md[0].split(":")[1], self._md[2]
        return MD()

    async def ack(self):
        self.acked = True

    async def nak(self, delay=0):
        self.naked = True


class WakePipelineGuards(unittest.TestCase):
    def _pipe(self, dispatch=None, nonce="NONCE1", profile="p"):
        route = WakeRoute(name="r1", subject="ci.result", rate_per_min=10)
        return WakePipeline(
            routes_by_subject={"ci.result": route}, subscribe_allowed=["ci.>"],
            ledger=WakeLedger(Path(tempfile.mkdtemp()) / "l.db"),
            dispatch=dispatch or (lambda r, p: asyncio.sleep(0)),
            profile_name=profile, nonce=nonce), route

    def run_async(self, coro):
        return asyncio.run(coro)

    def test_self_origin_skipped(self):
        pipe, route = self._pipe()
        env = build_envelope("x", {}, profile="p")  # nonce == this process
        msg = FakeMsg(__import__("json").dumps(env).encode())
        self.assertEqual(self.run_async(pipe.handle_message(msg, route)), "dispatched")  # nonce differs below
        env["origin"]["nonce"] = "NONCE1"
        msg2 = FakeMsg(__import__("json").dumps(env).encode())
        self.assertEqual(self.run_async(pipe.handle_message(msg2, route)), "skipped_self_origin")
        self.assertTrue(msg2.acked)

    def test_duplicate_skipped(self):
        pipe, route = self._pipe()
        m1 = FakeMsg(b'{"data": 1}', seq=42)
        self.assertEqual(self.run_async(pipe.handle_message(m1, route)), "dispatched")
        m2 = FakeMsg(b'{"data": 1}', seq=42)
        self.assertEqual(self.run_async(pipe.handle_message(m2, route)), "skipped_duplicate")

    def test_rate_throttle_naks(self):
        route = WakeRoute(name="burst", subject="ci.result", rate_per_min=1)
        pipe = WakePipeline({"ci.result": route}, ["ci.>"],
                            WakeLedger(Path(tempfile.mkdtemp()) / "l.db"),
                            lambda r, p: asyncio.sleep(0), "p", "N")
        v1 = self.run_async(pipe.handle_message(FakeMsg(b"{}", seq=1), route))
        v2 = self.run_async(pipe.handle_message(FakeMsg(b"{}", seq=2), route))
        self.assertEqual(v1, "dispatched")
        self.assertEqual(v2, "throttled")

    def test_dispatch_failure_leaves_unacked(self):
        pipe, route = self._pipe(dispatch=lambda r, p: _boom())
        msg = FakeMsg(b"{}", seq=99)
        with self.assertRaises(RuntimeError):
            self.run_async(pipe.handle_message(msg, route))
        self.assertFalse(msg.acked)  # -> redelivery with backoff


async def _boom():
    raise RuntimeError("dispatch down")


class Config(unittest.TestCase):
    def test_env_expansion(self):
        os.environ["HJS_TEST_TOKEN"] = "s3cr3t"
        s = Settings(servers=["h:4222"])
        merged = s.merge_platform_config(s, type("PC", (), {"extra": {"token": "${HJS_TEST_TOKEN}"}, "token": None})())
        self.assertEqual(merged.token, "s3cr3t")

    def test_auth_kwargs_precedence(self):
        s = Settings(token="t", creds="/x.creds")
        self.assertEqual(s.auth_kwargs(), {"token": "t"})
        s2 = Settings(creds="/x.creds")
        self.assertEqual(s2.auth_kwargs(), {"user_credentials": "/x.creds"})
        s3 = Settings(user="u", password="p")
        self.assertEqual(s3.auth_kwargs(), {"user": "u", "password": "p"})
        s4 = Settings(nkey="SUxxxx")
        self.assertEqual(s4.auth_kwargs(), {"nkeys_seed_str": "SUxxxx"})

    def test_wake_route_defaults(self):
        r = WakeRoute.from_dict({"name": "a", "subject": "x.y"})
        self.assertEqual(r.durable, "jetstream-a")
        with self.assertRaises(ValueError):
            WakeRoute.from_dict({"subject": "x.y"})


if __name__ == "__main__":
    unittest.main(verbosity=2)
