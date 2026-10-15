"""Unit tests: pure core (subject policy, envelope, ledger, wake pipeline, config).
stdlib unittest only — runs anywhere, no nats/gateway imports (LAN-hermetic gate)."""
from __future__ import annotations

import asyncio
import json
import os
import sys
import tempfile
import threading
import time
import types
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "plugin"))

from hermes_jetstream.subject import subject_matches, matches_any, SubjectPolicy, validate_subject_literal
from hermes_jetstream.envelope import (build_envelope, is_self_origin, render_prompt,
                                       find_secret_like, parse_envelope, GATEWAY_NONCE)
from hermes_jetstream.ledger import WakeLedger
from hermes_jetstream.wake import WakePipeline
from hermes_jetstream.config import Settings, WakeRoute
from hermes_jetstream.hooks import pick_fields, _TAPS
from hermes_jetstream.tools import _run_async, _on_bus_loop, BusLoopGone


class BusLoopHop(unittest.TestCase):
    """Gateway worker thread has no loop -> _run_async's asyncio.run makes a foreign
    loop where bus-bound futures never resolve. _on_bus_loop must hop onto bus.loop."""

    class _FakeBus:
        """Fake live bus: real background loop, publish returns a future bound to it."""
        connected = True

        def __init__(self, loop):
            self._loop = loop

        @property
        def loop(self):
            return self._loop

        async def publish(self, subject, body, headers=None):
            fut = self._loop.create_future()  # bound to the BUS loop, like nats-py's ack future

            def _resolve():
                fut.set_result("ACK")
            self._loop.call_later(0.05, _resolve)
            return await fut

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self.loop.run_forever, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join(timeout=2)
        self.loop.close()

    def test_worker_thread_hops_and_completes_fast(self):
        # The measured failure shape: no running loop in this thread -> asyncio.run
        # in _run_async -> bus future lives on another loop -> 5.00s timeout.
        bus = self._FakeBus(self.loop)
        t0 = time.monotonic()
        box = {}

        def worker():  # gateway tool handler thread: no event loop
            box["r"] = _run_async(_on_bus_loop(bus, lambda: bus.publish("x.y", b"p", None)))

        th = threading.Thread(target=worker)
        th.start()
        th.join(timeout=5)
        self.assertEqual(box.get("r"), "ACK")
        self.assertLess(time.monotonic() - t0, 1.0, "cross-loop hop must not stall")

    def test_same_loop_awaits_directly(self):
        bus = self._FakeBus(self.loop)

        async def drive():  # already on the bus loop -> no hop needed
            return await _on_bus_loop(bus, lambda: bus.publish("x.y", b"p", None))

        self.assertEqual(asyncio.run_coroutine_threadsafe(drive(), self.loop).result(5),
                         "ACK")

    def test_lost_loop_raises_for_ephemeral_fallback(self):
        class Dead:
            connected = True
            loop = None
        with self.assertRaises(BusLoopGone):
            asyncio.run(_on_bus_loop(Dead(), lambda: None))


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
        self._md = (stream, consumer, seq, attempt)
        self.acked = False
        self.naked = False

    @property
    def metadata(self):  # nats-py: metadata is a property, not a method
        stream, consumer, seq, attempt = self._md
        return types.SimpleNamespace(
            stream=stream, consumer=consumer, num_delivered=attempt,
            sequence=types.SimpleNamespace(stream=seq, consumer=seq))

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

    def test_redelivery_after_crash_claims_ledger(self):
        """Redelivery on the REAL nats-py shape (metadata is a property, num_delivered):
        first delivery dispatches+acks+claims; redelivery must skip AND the wake_claims
        row must exist — a dead dedupe key writes no row (rowcount is the proof)."""
        import sqlite3
        db = Path(tempfile.mkdtemp()) / "l.db"
        route = WakeRoute(name="r", subject="ci.result")
        pipe = WakePipeline({"ci.result": route}, ["ci.>"], WakeLedger(db),
                            lambda r, p: asyncio.sleep(0), "p", "N")
        m1 = FakeMsg(b'{"data": 1}', seq=77, attempt=1)
        self.assertEqual(self.run_async(pipe.handle_message(m1, route)), "dispatched")
        self.assertTrue(m1.acked)
        m2 = FakeMsg(b'{"data": 1}', seq=77, attempt=2)   # server redelivery, num_delivered=2
        self.assertEqual(self.run_async(pipe.handle_message(m2, route)), "skipped_duplicate")
        with sqlite3.connect(db) as c:
            n, status = c.execute(
                "SELECT COUNT(*), MAX(status) FROM wake_claims WHERE dedupe_key LIKE '%:77'"
            ).fetchone()
        self.assertEqual(n, 1)          # non-empty dedupe key -> the claim row exists
        self.assertEqual(status, "done")

    def test_rate_throttle_sheds_acked(self):
        route = WakeRoute(name="burst", subject="ci.result", rate_per_min=1)
        pipe = WakePipeline({"ci.result": route}, ["ci.>"],
                            WakeLedger(Path(tempfile.mkdtemp()) / "l.db"),
                            lambda r, p: asyncio.sleep(0), "p", "N")
        msg1 = FakeMsg(b"{}", seq=1)
        msg2 = FakeMsg(b"{}", seq=2)
        v1 = self.run_async(pipe.handle_message(msg1, route))
        v2 = self.run_async(pipe.handle_message(msg2, route))
        self.assertEqual(v1, "dispatched")
        self.assertEqual(v2, "shed")
        self.assertTrue(msg2.acked and not msg2.naked)  # shed = ack + drop, never naked

    def test_dispatch_failure_leaves_unacked(self):
        pipe, route = self._pipe(dispatch=lambda r, p: _boom())
        msg = FakeMsg(b"{}", seq=99)
        with self.assertRaises(RuntimeError):
            self.run_async(pipe.handle_message(msg, route))

    def test_burst_beyond_bucket_sheds_acked(self):
        """Storm gate: 10 msgs at rate 3/min -> exactly 3 dispatched; every excess
        shed (acked + dropped, same as the coalesced path), nothing left naked."""
        route = WakeRoute(name="storm", subject="ci.result", rate_per_min=3)
        pipe = WakePipeline({"ci.result": route}, ["ci.>"],
                            WakeLedger(Path(tempfile.mkdtemp()) / "l.db"),
                            lambda r, p: asyncio.sleep(0), "p", "N")
        msgs = [FakeMsg(b"{}", seq=i) for i in range(1, 11)]
        verdicts = [self.run_async(pipe.handle_message(m, route)) for m in msgs]
        self.assertEqual(verdicts.count("dispatched"), 3)
        self.assertEqual(verdicts.count("shed"), 7)
        self.assertTrue(all(v in ("dispatched", "shed") for v in verdicts))
        self.assertTrue(all(m.acked and not m.naked for m in msgs))  # everything acked

    def test_ack_after_dispatch_crash_redelivers(self):
        """Claim-then-ack: a dispatch crash leaves the msg unacked (server redelivers)
        AND the ledger claim dead — after reclaim the same stream seq dispatches again.
        A completed run never re-dispatches (covered by test_duplicate_skipped)."""
        import time as _t
        db = Path(tempfile.mkdtemp()) / "l.db"
        ledger = WakeLedger(db)
        route = WakeRoute(name="r", subject="ci.result")
        pipe = lambda disp: WakePipeline({"ci.result": route}, ["ci.>"], ledger, disp, "p", "N")
        msg = FakeMsg(b"{}", seq=7)
        with self.assertRaises(RuntimeError):
            self.run_async(pipe(lambda r, p: _boom()).handle_message(msg, route))
        self.assertFalse(msg.acked)                       # redelivery will happen
        m2 = FakeMsg(b"{}", seq=7)
        self.assertEqual(self.run_async(pipe(lambda r, p: asyncio.sleep(0))
                                         .handle_message(m2, route)), "skipped_duplicate")
        # simulate owner death + reclaim window, then redelivery must dispatch
        import sqlite3
        with sqlite3.connect(db) as c:
            c.execute("UPDATE wake_claims SET claimed_at=? WHERE 1", (_t.time() - 9999,))
            c.commit()
        m3 = FakeMsg(b"{}", seq=7)
        self.assertEqual(self.run_async(pipe(lambda r, p: asyncio.sleep(0))
                                        .handle_message(m3, route)), "dispatched")
        self.assertTrue(m3.acked)                         # ack AFTER dispatch accepted


class WakeExtras(unittest.TestCase):
    """hjs-530 coverage gaps: min_interval_s coalescing (acked-loss path explicit),
    event-job route flow at the pipeline seam, and the FakeMsg shape audit."""

    def run_async(self, coro):
        return asyncio.run(coro)

    def _pipe(self, dispatch, **route_kw):
        route = WakeRoute(name="hjs530", subject="ci.result", **route_kw)
        pipe = WakePipeline({"ci.result": route}, ["ci.>"],
                            WakeLedger(Path(tempfile.mkdtemp()) / "l.db"),
                            dispatch, "p", "N")
        return pipe, route

    def test_min_interval_coalesces_acked_and_recovers(self):
        """min_interval_s=60: 1st dispatches, 2nd inside the window returns 'coalesced'
        AND is ACKED (lossy by design — no ledger claim, so the event is gone), and a
        redelivery of the SAME seq still acks coalesced (the coalesce skip runs before
        claim). After the window the route dispatches again."""
        import sqlite3
        db = Path(tempfile.mkdtemp()) / "l.db"
        route = WakeRoute(name="coop", subject="ci.result", min_interval_s=60)
        pipe = WakePipeline({"ci.result": route}, ["ci.>"], WakeLedger(db),
                            lambda r, p: asyncio.sleep(0), "p", "N")
        m1 = FakeMsg(b'{"n": 1}', seq=1)
        m2 = FakeMsg(b'{"n": 2}', seq=2)
        m2b = FakeMsg(b'{"n": 2}', seq=2)          # redelivery of the coalesced seq
        self.assertEqual(self.run_async(pipe.handle_message(m1, route)), "dispatched")
        self.assertEqual(self.run_async(pipe.handle_message(m2, route)), "coalesced")
        self.assertTrue(m2.acked and not m2.naked)  # coalesced = ack + drop, never naked
        self.assertEqual(self.run_async(pipe.handle_message(m2b, route)), "coalesced")
        with sqlite3.connect(db) as c:              # lossy proof: coalesced seq wrote NO claim row
            rows = {r[0] for r in c.execute(
                "SELECT dedupe_key FROM wake_claims").fetchall()}
        self.assertTrue(any(k.endswith(":1") for k in rows))
        self.assertFalse(any(k.endswith(":2") for k in rows))
        # window expiry without sleeping 60s: rewind the interval clock
        pipe._buckets[route.name].min_seen = time.time() - 61
        m3 = FakeMsg(b'{"n": 3}', seq=3)
        self.assertEqual(self.run_async(pipe.handle_message(m3, route)), "dispatched")

    def test_event_job_route_roundtrip(self):
        """from_dict round-trips job/profile (the event-job fields) and clamps junk."""
        r = WakeRoute.from_dict({"name": "nightly", "subject": "ci.result",
                                 "job": " rotate-db ", "profile": "homelab",
                                 "min_interval_s": -5})
        self.assertEqual(r.job, "rotate-db")
        self.assertEqual(r.profile, "homelab")
        self.assertEqual(r.durable, "jetstream-nightly")
        self.assertEqual(r.min_interval_s, 0.0)     # negative clamped to 0
        plain = WakeRoute.from_dict({"name": "a", "subject": "x.y"})
        self.assertEqual((plain.job, plain.profile), ("", ""))

    def test_event_job_route_pipeline_seam_is_dispatch_route_prompt(self):
        """The wake-vs-job fork lives in the ADAPTER (_fire_event_job needs gateway
        imports); the pipeline only calls dispatch(route, prompt). Prove a route with
        .job set flows identically: dispatch receives that exact route object +
        rendered prompt — the fork is the adapter's job, not the pipeline's."""
        seen = []

        def spy(r, p):
            seen.append((r, p))
            return asyncio.sleep(0)
        pipe, route = self._pipe(spy, job="rotate-db", profile="homelab")
        msg = FakeMsg(b'{"k": 1}', seq=5)
        self.assertEqual(self.run_async(pipe.handle_message(msg, route)), "dispatched")
        self.assertEqual(len(seen), 1)
        self.assertIs(seen[0][0], route)            # the SAME route obj, .job intact
        self.assertEqual(seen[0][0].job, "rotate-db")
        self.assertIn("ci.result", seen[0][1])      # prompt rendered as for any wake
        self.assertTrue(msg.acked)

    def test_fake_msg_metadata_is_property_like_nats(self):
        """Shape audit (hjs-530): FakeMsg.metadata must be a plain attribute read —
        nats-py exposes it as a @property; any test/caller doing msg.metadata() would
        crash the same way production would. Also assert the fields _msg_identity reads."""
        m = FakeMsg(b"{}", seq=31, attempt=2)
        md = m.metadata                             # no parens — property access
        self.assertFalse(callable(md), "metadata must not be callable (nats-py property)")
        self.assertEqual((md.stream, md.consumer, md.num_delivered), ("hermes", "c", 2))
        self.assertEqual(md.sequence.stream, 31)


async def _boom():
    raise RuntimeError("dispatch down")


class WatchRegistryLogic(unittest.TestCase):
    def _reg(self, allowed=("ci.>",), max_watches=3, inject_ok=True):
        from hermes_jetstream.watch import WatchRegistry
        subs = []
        injects = []

        async def open_sub(subject, cb):
            subs.append(subject)
            return types.SimpleNamespace(cb=cb, unsubscribed=False)

        async def close_sub(sub):
            sub.unsubscribed = True

        async def inject(sid, text):  # async fake mirrors production async inject
            injects.append((sid, text))
            return inject_ok
        reg = WatchRegistry(policy=SubjectPolicy(list(allowed)), open_sub=open_sub,
                            close_sub=close_sub, inject=inject, max_watches=max_watches)
        return reg, subs, injects

    def test_allowlist_enforced_at_creation(self):
        reg, _, _ = self._reg()
        self.assertIn("ERROR", asyncio.run(reg.add("secret.thing", "s1")))
        self.assertIn("ERROR", asyncio.run(reg.add("", "s1")))
        self.assertIn("ERROR", asyncio.run(reg.add("ci.x", "")))  # no session context

    def test_watch_fire_injects_and_max_msgs_expires(self):
        reg, subs, injects = self._reg()
        out = asyncio.run(reg.add("ci.result.*", "sess-1", note="deploy gate", ttl_s=60, max_msgs=1))
        self.assertIn("Watching", out)
        w = reg._watches["w1"]
        msg = types.SimpleNamespace(subject="ci.result.red",
                                    data=json.dumps({"kind": "ci", "data": {"job": "r1"}}).encode())
        asyncio.run(w.sub.cb(msg))
        self.assertEqual(injects[0][0], "sess-1")
        self.assertIn("ci.result.red", injects[0][1])
        self.assertIn("deploy gate", injects[0][1])
        self.assertTrue(w.expired)          # max_msgs hit
        asyncio.run(reg.sweep())
        self.assertEqual(reg.count(), 0)    # swept away

    def test_inject_refused_closes_watch(self):
        reg, subs, _ = self._reg(inject_ok=False)
        asyncio.run(reg.add("ci.x", "s1"))
        msg = types.SimpleNamespace(subject="ci.x", data=b"{}")
        asyncio.run(reg._watches["w1"].sub.cb(msg))
        self.assertEqual(reg.count(), 0)      # refused inject -> watch removed

    def test_ttl_expiry_sweep_and_cap(self):
        reg, subs, _ = self._reg(max_watches=1)
        asyncio.run(reg.add("ci.a", "s1", ttl_s=0.01))
        time.sleep(0.02)
        asyncio.run(reg.sweep())
        self.assertEqual(reg.count(), 0)
        self.assertEqual(len(subs), 1)      # sub opened
        asyncio.run(reg.add("ci.a", "s1"))
        self.assertIn("limit", asyncio.run(reg.add("ci.b", "s2")))

    def test_unwatch_closes_sub(self):
        reg, subs, _ = self._reg()
        asyncio.run(reg.add("ci.x", "s1"))
        self.assertIn("Unwatched", asyncio.run(reg.remove("w1")))
        self.assertIn("ERROR", asyncio.run(reg.remove("w1")))


    def test_ensure_stream_never_creates(self):
        """Generality law: the plugin owns NO stream provisioning. No plugin module may
        call add_stream/delete_stream — only the drills (self-cleaning) do."""
        pkg = Path(__file__).resolve().parents[1] / "plugin" / "hermes_jetstream"
        for f in pkg.glob("*.py"):
            self.assertNotIn("add_stream", f.read_text(), f"{f.name} must not create streams")
            self.assertNotIn("delete_stream", f.read_text(), f"{f.name} must not delete streams")
            self.assertNotIn("update_stream", f.read_text(), f"{f.name} must not mutate streams")


class ConfigAudit(unittest.TestCase):
    def test_no_plaintext_secret_defaults_in_manifest(self):
        """Audit gate: every credential-shaped config key must default empty so secrets
        can only arrive via ${VAR} -> .env, never baked into config.yaml or defaults."""
        import re
        text = (Path(__file__).resolve().parents[1] / "plugin" / "hermes_jetstream" /
                "plugin.yaml").read_text()
        for key in ("token", "password", "nkey", "creds", "tls"):
            m = re.search(rf"^\s*{key}.*$", text, re.MULTILINE)
            if m:
                self.assertNotIn("default:", m.group(0).replace("default: {}", "")
                                 .replace("default: []", ""),
                                 f"{key} manifest line carries a default value")


class LifecycleTapPrivacy(unittest.TestCase):
    def test_whitelist_projection(self):
        out = pick_fields("session.start", {"platform": "tg", "session_key": "k",
                                            "message": "SECRET BODY", "extra": 1})
        self.assertEqual(out, {"platform": "tg", "session_key": "k"})

    def test_unknown_kind_projects_empty(self):
        self.assertEqual(pick_fields("nope", {"a": 1}), {})

    def test_tap_kinds_project_no_bodies(self):
        for kind, (_hook, fields) in _TAPS.items():
            forbidden = {"message", "prompt", "text", "args", "command", "payload_raw", "response"}
            self.assertFalse(forbidden & set(fields), f"{kind} whitelist leaks bodies")


class Config(unittest.TestCase):
    def test_env_expansion(self):
        os.environ["HJS_TEST_TOKEN"] = "s3cr3t"
        s = Settings(servers=["h:4222"])
        merged = s.merge_platform_config(s, type("PC", (), {"extra": {"token": "${HJS_TEST_TOKEN}"}, "token": None})())
        self.assertEqual(merged.token, "s3cr3t")

    def test_auth_kwargs_precedence(self):
        import tempfile
        from pathlib import Path
        d = tempfile.mkdtemp()
        jwt_file = Path(d) / "u.creds"
        jwt_file.write_text("-----BEGIN NATS USER JWT-----\neyJhbGciOiJlZDI1NTE5In0\n------END NATS USER JWT------\n")
        seed_file = Path(d) / "seed.creds"
        seed_file.write_text("-----BEGIN USER NKEY SEED-----\nSUAAA\n------END USER NKEY SEED------\n")
        s = Settings(token="t", creds=str(jwt_file))
        self.assertEqual(s.auth_kwargs(), {"token": "t"})
        s2 = Settings(creds=str(jwt_file))
        self.assertEqual(s2.auth_kwargs(), {"user_credentials": str(jwt_file)})
        # seed-only file must NOT route to user_credentials (nats-py JWT reader hangs)
        s2b = Settings(creds=str(seed_file))
        self.assertEqual(s2b.auth_kwargs(), {"nkeys_seed_str": "SUAAA"})
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
