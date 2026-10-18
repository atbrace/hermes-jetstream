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
        self.assertFalse(q.allows("other.webhook.push"))

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
        self.assertEqual(render_prompt("at {ts}", subject="s", payload={"ts": 12.5}, seq=1), "at 12.5")
        self.assertEqual(render_prompt("at {ts}", subject="s", payload={"no_ts": 1}, seq=1), "at ")

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
        self.ledger.claim("k4", "r", now=now)  # still CLAIMED — prune must never eat in-flight rows
        self.assertEqual(self.ledger.prune(older_than_s=1, now=now + 2), 1)
        self.assertFalse(self.ledger.claim("k4", now=now + 3))  # row survived prune -> still deduped in flight
        self.assertTrue(self.ledger.claim("k4", now=now + 9999))  # and reclaimable once stale


class FakeMsg:
    def __init__(self, data: bytes, seq=1, stream="hermes", consumer="c", attempt=1):
        self.data = data
        self._md = (stream, consumer, seq, attempt)
        self.acked = False
        self.naked = False
        self.terminaled = False

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

    async def term(self):
        self.terminaled = True


class WakePipelineGuards(unittest.TestCase):
    def _pipe(self, dispatch=None, nonce="NONCE1", profile="p"):
        route = WakeRoute(name="r1", subject="ci.result", rate_per_min=10)
        return WakePipeline(
            subscribe_allowed=["ci.>"],
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
        pipe = WakePipeline(["ci.>"], WakeLedger(db),
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
        pipe = WakePipeline(["ci.>"],
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
        pipe = WakePipeline(["ci.>"],
                            WakeLedger(Path(tempfile.mkdtemp()) / "l.db"),
                            lambda r, p: asyncio.sleep(0), "p", "N")
        msgs = [FakeMsg(b"{}", seq=i) for i in range(1, 11)]
        verdicts = [self.run_async(pipe.handle_message(m, route)) for m in msgs]
        self.assertEqual(verdicts.count("dispatched"), 3)
        self.assertEqual(verdicts.count("shed"), 7)
        self.assertTrue(all(v in ("dispatched", "shed") for v in verdicts))
        self.assertTrue(all(m.acked and not m.naked for m in msgs))  # everything acked

    def test_route_subject_outside_allowlist_acks_skip(self):
        """Config-drift guard: a route whose subject no longer matches subscribe_allowed
        must ack+skip, not redeliver forever."""
        route = WakeRoute(name="orphan", subject="other.thing")
        pipe = WakePipeline(["ci.>"], WakeLedger(Path(tempfile.mkdtemp()) / "l.db"),
                            lambda r, p: asyncio.sleep(0), "p", "N")
        msg = FakeMsg(b"{}", seq=1)
        self.assertEqual(self.run_async(pipe.handle_message(msg, route)), "skipped_not_allowed")
        self.assertTrue(msg.acked)

    def test_garbage_envelope_dispatches_raw_payload(self):
        """Non-JSON body is not an envelope: the wake still fires with the raw bytes as
        payload (rendered via str) — malformed events must not wedge the consumer."""
        seen = []
        route = WakeRoute(name="raw", subject="ci.result")
        pipe = WakePipeline(["ci.>"], WakeLedger(Path(tempfile.mkdtemp()) / "l.db"),
                            lambda r, p: seen.append(p) or asyncio.sleep(0), "p", "N")
        msg = FakeMsg(b"hello-not-json", seq=8)
        self.assertEqual(self.run_async(pipe.handle_message(msg, route)), "dispatched")
        self.assertIn("hello-not-json", seen[0])
        self.assertTrue(msg.acked)

    def test_bucket_refills_over_time(self):
        """Token bucket earns back at rate_per_min/60 per second: drained to 0 then 3s
        elapse at rate 60/min = exactly 3 tokens — three dispatches, a 4th sheds."""
        route = WakeRoute(name="refill", subject="ci.result", rate_per_min=60)
        pipe = WakePipeline(["ci.>"], WakeLedger(Path(tempfile.mkdtemp()) / "l.db"),
                            lambda r, p: asyncio.sleep(0), "p", "N")
        self.assertEqual(self.run_async(pipe.handle_message(FakeMsg(b"{}", seq=1), route)), "dispatched")
        b = pipe._buckets[route.name]
        b.tokens = 0.0
        b.updated = time.time() - 3           # 3s at 60/min = +3 tokens
        verdicts = [self.run_async(pipe.handle_message(FakeMsg(b"{}", seq=s), route))
                    for s in (2, 3, 4)]
        self.assertEqual(verdicts, ["dispatched", "dispatched", "dispatched"])
        self.assertEqual(self.run_async(pipe.handle_message(FakeMsg(b"{}", seq=5), route)), "shed")

    def test_ack_after_dispatch_crash_redelivers(self):
        """Claim-then-ack: a dispatch crash leaves the msg unacked (server redelivers)
        AND the ledger claim dead — after reclaim the same stream seq dispatches again.
        A completed run never re-dispatches (covered by test_duplicate_skipped)."""
        import time as _t
        db = Path(tempfile.mkdtemp()) / "l.db"
        ledger = WakeLedger(db)
        route = WakeRoute(name="r", subject="ci.result")
        pipe = lambda disp: WakePipeline(["ci.>"], ledger, disp, "p", "N")
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
        pipe = WakePipeline(["ci.>"],
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
        pipe = WakePipeline(["ci.>"], WakeLedger(db),
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




async def _boom():
    raise RuntimeError("dispatch down")


class WatchabilityStats(unittest.TestCase):
    """hjs-htk: every pipeline verdict is counted and clocked, so a session can ask
    'is my watch alive?' from plugin-owned surfaces alone (docs/BATTLE-TEST.md)."""

    def run_async(self, coro):
        return asyncio.run(coro)

    def _pipe(self, dispatch=None, rate=10):
        route = WakeRoute(name="st", subject="ci.result", rate_per_min=rate)
        pipe = WakePipeline(["ci.>"], WakeLedger(Path(tempfile.mkdtemp()) / "l.db"),
                            dispatch or (lambda r, p: asyncio.sleep(0)), "p", "N")
        return pipe, route

    def test_dispatch_counted_and_clocked(self):
        pipe, route = self._pipe()
        self.run_async(pipe.handle_message(FakeMsg(b'{"data": 1}', seq=7), route))
        s = pipe.stats_snapshot()["st"]
        self.assertEqual((s["attempted"], s["dispatched"]), (1, 1))
        self.assertGreater(s["last_attempt_ts"], 0)
        self.assertGreaterEqual(s["last_dispatch_ts"], s["last_attempt_ts"] - 5)

    def test_shed_and_duplicate_counted_separately(self):
        pipe, route = self._pipe(rate=1)
        self.run_async(pipe.handle_message(FakeMsg(b'{"a":1}', seq=1), route))
        for i in range(3):
            self.run_async(pipe.handle_message(FakeMsg(b'{"b":%d}' % i, seq=10 + i), route))
        s = pipe.stats_snapshot()["st"]
        self.assertEqual(s["attempted"], 4)
        self.assertEqual(s["dispatched"], 1)
        self.assertEqual(s["shed"], 3)

    def test_dispatch_failure_counted_as_failed_and_still_raises(self):
        async def boom(route, prompt):
            raise RuntimeError("dispatch down")
        pipe, route = self._pipe(dispatch=boom)
        with self.assertRaises(RuntimeError):
            self.run_async(pipe.handle_message(FakeMsg(b'{"x":1}', seq=2), route))
        s = pipe.stats_snapshot()["st"]
        self.assertEqual(s["failed"], 1)
        self.assertEqual(s["dispatched"], 0)
        self.assertGreater(s["last_attempt_ts"], 0)

    def test_idle_route_has_no_stats_not_fake_zero_row(self):
        pipe, _route = self._pipe()
        self.assertEqual(pipe.stats_snapshot(), {})  # nothing attempted = no row, not a lie


class FetchResumeCursor(unittest.TestCase):
    """hjs-9um: per-call ephemeral consumers replayed the subject from stream start,
    so a second fetch re-read acked messages. The fix is an in-process resume cursor;
    this pins the cursor config without touching NATS (behavior probed live too)."""

    def _tools(self):
        from hermes_jetstream.tools import JetstreamTools
        from hermes_jetstream.config import Settings
        return JetstreamTools(Settings(subscribe_allowed=["ci.>"]), None)

    def test_no_cursor_means_no_config_override(self):
        self.assertIsNone(self._tools()._resume_cfg("ci.x"))

    def test_cursor_produces_start_sequence_plus_one(self):
        t = self._tools()
        t._advance_cursor("ci.x", [FakeMsg(b"a", seq=41), FakeMsg(b"b", seq=42)])
        cfg = t._resume_cfg("ci.x")
        self.assertIsNotNone(cfg)
        self.assertEqual(str(cfg.deliver_policy), "DeliverPolicy.BY_START_SEQUENCE")
        self.assertEqual(cfg.opt_start_seq, 43)

    def test_cursor_only_advances(self):
        t = self._tools()
        t._advance_cursor("ci.x", [FakeMsg(b"a", seq=50)])
        t._advance_cursor("ci.x", [FakeMsg(b"b", seq=45)])   # out-of-order batch: no rewind
        self.assertEqual(t._fetch_cursor["ci.x"], 50)


class BusLoopCapture(unittest.TestCase):
    """hjs-c81 dogfood: nats-py stores no _loop on the client, so a property that
    reads _nc._loop is ALWAYS None — which silently killed the lifecycle tap and
    forced tools onto the ephemeral path. bus.loop must be the loop that ran connect()."""

    def run_async(self, coro):
        return asyncio.run(coro)

    class _FakeNC:
        def __init__(self): self.is_closed = False
        async def connect(self, *a, **k): pass
        def jetstream(self): return object()
        async def close(self, **k): self.is_closed = True

    class _FakeNats:
        def NATS(self): return BusLoopCapture._FakeNC()

    def test_loop_is_connect_loop_not_none(self):
        from hermes_jetstream import bus as bus_mod
        from hermes_jetstream.bus import NatsBus
        from hermes_jetstream.config import Settings
        saved = bus_mod.nats
        bus_mod.nats = self._FakeNats()
        try:
            async def t():
                b = NatsBus(Settings(servers=["nats://x"], stream="s"), "p")
                await b.connect()
                import asyncio as a
                self.assertIs(b.loop, a.get_running_loop())   # the old getattr path: None
                await b.stop()
                self.assertIsNone(b.loop)
            self.run_async(t())
        finally:
            bus_mod.nats = saved


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
    def test_manifest_defaults_carry_no_secrets(self):
        """The credential law, made real: auth/tls default to empty containers and no
        baked string default is secret-shaped (the old line-regex never matched a line)."""
        import re
        from hermes_jetstream.envelope import find_secret_like
        text = (Path(__file__).resolve().parents[1] / "plugin" / "hermes_jetstream" /
                "plugin.yaml").read_text()
        for key in ("auth", "tls"):
            self.assertRegex(text, rf"(?m)^\s*{key}:.*default:\s*\{{\}}\s*\}}\s*$",
                             f"config_schema {key} must default to an empty dict")
        string_defaults = re.findall(r'default:\s*"([^"]*)"', text)
        self.assertIsNone(find_secret_like(string_defaults),
                          "config_schema carries a secret-shaped default")


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

    def test_secret_expansion_prefers_agent_scope(self):
        # catalog rule 11: when agent.secret_scope is importable, ${VAR} goes
        # through get_secret (profile-scoped), never bare os.environ.
        import hermes_jetstream.config as cfg
        calls = []
        fake = types.ModuleType("agent.secret_scope")
        fake.get_secret = lambda name, default=None: (calls.append(name), "scoped")[1]
        fake_pkg = types.ModuleType("agent")
        fake_pkg.secret_scope = fake
        saved = {m: sys.modules.get(m) for m in ("agent", "agent.secret_scope")}
        sys.modules["agent"], sys.modules["agent.secret_scope"] = fake_pkg, fake
        try:
            os.environ["HJS_TEST_TOKEN"] = "environ-value"
            self.assertEqual(cfg._expand("${HJS_TEST_TOKEN}"), "scoped")
            self.assertEqual(calls, ["HJS_TEST_TOKEN"])
        finally:
            for m, v in saved.items():
                if v is None:
                    del sys.modules[m]
                else:
                    sys.modules[m] = v

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
        # wildcard subject -> wildcard default durable must fail at config load
        with self.assertRaises(ValueError):
            WakeRoute.from_dict({"name": "ci.*", "subject": "ci.*"})
        with self.assertRaises(ValueError):
            WakeRoute.from_dict({"name": "a", "subject": "x.y", "durable": "bad durable"})
        self.assertEqual(
            WakeRoute.from_dict({"name": "a", "subject": "x.y", "durable": "mine"}).durable,
            "mine")


class ToolsPublishFetch(unittest.TestCase):
    """JetstreamTools surface without a live bus: every refusal path is a string,
    the bus is never reached (patched to explode if it were)."""

    def _tools(self, publish_allowed=("ci.emit", "ops.>"), subscribe_allowed=("ci.>",),
               max_payload_bytes=4096):
        from hermes_jetstream.tools import JetstreamTools
        s = Settings(servers=["h:1"], publish_allowed=list(publish_allowed),
                     subscribe_allowed=list(subscribe_allowed),
                     max_payload_bytes=max_payload_bytes)
        t = JetstreamTools(s, ctx=None)

        async def explode(*a, **k):
            raise AssertionError("bus must not be reached on a refused publish")
        t._publish = explode
        t._fetch = explode
        return t

    def run_async(self, coro):
        return asyncio.run(coro)

    def test_wildcard_publish_refused(self):
        t = self._tools()
        self.assertIn("literal", self.run_async(t.nats_publish("ops.>", {}, kind="k")))

    def test_allowlist_refused(self):
        t = self._tools()
        out = self.run_async(t.nats_publish("other.thing", {}, kind="k"))
        self.assertIn("publish_allowed", out)

    def test_empty_allowlist_denies_everything(self):
        t = self._tools(publish_allowed=())
        self.assertIn("ERROR", self.run_async(t.nats_publish("ci.emit", {}, kind="k")))

    def test_secret_shaped_payload_refused(self):
        t = self._tools()
        out = self.run_async(t.nats_publish("ci.emit", {"key": "sk-" + "A" * 20}, kind="k"))
        self.assertIn("secret", out)

    def test_oversize_envelope_refused(self):
        t = self._tools(max_payload_bytes=100)
        out = self.run_async(t.nats_publish("ci.emit", {"big": "x" * 500}, kind="k"))
        self.assertIn("max_payload_bytes", out)

    def test_publish_success_reports_ack(self):
        t = self._tools()
        async def fake_publish(subject, body):
            t.sent = (subject, body)
            return types.SimpleNamespace(stream="hermes", seq=7, duplicate=False)
        t._publish = fake_publish
        out = self.run_async(t.nats_publish("ci.emit", {"ok": True}, kind="ci-result"))
        self.assertIn("seq=7", out)
        subject, body = t.sent
        self.assertEqual(subject, "ci.emit")
        env = json.loads(body)
        self.assertEqual(env["kind"], "ci-result")
        self.assertEqual(env["data"], {"ok": True})
        self.assertEqual(env["schema"], "hermes-jetstream.v1")

    def test_duplicate_ack_reported(self):
        t = self._tools()
        async def dup(subject, body):
            return types.SimpleNamespace(stream="s", seq=1, duplicate=True)
        t._publish = dup
        self.assertIn("duplicate", self.run_async(t.nats_publish("ci.emit", {}, kind="k")))

    def test_fetch_respects_allowlist_and_clamps(self):
        t = self._tools()
        self.assertIn("subscribe_allowed", self.run_async(t.nats_fetch("secret.thing")))
        seen = {}
        async def fake_fetch(subject, timeout, batch):
            seen.update(timeout=timeout, batch=batch)
            return []
        t._fetch = fake_fetch
        out = self.run_async(t.nats_fetch("ci.x", timeout_seconds=9999, batch=99))
        self.assertIn("No messages", out)
        self.assertEqual((seen["timeout"], seen["batch"]), (300.0, 10))  # clamped

    def test_fetch_json_and_raw_bodies(self):
        t = self._tools()
        async def fake_fetch(subject, timeout, batch):
            return [FakeMsg(b'{"a": 1}'), FakeMsg(b"plain")]
        t._fetch = fake_fetch
        out = json.loads(self.run_async(t.nats_fetch("ci.x")))
        self.assertEqual(out, [{"a": 1}, "plain"])

    def test_msg_id_is_deterministic(self):
        from hermes_jetstream.tools import _msg_id
        self.assertEqual(_msg_id("ci.x", b"abc"), _msg_id("ci.x", b"abc"))
        self.assertNotEqual(_msg_id("ci.x", b"abc"), _msg_id("ci.x", b"abd"))
        self.assertTrue(_msg_id("ci.x", b"abc").startswith("hermes-"))

    def test_msg_id_salted_by_subject(self):
        """Same body on two subjects of one stream must NOT share a wire msg-id
        (JetStream dedupes Nats-Msg-Id per stream, hjs-cxe)."""
        sent = []
        t = self._tools(publish_allowed=("ops.>",))
        async def fake_ephemeral(fn):
            class FakeJS:
                async def publish(self, subject, body, timeout=None, headers=None):
                    sent.append((subject, (headers or {})["Nats-Msg-Id"]))
            return await fn(FakeJS())
        t._ephemeral = fake_ephemeral
        del t._publish  # un-stub: this test exercises the real _publish wire headers
        body = b'{"kind":"event","data":{}}'
        self.run_async(t._publish("ops.alpha", body))
        self.run_async(t._publish("ops.beta", body))
        self.assertEqual([s for s, _ in sent], ["ops.alpha", "ops.beta"])
        self.assertNotEqual(sent[0][1], sent[1][1])  # no cross-subject dedupe collision
        self.run_async(t._publish("ops.alpha", body))
        self.assertEqual(sent[2][1], sent[0][1])     # in-subject republish keeps id (dedupes)


class SettingsFromCtx(unittest.TestCase):
    """The ctx.get_config surface: from_plugin_ctx must expand ${VAR}, coerce lists,
    and fail closed on junk routes."""

    class _Ctx:
        def __init__(self, d):
            self._d = d
        def get_config(self, key, default=None):
            return self._d.get(key, default)

    def test_full_roundtrip_with_env_expansion(self):
        os.environ["HJS_T2_TOKEN"] = "tok-1"
        s = Settings.from_plugin_ctx(self._Ctx({
            "servers": [" h1:4222 ", "h2:4222", ""],
            "auth": {"token": "${HJS_T2_TOKEN}"},
            "publish_allowed": ["ci.emit "],
            "subscribe_allowed": "ci.>",          # single string coerced to list
            "wake": [{"name": "w", "subject": "ci.result"}, "junk"],
        }))
        self.assertEqual(s.token, "tok-1")
        self.assertEqual(s.servers, ["h1:4222", "h2:4222"])   # trimmed, empties dropped
        self.assertEqual(s.publish_allowed, ["ci.emit"])
        self.assertEqual(s.subscribe_allowed, ["ci.>"])
        self.assertEqual([r.name for r in s.wake], ["w"])      # junk entries dropped

    def test_missing_env_var_expands_empty(self):
        os.environ.pop("HJS_T2_ABSENT", None)
        s = Settings.from_plugin_ctx(self._Ctx({"auth": {"token": "${HJS_T2_ABSENT}"}}))
        self.assertEqual(s.token, "")

    def test_tls_kwargs(self):
        s = Settings()
        self.assertEqual(s.tls_kwargs(), {})                    # disabled -> nothing
        import tempfile
        d = tempfile.mkdtemp()
        s2 = Settings(tls_enabled=True)
        self.assertIn("tls", s2.tls_kwargs())                   # bare TLS: default ctx
        s2b = Settings(tls_enabled=True, tls_insecure_skip_verify=True)
        import ssl
        self.assertIs(s2b.tls_kwargs()["tls"].verify_mode, ssl.CERT_NONE)
        s3 = Settings(tls_enabled=True, tls_cert=str(Path(d) / "nope.pem"),
                      tls_key=str(Path(d) / "nope-key.pem"))
        with self.assertRaises(OSError):                        # unreadable mTLS pair fails loud
            s3.tls_kwargs()

    def test_creds_file_unreadable_raises_valueerror(self):
        s = Settings(creds=str(Path(tempfile.mkdtemp()) / "missing.creds"))
        with self.assertRaises(ValueError):
            s.auth_kwargs()


class HookRegistration(unittest.TestCase):
    """register_hooks fail-closed gates: no config = no wire; unknown kinds ignored;
    every enabled kind registers its mapped hook."""

    class _Ctx:
        def __init__(self):
            self.hooks, self.unloads = [], []
        def register_hook(self, name, fn):
            self.hooks.append(name)
        def on_unload(self, fn):
            self.unloads.append(fn)

    def test_disabled_noops(self):
        from hermes_jetstream.hooks import register_hooks
        ctx = self._Ctx()
        register_hooks(ctx, Settings(lifecycle_emit=[]), "p", lambda: None)
        self.assertEqual(ctx.hooks, [])

    def test_unknown_kind_ignored_known_wired(self):
        from hermes_jetstream.hooks import register_hooks
        ctx = self._Ctx()
        s = Settings(lifecycle_emit=["session.start", "does.not.exist"])
        register_hooks(ctx, s, "p", lambda: None)
        self.assertEqual(ctx.hooks, ["on_session_start"])
        self.assertEqual(len(ctx.unloads), 1)
        ctx.unloads[0]()  # stop must be safe to call

    def test_tap_projects_before_queueing(self):
        """A hook callback must never queue bodies: only whitelisted fields ride."""
        from hermes_jetstream.hooks import LifecycleTap
        tap = LifecycleTap(Settings(lifecycle_emit=["session.start"]), "p", lambda: None)
        tap._emit("session.start")(platform="tg", session_key="k", message="SECRET")
        subject, body = tap.q.get_nowait()
        self.assertEqual(subject, "hermes.session.start")
        self.assertNotIn("SECRET", body.decode())
        self.assertIn('"session_key"', body.decode())
        tap.stop()


if __name__ == "__main__":
    unittest.main(verbosity=2)


class LostWakeAccounting(unittest.TestCase):
    """hjs-25g + hjs-xq5: a lost wake must be RECORDED wherever it dies."""

    def _pipe(self, db, dispatch=None):
        return WakePipeline(["ci.>"], WakeLedger(db, reclaim_after_s=10),
                            dispatch or (lambda r, p: asyncio.sleep(0)), "p", "N")

    def test_startup_reconcile_marks_and_counts(self):
        db = Path(tempfile.mkdtemp()) / "l.db"
        led = WakeLedger(db, reclaim_after_s=10)
        led.claim("s:c:9", "r1", now=time.time() - 30)   # dead: past reclaim window
        led.claim("s:c:10", "r1", now=time.time())       # fresh: in flight elsewhere
        rows = led.reconcile_lost()
        self.assertEqual([r[0] for r in rows], ["s:c:9"])
        pipe = self._pipe(db)
        pipe.note_lost("r1", n=len(rows))
        self.assertEqual(pipe.stats_snapshot()["r1"]["lost"], 1)
        self.assertEqual(pipe.stats_snapshot()["r1"]["attempted"], 0)  # not a delivery attempt
        self.assertEqual(led.reconcile_lost(), [])       # recorded once; idempotent
        self.assertTrue(led.claim("s:c:9", "r1"))        # lost row re-claims: second chance

    def test_final_delivery_failure_records_loss(self):
        db = Path(tempfile.mkdtemp()) / "l.db"
        pipe = self._pipe(db, dispatch=lambda r, p: _boom())
        m = FakeMsg(b'{"data": 1}', seq=77, attempt=5)   # max_deliver=5: final delivery
        with self.assertRaises(RuntimeError):
            asyncio.run(pipe.handle_message(m, WakeRoute(name="r1", subject="ci.result")))
        st = pipe.stats_snapshot()["r1"]
        self.assertEqual(st["lost"], 1)
        self.assertTrue(m.terminaled)                    # consumer closes it terminal
        import sqlite3
        with sqlite3.connect(db) as c:
            status = c.execute("SELECT status FROM wake_claims WHERE dedupe_key LIKE '%:77'").fetchone()[0]
        self.assertEqual(status, "lost")

    def test_penultimate_failure_still_redelivers(self):
        db = Path(tempfile.mkdtemp()) / "l.db"
        pipe = self._pipe(db, dispatch=lambda r, p: _boom())
        m = FakeMsg(b'{"data": 1}', seq=78, attempt=4)   # one left: NOT the final delivery
        with self.assertRaises(RuntimeError):
            asyncio.run(pipe.handle_message(m, WakeRoute(name="r1", subject="ci.result")))
        self.assertEqual(pipe.stats_snapshot()["r1"]["lost"], 0)
        self.assertFalse(m.terminaled)


def _boom():
    raise RuntimeError("dispatch failed")


class ReconnectLogging(unittest.TestCase):
    """hjs-2qk: NatsBus logs the gap and counts reconnections."""

    def test_disconnect_reconnect_cycle(self):
        from hermes_jetstream.bus import NatsBus
        bus = NatsBus(Settings(), "t")
        with self.assertLogs("hermes_jetstream.bus", level="INFO") as logs:
            asyncio.run(bus._on_disconnected())
            asyncio.run(bus._on_reconnected())
        self.assertEqual(bus.reconnect_count, 1)
        self.assertTrue(any("disconnected" in m and m.startswith("WARNING") for m in logs.output))
        self.assertTrue(any("reconnected" in m and "reconnect_count=1" in m
                            and m.startswith("INFO") for m in logs.output))

    def test_intentional_close_is_quiet(self):
        import logging
        from hermes_jetstream.bus import NatsBus
        bus = NatsBus(Settings(), "t")
        bus._closing = True
        records = []

        class _H(logging.Handler):
            def emit(self, record):
                records.append(record)

        lg = logging.getLogger("hermes_jetstream.bus")
        h = _H()
        lg.addHandler(h)
        try:
            asyncio.run(bus._on_disconnected())
        finally:
            lg.removeHandler(h)
        self.assertEqual([r for r in records if "disconnected" in r.getMessage()], [])
        self.assertEqual(bus.reconnect_count, 0)


class LifecycleTapCounters(unittest.TestCase):
    """hjs-azz: the tap counts every emit decision; failures are loud."""

    def test_emit_and_publish_counted(self):
        from hermes_jetstream.hooks import LifecycleTap, tap_stats
        tap = LifecycleTap(Settings(lifecycle_emit=["session.start"]), "p", lambda: None)
        before = tap_stats()["emitted"]
        tap._emit("session.start")(platform="tg", session_key="k")
        self.assertEqual(tap.stats()["emitted"], 1)
        self.assertGreaterEqual(tap_stats()["emitted"], before + 1)
        tap.stop()

    def test_publish_failure_counted_and_loud(self):
        from hermes_jetstream.hooks import LifecycleTap
        tap = LifecycleTap(Settings(lifecycle_emit=["session.start"]), "p", lambda: None)
        tap._emit("session.start")(platform="tg", session_key="k")

        class _DeadBus:
            loop = None
        bus = _DeadBus()
        tap.get_bus = lambda: bus
        # loop None -> the drain skips publish: must count as failed, not silently drop
        # by injecting a get_bus that raises instead:
        def _boom_bus():
            raise RuntimeError("bus gone")
        tap.get_bus = _boom_bus
        tap._stop.clear()
        t = threading.Thread(target=tap._drain, daemon=True)
        t.start()
        time.sleep(1.5)
        tap._stop.set()
        t.join(timeout=3)
        self.assertGreaterEqual(tap.stats()["failed"], 1)

    def test_oversize_dropped_counted(self):
        from hermes_jetstream.hooks import LifecycleTap
        tap = LifecycleTap(Settings(lifecycle_emit=["session.start"], max_payload_bytes=10),
                           "p", lambda: None)
        tap._emit("session.start")(platform="tg", session_key="k" * 100)
        self.assertEqual(tap.stats()["dropped"], 1)
        tap.stop()


class MonorepoDiscovery(unittest.TestCase):
    """hjs-c03: a bare-repo install publishes the clone root; plugin.yaml sits two
    levels below it and is invisible under the core's discovery depth rule, so the
    platform dies as an unhelpful None. Simulates the core's own rule (one-level
    recursion into a manifest-less dir, depth-capped) against the repo's real files."""

    ROOT = Path(__file__).resolve().parents[1]

    def _scan(self, root, depth=0):  # mirrors hermes_cli.plugins_discovery.scan_directory
        found = []
        for child in sorted(root.iterdir()):
            if not child.is_dir() or child.name.startswith((".", "__")):
                continue
            manifest = child / "plugin.yaml"
            if manifest.exists():
                found.append(manifest)
            elif depth >= 1:
                continue  # core: "no plugin.yaml, depth cap reached"
            else:
                found.extend(self._scan(child, depth + 1))
        return found

    def _published(self, unit_path):  # install publishes <unit> as <plugins_dir>/hermes-jetstream
        td = tempfile.mkdtemp()
        os.symlink(str(unit_path), str(Path(td) / "hermes-jetstream"))
        return self._scan(Path(td))

    def test_bare_repo_publish_hides_manifest(self):
        self.assertEqual(self._published(self.ROOT), [])  # substance reproduced

    def test_unit_subdir_publish_is_discoverable(self):
        found = self._published(self.ROOT / "plugin" / "hermes_jetstream")
        self.assertEqual(len(found), 1)
        text = found[0].read_text()
        self.assertRegex(text, r"(?m)^name:\s*hermes-jetstream\s*$")
        self.assertIn("python_dependencies", text)

    def test_readme_install_command_resolves_to_manifest(self):
        import re
        m = re.search(r"hermes plugins install\s+(\S+)", (self.ROOT / "README.md").read_text())
        self.assertIsNotNone(m, "README must document an install command")
        parts = m.group(1).rstrip("/").split("/")
        self.assertGreaterEqual(len(parts), 4, "identifier must carry the unit subdir")
        self.assertTrue((self.ROOT.joinpath(*parts[2:]) / "plugin.yaml").is_file())


class PlatformFailureNamesItsReason(unittest.TestCase):
    """hjs-c03: create_adapter returns None on missing deps and prints
    PlatformEntry.install_hint in that line — register() must fill it."""

    def test_register_advertises_install_hint(self):
        import hermes_jetstream
        captured = {}

        class Ctx:
            def get_config(self, key, default=None):
                return default
            def register_platform(self, **kw):
                captured.update(kw)
            def register_tool(self, **kw):
                pass
            def on_unload(self, fn):
                pass

        hermes_jetstream.register(Ctx())
        hint = captured.get("install_hint", "")
        self.assertIn("nats-py", hint)  # remedy 1: the dep itself
        self.assertIn("plugins install", hint)  # remedy 2: the unit-subdir identifier
        self.assertIsNotNone(captured.get("check_fn"))
