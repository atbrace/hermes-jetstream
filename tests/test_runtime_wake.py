"""Runtime wake routes (hjs-tr9): jetstream_wake register/unregister/list, offline.

The store, the tool verb, and the bind-side load (sweep -> parse -> allowlist) are
all pure stdlib. Adapter class construction needs the gateway (lazy by design),
so the live rebind is exercised through the real _on_bus_loop hop against a fake
adapter + fake bus loop — the same harness shape as test_core.BusLoopHop."""
from __future__ import annotations

import asyncio
import json
import tempfile
import threading
import unittest
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "plugin"))

from hermes_jetstream.config import Settings, WakeRoute
from hermes_jetstream.runtime_routes import RuntimeRouteStore, MAX_RUNTIME_ROUTES, tag_sources
from hermes_jetstream.tools import JetstreamTools


class StoreRoundtrip(unittest.TestCase):
    def setUp(self):
        self.store = RuntimeRouteStore(Path(tempfile.mkdtemp()) / "wake-routes.json")

    def _row(self, name="rt-ci", **kw):
        return {"name": name, "subject": "ci.result", "durable": f"jetstream-{name}",
                "prompt": "x", "max_payload_chars": 2000, "rate_per_min": 10,
                "min_interval_s": 0.0, "source": "runtime", "registered_at": 1.0, **kw}

    def test_put_rows_remove(self):
        self.assertEqual(self.store.rows(), {})
        self.store.put(self._row())
        self.assertEqual(list(self.store.rows()), ["rt-ci"])
        row = self.store.remove("rt-ci")
        self.assertEqual(row["subject"], "ci.result")
        self.assertIsNone(self.store.remove("rt-ci"))      # idempotent
        self.assertEqual(self.store.rows(), {})

    def test_wake_routes_parses_and_names_junk(self):
        self.store.put(self._row())
        self.store.put({"name": "rt-bad"})                 # no subject -> junk
        routes, problems = self.store.wake_routes()
        self.assertEqual([r.name for r in routes], ["rt-ci"])
        self.assertEqual([p[0] for p in problems], ["rt-bad"])

    def test_max_age_sweep(self):
        import time
        now = time.time()
        self.store.put(self._row("rt-live", expires_at=now + 600))
        self.store.put(self._row("rt-dead", expires_at=now - 1))
        self.store.put(self._row("rt-forever"))            # no expiry: never swept
        self.assertEqual(self.store.sweep(), ["rt-dead"])
        self.assertEqual(sorted(self.store.rows()), ["rt-forever", "rt-live"])
        self.assertEqual([r.name for r in self.store.wake_routes()[0]],
                         sorted(["rt-forever", "rt-live"]))
        self.assertEqual(self.store.sweep(), [])           # idempotent

    def test_corrupt_file_reads_empty(self):
        self.store._path.parent.mkdir(parents=True, exist_ok=True)
        self.store._path.write_text("not json{{{")
        self.assertEqual(self.store.rows(), {})


class _Harness(unittest.TestCase):
    """Tool verb against a tmp store; live-rebind recorded, bus never touched."""

    def setUp(self):
        import hermes_jetstream.runtime_routes as rr
        self.store = RuntimeRouteStore(Path(tempfile.mkdtemp()) / "wake-routes.json")
        self._saved_default_store = rr.default_store
        rr.default_store = lambda: self.store
        self.live = []

        tools = JetstreamTools(Settings(subscribe_allowed=["ci.>"],
                                        wake=list(getattr(self, "config_routes", []))), ctx=None)
        async def fake_live(op, arg):
            self.live.append((op, getattr(arg, "name", arg)))
        tools._live_runtime = fake_live
        self.tools = tools

    def tearDown(self):
        import hermes_jetstream.runtime_routes as rr
        rr.default_store = self._saved_default_store

    def run_async(self, coro):
        return asyncio.run(coro)


class ToolVerb(_Harness):
    def test_register_list_unregister_roundtrip(self):
        out = json.loads(self.run_async(self.tools.jetstream_wake(
            action="register", name="deploy", subject="ci.result", prompt="wake {subject}")))
        self.assertEqual(out["name"], "rt-deploy")
        self.assertEqual(out["durable"], "jetstream-rt-deploy")   # namespaced identity
        self.assertEqual(out["source"], "runtime")
        self.assertEqual(self.live, [("add", "rt-deploy")])
        listed = json.loads(self.run_async(self.tools.jetstream_wake(action="list")))
        self.assertEqual([r["name"] for r in listed], ["rt-deploy"])
        out = self.run_async(self.tools.jetstream_wake(action="unregister", name="deploy"))
        self.assertIn("Unregistered rt-deploy", out)
        self.assertEqual(self.live[-1], ("remove", "rt-deploy"))
        # re-unregister: idempotent, no ERROR
        out2 = self.run_async(self.tools.jetstream_wake(action="unregister", name="deploy"))
        self.assertNotIn("ERROR", out2)
        self.assertIn("No runtime wake routes.", self.run_async(self.tools.jetstream_wake(action="list")))

    def test_allowlist_refusal_is_loud_and_leaves_no_row(self):
        out = self.run_async(self.tools.jetstream_wake(action="register", name="s", subject="secret.thing"))
        self.assertIn("ERROR", out)
        self.assertIn("subscribe_allowed", out)
        self.assertEqual(self.store.rows(), {})
        self.assertEqual(self.live, [])

    def test_collide_with_config_route(self):
        # a config route literally named rt-ci blocks registering "ci"
        self.tools.settings.wake = [WakeRoute(name="rt-ci", subject="ci.x")]
        out = self.run_async(self.tools.jetstream_wake(action="register", name="ci", subject="ci.result"))
        self.assertIn("collides", out)
        # a config route squatting the derived durable blocks it too
        self.tools.settings.wake = [WakeRoute(name="odd", subject="ci.x", durable="jetstream-rt-y")]
        out2 = self.run_async(self.tools.jetstream_wake(action="register", name="y", subject="ci.result"))
        self.assertIn("collides", out2)
        self.assertEqual(self.store.rows(), {})

    def test_bad_name_rejected(self):
        self.assertIn("ERROR", self.run_async(self.tools.jetstream_wake(
            action="register", name="bad name", subject="ci.result")))

    def test_limit_cap_and_reregister_ok(self):
        for i in range(MAX_RUNTIME_ROUTES):
            self.assertIn("rt-r", self.run_async(self.tools.jetstream_wake(
                action="register", name=f"r{i}", subject="ci.result")))
        out = self.run_async(self.tools.jetstream_wake(action="register", name="more", subject="ci.result"))
        self.assertIn("limit", out)
        # re-register over an existing name is an update, allowed at the cap
        self.assertNotIn("ERROR", self.run_async(self.tools.jetstream_wake(
            action="register", name="r0", subject="ci.result", rate_per_min=2)))
        self.assertEqual(self.store.rows()["rt-r0"]["rate_per_min"], 2)


class BindRebind(_Harness):
    def test_rebind_after_restart_is_stable(self):
        """Register once, then 'bind' (load + sweep) twice: the store row re-binds
        identically every time, no duplicate rows, no drift."""
        self.run_async(self.tools.jetstream_wake(action="register", name="ci", subject="ci.result.*"))
        a = [r.name for r in self.store.wake_routes()[0]]
        b = [r.name for r in self.store.wake_routes()[0]]
        self.assertEqual(a, ["rt-ci"])
        self.assertEqual(a, b)
        self.assertEqual(len(self.store.rows()), 1)

    def test_expired_row_self_retires_at_bind(self):
        self.run_async(self.tools.jetstream_wake(action="register", name="ci",
                                                 subject="ci.result", max_age_s=100))
        row = self.store.rows()["rt-ci"]
        row["expires_at"] -= 200                            # simulate age passing
        self.store.put(row)
        self.assertEqual(self.store.sweep(), ["rt-ci"])     # bind-side sweep retires it
        self.assertEqual(self.store.wake_routes()[0], [])
        self.assertIn("No runtime wake routes.", self.run_async(self.tools.jetstream_wake(action="list")))


class AdapterRollback(unittest.TestCase):
    """hjs-a7i: re-register over an existing runtime route must not destroy the
    working route when the new live bind fails. Real adapter class over stubbed
    gateway imports (lazy-import design keeps this stdlib-testable)."""

    class _Sub:
        def __init__(self, tag, fail_drain=False):
            self.tag, self.drained, self.restored = tag, False, False
            self._fail = fail_drain
        async def drain(self):
            if self._fail:
                raise RuntimeError("drain refused")
            self.drained = True
        async def unsubscribe(self):
            self.drained = True

    def setUp(self):
        import types
        self._saved = {}
        for name, attrs in {
            "gateway": {},
            "gateway.config": {"Platform": lambda *a, **k: "jetstream"},
            "gateway.platforms": {},
            "gateway.platforms.base": {"BasePlatformAdapter": type("B", (), {"__init__": lambda s, c, p: None})},
        }.items():
            self._saved[name] = sys.modules.get(name)
            mod = types.ModuleType(name)
            for k, v in attrs.items():
                setattr(mod, k, v)
            sys.modules[name] = mod
        import asyncio as aio
        self.loop = aio.new_event_loop()
        from hermes_jetstream.adapter import build_adapter
        self.adapter = build_adapter(object(), Settings())

        class _P:
            @staticmethod
            def _stat(*a, **k):
                pass
        self.adapter._pipeline = _P()  # connected-guard passes; bind path only seeds stats

    def tearDown(self):
        for name, prev in self._saved.items():
            if prev is None:
                del sys.modules[name]
            else:
                sys.modules[name] = prev
        import hermes_jetstream.adapter as ad
        ad._ACTIVE_CLASS.clear()  # stub-built class must not leak to later tests
        self.loop.close()

    def _bus(self, fail_bind=False):
        class Bus:
            connected = True
            loop = None
            def __init__(self):
                self.subs = []
            async def subscribe_wake(self, *, subject, queue, on_message):
                if fail_bind:
                    raise RuntimeError("consumer refused")
                sub = AdapterRollback._Sub(subject)
                self.subs.append(sub)
                return sub
        bus = Bus()
        bus.loop = self.loop
        self.adapter._bus = bus
        return bus

    def _route(self, subject):
        return WakeRoute.from_dict({"name": "rt-x", "subject": subject})

    def test_failed_rebind_restores_prior_row_and_sub(self):
        bus = self._bus()
        aio_run = __import__("asyncio").run
        aio_run(self.adapter.add_runtime_route(self._route("ci.old")))
        prior = self.adapter._runtime_subs["rt-x"]
        bus2 = self._bus(fail_bind=True)  # new bus refuses the rebind
        with self.assertRaises(RuntimeError):
            aio_run(self.adapter.add_runtime_route(self._route("ci.new")))
        self.assertIs(self.adapter._runtime_subs["rt-x"], prior)  # old sub restored, not deleted
        self.assertFalse(prior.drained)                            # old sub never drained
        self.assertIn("rt-x", self.adapter._runtime_names)

    def test_successful_rebind_drains_prior_sub(self):
        bus = self._bus()
        import asyncio as aio
        aio.run(self.adapter.add_runtime_route(self._route("ci.old")))
        prior = self.adapter._runtime_subs["rt-x"]
        aio.run(self.adapter.add_runtime_route(self._route("ci.new")))
        self.assertTrue(prior.drained)   # no double-consumption of the same durable
        self.assertIsNot(self.adapter._runtime_subs["rt-x"], prior)

    def test_prior_drain_failure_never_orphans_two_subs(self):
        bus = self._bus()
        import asyncio as aio
        aio.run(self.adapter.add_runtime_route(self._route("ci.old")))
        prior = self.adapter._runtime_subs["rt-x"]
        prior._fail = True
        aio.run(self.adapter.add_runtime_route(self._route("ci.new")))
        # old sub could not be drained: the new sub is dropped, old stays live
        self.assertIs(self.adapter._runtime_subs["rt-x"], prior)
        self.assertFalse(prior.drained)
        self.assertTrue(bus.subs[-1].drained)   # the new sub was unsubscribed away


class SweepClock(unittest.TestCase):
    """hjs-plm: fresh-but-never-swept must not read as idle — status carries
    last_sweep_ts, 0.0 until the sweep loop completes its first pass."""

    def setUp(self):
        import types
        self._saved = {}
        for name, attrs in {
            "gateway": {},
            "gateway.config": {"Platform": lambda *a, **k: "jetstream"},
            "gateway.platforms": {},
            "gateway.platforms.base": {"BasePlatformAdapter": type("B", (), {"__init__": lambda s, c, p: None})},
        }.items():
            self._saved[name] = sys.modules.get(name)
            mod = types.ModuleType(name)
            for k, v in attrs.items():
                setattr(mod, k, v)
            sys.modules[name] = mod
        from hermes_jetstream.adapter import build_adapter
        self.adapter = build_adapter(object(), Settings())

    def tearDown(self):
        for name, prev in self._saved.items():
            if prev is None:
                del sys.modules[name]
            else:
                sys.modules[name] = prev
        import hermes_jetstream.adapter as ad
        ad._ACTIVE_CLASS.clear()

    def test_zero_until_first_pass(self):
        import asyncio

        class Ledger:
            pruned = 0
            def prune(self):
                Ledger.pruned += 1
        self.adapter._ledger = Ledger()
        self.assertEqual(self.adapter.status()["last_sweep_ts"], 0.0)
        asyncio.run(self.adapter._sweep_once())
        self.assertGreater(self.adapter.status()["last_sweep_ts"], 0.0)
        self.assertEqual(Ledger.pruned, 1)  # the pass really ran the body


class StatusSource(unittest.TestCase):
    def test_tag_sources(self):
        snap = {"cfg": {"dispatched": 1}, "rt-x": {"dispatched": 0}}
        out = tag_sources(snap, {"rt-x"})
        self.assertEqual(out["cfg"]["source"], "config")
        self.assertEqual(out["rt-x"]["source"], "runtime")


class LiveRebindHarness(unittest.TestCase):
    """The real tools._live_runtime path: hop onto the adapter's bus loop."""

    class _FakeSub:
        def __init__(self):
            self.drained = False
        async def drain(self):
            self.drained = True

    class _FakeBus:
        connected = True
        def __init__(self, loop):
            self._loop = loop
        @property
        def loop(self):
            return self._loop

    class _FakeAdapter:
        def __init__(self, loop):
            self._bus = LiveRebindHarness._FakeBus(loop)
            self.added, self.removed = [], []
        async def add_runtime_route(self, route):
            self.added.append(route.name)
        async def remove_runtime_route(self, name):
            self.removed.append(name)

    def setUp(self):
        import hermes_jetstream.adapter as ad
        import hermes_jetstream.runtime_routes as rr
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self.loop.run_forever, daemon=True)
        self.thread.start()
        self.adapter = self._FakeAdapter(self.loop)
        self._saved = ad.active_adapters
        ad.active_adapters = lambda: [self.adapter]
        self.store = RuntimeRouteStore(Path(tempfile.mkdtemp()) / "wake-routes.json")
        self._saved_ds = rr.default_store
        rr.default_store = lambda: self.store
        self.tools = JetstreamTools(Settings(subscribe_allowed=["ci.>"]), ctx=None)

    def tearDown(self):
        import hermes_jetstream.adapter as ad
        import hermes_jetstream.runtime_routes as rr
        ad.active_adapters = self._saved
        rr.default_store = self._saved_ds
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join(timeout=2)
        self.loop.close()

    def test_register_and_unregister_hop_to_bus_loop(self):
        out = json.loads(asyncio.run(self.tools.jetstream_wake(
            action="register", name="ci", subject="ci.result")))
        self.assertEqual(self.adapter.added, ["rt-ci"])
        self.assertEqual(out["name"], "rt-ci")
        asyncio.run(self.tools.jetstream_wake(action="unregister", name="ci"))
        self.assertEqual(self.adapter.removed, ["rt-ci"])

    def test_live_bind_failure_rolls_back_the_row(self):
        class Boom(self._FakeAdapter):
            async def add_runtime_route(self, route):
                raise RuntimeError("consumer refused")
        self.adapter = Boom(self.loop)
        import hermes_jetstream.adapter as ad
        ad.active_adapters = lambda: [self.adapter]
        out = asyncio.run(self.tools.jetstream_wake(action="register", name="ci", subject="ci.result"))
        self.assertIn("ERROR", out)
        self.assertEqual(self.store.rows(), {})   # no stray row behind a failed bind

    def test_failed_re_register_restores_the_prior_row(self):
        """hjs-a7i: update over a live route that fails must keep the OLD route."""
        asyncio.run(self.tools.jetstream_wake(action="register", name="ci", subject="ci.result"))
        class Boom(self._FakeAdapter):
            async def add_runtime_route(self, route):
                raise RuntimeError("consumer refused")
        self.adapter = Boom(self.loop)
        import hermes_jetstream.adapter as ad
        ad.active_adapters = lambda: [self.adapter]
        out = asyncio.run(self.tools.jetstream_wake(action="register", name="ci", subject="ci.other"))
        self.assertIn("ERROR", out)
        self.assertEqual(self.store.rows()["rt-ci"]["subject"], "ci.result")  # prior row stands
        self.assertNotIn("rt-ci", self.adapter.removed)  # prior live sub never torn down


if __name__ == "__main__":
    unittest.main(verbosity=2)
