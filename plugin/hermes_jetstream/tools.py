"""Agent-facing tools: nats_publish (emit) and nats_fetch (bounded server-held pull).

Both work from any process: prefer the gateway's live connection (adapter registry),
fall back to an ephemeral connect/close per call so CLI/cron sessions can emit too.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any, Dict, Optional

from .config import WakeRoute
from .envelope import build_envelope, find_secret_like
from .subject import SubjectPolicy, validate_subject_literal

logger = logging.getLogger("hermes_jetstream.tools")

PUBLISH_SCHEMA = {
    "type": "object",
    "properties": {
        "subject": {"type": "string", "description": "Literal JetStream subject (no wildcards); must match the plugin publish allowlist."},
        "payload": {"description": "JSON payload (object/string). Keep thin: events, not transcripts."},
        "kind": {"type": "string", "description": "Event kind label for the envelope (e.g. 'ci-result', 'note')."},
    },
    "required": ["subject", "payload"],
}

FETCH_SCHEMA = {
    "type": "object",
    "properties": {
        "subject": {"type": "string", "description": "Literal subject or one wildcard pattern; must match the plugin subscribe allowlist."},
        "timeout_seconds": {"type": "number", "description": "Server-held wait, 1–300s (default 30). One fetch, not a poll loop."},
        "batch": {"type": "number", "description": "Max messages (1-10, default 1)."},
    },
    "required": ["subject"],
}


def _active_bus():
    """Live bus from a connected adapter in THIS process, if any."""
    try:
        from .adapter import active_buses
        return active_buses()
    except Exception:
        return []


class BusLoopGone(Exception):
    """Live bus lost its loop between pick and use — caller falls back to ephemeral."""


async def _on_bus_loop(bus, coro_factory):
    """Run a bus call on the bus's OWN loop. Tool handlers reach us via _run_async:
    from a gateway worker thread asyncio.run makes a foreign loop where the
    connection's futures never resolve (publish timed out at 5.00s every time while
    the message still hit the wire — a retry could duplicate). Same loop: await
    directly; else hop with run_coroutine_threadsafe + wrap_future (a loop is always
    running in our thread: _run_async drives us under asyncio.run or create_task)."""
    loop = bus.loop
    if loop is None or not loop.is_running():
        raise BusLoopGone()
    if asyncio.get_running_loop() is loop:
        return await coro_factory()
    try:
        fut = asyncio.run_coroutine_threadsafe(coro_factory(), loop)
    except RuntimeError:
        raise BusLoopGone()  # loop stopped between check and submit
    return await asyncio.wrap_future(fut)


class JetstreamTools:
    def __init__(self, settings, ctx):
        self.settings = settings
        self.publish_policy = SubjectPolicy(settings.publish_allowed)
        self.fetch_policy = SubjectPolicy(settings.subscribe_allowed)
        # hjs-9um: per-subject in-process resume cursor (last stream seq returned).
        self._fetch_cursor: Dict[str, int] = {}

    def _resume_cfg(self, subject: str):
        """Ephemeral consumers default to DeliverPolicy.ALL, so a second fetch re-read
        what the first returned and acked. First call reads the subject tail (documented
        semantics); later calls resume after the cursor."""
        seq = self._fetch_cursor.get(subject)
        if seq is None:
            return None
        from nats.js.api import ConsumerConfig, DeliverPolicy
        return ConsumerConfig(deliver_policy=DeliverPolicy.BY_START_SEQUENCE,
                              opt_start_seq=seq + 1)

    def _advance_cursor(self, subject: str, msgs) -> None:
        try:
            last = max(m.metadata.sequence.stream for m in msgs)
        except Exception:
            return
        self._fetch_cursor[subject] = max(self._fetch_cursor.get(subject, 0), last)

    def _registry(self):
        """In-session watches live in the gateway adapter; tools reach them via the
        adapter registry. No adapter (CLI/cron process) = no watches: use nats_fetch."""
        try:
            from .adapter import active_watch_registries
            regs = active_watch_registries()
            return regs[0] if regs else None
        except Exception:
            return None

    # -- publish -----------------------------------------------------------
    async def nats_publish(self, subject: str, payload: Any, kind: str = "event", **_) -> str:
        subject = (subject or "").strip()
        try:
            validate_subject_literal(subject, "publish")
        except ValueError as exc:
            return f"ERROR: {exc}"
        if not self.publish_policy.allows(subject):
            return f"ERROR: {self.publish_policy.deny_message(subject, 'publish_allowed')}"
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except ValueError:
                pass  # plain strings are legal payloads
        leak = find_secret_like(payload)
        if leak:
            return "ERROR: payload refused — secret-shaped content detected; emit a thin event referencing the data instead."
        body = json.dumps(build_envelope(kind, payload), default=str).encode("utf-8")
        if len(body) > self.settings.max_payload_bytes:
            return f"ERROR: envelope {len(body)}B exceeds max_payload_bytes={self.settings.max_payload_bytes}"
        try:
            ack = await self._publish(subject, body)
        except Exception as exc:
            return f"ERROR: publish failed: {exc}"
        stream = getattr(ack, "stream", "?")
        seq = getattr(ack, "seq", "?")
        dup = " (duplicate, deduped by server)" if getattr(ack, "duplicate", False) else ""
        return f"Published {len(body)}B to {subject} [stream={stream} seq={seq}]{dup}"

    async def _publish(self, subject: str, body: bytes):
        bus = next((b for b in _active_bus() if b.connected), None)
        if bus is not None:
            try:
                return await _on_bus_loop(bus, lambda: bus.publish(
                    subject, body, headers={"Nats-Msg-Id": _msg_id(subject, body)}))
            except BusLoopGone:
                pass  # connection vanished between pick and use: ephemeral path below
        return await self._ephemeral(lambda js: js.publish(subject, body, timeout=5.0,
                                                headers={"Nats-Msg-Id": _msg_id(subject, body)}))

    # -- fetch -------------------------------------------------------------
    async def nats_fetch(self, subject: str, timeout_seconds: float = 30.0, batch: int = 1, **_) -> str:
        subject = (subject or "").strip()
        if not self.fetch_policy.allows(subject):
            return f"ERROR: {self.fetch_policy.deny_message(subject, 'subscribe_allowed')}"
        timeout_seconds = min(300.0, max(1.0, float(timeout_seconds)))  # MCP 300s cap precedent
        batch = min(10, max(1, int(batch)))
        try:
            msgs = await self._fetch(subject, timeout_seconds, batch)
        except Exception as exc:
            return f"ERROR: fetch failed: {exc}"
        if not msgs:
            return f"No messages on {subject} within {timeout_seconds:.0f}s."
        self._advance_cursor(subject, msgs)
        out = []
        for m in msgs:
            try:
                await m.ack()
            except Exception:
                pass
            try:
                out.append(json.loads(m.data.decode("utf-8", "replace")))
            except ValueError:
                out.append(m.data.decode("utf-8", "replace"))
        return json.dumps(out, default=str)[:16000]

    async def _fetch(self, subject: str, timeout: float, batch: int):
        cfg = self._resume_cfg(subject)
        bus = next((b for b in _active_bus() if b.connected), None)
        if bus is not None:
            try:
                return await _on_bus_loop(bus, lambda: bus.fetch(subject, timeout=timeout,
                                                                 batch=batch, config=cfg))
            except BusLoopGone:
                pass  # connection vanished between pick and use: ephemeral path below

        async def _go(js):
            psub = await js.pull_subscribe(subject, stream=self.settings.stream, config=cfg)
            try:
                from nats.errors import TimeoutError as NT
                try:
                    return await psub.fetch(batch, timeout=timeout)
                except NT:
                    return []
            finally:
                await psub.unsubscribe()
        return await self._ephemeral(_go)

    # -- runtime wake routes (hjs-tr9) ---------------------------------------
    async def jetstream_wake(self, action: str = "list", name: str = "", subject: str = "",
                             prompt: Optional[str] = None, rate_per_min=None,
                             min_interval_s=None, max_age_s=None, **_) -> str:
        """Durable wake routes at runtime: persisted plugin-side, live-rebound when a
        gateway adapter is connected, otherwise bound at the next platform bind."""
        from .runtime_routes import default_store, NAME_PREFIX, MAX_RUNTIME_ROUTES
        store = default_store()
        action = (action or "list").strip().lower()
        if action == "list":
            rows = store.rows()
            if not rows:
                return "No runtime wake routes."
            return json.dumps([rows[k] for k in sorted(rows)], default=str)
        nm = (name or "").strip()
        if not nm:
            return "ERROR: name required"
        nm = nm if nm.startswith(NAME_PREFIX) else NAME_PREFIX + nm
        if action == "unregister":
            row = store.remove(nm)
            await self._live_runtime("remove", nm)  # idempotent: no-op if not live here
            return (f"Unregistered {nm}.\n" + json.dumps(row, default=str) if row
                    else f"No runtime route {nm} (nothing to remove).")
        if action != "register":
            return f"ERROR: unknown action {action!r} (register|unregister|list)"
        try:
            route = WakeRoute.from_dict({"name": nm, "subject": subject,
                                         **({"prompt": prompt} if prompt else {}),
                                         **({} if rate_per_min is None else {"rate_per_min": rate_per_min}),
                                         **({} if min_interval_s is None else {"min_interval_s": min_interval_s})})
        except ValueError as exc:
            return f"ERROR: {exc}"
        if not self.fetch_policy.allows(route.subject):  # same allowlist as fetch/watch/wake
            return f"ERROR: {self.fetch_policy.deny_message(route.subject, 'subscribe_allowed')}"
        for r in self.settings.wake:  # namespaced, but an explicit config durable may still squat
            if r.name == nm or r.durable == route.durable:
                return f"ERROR: {nm} collides with config route {r.name!r}; pick another name"
        rows = store.rows()
        if nm not in rows and len(rows) >= MAX_RUNTIME_ROUTES:
            return f"ERROR: runtime route limit reached ({MAX_RUNTIME_ROUTES}); jetstream_wake unregister first"
        row = {"name": route.name, "subject": route.subject, "durable": route.durable,
               "prompt": route.prompt, "max_payload_chars": route.max_payload_chars,
               "rate_per_min": route.rate_per_min, "min_interval_s": route.min_interval_s,
               "source": "runtime", "registered_at": time.time()}
        if max_age_s is not None:
            age = min(30 * 86400, max(1.0, float(max_age_s)))
            row["max_age_s"] = age
            row["expires_at"] = row["registered_at"] + age
        store.put(row)
        try:
            await self._live_runtime("add", route)
        except Exception as exc:
            store.remove(nm)  # rollback: a refused/failed live bind never leaves a stray row
            return f"ERROR: live bind failed: {exc}"
        return json.dumps(row, default=str)

    async def _live_runtime(self, op: str, arg) -> None:
        """Rebind adapters hosted in THIS process (CLI/cron has none: the JSON store
        still stands, the routes bind at next platform start). Each adapter's work
        runs on its own bus loop — same hop law as _on_bus_loop publishing."""
        from .adapter import active_adapters
        for a in active_adapters():
            bus = getattr(a, "_bus", None)
            if bus is None or not bus.connected:
                continue  # not bound yet: connect() picks the store row up itself
            if op == "add":
                await _on_bus_loop(bus, lambda a=a: a.add_runtime_route(arg))
            else:
                await _on_bus_loop(bus, lambda a=a: a.remove_runtime_route(arg))

    # -- plumbing ----------------------------------------------------------
    async def _ephemeral(self, fn):
        """Direct connect for processes without the gateway adapter (CLI tool calls)."""
        from .bus import NatsBus
        bus = NatsBus(self.settings, "ephemeral")
        try:
            await bus.connect()
            return await fn(bus.jetstream)
        finally:
            await bus.stop()


def _msg_id(subject: str, body: bytes) -> str:
    # Subject-salted: JetStream dedupes Nats-Msg-Id per STREAM, so an unsalted
    # id shared across subjects on one stream would silently drop the second publish.
    import hashlib
    return "hermes-" + hashlib.sha256(subject.encode("utf-8") + b"\x00" + body).hexdigest()[:32]


WATCH_SCHEMA = {
    "type": "object",
    "properties": {
        "subject": {"type": "string", "description": "Subject or wildcard pattern to watch (literal, *, or trailing >); must match subscribe_allowed."},
        "note": {"type": "string", "description": "Short reminder attached to every injected wake (why you're watching)."},
        "ttl_seconds": {"type": "number", "description": "Auto-expire seconds (default 3600, max 21600)."},
        "max_msgs": {"type": "number", "description": "Stop after N injected events (0 = until TTL)."},
    },
    "required": ["subject"],
}

UNWATCH_SCHEMA = {
    "type": "object",
    "properties": {"watch_id": {"type": "string", "description": "Watch id from jetstream_watch."}},
    "required": ["watch_id"],
}

WAKE_SCHEMA = {
    "type": "object",
    "properties": {
        "action": {"type": "string", "enum": ["register", "unregister", "list"],
                   "description": "Verb (default: list)."},
        "name": {"type": "string", "description": "Route name; stored and reported as rt-<name> (namespaced away from config routes). Required for register/unregister."},
        "subject": {"type": "string", "description": "register: subject/wildcard to wake on; must match subscribe_allowed (refused loudly otherwise)."},
        "prompt": {"type": "string", "description": "register: wake prompt template, single-brace {subject} {payload} {seq} only."},
        "rate_per_min": {"type": "number", "description": "register: per-route token bucket (default 10); excess is shed."},
        "min_interval_s": {"type": "number", "description": "register: coalescing window, events closer are dropped."},
        "max_age_s": {"type": "number", "description": "register: auto-retire after this age (swept at platform bind); omit = never expires."},
    },
}


def register_tools(ctx) -> None:
    holder: Dict[str, JetstreamTools] = {}

    def _tools() -> JetstreamTools:
        if "t" not in holder:
            from .config import Settings
            holder["t"] = JetstreamTools(Settings.from_plugin_ctx(ctx), ctx)
        return holder["t"]

    def _bound(coro_factory):
        """Model-facing handler wrapper: a mis-guessed kwarg returns a schema hint,
        not a raw TypeError traceback (hjs-fso: agents guess data= for payload=)."""
        def _h(args, **kw):
            try:
                return _run_async(coro_factory(args))
            except TypeError as exc:
                return f"ERROR: bad tool arguments: {exc}"
        return _h

    ctx.register_tool(
        name="nats_publish", toolset="jetstream", schema=PUBLISH_SCHEMA,
        handler=_bound(lambda a: _tools().nats_publish(**a)),
        is_async=False, emoji="📡",
        description="Publish a JSON event to a NATS JetStream subject (allowlisted).")
    ctx.register_tool(
        name="nats_fetch", toolset="jetstream", schema=FETCH_SCHEMA,
        handler=_bound(lambda a: _tools().nats_fetch(**a)),
        is_async=False, emoji="📡",
        description="Server-held pull of events from a NATS JetStream subject (bounded wait, allowlisted).")

    def _watch(args, kw):
        # dispatch binds session_id per call (model_tools._execute_tool).
        reg = _tools()._registry()
        if reg is None:
            return "ERROR: no live gateway adapter — watches need the gateway process; use nats_fetch here."
        sid = str(kw.get("session_id") or kw.get("task_id") or "")
        return _run_async(reg.add(args.get("subject", ""), sid,
                                   note=args.get("note", ""),
                                   ttl_s=args.get("ttl_seconds"),
                                   max_msgs=args.get("max_msgs", 0)))

    def _unwatch(args, kw):
        reg = _tools()._registry()
        if reg is None:
            return "ERROR: no live gateway adapter."
        return _run_async(reg.remove(args.get("watch_id", "")))

    def _list(args, kw):
        reg = _tools()._registry()
        return reg.list_lines() if reg is not None else "No live gateway adapter."

    ctx.register_tool(
        name="jetstream_watch", toolset="jetstream", schema=WATCH_SCHEMA,
        handler=lambda args, **kw: _watch(args, kw),
        is_async=False, emoji="👁",
        description="Watch a NATS JetStream subject for this session: matching events are injected as turns (no polling). TTL-bounded.")
    ctx.register_tool(
        name="jetstream_unwatch", toolset="jetstream", schema=UNWATCH_SCHEMA,
        handler=lambda args, **kw: _unwatch(args, kw),
        is_async=False, emoji="👁",
        description="Stop a jetstream_watch subscription by id.")
    ctx.register_tool(
        name="jetstream_list", toolset="jetstream", schema={"type": "object", "properties": {}},
        handler=lambda args, **kw: _list(args, kw),
        is_async=False, emoji="📡",
        description="List active JetStream watches (id, subject, remaining TTL).")

    ctx.register_tool(
        name="jetstream_wake", toolset="jetstream", schema=WAKE_SCHEMA,
        handler=_bound(lambda a: _tools().jetstream_wake(**a)),
        is_async=False, emoji="📡",
        description="Register/unregister/list DURABLE wake routes at runtime (no gateway restart). Same gates as config wake routes: subject must match subscribe_allowed (refused loudly, never silently), rate/coalesce rails apply. Namespaced rt-<name>, persisted in the plugin data dir, live-bound when the gateway adapter is up and rebound at every platform bind; max_age_s self-retires abandoned routes. Output: the route row(s).")

    def _status(args, kw):
        try:
            from .adapter import active_status
            snaps = active_status()
        except Exception:
            snaps = []
        if not snaps:
            return ("No live gateway adapter in this process (CLI/cron). The platform's "
                    "status surface lives in the gateway; check gateway_state.json "
                    "platforms.jetstream.metrics instead.")
        return json.dumps(snaps, default=str, indent=1)

    ctx.register_tool(
        name="jetstream_status", toolset="jetstream", schema={"type": "object", "properties": {}},
        handler=lambda args, **kw: _status(args, kw),
        is_async=False, emoji="📡",
        description="JetStream platform health: connection, per-route wake counters (dispatched/shed/coalesced/skipped/failed/lost) with source=config|runtime, reconnect_count, lifecycle tap counters, last-attempt/last-dispatch clocks, active watches, last error. Answers 'is my watch alive?' and 'did I lose a wake?' without external monitoring.")


def _run_async(coro):
    """Tool handlers run sync; hop onto the running loop when inside one, else drive one."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    if loop is not None:
        import concurrent.futures
        fut = concurrent.futures.Future()
        async def _drive():
            try:
                fut.set_result(await coro)
            except BaseException as e:
                fut.set_exception(e)
        loop.create_task(_drive())
        return fut.result(timeout=330)
    return asyncio.run(coro)
