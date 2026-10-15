"""Agent-facing tools: nats_publish (emit) and nats_fetch (bounded server-held pull).

Both work from any process: prefer the gateway's live connection (adapter registry),
fall back to an ephemeral connect/close per call so CLI/cron sessions can emit too.
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Dict, Optional

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


class JetstreamTools:
    def __init__(self, settings, ctx):
        self.settings = settings
        self.publish_policy = SubjectPolicy(settings.publish_allowed)
        self.fetch_policy = SubjectPolicy(settings.subscribe_allowed)

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
            return await bus.publish(subject, body, headers={"Nats-Msg-Id": _msg_id(body)})
        return await self._ephemeral(lambda js: js.publish(subject, body, timeout=5.0,
                                                          headers={"Nats-Msg-Id": _msg_id(body)}))

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
        bus = next((b for b in _active_bus() if b.connected), None)
        if bus is not None:
            return await bus.fetch(subject, timeout=timeout, batch=batch)
        async def _go(js):
            psub = await js.pull_subscribe(subject, stream=self.settings.stream)
            try:
                from nats.errors import TimeoutError as NT
                try:
                    return await psub.fetch(batch, timeout=timeout)
                except NT:
                    return []
            finally:
                await psub.unsubscribe()
        return await self._ephemeral(_go)

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


def _msg_id(body: bytes) -> str:
    import hashlib
    return "hermes-" + hashlib.sha256(body).hexdigest()[:32]


def register_tools(ctx) -> None:
    holder: Dict[str, JetstreamTools] = {}

    def _tools() -> JetstreamTools:
        if "t" not in holder:
            from .config import Settings
            holder["t"] = JetstreamTools(Settings.from_plugin_ctx(ctx), ctx)
        return holder["t"]

    ctx.register_tool(
        name="nats_publish", toolset="jetstream", schema=PUBLISH_SCHEMA,
        handler=lambda args, **kw: _run_async(_tools().nats_publish(**args)),
        is_async=False, emoji="📡",
        description="Publish a JSON event to a NATS JetStream subject (allowlisted).")
    ctx.register_tool(
        name="nats_fetch", toolset="jetstream", schema=FETCH_SCHEMA,
        handler=lambda args, **kw: _run_async(_tools().nats_fetch(**args)),
        is_async=False, emoji="📡",
        description="Server-held pull of events from a NATS JetStream subject (bounded wait, allowlisted).")


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
