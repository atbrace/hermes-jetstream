"""Lifecycle tap: mirror Hermes plugin-hook events onto <subject_prefix>.<kind> (default hermes.<kind>).

Fire-and-forget with a bounded queue (agent/outbound_webhooks.py pattern): a slow or dead bus
drops events, it never blocks a hook callback. Privacy: per-kind FIELD WHITELISTS — tool bodies,
message text, and paths never enter the bus; opt-in via settings lifecycle.emit.
"""
from __future__ import annotations

import json
import logging
import queue
import threading
from typing import Any, Dict, Optional

from .envelope import build_envelope

logger = logging.getLogger("hermes_jetstream.hooks")

# kind -> hook name -> allowed payload fields (never bodies/text/paths; shell_hooks payload shape)
_TAPS = {
    "session.start": ("on_session_start", ("platform", "session_id", "session_key", "user_id")),
    "session.end":   ("on_session_end",   ("platform", "session_id", "session_key")),
    "session.reset": ("on_session_reset", ("platform", "session_key")),
    "agent.turn.start": ("pre_gateway_dispatch", ("platform", "user_id", "session_key")),
    "subagent.stop": ("subagent_stop",  ("task_id", "parent_session", "outcome")),
    "agent.stopped": ("agent_loop_stopped", ("session_key", "platform", "reason")),
}

_QUEUE_CAP = 256


def pick_fields(kind: str, kwargs: Dict[str, Any]) -> Dict[str, Any]:
    """Whitelist projection; unknown kinds project to {} (fail-closed privacy)."""
    allowed = _TAPS.get(kind, (None, ()))[1]
    return {k: kwargs[k] for k in allowed if k in kwargs}


class LifecycleTap:
    def __init__(self, settings, profile_name: str, get_bus):
        self.settings = settings
        self.profile = profile_name
        self.get_bus = get_bus            # () -> connected NatsBus | None
        self.prefix = settings.lifecycle_prefix
        self.q: "queue.Queue[tuple[str, bytes]]" = queue.Queue(maxsize=_QUEUE_CAP)
        self._stop = threading.Event()
        self._worker: Optional[threading.Thread] = None

    def start(self) -> None:
        if self._worker is not None:
            return
        self._worker = threading.Thread(target=self._drain, name="hjs-lifecycle-tap", daemon=True)
        self._worker.start()

    def stop(self) -> None:
        self._stop.set()

    def _emit(self, kind: str):
        def _handler(**kwargs):
            try:
                data = pick_fields(kind, kwargs)
                body = json.dumps(build_envelope(kind, data, profile=self.profile,
                                                 source="lifecycle-tap"), default=str).encode()
                if len(body) > self.settings.max_payload_bytes:
                    return
                self.q.put_nowait((f"{self.prefix}.{kind}", body))  # drops when bus is behind
            except Exception:
                logger.debug("[jetstream] lifecycle tap drop kind=%s", kind, exc_info=True)
        return _handler

    def _drain(self) -> None:
        import asyncio
        while not self._stop.is_set():
            try:
                subject, body = self.q.get(timeout=1.0)
            except queue.Empty:
                continue
            try:
                bus = self.get_bus()
                if bus is None:
                    continue  # no bus in this process: lifecycle events stay in-process
                loop = bus.loop
                if loop is not None:
                    import asyncio as _a
                    fut = _a.run_coroutine_threadsafe(bus.publish(subject, body), loop)
                    fut.result(timeout=5)
            except Exception:
                logger.debug("[jetstream] lifecycle publish dropped subject=%s", subject)


def register_hooks(ctx, settings, profile_name: str, get_bus) -> None:
    if not settings.lifecycle_emit:
        return
    tap = LifecycleTap(settings, profile_name, get_bus)
    enabled = set(settings.lifecycle_emit)
    unknown = sorted(enabled - set(_TAPS))
    if unknown:
        logger.warning("[jetstream] lifecycle_emit: unknown kind(s) ignored: %s", ", ".join(unknown))
    for kind, (hook_name, _fields) in _TAPS.items():
        if kind in enabled:
            ctx.register_hook(hook_name, tap._emit(kind))
    tap.start()
    ctx.on_unload(tap.stop)
