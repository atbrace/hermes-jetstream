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
_ACTIVE_TAPS: list = []
_TAPS_LOCK = threading.Lock()


def tap_stats() -> Dict[str, int]:
    """Summed counters across every LifecycleTap in this process (hjs-azz)."""
    with _TAPS_LOCK:
        taps = list(_ACTIVE_TAPS)
    out: Dict[str, int] = {}
    for t in taps:
        for k, v in t.stats().items():
            out[k] = out.get(k, 0) + v
    return out


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
        # hjs-azz: a fire-and-forget tap that silently drops is the same lie as an
        # erroring probe reading clean — every emit decision is counted.
        self._c_lock = threading.Lock()
        self._c = {"emitted": 0, "dropped": 0, "published": 0, "failed": 0}
        with _TAPS_LOCK:
            _ACTIVE_TAPS.append(self)

    def start(self) -> None:
        if self._worker is not None:
            return
        self._worker = threading.Thread(target=self._drain, name="hjs-lifecycle-tap", daemon=True)
        self._worker.start()

    def stop(self) -> None:
        self._stop.set()

    def stats(self) -> Dict[str, int]:
        with self._c_lock:
            return dict(self._c)

    def _count(self, name: str) -> None:
        with self._c_lock:
            self._c[name] += 1

    def _emit(self, kind: str):
        def _handler(**kwargs):
            try:
                data = pick_fields(kind, kwargs)
                body = json.dumps(build_envelope(kind, data, profile=self.profile,
                                                 source="lifecycle-tap"), default=str).encode()
                if len(body) > self.settings.max_payload_bytes:
                    self._count("dropped")
                    return
                self.q.put_nowait((f"{self.prefix}.{kind}", body))  # drops when bus is behind
                self._count("emitted")
            except queue.Full:
                self._count("dropped")
                logger.warning("[jetstream] lifecycle tap queue full (cap %d) — dropped %s",
                               _QUEUE_CAP, kind)
            except Exception:
                self._count("dropped")
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
                if bus is None or bus.loop is None:
                    self._count("failed")
                    continue  # no connected bus in this process: events stay in-process
                import asyncio as _a
                fut = _a.run_coroutine_threadsafe(bus.publish(subject, body), bus.loop)
                fut.result(timeout=5)
                self._count("published")
            except Exception:
                self._count("failed")
                logger.warning("[jetstream] lifecycle publish FAILED subject=%s", subject)


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
