"""Pillar 1: ad-hoc in-session subscriptions (jetstream_watch / _unwatch / _list).

An agent calls jetstream_watch(subject, note) mid-session; the gateway opens an
ephemeral push subscription on the bound stream and, on each matching message,
injects a rendered wake into the CALLING session via the consent-gated
ctx.inject_message rail (same path as webhook injection — the plugin never
bypasses user consent). Subscriptions are in-process, TTL-bounded, and swept:
they die with the gateway, never outlive max_ttl_s. nats_fetch remains the
portable fallback for non-gateway processes.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from .envelope import parse_envelope, render_prompt
from .subject import SubjectPolicy

logger = logging.getLogger("hermes_jetstream.watch")

WATCH_PROMPT = 'JetStream event on {subject}: {payload}'


@dataclass
class Watch:
    id: str
    subject: str          # literal or wildcard, allowlist-checked at creation
    session_id: str
    note: str
    expires: float
    max_msgs: int
    seen: int = 0
    sub: Any = field(default=None, repr=False)

    @property
    def expired(self) -> bool:
        return bool(time.time() > self.expires or (self.max_msgs and self.seen >= self.max_msgs))


class WatchRegistry:
    """Ad-hoc watches owned by the adapter (gateway loop). Pure logic is unit-testable;
    NATS wiring is injected via `open_sub`/`close_sub` callables."""

    def __init__(self, *, policy: SubjectPolicy, open_sub, close_sub, inject,
                 max_watches: int = 16, default_ttl_s: float = 3600,
                 max_ttl_s: float = 21600, max_payload_chars: int = 700):
        self.policy = policy
        self.open_sub = open_sub        # async (subject, cb) -> sub
        self.close_sub = close_sub      # async (sub) -> None
        self.inject = inject            # (session_id, text) -> bool
        self.max_watches = max_watches
        self.default_ttl_s = default_ttl_s
        self.max_ttl_s = max_ttl_s
        self.max_payload_chars = max_payload_chars
        self._watches: Dict[str, Watch] = {}
        self._seq = 0

    def count(self) -> int:
        return len(self._watches)

    async def add(self, subject: str, session_id: str, note: str = "",
                  ttl_s: Optional[float] = None, max_msgs: int = 0) -> str:
        subject = (subject or "").strip()
        if not subject or " " in subject:
            return "ERROR: watch subject must be a non-empty token path (no spaces)"
        if not self.policy.allows(subject):
            return f"ERROR: {self.policy.deny_message(subject, 'subscribe_allowed')}"
        if not session_id:
            return "ERROR: no session context — watch requires an interactive session"
        if len(self._watches) >= self.max_watches:
            return f"ERROR: watch limit reached ({self.max_watches}); jetstream_unwatch first"
        ttl = min(self.max_ttl_s, float(ttl_s)) if ttl_s else self.default_ttl_s
        self._seq += 1
        wid = f"w{self._seq}"
        watch = Watch(id=wid, subject=subject, session_id=session_id, note=note.strip()[:200],
                      expires=time.time() + ttl, max_msgs=max(0, int(max_msgs)))

        async def _on_msg(msg, w=watch) -> None:
            w.seen += 1
            text = render_prompt(WATCH_PROMPT, subject=msg.subject or w.subject,
                                 payload=_payload_of(msg), seq=0,
                                 max_payload_chars=self.max_payload_chars)
            if w.note:
                text += f" (watch note: {w.note})"
            try:
                if not self.inject(w.session_id, text):
                    logger.info("[jetstream] watch %s injection refused; closing", w.id)
                    await self.remove(w.id)
            except Exception:
                logger.exception("[jetstream] watch %s inject failed", w.id)

        try:
            watch.sub = await self.open_sub(subject, _on_msg)
        except Exception as exc:
            return f"ERROR: subscribe failed: {exc}"
        self._watches[wid] = watch
        return (f"Watching {subject} as {wid} (ttl {ttl:.0f}s"
                + (f", {max_msgs} msgs" if max_msgs else "")
                + "; injected into this session on event).")

    async def remove(self, watch_id: str) -> str:
        w = self._watches.pop(watch_id.strip(), None)
        if w is None:
            return f"ERROR: unknown watch {watch_id!r}"
        try:
            await self.close_sub(w.sub)
        except Exception:
            logger.debug("[jetstream] watch close error (ignored)", exc_info=True)
        return f"Unwatched {watch_id} ({w.subject})."

    def list_lines(self) -> str:
        if not self._watches:
            return "No active watches."
        now = time.time()
        rows = [f"{w.id} {w.subject} session={w.session_id[:24]} "
                f"remaining={max(0, w.expires - now):.0f}s seen={w.seen}"
                for w in self._watches.values()]
        return "\n".join(rows)

    async def sweep(self) -> None:
        for wid in [w.id for w in self._watches.values() if w.expired]:
            logger.info("[jetstream] watch %s expired — closing", wid)
            await self.remove(wid)

    async def close_all(self) -> None:
        for wid in list(self._watches):
            await self.remove(wid)


def _payload_of(msg) -> Any:
    data = getattr(msg, "data", b"") or b""
    env = parse_envelope(data)
    payload = env.get("data", env) if isinstance(env, dict) else data
    if isinstance(payload, (dict, list)):
        return json.dumps(payload, default=str)
    if isinstance(payload, bytes):
        return payload.decode("utf-8", "replace")
    return payload
