"""Wake pipeline: the decision path every wake consumer callback runs, pure + testable.

Order (all guards BEFORE claim, so a skip is an ack, not a redelivery):
  parse envelope -> no-self-wake -> subject allowlist -> rate bucket -> min-interval
  -> ledger claim -> render prompt -> dispatch -> ledger complete -> ack.
Ack happens after dispatch is accepted (claim-then-ack); a dispatch failure propagates so
JetStream redelivers with backoff, and the ledger reclaims after reclaim_after_s.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional

from .envelope import is_self_origin, parse_envelope, render_prompt
from .subject import matches_any


@dataclass
class _Bucket:
    tokens: float
    updated: float
    min_seen: float = 0.0


@dataclass
class WakePipeline:
    routes_by_subject: Dict[str, Any]                 # subject -> WakeRoute (configured routes)
    subscribe_allowed: list
    ledger: Any                                       # WakeLedger
    dispatch: Callable[[Any, str], Any]               # (route, prompt) -> awaitable/None
    profile_name: str
    nonce: str
    _buckets: Dict[str, _Bucket] = field(default_factory=dict)

    def _rate_ok(self, route, now: float) -> bool:
        cap = float(route.rate_per_min)
        b = self._buckets.setdefault(route.name, _Bucket(cap, now))
        b.tokens = min(cap, b.tokens + (now - b.updated) * (cap / 60.0))
        b.updated = now
        if b.tokens < 1.0:
            return False
        b.tokens -= 1.0
        return True

    def _interval_ok(self, route, now: float) -> bool:
        if route.min_interval_s <= 0:
            return True
        b = self._buckets.setdefault(route.name, _Bucket(route.rate_per_min, now))
        if now - b.min_seen < route.min_interval_s:
            return False
        b.min_seen = now
        return True

    async def handle_message(self, msg, route) -> str:
        """Returns a verdict string (unit-test seam). Raises only to force redelivery."""
        now = time.time()
        dedupe_key, seq, attempt = _msg_identity(msg)
        envelope = parse_envelope(getattr(msg, "data", b"") or b"")

        if is_self_origin(envelope, self.nonce, self.profile_name):
            await _ack(msg)
            return "skipped_self_origin"
        if route.subject not in self.subscribe_allowed and not matches_any(route.subject, self.subscribe_allowed):
            await _ack(msg)
            return "skipped_not_allowed"          # config drift guard, ack so it stops redelivering
        if not self._rate_ok(route, now):
            await _ack(msg)                       # storm rail: shed = ack + drop
            # ponytail: shed loses events by design; if demand ever needs delayed replay, use max_deliver=-1 + trimmed backoff (option (b), hjs-632)
            return "shed"
        if not self._interval_ok(route, now):
            await _ack(msg)
            return "coalesced"
        if dedupe_key and not self.ledger.claim(dedupe_key, route.name, now):
            await _ack(msg)
            return "skipped_duplicate"

        payload = envelope.get("data", envelope) if isinstance(envelope, dict) else (getattr(msg, "data", b""))
        prompt = render_prompt(route.prompt, subject=route.subject, payload=payload, seq=seq,
                               max_payload_chars=route.max_payload_chars)
        await self.dispatch(route, prompt)
        if dedupe_key:
            self.ledger.complete(dedupe_key, now)
        await _ack(msg)
        return "dispatched"


def _msg_identity(msg):
    try:
        md = msg.metadata  # property in nats-py, NOT a callable
        return f"{md.stream}:{md.consumer}:{md.sequence.stream}", md.sequence.stream, md.num_delivered
    except Exception:
        return "", 0, 1


async def _ack(msg) -> None:
    try:
        await msg.ack()
    except Exception:
        pass  # ack on a non-JetStream or closed msg is benign; consumer redelivers if real
