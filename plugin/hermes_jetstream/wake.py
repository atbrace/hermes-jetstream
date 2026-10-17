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

logger = __import__("logging").getLogger("hermes_jetstream.wake")


@dataclass
class _Bucket:
    tokens: float
    updated: float
    min_seen: float = 0.0


@dataclass
class WakePipeline:
    subscribe_allowed: list
    ledger: Any                                       # WakeLedger
    dispatch: Callable[[Any, str], Any]               # (route, prompt) -> awaitable/None
    profile_name: str
    nonce: str
    max_deliver: int = 5                              # must match bus ConsumerConfig(max_deliver)
    _buckets: Dict[str, _Bucket] = field(default_factory=dict)
    # hjs-htk watchability: every verdict increments a counter and stamps the clock, so
    # "nothing happened" and "working, nothing to do" and "shedding" are distinguishable
    # from the plugin's own surfaces (measured gap: docs/BATTLE-TEST.md).
    stats: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    def _stat(self, route_name: str, verdict: str, now: float, attempted: bool = True) -> None:
        s = self.stats.setdefault(route_name, {"attempted": 0, "last_attempt_ts": 0.0,
                                               "last_dispatch_ts": 0.0,
                                               "dispatched": 0, "shed": 0, "coalesced": 0,
                                               "skipped_self_origin": 0,
                                               "skipped_not_allowed": 0,
                                               "skipped_duplicate": 0, "failed": 0,
                                               "lost": 0})
        if attempted:
            s["attempted"] += 1
            s["last_attempt_ts"] = now
        if verdict in ("dispatched", "shed", "coalesced", "skipped_self_origin",
                       "skipped_not_allowed", "skipped_duplicate", "failed", "lost"):
            s[verdict] += 1
        if verdict == "dispatched":
            s["last_dispatch_ts"] = now

    def note_lost(self, route_name: str, n: int = 1) -> None:
        """Record wake losses not tied to a fresh delivery attempt (hjs-25g startup
        reconcile, hjs-xq5 final-delivery exhaustion). The counter makes 'lost N'
        visible wherever route stats surface, so a lost wake never reads as idle."""
        for _ in range(n):
            self._stat(route_name, "lost", time.time(), attempted=False)

    def stats_snapshot(self) -> Dict[str, Any]:
        return {name: dict(s) for name, s in self.stats.items()}

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
        """Returns a verdict string (unit-test seam). Raises only to force redelivery.
        Every verdict is counted (hjs-htk) before it is returned."""
        try:
            verdict = await self._handle(msg, route)
        except Exception:
            # dispatch failure forces redelivery; count it so an erroring route never
            # reads as clean silence (a probe that errors must never look healthy)
            self._stat(route.name, "failed", time.time())
            await self._record_final_delivery(msg, route)
            raise
        self._stat(route.name, verdict, time.time())
        return verdict

    async def _record_final_delivery(self, msg, route) -> None:
        """Last allowed delivery (num_delivered == max_deliver) just failed: the server
        will not redeliver, so the wake is gone. Record it durably (ledger 'lost'),
        visibly (WARNING + route 'lost' counter), and term the message so the consumer
        closes it with a terminal record instead of vanishing it silently (hjs-xq5)."""
        try:
            key, seq, attempt = _msg_identity(msg)
            if attempt < self.max_deliver:
                return
            self.note_lost(route.name)
            if key:
                self.ledger.mark_lost(key)
            logger.warning("[jetstream] wake LOST on final delivery: route=%s stream_seq=%s "
                           "deliveries=%d; server will not redeliver (recorded in wake-claims.db, "
                           "counted as lost=%d for this route)",
                           route.name, seq, attempt,
                           self.stats.get(route.name, {}).get("lost", 1))
            try:
                await msg.term()
            except Exception:
                pass  # term unsupported (unit FakeMsg) / already closed: record stands
        except Exception:
            logger.debug("[jetstream] final-delivery record failed", exc_info=True)

    async def _handle(self, msg, route) -> str:
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
