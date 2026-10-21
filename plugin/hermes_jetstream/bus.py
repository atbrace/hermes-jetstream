"""NATS connection supervisor: one connection, wake consumers as tasks, never raises into the loop.

Every callback funnels through _safe() — an exception here must degrade to a log line, never
propagate into the gateway event loop (a plugin crash takes the gateway's whole session roster).
"""
from __future__ import annotations

import asyncio
import inspect
import logging
import time
from typing import Any, Callable, Coroutine, Dict, List, Optional

logger = logging.getLogger("hermes_jetstream.bus")


async def _maybe_await(fn) -> None:
    r = fn()
    if inspect.isawaitable(r):
        await r

try:
    import nats
    from nats.errors import Error as NatsError
    from nats.js.errors import NotFoundError
    HAVE_NATS = True
except ImportError:
    HAVE_NATS = False


class NatsBus:
    def __init__(self, settings, profile_name: str,
                 disconnected_cb: Optional[Callable[[], None]] = None,
                 reconnected_cb: Optional[Callable[[], None]] = None,
                 error_cb: Optional[Callable[[Any], None]] = None):
        self.settings = settings
        self.profile_name = profile_name
        self._nc = None
        self._js = None
        self._loop: Optional["asyncio.AbstractEventLoop"] = None
        self._subs: List[Any] = []
        self._closing = False
        self._disconnected_cb = disconnected_cb
        self._reconnected_cb = reconnected_cb
        self._error_cb_user = error_cb
        # hjs-2qk: incident forensics need a log trail, not just a live state surface.
        self._down_since: Optional[float] = None
        self.reconnect_count = 0

    async def _on_disconnected(self) -> None:
        # nats-py fires this on intentional close() too — only the unplanned kind
        # is an incident worth a WARNING (stop() sets _closing first).
        if not self._closing:
            self._down_since = time.monotonic()
            logger.warning("[jetstream] disconnected from %s", self.settings.servers)
        if self._disconnected_cb is not None:
            await _maybe_await(self._disconnected_cb)

    async def _on_reconnected(self) -> None:
        self.reconnect_count += 1
        down = time.monotonic() - self._down_since if self._down_since else 0.0
        self._down_since = None
        logger.info("[jetstream] reconnected (after %.1fs down, reconnect_count=%d)",
                    down, self.reconnect_count)
        if self._reconnected_cb is not None:
            await _maybe_await(self._reconnected_cb)

    async def _error_cb(self, exc) -> None:
        logger.warning("[jetstream] client error: %s", exc)
        if self._error_cb_user is not None:
            try:
                self._error_cb_user(exc)
            except Exception:
                logger.debug("[jetstream] error_cb failed", exc_info=True)

    @property
    def connected(self) -> bool:
        return self._nc is not None and not self._nc.is_closed

    @property
    def jetstream(self):
        return self._js

    @property
    def loop(self):
        """The event loop this connection is bound to (for run_coroutine_threadsafe).

        Captured at connect(): nats-py binds its transport to the loop running
        connect() but never stores it on the client (no `_loop` attribute), so
        reading the instance attribute is the only correct source. None after
        close(). (hjs-c81 dogfood: reading `_nc._loop` was always None, which
        silently disabled the lifecycle tap and forced every tool call onto the
        ephemeral fallback.)
        """
        return self._loop if (self._nc is not None and not self._nc.is_closed) else None

    async def connect(self) -> None:
        if not HAVE_NATS:
            raise RuntimeError("nats-py is not installed; pip install 'nats-py>=2.7'")
        reconnect = self.settings.reconnect_attempts != 0
        kwargs: Dict[str, Any] = dict(
            name=f"hermes-jetstream:{self.profile_name}",
            allow_reconnect=reconnect,
            reconnect_time_wait=2,
            max_reconnect_attempts=self.settings.reconnect_attempts if reconnect else 1,
            connect_timeout=self.settings.connect_timeout_s,
            no_echo=True,  # same-connection loop guard (cross-connection guard = nonce ledger)
            error_cb=self._error_cb,  # logger, never nats-py's stderr default
            disconnected_cb=self._on_disconnected,  # logs + counts (hjs-2qk)
            reconnected_cb=self._on_reconnected,
        )
        kwargs.update(self.settings.auth_kwargs())
        if self.settings.tls_enabled:
            kwargs["tls"] = self.settings.tls_kwargs()["tls"]
        # NATS() + nc.connect() (not module-level connect) so the instance is
        # reachable to close() when connect raises: nats-py spawns its reconnect
        # task BEFORE auth/TLS failures surface, and a refused client that is
        # never closed retries forever in the background (auth_matrix proved it).
        nc = nats.NATS()
        try:
            await nc.connect(self.settings.servers, **kwargs)
        except BaseException:
            try:
                await nc.close(drain=False)
            except BaseException:
                pass  # half-open transport can raise CancelledError from wait_closed
            raise
        self._nc = nc
        self._js = nc.jetstream()
        self._loop = asyncio.get_running_loop()
        logger.info("[jetstream] connected to %s (stream=%s)", self.settings.servers, self.settings.stream)

    async def publish(self, subject: str, payload: bytes,
                      headers: Optional[Dict[str, str]] = None) -> Any:
        if self._js is None:
            raise RuntimeError("jetstream not connected")
        return await self._js.publish(subject, payload, timeout=5.0, headers=headers or None)

    async def subscribe_wake(self, *, subject: str, queue: str,
                             on_message: Callable[[Any], Coroutine]) -> Any:
        """Durable push consumer, manual ack: at-least-once with explicit ack discipline.
        nats-py law: for a queue sub the QUEUE NAME is the durable consumer name (single
        identity for deliver_group/durable/queue) — 'queue' below is that identity.
        Server-side storm rails: max_deliver + backoff + max_ack_pending via ConsumerConfig."""
        from nats.js.api import ConsumerConfig
        from nats.js.errors import NotFoundError

        async def _cb(msg) -> None:
            try:
                await on_message(msg)
            except Exception:
                logger.exception("[jetstream] wake callback failed (message redelivers)")

        cfg = ConsumerConfig(
            max_deliver=5,          # server law: must exceed len(backoff)
            ack_wait=120,           # wake dispatch is seconds; agent RUNS are fire-and-forget
            max_ack_pending=100,    # consumer-side backpressure
            backoff=[1.0, 5.0, 30.0, 120.0],
        )
        # nats-py >=2.16 law: subscribe_bind requires config.deliver_subject
        # whenever the durable already exists — the new-inbox branch runs only
        # on first create. A push consumer's deliver subject is immutable, so
        # the server's value is the only legal one: look it up and hand it back.
        try:
            info = await self._js.consumer_info(self.settings.stream, queue)
        except NotFoundError:
            info = None
        if info is not None:
            server_subject = getattr(info.config, "deliver_subject", None)
            if server_subject:
                cfg.deliver_subject = server_subject
        sub = await self._js.subscribe(
            subject, queue=queue, cb=_cb, durable=queue, stream=self.settings.stream,
            manual_ack=True,
            config=cfg,
        )
        self._subs.append(sub)
        return sub  # hjs-tr9: runtime unregister needs the handle (drain on demand)

    async def fetch(self, subject: str, *, timeout: float = 30.0, batch: int = 1,
                    config=None) -> List[Any]:
        """Server-held pull (no polling loop): one fetch() call blocks in the server up to timeout.
        Ephemeral pull consumer per call — no durable state, so a restart can't wedge it."""
        if self._js is None:
            raise RuntimeError("jetstream not connected")
        from nats.errors import TimeoutError as NatsTimeoutError
        psub = await self._js.pull_subscribe(subject, stream=self.settings.stream, config=config)
        try:
            return await psub.fetch(batch, timeout=timeout)
        except NatsTimeoutError:
            return []
        finally:
            try:
                await psub.unsubscribe()
            except Exception:
                pass

    async def subscribe_ephemeral(self, subject: str, cb) -> Any:
        """Live-only core subscription (no consumer state): in-session watches see events
        published AFTER the watch opens, never history — nats_fetch is the replay tool."""
        return await self._nc.subscribe(subject, cb=cb)

    async def stop(self) -> None:
        self._closing = True
        for sub in self._subs:
            try:
                await sub.drain()
            except Exception:
                try:
                    await sub.unsubscribe()
                except Exception:
                    pass
        self._subs.clear()
        if self._nc is not None:
            try:
                await self._nc.close()
            except Exception:
                logger.debug("[jetstream] close error (ignored)", exc_info=True)
        self._nc, self._js = None, None
