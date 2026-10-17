"""JetStream platform adapter: lifecycle host for the bus + wake consumers.

connect() (gateway startup / platform enable) opens the connection and one durable push
consumer per configured wake route; disconnect() drains them. Sends are a no-op: this
platform delivers events IN, replies flow through normal platform routes (raft precedent).
All gateway imports are lazy so the module loads (and unit tests import) without hermes.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Dict, Optional

from .bus import NatsBus, HAVE_NATS
from .config import Settings
from .envelope import GATEWAY_NONCE
from .ledger import WakeLedger
from .wake import WakePipeline
from .watch import WatchRegistry
from .subject import SubjectPolicy, matches_any

logger = logging.getLogger("hermes_jetstream.adapter")

_ACTIVE = set()
_ACTIVE_LOCK = __import__("threading").Lock()
_PLUGIN_CTX: list = []


def set_plugin_ctx(ctx) -> None:
    """Called from register(): the ctx owns the consent-gated inject_message rail."""
    _PLUGIN_CTX.clear()
    _PLUGIN_CTX.append(ctx)


def check_deps() -> bool:
    """PASSIVE probe (status displays call it freely; must not install)."""
    return HAVE_NATS


def validate_config(platform_config) -> bool:
    extra = getattr(platform_config, "extra", None) or {}
    return bool(extra.get("wake") or extra.get("servers") or extra.get("stream"))


def make_adapter_factory(factory_fn):
    """Wrap the PlatformConfig->adapter factory: a bad config fails the platform, not the gateway."""
    def _factory(platform_config):
        try:
            return factory_fn(platform_config)
        except Exception:
            logger.exception("[jetstream] adapter construction refused (bad config?)")
            return None
    return _factory


def _build_class():
    """Subclass BasePlatformAdapter lazily: the gateway import happens the first time an
    adapter is actually built (never at plugin load — CLI/cron/dashboard processes import
    this module too and must not pay for gateway imports)."""
    from gateway.platforms.base import BasePlatformAdapter

    class _Adapter(BasePlatformAdapter):
        def __init__(self, config, settings: Settings):
            from gateway.config import Platform
            super().__init__(config, Platform("jetstream"))
            self.settings = settings
            self._bus: Optional[NatsBus] = None
            self._pipeline: Optional[WakePipeline] = None
            self._ledger = WakeLedger(_plugin_data_dir() / "wake-claims.db")
            self._profile_name = _profile_name()
            self._watches: Optional[WatchRegistry] = None
            self._last_error: Optional[Dict[str, Any]] = None
            self._hb_last: float = 0.0

        async def connect(self, *, is_reconnect: bool = False) -> bool:
            if self._bus is not None and self._bus.connected:
                return True
            routes = [r for r in self.settings.wake
                      if r.subject in self.settings.subscribe_allowed
                      or matches_any(r.subject, self.settings.subscribe_allowed)]
            dropped = len(self.settings.wake) - len(routes)
            if dropped:
                logger.warning("[jetstream] %d wake route(s) outside subscribe_allowed — ignored", dropped)
            if not routes:
                logger.info("[jetstream] connected (publish-only: no wake routes configured)")
            hb = int(getattr(self.settings, "heartbeat_s", 0) or 0)
            hsubj = str(getattr(self.settings, "heartbeat_subject", "") or "").strip()
            if hb and not hsubj:
                logger.warning("[jetstream] heartbeat_s set but heartbeat_subject empty — heartbeat DISABLED (fail closed)")
                self.settings.heartbeat_s = 0
            elif hb and not SubjectPolicy([*self.settings.publish_allowed, hsubj]).allows(hsubj):
                logger.warning("[jetstream] heartbeat_subject %s not in publish_allowed — heartbeat DISABLED (fail closed)", hsubj)
                self.settings.heartbeat_s = 0
            self._last_error = None
            self._bus = NatsBus(self.settings, self._profile_name, error_cb=self._note_error)
            self._pipeline = WakePipeline(
                subscribe_allowed=self.settings.subscribe_allowed,
                ledger=self._ledger, dispatch=self._dispatch,
                profile_name=self._profile_name, nonce=GATEWAY_NONCE)
            self._reconcile_lost()
            self._watches = WatchRegistry(
                policy=SubjectPolicy(self.settings.subscribe_allowed),
                open_sub=self._bus.subscribe_ephemeral,
                close_sub=lambda sub: sub.unsubscribe(),
                inject=self._inject_to_session)
            try:
                await self._bus.connect()
                for r in routes:
                    await self._bus.subscribe_wake(
                        subject=r.subject, queue=r.durable,  # queue name == durable consumer name
                        on_message=lambda msg, route=r: self._pipeline.handle_message(msg, route))
                self._seed_route_stats(routes)
                self._sweep_task = asyncio.create_task(self._sweep_loop())
            except Exception:
                logger.exception("[jetstream] connect failed; platform stays down (gateway healthy)")
                self._bus = None
                return False
            self._mark_connected()
            self._write_metrics()
            with _ACTIVE_LOCK:
                _ACTIVE.add(self)
            return True

        _watch_keys: Dict[str, str] = {}

        def _reconcile_lost(self) -> None:
            """hjs-25g: a claimed-never-done ledger row past the reclaim window is the
            at-rest fingerprint of a crash between dispatch and ack — the wake is gone
            (the server already acked it). Say so loudly once at startup, and count it
            so the loss survives into every status surface."""
            try:
                rows = self._ledger.reconcile_lost()
            except Exception:
                logger.debug("[jetstream] startup reconcile failed", exc_info=True)
                return
            for key, route, claimed_at in rows:
                logger.warning("[jetstream] wake LOST to crash before restart: route=%s "
                               "claim=%s claimed_at=%s (acked pre-crash, server will not "
                               "redeliver)", route or "?", key[:48],
                               time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(claimed_at)))
                if self._pipeline:
                    self._pipeline.note_lost(route or "?")

        def _seed_route_stats(self, routes) -> None:
            """Every bound route gets a stats row at connect (hjs-25g): absent-vs-zero
            becomes explicit — a zero row means idle-but-alive, an absent row means
            the platform never bound."""
            if not self._pipeline:
                return
            for r in routes:
                self._pipeline._stat(r.name, "seed", 0.0, attempted=False)

        def _note_error(self, exc) -> None:
            self._last_error = {"ts": time.time(), "msg": str(exc)[:200]}

        def status(self) -> Dict[str, Any]:
            """Watchability surface (hjs-htk): what a session/tool/monitor can ask without
            a jsz client. Distinguishes idle from broken via per-route counters + clocks."""
            hb = getattr(self.settings, "heartbeat_s", 0)
            st = {
                "connected": bool(self._bus and self._bus.connected),
                "stream": self.settings.stream,
                "servers": list(self.settings.servers),
                "reconnect_count": self._bus.reconnect_count if self._bus else 0,
                "routes": self._pipeline.stats_snapshot() if self._pipeline else {},
                "watches": len(self._watches._watches) if self._watches else 0,
                "heartbeat_s": hb,
            }
            if self._last_error:
                st["last_error"] = self._last_error
            if self.settings.lifecycle_emit:
                try:
                    from .hooks import tap_stats
                    st["lifecycle"] = tap_stats()
                except Exception:
                    pass
            return st

        def _write_metrics(self) -> None:
            """Mirror status() into the gateway platform state so it survives without
            the tool (dashboard, files, any external collector). Never raises."""
            try:
                self._write_runtime_status_safe("metrics", platform_metrics=self.status())
            except Exception:
                logger.debug("[jetstream] metrics write failed", exc_info=True)

        async def _heartbeat(self) -> None:
            hb = getattr(self.settings, "heartbeat_s", 0)
            subject = str(getattr(self.settings, "heartbeat_subject", "") or "").strip()
            if not hb or not subject or not (self._bus and self._bus.connected):
                return
            now = time.time()
            if now - getattr(self, "_hb_last", 0) < hb:
                return
            self._hb_last = now
            st = self.status()
            st["kind"] = "hermes-jetstream.heartbeat"
            st["profile"] = self._profile_name
            st["ts"] = now
            st.pop("servers", None)
            try:
                import json
                await self._bus.publish(subject, json.dumps(st, default=str).encode())
            except Exception:
                logger.warning("[jetstream] heartbeat publish failed", exc_info=True)

        async def _sweep_loop(self) -> None:
            while True:
                await asyncio.sleep(60)
                try:
                    if self._watches:
                        await self._watches.sweep()
                    self._ledger.prune()  # keep wake-claims.db bounded (7d done rows)
                    self._write_metrics()
                    await self._heartbeat()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("[jetstream] watch sweep failed")

        async def _inject_to_session(self, session_ref: str, text: str) -> bool:
            """Watch fires -> inject into the bound session via the plugin ctx rail
            (consent-gated: ctx.inject_message enforces allow_gateway_injection itself).
            session_ref is the dispatch session_id; injection wants a session_key, so
            try it raw first, else resolve via the live gateway's session store (cached)."""
            ctx = _PLUGIN_CTX[0] if _PLUGIN_CTX else None
            if ctx is None:
                return False
            ref = self._watch_keys.get(session_ref, session_ref)
            try:
                if ctx.inject_message(text, role="user", session_key=ref):
                    return True
            except Exception:
                logger.debug("[jetstream] inject failed for %s", ref[:32], exc_info=True)
            try:  # session_id -> session_key via the injector's owning gateway runner
                from hermes_cli.plugins import get_plugin_manager
                inj = getattr(get_plugin_manager(), "_gateway_message_injector", None)
                store = getattr(inj[0], "async_session_store", None) if inj else None
                entry = await store.lookup_by_session_id(session_ref) if store else None
                if entry and entry.session_key:
                    self._watch_keys[session_ref] = entry.session_key
                    return bool(ctx.inject_message(text, role="user", session_key=entry.session_key))
            except Exception:
                logger.debug("[jetstream] session_id resolution failed", exc_info=True)
            return False

        async def _dispatch(self, route, prompt: str) -> None:
            if route.job:
                await self._fire_event_job(route, prompt)
                return
            if not self._message_handler:
                raise RuntimeError("gateway message handler not attached yet")  # -> redelivery
            chat_id = f"jetstream:{route.name}"
            source = self.build_source(chat_id=chat_id, chat_name=f"jetstream/{route.name}",
                                       chat_type="jetstream", user_id=f"jetstream:{route.name}",
                                       user_name=route.name)
            event = _message_event(text=prompt, source=source)
            await self.handle_message(event)

        async def _fire_event_job(self, route, prompt: str) -> None:
            """Event job: fire a cron job on this event (webhook cron_job semantics:
            transient per-run context, job's own prompt/skills/model/delivery apply,
            at-most-once claiming inside the cron layer; bad job ref -> logged, acked,
            same as webhook — a redelivery storm can't revive a deleted job).
            Queued, not awaited — a run is minutes long (webhook 202 contract)."""
            import asyncio
            from tools.cronjob_tools import execute_job_for_event

            async def _run():
                try:
                    with self._profile_scope(route.profile):
                        # to_thread copies contextvars, so the profile scope follows (webhook precedent).
                        result = await asyncio.to_thread(execute_job_for_event, route.job,
                            f"This run was triggered by JetStream event on '{route.subject}' "
                            f"via route '{route.name}' (not the schedule).\n\n{prompt}")
                    if not result.get("success"):
                        logger.warning("[jetstream] event job=%s route=%s incomplete: %s",
                                       route.job, route.name, result.get("error"))
                except Exception:
                    logger.exception("[jetstream] event job=%s route=%s failed", route.job, route.name)

            task = asyncio.create_task(_run())
            self._background_tasks = getattr(self, "_background_tasks", set())
            self._background_tasks.add(task)
            task.add_done_callback(self._background_tasks.discard)

        @staticmethod
        def _profile_scope(profile: str):
            from contextlib import nullcontext
            if not profile:
                return nullcontext()
            from gateway.run import _profile_runtime_scope
            from hermes_cli.profiles import get_profile_dir
            return _profile_runtime_scope(get_profile_dir(profile))

        async def disconnect(self) -> None:
            task = getattr(self, "_sweep_task", None)
            if task:
                task.cancel()
                self._sweep_task = None
            if self._watches:
                await self._watches.close_all()
            self._watches = None
            if self._bus:
                await self._bus.stop()
                self._bus = None
            with _ACTIVE_LOCK:
                _ACTIVE.discard(self)
            self._mark_disconnected()
            self._ledger.close()

        async def send(self, chat_id: str, content: str, reply_to: Optional[str] = None,
                       metadata: Optional[Dict[str, Any]] = None):
            from gateway.platforms.base import SendResult
            logger.debug("[jetstream] send no-op (inbound-event platform); chat_id=%s", chat_id)
            return SendResult(success=True)

        async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
            return {"name": f"jetstream/{chat_id}", "type": "jetstream"}

    return _Adapter


def _message_event(**kwargs):
    from gateway.platforms.event import MessageEvent, MessageType
    kwargs.setdefault("message_type", MessageType.TEXT)
    return MessageEvent(**kwargs)


def _plugin_data_dir():
    from pathlib import Path
    try:
        from plugins.plugin_storage import plugin_data_dir
        return plugin_data_dir("hermes-jetstream")
    except Exception:
        import os
        home = os.environ.get("HERMES_HOME") or os.path.expanduser("~/.hermes")
        p = Path(home) / "plugin-data" / "hermes-jetstream"
        p.mkdir(parents=True, exist_ok=True)
        return p


def _profile_name() -> str:
    import os
    if os.environ.get("HERMES_PROFILE"):
        return os.environ["HERMES_PROFILE"]
    try:
        from hermes_constants import get_hermes_home
        return get_hermes_home().name if get_hermes_home().name != ".hermes" else "default"
    except Exception:
        return "default"


_ACTIVE_CLASS: list = []


def active_buses():
    """Live NatsBus objects from connected adapters in this process (tool fast path)."""
    with _ACTIVE_LOCK:
        return [a._bus for a in _ACTIVE if getattr(a, "_bus", None) is not None]


def active_watch_registries():
    with _ACTIVE_LOCK:
        return [a._watches for a in _ACTIVE if getattr(a, "_watches", None) is not None]


def active_status():
    """Per-adapter watchability snapshots for this process (hjs-htk)."""
    with _ACTIVE_LOCK:
        return [a.status() for a in _ACTIVE]


def build_adapter(platform_config, settings: Settings):
    """Factory entry (PlatformConfig -> adapter)."""
    if not _ACTIVE_CLASS:
        _ACTIVE_CLASS.append(_build_class())
    return _ACTIVE_CLASS[0](platform_config, settings)


def shutdown_active_adapters() -> None:
    """Best-effort teardown on plugin unload; the running loop may be gone."""
    with _ACTIVE_LOCK:
        adapters = list(_ACTIVE)
    for a in adapters:
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                loop.create_task(a.disconnect())
        except Exception:
            pass
