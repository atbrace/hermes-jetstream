"""JetStream platform adapter: lifecycle host for the bus + wake consumers.

connect() (gateway startup / platform enable) opens the connection and one durable push
consumer per configured wake route; disconnect() drains them. Sends are a no-op: this
platform delivers events IN, replies flow through normal platform routes (raft precedent).
All gateway imports are lazy so the module loads (and unit tests import) without hermes.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, Optional

from .bus import NatsBus, HAVE_NATS, _msg_meta
from .config import Settings
from .envelope import GATEWAY_NONCE
from .ledger import WakeLedger
from .wake import WakePipeline
from .subject import matches_any

logger = logging.getLogger("hermes_jetstream.adapter")

_ACTIVE = set()
_ACTIVE_LOCK = __import__("threading").Lock()


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
            self._bus = NatsBus(self.settings, self._profile_name)
            self._pipeline = WakePipeline(
                routes_by_subject={r.subject: r for r in routes},
                subscribe_allowed=self.settings.subscribe_allowed,
                ledger=self._ledger, dispatch=self._dispatch,
                profile_name=self._profile_name, nonce=GATEWAY_NONCE)
            try:
                await self._bus.connect()
                for r in routes:
                    await self._bus.subscribe_wake(
                        subject=r.subject, queue=r.durable,  # queue name == durable consumer name
                        on_message=lambda msg, route=r: self._pipeline.handle_message(msg, route))
            except Exception:
                logger.exception("[jetstream] connect failed; platform stays down (gateway healthy)")
                self._bus = None
                return False
            self._mark_connected()
            with _ACTIVE_LOCK:
                _ACTIVE.add(self)
            return True

        async def _dispatch(self, route, prompt: str) -> None:
            if not self._message_handler:
                raise RuntimeError("gateway message handler not attached yet")  # -> redelivery
            chat_id = f"jetstream:{route.name}"
            source = self.build_source(chat_id=chat_id, chat_name=f"jetstream/{route.name}",
                                       chat_type="jetstream", user_id=f"jetstream:{route.name}",
                                       user_name=route.name)
            event = _message_event(text=prompt, source=source)
            await self.handle_message(event)

        async def disconnect(self) -> None:
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
