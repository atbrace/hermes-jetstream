"""hermes-jetstream — NATS JetStream event-bus plugin for Hermes Agent.

register(ctx) wires three things:
  1. a ``jetstream`` gateway platform adapter — the lifecycle host: its
     connect()/disconnect() own the NATS connection and wake consumers
     (webhook/raft adapter precedent, the only sanctioned long-lived home);
  2. the ``nats_publish`` tool — allowlisted event emission from sessions;
  3. unload teardown for the connection.

The plugin must load in EVERY hermes process (gateway, CLI, cron, dashboard);
only the gateway enables the platform and connects, so every long-lived piece
is guarded and lazy. Nothing here touches the gateway event loop synchronously.
"""
from __future__ import annotations

import logging

logger = logging.getLogger("hermes_jetstream")


def register(ctx) -> None:
    from .config import Settings
    from .adapter import make_adapter_factory, check_deps, validate_config

    settings = Settings.from_plugin_ctx(ctx)

    def _factory(platform_config):
        # PlatformConfig.extra overlays the plugin settings block so the
        # standard `platforms.jetstream:` config surface wins over defaults.
        merged = Settings.merge_platform_config(settings, platform_config)
        from .adapter import build_adapter
        return build_adapter(platform_config, merged)

    ctx.register_platform(
        name="jetstream",
        label="JetStream",
        adapter_factory=make_adapter_factory(_factory),
        check_fn=check_deps,
        ensure_deps_fn=None,  # nats-py is declared in plugin.yaml; operator installs
        validate_config=validate_config,
        emoji="📡",
        platform_hint="JetStream event bus available via nats_publish tool and configured wake routes.",
    )

    from .tools import register_tools
    register_tools(ctx)

    from .hooks import register_hooks
    from .adapter import active_buses
    from .subject import SubjectPolicy

    def _get_bus():
        return next((b for b in active_buses() if b.connected), None)

    if settings.lifecycle_emit:
        if not SubjectPolicy([*settings.publish_allowed, f"{settings.lifecycle_prefix}.>"]).allows(
                f"{settings.lifecycle_prefix}.session.start"):
            logger.warning("[jetstream] lifecycle_emit enabled but publish prefix %s.> not in "
                           "publish_allowed — tap DISABLED (fail closed)", settings.lifecycle_prefix)
        else:
            register_hooks(ctx, settings, _profile_label(), _get_bus)

    def _teardown():
        from .adapter import shutdown_active_adapters
        try:
            shutdown_active_adapters()
        except Exception:  # teardown is best-effort; adapter.disconnect covers the normal path
            logger.debug("hermes-jetstream teardown noop", exc_info=True)

    ctx.on_unload(_teardown)


def _profile_label() -> str:
    import os
    if os.environ.get("HERMES_PROFILE"):
        return os.environ["HERMES_PROFILE"]
    try:
        from hermes_constants import get_hermes_home
        name = get_hermes_home().name
        return name if name and name != ".hermes" else "default"
    except Exception:
        return "default"
