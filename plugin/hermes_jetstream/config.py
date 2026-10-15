"""Plugin settings: every infra-shaped value is config, defaults fit a stock local nats-server.

Resolution order: plugin.yaml config_schema defaults < plugins.entries.hermes-jetstream.settings
(ctx.get_config) < platforms.jetstream.extra (adapter overlay). Secrets (token, creds paths, CA
paths) live in .env expansion: any string value containing ``${VAR}`` is expanded from
``os.environ`` at load, so credentials stay in Hermes' .env, never in config.yaml.
"""
from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List

_ENV_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")

DEFAULT_SERVERS = ["localhost:4222"]
DEFAULT_STREAM = "hermes"
DEFAULT_MAX_PAYLOAD = 32768  # client cap; server max_payload (1MiB default) still applies


def _expand(value: Any) -> Any:
    """Expand ${VAR} in strings (secrets live in .env; config.yaml holds the reference).
    A missing var expands empty (fail-open on value) but names itself in the log —
    a typo'd credential otherwise surfaces as a generic auth refusal."""
    if isinstance(value, str):
        def _sub(m):
            name = m.group(1)
            if name not in os.environ:
                logging.getLogger("hermes_jetstream.config").warning(
                    "[jetstream] config references ${%s} but it is not set — expanded empty", name)
            return os.environ.get(name, "")
        return _ENV_RE.sub(_sub, value)
    if isinstance(value, dict):
        return {k: _expand(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_expand(v) for v in value]
    return value


def _subject_list(value: Any) -> List[str]:
    if not value:
        return []
    if isinstance(value, str):
        value = [value]
    return [str(v).strip() for v in value if str(v).strip()]


@dataclass
class WakeRoute:
    """One subject->wake route. Template-rendered prompt (never raw payload concat).
    With `job` set it's an EVENT JOB: fire that cron job (execute_job_for_event rail,
    same as webhook cron_job routes) instead of waking a session."""
    name: str
    subject: str                      # must match subscribe_allowed
    durable: str = ""                 # default: jetstream-<name>
    prompt: str = "JetStream event on {subject}. Payload: {payload}"
    max_payload_chars: int = 2000     # payload truncation into the prompt
    rate_per_min: int = 10            # per-route token bucket
    min_interval_s: float = 0.0       # coalescing: drop events closer than this
    job: str = ""                     # cron job id/name -> event job
    profile: str = ""                 # profile owning the job (event jobs only)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "WakeRoute":
        name = str(d.get("name", "")).strip()
        if not name:
            raise ValueError("wake route requires a non-empty name")
        subject = str(d.get("subject", "")).strip()
        if not subject:
            raise ValueError(f"wake route {name!r} requires a subject")
        durable = str(d.get("durable") or f"jetstream-{name}")
        # NATS rejects wildcards/whitespace in consumer names; fail at config load,
        # not at connect where the error takes down every route on the platform.
        if not durable or "*" in durable or ">" in durable or any(c.isspace() for c in durable):
            raise ValueError(f"wake route {name!r}: durable {durable!r} must be a token (no '*', '>', whitespace)")
        return cls(
            name=name, subject=subject,
            durable=durable,
            prompt=str(d.get("prompt") or cls.prompt),
            max_payload_chars=max(64, int(d.get("max_payload_chars", cls.max_payload_chars))),
            rate_per_min=max(1, int(d.get("rate_per_min", cls.rate_per_min))),
            min_interval_s=max(0.0, float(d.get("min_interval_s", 0.0))),
            job=str(d.get("job") or "").strip(),
            profile=str(d.get("profile") or "").strip(),
        )


@dataclass
class Settings:
    servers: List[str] = field(default_factory=lambda: list(DEFAULT_SERVERS))
    stream: str = DEFAULT_STREAM
    nkey: str = ""
    token: str = ""
    user: str = ""
    password: str = ""
    creds: str = ""                   # path to nkeys creds file
    tls_enabled: bool = False
    tls_ca: str = ""
    tls_cert: str = ""                # mTLS client cert
    tls_key: str = ""
    tls_insecure_skip_verify: bool = False
    publish_allowed: List[str] = field(default_factory=list)   # empty = publish disabled (fail closed)
    subscribe_allowed: List[str] = field(default_factory=list) # empty = no wake routes honored
    wake: List[WakeRoute] = field(default_factory=list)
    max_payload_bytes: int = DEFAULT_MAX_PAYLOAD
    connect_timeout_s: float = 10.0
    reconnect_attempts: int = -1      # -1 = never give up (outage-tolerant)
    lifecycle_emit: list = field(default_factory=list)   # empty = tap OFF; list kinds to enable
    lifecycle_prefix: str = "hermes"

    @classmethod
    def from_plugin_ctx(cls, ctx) -> "Settings":
        g = ctx.get_config
        tls = _expand(g("tls", {}) or {})
        auth = _expand(g("auth", {}) or {})
        return cls(
            servers=_subject_list(_expand(g("servers", DEFAULT_SERVERS))) or list(DEFAULT_SERVERS),
            stream=str(_expand(g("stream", DEFAULT_STREAM))) or DEFAULT_STREAM,
            nkey=str(auth.get("nkey", "")), token=str(auth.get("token", "")),
            user=str(auth.get("user", "")), password=str(auth.get("password", "")),
            creds=str(auth.get("creds", "")),
            tls_enabled=bool(tls.get("enabled", False)),
            tls_ca=str(tls.get("ca", "")), tls_cert=str(tls.get("cert", "")),
            tls_key=str(tls.get("key", "")),
            tls_insecure_skip_verify=bool(tls.get("insecure_skip_verify", False)),
            publish_allowed=_subject_list(_expand(g("publish_allowed", []))),
            subscribe_allowed=_subject_list(_expand(g("subscribe_allowed", []))),
            wake=[WakeRoute.from_dict(r) for r in (_expand(g("wake", [])) or []) if isinstance(r, dict)],
            max_payload_bytes=int(g("max_payload_bytes", DEFAULT_MAX_PAYLOAD) or DEFAULT_MAX_PAYLOAD),
            lifecycle_emit=_subject_list(_expand(g("lifecycle_emit", []))),
            lifecycle_prefix=str(g("lifecycle_prefix", "hermes")) or "hermes",
        )

    @classmethod
    def merge_platform_config(cls, base: "Settings", platform_config) -> "Settings":
        """platforms.<name>.extra overlays the plugin settings block (standard platform surface)."""
        extra = dict(getattr(platform_config, "extra", None) or {})
        if not extra:
            return base
        tok = getattr(platform_config, "token", None)
        if tok:
            base = cls(**{**base.__dict__, "token": str(tok)})
        if "servers" in extra:
            base.servers = _subject_list(_expand(extra["servers"])) or base.servers
        if "stream" in extra:
            base.stream = str(_expand(extra["stream"])) or base.stream
        for key in ("nkey", "token", "user", "password", "creds"):
            if key in extra:
                setattr(base, key, str(_expand(extra[key])))
        if "tls" in extra:
            t = _expand(extra["tls"] or {})
            base.tls_enabled = bool(t.get("enabled", base.tls_enabled))
            base.tls_ca = str(t.get("ca", base.tls_ca))
            base.tls_cert = str(t.get("cert", base.tls_cert))
            base.tls_key = str(t.get("key", base.tls_key))
            base.tls_insecure_skip_verify = bool(t.get("insecure_skip_verify", base.tls_insecure_skip_verify))
        if "publish_allowed" in extra:
            base.publish_allowed = _subject_list(_expand(extra["publish_allowed"]))
        if "subscribe_allowed" in extra:
            base.subscribe_allowed = _subject_list(_expand(extra["subscribe_allowed"]))
        if "wake" in extra:
            base.wake = [WakeRoute.from_dict(r) for r in (_expand(extra["wake"]) or []) if isinstance(r, dict)]
        return base

    def tls_kwargs(self) -> Dict[str, Any]:
        """nats-py takes an ssl.SSLContext in tls= (None disables)."""
        import ssl as _ssl
        if not self.tls_enabled:
            return {}
        ctx_ = _ssl.create_default_context(cafile=self.tls_ca or None)
        if self.tls_cert and self.tls_key:
            ctx_.load_cert_chain(certfile=self.tls_cert, keyfile=self.tls_key)
        if self.tls_insecure_skip_verify:
            ctx_.check_hostname = False
            ctx_.verify_mode = _ssl.CERT_NONE
        return {"tls": ctx_}

    def auth_kwargs(self) -> Dict[str, Any]:
        """Auth modes nats-py supports: token | creds (nkeys file) | nkey seed (client-signed
        nonce) | user/password. nkeys_seed_str makes the client sign without a signature_cb."""
        kw: Dict[str, Any] = {}
        if self.token:
            kw["token"] = self.token
        elif self.creds:
            # nats-py trap (hjs-bt0 drill): the user_credentials FILE path always
            # reads a "BEGIN NATS USER JWT" block; a seed-only creds file makes it
            # loop forever on EOF. Seed-only files route to nkeys_seed_str instead.
            try:
                text = Path(self.creds).read_text()
            except OSError as exc:
                raise ValueError(f"jetstream: creds file unreadable: {exc}") from exc
            if "USER NKEY SEED" in text and "BEGIN NATS USER JWT" not in text:
                seed = text.split("USER NKEY SEED-----", 1)[1].split("-----", 1)[0].strip()
                kw["nkeys_seed_str"] = seed
            else:
                kw["user_credentials"] = self.creds
        elif self.nkey:
            kw["nkeys_seed_str"] = self.nkey
        elif self.user:
            kw["user"] = self.user
            kw["password"] = self.password
        return kw
