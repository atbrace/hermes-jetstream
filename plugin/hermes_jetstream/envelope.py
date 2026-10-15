"""Event envelope, prompt rendering, payload hygiene. No nats/hermes imports (unit-testable)."""
from __future__ import annotations

import json
import re
import secrets
import time
from typing import Any, Dict, Optional

# Gateway-process nonce: identifies THIS process for no-self-wake (origin envelope is
# self-asserted until server ACLs exist — it breaks loops, it is not an auth boundary).
GATEWAY_NONCE = secrets.token_hex(8)

# Refuse-by-default secret shapes on emit (belt-braces; the real control is emitting thin events).
_SECRET_PATTERNS = [
    re.compile(r"(?i)\b(sk-[A-Za-z0-9_\-]{16,}|ghp_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,})\b"),
    re.compile(r"(?i)-----BEGIN [A-Z ]*PRIVATE KEY-----"),
    re.compile(r"(?i)\bAKIA[0-9A-Z]{16}\b"),
    re.compile(r"(?i)\b(xox[abprs]-[A-Za-z0-9\-]{10,})\b"),
]


def find_secret_like(payload: Any) -> Optional[str]:
    """Return the name of the first secret-shaped pattern found in a JSON view of payload."""
    text = payload if isinstance(payload, str) else json.dumps(payload, default=str)
    for pat in _SECRET_PATTERNS:
        if pat.search(text):
            return pat.pattern
    return None


def build_envelope(kind: str, data: Any, *, profile: str = "", session_id: str = "",
                   source: str = "tool") -> Dict[str, Any]:
    return {
        "schema": "hermes-jetstream.v1",
        "kind": kind,
        "ts": time.time(),
        "origin": {"profile": profile, "session": session_id, "nonce": GATEWAY_NONCE, "source": source},
        "data": data,
    }


def is_self_origin(envelope: Any, nonce: str, profile: str) -> bool:
    """No-self-wake: an event emitted by this same process (and profile) must never wake it back."""
    if not isinstance(envelope, dict) or not nonce:
        return False
    origin = envelope.get("origin") or {}
    n = str(origin.get("nonce", ""))
    return bool(n) and n == nonce and str(origin.get("profile", "")) == profile


def render_prompt(template: str, *, subject: str, payload: Any, seq: int,
                  max_payload_chars: int = 2000) -> str:
    """Single-brace substitution only ({subject} {payload} {seq} {ts}) — no attribute traversal,
    payload JSON-serialized and truncated. Raw payloads never reach prompts unrendered."""
    if isinstance(payload, (dict, list)):
        payload_text = json.dumps(payload, default=str, separators=(",", ":"))
    else:
        payload_text = str(payload)
    if len(payload_text) > max_payload_chars:
        payload_text = payload_text[:max_payload_chars] + "…[truncated]"
    ts = ""
    if isinstance(payload, dict) and "ts" in payload:
        ts = str(payload.get("ts"))
    out = template
    for key, value in (("subject", subject), ("payload", payload_text), ("seq", str(seq)), ("ts", ts)):
        out = out.replace("{" + key + "}", value)
    return out


def parse_envelope(payload: bytes) -> Optional[Dict[str, Any]]:
    try:
        obj = json.loads(payload.decode("utf-8", "replace"))
    except (ValueError, UnicodeDecodeError):
        return None
    return obj if isinstance(obj, dict) else None
