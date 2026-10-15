"""Subject policy: default-deny allowlists and NATS wildcard matching (stdlib-only, unit-testable)."""
from __future__ import annotations

from typing import Iterable, List


def _tokens(pattern: str) -> List[str]:
    return pattern.split(".")


def subject_matches(subject: str, pattern: str) -> bool:
    """NATS wildcard matching: '*' one token, '>' trailing rest. Exact match otherwise."""
    st, pt = _tokens(subject), _tokens(pattern)
    for i, p in enumerate(pt):
        if p == ">":
            return i == len(pt) - 1 and len(st) > i
        if i >= len(st):
            return False
        if p != "*" and p != st[i]:
            return False
    return len(st) == len(pt)


def matches_any(subject: str, patterns: Iterable[str]) -> bool:
    return any(subject_matches(subject, p) for p in patterns)


class SubjectPolicy:
    """Default-deny: empty allowlist denies everything (fail closed at the trust boundary)."""

    def __init__(self, allowed: Iterable[str]):
        self._allowed = [p for p in allowed if p]

    def allows(self, subject: str) -> bool:
        subject = (subject or "").strip()
        if not subject or not self._allowed:
            return False
        return matches_any(subject, self._allowed)

    def deny_message(self, subject: str, kind: str) -> str:
        return (f"Subject {subject!r} is not in this plugin's {kind} allowlist "
                f"(plugins.entries.hermes-jetstream.settings.{kind}); deny-by-default")


def validate_subject_literal(subject: str, kind: str) -> None:
    """Publish targets must be literal subjects — no wildcards a model can widen."""
    if "*" in subject or ">" in subject:
        raise ValueError(f"{kind} subjects must be literal (no * or > wildcards); got {subject!r}")
