# Contributing

Development happens here on GitHub: branch, PR, CI, merge. No CLA, no process.

## Gate

`scripts/ci.sh` is the single gate and CI is its mirror: unit tests (stdlib-only),
a generality grep-gate over `plugin/` (the plugin must run against any
nats-server, so shipped source carries no vendor, deployment, or host names),
syntax, and `hermes plugins doctor`. The `live` CI job boots a scratch
`nats-server -js` and runs the smoke, auth-matrix, and cluster drills against
it; locally, `NATS_BIN=/path/to/nats-server ./scripts/ci.sh` does the same.
Run it before requesting review; CI on a PR must be green.

### Environment facts (stop re-discovering these)

- Interpreter: any `python3` passes the hermetic gate (the unit tests import
  `nats` nowhere). The live drills need `pip install 'nats-py>=2.7,<3' nkeys` —
  the exact pin CI uses.
- nats-py 2.x naming traps (measured, paid twice in one evening before they
  were written down): `ConsumerConfig` kwargs are `deliver_policy`,
  `opt_start_seq`, `max_deliver`, `ack_wait`, `filter_subjects` — not
  `delivery_policy`/`start_sequence`/`max_delivery`; and `msg.metadata` is a
  property, not callable (`m.metadata.sequence.stream`, `m.metadata.num_delivered`).
- Caps live in one place: emit cap `max_payload_bytes` (default 32768) and wake
  prompt `{payload}` truncation `max_payload_chars` (default 2000), both in
  `docs/CONFIG-REFERENCE.md`. Build oversized payloads in code (script +
  nats-py publish), never as embedded tool-call literals.

## Conventions

- One behavior per PR; the commit message says what and why, the diff says how.
- Tests assert observable behavior (counters, ledger rows, acks), not internals.
- New config keys land with a `docs/CONFIG-REFERENCE.md` entry in the same PR.
- A PR that only cuts code or fixes a lint is always welcome.

## Before you start on something big

Open an issue first. The plugin's core stance (general purpose, zero
infrastructure assumptions, at-least-once with a loss record instead of
silent drops) is settled; anything that bends it wants a conversation.
