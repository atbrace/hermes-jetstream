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

## Conventions

- One behavior per PR; the commit message says what and why, the diff says how.
- Tests assert observable behavior (counters, ledger rows, acks), not internals.
- New config keys land with a `docs/CONFIG-REFERENCE.md` entry in the same PR.
- A PR that only cuts code or fixes a lint is always welcome.

## Before you start on something big

Open an issue first. The plugin's core stance (general purpose, zero
infrastructure assumptions, at-least-once with a loss record instead of
silent drops) is settled; anything that bends it wants a conversation.
