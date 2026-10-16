# Changelog

## 0.10.0 — watchability surface (2026-10-16)

Found and closed by live dogfood on a production bus (hjs-9lm → docs/BATTLE-TEST.md):
every pipeline verdict is now counted per route — including `failed` on dispatch
exceptions, so an erroring route never reads as clean silence. The snapshot is exposed
three ways: `jetstream_status` tool (sessions), `platforms.jetstream.metrics` in
`gateway_state.json` (files/dashboard), and an opt-in heartbeat publish
(`heartbeat_s` + `heartbeat_subject`, fail-closed on the publish allowlist) so a plain
`nats watch` is a complete dead-man. No external monitoring required — the plugin's
job is to be watchable; alerting stays the operator's.

Also: silence drill (`tests/silence_drill.py`) wired into `scripts/ci.sh`; poison
exhaustion now provably loses wakes with no record (hjs-xq5 open).

## 0.9.0 — first community release candidate (2026-10-15)

MVP + all V1 gates except final catalog submission.

- Platform plugin: gateway-resident NATS JetStream connection, wake consumers
  (durable, queue-group, claim-then-ack, idempotency ledger, no-self-wake).
- Tools: `nats_publish` (allowlisted, secret-scanned, size-capped), `nats_fetch`
  (server-held pull ≤300s), `jetstream_watch` / `jetstream_unwatch` /
  `jetstream_list` (in-session subscriptions via consent-gated injection).
- Event jobs: wake route `job:` fires existing cron jobs (webhook `cron_job` semantics).
- Lifecycle tap: session/agent/subagent events mirrored to `<prefix>.<kind>` with
  privacy field-whitelists (opt-in, fail-closed).
- Auth: no-auth, token, user/pass, nkey seed, creds file, TLS, mTLS — all proven
  against scratch nats-server, negatives included (tests/auth_matrix.py).
- Generality: zero environment assumptions (CI grep-gate), never creates/mutates
  user streams, all infra values are config.

Known limits: watches are gateway-process in-memory (TTL ≤6h, cap 16); wake rate rail
sheds (ack+drop) above `rate_per_min` (hjs-632); DLQ on exhausted crash-recovery
retries not yet implemented (`max_deliver=5` + backoff).
