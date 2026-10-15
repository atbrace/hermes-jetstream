# Changelog

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

Known limits: watches are gateway-process in-memory (TTL ≤6h, cap 16); DLQ on
exhausted wake retries not yet implemented (excess NAKs + `max_deliver=5` + backoff).
