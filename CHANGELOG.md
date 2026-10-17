# Changelog

## 0.11.0 — every lost wake leaves a record (2026-10-16)

The "a probe that errors must never read as clean" doctrine, applied to wakes:
nothing that loses an event stays silent anymore.

- Poison exhaustion recorded (hjs-xq5): when the final allowed delivery fails, the
  plugin logs a WARNING, writes ledger `status='lost'`, counts route `lost`, and
  `term()`s the message. The silence drill S4 now asserts the record exists.
  (A DLQ *subject* remains a documented non-goal; the record is the deliverable.)
- Crash losses found at startup (hjs-25g): claimed-never-done ledger rows past the
  reclaim window are reconciled at connect — logged once, counted as `lost`. A
  restart no longer erases the difference between "route idle" and "route lost a
  wake to the crash". Route stats rows are also seeded at bind, so an absent
  metrics row now strictly means the platform never came up.
- Reconnect forensics (hjs-2qk): unplanned disconnects log at WARNING, reconnects
  log at INFO with the down duration, and `reconnect_count` rides in every status
  surface (intentional shutdown stays quiet).
- Lifecycle tap counted (hjs-azz): emitted/dropped/published/failed per tap process,
  surfaced as `lifecycle` in the status snapshot; publish failures moved from debug
  to WARNING.

## 0.10.0 — watchability surface + first live-campaign fixes (2026-10-16)

Watchability (hjs-htk): every pipeline verdict is now counted per route —
including `failed` on dispatch exceptions, so an erroring route never reads as clean
silence. The snapshot is exposed three ways: `jetstream_status` tool (sessions),
`platforms.jetstream.metrics` in `gateway_state.json` (files/dashboard), and an
opt-in heartbeat publish (`heartbeat_s` + `heartbeat_subject`, fail-closed on the
publish allowlist) so a plain `nats watch` is a complete dead-man. No external
monitoring required — the plugin's job is to be watchable; alerting stays the
operator's.

Found and fixed by the first live campaign on a real production bus: `bus.loop` was always None (nats-py stores no `_loop`),
which had silently disabled the lifecycle tap and forced every tool call onto the
ephemeral connect path; wake sessions now authorize per-platform via
`JETSTREAM_ALLOWED_USERS` instead of default-denying to the global allowlist;
mis-guessed tool kwargs return a hint instead of a raw TypeError.

Also: silence drill (`tests/silence_drill.py`) wired into `scripts/ci.sh`; poison
exhaustion now provably loses wakes with no record (open). `nats_fetch` now resumes
after the last returned message within a process instead of replaying acked
messages (see CONFIG-REFERENCE fetch resume semantics).

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
