# Changelog

## Unreleased

- **Runtime wake routes (`jetstream_wake`)**: register/unregister/list DURABLE wake routes at runtime — no config edit, no gateway restart. Rows persist in the plugin data dir (`wake-routes.json`), rebind at every platform bind, and carry the full config-route gate set: `subscribe_allowed` match required (loud refusal at register), same prompt/rate rails, `rt-`-namespaced names that can't collide with config routes. Unregister is idempotent; optional `max_age_s` self-retires abandoned routes at bind; `jetstream_status` shows runtime rows with `source: "runtime"` (config rows now `source: "config"`). See CONFIG-REFERENCE "Runtime wake routes". (`feat(wake)` hjs-tr9)

## 1.1.0 — pilot-hardened (2026-10-18)

Six fixes from the haus-estate pilot, each reproduced, adversarially reviewed, and merged with green CI:

- **Dedupe is now subject-safe**: `Nats-Msg-Id` is salted with the subject hash, so two events on different subjects can no longer silently collide on one stream (the old unsalted path dropped the second publish). Same-subject republish dedupe unchanged. (`fix(dedupe)` e42ea6d)
- **Platform creation failures are loud**: a missing-requirements refusal now prints an install hint naming the remedy and the unit-subdir install identifier; README gained the Install section it was missing. Core monorepo discovery remains the out-of-scope remainder (evidence in `docs/pilot/hjs-c03.md`). (`fix(platform-registration)` f2c3efc)
- **Pre-restart validation**: `scripts/validate_settings.py` dry-runs the settings block through the real loader and exits nonzero naming every offending key — schema typos die before the restart gate, not after. Ships with `docs/wake-routes.schema.json`. (`feat(validate)` cbff687)
- **Receipt inspection without a nats CLI**: CONFIG-REFERENCE + `examples/nats-box-receipts.md` give a nats-box sidecar and copy-paste recipes against the plugin's real stream/consumer names. (`docs(receipts)` d096678)
- **Capabilities disclosure**: README clarifies `plugins capabilities` is the consent-gate surface; the six jetstream tools show under `hermes tools list`. (`docs(caps)` 9bdae20)
- **nats-py traps documented**: measured `get_msg`/`RawStreamMsg` signatures for the supported range, plus a flagged `ConsumerConfig.backoff` effective-floor defect (needs >=2.8 though the pin says >=2.7). (`docs(nats-py)` f1ddb67)

Public API, config keys, and wire behavior unchanged; dedupe header format changes on the wire (msg-ids become subject-salted) — a bug-compatible external publisher relying on cross-subject unsalted ids would see behavior change (none known).


## 1.0.1 — profile-scoped secret reads (2026-10-17)

Catalog admission rule 11: `${VAR}` secret expansion in plugin settings now
resolves through `agent.secret_scope.get_secret` (profile-scoped under gateway
multiplexing) instead of a bare `os.environ` read. Single-profile behavior is
unchanged; under multiplexing a profile can no longer pick up another profile's
credential. Public API, config keys, and wire behavior unchanged.

## 1.0.0 — first stable release (2026-10-17)

Promoted 0.11.0 to stable. Every gate in `docs/V1-READINESS.md` is checked except
catalog submission, an external distribution step that does not affect the plugin.

- Public API frozen: the six tools (`jetstream_watch` / `jetstream_unwatch` /
  `jetstream_list` / `jetstream_status` / `nats_publish` / `nats_fetch`), the
  `wake:` route schema, and the settings keys in `docs/CONFIG-REFERENCE.md`.
  Breaking changes now require a major version.
- Auth matrix complete: no-auth, token, nkeys/ncreds, TLS with custom CA, mTLS.
  Evidence: `tests/auth_matrix.py`, 7 modes + 4 negative refusals.
- Reliability: at-least-once wakes with an idempotent ledger, storm rails that ack
  and drop excess, every lost wake recorded (0.11.0), reconnect forensics,
  durable consumers that rebind and redeliver across node loss (cluster drill).
- Generality: zero environment assumptions, grep-gated in CI; works against stock
  single-node and 3-node JetStream; the plugin never creates or mutates streams.

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
