# hermes-jetstream

NATS JetStream plugin for [Hermes Agent](https://hermes-agent.nousresearch.com) — first-class event-bus integration, no core changes.

**Status: pre-MVP.** Issue tracking: `hjs-` beads (this repo, `.beads/`). Research baseline: [`research/00-FINDINGS.md`](research/00-FINDINGS.md). North star: [`docs/V1-READINESS.md`](docs/V1-READINESS.md).

## What it will do

- **Subscribe in-session** — agents watch JetStream subjects ad-hoc (`jetstream_subscribe` / `jetstream_fetch`); events wake or steer sessions instead of polling.
- **Event jobs** — cron-style jobs triggered by subject match instead of a schedule, delivered through Hermes' existing job rails (cron delivery + `/cron` UI free).
- **Emit** — `jetstream_publish` tool + a lifecycle hook tap that mirrors Hermes gateway/session/agent events onto a `hermes.>` subject stream.

## Design stance

- **General purpose by rule.** No assumptions about your infrastructure: server URLs, credentials, streams, and subjects are all plugin config. Tested against stock `nats:2-alpine` in Docker; anything that speaks NATS + JetStream works.
- Gateway-resident supervisor (`kind: platform` plugin), durable consumers, manual ack, idempotent wake ledgers — at-least-once, deduped.
- Default-deny subject publish allowlist; loop breakers (no-self-wake) on by default.

## Configuration (all under `plugins.entries.hermes-jetstream.settings`)

```yaml
servers: ["localhost:4222"]        # any nats-client-url
stream: hermes                     # must bind your subject space
publish_allowed: ["hermes.agent.>"]     # empty = publishing DENIED
subscribe_allowed: ["ci.>", "hermes.>"] # gates wake routes AND nats_fetch
wake:                                       # subject -> wake routes
  - name: ci-red
    subject: ci.result.red
    prompt: "CI went red: {payload}. Investigate."
    rate_per_min: 5
  - name: deploy-audit                  # event job: fire an existing cron job instead
    subject: deploy.finished
    job: nightly-audit                  # cron job id or name (execute_job_for_event rail)
    profile: homelab                    # optional: profile owning the job
auth: { token: "${NATS_TOKEN}" }            # or creds/nkey/user; secrets stay in .env
tls: { enabled: true, ca: /etc/ssl/nats-ca.pem }
lifecycle_emit: [session.start, session.end, agent.turn.start]  # mirror to hermes.<kind>
lifecycle_prefix: hermes
```

## Multiplex-profile behavior

One adapter per profile that enables the platform: each profile's `settings` (servers,
allowlists, wake routes, auth) produce their own connection, and durable consumer names
carry the profile's route names, so profiles never share a queue group. Two profiles
pointing at the same `wake[].durable` on the same server intentionally form one queue
group (shared dispatch) — that is NATS queue-group semantics, not an accident. Watches
are per-gateway-process in-memory state; only the profile whose gateway loaded the
platform serves them.

## Testing

```bash
python -m unittest tests.test_core        # stdlib-only, no network
HJS_SMOKE_SERVERS=localhost:4222 python tests/smoke_live.py   # live drill, self-cleaning
```

## JetStream model in one paragraph (why both pillars can coexist)

JetStream layers durable **consumers** on plain NATS fan-out pub/sub. Publishing to a subject fans a copy to every matching subscription (nothing is "taken out"); durable consumers track their own ack progress over a retained log, and queue groups over one consumer give SQS-style competing-consumer dispatch when you want exactly-one delivery. Ack advances the consumer, it never deletes from the stream — deletion is stream retention policy (`limits` / `workqueue` / `interest`). So the same event can fan out to every watching session *and* be work-queued to a single event job.
