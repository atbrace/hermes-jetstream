# hermes-jetstream

NATS JetStream plugin for [Hermes Agent](https://hermes-agent.nousresearch.com). Event-bus integration with no core changes.

**Status: release candidate.** Config reference: [`docs/CONFIG-REFERENCE.md`](docs/CONFIG-REFERENCE.md) · Examples: [`examples/`](examples/) · Changelog: [`CHANGELOG.md`](CHANGELOG.md).

## What it does

- **Watch in-session.** `jetstream_watch(subject, note)` injects matching events into your live session as turns. No polling. TTL-bounded, allowlisted.
- **Wake routes and event jobs.** Config-declared `wake:` routes turn subjects into session wakes: template prompts, rate rails that ack and drop excess events, idempotent at-least-once delivery. A route can instead fire an existing cron job via the `job:` field.
- **Emit.** `nats_publish` from any session, allowlisted, secret-scanned, thin envelope. An opt-in lifecycle tap mirrors Hermes events onto `hermes.>` subjects.
- **Replay.** `nats_fetch` does one server-held pull (timeout up to 300 s), not a poll loop.

## Design stance

- **General purpose by rule.** No assumptions about your infrastructure: server URLs, credentials, streams, and subjects are all plugin config. Tested against stock `nats:2-alpine` in Docker; anything that speaks NATS + JetStream works.
- The plugin runs as a gateway-resident supervisor (`kind: platform` plugin). It uses durable consumers with manual ack and an idempotent wake ledger, so delivery is at-least-once and deduped.
- Publishing is default-deny through the subject allowlist. Loop breakers (no-self-wake) are on by default.

## Quickstart

```bash
docker run -d -p 4222:4222 nats:2-alpine -js
nats stream add hermes --subjects 'hermes.> ci.>' --storage file   # you own the stream; the plugin never creates one
pip install "nats-py>=2.7,<3"                                       # + nkeys for nkey/creds auth
```

Enable the platform (`platforms.jetstream.enabled: true`), restart the gateway, then run `jetstream_watch(subject="ci.>")` from any session. Now run `nats pub ci.result.red '{"job":"api"}'` and watch the wake arrive.

## Configuration

All settings live under `plugins.entries.hermes-jetstream.settings`. Full reference: [docs/CONFIG-REFERENCE.md](docs/CONFIG-REFERENCE.md).

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

One adapter per profile that enables the platform. Each profile's `settings` (servers, allowlists, wake routes, auth) produce their own connection, and durable consumer names carry the profile's route names, so profiles never share a queue group. Two profiles pointing at the same `wake[].durable` on the same server intentionally form one queue group (shared dispatch). That is NATS queue-group semantics, not an accident. Watches are per-gateway-process in-memory state; only the profile whose gateway loaded the platform serves them.

## Testing

```bash
python -m unittest tests.test_core        # stdlib-only, no network
HJS_SMOKE_SERVERS=localhost:4222 python tests/smoke_live.py   # live drill, self-cleaning
```

## The JetStream model in one paragraph

JetStream layers durable consumers on plain NATS pub/sub. Publishing to a subject fans a copy to every matching subscription, and no consumer takes the message away. Durable consumers track their own ack progress over a retained log, and queue groups over one consumer give SQS-style competing-consumer dispatch when you want exactly-one delivery. Ack advances the consumer, it never deletes from the stream. Deletion is stream retention policy (`limits` / `workqueue` / `interest`). So the same event can fan out to every watching session and be work-queued to a single event job.
