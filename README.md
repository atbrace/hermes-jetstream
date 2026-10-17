# hermes-jetstream

NATS JetStream plugin for [Hermes Agent](https://hermes-agent.nousresearch.com). It connects agents to your event bus: NATS events wake sessions and fire jobs, and agents publish events that the rest of your system consumes. No core changes.

**Status: release candidate.** Config reference: [`docs/CONFIG-REFERENCE.md`](docs/CONFIG-REFERENCE.md) · Examples: [`examples/`](examples/) · Changelog: [`CHANGELOG.md`](CHANGELOG.md).

## What it does

- **Watch in-session.** `jetstream_watch(subject, note)` injects matching events into your live session as turns. No polling. TTL-bounded, allowlisted.
- **Wake routes and event jobs.** Config-declared `wake:` routes turn subjects into session wakes: template prompts, rate rails that ack and drop excess events, idempotent at-least-once delivery. A route can instead fire an existing cron job via the `job:` field.
- **Emit.** `nats_publish` from any session, allowlisted, secret-scanned, thin envelope. An opt-in lifecycle tap mirrors Hermes events onto `hermes.>` subjects, so other services can watch the agent.
- **Replay.** `nats_fetch` does one server-held pull (timeout up to 300 s), not a poll loop.

Together these make the agent an ordinary citizen of your event flow. Anything on the bus can start it, it can hand work to exactly one consumer through a queue group, and it reports what it does as plain NATS messages.

## Design stance

- **General purpose by rule.** No assumptions about your infrastructure: server URLs, credentials, streams, and subjects are all plugin config. Tested against stock `nats:2-alpine`; anything that speaks NATS + JetStream works.
- The plugin runs as a gateway-resident supervisor (`kind: platform` plugin). It uses durable consumers with manual ack and an idempotent wake ledger, so delivery is at-least-once and deduped.
- Publishing is default-deny through the subject allowlist. Loop breakers (no-self-wake) are on by default.

## Quickstart

Point the plugin at a NATS server that has JetStream enabled, and create a stream over the subject space you want agents to see (the plugin never creates one):

```bash
nats stream add hermes --subjects 'hermes.> demo.>' --storage file
pip install "nats-py>=2.7,<3"   # + nkeys for nkey/creds auth
```

No server yet? `docker run -d -p 4222:4222 nats:2-alpine -js` gives you one in seconds.

Add the plugin settings (minimum: `servers` and `stream`, see below), set `platforms.jetstream.enabled: true`, and restart the gateway. Then run `jetstream_watch(subject="demo.>")` from a session and publish something:

```bash
nats pub demo.hello '{"msg":"wake up"}'
```

The event arrives in the session as a turn. From there, add `wake:` routes to run work with no session open, and `lifecycle_emit` to let the rest of your system watch the agent.

## Configuration

All settings live under `plugins.entries.hermes-jetstream.settings`. Full reference: [docs/CONFIG-REFERENCE.md](docs/CONFIG-REFERENCE.md).

```yaml
servers: ["localhost:4222"]        # any nats-client-url
stream: hermes                     # must bind your subject space
publish_allowed: ["hermes.agent.>"]     # empty = publishing DENIED
subscribe_allowed: ["hermes.>", "demo.>"] # gates wake routes AND nats_fetch
wake:                                       # subject -> wake routes
  - name: demo-hello
    subject: demo.hello
    prompt: "Event on {subject}: {payload}. Handle it."
    rate_per_min: 5
  - name: demo-job                      # event job: fire an existing cron job instead
    subject: demo.finished
    job: nightly-audit                  # cron job id or name (execute_job_for_event rail)
    profile: default                    # optional: profile owning the job
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
