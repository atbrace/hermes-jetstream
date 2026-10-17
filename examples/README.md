# Examples

Drop-in `settings` blocks. Each example names its own subjects, so add them to your
`subscribe_allowed` and to a stream you own (the plugin never creates one), e.g.:

```bash
nats stream add hermes --subjects 'hermes.> ci.> deploy.> team.notes.>' --storage file --max-msgs -1
```

## 1. Wake the agent when CI goes red

```yaml
subscribe_allowed: ["ci.result.red"]
wake:
  - name: ci-red
    subject: ci.result.red
    prompt: "CI red on {subject}. Payload: {payload}. Pull the failing log, find root cause, report."
    rate_per_min: 3        # a red-flaky suite can't spam the agent
    min_interval_s: 120    # one wake per 2-minute red burst
```

The gateway session `jetstream/ci-red` opens and runs the prompt. The plugin never
creates the stream; your CI publishes `{"job":"api","branch":"main"}` to
`ci.result.red` (e.g. `nats pub ci.result.red '{"job":"api"}'` from a pipeline step).

## 2. Event job: fire an existing cron job on deploy-finished

```yaml
subscribe_allowed: ["deploy.finished"]
wake:
  - name: post-deploy-audit
    subject: deploy.finished
    job: nightly-audit        # existing cron job (id or name)
    profile: ops              # whose cron store owns it
```

Same semantics as the webhook adapter's `cron_job` routes: the job keeps its own
prompt/skills/model/delivery; the event payload rides as transient per-run context.
At-most-once claiming happens inside the cron layer.

## 3. Cross-agent chatter: two Hermes sessions over one bus

Profile A's session, mid-task:

```
jetstream_watch(subject="team.notes.>", note="review thread")
```

Profile B (any process, even CLI) emits:

```
nats_publish(subject="team.notes.design", payload={"topic":"sidebar width","link":"#42"})
```

A's session gets the note injected as a turn within milliseconds — no polling, no
shared session. TTL (default 1h) bounds the subscription; `jetstream_list` shows
remaining time. `nats_fetch(subject="team.notes.>", timeout_seconds=5)` replays the
durable history on demand instead.

## 4. Mirror Hermes' own lifecycle for dashboards/automation

```yaml
publish_allowed: ["hermes.>"]
lifecycle_emit: ["session.start", "session.end", "subagent.stop"]
```

Emits thin `hermes.session.start` / `hermes.subagent.stop` events (whitelisted fields
only — never message bodies). Fan out to `hermes.>` from a dashboard consumer, or wake
another Hermes on every completed subagent via a wake route on `hermes.subagent.stop`.
