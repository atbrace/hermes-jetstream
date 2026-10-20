# Agent usage guide — listening is the first move

You are an agent on an event bus. Before you poll, before you cron, before you
ask "should I check again?": the bus can call *you*. Every subject below is a
placeholder (`ops.myco.*`, stream `ops`) — substitute your own; nothing here
assumes any particular infrastructure.

## Pick a surface

| Want | Use | Lifetime | Where state lives |
|---|---|---|---|
| Wake me — or fire a cron job (an **event job**: set the route's `job:` field, config-only; `jetstream_wake` has no `job` arg) — on this subject, forever, config-managed | `wake:` route in settings | until config edit | gateway config; durable consumer |
| Wake me on this subject, but it's *my task's* route, not the operator's | `jetstream_wake(action="register", …)` | until you unregister, or `max_age_s` self-retire | plugin data dir (`wake-routes.json`); durable consumer |
| Inject matching events into **this session** while I work | `jetstream_watch(subject, note)` | TTL (default 3600 s, max 21600 s), ≤16 watches, dies with the gateway | in-process only |
| Read what happened (backlog / last N / one bounded wait) | `nats_fetch(subject, timeout_seconds≤300, batch≤10)` | per call, with an in-process resume cursor | server-held pull, no durable state |
| One event, one waiter, nobody needs to survive downtime | `nats_publish` from the producer + `nats_fetch(timeout=…)` from the caller | the call | nowhere |

Rule of thumb: **watch** while a session is alive to receive it, **runtime wake
route** when the wake must happen with no session open and you own the route's
lifecycle, **config wake route** when the operator owns it, **fetch** when
history matters more than timing, **publish+fetch** for a one-shot handoff
(the caller's `nats_fetch` with a timeout *is* the waitfor — server-held, no
poll loop). All of them are gated by `subscribe_allowed` (wake routes, watches,
fetch alike); publishing is gated separately by `publish_allowed`, empty = denied.

## Find your subjects first

1. `jetstream_status` — always first. Connection state, one row per wake route
   tagged `source: config|runtime` with dispatched/shed/coalesced/lost counters,
   active watches, last error. An absent route row means the platform never
   bound.
2. Your operators' settings answer "what am I *allowed* to touch":
   `subscribe_allowed` (read side) and `publish_allowed` (write side). A route
   whose subject matches no `subscribe_allowed` pattern is refused at
   registration — loudly, never silently.
3. What actually *flows*: the JetStream monitor (`/jsz`) is aggregates-only —
   per-stream and per-consumer endpoints 404 there (measured; see
   CONFIG-REFERENCE "Inspecting receipts"). Use the `nats` CLI (official
   `natsio/nats-box` sidecar when the broker image ships no CLI):
   `nats stream subjects ops "ops.myco.>"` for per-subject counts,
   `nats stream info ops` for the subject list the stream binds.

## Recipes

**Register a runtime route for a subject you should be woken for:**

```python
jetstream_wake(action="register", name="deploy-approve", subject="ops.myco.deploy.requested",
               prompt="Deploy request: {payload}. Review and act.",
               rate_per_min=5, max_age_s=86400)
```

The name is stored as `rt-deploy-approve` (durable `jetstream-rt-deploy-approve`)
— the `rt-` prefix is forced, so you can never shadow a config route. Re-register
an existing name to update it; total runtime routes cap at 16. From a CLI/cron
process (no live adapter) the row still persists and binds at the next platform
start. Wake sessions authorize as `jetstream:rt-<name>` — the operator must have
that in `JETSTREAM_ALLOWED_USERS` (or `*`). Unsure whether your route is
authorized? `python3 scripts/wake_authz_digest.py --home <profile-home>` prints
the allowlist and a per-route AUTHORIZED|DROPPED verdict from disk, no gateway needed.

**Tear it down idempotently at the end of your task** (safe to call even if a
prior run already removed it, or it never bound here):

```python
jetstream_wake(action="unregister", name="deploy-approve")   # also accepts rt-deploy-approve
```

**Watch with a TTL while a task runs** — matching events land in this session
as turns, no polling:

```python
wid = jetstream_watch("ops.myco.build.finished", note="waiting on my build", ttl_seconds=1800, max_msgs=1)
# (subject may be literal, `*` per token, or trailing `>` — NATS has no `#`)
# … on teardown: jetstream_unwatch(watch_id=wid)
```

**Replay the backlog after downtime:** call `nats_fetch(subject, batch=10,
timeout_seconds=10)` repeatedly until it returns "No messages" (batch caps at 10,
so a deep backlog is a loop). The first fetch in a process starts at the
**oldest event the stream still retains** (the ephemeral consumer defaults to
`DeliverPolicy.ALL`), and each call resumes after the last seq it returned — so
successive fetches walk forward without re-delivering. The cursor is in-process
only: a restart resets it to the same oldest-retained walk. For exactly-once
across restarts, use a durable wake route instead.

## The twin-trigger law

A runtime wake route carries two independent clocks, and both must be able to
die quietly:

- **You stop caring first:** `max_age_s` (clamped 1 s – 30 d) self-retires an
  abandoned route — swept at every platform bind. Give agent-owned routes a
  `max_age_s`; a route nobody unregistereds dies on the next restart instead of
  waking you forever. (Expiry is enforced at bind, not by a live timer: an
  expired route can still fire in a gateway that never restarts. Unregister
  when you're done; `max_age_s` is the safety net.)
- **The bus remembers while you sleep:** the durable consumer is server-side
  state. While the gateway is down, matching events pile up un-acked on the
  consumer, not lost — and redeliver when it rebinds. Two honest limits: the
  per-route `rate_per_min` token bucket treats a backlog burst as a storm and
  **sheds the excess (ack and drop)**, and each message has a redelivery budget
  (`max_deliver=5`, backoff 1/5/30/120 s) after which the wake is recorded as
  `lost`, never silently. Sleep short, or replay deliberately with `nats_fetch`.

## Gotchas (all real, all in this repo's docs)

- **Wake semantics are at-most-once per event.** Delivery is at-least-once, but
  claim-then-ack plus the wake ledger means one event fires your route at most
  once — and possibly zero times (shed by the rate rail, dropped by
  `min_interval_s` coalescing, or `lost` after the redelivery budget). There is
  no DLQ: losses are recorded in the ledger and counted in
  `jetstream_status.lost`; replay after a loss is your call (`nats_fetch` or
  republish upstream).
- **Dedupe is per stream, salted by subject.** The plugin sets
  `Nats-Msg-Id = "hermes-" + sha256(subject + NUL + body)[:32]`, so identical
  payloads on *different* subjects never collide, and the same envelope twice on
  one subject is deduped on purpose (the publish reply says
  `duplicate, deduped by server`). If you publish to the plugin's stream
  yourself, salt your msg-ids by subject too.
- **No-self-wake is on by default:** envelopes originated by this plugin profile
  are acked and counted `skipped_self_origin`, so a publish→wake→publish loop
  breaks itself.
- **Prompts are templates, not concat.** `{subject} {payload} {seq} {ts}` only,
  payload truncated at `max_payload_chars` (default 2000). Keep published events
  thin — events, not transcripts; secret-shaped payloads are refused outright.
- **Writing your own nats-py glue around these tools?** Read
  [CONFIG-REFERENCE "nats-py version traps"](CONFIG-REFERENCE.md) first:
  `get_msg` names its stream argument `stream_name` (positional only),
  `RawStreamMsg` has no `.payload`, `msg.metadata` is a property, and
  `ConsumerConfig(backoff=…)` needs ≥ 2.8 despite the declared 2.7 floor.
- **Dry-run settings before every restart:**
  `python3 scripts/validate_settings.py settings.json` kills wake typos before
  the restart gate. Schema mirror for editors:
  [`wake-routes.schema.json`](wake-routes.schema.json); the parser in
  `config.py` is the source of truth.

See also: [CONFIG-REFERENCE](CONFIG-REFERENCE.md) (all keys, runtime routes,
receipts) · [AGENT-PLUGINS-ADAPTER-NOTES](AGENT-PLUGINS-ADAPTER-NOTES.md) (how
the runtime-route pattern works internally) ·
[examples/nats-box-receipts.md](../examples/nats-box-receipts.md).
