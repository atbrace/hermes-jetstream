# Config reference

All keys live under `plugins.entries.hermes-jetstream.settings` (platform config under
`platforms.jetstream.extra` overrides `servers`/`token` when the platform is enabled).
`${VAR}` in any string expands from the Hermes `.env`.

| Key | Type | Default | Meaning |
|---|---|---|---|
| `servers` | list | `["localhost:4222"]` | Any nats-client-urls; TLS URLs (`tls://`) honoured via `tls.enabled`. |
| `stream` | str | `hermes` | JetStream stream the wake consumers and `nats_fetch` bind. **The plugin never creates or mutates it — it must exist and bind your subjects.** |
| `publish_allowed` | list | `[]` | Subject patterns `nats_publish` may target. Empty = publishing DENIED (fail closed). No wildcards in publish targets themselves. |
| `subscribe_allowed` | list | `[]` | Gates wake routes, `nats_fetch`, and `jetstream_watch` patterns. |
| `wake` | list | `[]` | Wake routes (below). Item schema: [`wake-routes.schema.json`](wake-routes.schema.json). Dry-run your whole settings block before the restart gate: `python3 scripts/validate_settings.py settings.json` (exit 1 names every problem; see Validating before restart). |
| `wake[].name` | str | required | Route identity (also bucket/coalesce key). |
| `wake[].subject` | str | required | Literal or wildcard; must match `subscribe_allowed`. |
| `wake[].durable` | str | `jetstream-<name>` | Durable consumer **and** queue group name (NATS requires equality for queue subs). Same value across gateways = one shared queue group. |
| `wake[].prompt` | str | `JetStream event on {subject}. Payload: {payload}` | Template; single-brace `{subject} {payload} {seq} {ts}` only, payload truncated. |
| `wake[].max_payload_chars` | int | `2000` | Payload truncation into the prompt. |
| `wake[].rate_per_min` | int | `10` | Per-minute shed threshold (token bucket); excess is ACKED AND DROPPED (shed), like the coalesced path. |
| `wake[].min_interval_s` | float | `0` | Coalescing: events closer than this are acked and dropped. |
| `wake[].job` | str | `""` | Set to a cron job id/name → route becomes an EVENT JOB: fires the job via the same rail as `cronjob(action='run', prompt=…)` instead of waking a session. |
| `wake[].profile` | str | `""` | Profile owning the job (event jobs only). |
| `auth.token` | str | `""` | Static token. Precedence: token > creds > nkey > user/pass. |
| `auth.creds` | str | `""` | Resolver-style creds file (JWT+nkey) **or** seed-only file (auto-routed; nats-py's plain reader hangs on seed files — the plugin sniffs and fixes). |
| `auth.nkey` | str | `""` | `SU…` seed, client signs the server nonce. |
| `auth.user` / `auth.password` | str | `""` | Username auth. |
| `tls.enabled` | bool | `false` | Wrap the connection in TLS. |
| `tls.ca` | str | system CA | CA bundle path. |
| `tls.cert` / `tls.key` | str | `""` | Client cert pair for mTLS. |
| `tls.insecure_skip_verify` | bool | `false` | Testing escape hatch; disables hostname + chain verification. |
| `lifecycle_emit` | list | `[]` | Mirror these lifecycle kinds to `<lifecycle_prefix>.<kind>`: `session.start`, `session.end`, `session.reset`, `agent.turn.start`, `subagent.stop`, `agent.stopped`. Requires the prefix in `publish_allowed` or the tap stays disabled. |
| `lifecycle_prefix` | str | `hermes` | Subject prefix for lifecycle mirror events. |
| `max_payload_bytes` | int | `32768` | Client-side emit cap (server `max_payload` also applies). |
| `heartbeat_s` | int | `0` | Opt-in liveness heartbeat: publish a status snapshot every N seconds. `0` = off. Requires `heartbeat_subject`; disabled with a warning if the subject isn't publish-allowlisted (fail closed). |
| `heartbeat_subject` | str | `""` | Subject for heartbeat publishes (e.g. `hermes.heartbeat`). Add it to `publish_allowed`. |

## Publish dedupe — per-stream, not per-subject

JetStream dedupes `Nats-Msg-Id` across the **whole stream**, not per subject:
two publishes carrying the same msg-id on different subjects bound to one
stream are treated as duplicates and the server **silently drops the second**
(ack reports `duplicate=True`, the event never lands). The plugin therefore
salts its msg-id with the subject (`_msg_id = sha256(subject + NUL + body)`),
so identical payloads on different subjects can never collide, while an
in-subject republish of the same envelope still dedupes as intended. If you
publish to the plugin's stream yourself with your own `Nats-Msg-Id`, salt it
by subject too.
=======
## Validating before restart

A typo'd wake route otherwise costs a full restart cycle to discover: the
gateway logs `adapter construction refused (bad config?)` and the platform
stays down. Check the block first, no NATS connection involved:

```bash
python3 scripts/validate_settings.py settings.json   # the JSON under plugins.entries.hermes-jetstream.settings
```

Exit 0 = the loader accepts the block; exit 1 prints one `ERROR:` line per
problem naming its path (`wake[0] ...`, `heartbeat_subject ...`). It reuses
the plugin's own `Settings.from_plugin_ctx`/`WakeRoute.from_dict` — no second
parser — and additionally catches the silent-runtime-failure shapes the
parser can't see: duplicate route names, a route subject matching no
`subscribe_allowed` pattern (wakes would ack-and-drop), a heartbeat subject
missing from `publish_allowed`. `docs/wake-routes.schema.json` mirrors the
route item shape for editor validation; the parser in `config.py` remains the
source of truth.

## Runtime wake routes (`jetstream_wake`)

`jetstream_wake(action="register", name, subject, …)` adds a DURABLE wake route at
runtime — no config edit, no gateway restart. Rows persist in a plugin-owned JSON
file (`<plugin-data>/wake-routes.json`, same durability class as the wake ledger)
and are rebound at every platform bind; `action="list"` and `action="unregister"`
(idempotent) round out the verb. Runtime rows appear in `jetstream_status` next to
config routes, tagged `source: "runtime"`.

The gates are the config gates, not weaker: the subject must match
`subscribe_allowed` (registration is refused loudly otherwise, never silently
dropped), the row parses through the same `WakeRoute` rules (prompt template
policy, rate rails, durable-name law), and the pipeline's rate/coalesce/dedupe
rails run unchanged. Names are namespaced to `rt-<name>` (durable
`jetstream-rt-<name>`) so a runtime route can never shadow a config route; a name
or durable that still collides with a config route is refused. Optional
`max_age_s` self-retires abandoned routes: expiry is swept at platform bind.
Registration caps at 16 routes; re-registering an existing name updates it.
From a CLI/cron process (no live adapter) the row persists and binds at the next
platform start. `jetstream_watch` remains the right tool for ephemeral,
session-scoped interest; this verb is for durable consumers an agent owns.

## Auth

Wake sessions are authorized through the standard gateway allowlist. Set
`JETSTREAM_ALLOWED_USERS` (comma-separated, entries like `jetstream:<route-name>` or
`*`) to allow wake routes for this platform without touching the global
`GATEWAY_ALLOWED_USERS`. Route names you do not list stay default-deny.

### `nats_fetch` resume semantics

`nats_fetch` reads the subject from its tail on the first call per process
(history is what `wake` routes and watches deliver; fetch is for what happens
next). Within the same gateway process, later calls resume after the last
returned message, so successive fetches never re-deliver what they already
returned. The cursor is in-process only: a gateway restart resets it to
tail-once, and other processes keep their own cursors. For exactly-once across
restarts, use a wake route (durable consumer) or pass your own durable consumer
via the API.

## Watchability (no external monitoring required)

Three plugin-owned surfaces answer "is my watch alive?":

1. **`jetstream_status` tool** — per-route counters (`dispatched`/`shed`/`coalesced`/
   `skipped_*`/`failed`/`lost`), `attempted`, last-attempt and last-dispatch clocks,
   connection state, `reconnect_count`, active watches, lifecycle tap counters when the
   tap is on, and the last client error. Every route you configured shows a row from the
   moment the platform binds (zeroed), so an absent row means the platform never came up.
2. **`gateway_state.json → platforms.jetstream.metrics`** — the same snapshot, mirrored
   every 60 s for anything that reads files (dashboard, cron probe, your own collector).
3. **Heartbeat events** — with `heartbeat_s` set, a status JSON lands on your subject
   every interval; a plain `nats watch hermes.heartbeat` (or any consumer) is then a
   complete dead-man: no message within 2× the interval means the platform is dead.

A wake that dies is never silent: final-delivery exhaustion, and claims orphaned by a
crash (found at startup reconcile), are logged at WARNING, written to the ledger
(`status='lost'`), and counted as route `lost` (hjs-xq5, hjs-25g). There is no DLQ
subject: the plugin records the loss, it does not resurrect the event. Replay after a
recorded loss is your call — `nats_fetch` the subject window, or republish upstream.

Wire whatever dead-man you already run to whichever surface is cheapest for you. The
plugin distinguishes idle from shed from broken; *alerting on it* stays your side, and
the watchdog must be separate from the thing it watches.

## Inspecting receipts without a nats CLI on the broker

Pilots on a containerized broker hit two walls: the stock `nats` server image ships no
CLI, and the JetStream monitor (`/jsz`) exposes account/stream **aggregates only** —
per-stream and per-consumer detail 404s there. The official `natsio/nats-box` image is
the CLI sidecar: run it against the same server with the same posture as your plugin
settings (pick the auth flag matching your `auth.*` precedence, add `--tlscacert`/
`--tlsclientcert`/`--tlsclientkey` to match `tls.*`):

```sh
# auth flag: --creds /creds/jetstream.creds | --token "$TOKEN" | --nkey <SU…seed>
# tls flags: --tlscacert/--tlsclientcert/--tlsclientkey to match settings tls.*
docker run --rm -it --network host \
  -v "$PWD/creds:/creds:ro" \
  natsio/nats-box:latest \
  nats --server nats://localhost:4222 --creds /creds/jetstream.creds stream info hermes
```

Copy-paste receipt flow (stream name = your `stream` setting; wake durable/queue =
`jetstream-<wake[].name>`; drop `--server/--creds` if your alias or local broker covers it):

```sh
nats --server nats://localhost:4222 stream ls                                # stream exists?
nats --server … stream info hermes                                           # msgs, first/last seq, subjects
nats --server … consumer info hermes jetstream-<route>                       # ack floor, num_redelivered, pending
nats --server … stream get hermes --last-for 'ci.result.red'                # last event on a subject (payload shown)
nats --server … stream subjects hermes "hermes.>"                            # per-subject counts
nats --server … consumer report hermes                                       # all wake consumers in one view
```

Then join to the plugin side: the ledger in `<plugin-data>/wake-claims.db` keys rows as
`<stream>:<consumer>:<stream_seq>` (`sqlite3 wake-claims.db "select * from wake_claims
order by claimed_at desc limit 10"`), so a stream seq from `consumer info` maps straight
to a `claimed`/`done` wake. Seq present in the stream but no row → the consumer never
delivered it; `done` row + no session wake → check `jetstream_status` counters.
Full runnable version: [examples/nats-box-receipts.md](../examples/nats-box-receipts.md).
=======
## nats-py version traps (verified against the pin, nats-py 2.16.0)

The plugin declares `nats-py>=2.7,<3` (`plugin.yaml`). Signatures below were
introspected against the installed 2.16.0 and cross-checked at the v2.7.0 floor
(log: `docs/pilot/hjs-vlt.md`). These are the traps that cost piloting lanes time:

- **`get_msg()` names its stream argument `stream_name`, not `stream`** — the only
  JetStream call that does; `publish`/`subscribe`/`pull_subscribe` all use `stream=`.
  Passing it by keyword raises `TypeError: ... unexpected keyword argument 'stream'`.
  ```python
  msg = await js.get_msg("hermes", seq=42)         # correct (positional stream)
  msg = await js.get_msg(stream="hermes", seq=42)  # TypeError
  ```
- **`RawStreamMsg` (what `get_msg` returns) exposes `.seq` / `.headers`, not `.payload`.**
  Fields: `subject, seq, data, hdrs, headers, stream, time`; `.sequence` is an alias of
  `.seq`, `.hdrs` is the raw-bytes twin of the decoded `.headers` dict. `.time` does not
  exist at the v2.7.0 floor — read it guarded or not at all.
- **`ConsumerConfig(backoff=...)` needs nats-py ≥ 2.8.** `bus.subscribe()` passes
  `backoff`, so the declared `>=2.7` floor can crash a wake consumer with an
  unexpected-kwarg `TypeError` on 2.7.x; install 2.8+ (effective floor).
- **`Msg.metadata` is a property, not a callable.** Use `msg.metadata.sequence.stream`;
  `msg.metadata()` raises at call time, not import time.
- **The client has no `_loop` attribute.** nats-py binds its transport to whatever loop
  runs `connect()` but never stores it; capture `asyncio.get_running_loop()` right after
  `await nc.connect(...)` (that's what `bus.loop` does — reading `_nc._loop` is silently `None`).

Everything else the plugin/drills call (`publish(..., headers=)` → `PubAck.stream/.seq/.duplicate`,
`pull_subscribe(..., config=ConsumerConfig(opt_start_seq=...))`, `fetch(batch, timeout=)`,
`nats.errors.TimeoutError`, `nats.js.errors.NotFoundError/BadRequestError`,
`StreamConfig`, `DeliverPolicy.BY_START_SEQUENCE`) is unchanged across 2.7.0 → 2.16.0.

## Gate parity (CI)

`scripts/ci.sh` IS the gate: unit tests, the generality grep-gate, syntax, and doctor.
`.github/workflows/ci.yml` adds the live jobs (smoke, auth matrix, cluster drill, silence
drill) on top by booting its own scratch nats-server. A forge with no
runner still gates honestly: run `./scripts/ci.sh` before merging — it is byte-for-byte
the same hermetic check the `CI / core` context runs, and `NATS_BIN=/path/to/nats-server
./scripts/ci.sh` reproduces the `CI / live` additions. No behavior lives only in YAML.
