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
| `wake` | list | `[]` | Wake routes (below). |
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

Three plugin-owned surfaces answer "is my watch alive?" (see `docs/BATTLE-TEST.md` for
why they exist):

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

## Gate parity (CI)

`scripts/ci.sh` IS the gate: unit tests, the generality grep-gate, syntax, and doctor.
`.github/workflows/ci.yml` adds the live jobs (smoke, auth matrix, cluster drill, silence
drill) on top by booting its own scratch nats-server. A Forgejo/GitHub instance with no
runner still gates honestly: run `./scripts/ci.sh` before merging — it is byte-for-byte
the same hermetic check the `CI / core` context runs, and `NATS_BIN=/path/to/nats-server
./scripts/ci.sh` reproduces the `CI / live` additions. No behavior lives only in YAML.
