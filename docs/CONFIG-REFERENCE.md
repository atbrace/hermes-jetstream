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
