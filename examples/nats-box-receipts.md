# Receipt inspection with a nats-box sidecar (no CLI on the broker)

For pilots whose broker runs in a container (no `nats` CLI in the image, `/jsz` gives
aggregates only). `natsio/nats-box` carries the CLI; run it against the same server
with the same auth/TLS posture as your plugin settings.

```sh
# flags mirror plugin settings: auth.token/creds/nkey -> --token/--creds/--nkey;
# tls.enabled -> --tlssecure + --tlscacert (+ --tlsclientcert/--tlsclientkey for mTLS)
NATS="nats --server nats://localhost:4222 --creds /creds/jetstream.creds"

docker run --rm -it --network host -v "$PWD/creds:/creds:ro" natsio/nats-box:latest \
  $NATS stream info hermes
```

Replace `hermes` with your `stream` setting and `jetstream-<route>` with your
`wake[].durable` (default `jetstream-<wake[].name>`). The receipt questions, in order:

```sh
$NATS stream ls                                  # stream exists / binds your subjects?
$NATS stream info hermes                         # msg count, first/last seq, subject list
$NATS consumer info hermes jetstream-ci-red      # ack floor, num_redelivered, num_ack_pending
$NATS stream get hermes --last-for 'ci.result.red'   # last event on a subject + payload
$NATS stream subjects hermes "hermes.>"          # per-subject message counts
$NATS consumer report hermes                     # every wake consumer in one view
```

Join to the plugin side: `<plugin-data>/wake-claims.db` keys rows as
`<stream>:<consumer>:<stream_seq>`, so a stream seq from `consumer info` maps to a
`claimed`/`done` wake:

```sh
sqlite3 wake-claims.db "select * from wake_claims order by claimed_at desc limit 10"
```

- Seq in the stream, no ledger row → consumer never delivered it (check gateway up +
  `jetstream_status` `attempted`).
- `done` row but no session wake → authorization or dispatch; check
  `JETSTREAM_ALLOWED_USERS` and `jetstream_status` `skipped_*` counters.
- `claimed` row older than `reclaim_after_s` → dead owner; the next redelivery re-dispatches.
