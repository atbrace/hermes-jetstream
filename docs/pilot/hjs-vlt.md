# hjs-vlt — nats-py version traps: verification log

Bead: hjs-vlt. Question: what exact pinned-version nats-py API surface does this
repo rely on, and which signatures have bitten the piloting lanes?

## Pin

- `plugin/hermes_jetstream/plugin.yaml` → `nats-py>=2.7,<3`; CI installs the same range (`.github/workflows/ci.yml`). No requirements.txt/lockfile.

## Verification method

Introspected the nats-py actually installed on this box (`nats-py 2.16.0`, satisfies the pin)
with `inspect.signature` / `dataclasses.fields`; cross-checked the floor of the pin
(`v2.7.0`) and `v2.8.0`/`v2.9.0`/`v2.10.0` against the GitHub tag sources (raw fetch, no side effects).

## Findings (all empirically verified)

1. `JetStreamContext.get_msg(stream_name, seq=None, subject=None, direct=False, next=False)`
   — first parameter is **`stream_name`**, positional, across the whole pin range
   (identical in v2.7.0 and v2.16.0). `js.get_msg(stream="x", seq=1)` → verified
   `TypeError: JetStreamManager.get_msg() got an unexpected keyword argument 'stream'`.
   The trap is the inconsistency: `publish()`, `subscribe()`, `pull_subscribe()` all take
   `stream=` — `get_msg` alone does not.
2. `RawStreamMsg` (return of `get_msg`, `nats.js.api`) — dataclass fields
   `[subject, seq, data, hdrs, headers, stream, time]` (2.16.0). `.seq` and `.headers`
   (decoded dict) confirmed; `.hdrs` is the raw-bytes twin; `.sequence` is a property
   alias of `.seq`; there is no `.payload`. `time` is absent at v2.7.0 (added later) —
   do not read it unguarded on the floor of the pin.
3. **Pin-floor bug found:** `ConsumerConfig(backoff=...)` was introduced in **v2.8.0**
   (0 occurrences in v2.7.0's `api.py`, 5 in v2.8.0's). `bus.subscribe()` passes
   `backoff=[1.0, 5.0, 30.0, 120.0]`, so the declared floor `>=2.7` can crash the wake
   consumer with an unexpected-kwarg TypeError. Effective floor: 2.8. (Docs lane —
   pin not changed; flagged for the owner.)
4. `js.publish(..., timeout=..., headers=...)` → `PubAck` fields `[stream, seq, duplicate, ...]`
   — matches `tools.py` / `smoke_live.py` / `auth_matrix.py` use of `ack.stream/.seq/.duplicate`. Stable.
5. `PullSubscription.fetch(batch=1, timeout=5)` — matches `bus.fetch`. Stable.
6. `Msg.metadata` is a **property**, not a callable (verified: `Msg.metadata` is a
   `property` object; `md.sequence` is a `SequencePair` with `.stream`/`.consumer`;
   `md.num_delivered` present) — matches `wake._msg_identity`, which already carries a
   code comment about it.
7. No `_loop` attribute on the nats client (bus.py `loop` property docstring states
   nats-py never stores the loop on the client); loop must be captured around `connect()`.
8. All other imports the repo makes (`nats.errors.TimeoutError`,
   `nats.js.errors.NotFoundError/BadRequestError`, `nats.js.api.StreamConfig`,
   `DeliverPolicy.BY_START_SEQUENCE`, `ConsumerConfig.opt_start_seq/max_ack_pending`)
   exist unchanged at v2.7.0 and v2.16.0.

## Verdict

Traps section shipped in `docs/CONFIG-REFERENCE.md` ("nats-py version traps").
One actionable defect surfaced: the `>=2.7` floor understates the real requirement (2.8)
because of `ConsumerConfig.backoff` — owner call to bump the pin.
