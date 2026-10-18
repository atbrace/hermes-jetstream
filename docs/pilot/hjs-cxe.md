# hjs-cxe — JetStream dedupe is per-stream, not per-subject

**Bead:** hjs-cxe (P1, from peer pilot). **Verdict: substance reproduces.**

## The bug

JetStream's `Nats-Msg-Id` dedupe window is scoped to the *stream*, not the
subject. Two publishes carrying the same msg-id on different subjects that are
both bound to one stream are treated as duplicates: the server acks the second
with `duplicate=True` and the event is silently lost.

The plugin derived the msg-id from the body only
(`tools._msg_id(body) = "hermes-" + sha256(body)[:32]`), so a retried/dual
publish of an identical envelope body to two subjects on the `hermes` stream
collides.

## Stdlib repro (no live NATS)

Intercept the wire headers the plugin would send (fake JetStream ctx on the
ephemeral path), publish one identical body to two subjects:

```python
sys.path.insert(0, ".../plugin")
from hermes_jetstream.config import Settings
from hermes_jetstream.tools import JetstreamTools

t = JetstreamTools(Settings(servers=["h:1"], publish_allowed=["ops.>"],
                           subscribe_allowed=["ops.>"]), ctx=None)
sent = []
async def fake_ephemeral(fn):
    class FakeJS:
        async def publish(self, subject, body, timeout=None, headers=None):
            sent.append((subject, headers["Nats-Msg-Id"]))
    return await fn(FakeJS())
t._ephemeral = fake_ephemeral

body = b'{"kind":"event","data":{"id":"X"}}'   # identical body both times
asyncio.run(t._publish("ops.alpha", body))
asyncio.run(t._publish("ops.beta", body))
```

Output (Python 3, repo at ba047ef):

```text
subject=ops.alpha  Nats-Msg-Id=hermes-a3350f4ff827b5538873c34925c59a7d
subject=ops.beta   Nats-Msg-Id=hermes-a3350f4ff827b5538873c34925c59a7d
same header across subjects: True
```

After the fix the same script prints distinct ids
(`hermes-fedebc50…` / `hermes-ed271780…`) — collision gone.

Same header on both subjects ⇒ on a real server the second publish is
deduped away. Note `tools._publish` is the only site that sets
`Nats-Msg-Id`; lifecycle-mirror and heartbeat publishes carry no msg-id and
are unaffected.

## Fix

Salt the msg-id with the subject: `_msg_id(subject, body) =
"hermes-" + sha256(subject + b"\x00" + body)[:32]`. A caller-supplied body
that repeats across subjects now yields distinct wire ids (no cross-subject
collision), while a republish on the *same* subject keeps the same id, so
in-subject idempotent republish still dedupes as before. One hash-input
change, no new config or knobs; docs/CONFIG-REFERENCE.md carries the
per-stream-dedupe warning.
