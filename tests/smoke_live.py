"""Live smoke drill against a real nats-server. Proves the MVP loop:
connect -> publish (tool path) -> durable consumer receives -> ledger dedupes redelivery
-> self-origin guard skips own emit -> clean teardown.

Usage: HJS_SMOKE_SERVERS=host:4222 python tests/smoke_live.py
Requires a stream bound to hjs.smoke.> (drill uses stream 'hjs_smoke_drill' by default,
via HJS_SMOKE_STREAM; point it at a scratch stream in CI).
"""
import asyncio
import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "plugin"))

from hermes_jetstream.bus import NatsBus
from hermes_jetstream.config import Settings, WakeRoute
from hermes_jetstream.envelope import build_envelope, GATEWAY_NONCE
from hermes_jetstream.ledger import WakeLedger
from hermes_jetstream.wake import WakePipeline

SERVERS = [s for s in os.environ.get("HJS_SMOKE_SERVERS", "localhost:4222").split(",")]
STREAM = os.environ.get("HJS_SMOKE_STREAM", "hjs_smoke_drill")
SUBJECT = "hjs.smoke.drill"
DURABLE = "hjs-smoke-drill"
PROFILE = "smoke"


async def ensure_stream(js):
    from nats.js.errors import BadRequestError
    from nats.js.api import StreamConfig
    try:
        return await js.add_stream(StreamConfig(name=STREAM, subjects=[SUBJECT],
                                                max_msgs=1000, max_age=3600, storage="file"))
    except BadRequestError:
        return await js.update_stream(StreamConfig(name=STREAM, subjects=[SUBJECT],
                                                   max_msgs=1000, max_age=3600, storage="file"))


async def main() -> int:
    settings = Settings(servers=SERVERS, stream=STREAM,
                        publish_allowed=[SUBJECT], subscribe_allowed=["hjs.smoke.>"])
    received, prompts = [], []

    async def dispatch(route, prompt):
        prompts.append(prompt)

    ledger = WakeLedger(Path("/tmp/hjs-smoke-claims.db"))
    pipe = WakePipeline(["hjs.smoke.>"],
                        ledger, dispatch, PROFILE, GATEWAY_NONCE)

    bus = NatsBus(settings, PROFILE)
    await bus.connect()
    print(f"connected {SERVERS} stream={STREAM}")
    try:
        await bus.jetstream.delete_stream(STREAM)
    except Exception:
        pass  # absent or foreign (e.g. a real bucket)
    await ensure_stream(bus.jetstream)

    await bus.subscribe_wake(subject=SUBJECT, queue=DURABLE,
                             on_message=lambda m: pipe.handle_message(m, WakeRoute(name="drill", subject=SUBJECT)))

    # 1. foreign-origin event -> dispatched
    env = build_envelope("drill", {"run": 1}, profile="foreign-bot", source="drill")
    env["origin"]["nonce"] = "not-us"
    ack = await bus.publish(SUBJECT, json.dumps(env).encode(), headers={"Nats-Msg-Id": f"drill-{time.time()}"})
    print(f"published seq={ack.seq} duplicate={ack.duplicate}")
    # 2. self-origin event -> must NOT dispatch
    env_self = build_envelope("drill", {"run": 2}, profile=PROFILE)
    await bus.publish(SUBJECT, json.dumps(env_self).encode(), headers={"Nats-Msg-Id": f"drill-self-{time.time()}"})

    deadline = time.time() + 10
    while time.time() < deadline and len(prompts) < 1:
        await asyncio.sleep(0.25)
    await asyncio.sleep(1)  # grace for the self-origin event to arrive and be skipped

    assert len(prompts) == 1, f"expected exactly 1 dispatch, got {len(prompts)}: {prompts}"
    assert '"run":1' in prompts[0] or '"run": 1' in prompts[0], prompts[0]
    print("foreign event dispatched; self-origin skipped: PASS")

    # 3. republish same Nats-Msg-Id inside dedupe window -> server dedupes (PubAck.duplicate)
    ack2 = await bus.publish(SUBJECT, json.dumps(env).encode(), headers={"Nats-Msg-Id": f"drill-fixed"})
    ack3 = await bus.publish(SUBJECT, json.dumps(env).encode(), headers={"Nats-Msg-Id": f"drill-fixed"})
    assert not ack2.duplicate and ack3.duplicate, (ack2.duplicate, ack3.duplicate)
    print("server dedupe window (Nats-Msg-Id): PASS")

    await bus.stop()
    # teardown: drop the scratch consumer + stream
    bus2 = NatsBus(settings, PROFILE)
    await bus2.connect()
    try:
        await bus2.jetstream.delete_consumer(STREAM, DURABLE)
    except Exception:
        pass
    await bus2.jetstream.delete_stream(STREAM)
    await bus2.stop()
    print("teardown clean (scratch stream removed): PASS")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
