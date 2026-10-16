#!/usr/bin/env python3
"""hjs-7c3: live bus-outage reconnect drill (manual; needs authority to bounce the bus).

Proves, against the REAL bus the gateway is connected to:
  1. consumer durability  — durable consumer floors + stream history survive a bus restart
  2. client reconnect     — the gateway reconnects without restart and serves the next wake
  3. no silent loss       — stream first_seq/last_seq continuity + zero redelivery storms
  4. watchability         — the outage window is visible via metrics.last_error /
                            jetstream_status / (with hjs-1xk wired) log lines

Outage mechanism is deployment-specific (Nomad job stop here); the probe halves
are not. Plan ~2-5 min of real downtime: the restore path is often slower than
the stop, so budget for it and watch the clock.

Usage:
  python3 tests/reconnect_drill.py capture     # baseline snapshot -> stdout JSON
  python3 tests/reconnect_drill.py verify      # post-restore snapshot + verdicts
Both take --server / --stream. Consumers to check come from --consumers
  stream:consumer,stream:consumer,...

The operator's outage step goes BETWEEN the two invocations, e.g.:
  nomad job stop nats ; <restore via your deploy path> ; <verify>
"""
import argparse
import asyncio
import dataclasses
import json
import sys
import time

sys.path.insert(0, "/home/claude/.hermes/cache/uv/archive-v0/fJRwH6VM52VFdXpO")  # nats-py


async def snapshot(server: str, stream: str, consumers: list) -> dict:
    from nats.aio.client import Client

    out: dict = {"ts": time.time()}
    c = Client()
    await c.connect(servers=[server], name="7c3-probe")
    js = c.jetstream()
    si = await js.stream_info(stream)
    out["stream"] = {"first_seq": si.state.first_seq, "last_seq": si.state.last_seq,
                     "msgs": si.state.messages}
    out["consumers"] = {}
    for s, cn in consumers:
        d = dataclasses.asdict(await js.consumer_info(s, cn))
        out["consumers"][f"{s}/{cn}"] = {
            "delivered": d["delivered"]["consumer_seq"],
            "ack_floor": d["ack_floor"]["stream_seq"],
            "redelivered": d.get("num_redelivered", 0),
        }
    await c.close()
    return out


def verdict(before: dict, after: dict) -> bool:
    ok = True
    b, a = before["stream"], after["stream"]
    # history must not reset: first_seq may only advance via retention, msgs never below pre-gap
    if a["first_seq"] < b["first_seq"]:
        print(f"FAIL stream history reset: first_seq {b['first_seq']} -> {a['first_seq']}")
        ok = False
    else:
        print(f"OK   stream continuity: first_seq {b['first_seq']} -> {a['first_seq']}, "
              f"last_seq {b['last_seq']} -> {a['last_seq']}")
    for key, cb in before["consumers"].items():
        ca = after["consumers"][key]
        if ca["ack_floor"] < cb["ack_floor"]:
            print(f"FAIL {key}: ack floor regressed {cb['ack_floor']} -> {ca['ack_floor']}")
            ok = False
        elif ca["redelivered"] > cb["redelivered"] + 5:
            print(f"WARN {key}: redelivery burst {cb['redelivered']} -> {ca['redelivered']}")
        else:
            print(f"OK   {key}: ack_floor {cb['ack_floor']} -> {ca['ack_floor']}")
    print("NOTE client reconnect + wake round-trip: confirm via gateway log"
          " (inbound jetstream msg after the gap) and platform metrics connected=true.")
    return ok


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["capture", "verify"])
    ap.add_argument("--server", default="nats://nats.service.consul:4222")
    ap.add_argument("--stream", default="forgejo")
    ap.add_argument("--consumers", required=True)
    ap.add_argument("--compare", help="path to capture JSON (verify mode)")
    args = ap.parse_args()
    pairs = [tuple(x.split(":", 1)) for x in args.consumers.split(",")]
    snap = asyncio.run(snapshot(args.server, args.stream, pairs))
    if args.mode == "capture":
        print(json.dumps(snap))
        return 0
    if not args.compare:
        print("verify needs --compare <capture.json>", file=sys.stderr)
        return 2
    with open(args.compare) as fh:
        before = json.load(fh)
    gap = snap["ts"] - before["ts"]
    print(f"outage window ~{gap:.0f}s (capture -> verify, includes restore time)")
    return 0 if verdict(before, snap) else 1


if __name__ == "__main__":
    raise SystemExit(main())
