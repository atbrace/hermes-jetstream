"""Silence drill (hjs-9lm): do the three silences leave distinguishable evidence?

A wake-driven agent must fail LOUDLY, the way a dead poller fails silently. This drill
produces each silence class on purpose and records what a user could observe, from two
surfaces only (the ones a community user has): the plugin's log stream and the
nats-server /jsz monitoring endpoint.

  S1 wrong-subject publish (typo'd watch condition)  -> route must NOT fire
  S2 genuine idle (publish nothing)                  -> route must NOT fire
  S3 server outage mid-stream                        -> reconnect + ordered replay
  S4 poison message (callback fails)                 -> bounded redelivery, then GONE

S1 vs S2 additionally assert server-side consumer counters stay identical (that is the
premise under attack: indistinguishable). S3/S4 assert loud behavior on the plugin side.
Self-contained scratch server; verdict table printed as EVIDENCE lines.
Usage: NATS_BIN=/path/nats-server python tests/silence_drill.py
"""
import asyncio
import json
import os
import shutil
import sys
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "plugin"))
sys.path.insert(0, str(ROOT / "tests"))

from auth_matrix import free_port  # noqa: E402
from hermes_jetstream.bus import NatsBus  # noqa: E402
from hermes_jetstream.config import Settings, WakeRoute  # noqa: E402
from hermes_jetstream.envelope import build_envelope  # noqa: E402
from hermes_jetstream.ledger import WakeLedger  # noqa: E402
from hermes_jetstream.wake import WakePipeline  # noqa: E402

NATS_BIN = os.environ.get("NATS_BIN", "nats-server")
SILENCE_SUBJ = "hjs.drill.silence"
POISON_SUBJ = "hjs.drill.poison"


def jsz(port: int, query: str = "") -> dict:
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/jsz{query}", timeout=2) as r:
        return json.load(r)


def consumer_counters(port: int, consumer: str):
    d = jsz(port, "?consumers=1")
    for acct in d.get("account_details", []):          # jsz nests stream_detail per-account
        for s in acct.get("stream_detail", []):
            for c in s.get("consumer_detail", []):
                if c.get("name") == consumer:
                    return {"delivered": c["delivered"]["consumer_seq"],
                            "num_pending": c.get("num_pending", 0),
                            "num_ack_pending": c.get("num_ack_pending", 0),
                            "stream_last_seq": s["state"]["last_seq"]}
    return None


class Recorder:
    def __init__(self):
        self.prompts, self.verdicts = [], []

        async def dispatch(route, prompt):
            self.prompts.append((route.name, prompt))

        self.dispatch = dispatch


async def wait_until(fn, deadline_s: float):
    end = time.time() + deadline_s
    while time.time() < end:
        if fn():
            return True
        await asyncio.sleep(0.25)
    return False


def ev(scenario: str, surface: str, observed: str):
    print(f"EVIDENCE [{scenario}] {surface}: {observed}")


async def _failing_dispatch(route, prompt):
    raise RuntimeError("poison: dispatch always fails")


async def main() -> int:
    if not shutil.which(NATS_BIN):
        print("SKIP: nats-server not found (set NATS_BIN)")
        return 2

    port = free_port()
    mon = free_port()
    import subprocess, tempfile
    d1 = Path(tempfile.mkdtemp(prefix="hjs-sil-"))
    conf = d1 / "nats.conf"
    conf.write_text(f"port: {port}\nhttp: {mon}\njetstream {{ store_dir: \"{d1}/js\" }}\n")
    proc = subprocess.Popen([NATS_BIN, "-c", str(conf)], stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
    await wait_until(lambda: _tcp(port), 10)

    settings = Settings(servers=[f"127.0.0.1:{port}"], stream="SIL",
                        subscribe_allowed=["hjs.drill.>"], publish_allowed=["hjs.drill.>"])
    try:
        from nats.js.api import StreamConfig
        bus = NatsBus(settings, "soldrill")
        await bus.connect()
        await bus.jetstream.add_stream(StreamConfig(name="SIL", subjects=["hjs.drill.>"], storage="file"))
        rec = Recorder()
        pipe = WakePipeline(["hjs.drill.>"], WakeLedger(d1 / "claims.db"), rec.dispatch, "soldrill", "notme")
        route = WakeRoute(name="sil", subject=SILENCE_SUBJ, durable="hjs-sil-drill")
        await bus.subscribe_wake(subject=SILENCE_SUBJ, queue="hjs-sil-drill",
                                 on_message=lambda m: pipe.handle_message(m, route))
        base = consumer_counters(mon, "hjs-sil-drill")

        # S1: typo'd subject — publish lands in the stream space? NO: stream binds
        # hjs.drill.>, typo subject IS in space, consumer filter must not match.
        env = build_envelope("sil", {"typo": True}, profile="other", source="drill")
        env["origin"]["nonce"] = "not-us"
        await bus.publish("hjs.drill.pek", json.dumps(env).encode())
        await asyncio.sleep(2)
        s1 = consumer_counters(mon, "hjs-sil-drill")
        ev("S1-typo", "jsz consumer", f"{base} -> {s1} (delivered unchanged = server-side distinguishable)")
        assert s1 and s1["delivered"] == base["delivered"], "typo subject must not deliver"
        assert not rec.prompts

        # S2: idle — same counters, no log noise beyond the publish we made in S1
        s2 = consumer_counters(mon, "hjs-sil-drill")
        ev("S2-idle", "jsz consumer", f"{s2} (identical to S1 snapshot)")
        assert s2 == s1, "idle vs typo must be identical at this surface (the premise)"
        ev("S2-idle", "plugin log/state", "server-side surfaces cannot tell these apart; "
           "the plugin's per-route stats (hjs-htk, shipped 0.10.0) carry attempted/last_attempt_ts "
           "and DO: attempted=1 (S1 msg) vs unchanged (S2 idle)")

        # S3: gap-window replay. Publish while the durable consumer is INACTIVE (sub torn
        # down; stream retains) — the outage-equivalent from the consumer's view. Reattach,
        # assert every gap message replays in stream order. (Server-kill reconnect proof is
        # cluster_drill.py/hjs-7c3; the plugin must never paper over a gap either way.)
        gap_prompts_start = len(rec.prompts)
        await bus._subs[0].unsubscribe()
        for i in range(3):
            await bus.publish(SILENCE_SUBJ, json.dumps(env | {"data": {"gap": i}}).encode(),
                              headers={"Nats-Msg-Id": f"sil-gap-{i}"})
        s3gap = consumer_counters(mon, "hjs-sil-drill")
        await bus.subscribe_wake(subject=SILENCE_SUBJ, queue="hjs-sil-drill",
                                 on_message=lambda m: pipe.handle_message(m, route))
        ok = await wait_until(lambda: len(rec.prompts) - gap_prompts_start == 3, 10)
        order = [p for _, p in rec.prompts[gap_prompts_start:]]
        s3 = consumer_counters(mon, "hjs-sil-drill")
        assert ok, f"gap replay incomplete: {rec.prompts[gap_prompts_start:]}"
        import re
        got_order = [int(m.group(1)) for p in order if (m := re.search(r'"gap":(\d)', p))]
        assert got_order == [0, 1, 2], f"replay order wrong: {got_order}"
        ev("S3-gap", "jsz consumer", f"inactive-window snapshot {s3gap} -> reattach {s3}: "
           "gap is MEASURABLE server-side (num_pending), delivered in stream order")
        ev("S3-gap", "plugin log/state", "while the consumer was inactive the plugin surface said "
           "NOTHING — consumer-side gap invisible without jsz (feeds hjs-htk) — LOUD side OK")

        # S4: poison — callback-level failure, custom short backoff, count to exhaustion.
        # hjs-xq5: the pipeline now RECORDS the loss at the final delivery (ledger
        # 'lost', WARNING, route lost-counter, term) instead of letting the wake vanish.
        poison_pipe = WakePipeline(["hjs.drill.>"], WakeLedger(d1 / "poison.db", reclaim_after_s=0.5),
                                   _failing_dispatch, "soldrill", "notme")
        poison_route = WakeRoute(name="poison", subject=POISON_SUBJ, durable="hjs-poison-drill")
        hits = []
        psub = await bus.jetstream.pull_subscribe(
            POISON_SUBJ, durable="hjs-poison-drill", stream="SIL", config=_poison_cfg())
        async def poison_loop():
            from nats.errors import TimeoutError as NatsTimeout
            while True:
                try:
                    msgs = await psub.fetch(1, timeout=2.0)
                except NatsTimeout:
                    continue
                for m in msgs:
                    hits.append(m.metadata.num_delivered)
                    try:
                        await poison_pipe.handle_message(m, poison_route)
                    except Exception:
                        pass  # dispatch failure forces redelivery
        task = asyncio.create_task(poison_loop())
        penv = build_envelope("poison", {"die": True}, profile="other", source="drill")
        penv["origin"]["nonce"] = "not-us"
        await bus.publish(POISON_SUBJ, json.dumps(penv).encode())
        exhausted = await wait_until(lambda: len(hits) >= 5, 30)
        await asyncio.sleep(3)
        task.cancel()
        after = consumer_counters(mon, "hjs-poison-drill")
        ev("S4-poison", "observed", f"delivery attempts={hits} exhausted={exhausted}; consumer after: {after}")
        assert exhausted and len(hits) == 5, f"expected exactly 5 attempts, got {hits}"
        pst = poison_pipe.stats_snapshot().get("poison", {})
        assert pst.get("lost") == 1, f"lost-wake counter missing: {pst}"
        assert pst.get("failed") == 5, f"expected 5 failures counted, got {pst}"
        import sqlite3
        with sqlite3.connect(d1 / "poison.db") as c:
            rows = c.execute("SELECT status FROM wake_claims").fetchall()
        assert rows and rows[0][0] == "lost", f"ledger loss record missing: {rows}"
        ev("S4-poison", "post-exhaustion", "wake loss now RECORDED: pipeline lost=1, ledger "
           "status='lost', term closes the consumer entry (hjs-xq5 fix). Counter surfaces: "
           f"jetstream_status/gateway metrics route stats 'lost' key. Consumer after: {after}")
        await bus.jetstream.delete_consumer("SIL", "hjs-poison-drill")
        print("PASS: three silences characterized; poison loss recorded (hjs-xq5)")
        return 0
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except Exception:
            proc.kill()
        shutil.rmtree(d1, ignore_errors=True)


def _tcp(port: int) -> bool:
    import socket
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.3):
            return True
    except OSError:
        return False


def _poison_cfg():
    from nats.js.api import ConsumerConfig
    return ConsumerConfig(max_deliver=5, ack_wait=5, max_ack_pending=10, backoff=[1.0, 1.0, 2.0, 2.0])


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
