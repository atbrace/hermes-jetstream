"""3-node JetStream cluster drill (hjs-8u7): durable consumer survives a node kill.

Boots 3 clustered nats-servers, creates an R=3 stream, opens a durable queue sub via
node A, kills node A, publishes via node B, asserts the wake still arrives (server-side
consumer state rebinds). Self-cleaning. Usage: NATS_BIN=/path/nats-server python tests/cluster_drill.py
"""
import asyncio
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "plugin"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tests"))
from auth_matrix import Server, free_port  # noqa: E402
from hermes_jetstream.bus import NatsBus  # noqa: E402
from hermes_jetstream.config import Settings  # noqa: E402

NATS_BIN = os.environ.get("NATS_BIN", "nats-server")


def cluster_node(peers: str, idx: int, cluster_port: int) -> Server:
    return Server("port: $PORT\n"
                  f"server_name: n{idx}\n"
                  f'cluster {{ name: hjs, port: {cluster_port}, routes: {peers} }}\n'
                  'jetstream { store_dir: "$DIR/js" }\n',
                  f"cl{idx}")


async def main() -> int:
    if not shutil.which(NATS_BIN):
        print("SKIP: nats-server not found")
        return 2
    nodes = []
    ports = [free_port() for _ in range(3)]
    peers = "[" + ", ".join(f'"nats://127.0.0.1:{p}"' for p in ports) + "]"
    for i, cp in enumerate(ports):
        nodes.append(cluster_node(peers, i, cp))
    await asyncio.sleep(4)  # raft leader election + JS meta layer formation
    got = asyncio.Queue()

    async def cb(msg):
        await got.put(msg.data)
        await msg.ack()

    all_servers = [f"127.0.0.1:{n.port}" for n in nodes]
    sub_bus = NatsBus(Settings(servers=[all_servers[0]], stream="CLUSTR"), "clustertest")
    pub_bus = NatsBus(Settings(servers=all_servers[1:], stream="CLUSTR"), "clustertest-pub")
    try:
        await sub_bus.connect()
        await pub_bus.connect()
        from nats.js.api import StreamConfig
        for attempt in range(10):
            try:
                await sub_bus.jetstream.add_stream(StreamConfig(name="CLUSTR", subjects=["hjs.cluster.>"],
                                                               num_replicas=3, max_msgs=100))
                break
            except Exception:
                if attempt == 9:
                    raise
                await asyncio.sleep(2)   # meta layer still electing
        await sub_bus.subscribe_wake(subject="hjs.cluster.>", queue="clsub", on_message=cb)
        await pub_bus.publish("hjs.cluster.a", b"pre-kill")
        await asyncio.wait_for(got.get(), 5)
        print("delivery via cluster: PASS")

        nodes[0].proc.terminate()   # kill the node the sub connection rode
        nodes[0].proc.wait(timeout=10)
        await asyncio.sleep(3)      # client reconnect + consumer rebind
        await pub_bus.publish("hjs.cluster.b", b"post-kill")
        # at-least-once: the pre-kill ack may have died with node0, so a redelivery
        # of b"pre-kill" is expected; wait for post-kill through any redelivers.
        deadline = time.time() + 20
        seen = []
        while time.time() < deadline:
            msg = await asyncio.wait_for(got.get(), 10)
            seen.append(msg)
            if msg == b"post-kill":
                break
        assert b"post-kill" in seen, seen
        print(f"durable consumer survived node kill (rebind; seen={seen}): PASS")
    finally:
        for n in nodes:
            try:
                n.stop()
            except Exception:
                pass
    print("CLUSTER DRILL: PASS (R=3 stream, node-kill consumer rebind)")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
