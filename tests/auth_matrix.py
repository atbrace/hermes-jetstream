"""Auth matrix drill (hjs-bt0): every auth mode proven against scratch nats-server instances.

Modes: no-auth | token (incl. wrong-token refusal) | user/pass | nkey seed (SU...) | seed creds
file | TLS (self-signed CA, plaintext refused) | mTLS (client cert, no-cert refused).
Server configs written inline; PKI generated with openssl; consumers/streams self-cleaning.
Usage: NATS_BIN=/path/nats-server python tests/auth_matrix.py   (exit 2 = skipped)
"""
import asyncio
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "plugin"))
from hermes_jetstream.bus import NatsBus
from hermes_jetstream.config import Settings

NATS_BIN = os.environ.get("NATS_BIN", "nats-server")
JS = 'jetstream { store_dir: "$DIR/js" }\n'


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Server:
    def __init__(self, conf: str, tag: str):
        self.dir = Path(tempfile.mkdtemp(prefix=f"hjs-{tag}-"))
        self.port = free_port()
        conf_path = self.dir / "nats.conf"
        conf_path.write_text(conf.replace("$PORT", str(self.port)).replace("$DIR", str(self.dir)))
        self.proc = subprocess.Popen([NATS_BIN, "-c", str(conf_path)],
                                     stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        deadline = time.time() + 10
        while time.time() < deadline:
            try:
                with socket.create_connection(("127.0.0.1", self.port), timeout=0.3):
                    return
            except OSError:
                time.sleep(0.2)
        out = b""
        try:
            self.proc.wait(timeout=2)
            out = self.proc.stdout.read()[-800:]
        except Exception:
            self.proc.kill()
        raise RuntimeError(f"nats-server ({tag}) failed to boot: {out.decode(errors='replace')}")

    @property
    def servers(self):
        return [f"127.0.0.1:{self.port}"]

    def stop(self):
        self.proc.terminate()
        try:
            self.proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.proc.kill()
        shutil.rmtree(self.dir, ignore_errors=True)


def mk_nkey():
    import os as _os
    import nkeys
    seed = nkeys.encode_seed(_os.urandom(32), nkeys.PREFIX_BYTE_USER)
    seed = seed if isinstance(seed, str) else seed.decode()
    kp = nkeys.from_seed(seed.encode())
    pub = kp.public_key
    pub = pub.decode() if isinstance(pub, bytes) else pub
    return seed, pub


def mk_creds_file(seed: str) -> str:
    d = Path(tempfile.mkdtemp(prefix="hjs-creds-"))
    p = d / "u.creds"
    p.write_text(f"-----BEGIN USER NKEY SEED-----\n{seed}\n------END USER NKEY SEED------\n")
    return str(p)


def mk_pki() -> dict:
    d = Path(tempfile.mkdtemp(prefix="hjs-pki-"))
    R = lambda *a, **kw: subprocess.run(list(a), check=True, capture_output=True, **kw)
    R("openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "2",
      "-keyout", d / "ca.key", "-out", d / "ca.pem", "-subj", "/CN=hjs-ca",
      "-addext", "basicConstraints=critical,CA:TRUE",
      "-addext", "keyUsage=critical,keyCertSign,cRLSign")
    R("openssl", "req", "-newkey", "rsa:2048", "-nodes", "-keyout", d / "srv.key",
      "-out", d / "srv.csr", "-subj", "/CN=localhost")
    R("openssl", "x509", "-req", "-in", d / "srv.csr", "-CA", d / "ca.pem", "-CAkey", d / "ca.key",
      "-CAcreateserial", "-days", "2", "-out", d / "srv.pem", "-extfile", "-", "-extensions", "v3",
      input=b"[v3]\nsubjectAltName=DNS:localhost,IP:127.0.0.1\n")
    R("openssl", "req", "-newkey", "rsa:2048", "-nodes", "-keyout", d / "cli.key",
      "-out", d / "cli.csr", "-subj", "/CN=hjs-client")
    R("openssl", "x509", "-req", "-in", d / "cli.csr", "-CA", d / "ca.pem", "-CAkey", d / "ca.key",
      "-CAcreateserial", "-days", "2", "-out", d / "cli.pem")
    return {"ca": str(d / "ca.pem"), "srv_cert": str(d / "srv.pem"), "srv_key": str(d / "srv.key"),
            "cli_cert": str(d / "cli.pem"), "cli_key": str(d / "cli.key")}


async def roundtrip(settings: Settings) -> str:
    # Negative tests want a single connect attempt (no reconnect storm on refusal).
    settings.reconnect_attempts = 0
    bus = NatsBus(settings, "authmatrix")
    try:
        await asyncio.wait_for(bus.connect(), timeout=15)
        try:
            await bus.jetstream.add_stream(name="AUTHMATRIX", subjects=["hjs.auth.>"],
                                           max_msgs=100, max_age=300, storage="file")
        except Exception:
            pass
        ack = await bus.publish("hjs.auth.probe", b'{"ok":1}')
        return f"seq={ack.seq}"
    finally:
        try:
            await asyncio.wait_for(bus.stop(), timeout=10)
        except (asyncio.TimeoutError, Exception):
            pass  # teardown noise on a refused/half-TLS connection is expected


async def expect_refused(settings: Settings, label: str):
    try:
        await asyncio.wait_for(roundtrip(settings), timeout=20)
    except (asyncio.TimeoutError, Exception) as exc:
        # hang == refused too when the client can't complete the handshake
        print(f"  refused as expected: {label} ({type(exc).__name__})")
        return
    raise AssertionError(f"{label}: connect/publish unexpectedly SUCCEEDED")


async def main() -> int:
    if not shutil.which(NATS_BIN):
        print(f"SKIP: {NATS_BIN} not found")
        return 2

    srv = Server("port: $PORT\n" + JS, "noauth")
    await roundtrip(Settings(servers=srv.servers))
    print("no-auth: PASS")
    srv.stop()

    srv = Server("port: $PORT\n" + JS + '\nauthorization { token: "tok3n-pro", timeout: 1 }\n', "token")
    await expect_refused(Settings(servers=srv.servers, token="wrong"), "wrong token")
    await expect_refused(Settings(servers=srv.servers), "no token")
    await roundtrip(Settings(servers=srv.servers, token="tok3n-pro"))
    print("token: PASS")
    srv.stop()

    seed, pub = mk_nkey()
    creds = mk_creds_file(seed)
    srv = Server("port: $PORT\n" + JS + f"""
authorization {{
  timeout: 1
  users: [
    {{ user: alice, password: "pw-alice" }}
    {{ nkey: {pub} }}
  ]
}}
""", "auth")
    await roundtrip(Settings(servers=srv.servers, user="alice", password="pw-alice"))
    print("user/password: PASS")
    await expect_refused(Settings(servers=srv.servers, user="alice", password="bad"), "wrong password")
    await roundtrip(Settings(servers=srv.servers, nkey=seed))
    print(f"nkey seed ({seed[:5]}..., client-signed nonce): PASS")
    await roundtrip(Settings(servers=srv.servers, creds=creds))
    print("seed creds file: PASS")
    srv.stop()

    pki = mk_pki()
    srv = Server("port: $PORT\n" + JS + f"""
tls {{
  ca_file: "{pki['ca']}"
  cert_file: "{pki['srv_cert']}"
  key_file: "{pki['srv_key']}"
  timeout: 5
}}
""", "tls")
    await roundtrip(Settings(servers=srv.servers, tls_enabled=True, tls_ca=pki["ca"]))
    print("TLS (self-signed CA verified): PASS")
    await expect_refused(Settings(servers=srv.servers), "plaintext to TLS-required server")
    srv.stop()

    srv = Server("port: $PORT\n" + JS + f"""
tls {{
  ca_file: "{pki['ca']}"
  cert_file: "{pki['srv_cert']}"
  key_file: "{pki['srv_key']}"
  timeout: 5
  verify: true
}}
""", "mtls")
    await roundtrip(Settings(servers=srv.servers, tls_enabled=True, tls_ca=pki["ca"],
                            tls_cert=pki["cli_cert"], tls_key=pki["cli_key"]))
    print("mTLS (client cert): PASS")
    await expect_refused(Settings(servers=srv.servers, tls_enabled=True, tls_ca=pki["ca"]),
                         "TLS without client cert (verify: true)")
    srv.stop()

    print("\nAUTH MATRIX COMPLETE: no-auth, token, user/pass, nkey-seed, creds-file, TLS, mTLS "
          "(+ 4 negative refusals proven)")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
