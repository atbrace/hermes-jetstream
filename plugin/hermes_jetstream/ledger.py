"""Wake-claim ledger (SQLite, plugin-data): idempotent at-least-once wakes.

Claim-then-ack: a delivery is durably CLAIMED before dispatch, marked DONE after the wake is
accepted. Redelivery of a DONE row acks+skips. A CLAIMED row older than reclaim_after_s is a
dead-owner artifact and is re-dispatched (cron executions.db's dead-owner reclaim pattern).
"""
from __future__ import annotations

import sqlite3
import threading
import time
from pathlib import Path

_SCHEMA = """
CREATE TABLE IF NOT EXISTS wake_claims (
    dedupe_key TEXT PRIMARY KEY,
    route      TEXT NOT NULL,
    status     TEXT NOT NULL,            -- claimed | done
    claimed_at REAL NOT NULL,
    done_at    REAL
);
"""


class WakeLedger:
    def __init__(self, db_path: Path, reclaim_after_s: float = 900.0):
        self._lock = threading.Lock()
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(str(db_path), check_same_thread=False, timeout=10)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.executescript(_SCHEMA)
        self._db.commit()
        self._reclaim_after_s = reclaim_after_s

    def claim(self, dedupe_key: str, route: str = "", now: float | None = None) -> bool:
        """True = fresh claim, dispatch it. False = already done/in-flight (ack+skip)."""
        now = now if now is not None else time.time()
        with self._lock:
            row = self._db.execute(
                "SELECT status, claimed_at FROM wake_claims WHERE dedupe_key=?", (dedupe_key,)
            ).fetchone()
            if row is not None:
                status, claimed_at = row
                if status == "done":
                    return False
                if now - float(claimed_at) < self._reclaim_after_s:
                    return False  # in flight elsewhere, let it ride
                self._db.execute(
                    "UPDATE wake_claims SET claimed_at=? WHERE dedupe_key=?", (now, dedupe_key))
            else:
                self._db.execute(
                    "INSERT INTO wake_claims(dedupe_key, route, status, claimed_at) VALUES(?,?,?,?)",
                    (dedupe_key, route, "claimed", now))
            self._db.commit()
            return True

    def complete(self, dedupe_key: str, now: float | None = None) -> None:
        now = now if now is not None else time.time()
        with self._lock:
            self._db.execute(
                "UPDATE wake_claims SET status='done', done_at=? WHERE dedupe_key=?",
                (now, dedupe_key))
            self._db.commit()

    def reconcile_lost(self, now: float | None = None) -> list:
        """Startup crash-reconcile (hjs-25g): 'claimed' rows past reclaim_after_s are
        evidence the owner died mid-run (the server had already acked the wake, so
        redelivery never revisits them). Mark them 'lost' so the loss is recorded
        once and survives restarts; a late redelivery of a lost row still
        re-claims (status != done), which is the right second chance."""
        now = now if now is not None else time.time()
        with self._lock:
            rows = self._db.execute(
                "SELECT dedupe_key, route, claimed_at FROM wake_claims "
                "WHERE status='claimed' AND claimed_at < ? ORDER BY claimed_at",
                (now - self._reclaim_after_s,)).fetchall()
            if rows:
                self._db.executemany(
                    "UPDATE wake_claims SET status='lost', done_at=? WHERE dedupe_key=?",
                    [(now, r[0]) for r in rows])
                self._db.commit()
            return rows

    def mark_lost(self, dedupe_key: str, now: float | None = None) -> None:
        """Record a wake lost at its final delivery (hjs-xq5): the server gives up
        after max_deliver, so this is the plugin's only durable record of the loss."""
        now = now if now is not None else time.time()
        with self._lock:
            self._db.execute(
                "UPDATE wake_claims SET status='lost', done_at=? "
                "WHERE dedupe_key=? AND status='claimed'", (now, dedupe_key))
            self._db.commit()

    def prune(self, older_than_s: float = 7 * 86400, now: float | None = None) -> int:
        now = now if now is not None else time.time()
        with self._lock:
            cur = self._db.execute(
                "DELETE FROM wake_claims WHERE status IN ('done','lost') AND done_at < ?",
                (now - older_than_s,))
            self._db.commit()
            return cur.rowcount

    def close(self) -> None:
        with self._lock:
            self._db.close()
