"""Durable wake routes registered at runtime by the `jetstream_wake` tool verb (hjs-tr9).

Persistence: one JSON file in the plugin data dir — same durability class as the
wake ledger — so routes survive gateway restarts and are rebound at platform
bind. Gates are identical to config routes, not weaker: rows parse through the
same WakeRoute.from_dict (durable-name and prompt rules), the tool refuses a
subject outside subscribe_allowed loudly at register time, and the pipeline's
rate/coalesce/dedupe rails run unchanged. Names carry the `rt-` prefix so a
runtime route can never shadow or collide with a config route's identity.
"""
from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .config import WakeRoute

STORE_FILENAME = "wake-routes.json"
NAME_PREFIX = "rt-"
MAX_RUNTIME_ROUTES = 16  # durable consumers are server-side state; same cap shape as watches


class RuntimeRouteStore:
    """name -> row dict in one atomically-replaced JSON file."""

    def __init__(self, path):
        self._path = Path(path)

    def rows(self) -> Dict[str, Dict[str, Any]]:
        try:
            data = json.loads(self._path.read_text())
        except (OSError, ValueError):
            return {}  # absent or corrupt == empty; a write replaces the file wholesale
        routes = data.get("routes", {}) if isinstance(data, dict) else {}
        return {str(k): v for k, v in routes.items() if isinstance(v, dict)}

    def put(self, row: Dict[str, Any]) -> None:
        rows = self.rows()
        rows[row["name"]] = row
        self._write(rows)

    def remove(self, name: str) -> Optional[Dict[str, Any]]:
        """Returns the removed row, or None if absent (unregister is idempotent)."""
        rows = self.rows()
        row = rows.pop(name, None)
        if row is None:
            return None
        self._write(rows)
        return row

    def sweep(self, now: Optional[float] = None) -> List[str]:
        """Self-retire routes past their max_age (abandoned agent routes die at bind)."""
        now = now if now is not None else time.time()
        gone = [n for n, r in self.rows().items()
                if isinstance(r.get("expires_at"), (int, float)) and r["expires_at"] < now]
        for n in gone:
            self.remove(n)
        return gone

    def wake_routes(self) -> Tuple[List[WakeRoute], List[Tuple[str, str]]]:
        """Live rows as WakeRoute objects; junk rows come back as (name, problem) so
        the bind path names them loudly instead of vanishing them."""
        routes, problems = [], []
        for name, row in self.rows().items():
            try:
                routes.append(WakeRoute.from_dict(row))
            except ValueError as exc:
                problems.append((name, str(exc)))
        return routes, problems

    def _write(self, rows: Dict[str, Dict[str, Any]]) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._path.with_name(self._path.name + ".tmp")
        tmp.write_text(json.dumps({"routes": rows}, indent=1, sort_keys=True))
        os.replace(tmp, self._path)


def default_store() -> RuntimeRouteStore:
    """The one production store path (same dir as wake-claims.db)."""
    from .adapter import _plugin_data_dir
    return RuntimeRouteStore(_plugin_data_dir() / STORE_FILENAME)


def tag_sources(routes: Dict[str, Any], runtime_names) -> Dict[str, Any]:
    """jetstream_status route rows carry source: config|runtime (hjs-tr9)."""
    for name, row in routes.items():
        if isinstance(row, dict):
            row["source"] = "runtime" if name in runtime_names else "config"
    return routes
