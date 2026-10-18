# hjs-y4t — gap evidence: no pre-restart validate for wake routes

Claimed 2026-10-18, verified against `ba047ef963` (origin/main).

## Gap evidence

1. **The only wake-route validation is `WakeRoute.from_dict`**
   (`plugin/hermes_jetstream/config.py:79-100`): raises `ValueError` for
   missing/blank `name`/`subject` and illegal `durable`. It runs only via
   `Settings.from_plugin_ctx` at plugin registration — i.e. inside the gateway
   process, at start.
2. **`plugin.yaml` `config_schema` declares `wake: { type: list, default: [] }`
   only** (line 28) — the item shape is declared nowhere machine-readable;
   the accepted-keys/types truth lives solely in the dataclass + the
   `docs/CONFIG-REFERENCE.md` table.
3. **`adapter.validate_config` is a non-empty check** (`adapter.py:41-43`:
   `bool(extra.get("wake") or ...)`), not a schema check.
4. A malformed route therefore surfaces at restart: `make_adapter_factory`
   (`adapter.py:46-53`) catches the `ValueError`, logs
   "adapter construction refused (bad config?)" and returns `None` — the whole
   platform dies with one log line. A typo in one route costs a full restart
   cycle to discover (the peer incident behind this bead).
5. No CLI/script surface exists to dry-run settings before that gate:
   `grep -rn validate` finds nothing resembling a pre-restart validator;
   `scripts/` holds only `ci.sh`.

## Verdict

Reproduced as a real gap. Implement: hand-written JSON Schema mirroring the
parser truth (`docs/wake-routes.schema.json`), plus
`python3 scripts/validate_settings.py <settings.json>` reusing
`Settings.from_plugin_ctx` (the existing loader; no second parser, no nats-py,
no network) that names every problem and exits nonzero.
