# hjs-zkc — `plugins capabilities` shows `(none)` despite 6 declared tools

**Verdict: out_of_scope (core-owned surface). Docs clarification shipped.**

## Reproduction

```
$ hermes plugins capabilities hermes-jetstream
hermes-jetstream (user)
  declared: (none)
```

Reproduces exactly as reported.

## Ownership finding

The command is rendered by Hermes **core**, not this plugin:

- Dispatch: `hermes_cli/plugins_cmd.py` (`_PLUGIN_ACTIONS["capabilities"]` → `cmd_capabilities`).
- Renderer: `hermes_cli/plugins_cmd_capabilities.py::cmd_capabilities` — prints
  "declared vs granted" per plugin.
- Semantic source: `hermes_cli/plugin_capabilities.py::CAPABILITY_REGISTRY` — the
  *consent-layer* registry of trust gates (`tools.override`, `llm.*_override`,
  `gateway.platform_actions`). It reads the manifest `capabilities:` key, not
  `provides_tools`.

This plugin declares **zero** consent-gated capabilities and correctly has no
`capabilities:` key in `plugin.yaml` — it replaces no built-in tools, overrides no
provider/model/agent/profile/task lane, and does not act on chat platforms. `(none)`
on that surface is truthful, not a bug. The reported confusion is the surface's
meaning being conflated with the tool surface.

Where the 6 tools **are** visible:

- `hermes tools list` — lists the `jetstream` plugin toolset (enabled/disabled per platform).
- Runtime deferred tool search in sessions (`jetstream_*`, `nats_*` entries).
- `hermes plugins info <catalog-name>` shows a `Tools:` line for catalog entries.

## Fix shipped

Repo-side disclosure only: one bullet in README "Disclosures" naming the two
surfaces and the exact command that lists the tools. No code change — nothing in
this repo makes the tools invisible; the invisibility is by core design
(`plugins capabilities` is deliberately scoped to the consent registry).

Candidate core ask (not ours to implement): when a plugin declares no capabilities
but does declare `provides_tools`, print a hint line pointing at the toolset
surface instead of bare `(none)`.
