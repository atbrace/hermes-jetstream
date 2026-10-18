# hjs-c03 — platform discovery cannot find the plugin unit in a monorepo layout

## Verdict

**Substance reproduced; the deep-discovery fix is core-side (out_of_scope for this
repo); the repo-side directions that ARE available are shipped here.**

Reproduced: a bare-identifier install publishes the repo clone root as the plugin
unit directory, and this repo's `plugin.yaml` sits two levels below it at
`plugin/hermes_jetstream/plugin.yaml` — outside the documented discovery depth, so
the manifest is invisible, `python_dependencies` (nats-py) is never processed,
`check_deps()` is False, and platform creation returns None. The chain is
deterministic from the layout alone (`tests/test_core.py::MonorepoDiscovery`
simulates the core's depth rule against the repo's own files and asserts both the
invisible bare-clone case and the discoverable subdir-clone case).

## Exact core locations (out_of_scope — do not fix from this repo)

- `hermes_cli/plugins_discovery.py::scan_directory` — the depth cap. Accepts
  `<root>/<name>/plugin.yaml` and one-level category `<root>/<cat>/<name>/plugin.yaml`;
  a manifest-less dir at `depth >= 1` is skipped (`"no plugin.yaml, depth cap
  reached"`). Honoring deeper layouts (e.g. `plugin/<pkg>/plugin.yaml` under a
  published clone) would live here.
- `gateway/platform_registry.py::PlatformRegistry.create_adapter` — the silent-None
  site. When `check_fn` is False and `ensure_deps_fn` is None it logs
  `Platform '%s' requirements not met%s` and returns None; `%s` hint comes from
  `PlatformEntry.install_hint`. This is the sanctioned loudness hook for plugins.
- `hermes_cli/plugins_cmd.py::_resolve_git_url` + `plugins_cmd_install.py::_install_plugin_core`
  — install ALREADY supports unit-subdir identifiers (`owner/repo/<subdir>`, `#frag`,
  tree URLs); the failure mode is publishing the bare clone root, which is only a
  warning (`hermes-jetstream doesn't contain plugin.yaml…`), not a hard error.
  A hard error there would be a core fix.

## Shipped here

1. `plugin/hermes_jetstream/__init__.py` — `register_platform(..., install_hint=...)`:
   one line; the core's creation-failure line now names the remedy (install nats-py,
   and reinstall with the unit-subdir identifier if the manifest was buried).
2. `README.md` — new Install section: the identifier must point at
   `plugin/hermes_jetstream`. The README previously shipped no install command at all
   (root cause of the friction report: peers guessed the bare repo identifier).
3. `tests/test_core.py::MonorepoDiscovery` — behavior tests for both publish shapes
   plus the README command resolving to a real manifest directory;
   `PlatformFailureNamesItsReason` asserts `register()` advertises the hint.

## Not done (named, per ladder)

- Repo restructure (manifest at clone-root or depth-1): rejected — renames every
  install path, CI, and the working flat installs on every existing machine for a
  layout the install contract already handles via the subdir identifier.
- Logging from `check_deps()`: it is a passive probe called freely by status
  displays (core contract: side-effect-free); `install_hint` is the designed channel.
