#!/usr/bin/env bash
# One gate for every surface: unit tests, generality grep-gate, plugin doctor,
# live drills (opt-in via env). CI-ready; runs identically on a laptop.
#   ./scripts/ci.sh                      # hermetic gates
#   HJS_SMOKE_SERVERS=h:4222 ./scripts/ci.sh           # + live smoke drill
#   NATS_BIN=/path/nats-server ./scripts/ci.sh         # + auth matrix (boots scratch servers)
set -uo pipefail
cd "$(dirname "$0")/.."
PY="${PYTHON:-python3}"
fail=0
note() { printf '\n=== %s ===\n' "$1"; }

note "unit tests (stdlib-only)"
"$PY" -m unittest tests.test_core 2>&1 || fail=1

note "generality gate (no environment references in plugin/)"
# The plugin must run against any nats-server: no vendor, deployment, or host
# specifics may appear in shipped source. Source files only (.py/.yaml): the
# checker's own pattern list and build artifacts must not self-match.
if grep -rniE --include='*.py' --include='*.yaml' \
   -e consul -e '192\.168' -e initzero -e nomad -e 'service\.consul' -e pool-runner \
   plugin/; then
  echo "FAIL: environment reference found"; fail=1
else
  echo "clean"
fi

note "syntax"
"$PY" -m compileall -q plugin tests && echo ok || fail=1

if command -v hermes >/dev/null 2>&1; then
  note "hermes plugins doctor"
  hermes plugins doctor "$(pwd)/plugin/hermes_jetstream" 2>&1 || fail=1
else
  note "hermes plugins doctor — skipped (hermes not on PATH)"
fi

if [ -n "${HJS_SMOKE_SERVERS:-}" ]; then
  note "live smoke drill (wake/dedupe/self-origin)"
  "$PY" tests/smoke_live.py || fail=1
fi

if [ -n "${NATS_BIN:-}" ]; then
  note "auth matrix (7 modes + negative refusals)"
  "$PY" tests/auth_matrix.py || fail=1
  note "silence drill (three silences + poison exhaustion)"
  "$PY" tests/silence_drill.py || fail=1
fi

note "RESULT"
[ "$fail" = 0 ] && echo PASS || echo FAIL
exit "$fail"
