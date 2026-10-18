# Agent-plugin adapter notes

Notes for plugin authors on how agent tool calls and the gateway platform
adapter cooperate. The pattern below is what `jetstream_wake` (hjs-tr9) uses;
any tool that mutates platform state should follow it.

## Runtime-routes pattern (tool-owned durable state, adapter-rebound)

A tool verb registers durable state (here: wake routes) that the platform
adapter consumes. Three rules make it work without a gateway restart:

1. **Plugin-side persistence, adapter-neutral.** The tool writes rows to a
   JSON file in the plugin data dir (`runtime_routes.RuntimeRouteStore`,
   atomic replace). A CLI/cron process with no live adapter still succeeds —
   the row binds at the next platform start.
2. **Rebind at every bind.** The adapter loads (and max-age-sweeps) the store
   inside `connect()` and merges the rows with config routes *before* the
   `subscribe_allowed` filter, so runtime state is never privileged over
   config. Junk rows are named in a warning, never silently dropped.
3. **Live hop through the adapter's own loop.** A tool handler runs on the
   gateway's thread, not the bus loop. It reaches live adapters via
   `adapter.active_adapters()` and runs the bind/unbind coroutine through
   each adapter's `_on_bus_loop` — same hop law as publishing. Unbind drains
   the subscription handle (`bus.subscribe_wake` returns it).

Gate parity is law: the tool validates through the same `WakeRoute` parser,
the same `subscribe_allowed` allowlist (loud refusal at register), and the
same pipeline rails as a config route; names are namespaced (`rt-`) so they
can never shadow a config route's durable identity. Unregister is idempotent;
failed live binds roll the row back.
