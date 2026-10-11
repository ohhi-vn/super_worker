# Proposal

## Why

Standalone workers are the supervisor's simplest and most direct process type (add/get/remove/send by id), yet the path is the least hardened: a worker that fails to start is reported as `{:ok, id}` and leaves an orphan info row, a crash-looping `:permanent` worker restarts forever with no intensity ceiling, deliberate removal logs misleading "unexpected" warnings, missing-worker errors use an atom (`:not_found`) inconsistent with groups, and every send/get/remove/add performs a full-table ETS scan. Verifying every standalone function and Supervisor API while fixing performance and error handling closes the last gap after the group and chain hardening and prevents cascading supervisor bugs.

## What Changes

- Spawn atomicity and honest results: `add_standalone_worker` SHALL return `{:ok, id}` only when the process actually started; otherwise `{:error, {:spawn_failed, cause}}` with the cause preserved, and no worker info or ref row left behind. **BREAKING** for callers relying on the current always-`{:ok, id}` reply.
- Restart intensity: implement `:max_restarts`, `:max_seconds`, and `:auto_restart_time` for standalone workers (currently accepted but ignored). A worker that crashes past the ceiling within the window stops being restarted; the partition stays healthy and the event is logged with the cause.
- Error contract: standalone not-found lookups return `{:error, :worker_not_found}` via `SuperWorker.Supervisor.ErrorHandler`, matching groups. **BREAKING** for callers matching the legacy `{:error, :not_found}` on `send_to_standalone_worker`, `get_pid_standalone_worker`, and `remove_standalone_worker`.
- Removal hygiene: a deliberately removed worker SHALL NOT produce a spurious "unexpected worker data" warning; the resulting `:DOWN` is treated as already-cleaned.
- Performance: the fresh add path uses the O(1) `Db.put_new_worker/5` instead of the scanning `put_worker/5`; `shutdown` uses pid/id-only selects with no per-worker second lookup; ownership filtering avoids re-reading each standalone worker.
- Verification: function-by-function matrix for all standalone `Supervisor` APIs plus the internal `Looper`/`Partition`/`Db` paths, all three restart strategies, spawn-failure, removal, restart-intensity, and scale behavior.

## Capabilities

### New Capabilities
- `supervisor-standalone`: standalone worker lifecycle, id-based lookup and messaging, restart strategies and intensity, spawn atomicity, removal cleanup, and performance/error contracts.

### Modified Capabilities
- None. `supervisor-group`, `supervisor-chain`, and `function-chain` requirements are unchanged; standalone behavior is defined in its own new capability.

## Impact

- Affected code: `lib/supervisor/looper.ex` (standalone start/spawn/remove/DOWN/restart paths, shutdown), `lib/supervisor/supervisor.ex` (standalone API docs/specs and not-found mapping), `lib/supervisor/db/db.ex` (pid/id-only select for standalone, reuse `put_new_worker/5`), `lib/supervisor/worker/worker.ex` (validate/plumb restart-intensity options).
- APIs: `Supervisor.add_standalone_worker/4`, `send_to_standalone_worker/4`, `get_pid_standalone_worker/3`, `remove_standalone_worker/3`, `list_standalone_workers/2`; success shapes preserved; error atoms tightened and spawn failures surfaced.
- Dependencies: none new; ETS (`:ets`), `Logger`, existing `ErrorHandler`, `Utils`, `Constants`.
- Systems/tests: `test/standart_test/standalone_test.exs`, `test/workload_test/standalone_workers_test.exs`, `benchmarks/bench_quick.exs`; no data migration (ETS-only runtime state).
