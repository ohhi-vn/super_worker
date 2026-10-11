# Proposal

## Why

`SuperWorker.Supervisor.Group` is the hot path for every grouped worker (add/get/restart/remove/messaging/count). Profiling the current code shows redundant ETS round-trips (up to 3 lookups per `remove_worker`/`restart_worker`), full `Worker` struct fetches for count/kill/broadcast, inconsistent error atoms, swallowed broadcast failures in `Looper`, and orphan ETS rows left behind on spawn failure. At scale this means higher latency, stale state, and errors callers cannot match on reliably. Verifying every Group function while fixing performance and error handling now prevents cascading supervisor bugs.

## What Changes

- Standardize Group error contract on `SuperWorker.Supervisor.ErrorHandler` atoms: `:worker_not_found`, `:worker_already_exists`, `:group_not_found`, `{:invalid_options, detail}`, `{:error, {:spawn_failed, cause}}` with cause preserved for observability. **BREAKING** for direct `Group` callers matching on legacy atoms (`:worker_exists`, `:cannot_send`, `:not_found`/`:not_alive` from kill paths, string `"Invalid group restart strategy..."`).
- Require `id` in `Group.check_options/1`; reject `nil`/missing ids instead of creating `nil`-id groups.
- Single-lookup worker paths: collapse `worker_exists?` + `get_worker` + `kill_worker` chains into one `Db` read; delete both ref-row and info-row atomically on remove/restart/spawn-failure.
- Performance: `count_workers/1` via ETS `select_count` (no struct fetch); `kill_all_workers/2`, `broadcast/2`, `get_all_workers/1` use pid/id-only selects and skip dead pids; simplify `to_struct/1` to `struct/2`; fix double `delete_worker_by_id` + `delete_worker_info` sequence.
- Correctness fixes: propagate `Group.broadcast/2` errors through `Looper` (`:broadcast_to_group` currently always replies `{:ok, :sent}`); alive-check before `send_message/3`; fix `register_worker_name/1` registration race; clean stale ref rows on `restart_worker` `:not_alive` path; preserve link-failure cleanup without killing the caller.
- Verification: function-by-function matrix for all 14 public Group functions plus Supervisor Group APIs (`add_group`, `add_group_worker`, `send_to_group_worker`, `broadcast_to_group`, `send_to_group_random`, `restart_group_worker`, `restart_group`, `remove_group_worker`, `remove_group`, `count_workers_in_group`, `group_exists?`, `get_pid_group_worker`), restart strategies (`:one_for_one`, `:one_for_all`), crash paths, and benchmarks.

## Capabilities

### New Capabilities
- `supervisor-group`: Group lifecycle, worker CRUD, intra-group messaging, restart semantics, and performance/error contracts for grouped workers.

### Modified Capabilities
- None. Existing `function-chain` spec is unrelated (chain-node adapter contracts only); no requirement text changes.

## Impact

- Affected code: `lib/supervisor/group.ex` (primary), `lib/supervisor/db/db.ex` (count/select helpers), `lib/supervisor/looper.ex` (`:broadcast_to_group` error propagation, restart paths), `lib/supervisor/supervisor.ex` (Group API docs/specs only if error atoms change).
- APIs: Supervisor Group APIs keep success shapes (`{:ok, id}`, `:ok`); error atoms become consistent (see BREAKING note above for direct `Group` users).
- Dependencies: none new; ETS (`:ets`), `Logger`, existing `ErrorHandler`, `Utils`.
- Systems/tests: `test/standart_test/group_test.exs`, `my_group_api_test.exs`, `workload_test/group_workers_test.exs`, `benchmarks/bench_quick.exs`; no migration (ETS-only runtime state).
