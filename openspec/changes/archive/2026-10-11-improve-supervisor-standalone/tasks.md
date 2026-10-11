# Tasks

## 1. Baseline and verification harness

- [x] 1.1 Capture the pre-change baseline by running `mix test test/standart_test/standalone_test.exs test/workload_test/standalone_workers_test.exs` and `mix run benchmarks/bench_quick.exs`, and record pass/fail plus add/message/shutdown timings for later comparison.
- [x] 1.2 Build a function-by-function verification matrix covering `Supervisor.add_standalone_worker/4`, `send_to_standalone_worker/4`, `get_pid_standalone_worker/3`, `remove_standalone_worker/3`, `list_standalone_workers/2`, the `Looper` start/DOWN/remove/shutdown paths, and all three restart strategies, and verify every matrix row maps to an existing or newly named test.

## 2. Worker options and struct plumbing

- [x] 2.1 Add `max_restarts`, `max_seconds`, and `auto_restart_time` fields to `SuperWorker.Supervisor.Worker` (defaults `10`, `10`, `nil`) and validate them as positive integers in `Worker.from_config/1`, and verify `mix test test/standart_test/constants_test.exs` still passes and a new unit test rejects non-positive values with `{:error, {:invalid_options, _}}`.
- [x] 2.2 Change `Worker.validate_restart_strategy/1` to return `{:error, {:invalid_options, detail}}` instead of a bare string, and verify the invalid-strategy test asserts `{:error, {:invalid_options, _}}` and starts no process.
- [x] 2.3 Verify the three intensity fields survive `Worker.from_config/1` into the struct (no silent drop) with a unit test asserting each field round-trips.

## 3. Honest spawn results and atomic add

- [x] 3.1 Refactor `Looper.sup_start_child/3` to return `{:ok, ref, pid}` on success and `{:error, {:spawn_failed, cause}}` on failure, wrapping the GenServer `apply/3` in `try/catch` to preserve the cause, and verify a failing GenServer start returns the cause-preserving error.
- [x] 3.2 Rewrite `Looper.start_worker/3`'s `:standalone` branch to call `sup_start_child` first and persist the worker-info and ref rows only on success, replying `{:error, {:spawn_failed, _}}` and leaving zero rows on failure, and verify a new test asserts no info/ref row remains and the partition still serves other workers.
- [x] 3.3 Verify a successful add still returns `{:ok, id}` and the worker is retrievable, and that a duplicate id still returns `{:error, :worker_already_exists}` via `mix test test/standart_test/standalone_test.exs --only standalone_add_workers` and the duplicate-id workload test.

## 4. Restart intensity and state threading

- [x] 4.1 Add `restarts: %{}` to the partition loop state and thread the updated state through `process_worker_down/2` (`state = restart_standalone(state, ...)`) so intensity history persists, and verify existing restart tests (`standalone_restart_worker`, `standalone_restart_gen_server_worker`) still pass.
- [x] 4.2 Implement windowed intensity in `restart_standalone/3`: drop timestamps older than `max_seconds`, restart and record while below `max_restarts`, and stop with a cause-carrying log once the ceiling is reached, and verify a crash-loop test (a worker that always exits abnormally) stops restarting and leaves the partition serving other workers.
- [x] 4.3 Schedule `Process.send_after(self(), {:restart_worker, id}, auto_restart_time)` when the ceiling is exceeded and `auto_restart_time` is set, add the `{:restart_worker, id}` `main_loop/1` clause that restarts only if the worker-info row still exists, and verify a test shows restart resumes after the cooldown and does not restart a worker removed during the cooldown.
- [x] 4.4 Verify strategy semantics are unchanged by running `mix test test/workload_test/standalone_workers_test.exs` (permanent/transient/temporary and mixed-strategy tests) all green.

## 5. Error contract and removal hygiene

- [x] 5.1 Extend `worker_lookup_error/2` so `{:standalone, nil}` maps to `ErrorHandler.not_found(:worker)`, and route the `:send_to_standalone_worker` and `:remove_standalone_worker` handlers through it, and verify send/get/remove of an unknown id all return `{:error, :worker_not_found}`.
- [x] 5.2 Update `test/standart_test/standalone_test.exs` assertions that expect `{:error, :not_found}` (send and remove tests) to `{:error, :worker_not_found}`, and verify `mix test test/standart_test/standalone_test.exs` is green.
- [x] 5.3 Change `process_worker_down/2`'s generic clause to log at debug without a warning when the ref row is already gone, while still warning when a ref exists but its info row is missing, and verify the remove test produces no "unexpected worker data" warning (via `@moduletag :capture_log` assertion or log review) and the worker is unreachable afterwards.
- [x] 5.4 Add a `CHANGELOG.md` entry noting the BREAKING standalone not-found atom change and the new spawn-failure/restart-intensity behavior, and verify the entry references the correct error atoms.

## 6. Standalone read and lifecycle performance

- [x] 6.1 Give `sup_start_child/3` a `:fresh | :restart` mode: fresh adds use `Db.put_new_worker/5`, restarts use `Db.put_worker/5`, and verify a fresh add no longer performs the stale-row scan (assert via code inspection and an add-scaling benchmark).
- [x] 6.2 Rewrite the standalone branch of `Looper.shutdown/2` to use `Db.get_worker_pids_by_parent(table, {:standalone, nil})` filtered by `partition_owns?` and kill pids directly, and verify a shutdown test terminates all owned standalone workers with no leftover rows.
- [x] 6.3 Switch `supervisor_info/2`'s `num_standalone_workers` to `Db.count_workers_by_parent/2`, and verify `supervisor_info` still reports the correct standalone count via the existing introspection test.
- [x] 6.4 Run `mix run benchmarks/bench_quick.exs` and verify add/message/shutdown timings are within budget and no full-table scan remains on the fresh-add path.

## 7. Integration verification and docs

- [x] 7.1 Update the `Supervisor` standalone API `@doc`/`@spec` text to document returned error atoms, spawn-failure, and restart-intensity options, and verify documented atoms match the implementation by doc review.
- [x] 7.2 Run `mix test test/standart_test/standalone_test.exs test/workload_test/standalone_workers_test.exs` and a full `mix test`, and verify all green (or record pre-existing failures as evidence).
- [x] 7.3 Run `mix credo lib/supervisor/looper.ex lib/supervisor/db/db.ex lib/supervisor/worker/worker.ex lib/supervisor/supervisor.ex` (or `mix dialyzer` if available), and verify no new warnings on the changed files.

## Workflow follow-up

- Archive the change after review requirements are satisfied.
- Verify the archived result updates `openspec/specs/supervisor-standalone/spec.md`.
