# Tasks

## 1. Baseline and verification harness

- [x] 1.1 Capture pre-change baseline by running `mix test test/standart_test/group_test.exs test/standart_test/my_group_api_test.exs` and `mix run benchmarks/bench_quick.exs`, and verify both complete with recorded timings for later comparison.
- [x] 1.2 Build a function-by-function verification matrix covering all 14 Group functions plus Supervisor Group APIs and both restart strategies, and verify every matrix row maps to an existing test or a new test name owned by a later task.

## 2. ETS read-shape performance foundation

- [x] 2.1 Add `Db.count_workers_by_parent/2` via `:ets.select_count/2` reusing the `get_worker_infos_by_parent` match-spec, with unit coverage for 0/1/N members, and verify `mix test test/standart_test/db_test.exs` passes.
- [x] 2.2 Add pid/id-only select for `get_worker_pids_by_parent` (no full `Worker` struct materialization) and verify count-vs-oracle test (select_count equals `get_all_workers` length) passes.
- [x] 2.3 Switch `Group.count_workers/1` to the new count helper and `kill_all_workers/2` iteration to the pid-only select, and verify `mix test test/standart_test/group_test.exs` count/kill cases pass.

## 3. Validation, add path, and spawn atomicity

- [x] 3.1 Harden `Group.check_options/1` to reject nil/missing `id` with `{:invalid_options, _}` and map invalid strategy from bare string to `{:invalid_options, _}`, with tests for missing id / bad strategy / valid creation, and verify the new tests pass.
- [x] 3.2 Make `Group.add_worker/2` single-lookup (one `get_worker_info` read, no separate `worker_exists?` round-trip), return `:worker_already_exists` on duplicate, generate random id for nil/false, and verify duplicate/missing-id scenarios pass.
- [x] 3.3 Make failed spawns atomic (no info or ref row left behind) with `{:error, {:spawn_failed, cause}}` preserving cause for GenServer start error/raise/exit cases, and verify spawn-failure tests plus partition-still-healthy assertion pass.
- [x] 3.4 Simplify `Group.to_struct/1` to `struct/2` + `Map.take` with unchanged semantics, and verify `mix test test/standart_test/group_test.exs test/standart_test/validator_test.exs` passes.

## 4. Single-lookup remove, kill, and restart

- [x] 4.1 Rewrite `Group.remove_worker/2` as one info read plus one ref read followed by kill and joint ref+info delete, returning `{:ok, :worker_removed}` or `{:error, :worker_not_found}`-family, and verify live-remove and dead-row tests pass with zero leftover ETS rows asserted.
- [x] 4.2 Converge both `kill_worker` clauses on one lookup with `:worker_not_found` for missing rows and normalized dead-pid error, rework `kill_all_workers/2` to pid-only iteration returning `:ok` or `{:error, [{:error, worker_id, reason}]}`, and verify kill/kill-all tests pass.
- [x] 4.3 Rewrite both `restart_worker` clauses to thread the single fetched worker through kill, clean both ref and info rows on `:not_alive`/`:not_found` stale paths before respawn, and verify live-restart, dead-restart-recovery, and unknown-id tests pass.
- [x] 4.4 Verify `:one_for_one` vs `:one_for_all` crash isolation and `:normal`-exit-no-restart via stateful-sibling tests, and verify `mix test test/standart_test/group_test.exs --only group_restart_all_workers` and related restart tags pass.

## 5. Messaging, Looper propagation, and registration race

- [x] 5.1 Harden `Group.send_message/3` with alive-check returning `:worker_not_found` for missing/dead targets and `Group.broadcast/2` on pid-only select with per-pid `{:error, pid, reason}` collection, and verify direct-send and broadcast N-worker tests pass.
- [x] 5.2 Fix `Looper` `:broadcast_to_group` to forward `Group.broadcast/2` errors instead of hard-coding `{:ok, :sent}`, and verify a partial-failure broadcast test asserting `{:error, errors}` at the Supervisor API passes.
- [x] 5.3 Fix `register_worker_name/1` check-then-act race with atomic `Process.register` rescue on `ArgumentError`, keep duplicate-name warning, and verify named-worker and duplicate-name tests pass.
- [x] 5.4 Unwrap `{_, id}` worker ids consistently across `get_worker`, kill, restart, remove, and send paths with shared helper, and verify tuple-id test passes.

## 6. Error-contract sweep, docs, and full verification

- [x] 6.1 Sweep `Group` for legacy shapes (`:worker_exists`, `:cannot_send`, bare `:not_found`/`:not_alive`, bare strings) replacing with `ErrorHandler` atoms, grep in-repo callers (`Looper`, `Supervisor`, tests) and update matches, and verify `rg "worker_exists|:cannot_send|Invalid group restart strategy" lib test` shows only intentional test fixtures.
- [x] 6.2 Update Supervisor Group API docs/specs and add CHANGELOG BREAKING note for tightened error atoms, and verify documented error atoms match implementation via doc review.
- [x] 6.3 Run full verification: `mix test test/standart_test/group_test.exs test/standart_test/my_group_api_test.exs test/workload_test/group_workers_test.exs`, plus `mix test` for regressions, and verify all green (or record pre-existing failures as evidence).
- [x] 6.4 Run scale evidence: 1000-worker count/broadcast/kill-all via `mix run benchmarks/bench_quick.exs` plus `mix credo lib/supervisor/group.ex lib/supervisor/db/db.ex` (or `mix dialyzer` if available), and verify timings within budget with no full-struct scan on count-only path.
