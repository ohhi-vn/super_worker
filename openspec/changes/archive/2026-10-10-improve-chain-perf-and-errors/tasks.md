# Tasks

## 1. Baseline and regression tests

- [x] 1.1 Record the pre-change baseline with `mix run benchmarks/bench_main.exs` (chain message passing and add-worker rows) into `benchmarks/CHAIN_BASELINE.md` and verify the file lists throughput for each row
- [x] 1.2 Add failing regression test: a multi-replica node delivers to every replica (verify test fails before the fix: only the last replica receives data)
- [x] 1.3 Add failing regression test: removing a middle node then sending data does not run the finished callback early and reaches the remaining nodes (verify failure before fix)
- [x] 1.4 Add failing regression test: a message routed to a killed replica does not leave an entry in the upstream queue (verify failure before fix via queue size)
- [x] 1.5 Add failing regression test: `:rest_for_one` crash of a middle node restarts the tail instead of raising (verify failure before fix)
- [x] 1.6 Add failing regression test: `remove_worker/2` on a missing id returns `{:error, :worker_not_found}` (verify failure before fix: returns `true`)
- [x] 1.7 Add failing regression test: a multi-replica add with a conflicting replica id spawns no replica (verify failure before fix)
- [x] 1.8 Add failing regression test tagged `:slow`: a chain worker that receives no message for 31 s keeps the same pid (verify failure before fix: the pid changes after the 30 s exit and restart)

## 2. Routing storage in Db

- [x] 2.1 Change `Db.put_chain_order/5` to store a replica list per `{:chain_order, chain_id, order}` with replace-by-worker-id semantics, and add `Db.delete_chain_replica/4` that removes one replica and deletes the row when empty; verify with `test/standart_test/chain_db_test.exs` (append, replace on restart, delete last replica)
- [x] 2.2 Add node counter helpers `Db.next_chain_order/2`, `Db.release_chain_order/2`, and `Db.get_chain_node_count/2` using `:ets.update_counter/4` with default insert; verify counter increments and decrements in the Db unit test
- [x] 2.3 Add round-robin counter helper `Db.next_round_robin/3` (atomic `update_counter`); verify it returns 0,1,2,0,... modulo replica count in the Db unit test
- [x] 2.4 Delete the node counter and round-robin rows in `Db.delete_chain/2`; verify no `:chain_nodes` or `:chain_rr` rows remain after chain removal
- [x] 2.5 Make `Chain.count_workers/1` call the count-only `Db.count_workers_by_parent/2` instead of materializing worker infos; verify existing `count_workers` tests pass

## 3. Chain membership: add, remove, shift

- [x] 3.1 Rewrite `Chain.add_worker/2` to allocate the order from the node counter after successful spawn, and pre-check all `{:multi_workers, id, i}` ids before spawning; remove `{:ok, _} =` matches in the reduce; verify regression test 1.7 passes and existing add tests still pass
- [x] 3.2 Rewrite `Chain.remove_worker/2` to return `:ok` or `{:error, :worker_not_found}`, release the replica entry, and when the last replica leaves delete the node row, shift later nodes down (rows and worker-info `order`), and decrement the counter; verify regression tests 1.4 and 1.6 pass and a 3-node removal of the middle node leaves orders 1, 2
- [x] 3.3 Update `Looper` `:remove_chain_worker` clause to pass the `Chain.remove_worker/2` result through without `true` matching; verify the public `Sup.remove_chain_worker/3` returns `:ok` for an existing worker and `{:error, :worker_not_found}` for a missing one
- [x] 3.4 Verify `Chain.remove_chain` path in Looper removes all replicas and leaves no `:chain_order` rows; add a test that re-adding a chain with the same id starts at order 1 with no stale routing
- [x] 3.5 Make `kill_worker/2` return the same `{:ok, chain}` / `{:error, :not_found}` contract without touching routing rows; verify kill keeps the replica entry so restart can replace the pid

## 4. Messaging and forwarding

- [x] 4.1 Make each worker read its own current order from its worker-info row per message and remove the first-node `get_chain_order/3` check; verify confirmations are still sent upstream for non-first nodes and not for the first node
- [x] 4.2 Capture the chain struct in the worker spawn closure and remove the per-message `Db.get_chain/2`; verify `forward_data/6` has no `Db` match that can raise
- [x] 4.3 Implement replica selection in `Chain.Messaging.send_next/3` for `:broadcast`, `:random`, `:round_robin`, `:partition`, with `Process.alive?/1` filtering and `{:error, :no_live_replica}`; verify each send type with a dedicated test (see 4.4)
- [x] 4.4 Add tests for each send type: round-robin spread over 3 replicas (2 each for 6 messages), broadcast reaching all replicas, partition stable per data, random reaching exactly one replica, single replica receiving under every type
- [x] 4.5 Update `forward_data/6` to drop the queue entry for any result other than `{:ok, :sent}` and log `{:error, reason}`; verify regression test 1.4 passes and the queue length returns to 0 after a no-consumer forward
- [x] 4.6 Verify the finished callback runs once per completed copy with `:broadcast` and once for normal send types, including `{:fun, f}` and `{m, f, a}`, and that callback failures return `{:error, reason}` with the real reason

## 5. Restart strategies and error reporting

- [x] 5.1 Rewrite `Chain.restart_all_workers/1` to collect per-worker outcomes through a shared `respawn/2` helper and return `{:error, {:restart_failed, ids}}` when any fail; remove the `{:ok, workers} =` match; verify the one_for_all existing tests pass and a unit test covers the failure list
- [x] 5.2 Add `Chain.restart_from_worker/2` for `:rest_for_one` (failed order and higher, ascending) using `respawn/2`; verify regression test 1.5 at the Supervisor level and a unit test asserting the lower node keeps its pid
- [x] 5.3 Add a `:rest_for_one` clause to `Looper.restart_chain/4` and make all three clauses log the restart result instead of discarding it; verify a crash in a 3-node `:rest_for_one` chain restarts only the tail and logs no error for normal exits
- [x] 5.4 Verify `restart_worker/2` still respawns when the ref row is already cleaned up and returns `{:error, :worker_not_found}` for unknown ids; keep the existing tests green
- [x] 5.5 Remove the dead `try/catch` around `Process.link/1` in `do_spawn_worker/2`; verify spawn and link behavior is covered by the existing add/restart tests

## 6. Validation and idle behavior

- [x] 6.1 Move chain option rules into `Chain.Config.new/1` and make `Chain.check_options/1` delegate to it; remove duplicate `validate_*` functions from `chain.ex`; verify `chain_config_test.exs` and the `check_options` test in `chain_test.exs` pass
- [x] 6.2 Remove the `after 30_000` exit from `loop_chain/3`, keeping the 5 s `loop_send/3` wait; verify regression test 1.8 passes and the queue-full timeout test still exits with `:queue_full`
- [x] 6.3 Remove duplicate `@spec` lines in `chain.ex` and run `mix compile --warnings-as-errors`; verify it compiles with no warnings

## 7. Verification of every public Chain function and feature

- [x] 7.1 Add `test/standart_test/chain_contract_test.exs` covering `check_options/1`, `get_worker/2`, `worker_exists?/2`, `get_all_workers/1`, `add_worker/2`, `restart_worker/2`, `restart_all_workers/1`, `restart_from_worker/2`, `count_workers/1`, `remove_worker/2`, `kill_worker/2` with one happy-path and one error-path assertion each; verify `mix test test/standart_test/chain_contract_test.exs` passes
- [x] 7.2 Add contract tests for `Chain.Messaging.new_data/2` and `send_next/3` (order 1 delivery, end-of-chain callback, `:no_live_replica`); verify with `mix test test/standart_test/chain_messaging_test.exs`
- [x] 7.3 Update `test/standart_test/chain_config_test.exs` for the single validation path (each invalid field and the valid case); verify the file passes
- [x] 7.4 Update `Chain` moduledoc and `guides` chain section to document the send types as implemented, replica behavior, restart strategies, and error return shapes; verify the documented example in the guide runs under `mix run` as written

## 8. Integration and performance checks

- [x] 8.1 Run the full chain test set (`mix test test/standart_test/chain_test.exs test/workload_test/chain_workers_test.exs test/standart_test/function_chain_test.exs`) and verify zero failures
- [ ] 8.2 Run the full test suite `mix test` and verify no new failures compared with the pre-change run
- [x] 8.3 Run `mix run benchmarks/bench_main.exs` again and compare with `benchmarks/CHAIN_BASELINE.md`; verify chain message passing throughput is not lower than baseline and add-worker cost for 1000 nodes is sub-quadratic
- [x] 8.4 Run `mix format --check-formatted` and `mix credo` (if configured) on the changed files; verify no formatting or credo issues in `lib/supervisor/chain`, `lib/supervisor/db/db.ex`, and `lib/supervisor/looper.ex`

## Workflow follow-up

- Review the change with the project's review process before implementation is accepted.
- Run `openspec validate improve-chain-perf-and-errors --strict` and fix any delta formatting issues.
- Archive the change after implementation and verification are complete.
