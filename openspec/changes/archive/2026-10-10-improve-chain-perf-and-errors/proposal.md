# Proposal

## Why

`SuperWorker.Supervisor.Chain` (`lib/supervisor/chain/chain.ex`) is the sequential pipeline node of the supervisor. Reading the code and its call sites shows problems that affect correctness under normal use, not only speed:

- **Replicas never receive data.** `add_worker/2` with `num_workers > 1` gives all replicas one shared order, but `Db.put_chain_order/5` stores a single `{worker_id, pid}` per order, so each replica overwrites the previous one and only the last replica is routable.
- **`send_type` has no effect.** `:broadcast`, `:random`, `:partition` and `:round_robin` are validated and stored, but `Chain.Messaging.send_next/3` routes purely by order.
- **Stale routing after removal.** `remove_worker/2` and `remove_chain` never delete `{:chain_order, chain_id, order}` rows. Routing can point at dead pids, and `add_worker/2` assigns `count + 1`, which can collide with an existing order after a removal. A message sent to a dead pid still leaves its queue entry in place, which wedges the upstream worker.
- **Gaps end the chain early.** If a middle order is missing, `send_next/3` treats it as end-of-chain and runs the finished callback prematurely.
- **`rest_for_one` crashes the partition.** `:rest_for_one` is accepted by validation, but `Looper.restart_chain/4` only has `:one_for_one` and `:one_for_all` clauses, so a crash raises `FunctionClauseError`.
- **Silent or inconsistent errors.** `remove_worker/2` returns `true` whether or not the worker existed. `restart_all_workers/1` builds a failure list that can never be populated. Looper ignores the results of `restart_worker/2` and `restart_all_workers/1`. Multi-replica `add_worker/2` pattern-matches `{:ok, chain} = ...` inside a reduce, so a conflict raises and leaves earlier replicas running.
- **Crash-prone hot path.** `forward_data/6` matches `{:ok, chain} = Db.get_chain/2` for every message, and `restart_all_workers/1` matches `{:ok, workers} = ...`.
- **Duplicated validation.** `Chain.check_options/1` and `Chain.Config.new/1` implement the same checks separately; only `check_options/1` runs in production paths.
- **Per-message and per-add overhead.** Each forwarded message performs several ETS reads (`get_chain/2`, `get_chain_order/3` in `loop_chain/3`, and again in `send_next/3`). `add_worker/2` loads every worker of the chain to compute the next order (O(n) per add). `count_workers/1` materializes all worker infos.
- **Idle timeout restarts.** Chain workers exit with `:timeout` after 30 seconds without a message and the supervisor restarts them as abnormal exits.

The change fixes these problems, implements the documented send types across replicas, and adds a verification pass that checks every public function of `Chain` against the new capability spec.

## What Changes

- Store all replicas of a node in the routing entry and make every replica routable.
- Implement the four send types as replica routing: `:broadcast` (all replicas), `:random`, `:round_robin` (per-node counter), and `:partition` (hash of data).
- Keep chain orders contiguous: removing the last replica of a node releases its order and shifts later nodes down; add allocates the next order from an O(1) counter.
- Delete routing rows on worker removal, kill-without-respawn, and chain removal, so no routing entry points at a dead pid.
- Implement `:rest_for_one` restart (failed worker plus every higher order) in the chain restart path.
- Make `remove_worker/2` return `:ok` or `{:error, :worker_not_found}`. **BREAKING** for direct callers that matched `true`; the only in-repo caller is `Looper`, which is updated in the same change.
- Return error tuples instead of raising in `add_worker/2` (all-or-nothing multi-replica add), `restart_all_workers/1` (per-worker failure list), and `forward_data/6` (no per-message chain lookup).
- Remove the idle `:timeout` exit: chain workers stay alive when idle.
- Consolidate chain option validation into one code path; `Chain.Config.new/1` delegates to it or is removed if unused.
- Reduce ETS reads per forwarded message and count workers without materializing them.
- Add regression tests for every bug above before fixing it, and a verification test per public `Chain` function and per send type / restart strategy.

## Capabilities

### New Capabilities

- `supervisor-chain`: Observable contract of supervisor chain nodes: option validation, worker add/remove/restart/kill, replica routing by send type, sequential forwarding with backpressure, restart strategies, finished callbacks, error return shapes, and scale budgets. Sibling of the existing `supervisor-group` capability.

### Modified Capabilities

- None. `supervisor-group` and `function-chain` are not changing requirements. `function-chain` covers the in-process `SuperWorker.FunctionChain` pipeline, which is a separate feature.

## Impact

- **Code**: `lib/supervisor/chain/chain.ex`, `lib/supervisor/chain/messaging/messaging.ex`, `lib/supervisor/chain/config/config.ex`, `lib/supervisor/looper.ex` (`restart_chain` clauses, remove result handling, ignored restart results), `lib/supervisor/db/db.ex` (chain order rows become replica lists; order counter; count helper), `lib/supervisor/supervisor.ex` (only if chain API error shapes change).
- **Tests**: `test/standart_test/chain_test.exs`, `chain_messaging_test.exs`, `chain_config_test.exs`, `test/workload_test/chain_workers_test.exs`, plus a new supervisor-chain verification suite.
- **Benchmarks**: `benchmarks/bench_main.exs` ("Chain worker message passing") is the before/after baseline.
- **Public API**: `remove_worker/2` result changes from `true` to `:ok` / `{:error, :worker_not_found}`. Other documented return shapes are preserved; new error shapes are added only where the code currently raises or returns an incorrect value. No new dependencies.
- **Behavior changes**: multi-replica nodes start receiving data (previously only the last replica did); `:broadcast` delivers duplicate copies downstream; idle chain workers no longer restart every 30 seconds; `:rest_for_one` works instead of crashing.
- **Operational risk**: A message in flight to a worker that is killed before respawn is dropped; this is documented, not hidden.
