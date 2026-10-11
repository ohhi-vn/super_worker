# Design

## Context

A chain is a set of nodes ordered 1..n. Each node has one or more replica processes (`num_workers`). Routing is ETS-based: `{:chain_order, chain_id, order}` maps an order to the replicas that serve it. Each worker process loops on its mailbox, runs the user function, confirms the upstream message with `{:processed, ...}`, and forwards the result to the next order via `Chain.Messaging.send_next/3`. When no next order exists, the finished callback runs.

Writes to chain tables happen in the owning partition's message handling (public API and crash-handler restarts). Chain rows are in-memory ETS; there is no persisted data, so no data migration is needed.

See `proposal.md` for the defects and `specs/supervisor-chain/spec.md` for the required behavior.

## Goals / Non-Goals

**Goals:**
- Every replica is routable; `send_type` selects replicas as specified.
- Orders stay contiguous, so `:not_found` at `order + 1` means end-of-chain and never a gap.
- No routing entry references a removed or killed-without-respawn pid.
- Errors are returned as tuples; no `=` matches on fallible Db calls in add, remove, restart, or forward paths.
- Per-message ETS reads and per-add work do not grow with chain length.
- Validation has one definition.

**Non-Goals:**
- Persisting chains across supervisor restarts.
- Exactly-once delivery. A message sent to a worker that dies before respawn is dropped and logged.
- Changing `SuperWorker.FunctionChain` (separate capability).
- Changing `:one_for_all` semantics beyond returning per-worker failures.
- Changing the 5-second queue wait or queue semantics.

## Decisions

### D1. Node routing row holds a replica list

`{:chain_order, chain_id, order}` stores `[{worker_id, pid}, ...]` for all live replicas of the node instead of a single tuple. `put_chain_order/5` appends or replaces the entry for `worker_id` (restart replaces the pid in place). `delete_chain_order` is called when the last replica leaves.

*Alternative considered:* one row per replica `{:chain_replica, chain_id, order, worker_id}` selected with match specs. Rejected: routing reads become selects on every hop, and the replica set is always tiny (replicas per node), so a single list read is cheaper and simpler.

### D2. Contiguous orders with an O(1) node counter

A counter `{:chain_nodes, chain_id}` is incremented with `:ets.update_counter/4` (default-insert 0) when a new node is added and decremented when a node is released. A new node's order is `counter + 1`. Node counter updates happen only after the node's replicas spawn successfully, so a failed add leaves no counter drift.

Removing the last replica of node `k` deletes its routing row, shifts rows `k+1..n` down by one, and updates each shifted worker's `order` in its worker-info row, then decrements the counter. This is O(n) per node removal; additions are O(1).

*Alternative considered:* sparse orders with "next existing order" lookup. Rejected: ETS `set` tables have no ordered successor lookup, and sparse orders would require scanning on every hop.

*Alternative considered:* leave gaps and treat `:not_found` as end-of-chain. Rejected: this is the current premature-callback bug.

### D3. Workers resolve their own order from ETS

Because removal can shift orders, a worker process cannot rely on the `order` captured in its spawn closure. Each received message reads the worker's current order from its worker-info row (one read). That read also replaces the current first-node check (`get_chain_order(table, chain_id, 1)` plus comparison by worker id), which only worked with one pid per order.

The chain struct needed for forwarding (`id`, `table`, `send_type`, `finished_callback`, `queue_length`) is captured in the spawn closure. These fields are immutable after creation, so `Db.get_chain/2` per message is removed.

*Risk:* a shift racing with an in-flight message can route one message to the adjacent node. See Risks.

### D4. Replica selection in Messaging

`Chain.Messaging.send_next/3` reads the replica list for `order` and selects according to `chain.send_type`:

| send_type | Target(s) |
|---|---|
| `:broadcast` | every replica |
| `:random` | `Enum.random/1` |
| `:round_robin` | `index = update_counter({:chain_rr, chain_id, order}, +1) rem n`, default-inserted at 0 |
| `:partition` | `Utils.get_hash_order(msg.data, n)` (1-based) |

A single replica always receives the message regardless of type. `send_next/3` returns `{:ok, :sent_to_one}` when one replica was signalled and `{:ok, :sent_to_many}` for broadcast (the existing public `send_to_chain` result is preserved), `{:error, :not_found}` only when no node exists at `order` (end-of-chain), and `{:error, :no_live_replica}` when the row exists but no pid is alive (checked with `Process.alive?/1`, since replicas are local). `forward_data/6` keeps the queue entry only for the two sent results and drops it otherwise, which fixes the wedge.

`:broadcast` sends one copy per replica. The sender's single queue entry is released by the first `{:processed, ...}` confirmation; later confirmations for the same id are no-ops (`MapQueue.remove/2` is idempotent). The finished callback runs once per completed copy at the last node (documented in the spec).

*Alternative considered:* per-replica queue entries for broadcast. Rejected: it multiplies backpressure accounting for a fan-out that is already documented as duplicate delivery.

### D5. Restart strategies

- `:one_for_one` → `restart_worker/2` (unchanged behavior; ref row cleanup tolerated).
- `:one_for_all` → `restart_all_workers/1`, rewritten to collect per-worker results and return `{:ok, chain}` or `{:error, {:restart_failed, failed_ids}}`.
- `:rest_for_one` → new `restart_from_worker/2`: selects the failed worker's order and all higher orders, sorts ascending, and respawns them with the same helper as restart-all.

A shared private helper `respawn(chain, worker)` kills the old pid, deletes its ref row, and calls `do_spawn_worker/2`. Looper's `restart_chain/4` gets a `:rest_for_one` clause and ignores normal exits as the other clauses do. Looper logs the restart result instead of dropping it.

### D6. Error contract and removal

- `remove_worker/2` returns `:ok` after killing the worker, deleting its ref and info rows, and releasing its routing entry (and shifting orders if it was the last replica). It returns `{:error, :worker_not_found}` otherwise. Looper's `:remove_chain_worker` clause passes the result through.
- `kill_worker/2` keeps `{:ok, chain}` / `{:error, :not_found}` and does not touch routing (restart needs the entry).
- `add_worker/2` writes the ref row with `Db.put_new_worker/5`, which skips the stale-row scan of `Db.put_worker/5` (that scan made each add O(N)). Restarts keep `put_worker/5`.
- `add_worker/2` for `num_workers > 1` validates that none of the `{:multi_workers, id, i}` ids exist before spawning any replica. The reduce no longer uses `{:ok, _} =` matches; add is all-or-nothing by pre-check.
- `forward_data/6` no longer pattern-matches `Db` results.

### D7. Idle workers

The `after 30_000 -> exit(:timeout)` clause is removed. `loop_chain/3` blocks on `receive` with no timeout. The `after timeout` in `loop_send/3` (5 s queue wait) is kept, because it defines the backpressure contract.

### D8. Single validation path

`Chain.Config.new/1` owns the rules (restart strategy, send type, callback, queue length) and returns `{:error, {:invalid_..., value}}`. `Chain.check_options/1` delegates to it and the duplicate `validate_*` functions in `Chain` are removed. `Supervisor`'s chain option path is unchanged in shape.

*Tradeoff:* error terms from `check_options/1` change from prose strings (`"Invalid send type, ..."`) to tagged tuples. Callers match on `{:error, _}`; the in-repo test asserting `{:error, _}` still passes. Logged messages change accordingly.

### D9. Cheaper counts and listings

`count_workers/1` uses `Db.count_workers_by_parent/2` (count-only select). `get_all_workers/1` is unchanged. Duplicate `@spec` lines are removed.

## Risks / Trade-offs

- **Order shift races with in-flight messages** → Shift runs inside one partition message handler, and the worker reads its order per message, so the window is one message; mitigation: a test sends traffic while removing an upstream replica and asserts no message reaches a wrong node in a steady-state check. If the race proves material, add a per-chain version check in `send_next/3`.
- **Broadcast duplicates downstream** → Documented in the spec; finished callback runs once per copy. Mitigation: the callback behavior is covered by a scenario.
- **Messages dropped during kill-before-respawn** → Restart respawns synchronously in the same handler, so the window is one handler invocation. Data sent in that window is dropped and logged rather than wedging the upstream queue.
- **Partition write serialization is assumed** → Chain tables are mutated from the owning partition's handler. Verify during implementation; if any path writes from a caller process, the counter and list updates need `:ets` atomic operations for the affected part.
- **`:round_robin` counter is per-chain, shared across upstream replicas** → Intended (the counter is the single source of routing order). Counter row is removed with the chain.
- **Dead `try/catch` around `Process.link/1` in `do_spawn_worker/2`** → Removed; the new worker cannot crash before linking because it runs no user code until it receives a message, and no message is sent before `add_worker/2` returns.

## Migration Plan

No persisted state. Deploy as a normal code change. Rollback is a revert; in-memory chain tables are rebuilt when the supervisor restarts. The `remove_worker/2` return change requires the Looper update in the same commit.

## Open Questions

None. Replica routing, send type semantics, idle behavior, and `:rest_for_one` were decided with the user or are specified above.
