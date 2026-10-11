# Design

## Context

See `proposal.md` — Why for motivation and `specs/supervisor-standalone/spec.md` for the required behavior.

A standalone worker is started by the partition that owns it: `Looper.start_worker/3` (`lib/supervisor/looper.ex:679`) writes a worker-info row, calls `sup_start_child/2` (`looper.ex:866`), and always replies `{:ok, worker.id}`. `sup_start_child` spawns with `spawn_monitor/1` for functions/mfa (or `apply/3` for `{:gen_server, ...}`), optionally `Process.link/1`s it, and writes the ref row with the scanning `Db.put_worker/5` (`db.ex:32`). All worker DB state lives in one public ETS `:set` keyed by `{:ref, ref}` plus `{:worker, id, {type, parent}}` info rows, shared by every partition; by-id lookups (`Db.get_worker_by_id/3`, `db.ex:61`) are `match_object` scans. The partition runs a single `Looper.main_loop/1` that serializes all public API messages and worker `:DOWN`s, so per-worker bookkeeping can live in the partition's loop state.

Restart strategy options `:max_restarts`, `:max_seconds`, and `:auto_restart_time` are accepted by `Constants.Types.standalone_params/0` (`types.ex:30`) but are not struct fields on `SuperWorker.Supervisor.Worker` and are never read, so they are silently dropped and `:permanent` workers crash-loop unbounded (`looper.ex:943`).

## Goals / Non-Goals

**Goals:**
- `add_standalone_worker` reports real spawn outcomes and leaves no orphan rows on failure.
- Standalone not-found errors match the `ErrorHandler` vocabulary used by groups.
- `:permanent` crash loops are bounded by `:max_restarts` / `:max_seconds`, with optional `:auto_restart_time` recovery.
- Removal is clean and warning-free; the trailing `:DOWN` is treated as already handled.
- Fresh adds are O(1) (no per-add full-table scan); shutdown uses pid/id-only selects with one lookup per worker.
- The partition keeps serving other workers through spawn failures and exceeded intensity.

**Non-Goals:**
- No change to group or chain behavior, or to the shared by-id lookup shape (still a scan; we reduce how often it is called).
- No new ETS secondary index or table-layout migration.
- No graceful (non-`:kill`) shutdown — the existing `Looper` TODO stays.
- No persistence of restart intensity across a partition restart.
- No change to partition hashing or success shapes (`{:ok, id}`, `{:ok, pid}`).

## Decisions

### D1. Honest spawn results and atomic add

`sup_start_child/3` returns `{:ok, ref, pid}` on success or `{:error, {:spawn_failed, cause}}` on failure, and never swallows errors. GenServer starts wrap `apply/3` in `try/catch` so a raise/exit becomes a cause-preserving error; `spawn_monitor/1` for functions/mfa cannot fail to spawn (an immediately dying process is a later `:DOWN`, not a spawn failure). `start_worker/3`'s `:standalone` branch calls `sup_start_child` first, and only on success writes the worker-info row and ref row and replies `{:ok, id}`; on failure it replies the error and writes nothing. Both rows are written in straight-line code inside the same loop iteration, so no `:DOWN` can interleave between them.

- *Alternative:* keep `put_worker_info` before spawn and compensate on failure. Rejected: compensation windows and the current orphan-row bug; insert-after-success is strictly smaller.
- *Alternative:* make function/mfa spawns report failure by running the function synchronously once. Rejected: changes worker semantics and would execute user code twice.

### D2. Restart intensity in partition loop state

Add `max_restarts`, `max_seconds`, and `auto_restart_time` fields to `SuperWorker.Supervisor.Worker` (defaults `10`, `10`, `nil`), validate them as positive integers in `Worker.from_config/1`, and return `{:error, {:invalid_options, detail}}` (not a bare string) for a bad strategy. The partition tracks `restarts: %{worker_id => [monotonic_ms]}` in its loop state. On each crash, `restart_standalone/3` drops timestamps older than `:max_seconds`; below the ceiling it records the timestamp and restarts; at/above the ceiling it stops restarting, logs the cause, and (when `:auto_restart_time` is set) schedules `Process.send_after(self(), {:restart_worker, worker_id}, auto_restart_time)`. The `{:restart_worker, id}` handler restarts only if the worker's info row still exists (the user may have removed it during the cooldown).

`process_worker_down/2` currently discards `restart_standalone`'s returned state and calls `main_loop(state)` with the original state (`looper.ex:748-778`); this change threads the updated state (`state = restart_standalone(...)`) so intensity history persists. This is a required wiring fix, not just bookkeeping.

- *Alternative:* persist restart history in ETS per worker. Rejected: adds a write per crash and needs its own cleanup; the partition is the single serial owner, so in-memory state suffices.
- *Alternative:* terminate the partition when intensity is exceeded (OTP supervisor behavior). Rejected: it would kill unrelated workers on the partition; the spec requires the partition to stay healthy.
- *Trade-off:* history is lost if the partition itself restarts, briefly allowing a fresh window of restarts. Acceptable: partition restart is rare and unrelated to worker crash loops.

### D3. Unified not-found errors

Route standalone lookups through `ErrorHandler`: extend `worker_lookup_error/2` (`looper.ex:853`) so `{:standalone, nil}` (and the send/remove handlers that currently return the raw error) return `{:error, :worker_not_found}`. The `Db` layer keeps returning `{:error, :not_found}`; the mapping happens at the partition/API boundary, exactly as groups already do.

- *Alternative:* change `Db.get_worker_by_id/3` to return `:worker_not_found`. Rejected: `Db` is shared with group/chain and must stay vocabulary-neutral; boundary mapping is the established pattern.

### D4. Removal hygiene

`remove_standalone_worker` keeps `Process.exit(pid, :kill)` (untrappable, so removal always succeeds) and deletes both rows. The ensuing `:DOWN` misses both rows in `process_worker_down/2`'s generic clause; change that clause to log at debug (no warning) when the ref row is already gone, while still warning when a ref exists but its info row is missing (genuine corruption). This removes the spurious warning without weakening real diagnostics.

- *Alternative:* kill with a dedicated reason (`:removed`) and add a `:DOWN` clause for it. Rejected: a non-`:kill` signal may not terminate a process that traps exits, so removal would not be guaranteed.

### D5. Cheaper standalone reads

- Fresh adds persist via `Db.put_new_worker/5` (`:fresh` mode) instead of `Db.put_worker/5`, removing the per-add full-table stale scan; restarts keep `Db.put_worker/5` (`:restart` mode) as a defensive purge, matching the chain decision (`2026-10-10-improve-chain-perf-and-errors` D6). The restart case is not on the add hot path.
- `shutdown/2` (`looper.ex:105-115`) replaces `get_all_standalone_worker_infos` + per-worker `get_worker_by_id` with `Db.get_worker_pids_by_parent(table, {:standalone, nil})` (an existing pid/id-only select) filtered by `partition_owns?`, then kills the pid directly — one scan total instead of one per worker.
- `supervisor_info/2` uses `Db.count_workers_by_parent/2` for `num_standalone_workers` instead of materializing every info struct.
- `list_standalone_workers/2` keeps the struct read (it needs `name`/`restart_strategy`) since it is a rare introspection call.

### D6. State threading through the loop

`main_loop/1` gains a `{:restart_worker, worker_id}` clause for `auto_restart_time` timers. The `:DOWN`/public-api handlers continue to return the state they pass to `main_loop`, and the standalone restart path now threads it as described in D2.

## Risks / Trade-offs

- **Intensity history lost on partition restart** → documented; partition restarts are rare and independent of worker crash loops.
- **`{:restart_worker, id}` fires after a user removed the worker** → handler checks the worker-info row still exists before restarting; removal deletes it.
- **`put_new_worker/5` name is misleading; it upserts but does not purge other ref rows for the id** → on both add (no rows) and restart (`process_worker_down` deleted the old ref first) there is no competing row; the restart path uses `put_worker/5` anyway.
- **BREAKING `:not_found` → `:worker_not_found`** → in-repo tests asserting `{:error, :not_found}` are updated; `CHANGELOG.md` gets a BREAKING note; success shapes unchanged.
- **Crash-looping worker with no `auto_restart_time` stays stopped** → intended per spec; the ceiling-exceeded event is logged with the cause, and the caller can re-add or configure `auto_restart_time`.
- **Blocking GenServer `init` still runs in the partition loop** → unchanged; noted as a non-goal.

## Migration Plan

No persisted state (ETS runtime-only), so no data migration. Deploy `looper.ex`, `supervisor.ex`, `db.ex`, and `worker.ex` together (one behavior change: spawn results and error atoms). Rollback is a revert; in-memory worker state is rebuilt when the supervisor restarts. Update `test/standart_test/standalone_test.exs` error-atom assertions and add a `CHANGELOG.md` BREAKING note for the not-found atom.

## Open Questions

- None blocking. Defaults `max_restarts: 10`, `max_seconds: 10` (matching `SuperPool`), `auto_restart_time: nil` (no auto-resume unless configured) are recorded here; they can be adjusted during implementation without changing the specs.
