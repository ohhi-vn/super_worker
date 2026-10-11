# Design

## Context

See `proposal.md` Why for motivation. Current state: `SuperWorker.Supervisor.Group` (`lib/supervisor/group.ex`) does up to 3 ETS reads per `remove_worker`/`restart_worker` (`worker_exists?` → `get_worker` → `kill_worker` → `Db.get_worker_by_id`), fetches full `Worker` structs for count/kill/broadcast, and returns mixed error shapes (atoms, bare strings, `:worker_exists` vs `:worker_already_exists`, `:cannot_send`). `Db` (`lib/supervisor/db/db.ex`) already has a `select`-based `get_worker_infos_by_parent/2` but `get_worker_pids_by_parent/2`, `get_workers_by_parent/2`, and count paths still use `match_object` full scans. `Looper` (`lib/supervisor/looper.ex` `:broadcast_to_group`) discards `Group.broadcast/2` errors and always replies `{:ok, :sent}`. `spawn_worker/2` inserts the info row before spawn and maps all failures to bare `:spawn_failed`, leaving orphans. Partitions serialize Group calls, so no new locking is needed — single-lookup refactors are safe.

## Goals / Non-Goals

**Goals:**
- One ETS read per worker mutation path; count-only queries use `select_count`; broadcast/kill-all use pid/id-only selects.
- Single error vocabulary via existing `ErrorHandler`; spawn failures carry cause; broadcast errors reach API callers.
- No orphan rows on spawn/restart failure; both ref-row and info-row cleaned together.
- Full verification matrix stays green plus a large-group benchmark budget.

**Non-Goals:**
- No change to partition hashing, restart strategy semantics, chain/standalone paths, or public success shapes (`{:ok, id}`, `:ok`).
- No new dependencies, no telemetry events, no graceful-shutdown work (existing TODO in `Looper` untouched).
- No ETS schema migration; table layout unchanged.

## Decisions

### 1. Single-lookup worker paths in `Group`
Collapse `worker_exists?` + `get_worker` + `get_worker_by_id` into one `Db.get_worker_info` / `get_worker_by_id` read per operation, threading the fetched `Worker` + `{ref, pid}` through kill/delete/spawn. `remove_worker/2` becomes: one info read → one ref read → `Process.exit` → delete both rows.
- *Alternative considered:* keep guard-style `worker_exists?` pre-checks for readability. Rejected: doubles ETS traffic on the hottest paths and introduces TOCTOU between check and act.
- *Alternative considered:* `Task.async_stream` for `kill_all_workers`. Rejected for now: partition-loop sequential kill is simpler and ordered; parallelism helps only past ~1k workers and complicates error ordering. Keep sequential but pid-only.

### 2. ETS read-shape fixes in `Db`
Add `count_workers_by_parent/2` via `:ets.select_count/2` with the existing match-spec pattern; add `get_worker_pids_by_parent` select variant returning `{worker_id, pid}` without materializing `Worker` structs; keep `get_worker_infos_by_parent/2` for paths that truly need structs (restart-all).
- *Alternative:* `:ets.match_object` + `length`. Rejected: copies full tuples into the caller heap; `select_count` never materializes.
- *Alternative:* cache counts in partition state. Rejected: invalidation across partitions adds coherence bugs for marginal gain.

### 3. Unified errors via `ErrorHandler`
Map all Group failures to `ErrorHandler.not_found(:worker/:group)`, `already_exists(:worker)`, `invalid_options(detail)`, and `{:error, {:spawn_failed, cause}}`. `check_options/1` adds explicit nil-`id` rejection and converts strategy errors from bare strings. `kill_worker` by-struct vs by-id converge on `:worker_not_found` for missing rows and `{:error, {:not_alive, ...}}`→ normalized to `:worker_not_found`-family per spec (no bare `:not_alive` leak). Keep Supervisor-level API success shapes unchanged; only error atoms tighten.
- *Alternative:* preserve every legacy atom for compat. Rejected: spec requires a matchable contract; legacy shapes (`:worker_exists`, `:cannot_send`, bare strings) are exactly what callers cannot handle. BREAKING is documented in proposal.
- *Alternative:* new `Group.Error` module. Rejected: `ErrorHandler` already owns this vocabulary; reuse it.

### 4. Spawn atomicity and link safety
Insert info row only after successful `do_spawn_worker`, or delete both rows in the `catch` arms before returning. Preserve cause (`{:spawn_failed, {kind, reason}}`) for logs while spec-visible shape stays `{:error, {:spawn_failed, _}}`. Keep `spawn_monitor` + `Process.link` pair (partition traps exits, needs both DOWN for restart and link for shutdown kill); on link failure, kill the child and clean rows before throwing.
- *Alternative:* insert-then-compensate only on failure. Equivalent; chosen order (insert-after-success where possible) minimizes compensation windows. Where monitor ref must exist before link, compensate explicitly.

### 5. `Looper` broadcast propagation + name-registration race
`process_public_api_message` for `:broadcast_to_group` forwards `Group.broadcast/2`'s `{:error, errors}` instead of hard-coding `{:ok, :sent}`. `register_worker_name/1` switches to `try: Process.register` + rescue `ArgumentError` (atomic) rather than `whereis?` → `register` check-then-act.
- *Alternative:* leave Looper mapping as-is. Rejected: violates spec scenario "Broadcast failure is visible to API caller".

### 6. `to_struct/1` simplification
Replace `Enum.reduce` over `Map.from_struct` keys with `struct(Group, Map.take(options, Map.keys(%Group{id: nil})))`. Same semantics, one pass, no per-field `has_key?`.

## Risks / Trade-offs

- [Risk] Tightened error atoms break direct `Group` callers matching legacy shapes → Mitigation: BREAKING noted in proposal; Supervisor API success paths unchanged; tasks include grep-and-update of in-repo callers + CHANGELOG note.
- [Risk] `select_count` match-spec typo silently counts wrong subset → Mitigation: unit test count vs `get_all_workers` length on 0/1/N groups before/after; keep existing `get_worker_infos_by_parent` spec as oracle in tests.
- [Risk] Insert-after-spawn reorders monitor-ref persistence → Mitigation: `Db.put_worker` still happens inside `do_spawn_worker` before link (ref needed for DOWN handling); compensation delete covers failure arms; restart `:not_alive` path deletes both row kinds.
- [Risk] Broadcast now surfaces errors callers previously ignored → Mitigation: spec-mandated; Looper callers already handle `{:error, _}` for other APIs; add explicit test for partial-failure broadcast.
- [Risk] Large-group benchmark flaky on CI → Mitigation: budget asserted in `bench_quick` run locally, not as a hard ExUnit timeout; ExUnit asserts correctness, bench asserts budget with generous margin.

## Migration Plan

- No data migration (ETS runtime-only). Deploy: ship Group + Db + Looper changes together (they are one atomic behavior change for broadcast errors). Rollback: revert the three files together; old callers accepting both old and new atoms during transition should match on `{:error, _}` families, not exact atoms.
- Docs: update `Supervisor` Group API `@doc`/`@spec` error mentions and `CHANGELOG.md` BREAKING note.

## Open Questions

- None blocking. Benchmark budget numbers (exact ms for 1000-worker count/broadcast/kill-all) will be filled from `bench_quick` runs during implementation and recorded in tasks evidence, without changing specs or approach.
