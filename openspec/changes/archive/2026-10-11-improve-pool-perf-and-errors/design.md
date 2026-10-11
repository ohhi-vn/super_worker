# Design

## Context

See proposal.md (Why) for motivation. Current state (observed in `lib/pool/`): each partition is one GenServer owning a FIFO queue, `idle`/`busy` bookkeeping, retry timers, and crash requeues. The overload check already uses a cached `queue_len` (O(1)); the remaining hot-path costs are idle-list append (`idle ++ [pid]`), `List.delete` on DOWN, and `Enum.find` over `busy` to locate a crashed worker's job. Submissions do one partition call plus, when middleware is configured, one breaker `check_enqueue` call. `circuit_breaker:` options are unvalidated. Error edges are documented in the `SuperWorker.Pool` moduledoc (one crash requeue, dead-letter via `:on_failure`, fail-open breaker) but have no spec owning them.

## Goals / Non-Goals

**Goals:**
- Bound per-job partition work so submit, dispatch, and crash handling do not grow with backlog or worker count.
- Validate all pool options (including breaker thresholds) at start with actionable errors.
- Keep every drop measurable (telemetry + logs) without letting middleware or callback failures break the pool.

**Non-Goals:**
- No public API changes (`run` / `run_async` / `await` / `cast` / `info` / `stop` shapes unchanged).
- No durable queue: in-memory semantics stay (survives worker/partition restarts, not node crashes).
- No new dependencies, no partition-model redesign, no ordering-model change (per-partition FIFO + hash affinity stay).

## Decisions

### 1. Reverse worker→job index for crash lookup

Keep `busy` (`job_id → worker_pid`) as the source of truth and add a `busy_by_worker` (`worker_pid → job_id`) index maintained on dispatch, release, and completion. Crash handling then resolves the in-flight job in O(1).

- Alternative considered: keep `Enum.find` over `busy`. Rejected: O(n) per crash turns a crash storm into a partition stall; the index is two map writes on an already-O(1) path.

### 2. Constant-time idle bookkeeping

Register workers by prepending and track membership via the existing `monitors` map (already O(1) keyed by pid); remove by swap-with-last or map-backed set instead of `++` append and `List.delete` scans. Dispatch order stays FIFO on the job queue; worker selection order is unspecified and stays so.

- Alternative considered: leave `idle ++ [pid]` / `List.delete`. Rejected: O(n) per register/DOWN on a path that runs per job and per crash.

### 3. Keep the cached queue length, keep early rejection order

Retain `queue_len` for the O(1) `:overloaded` check and keep the check order: breaker `check_enqueue` → overload gate → enqueue. No extra blocking hops on accept.

- Alternative considered: moving overload state to ETS for lock-free reads. Rejected: submissions already serialize through the partition call; ETS adds consistency questions for zero measurable gain.

### 4. Validate breaker options at start_link

Validate `failure_threshold` (positive integer) and `reset_timeout` (positive integer) in `Pool.validate/1` alongside existing option checks, returning the same `{:error, {:invalid, {key, value}}}` shapes.

- Alternative considered: lazy validation on first use. Rejected: misconfiguration would surface mid-traffic as silent fail-open instead of a start-time error.

### 5. Keep fail-open isolation, add telemetry for every drop

Keep current isolation (breaker-down fails open, middleware `check_enqueue`/`notify` crashes are logged and swallowed, `:on_failure`/`:on_result` crashes are logged) and emit telemetry for overload, retry, crash, dead-letter, circuit open/close, and callback failure so no drop is silent.

- Alternative considered: crashing the partition on callback/middleware failure to "fail loud". Rejected: user code must never take down the pool; measured drops plus logs give operators the signal without the blast radius.

### 6. Keep one crash requeue (poison-pill bound)

No change to the retry/crash budget interaction: one crash requeue per job charged against `max_attempts`, then dead-letter. This is the existing moduledoc contract; the spec now owns it so regressions are caught.

### 7. Supervisor shutdown and registration races requeue uncharged

A `:shutdown` / `{:shutdown, _}` exit is supervisor-initiated (a `rest_for_one` cascade after a sibling crashed, or pool shutdown), never a job failure. A `:noproc` DOWN means the worker died between its ready liveness check and its monitor, so it never ran while tracked — a dispatch in the same handler may still have handed it a job whose send was lost. Both requeue at the queue front without incrementing `attempts` or setting `crash_requeued`, so a healthy job is never dead-lettered as a poison pill. A stale ready from an already-dead worker is dropped outright (the replacement re-registers itself), keeping dead pids out of the idle set. Crash telemetry still fires for observability.

- Alternative considered: charging shutdowns like crashes. Rejected: the cascade deterministically burns the one crash requeue of an innocent job and dead-letters it on the next shutdown — observed as `{:error, {:worker_crashed, :shutdown}}` with two workers on one partition. Keeping `rest_for_one` (it is load-bearing for partition-crash recovery) plus uncharged shutdown requeues contains the blast radius without a supervision redesign.

## Risks / Trade-offs

- [Risk] Index and idle-set drift from `busy`/`idle` on an unhandled path → stale entries leak memory or misroute. Mitigation: update both structures in the same state transition (`dispatch`, `release_worker`, `complete`, `handle_worker_down`); add a coverage test asserting index consistency after crash + retry + completion sequences.
- [Risk] Breaker validation rejects configs that previously "worked" (silently ignored). Mitigation: start-time error is intentional and actionable; no valid config changes shape.
- [Risk] Extra telemetry events add per-job overhead. Mitigation: `:telemetry.execute` is a no-op without handlers; keep payloads to small maps already built for logging.
- [Risk] Benchmarks on CI hardware are noisy. Mitigation: assert budget ratios (bounded vs. backlog size) rather than absolute times, plus a regression test with a large backlog asserting prompt overload rejection.

## Migration Plan

Internal-only change; no config migration. Deploy by releasing the new version; callers keep the same options and result shapes. Rollback by reverting to the previous version — no persisted state to migrate (in-memory queues only).
