# Proposal

## Why

`SuperWorker.Pool` is the primary public job API (`run` / `run_async` / `cast`), and each partition is a single GenServer that owns the queue, dispatch, retries, and crash requeues. Code inspection shows linear work on the hot path (idle-list append, `List.delete` on worker DOWN, `Enum.find` over the busy map to locate a crashed worker's job), a per-submission breaker round-trip, unvalidated `circuit_breaker:` options, and lossy error edges (async `await` timeout drops the job, overloaded casts log-and-drop, callback crashes only log). This change bounds the hot path and makes overload, retry, crash, and callback-failure behavior explicit and observable.

## What Changes

- Bound per-job partition work: O(1) overload check (keep cached `queue_len`), O(1) crash-job lookup (reverse worker→job index instead of scanning `busy`), and constant-time idle bookkeeping on register / release / DOWN.
- Keep submission fast: single enqueue path with early `:overloaded` / `:circuit_open` rejection before queueing; no extra blocking hops on the accept path beyond the existing partition call.
- Validate `circuit_breaker:` options (`failure_threshold`, `reset_timeout`) at `start_link` with actionable `{:error, {:invalid, ...}}` shapes, matching existing option validation.
- Stabilize error contracts (no new public API): `run` / `await` keep returning `{:error, reason}` with `reason` in `:overloaded`, `:timeout`, `{:retries_exhausted, reason}`, `{:worker_crashed, reason}`, `:circuit_open`, `:pool_not_found`, `:pool_unavailable`; poison-pill jobs dead-letter after one crash requeue instead of crash-looping; `on_failure` stays the single source of truth for jobs that ran and finally failed; `on_result` keeps covering both cast outcomes.
- Make failures observable without breaking the pool: keep fail-open breaker checks and swallow-and-log middleware/callback crashes, but emit telemetry for overload, retry, crash, dead-letter, and circuit open/close so drops are measurable.
- Document the in-memory durability boundary (queued jobs survive worker/partition restarts, not node crashes) in behavior terms so callers know when to put a persisted layer behind `:on_failure`.

## Capabilities

### New Capabilities

- `pool`: observable submission, queueing/backpressure, dispatch, retry/backoff, crash-requeue, dead-letter, circuit-breaker, validation, and scale contracts for the partitioned process pool.

### Modified Capabilities

- None. No existing spec covers the Pool (inventory: `function-chain`, `supervisor-chain`, `supervisor-group`, `supervisor-standalone`); supervisor and function-chain behaviors are untouched.

## Impact

- Affected code: `lib/pool/` (`pool.ex`, `partition.ex`, `partition_supervisor.ex`, `worker_proc.ex`, `backoff.ex`, `middleware.ex`, `middleware/circuit_breaker.ex`), pool moduledoc behavior docs.
- Tests: `test/pool/` (api, overload, retry, crash, middleware, coverage) plus benchmarks in `benchmarks/` for submit/dispatch scale.
- No public API changes and no new dependencies; `run` / `run_async` / `await` / `cast` / `info` / `stop` shapes stay stable.
