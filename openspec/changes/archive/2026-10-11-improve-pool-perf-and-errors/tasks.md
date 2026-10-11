# Tasks

## 1. Bound partition hot path

- [x] 1.1 Add a worker→job reverse index alongside `busy` in `lib/pool/partition.ex`, maintained on dispatch, release, completion, and crash handling, and verify a new consistency test (crash + retry + completion sequence asserting index matches `busy`) passes via `mix test test/pool/crash_test.exs`
- [x] 1.2 Replace idle-list append/scan bookkeeping with constant-time register/release/DOWN handling keyed by the existing monitors map in `lib/pool/partition.ex`, and verify `mix test test/pool/api_test.exs test/pool/overload_test.exs` passes
- [x] 1.3 Add a backlog-scale regression test asserting overload rejection stays prompt with a large backlog (ratio-based, not absolute timing), and verify it passes via `mix test test/pool/overload_test.exs`

## 2. Validation and error contracts

- [x] 2.1 Validate `circuit_breaker:` `failure_threshold` / `reset_timeout` in `Pool.validate/1` with `{:error, {:invalid, _}}` shapes, and verify new start_link rejection tests plus existing `mix test test/pool/api_test.exs` pass
- [x] 2.2 Cover poison-pill, retry-exhaustion, and crash dead-letter contracts (`on_failure` as single source of truth, `on_result` for both cast outcomes, callback crashes isolated with logs) with tests in `test/pool/`, and verify `mix test test/pool/retry_test.exs test/pool/crash_test.exs test/pool/middleware_test.exs` passes
- [x] 2.3 Cover breaker fail-fast/probe semantics (open rejects without running or firing `on_failure`, probe closes/reopens, breaker-down fails open), and verify `mix test test/pool/middleware_test.exs` passes

## 3. Observability and docs

- [x] 3.1 Emit telemetry for overload, retry, crash, dead-letter, circuit open/close, and callback failure with pool/partition identity, and verify telemetry assertions pass via `mix test test/pool/coverage_test.exs`
- [x] 3.2 Update the `SuperWorker.Pool` moduledoc behavior docs (error shapes, overload/cast-drop, retry/crash, breaker, in-memory durability boundary) to match the new spec, and verify the documented `info` output and error shapes match actual calls in a doctest-style check

## 4. Scale and integration

- [x] 4.1 Run bursty-submit and crash-storm scale checks (concurrent submitters, many crashing workers) against the benchmark budget using `benchmarks/bench_quick.exs`, and verify budgets hold and healthy partitions keep serving jobs
- [x] 4.2 Run the full pool suite and validation, and verify `mix test test/pool` and `openspec validate "improve-pool-perf-and-errors"` pass
