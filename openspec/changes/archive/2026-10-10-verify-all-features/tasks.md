# Tasks

## 1. Surface inventory

- [x] 1.1 Map Supervisor public API (`lib/supervisor/supervisor.ex`, group/chain/standalone, partitions, `running_supervisors`, `supervisor_info`, `list_groups/chains/standalone_workers`, `stop/3`) to guides and test files, and verify the map lists every public function with its test file.
- [x] 1.2 Map Pool public API (`lib/pool/pool.ex` `start_link/run/run_async/await/cast/stop/info`, `Worker` behaviour, `Middleware`, `Backoff`, routing/backpressure/retry/`on_failure`/circuit-breaker semantics) to guides and test files, and verify no public function lacks a map row.
- [x] 1.3 Map FunctionChain core (`lib/function_chain.ex`, steps/branches/parallel/checkpoint/resume/tracer/store) plus integrations (`lib/pool/function_chain.ex`, `lib/supervisor/function_chain.ex`) and `openspec/specs/function-chain/spec.md` scenarios to guides and test files, and verify every spec scenario has a covering test pointer.
- [x] 1.4 Map utilities and config (`Supervisor.CircuitBreaker`, `TermStorage`, `Utils.safe_call`, `ConfigLoader.{Parser,ConfigParser,Bootstrap}`, `Supervisor.{Db,Validator,Looper}`) to guides and test files, and verify the inventory is committed as the triage reference.

## 2. Baseline verification battery

- [x] 2.1 Run `mix test --seed 0` capturing full output to a file and verify the recorded result line (passed/failed counts) matches the file.
- [x] 2.2 Run `mix test --seed 12345` and one default-seed `mix test`, capturing outputs, and verify all three seed results are recorded with any seed-sensitive failures named by file:line.
- [x] 2.3 Run `mix format --check-formatted` and `mix credo --strict` and verify both exit clean or every reported issue is recorded with file:line (known: one `NotResumableError` naming flag).
- [x] 2.4 Run `mix dialyzer` once (allowing PLT build) and `mix test --cover` for coverage, and verify warnings and uncovered public functions are recorded or dialyzer skip is documented with reason.

## 3. Supervisor surface verification

- [x] 3.1 Exercise group/chain/standalone lifecycle, restart strategies, partition crash isolation, and `stop/3` paths against `guides/supervisor/SUPERVISOR_GUIDE.md` and moduledocs, and verify each documented result shape and error atom is confirmed or logged as drift.
- [x] 3.2 Exercise introspection (`running_supervisors`, `supervisor_info`, `list_groups/chains/standalone_workers`) and messaging/broadcast APIs, and verify counts, liveness fields, and reply shapes match docs or drift is recorded.
- [x] 3.3 Repair Supervisor-scoped wrong/flaky tests and doc drift found in 3.1–3.2, and verify the touched test files pass via `mix test <file>` and doc edits match observed behavior.

## 4. Pool surface verification

- [x] 4.1 Exercise `run/run_async/await/cast`, routing (`round-robin`/`hash`), `max_queue` `:overloaded`, retries/backoff/jitter, crash requeue-once semantics, `on_failure` dead-letter, and `info/1` against `guides/pool/POOL_GUIDE.md` and `Pool` moduledoc, and verify each behavior or drift is recorded.
- [x] 4.2 Exercise `Pool.Middleware` pipeline plus `Telemetry` events and `CircuitBreaker` trip/reject (`:circuit_open` vs `on_failure` boundary), and verify event names and rejection shapes match docs or drift is recorded.
- [x] 4.3 Repair Pool-scoped wrong/flaky tests and doc drift found in 4.1–4.2, and verify the touched test files pass via `mix test <file>` and doc edits match observed behavior.

## 5. FunctionChain and integrations verification

- [x] 5.1 Exercise FunctionChain core (linear/branch/parallel steps, retry, checkpoint/resume, tracer, ETS store) against `guides/function_chain/FUNCTION_CHAIN_GUIDE.md`, and verify each operator’s success/error shapes or drift is recorded.
- [x] 5.2 Exercise `openspec/specs/function-chain/spec.md` scenarios (failure-metadata stripping across pool/supervisor/worker-loop adapters, success-contract preservation) plus `Pool.FunctionChain` (`run_opts`, `on_error` `:error`/`:retry`) and `Supervisor.FunctionChain` (`chain_node_fun/2`, `job_loop/2` envelope/`on_result`), and verify every scenario passes or is triaged.
- [x] 5.3 Repair FunctionChain-scoped wrong/flaky tests and doc drift found in 5.1–5.2, and verify the touched test files pass via `mix test <file>` and doc edits match observed behavior.

## 6. Utilities and config verification

- [x] 6.1 Exercise `CircuitBreaker.call/2` (closed/open/half-open, `half_open_max_calls`), `TermStorage` (`nil` vs missing, `get_all/0` shape), and `Utils.safe_call/1,3` against README/moduledocs, and verify each shape or drift is recorded.
- [x] 6.2 Exercise `ConfigLoader` parsing/bootstrap (`guides/config/CONFIG_GUIDE.md`, `CONFIG_QUICKSTART.md`, `start_with_config/1`, pid `:link` acceptance) with the example configs in `config/`, and verify declared trees start and invalid options fail as documented or drift is recorded.
- [x] 6.3 Repair utility/config-scoped wrong/flaky tests and doc drift found in 6.1–6.2, and verify the touched test files pass via `mix test <file>` and doc edits match observed behavior.

## 7. Triage and root-cause fixes

- [x] 7.1 Label every recorded anomaly with exactly one of flaky-test / wrong-test / doc-drift / real-bug per design §Decisions-4, and verify the triage list has no unlabeled failure.
- [x] 7.2 Fix each real bug at its owning layer with a minimal behavior-preserving correction plus a regression test in the owning test file, and verify each fix passes via `mix test <regression-file>:<line>` and the failure no longer reproduces.
- [x] 7.3 Re-run flaky suspects in isolation (`mix test <file> --seed <failing-seed>`) and quarantine or repair timing-dependent assertions, and verify each suspect passes on three consecutive isolated runs.

## 8. Final gates and report

- [x] 8.1 Run the full battery once more (`mix test` default seed, `mix format --check-formatted`, `mix credo --strict`) and verify all three exit clean with outputs saved.
- [x] 8.2 Write the verification report (inventory link, per-seed results, triage table, fixes with file:line, residual risks) and verify it names every fix commit and every unresolved warning.
