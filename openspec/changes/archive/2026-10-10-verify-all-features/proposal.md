# Proposal

## Why

SuperWorker v0.10.0 ships three major surfaces (partitioned Supervisor with group/chain/standalone workers, partitioned Pool, FunctionChain + integrations) with ~692 tests but no recent end-to-end correctness sweep. A full run shows 690/692 passing on one seed and 692/692 on another, plus noisy error logs and one Credo consistency flag — so correctness, flakiness, and docs-vs-behavior drift are unverified. A single systematic verification pass is needed before further feature work.

## What Changes

- Inventory every public feature/function surface (Supervisor APIs, Pool APIs, FunctionChain core + `Pool.FunctionChain` + `Supervisor.FunctionChain`, CircuitBreaker, TermStorage, ConfigLoader, introspection/observability) and map each to its implementation, guides, and tests.
- Run the full verification battery: `mix test` (multiple seeds), `mix format --check-formatted`, `mix credo --strict`, `mix dialyzer` (or document why skipped), and coverage review.
- Audit guides (`SUPERVISOR_GUIDE`, `POOL_GUIDE`, `FUNCTION_CHAIN_GUIDE`, `CONFIG_GUIDE`/`QUICKSTART`) and moduledocs against observed behavior; record every drift.
- Triage every failure into: flaky test, wrong test, doc drift, or real implementation bug. Fix implementation bugs at root cause with minimal behavior-preserving corrections plus regression tests; fix wrong/flaky tests; update docs where implementation is already correct.
- Deliver a verification report (what was run, pass/fail per seed, fixes applied, residual risks). No new features, no API redesign, no intentional behavior change.

## Capabilities

### New Capabilities

- None — this change introduces no new system behavior.

### Modified Capabilities

- None — this change does not alter any spec-level requirement. Existing `function-chain` spec remains the contract; bug fixes align implementation to it rather than changing it. This change opts out of specs via `skip_specs: true` in `.openspec.yaml`.

## Impact

- Code: read-only audit across `lib/` (56 `.ex` files: supervisor, pool, function_chain, circuit_breaker, term_storage, config_loader); edits limited to root-cause bug fixes and test/doc corrections discovered during verification.
- Tests: `test/` (45 `.exs` files, ~654 tests + 25 doctests + 13 properties, ~55s per full run); may add/repair regression tests.
- Tooling: `mix test`, `mix format`, `mix credo`, `mix dialyzer`, `mix coveralls`-style coverage; no new dependencies.
- Docs: guides and moduledocs corrections only; no public API additions/removals intended.
