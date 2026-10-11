# Design

## Context

See proposal.md — Why. Current state observed in repo:

- `lib/` has 56 `.ex` files. Largest surfaces: `Supervisor.supervisor.ex` (~1364 lines, group/chain/standalone + partitions + introspection), `FunctionChain.function_chain.ex` (~1199 lines, steps/branches/parallel/checkpoint/tracer/store), `Pool.pool.ex` (~614 lines) plus `Pool.{Partition,Server,Worker,Middleware,Backoff}` and integrations `Pool.FunctionChain` / `Supervisor.FunctionChain`.
- Supporting surfaces: `Supervisor.CircuitBreaker`, `TermStorage`, `Utils.safe_call`, `ConfigLoader.{Parser,ConfigParser,Bootstrap}`, `Supervisor.{Db,Validator,Looper,Group,Chain.Worker}`.
- `test/` has 45 `.exs` files (~654 tests + 25 doctests + 13 properties). Full `mix test` costs ~55s. Observed: one run 690/692, rerun 692/692 same machine — seed/order sensitivity is uncharacterized. Error-level logs are emitted by passing tests, masking real failures.
- Static gates: `mix format --check-formatted` clean, `mix credo --strict` reports 1 consistency issue (`NotResumableError` naming), dialyzer/coveralls state unknown.
- Contracts: `openspec/specs/function-chain/spec.md` (failure-metadata normalization) is the only formal spec; guides + moduledocs are the de facto oracle and may have drifted (v0.10.0 FunctionChain integration is newest).

Constraint: verification must not redesign APIs or add features; any code edit must be a root-cause, behavior-preserving correction back toward the documented contract.

## Goals / Non-Goals

**Goals:**

- Produce a complete surface inventory (public function → implementation → guide → test) so “all features verified” is auditable, not a claim.
- Establish a repeatable battery (test seeds, format, credo, dialyzer, coverage) with per-run evidence.
- Triage every anomaly into exactly one of: flaky test, wrong test, doc drift, real bug — then fix at root cause.
- Leave the repo green on the battery with a written residual-risk list.

**Non-Goals:**

- No new features, API redesigns, performance tuning, or dependency upgrades (unless a verification step is blocked without one).
- No broad refactors to “make verification easier”; verification adapts to current architecture.
- No rewriting guides as tutorials; doc edits are corrections to match verified behavior.

## Decisions

### 1. Inventory-first, then battery, then triage-fix (over fix-as-you-go)

Build the surface map before running the battery so failures can be attributed to a surface immediately. Alternative — run tests first and inventory later — was rejected because unattributed failures in a 692-test suite with noisy logs cause duplicate investigation.

### 2. Oracles ranked: formal spec > moduledoc/guide > test (over tests-are-truth)

When code, docs, and tests disagree: `openspec/specs/function-chain/spec.md` wins where it applies; otherwise the moduledoc/guide promise wins unless it contradicts observed OTP constraints (then code + doc fix together). Tests are the weakest oracle because the observed seed variance suggests some tests encode timing assumptions. Rationale: specs describe promised behavior; tests in this repo already show flakiness.

### 3. Battery: fixed seeds + default seed, three full runs minimum (over single run)

Run `mix test --seed 0`, one fixed non-zero seed, and one default-seed run; record pass/fail per seed. Alternative — single default run — was rejected because the 690→692 swing would have been missed. Cost is ~3×55s, acceptable for a one-time sweep.

### 4. Failure taxonomy enforced at triage (over ad-hoc fixes)

Every failure gets one label before any edit: flaky (passes on retry/seed change, timing-dependent), wrong test (asserts undocumented behavior), doc drift (implementation matches spec, docs don’t), real bug (implementation violates spec/docs). Real bugs are fixed at root cause in the owning layer (`Supervisor.*`, `Pool.*`, `FunctionChain.*`) per repo layering, not with guards at call sites. Rationale: prevents misplaced patches and duplicated guards.

### 5. Static gates treated as verification signals, not blockers to bypass

`format --check-formatted` and `credo --strict` must pass; dialyzer runs once (PLT build allowed) and any unresolved warning becomes residual risk with reason. Coverage reviewed for untested public functions, not chased to 100%. Alternative — skipping dialyzer for speed — rejected because partitioned-supervisor and chain-resume paths have typespecs worth checking once.

### 6. Log-noise reduction is diagnostic hygiene, not behavior change

Passing tests emitting `[error]` logs stay passing, but repeated noise signatures (e.g., worker-not-found during chain tests) are catalogued so real errors remain visible. No log-level changes to production code unless a log call is proven wrong (wrong level leaks into triage as real bug).

## Risks / Trade-offs

- [Risk] Full suite is slow (~55s) and seed-sensitive → Mitigation: run targeted files first during triage (`mix test path:line`), reserve full runs for entry/exit evidence.
- [Risk] Noisy error logs hide real failures → Mitigation: capture full output to file per run, grep `Failure|failed|error` signatures against the inventory rather than eyeballing tail output.
- [Risk] Fixing a “real bug” changes behavior someone depends on → Mitigation: only fix toward the documented contract; any fix that would alter a promised result shape requires stopping and asking before proceeding.
- [Risk] Dialyzer PLT build is slow/fragile on first run → Mitigation: time-box; on failure record as residual risk instead of blocking verification.
- [Risk] Guides are large (4 guide areas); exhaustive doc-vs-code diff could sprawl → Mitigation: audit public-function level (signature + result shapes + error atoms like `:overloaded`, `:circuit_open`, retry/backoff semantics), not prose style.

## Migration Plan

No deployment or data migration. Landing strategy for fixes found during apply:

1. Land test/doc-only corrections first (no runtime risk).
2. Land each root-cause bug fix separately with its regression test; keep diffs minimal and in the owning module.
3. Rollback for any fix is `git revert` of that fix commit; verification report records which commit maps to which triaged failure so revert is selective.

## Open Questions

None — seed choices, oracle ranking, and triage taxonomy above fully determine the task breakdown.
