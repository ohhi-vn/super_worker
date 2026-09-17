# Design: normalize-function-chain-results

## Context

`SuperWorker.FunctionChain.Executor.run/3` can return `{:error, reason, meta}` when
`return_context: true` is set, but both FunctionChain adapters present failures to
callers as plain `{:error, reason}`. That conversion was previously duplicated:
an inline 3-tuple clause in `SuperWorker.Pool.FunctionChain.handle_job/2`, a private
`normalize/1` plus two re-inlined clauses in `SuperWorker.Supervisor.FunctionChain`.
See proposal.md for motivation and specs/function-chain/spec.md for the behavior
contract.

## Goals / Non-Goals

**Goals:**

- One shared, documented normalization function owned by the layer that produces
  the 3-tuple shape (the Executor).
- Behavior-preserving consolidation: identical outputs for every result shape
  `Executor.run/3` can return.

**Non-Goals:**

- No changes to adapter public APIs, message protocols, retry policy, or the
  `return_context` option itself.
- No unification of the two circuit breakers (they have different process models
  and were assessed as deliberate designs, not duplication).
- No remediation of repository-wide Credo/Dialyzer findings or inclusion of the
  separately requested chain acknowledgment fix in this refactor's scope.

## Decisions

- **Location: `Executor.drop_meta/1`** — the 3-tuple shape is an `Executor.run/3`
  output contract, so the inverse transform belongs with its producer. A private
  helper per adapter was the alternative; it would keep three copies of one
  intent (the original problem).
- **Identity for non-matching shapes** — `{:ok, _}` and plain `{:error, _}` pass
  through unchanged, mirroring each adapter's existing `other ->` fallthrough, so
  a single call site per run replaces multi-clause case matching.
- **Call placement: normalize before dispatch** — each adapter applies
  `Executor.drop_meta/1` to the run result immediately, then keeps its own
  success/error handling (pool: failure classification incl. `:on_error: :retry`;
  supervisor: `{:next, _}` forwarding, envelope replies, `:on_result` callbacks).
  Normalizing in the Executor's `run/3` itself was rejected: it would hide the
  meta from hosts that want it (`return_context: true` exists for that).
- **Test coverage** — direct unit tests for `drop_meta/1` (all three shapes plus
  identity cases) in `test/function_chain/executor_test.exs`, plus the existing
  adapter suites covering both `return_context` modes.

## Risks / Trade-offs

- [New public API surface on Executor] → Small, documented, consistent with its
  "shared plumbing" moduledoc role; no existing name collisions.
- [Normalization could change nested payloads or adapter policies] -> The helper
  matches only an outer three-element error tuple. Direct tests verify identity
  for other shapes; adapters retain their own dispatch and retry handling.
- [Quality gates remain red] -> The latest full suite passes 692 checks,
  including the separately requested chain acknowledgment regression test.
  Formatting and `git diff --check` pass. Credo reports one naming issue and
  Dialyzer reports 40 findings (39 before the acknowledgment fix). The additional
  finding exposes the existing message type mismatch; task 3.1 remains unchecked.

## Migration Plan

No data migration or configuration change is required. Deploy the shared helper
and both adapter updates together. Roll back their changes together to restore
inline normalization without changing result contracts. Keep the separate chain
acknowledgment fix independent of this refactor's rollback.
