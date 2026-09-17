# Proposal: normalize-function-chain-results

## Why

The `{:error, reason, meta} -> {:error, reason}` metadata-stripping transform (needed when `Executor.run/3` is called with `return_context: true` but callers expect the plain 2-tuple contract) is hand-inlined in three places across the two FunctionChain adapters, each with its own private comment/normalizer. Duplicated intent with divergent structure invites drift.

## What Changes

- Add a public normalization helper to `SuperWorker.FunctionChain.Executor` (e.g. `drop_meta/1`): maps `{:error, reason, _meta}` to `{:error, reason}`, identity for any other shape.
- `lib/pool/function_chain.ex`: replace the inline 3-tuple clause in `handle_job/2` (lines ~80-86) with a call to the shared helper before failure classification.
- `lib/supervisor/function_chain.ex`: replace the private `normalize/1` and the two re-inlined 3-tuple clauses (`do_job_loop` reply branch line ~107, `chain_node_run` line ~156) with the shared helper.
- No behavior change: all three sites already discard the meta element unconditionally; identical results for every shape `Executor.run/3` can return.

## Capabilities

### New Capabilities

- `function-chain`: result contracts SuperWorker adapters expose when running FunctionChain jobs (plain two-tuple errors regardless of chain run options).

### Modified Capabilities

(none)

## Impact

- `lib/function_chain/executor.ex` — small public function added (consistent with its role as shared plumbing).
- `lib/pool/function_chain.ex`, `lib/supervisor/function_chain.ex` — internal call sites consolidated; public adapter APIs unchanged.
- Covered by existing adapter tests exercising both `return_context` on/off paths (`test/pool/function_chain_test.exs`, `test/workload_test/function_chain_test.exs`); no new tests strictly required, though a direct `Executor.drop_meta/1` test may be added for clarity.
