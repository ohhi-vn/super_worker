# Tasks: normalize-function-chain-results

## 1. Shared helper

- [x] 1.1 Add public `SuperWorker.FunctionChain.Executor.drop_meta/1` (`{:error, reason, _meta} -> {:error, reason}`, identity otherwise) with @doc; verify with a direct ExUnit case in `test/function_chain/executor_test.exs` covering all three shapes plus 2-tuple identity.

## 2. Adapter consolidation

- [x] 2.1 `lib/pool/function_chain.ex` — replace the inline 3-tuple clause in `handle_job/2` (~lines 80-86) with `Executor.drop_meta/1` before `classify_failure/2`; verify `mix test test/pool/function_chain_test.exs` passes including `return_context: true` cases (~lines 54-56, 109-113).
- [x] 2.2 `lib/supervisor/function_chain.ex` — delete private `normalize/1` (~line 131), replace its uses and the re-inlined clauses in `do_job_loop` reply branch (~line 107) and `chain_node_run` (~line 156) with `Executor.drop_meta/1`; verify `mix test test/workload_test/function_chain_test.exs` passes including `return_context: true` cases (~lines 51-55, 120-133).

## 3. Verification

- [ ] 3.1 Full suite + lint: `mix test` and `mix lint` (or credo if aliased) pass with no new warnings.

Verification status: the latest full suite passed (692 checks), including the separately requested chain acknowledgment regression test; the focused helper and adapter suite passed (19 tests). Formatting and `git diff --check` passed. The project configures `mix quality`, not `mix lint`. Credo reports one consistency issue. Dialyzer reports 40 findings, compared with 39 before the separate acknowledgment fix; the additional finding exposes the existing message type mismatch. No findings were reported in the three normalization modules. Quality checks are not green, so task 3.1 remains incomplete. The acknowledgment fix is separate from this refactor; no repository-wide quality cleanup or waiver has been approved.
