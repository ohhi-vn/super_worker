# Design

## Context

See proposal.md Why. Current state (observed): `README.md` (254 lines) mixes landing, tutorial, and reference roles with typos and a duplicated "Guide"/"Guides" section; five guides total ~1,480 lines with inconsistent structure, duplicated fault-tolerance/restart/Pool-options content, stale terms ("main process", "partition technic"), unexplained `link: false` in every example, and a broken relative link (`SUPERVISOR_GUIDE.md` → `CONFIG_GUIDE.md`, should be `../config/CONFIG_GUIDE.md`). `mix.exs` wires `guides/**/*.md` + `README.md` into ex_doc via `extras()` with per-area groups — file renames would change doc URLs, so paths stay fixed. `doc/` is generated output, not hand-edited. No behavior changes; `skip_specs: true`.

## Goals / Non-Goals

**Goals:**
- One predictable skeleton across all guides so readers can scan any guide the same way.
- README becomes a <120-line landing page with a chooser and reading map.
- Zero lost technical detail after dedupe; every removed duplication becomes a cross-link.
- All relative links valid from both GitHub and ex_doc rendering.

**Non-Goals:**
- No API, config-format, or runtime changes; no new guides or file renames.
- No rewrite of moduledocs or generated `doc/` output by hand.
- No Hex packaging or `mix.exs` docs-config changes unless a link fix requires it.

## Decisions

1. **Keep all six file paths; restructure in place.**
   Rationale: `mix.exs extras()` globs `guides/**/*.md` and the Hex package ships `guides/`; renames would break doc URLs, Hex file lists, and existing external links. Restructure content within fixed paths instead.
   Alternative considered: consolidate into fewer files — rejected, would churn URLs and review scope.

2. **Single guide skeleton: Overview → When to use / not → Quick start → Core concepts → Reference (options/tables) → Error handling → Introspection → Next steps.**
   Rationale: observed guides each order these differently (e.g. Pool buries timeouts mid-file; Supervisor mixes chain edge-cases into intro bullets). One order lets the chooser and "Next steps" cross-links work uniformly.
   Alternative: per-guide bespoke structure — rejected, preserves the current scan problem.

3. **Single-source shared concepts; cross-link, don't copy.**
   - Fault-tolerance/partition diagram + prose live in Supervisor Guide only; Pool Guide links to it and keeps one Pool-specific paragraph.
   - Restart-strategy definitions live in Supervisor Guide (process) vs FunctionChain Guide (step policy); Config Guide links to both instead of restating.
   - Pool `start_link/1` full option list lives in Pool Guide only; README keeps a 3-line minimal example.
   Rationale: current copies have already drifted (e.g. "master" vs "main process"). Single source prevents future drift.
   Alternative: full duplication per guide for standalone reading — rejected, caused the current drift.

4. **Chooser + reading map in README, repeated as one paragraph per guide intro.**
   Rationale: the most common confusion observed is Supervisor-chain vs FunctionChain vs Pool. A 3-row table (durable processes / in-memory jobs / in-process pipeline) at the top of the funnel resolves it early.
   Alternative: chooser only in README — rejected, deep-linked readers miss it.

5. **Config Quickstart vs Guide split by contract: quickstart shows only the 5-minute path + 3 patterns + troubleshooting pointers; all format reference, validation rules, and full troubleshooting move to the Guide.**
   Rationale: the two files currently overlap ~60% (same config snippet, same options). The split makes each file's job obvious.
   Alternative: merge into one file — rejected, loses the quick on-ramp and changes doc URLs.

6. **Terminology and example conventions fixed once and applied mechanically.**
   Master/partition/ETS wording from Supervisor Guide wins; `alias SuperWorker.Supervisor, as: Sup` introduced per example; `link: false` explained once in Supervisor Quick start and omitted elsewhere unless relevant; consistent `:sup1` / `MyPool` names.
   Rationale: removes the largest readability tax with the smallest review burden.

## Risks / Trade-offs

- [Risk] Dedupe accidentally drops a nuance (e.g. Pool front-of-queue retry vs chain queue_full exit) → Mitigation: per-guide content-preservation checklist in tasks; reviewer diffs old vs new section by section.
- [Risk] Relative links valid on GitHub but broken under ex_doc (or vice versa) → Mitigation: use `../pool/POOL_GUIDE.md`-style repo-relative links (current convention) and verify with `mix docs` build + click-through of every cross-link.
- [Risk] README cut too far, losing keywords new users search for → Mitigation: keep all six file links, feature names, and the minimal Pool/Supervisor/Chain snippets in README; move only option tables and deep prose.
- [Risk] Style drift returns after this change → Mitigation: short contributor note (skeleton + terms) at top of tasks verification; no new tooling.

## Migration Plan

Docs-only, no deployment. Apply edits to `README.md` + `guides/**/*.md`, run `mix docs` to confirm no ex_doc warnings and links resolve, `git diff --stat` review per file. Rollback is `git revert`. No migration needed.

## Open Questions

None. File set, skeleton, and dedupe ownership are fixed above; remaining wording choices are editorial and safe to resolve during implementation.
