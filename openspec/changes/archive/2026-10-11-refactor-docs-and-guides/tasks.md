# Tasks

## 1. Baseline and conventions

- [x] 1.1 Record current per-file line counts, headings, and all cross-guide links (including the broken `SUPERVISOR_GUIDE.md` → `CONFIG_GUIDE.md` link), and verify the inventory lists all six files (`README.md` + five guides) with the broken link flagged.
- [x] 1.2 Apply the design's terminology and example conventions (master/partition/ETS wording, `alias SuperWorker.Supervisor, as: Sup` per example, `link: false` explained once, consistent `:sup1`/`MyPool` names) across all six files, and verify with `rg` that stale terms ("main process", "partition technic", "communitcate", "three type") no longer appear.

## 2. Supervisor Guide (single-source owner)

- [x] 2.1 Restructure `guides/supervisor/SUPERVISOR_GUIDE.md` to the shared skeleton and split the dense chain "Notes" bullet wall into subsections (entry/routing, queue/backpressure, callbacks, restart strategies, removal), and verify each old subsection's details are present under a heading by diffing old vs new section by section.
- [x] 2.2 Make the Supervisor Guide the canonical home of the fault-tolerance/partition diagram + prose and process restart-strategy definitions, fix the Config Guide link to `../config/CONFIG_GUIDE.md`, and verify every cross-link in the file resolves from the repo root.

## 3. Pool Guide

- [x] 3.1 Restructure `guides/pool/POOL_GUIDE.md` to the shared skeleton, replace its duplicated fault-tolerance prose with a cross-link to the Supervisor Guide keeping one Pool-specific paragraph (front-of-queue retry, poison-pill, `:on_failure`), and verify no Pool-specific nuance was dropped by diffing old vs new.
- [x] 3.2 Repair the error-reason table (including the malformed `cast/2` row) and tighten Architecture vs Overview overlap, and verify the table renders correctly via `mix docs` output inspection.

## 4. FunctionChain Guide

- [x] 4.1 Restructure `guides/function_chain/FUNCTION_CHAIN_GUIDE.md` to the shared skeleton with a one-paragraph chooser (Supervisor-chain vs FunctionChain vs Pool) in the intro, and verify the step-policy (`:halt`/`:retry`/`:ignore`), arg-override, parallel/branch, and checkpoint/resume details are all still present by diffing old vs new.
- [x] 4.2 Make the FunctionChain Guide the canonical home of step-policy definitions referenced by Pool/Supervisor integration sections, and verify both integration sections link back correctly.

## 5. Config Quickstart and Guide split

- [x] 5.1 Trim `guides/config/CONFIG_QUICKSTART.md` to the 5-minute path + 3 patterns + troubleshooting pointers only, and verify it contains no full format-reference tables by comparing against the Guide.
- [x] 5.2 Restructure `guides/config/CONFIG_GUIDE.md` to the shared skeleton as the full format reference (options, validation, troubleshooting), replacing restated restart strategies with links to the Supervisor/FunctionChain guides and removing filler (generic best-practices list, License section, Module Details API stubs), and verify every removed detail is either linked or intentionally dropped in the review diff.

## 6. README landing page

- [x] 6.1 Rewrite `README.md` as a <120-line landing page (what it is, 60-second quickstart, 3-row primitive chooser, reading map + links to all five guides), moving deep Pool/Supervisor reference material into the guides, and verify line count and that all six doc links plus feature keywords remain.
- [x] 6.2 Fix README typos and duplicated "Guide"/"Guides" sections and unify intro wording with the guides' Overview language, and verify by reading the rendered README top to bottom.

## 7. Integration verification

- [x] 7.1 Verify every code snippet in the six files is syntactically valid Elixir (format/parse check) and uses the unified conventions, fixing any snippet that fails.
- [x] 7.2 Run `mix docs` and verify zero ex_doc warnings plus click-through of every cross-guide link in both GitHub-relative and built-doc form; verify `git diff --stat` shows only the six intended files.
