# Proposal

## Why

User-facing docs (`README.md` plus five files in `guides/`) are accurate but hard to scan and learn from: the README mixes landing-page, tutorial, and API-reference roles, the guides use inconsistent structure and terminology, content is duplicated across files, and at least one cross-guide link is broken. New users have no clear reading order or "which primitive do I need" guidance.

## What Changes

- Rewrite `README.md` as a short landing page: what the library is, 60-second quickstart, primitive chooser (Supervisor vs Pool vs FunctionChain), links into guides. Move deep Pool/Supervisor reference material out of the README into the guides.
- Normalize all five guides (`SUPERVISOR_GUIDE.md`, `POOL_GUIDE.md`, `FUNCTION_CHAIN_GUIDE.md`, `CONFIG_GUIDE.md`, `CONFIG_QUICKSTART.md`) to one consistent structure: Overview → When to use / when not to use → Quick start → Core concepts → Options/reference → Error handling → Introspection/observability → Next steps.
- Fix language, typos, and stale terminology (e.g. "main process" vs "master", "partition technic", "communitcate", "three type of processes"); unify example style (aliases, `link: false` explained once, consistent `:sup1`/`MyPool` usage).
- Deduplicate repeated content (fault-tolerance diagram, restart-strategy tables, Pool options) via single-source sections + cross-links instead of copies.
- Fix broken/relative links (e.g. Supervisor Guide → Config Guide) and add a docs reading map plus a Supervisor-chain vs FunctionChain vs Pool chooser.
- Clarify Config Quickstart vs Config Guide split: quickstart = 5-minute path only; guide = full format reference + troubleshooting. Remove low-value filler (generic best-practices list, License section, Module Details API stubs that duplicate `mix docs`).
- No behavior, API, or config-format changes.

## Capabilities

### New Capabilities

None — docs-only refactor, no system behavior is introduced.

### Modified Capabilities

None — no existing spec REQUIREMENTS change. This change opts out of specs via `skip_specs: true` in `.openspec.yaml` (pure docs refactor per schema guidance).

## Impact

- Affects only `README.md` and `guides/**/*.md` source files.
- No code, public API, config format, dependency, or runtime changes.
- Generated output in `doc/` is refreshed via `mix docs`, not hand-edited.
- Risk is low: broken links or lost detail during dedupe; mitigated by link checks and a content-preservation review per guide.
