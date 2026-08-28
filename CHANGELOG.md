# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.7.0]

### Fixed

- **Chain data loss under backpressure**: `loop_send/3` (queue-full wait loop) consumed `{:new_data, ...}` messages arriving while a worker was blocked and discarded them as unknown messages. Non-matching messages now stay in the mailbox and are processed in order once the queue drains.
- **Chain worker queue wedge**: `forward_data/6` queued every forwarded message even when no downstream worker existed (the finished callback ran instead), so the last worker's queue accumulated entries nothing would ever confirm and the chain froze after `queue_length` messages. The queue entry is now dropped when no downstream worker consumed the message.
- **Shutdown did not kill workers**: `Looper.shutdown/2` compared a partition *pid* against the partition *id*, so the kill branch never matched; it also dereferenced `worker.pid`, a field the `Worker` struct does not have. Shutdown now resolves live pids from the registry and compares against the partition hash order, as documented in `stop/3`.
- **`queue_length` chain option was ignored**: chain workers were always spawned with the default queue length (50) regardless of the chain's `queue_length` config; workers now inherit it at spawn time.
- **`Supervisor` partition cache never worked**: `handle_call({:query_target_partition, _})` discarded the result of `put_in_cache/3`, so every API call paid a fresh lookup. The cache is now persisted (and cleared on partition restarts, as before).
- **`Chain.restart_all_workers/1` logged a spurious failure** on every successful respawn because `do_spawn_worker/2` returns the worker struct, not `{:ok, _}`; success/failure classification is now correct.
- **`send_to_group_random/3,4` replied with the caller's data** instead of `:ok` because `Group.send_message/3` returned the result of `send/2`; it now returns `:ok`.
- **Documented pid `:link` was rejected**: `Validator` only accepted booleans for `:link`, making the pid-link feature unreachable through `start_with_config/1`; pids are now accepted (the ConfigLoader parser already allowed them).
- Removed dead code: `Worker.validate_option/2` clauses for `:order` were unreachable because `:order` is filtered out before validation.

### Changed

- Test suite expanded to cover failure paths across `Chain`, `Group`, `Looper`, `Supervisor`, `Db`, `Parser`, `Bootstrap`, `Validator`, `Worker`, `CircuitBreaker` and `Error` (total coverage 91% → 97%): group send/broadcast APIs, shutdown kills, restart-skip on normal exits, spawn-failure handling, queue-full backpressure, stale-registry cleanup and option-validation errors.

## [0.6.0]

### Added

- Introspection API on `SuperWorker.Supervisor`:
  - `running_supervisors/0` — discover live supervisors on the node;
  - `supervisor_info/1,2` — partition health (liveness, message queue depth) plus group/chain/worker counts;
  - `list_groups/1,2`, `list_chains/1,2`, `list_standalone_workers/1,2`.
- `SuperWorker.Supervisor.Utils.safe_call/1,3` — invoke user functions without letting exceptions escape.
- Fault tolerance: crashed partitions are detected via monitors and restarted individually by the supervisor master; partitions monitor the master so nothing outlives the supervisor.
- `SuperWorker.Log.debug/1` now compiles away completely when disabled, including its arguments.

### Changed

- `SuperWorker.CircuitBreaker.call/2` executes the protected function in the caller process instead of inside the breaker GenServer, so concurrent calls are not serialized and slow calls cannot block state queries. Half-open probes are limited by `half_open_max_calls`; the call that trips the threshold returns the real error.
- Chain finished-callback failures return the actual reason (`{:error, {kind, reason}}`) instead of a generic `{:error, :callback_failed}`.
- `SuperWorker.TermStorage.get/1` distinguishes a stored `nil` from a missing key; `get_all/0` returns plain `{key, value}` pairs without the internal module prefix.
- `stop/3` with any shutdown type other than `:kill` falls back to a brutal kill instead of crashing partitions (graceful shutdown is not implemented yet).

### Fixed

- A crashing partition no longer takes down the whole supervisor.
- `Chain.restart_all_workers/1` crashed on `worker.pid` (field does not exist); it now resolves pids from the registry and respawns every node.
- `Chain.restart_worker/2` skipped respawning when invoked from the crash handler because the ref row was already cleaned up.
- `restart_group_worker` API always replied `:ok`, discarding failures.
- Invalid GenServer child specs crashed callers (`convert_gen_server_specs/2` matched `{:ok, _}` unconditionally).
- GenServer start failures escaped as uncaught throws and could crash partition processes; they are converted to `{:error, :spawn_failed}`.
- Standalone workers stored the partition number as their supervisor id; `get_my_supervisor/0` now returns the supervisor id.
- Typo in error reason: `:chan_not_found` → `:chain_not_found`.
- Undefined variables inside debug log closures surfaced when compiling with `debug_log: true`.

### Documentation

- README sections for fault tolerance, introspection and utilities.
- Moduledocs for `Looper`, `Partition`, `TermStorage` and `Log`; truthful `stop/3` docs; corrected specs.

## [0.5.0]

Initial documented release: groups, chains and standalone workers under one dynamic supervisor with per-parent restart strategies.
