# Spec Delta

## Purpose

Defines the observable lifecycle, messaging, restart, error, and performance contracts for supervisor groups so grouped workers behave consistently at scale.

## ADDED Requirements

### Requirement: Group options validation

The system SHALL require a non-nil group `id` and reject unknown options and invalid restart strategies with `{:invalid_options, detail}` style errors.

#### Scenario: Missing id is rejected

- **WHEN** a caller adds a group without an `id`
- **THEN** the system returns `{:error, {:invalid_options, _}}` and creates no group row

#### Scenario: Invalid restart strategy is rejected

- **WHEN** a caller adds a group with a strategy outside `:one_for_one` / `:one_for_all`
- **THEN** the system returns `{:error, {:invalid_options, _}}` and creates no group row

#### Scenario: Valid options create a group

- **WHEN** a caller adds a group with an `id` and a supported strategy
- **THEN** the system returns `{:ok, group_id}` and the group is listed with its strategy

### Requirement: Group worker add is atomic and idempotent-safe

The system SHALL reject duplicate worker ids with `:worker_already_exists`, assign a random id when none is given, and leave no ETS rows behind when spawning fails.

#### Scenario: Duplicate worker id is rejected

- **WHEN** a caller adds a worker whose id already exists in the group
- **THEN** the system returns `{:error, :worker_already_exists}` and spawns no new process

#### Scenario: Missing worker id is generated

- **WHEN** a caller adds a worker with `nil` or `false` id
- **THEN** the system assigns a random binary id and the worker becomes retrievable

#### Scenario: Failed spawn leaves no rows

- **WHEN** a GenServer worker fails to start or its start raises/exits
- **THEN** the caller receives `{:error, {:spawn_failed, _}}`, no worker info or ref row remains, and the partition still serves API calls

### Requirement: Group worker reads are consistent

The system SHALL return `:worker_not_found` for unknown workers and accurate counts and membership for existing groups.

#### Scenario: Get unknown worker fails

- **WHEN** a caller reads, kills, restarts, removes, or messages a missing worker id
- **THEN** the system returns `{:error, :worker_not_found}`

#### Scenario: Count reflects membership

- **WHEN** a group holds N workers
- **THEN** count returns `{:ok, N}`, `get_all_workers` returns N infos, and `worker_exists?` is true only for members

#### Scenario: Tuple worker id is unwrapped

- **WHEN** a caller passes `{_, id}` as worker id
- **THEN** the system resolves it identically to `id`

### Requirement: Group worker removal cleans all state

The system SHALL kill the live process and delete both the ref row and the info row; a dead-process row SHALL return a terminal error without partial deletes.

#### Scenario: Remove live worker succeeds

- **WHEN** a caller removes a live worker
- **THEN** the system returns `{:ok, :worker_removed}`, the pid is dead, and no ref or info row remains

#### Scenario: Remove dead worker reports failure

- **WHEN** the worker row points to a dead pid
- **THEN** the system returns `{:error, _}` and does not report success

### Requirement: Group single-worker restart recovers dead state

The system SHALL restart by worker struct or id, clean stale rows when the old process is already dead or missing, and return `:worker_not_found` for unknown ids.

#### Scenario: Restart live worker replaces process

- **WHEN** a caller restarts a live worker
- **THEN** the old pid dies, a new live pid serves the same worker id, and state is reset

#### Scenario: Restart dead-or-missing row still recovers

- **WHEN** the worker info exists but the pid is dead, or no row exists but the id is known via lookup fallback
- **THEN** the system removes stale rows, spawns a fresh process, and returns success

#### Scenario: Restart unknown id fails

- **WHEN** a caller restarts an id with no worker info
- **THEN** the system returns `{:error, :worker_not_found}`

### Requirement: Group kill reports per-worker outcome

The system SHALL kill one worker with `:killed` / `:not_alive` / `:worker_not_found` semantics and kill-all with `:ok` or `{:error, [{:error, worker_id, reason}]}` listing each failure.

#### Scenario: Kill live worker succeeds

- **WHEN** a caller kills a live worker process
- **THEN** the system returns `{:ok, :killed}`

#### Scenario: Kill-all with dead member reports that member

- **WHEN** a group contains at least one dead-pid row during kill-all
- **THEN** the system returns `{:error, errors}` naming each failed worker id and reason

### Requirement: Group messaging delivers or reports failure

The system SHALL deliver direct messages to live workers, broadcast to all live members, and never report success when delivery failed; broadcast failures SHALL propagate to the Supervisor API caller.

#### Scenario: Direct send to live worker succeeds

- **WHEN** a caller sends data to a live group worker
- **THEN** the worker receives the message and the call returns `:ok`

#### Scenario: Direct send to unknown worker fails

- **WHEN** a caller sends data to a missing worker
- **THEN** the system returns `{:error, :worker_not_found}` and delivers nothing

#### Scenario: Broadcast reaches all live workers

- **WHEN** a caller broadcasts to a group with N live workers
- **THEN** all N workers receive the message and the call returns `:ok`

#### Scenario: Broadcast failure is visible to API caller

- **WHEN** any member delivery fails during broadcast
- **THEN** the Supervisor `broadcast_to_group` call returns `{:error, errors}` instead of `{:ok, :sent}`

### Requirement: Group restart strategies isolate failures

The system SHALL restart only the crashed worker under `:one_for_one` and all group members under `:one_for_all`; `:normal` exits SHALL never trigger a restart.

#### Scenario: One-for-one crash restarts only the crashed worker

- **WHEN** a worker crashes abnormally in a `:one_for_one` group holding stateful siblings
- **THEN** only the crashed worker gets a fresh process and siblings keep their state

#### Scenario: One-for-all crash restarts every member

- **WHEN** a worker crashes abnormally in a `:one_for_all` group
- **THEN** every member gets a fresh process and prior per-worker state is cleared

#### Scenario: Normal exit restarts nothing

- **WHEN** a group worker exits with `:normal`
- **THEN** the system performs no restart under either strategy

### Requirement: Group errors use a single consistent contract

The system SHALL return only documented atoms (`:worker_not_found`, `:worker_already_exists`, `:group_not_found`, `{:invalid_options, _}`, `{:spawn_failed, _}` families) and SHALL NOT emit legacy shapes.

#### Scenario: Legacy error shapes are gone

- **WHEN** any Group worker operation fails
- **THEN** the reason is one of the documented atoms and never `:worker_exists`, `:cannot_send`, `:not_found`, `:not_alive` alone, nor a bare string

#### Scenario: Spawn failure preserves cause

- **WHEN** a worker spawn fails
- **THEN** the error carries the underlying cause for logs/telemetry instead of a bare `:spawn_failed`

### Requirement: Group operations meet scale budgets

The system SHALL serve count, broadcast, kill-all, and restart-all on groups up to 1000 workers within the benchmark budget and without fetching unneeded full worker payloads for count-only queries.

#### Scenario: Large-group count stays fast

- **WHEN** a group holds 1000 workers and a caller counts members
- **THEN** the call returns the correct count within the `bench_quick` budget and performs no full-struct scan

#### Scenario: Large-group broadcast and kill-all complete

- **WHEN** a group holds 1000 live workers and a caller broadcasts then kills all
- **THEN** both calls complete within the benchmark budget with per-failure details for any dead member
