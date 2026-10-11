# Spec Delta

## Purpose

Defines the observable lifecycle, id-based lookup and messaging, restart, intensity, error, and performance contracts for supervisor standalone workers so they behave consistently and safely at scale.

## ADDED Requirements

### Requirement: Standalone worker options validation

The system SHALL accept `:restart_strategy` (`:permanent` / `:transient` / `:temporary`) and positive-integer `:max_restarts`, `:max_seconds`, and `:auto_restart_time`, rejecting an invalid strategy with `{:error, {:invalid_options, detail}}`.

#### Scenario: Invalid restart strategy is rejected

- **WHEN** a caller adds a standalone worker with a strategy outside `:permanent` / `:transient` / `:temporary`
- **THEN** the system returns `{:error, {:invalid_options, _}}` and starts no process

#### Scenario: Invalid restart intensity is rejected

- **WHEN** a caller passes a non-positive or non-integer `:max_restarts`, `:max_seconds`, or `:auto_restart_time`
- **THEN** the system returns `{:error, {:invalid_options, _}}` and starts no process

#### Scenario: Valid options start a worker

- **WHEN** a caller adds a standalone worker with a supported strategy and valid intensity options
- **THEN** the system returns `{:ok, id}` and the worker is retrievable by id

### Requirement: Standalone worker add is atomic

The system SHALL return `{:ok, id}` only when the worker process started, return `{:error, {:spawn_failed, cause}}` preserving the cause otherwise, and leave no worker info or ref row behind on failure.

#### Scenario: Successful add starts the process

- **WHEN** a caller adds a standalone worker whose function or GenServer starts successfully
- **THEN** the system returns `{:ok, id}` and `get_pid_standalone_worker` returns a live pid

#### Scenario: Failed spawn reports failure and leaves no rows

- **WHEN** a GenServer worker fails to start or its start raises/exits
- **THEN** the caller receives `{:error, {:spawn_failed, _}}`, no worker info or ref row remains, and the partition still serves API calls

#### Scenario: Duplicate worker id is rejected

- **WHEN** a caller adds a standalone worker whose id already exists
- **THEN** the system returns `{:error, :worker_already_exists}` and spawns no new process

#### Scenario: Missing worker id is generated

- **WHEN** a caller adds a standalone worker without an id
- **THEN** the system assigns a unique id and the worker becomes retrievable

### Requirement: Standalone worker errors use a consistent contract

The system SHALL return `{:error, :worker_not_found}` for send, get-pid, and remove operations on an unknown or already-removed worker id and SHALL NOT emit the legacy `:not_found` shape for standalone.

#### Scenario: Send to unknown worker fails

- **WHEN** a caller sends data to a missing standalone worker id
- **THEN** the system returns `{:error, :worker_not_found}` and delivers nothing

#### Scenario: Get pid of unknown worker fails

- **WHEN** a caller requests the pid of a missing standalone worker id
- **THEN** the system returns `{:error, :worker_not_found}`

#### Scenario: Remove unknown worker fails

- **WHEN** a caller removes a missing standalone worker id
- **THEN** the system returns `{:error, :worker_not_found}`

### Requirement: Standalone worker removal cleans all state

The system SHALL stop the live process and delete both its ref row and info row, return `{:ok, id}`, and treat the resulting process-down notification as already cleaned without emitting a spurious "unexpected worker data" warning.

#### Scenario: Remove live worker succeeds

- **WHEN** a caller removes a live standalone worker
- **THEN** the system returns `{:ok, id}`, the pid is dead, and no ref or info row remains

#### Scenario: Removed worker is unreachable

- **WHEN** a caller sends to, reads, or removes a previously removed worker id
- **THEN** the system returns `{:error, :worker_not_found}`

#### Scenario: Removal does not warn on the trailing down message

- **WHEN** a removed worker's process-down notification arrives after cleanup
- **THEN** the partition logs no "unexpected worker data" warning and keeps serving other workers

### Requirement: Standalone restart strategies isolate failure

The system SHALL restart a `:permanent` worker on any exit, a `:transient` worker only on abnormal exit, and never restart a `:temporary` worker; restarted workers SHALL have fresh state and SHALL NOT affect other standalone workers.

#### Scenario: Permanent worker restarts after crash

- **WHEN** a `:permanent` standalone worker crashes
- **THEN** a new live process serves the same worker id

#### Scenario: Transient worker restarts only on abnormal exit

- **WHEN** a `:transient` standalone worker exits abnormally
- **THEN** it restarts, and when it exits with `:normal` it does not restart

#### Scenario: Temporary worker never restarts

- **WHEN** a `:temporary` standalone worker crashes
- **THEN** no new process is started and the worker becomes unreachable

#### Scenario: One worker's crash leaves siblings running

- **WHEN** one standalone worker crashes while others hold state
- **THEN** the siblings keep their state and continue responding

### Requirement: Standalone restart intensity bounds crash loops

The system SHALL limit restarts to `:max_restarts` within any `:max_seconds` window; once the ceiling is exceeded the system SHALL stop restarting that worker, log the event with the cause, and keep the partition healthy. When `:auto_restart_time` is provided the system SHALL wait that cooldown before resuming restart attempts.

#### Scenario: Crash loop stops at the ceiling

- **WHEN** a `:permanent` worker crashes more than `:max_restarts` times within `:max_seconds`
- **THEN** the system stops restarting it, logs the cause, and the partition continues serving other workers

#### Scenario: Restart resumes after cooldown

- **WHEN** `:auto_restart_time` is set and the restart ceiling was exceeded
- **THEN** after the cooldown elapses the worker is eligible for restart again

#### Scenario: Restart within the window is allowed

- **WHEN** a worker crashes fewer than `:max_restarts` times within `:max_seconds`
- **THEN** each crash is restarted normally

### Requirement: Standalone introspection reflects live workers

The system SHALL list each existing standalone worker with its `id`, `name`, and `restart_strategy`, and SHALL omit workers that failed to start or were removed.

#### Scenario: Listing shows existing workers

- **WHEN** a supervisor holds N standalone workers
- **THEN** `list_standalone_workers` returns those N workers with their id, name, and restart strategy

#### Scenario: Failed or removed workers are absent

- **WHEN** a worker failed to start or was removed
- **THEN** it does not appear in `list_standalone_workers`

### Requirement: Standalone operations meet scale budgets

The system SHALL add, message, read, remove, and shut down standalone workers up to the benchmark scale within budget, and the fresh-add path SHALL NOT perform a full-table scan per add.

#### Scenario: Adding many workers scales linearly

- **WHEN** a caller adds several hundred standalone workers
- **THEN** all are started and retrievable within the benchmark budget without a per-add full-table scan

#### Scenario: Messaging scales with concurrent senders

- **WHEN** many callers send to standalone workers concurrently
- **THEN** messages are delivered without loss within the benchmark budget

#### Scenario: Shutdown of many workers completes

- **WHEN** a supervisor holding hundreds of standalone workers is stopped
- **THEN** all owned workers are terminated and the supervisor stops within the benchmark budget
