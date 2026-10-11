# pool Specification

## Purpose

Defines the observable submission, backpressure, retry, crash-recovery, dead-letter, circuit-breaker, validation, and scale contracts for the partitioned process pool so jobs run with bounded latency and no silent loss.

## Requirements
### Requirement: Pool submissions return stable result shapes

The system SHALL return `{:ok, result}` on success and `{:error, reason}` on failure for `run` and `await`, where `reason` is the job's own error or one of the documented pool reasons.

#### Scenario: Successful job returns its result

- **WHEN** a caller runs a job whose worker returns success
- **THEN** the system returns `{:ok, result}` with the worker's result

#### Scenario: Failed job returns its error

- **WHEN** a caller runs a job whose worker returns an expected error
- **THEN** the system returns `{:error, reason}` with the worker's reason

#### Scenario: Missing pool is reported

- **WHEN** a caller submits to a pool name that is not running
- **THEN** the system returns `{:error, :pool_not_found}` and runs nothing

#### Scenario: Caller timeout drops only the wait

- **WHEN** a caller times out waiting for a job
- **THEN** the system returns `{:error, :timeout}` to the caller while the job keeps running to completion

### Requirement: Pool rejects overload before queueing

The system SHALL reject submissions beyond the per-partition queue bound immediately with `{:error, :overloaded}` without running the job, and SHALL accept jobs again once the backlog drains.

#### Scenario: Full partition rejects new submissions

- **WHEN** a partition's backlog is full and no worker is idle
- **THEN** the system returns `{:error, :overloaded}` and the job never runs

#### Scenario: Queue drains and recovers

- **WHEN** queued jobs complete and free backlog space
- **THEN** new submissions are accepted again

#### Scenario: Overloaded cast does not block the caller

- **WHEN** a fire-and-forget cast arrives at a full partition
- **THEN** the call returns promptly and the job is dropped with a warning and a telemetry event

### Requirement: Pool dispatches queued jobs in order with routing affinity

The system SHALL dispatch queued jobs first-come-first-served to idle workers within a partition, and hash routing SHALL keep a job and its retries on the same partition.

#### Scenario: Queued jobs run in submission order

- **WHEN** several jobs wait while workers are busy
- **THEN** they start in the order they were accepted as workers free up

#### Scenario: Hash routing keeps retries together

- **WHEN** a job submitted with hash routing retries
- **THEN** the retry runs on the same partition as the original attempt

### Requirement: Pool retries with backoff then dead-letters

The system SHALL reschedule `{:retry, reason}` jobs after the configured backoff without holding a worker, and SHALL return `{:error, {:retries_exhausted, reason}}` after `max_attempts` attempts.

#### Scenario: Retrying job eventually succeeds

- **WHEN** a job asks for retry fewer times than `max_attempts`
- **THEN** each retry runs after its backoff delay and success returns `{:ok, result}`

#### Scenario: Exhausted retries report the last reason

- **WHEN** a job keeps asking for retry through `max_attempts`
- **THEN** the system returns `{:error, {:retries_exhausted, reason}}` with the last reason

#### Scenario: Retry does not hold worker capacity

- **WHEN** a job waits out its backoff delay
- **THEN** other queued jobs can run on the freed worker before the retry is due

### Requirement: Pool crash requeues once and isolates failure

The system SHALL requeue a crashed worker's in-flight job at most once against its retry budget, SHALL dead-letter a repeatedly crashing job instead of crash-looping, and SHALL keep other workers serving jobs.

#### Scenario: Crash requeues the in-flight job

- **WHEN** a worker crashes while running a job for the first time
- **THEN** the job is requeued once and a restarted worker runs it against its retry budget

#### Scenario: Poison-pill job is dead-lettered

- **WHEN** a job crashes its worker on every attempt
- **THEN** the system returns `{:error, {:worker_crashed, reason}}` after the requeue instead of requeueing again

#### Scenario: Sibling workers survive a crash

- **WHEN** one worker crashes while others hold jobs
- **THEN** the other workers keep serving jobs and partition health is preserved

#### Scenario: Supervisor shutdown requeues without charging the crash budget

- **WHEN** a worker holding a job exits `:shutdown` from supervisor shutdown or a rest_for_one cascade after a sibling crashed, or `:noproc` from a registration race where the worker died untracked
- **THEN** the job is requeued without incrementing attempts or marking it crash-requeued, and a crash telemetry event is still emitted

### Requirement: Pool delivers failures to on_failure and casts to on_result

The system SHALL invoke `:on_failure` with `(job, reason)` for every job that ran and finally failed, and SHALL invoke `:on_result` with `(job, outcome)` for every cast job covering both success and failure.

#### Scenario: Exhausted job fires on_failure

- **WHEN** a job exhausts retries or crashes finally
- **THEN** `:on_failure` is invoked with the job and the raw failure reason

#### Scenario: Cast success and failure reach on_result

- **WHEN** a cast job completes or finally fails and `:on_result` is configured
- **THEN** `:on_result` is invoked with the job and its outcome

#### Scenario: Callback crash never breaks the pool

- **WHEN** an `:on_failure` or `:on_result` callback raises or exits
- **THEN** the pool logs the failure, emits a telemetry event, and keeps serving jobs

### Requirement: Pool circuit breaker fails fast and probes recovery

The system SHALL reject submissions with `{:error, :circuit_open}` while the breaker is open without running the job, SHALL NOT fire `:on_failure` for rejected jobs, and SHALL let one probe through after the reset timeout.

#### Scenario: Open circuit rejects submissions

- **WHEN** consecutive failures trip the breaker
- **THEN** new submissions return `{:error, :circuit_open}` and no worker runs them

#### Scenario: Probe closes or reopens the circuit

- **WHEN** the reset timeout elapses and the probe job succeeds
- **THEN** the circuit closes, and when the probe fails the circuit opens again

#### Scenario: Breaker failure is observable

- **WHEN** the breaker trips or closes
- **THEN** the system emits circuit open and closed telemetry events with pool and partition identity

### Requirement: Pool validates options at start

The system SHALL reject invalid pool options at `start_link` with `{:error, {:invalid, {key, value}}}` or `{:error, {:missing, :name}}` shapes and SHALL start no pool, including invalid circuit-breaker thresholds and timeouts.

#### Scenario: Missing name is rejected

- **WHEN** a caller starts a pool without a `:name`
- **THEN** the system returns `{:error, {:missing, :name}}` and starts nothing

#### Scenario: Invalid breaker options are rejected

- **WHEN** a caller passes a non-positive `failure_threshold` or non-positive `reset_timeout`
- **THEN** the system returns `{:error, {:invalid, _}}` and starts nothing

#### Scenario: Valid options start the pool

- **WHEN** a caller starts a pool with supported options
- **THEN** the system returns `{:ok, pid}` and the pool accepts jobs

### Requirement: Pool exposes queue and worker health

The system SHALL report per-partition queue length, busy workers, and idle workers via `info`, and SHALL emit telemetry for overload, retry, crash, dead-letter, and circuit events.

#### Scenario: Info reflects live backlog

- **WHEN** a caller inspects a pool with queued and running jobs
- **THEN** `info` reports each partition's queue, busy, and idle counts

#### Scenario: Overload and dead-letter are measurable

- **WHEN** a submission is rejected for overload or a job dead-letters
- **THEN** the system emits the corresponding telemetry event with pool and partition identity

### Requirement: Pool operations meet scale budgets

The system SHALL submit, dispatch, message, and stop pool jobs up to the benchmark scale within budget, with per-job partition work bounded so a busy pool does not degrade with backlog size.

#### Scenario: Bursty submissions stay bounded

- **WHEN** many callers submit jobs concurrently to a sized pool
- **THEN** all accepted jobs complete within the benchmark budget and overload rejections return promptly

#### Scenario: Crash storm stays contained

- **WHEN** many workers crash while jobs are in flight
- **THEN** requeues and dead-letters complete within budget and healthy partitions keep serving jobs
