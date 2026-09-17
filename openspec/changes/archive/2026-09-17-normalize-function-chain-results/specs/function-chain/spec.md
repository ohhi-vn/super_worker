## Purpose

Defines the result contracts SuperWorker adapters expose when running FunctionChain jobs, so callers receive stable results regardless of chain run options such as return-context metadata.

## ADDED Requirements

### Requirement: Adapter failures exclude run-context metadata

FunctionChain adapters SHALL discard the metadata element of a failed run's `{:error, reason, metadata}` result while preserving the failure reason and the adapter's existing error policy. A plain `{:error, reason}` failure SHALL receive the same handling.

#### Scenario: Pool job fails without retry classification

- **WHEN** a FunctionChain pool job fails with return-context metadata enabled and the worker uses the default error policy
- **THEN** the caller receives `{:error, reason}` without the run-context metadata element

#### Scenario: Pool job fails with retry classification

- **WHEN** a FunctionChain pool job fails with return-context metadata enabled and the worker uses the retry error policy
- **THEN** the pool applies its existing retry policy using the original failure reason without the run-context metadata
- **AND** retry exhaustion retains the existing `{:error, {:retries_exhausted, reason}}` result contract

#### Scenario: Supervisor chain node fails

- **WHEN** a supervisor chain node's FunctionChain run fails with return-context metadata enabled
- **THEN** the node function returns `{:error, reason}` without the metadata element rather than forwarding a successful result

#### Scenario: Worker loop replies to a failed request

- **WHEN** a standalone or group worker loop receives a valid request envelope and the chain fails with return-context metadata enabled
- **THEN** the reply contains `{:error, reason}` without metadata and preserves the request reference
- **AND** the loop remains available for subsequent requests

#### Scenario: Worker loop reports a failed job to its callback

- **WHEN** a worker loop has a result callback configured and either a plain job or an enveloped request fails with return-context metadata enabled
- **THEN** the callback receives the original job and `{:error, reason}` without metadata

#### Scenario: Plain errors retain the same handling

- **WHEN** any adapter receives a plain `{:error, reason}` chain result
- **THEN** it applies the same failure policy and delivery contract as for a metadata-bearing failure with that reason

### Requirement: Successful results retain adapter-specific contracts

Adapters SHALL preserve successful result values and their existing delivery protocols when normalizing failure metadata.

#### Scenario: Pool job succeeds

- **WHEN** a FunctionChain pool job succeeds with value `value`
- **THEN** the caller receives `{:ok, value}`

#### Scenario: Supervisor chain node succeeds

- **WHEN** a supervisor chain node's FunctionChain run succeeds with value `value`
- **THEN** the node function returns `{:next, value}` for forwarding

#### Scenario: Worker loop succeeds

- **WHEN** a worker loop processes a valid request envelope and its chain succeeds with value `value`
- **THEN** the reply contains `{:ok, value}` and preserves the request reference
- **AND** a configured result callback receives the original job and `{:ok, value}`
