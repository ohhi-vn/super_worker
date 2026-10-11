## Purpose

Defines the observable contract of supervisor chain nodes: option validation, worker add/remove/restart/kill, sequential message forwarding with backpressure, finished callbacks, error return shapes, and scale budgets. Sibling of the `supervisor-group` capability.

## ADDED Requirements

### Requirement: Chain options validation

The system SHALL validate chain options before creating a chain: restart strategy MUST be one of `:one_for_one`, `:one_for_all`, `:rest_for_one`; send type MUST be one of `:broadcast`, `:random`, `:partition`, `:round_robin`; `queue_length` MUST be a positive integer; `finished_callback` MUST be nil, `{:fun, fun/1}`, or `{module, function, args}`.

#### Scenario: Invalid option is rejected

- **WHEN** a caller checks chain options with an invalid restart strategy, send type, callback, or queue length
- **THEN** the system returns an error identifying the invalid field and creates no chain

#### Scenario: Valid options produce a chain

- **WHEN** a caller checks chain options that satisfy all rules
- **THEN** the system returns `{:ok, chain}` with the normalized values

#### Scenario: Validation has one definition

- **WHEN** the same options are checked through the supervisor API or the chain config module
- **THEN** both return the same result for the same input

### Requirement: Chain worker add assigns sequential order

The system SHALL append each added node to the end of the chain with the next order and SHALL keep orders contiguous from 1 to the number of nodes. A worker whose id already exists in the chain MUST be rejected.

#### Scenario: Duplicate worker id is rejected

- **WHEN** a caller adds a worker whose id already exists in the chain
- **THEN** the system returns `{:error, :already_exists}` and the chain's worker count is unchanged

#### Scenario: Workers receive increasing order

- **WHEN** a caller adds three nodes to an empty chain
- **THEN** the nodes hold orders 1, 2, 3 in insertion order

#### Scenario: Add after removal does not collide

- **WHEN** a caller removes the node at order 2 of a 3-node chain and then adds a new node
- **THEN** the remaining nodes keep their relative order, the orders stay contiguous, and the new node is placed at the end

### Requirement: Multi-worker node add is all-or-nothing

The system SHALL either spawn all `num_workers` workers of a chain node or none of them; a failure part-way through MUST NOT leave a partially spawned node behind and MUST be returned as an error.

#### Scenario: Multi-worker node spawns every instance

- **WHEN** a caller adds a chain node with `num_workers: 3`
- **THEN** the system returns `{:ok, chain}` and exactly three workers are alive for that node

#### Scenario: Conflict during multi-worker add is reported

- **WHEN** a multi-worker add conflicts with an existing worker id
- **THEN** the system returns an error and leaves no new worker of that node alive

### Requirement: Chain worker removal releases all state

The system SHALL, on worker removal, kill the worker and delete its worker row and its replica routing entry so that no routing entry points at a dead process. When the last replica of a node is removed, the node's order is released and later nodes shift down to keep orders contiguous. The removal result MUST indicate whether a worker was actually removed.

#### Scenario: Removing an existing worker

- **WHEN** a caller removes an existing chain worker
- **THEN** the worker process is terminated, it is no longer listed by the chain, and no routing entry references its pid

#### Scenario: Removing the last replica releases the order

- **WHEN** a caller removes the only replica of a node in the middle of a chain
- **THEN** the later nodes shift down by one order and messages continue to reach them in the same relative order

#### Scenario: Removing a missing worker

- **WHEN** a caller removes a worker id that does not exist in the chain
- **THEN** the system returns an error and does not change other workers

#### Scenario: Removed chain leaves no routing entries

- **WHEN** a caller removes a chain that had workers
- **THEN** no order row for that chain remains and no process of that chain is alive

### Requirement: Chain single-worker restart recovers the node

The system SHALL restart a single chain worker, keeping its id and order, including when the worker's process reference was already cleaned up by the crash handler.

#### Scenario: Killed worker is respawned

- **WHEN** a chain worker is killed and the chain restart strategy is `:one_for_one`
- **THEN** a new process with the same worker id and order is alive and receives subsequent messages

#### Scenario: Restart of a missing worker

- **WHEN** a caller restarts a worker id that does not exist in the chain
- **THEN** the system returns `{:error, :worker_not_found}`

#### Scenario: Restart after crash handler cleanup

- **WHEN** a chain worker crashed and its reference row was already removed before restart
- **THEN** the system still respawns the worker and returns success

### Requirement: Chain all-workers restart reports per-worker outcome

The system SHALL restart every worker of a chain for `:one_for_all` and SHALL report each worker that could not be respawned as an error instead of logging success.

#### Scenario: All workers are respawned

- **WHEN** one worker of a `:one_for_all` chain crashes
- **THEN** every worker of that chain is respawned with its id and order

#### Scenario: Respawn failure is reported

- **WHEN** at least one worker cannot be respawned during a restart-all
- **THEN** the system returns an error listing the failed worker ids and logs them

#### Scenario: Restart-all with no workers

- **WHEN** a restart-all runs on a chain with no workers
- **THEN** the system returns success without spawning any process

### Requirement: Killing a chain worker removes its routing

The system SHALL kill a chain worker on request and delete its reference row; a kill of a missing worker MUST return an error.

#### Scenario: Kill an existing worker

- **WHEN** a caller kills an existing chain worker
- **THEN** the process terminates and its reference row is deleted

#### Scenario: Kill a missing worker

- **WHEN** a caller kills a worker id that does not exist
- **THEN** the system returns `{:error, :not_found}`

### Requirement: Chain forwards data in order

The system SHALL deliver each forwarded message to the node with the next order (to replicas as defined by the send type), and SHALL run the finished callback with the message data when no next node exists. With a broadcast send type, each replica of the last node completes its own copy and the callback runs once per completed copy.

#### Scenario: Data flows through all workers

- **WHEN** a message is sent to a chain of three workers that each return `{:next, data}`
- **THEN** each worker processes the message in order and the finished callback receives the final data once

#### Scenario: Message with no downstream worker

- **WHEN** the last worker forwards data and no downstream worker exists
- **THEN** the finished callback runs and no queue entry remains for that message

#### Scenario: No message is routed to a dead worker

- **WHEN** a worker in the middle of the chain is removed or killed
- **THEN** subsequent messages are not sent to the dead process and the upstream queue does not grow without bound

### Requirement: Worker result handling

The system SHALL interpret worker results as: `{:next, data}` or plain data forwards data; `{:drop, reason}` and `{:error, reason}` stop forwarding and keep the worker alive; `{:stop, reason}` terminates the worker; a crash inside the worker function MUST NOT terminate the worker.

#### Scenario: Plain return passes through

- **WHEN** a worker returns a plain value
- **THEN** the value is forwarded to the next worker

#### Scenario: Drop or error stops forwarding

- **WHEN** a worker returns `{:drop, reason}` or `{:error, reason}`
- **THEN** nothing is forwarded, the worker stays alive, and the reason is logged

#### Scenario: Worker function crash keeps the worker alive

- **WHEN** the worker function raises or exits
- **THEN** the error is logged, the message is not forwarded, and the worker keeps processing later messages

### Requirement: Queue backpressure

The system SHALL bound each worker's in-flight queue to `queue_length`; when full, the worker MUST wait for downstream confirmation for up to 5 seconds, keep unrelated messages in its mailbox for later processing, and exit with `:queue_full` if no confirmation arrives.

#### Scenario: Full queue waits for confirmation

- **WHEN** a worker's queue is full and downstream confirms a message within the wait window
- **THEN** the worker continues forwarding and no message is dropped

#### Scenario: Unrelated messages are not discarded while blocked

- **WHEN** a worker is blocked on a full queue and receives a chain data message
- **THEN** the message is processed after the queue drains and is not lost

#### Scenario: Confirmation timeout exits the worker

- **WHEN** a worker's queue stays full past the wait window
- **THEN** the worker exits with `:queue_full`

### Requirement: Replica routing follows the send type

The system SHALL deliver each message from a node to its replicas according to the chain send type: `:broadcast` to every replica, `:random` to one uniformly chosen replica, `:round_robin` to replicas in turn, and `:partition` to the replica selected by a hash of the message data (same data, same replica). A node with a single replica MUST receive every message regardless of send type.

#### Scenario: Round-robin spreads messages across replicas

- **WHEN** a node has three replicas and the chain send type is `:round_robin` and six messages are sent
- **THEN** each replica receives exactly two messages

#### Scenario: Broadcast reaches every replica

- **WHEN** a node has three replicas and the chain send type is `:broadcast`
- **THEN** each replica receives every message

#### Scenario: Partition is stable per data

- **WHEN** the chain send type is `:partition` and the same data is sent twice
- **THEN** both messages reach the same replica

#### Scenario: Random reaches a replica

- **WHEN** the chain send type is `:random` and a node has replicas
- **THEN** each message reaches exactly one replica of that node

#### Scenario: Every replica remains reachable

- **WHEN** a multi-replica node is added
- **THEN** every replica is a valid routing target, not only the last one added

### Requirement: Chain restart strategies

The system SHALL apply the chain restart strategy when a chain worker terminates abnormally: `:one_for_one` restarts only the failed worker, `:one_for_all` restarts every worker of the chain, and `:rest_for_one` restarts the failed worker and every worker with a higher order. Normal exits MUST NOT trigger a restart.

#### Scenario: rest_for_one restarts the tail

- **WHEN** the worker at order 2 of a 4-node chain crashes under `:rest_for_one`
- **THEN** the workers at orders 2, 3 and 4 are restarted and the worker at order 1 keeps running

#### Scenario: Normal exit is ignored

- **WHEN** a chain worker exits with reason `:normal`
- **THEN** no restart is performed under any strategy

### Requirement: Finished callback failure contract

The system SHALL run the finished callback for each completed message and SHALL return `{:ok, :callback_executed}` on success, `{:error, reason}` with the actual reason on failure, and `{:error, :invalid_callback}` when the callback definition is invalid; a missing callback MUST return `{:ok, :no_callback}`.

#### Scenario: Callback success

- **WHEN** the finished callback returns normally
- **THEN** the system reports `{:ok, :callback_executed}`

#### Scenario: Callback failure returns the reason

- **WHEN** the finished callback raises or exits
- **THEN** the system returns `{:error, reason}` with the actual reason and logs it

#### Scenario: No callback configured

- **WHEN** no finished callback is configured
- **THEN** the system returns `{:ok, :no_callback}`

### Requirement: Chain read operations are consistent

The system SHALL return the same worker set from `get_worker/2`, `worker_exists?/2`, `get_all_workers/1`, and `count_workers/1` for the same chain state, and SHALL return `{:error, :worker_not_found}` for unknown ids.

#### Scenario: Counts match listings

- **WHEN** a chain has N workers
- **THEN** `count_workers/1` returns N and `get_all_workers/1` returns N workers

#### Scenario: Existence check matches lookup

- **WHEN** a worker id is looked up
- **THEN** `worker_exists?/2` is true exactly when `get_worker/2` returns `{:ok, _}`

### Requirement: Chain operations meet scale budgets

The system SHALL keep per-message forwarding cost and per-worker add cost from growing with chain length beyond the baseline measured by `benchmarks/bench_main.exs` ("Chain worker message passing") before this change.

#### Scenario: Adding many workers stays linear

- **WHEN** a caller adds N workers to one chain
- **THEN** total add work does not grow faster than linearly in N

#### Scenario: Per-message cost does not regress

- **WHEN** the chain message passing benchmark runs after the change
- **THEN** its throughput is not lower than the pre-change baseline

### Requirement: Idle chain workers remain available

The system SHALL keep an idle chain worker alive and available for messages, and SHALL NOT restart it solely because no message arrived within a timeout.

#### Scenario: Idle worker still receives data

- **WHEN** a chain worker receives no messages for longer than the previous idle timeout
- **THEN** it is still alive and processes the next message without a restart
