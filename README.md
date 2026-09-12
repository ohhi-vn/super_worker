# Intro

The library support for newbies work with process & supervisor in Elixir.
Easy to add & identify processes in system.

*Note: Library is still unstable, please don't use for product.*

## Guides

Full guides ship with the docs (`mix docs`) and in the Hex package:

- [Supervisor Guide](guides/supervisor/SUPERVISOR_GUIDE.md) — groups, chains and standalone workers, fault tolerance, introspection.
- [Pool Guide](guides/pool/POOL_GUIDE.md) — a partitioned job pool: run/cast, retries with backoff, middleware.
- [FunctionChain Guide](guides/function_chain/FUNCTION_CHAIN_GUIDE.md) — composable in-process pipelines with retry, branches and checkpoint/resume.
- [Config Guide](guides/config/CONFIG_GUIDE.md) — declarative supervisor trees in config files ([Quickstart](guides/config/CONFIG_QUICKSTART.md)).

## Guide

Just declare function for worker (task) and input (in param or stream) and run.
Library is created for dev can add workers in runtime without care too much about design supervisor tree.
It's matched with dynamic typed language like Elixir.

## Features

Group processes for easy to maintain and communitcate with group.

Chain processes for work with multi steps or stream processing by process.

Standalone processes for add new process & interact with processes by id.

Each Group/Chain/Standalone process has it restart strategy.

## Supervisor

Support three type of processes in one supervisor. Can declare by config or add in runtime.

```mermaid
graph LR
Client(Client) <-->|api| Supervisor
    Supervisor--> Group_1
    Supervisor--> Chain_1
    Supervisor-->Worker_standalone1
    Supervisor-->Worker_standalone2
    Group_1-->Worker_g1
    Group_1-->Worker_g2
    Group_1-->Worker_g3
    Chain_1-->Worker_c1
    Worker_c1-->Worker_c2
    Worker_c2-->Worker_c3
```

**Type of processes:**

- Group processes
- Chain processes
- Standalone processes

### Group processes

All processes in supervisor have same group_id.
If a process in group is crashed, all other processes will be died follow.
Avoid using trap_exit in process to avoid side effect.

Support send message to worker or broadcast to all workers in a group. Dev don't need to implement a way for transfer data to worker.

```mermaid
graph LR
    Group_1-->Worker_1
    Group_1-->Worker_2
    Group_1-->Worker_3
```

### Chain processes

Support chain task type. The data after process in a process will be passed to next process in chain.
If a process is crashed, all other process in chain will be died follow (depend restart strategy of chain).

From foreign process data can pass to chain (first worker in chain or directly to a worker with id) by Supervisor APIs.

Can config function to call in the end of chain or self implement code in the last worker in the chain.

```mermaid
graph LR
    Worker_c1-->Worker_c2
    Worker_c2-->Worker_c3
```

### Standalone processes

This for standalone worker run in supervisor, it has owner restart strategy.
If a standalone worker is crashed, it doesn't affect to other standalone workers or workers in group/chain.

```mermaid
graph LR
    Supervisor-->Worker_standalone1
    Supervisor-->Worker_standalone2
    Supervisor-->Worker_standalone3
```

## Fault tolerance

Workers are spread over one or more partitions. Each partition is monitored by
the supervisor master, so failures are contained and recovered automatically:

- a crashing **worker** is restarted by its partition following the restart
  strategy of its parent (standalone / group / chain). Workers on other
  partitions are never affected;
- a crashing **partition** is restarted individually — the supervisor and all
  other partitions keep serving requests;
- every partition watches the master process, so nothing outlives the
  supervisor itself.

```mermaid
graph LR
    Master(Supervisor master) -->|monitor| P1(Partition 1)
    Master -->|monitor| P2(Partition 2)
    P1 --> W1(Worker)
    P1 --> W2(Worker)
    P2 --> W3(Worker)
```

## Pool — a simple, partitioned job pool

`SuperWorker.Pool` is a job pool on top of the same ideas: hand it a function,
an MFA or a worker module, then submit jobs — no supervision tree to design,
no message protocols to implement. It is a **single-node, in-memory pool**:
queued jobs survive worker crashes and partition restarts, but not a node
crash.

```elixir
# 1. A bare function
{:ok, _} =
  SuperWorker.Pool.start_link(
    name: MyPool,
    task: fn job -> do_work(job) end,
    size: 20
  )

# 2. An MFA
{:ok, _} =
  SuperWorker.Pool.start_link(name: MyPool, task: {MyMod, :process, []}, size: 20)

# 3. A stateful worker module (holds a connection, a buffer, ...)
{:ok, _} =
  SuperWorker.Pool.start_link(
    name: MyPool,
    worker: MyApp.ImageResizer,
    size: 20,
    retry: [max_attempts: 5, backoff: {:exponential, base: 200, max: 10_000, jitter: true}],
    on_failure: {MyApp.DeadLetter, :store, []},
    middleware: [SuperWorker.Pool.Middleware.Telemetry, SuperWorker.Pool.Middleware.CircuitBreaker]
  )
```

```elixir
{:ok, result} = SuperWorker.Pool.run(MyPool, job)      # sync: blocks until done
{:ok, result} = SuperWorker.Pool.run(MyPool, job, timeout: 5_000)
{:ok, ref} = SuperWorker.Pool.run_async(MyPool, job)   # Task-like ref
{:ok, result} = SuperWorker.Pool.await(ref)
{:ok, result} = SuperWorker.Pool.await(ref, 5_000)     # custom timeout
:ok = SuperWorker.Pool.cast(MyPool, job)               # fire-and-forget
```

Key properties:

- **Partitioned** — workers and queues are split over `partitions`
  (default: schedulers online) independent bottlenecks; jobs are routed by
  round robin or by hashing the job (`:routing: :hash`).
- **Backpressure** — each partition has a bounded queue (`max_queue`);
  overflow submissions immediately fail with `{:error, :overloaded}`.
- **Retries with backoff** — `{:retry, reason, state}` from a worker
  reschedules the job (via `Process.send_after`, never `Process.sleep`) until
  `max_attempts`; `{:error, reason, state}` is a final failure.
- **Crash isolation** — a worker crash restarts only that worker and its
  in-flight job is requeued once (poison-pill safe) and charged to its retry
  budget; after retries are exhausted, `:on_failure` fires — no silent job
  loss.
- **Pluggable middleware** — Plug-style pipeline around job execution
  (`SuperWorker.Pool.Middleware`); `Telemetry` and `CircuitBreaker` ship as
  built-ins, roll your own for rate limiting, request-id propagation, etc.

The worker behaviour for stateful workers:

```elixir
defmodule MyApp.ImageResizer do
  @behaviour SuperWorker.Pool.Worker

  @impl true
  def init(opts), do: {:ok, open_connection(opts)}

  @impl true
  def handle_job(job, conn) do
    case resize(conn, job) do
      {:ok, result, conn} -> {:ok, result, conn}
      {:timeout, conn} -> {:retry, :timeout, conn}   # transient: try again
      {:invalid, conn} -> {:error, :invalid, conn}   # final: fail the job
    end
  end
end
```

Durability is in-memory only: queued jobs survive worker crashes and
partition restarts, not a node crash. See `SuperWorker.Pool` moduledoc for
the full option list and error shapes.

## Introspection & observability

Running supervisors can be discovered and inspected at runtime:

```elixir
# All supervisors started by the library on this node.
Sup.running_supervisors()
# [{:sup1, #PID<0.200.0>}]

# Partition health + counts of groups/chains/workers.
{:ok, info} = Sup.supervisor_info(:sup1)
info.num_groups
info.partitions |> Enum.all?(& &1.alive?)

# List groups/chains/standalone workers with their configuration.
{:ok, groups} = Sup.list_groups(:sup1)
{:ok, chains} = Sup.list_chains(:sup1)
{:ok, workers} = Sup.list_standalone_workers(:sup1)
```

## Utilities

The library ships a few small focused helpers:

- `SuperWorker.CircuitBreaker` — protect external calls with a closed/open/
  half-open circuit. The protected function runs in the caller process, so
  slow calls never block the breaker:

  ```elixir
  SuperWorker.CircuitBreaker.call(:external_api, fn ->
    Req.get("https://api.example.com")
  end)
  ```

- `SuperWorker.TermStorage` — thin `:persistent_term` wrapper for rarely
  changing node-wide data.

- `SuperWorker.Supervisor.Utils.safe_call/1,3` — invoke user functions without
  letting exceptions escape:

  ```elixir
  {:error, {:error, %RuntimeError{}}} = Utils.safe_call(fn -> raise "boom" end)
  ```

## Planned features

- Multiprocess per chain node.
- Auto scale for chain.
- Distributed in cluster.
