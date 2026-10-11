# SuperWorker

An Elixir library for supervised background work. Declare workers as plain
functions and run them — in long-lived supervised processes, in a partitioned
job pool, or as in-process pipelines — without designing a supervision tree
upfront.

*Note: the library is still unstable, please don't use it in production.*

## Which primitive do I need?

| Need | Use | Guide |
|------|-----|-------|
| Long-lived processes kept alive, messaged by id, restarted on crash | Supervisor (groups, chains, standalone workers) | [Supervisor Guide](guides/supervisor/SUPERVISOR_GUIDE.md) |
| Short jobs submitted to workers, with retries, backoff and backpressure | Pool | [Pool Guide](guides/pool/POOL_GUIDE.md) |
| Data-transformation steps with per-step retry/skip/branch/resume, no processes | FunctionChain | [FunctionChain Guide](guides/function_chain/FUNCTION_CHAIN_GUIDE.md) |

Supervisors declared in config files: [Config Guide](guides/config/CONFIG_GUIDE.md)
([Quickstart](guides/config/CONFIG_QUICKSTART.md)).

## 60-second quickstart

```elixir
alias SuperWorker.Supervisor, as: Sup

# A supervisor holding one group worker (see the Supervisor Guide).
{:ok, _} = Sup.start_with_config([id: :sup1, num_partitions: 2, link: false])
{:ok, _} = Sup.add_group(:sup1, [id: :group1, restart_strategy: :one_for_one])
{:ok, _} = Sup.add_group_worker(:sup1, :group1, fn -> do_work() end, [id: :w1])

# Or a pool running jobs (see the Pool Guide).
{:ok, _} = SuperWorker.Pool.start_link(name: MyPool, task: fn job -> do_work(job) end, size: 20)
{:ok, result} = SuperWorker.Pool.run(MyPool, job)
```

## Guides

Start with the chooser above, then read in any order:

- [Supervisor Guide](guides/supervisor/SUPERVISOR_GUIDE.md) — groups, chains and standalone workers, fault tolerance, introspection.
- [Pool Guide](guides/pool/POOL_GUIDE.md) — a partitioned job pool: run/cast, retries with backoff, middleware, telemetry.
- [FunctionChain Guide](guides/function_chain/FUNCTION_CHAIN_GUIDE.md) — composable in-process pipelines with retry, branches and checkpoint/resume.
- [Config Guide](guides/config/CONFIG_GUIDE.md) — declarative supervisor trees in config files ([Quickstart](guides/config/CONFIG_QUICKSTART.md)).

Full API reference ships with the docs (`mix docs`) and in the Hex package.

## Features

- **Supervisor** — groups (broadcast + direct messaging), ordered chains with
  backpressure, and standalone workers, each with its own restart strategy.
- **Pool** — partitioned workers and bounded queues, retries with backoff,
  crash isolation with poison-pill safety, and pluggable middleware
  (telemetry, circuit breaker).
- **FunctionChain** — in-process pipelines with per-step error policies,
  parallel/branch steps, runtime argument overrides, and checkpoint/resume.
- **ConfigLoader** — declare supervisors, groups, chains and workers in
  `config.exs` and boot them automatically.
- **Introspection** — list supervisors, groups, chains and workers at runtime.

## Planned features

- Multiprocess per chain node.
- Auto scale for chain.
- Distributed in cluster.
