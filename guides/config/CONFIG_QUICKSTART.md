# ConfigLoader Quick Start Guide

Get started with SuperWorker's ConfigLoader in 5 minutes. It automatically
starts SuperWorker supervisors from your application configuration files —
define your supervision tree in `config.exs` instead of writing code.

New to the concepts (groups, chains, standalone workers, restart strategies)?
Read the [Supervisor Guide](../supervisor/SUPERVISOR_GUIDE.md) first; this
guide only shows the config shapes. Full option reference lives in the
[Config Guide](CONFIG_GUIDE.md).

## Basic setup

In `config/config.exs`:

```elixir
import Config

config :super_worker, :my_app_supervisor,
  options: [
    num_partitions: 2,
    link: false
  ],
  groups: [
    api_workers: [
      restart_strategy: :one_for_one,
      workers: [
        [
          mfa: {MyApp.Worker, :start_link, []},
          options: [id: :worker_1]
        ]
      ]
    ]
  ]
```

That's it — the supervisor starts automatically when your application boots.
No additional code needed.

## Common patterns

### Pattern 1: Background job workers

```elixir
config :super_worker, :job_supervisor,
  options: [num_partitions: 4, link: false],
  groups: [
    job_workers: [
      restart_strategy: :one_for_one,
      workers: [
        [mfa: {MyApp.Jobs.EmailWorker, :start_link, []}, options: [id: :email]],
        [mfa: {MyApp.Jobs.ImageWorker, :start_link, []}, options: [id: :image]]
      ]
    ]
  ]
```

### Pattern 2: Processing pipeline (chain)

```elixir
config :super_worker, :pipeline_supervisor,
  options: [num_partitions: 2, link: false],
  chains: [
    data_pipeline: [
      restart_strategy: :rest_for_one,
      send_type: :round_robin,
      workers: [
        [mfa: {MyApp.Validate, :start_link, []}, options: [id: :step1]],
        [mfa: {MyApp.Transform, :start_link, []}, options: [id: :step2]],
        [mfa: {MyApp.Save, :start_link, []}, options: [id: :step3]]
      ]
    ]
  ]
```

### Pattern 3: Mixed workers

```elixir
config :super_worker, :app_supervisor,
  options: [num_partitions: 4, link: false],
  groups: [
    api_group: [restart_strategy: :one_for_one, workers: [...]]
  ],
  chains: [
    processing_chain: [restart_strategy: :rest_for_one, workers: [...]]
  ],
  workers: [
    # Standalone workers with individual restart strategies
    [
      mfa: {MyApp.Cache, :start_link, []},
      options: [id: :cache, restart_strategy: :permanent]
    ]
  ]
```

## Worker requirements

Your worker modules must implement `start_link/0` (or similar arity) and return:

```elixir
defmodule MyApp.Worker do
  def start_link do
    # Start your worker process
    pid = spawn(fn -> worker_loop() end)
    {:ok, pid}
  end

  defp worker_loop do
    receive do
      msg ->
        # Handle message
        worker_loop()
    end
  end
end
```

## Runtime control

Load supervisors manually:

```elixir
# Load all configured supervisors
SuperWorker.ConfigLoader.ConfigParser.load()

# Load a specific supervisor
SuperWorker.ConfigLoader.ConfigParser.load_one(:my_app_supervisor)
```

Check if running:

```elixir
SuperWorker.Supervisor.running?(:my_app_supervisor)
```

Stop a supervisor:

```elixir
SuperWorker.Supervisor.stop(:my_app_supervisor)
```

## Environment-specific config

```elixir
# config/dev.exs
import Config
config :super_worker, :my_supervisor,
  options: [num_partitions: 1, link: false]

# config/prod.exs
import Config
config :super_worker, :my_supervisor,
  options: [num_partitions: 8, link: false]
```

## Troubleshooting pointers

**Supervisor not starting?** Check logs for parsing errors, ensure every
group/chain/worker has an `:id`, and verify worker modules exist and are
compiled.

**Workers crashing on boot?** Ensure worker `start_link` returns
`{:ok, pid}` and the process doesn't exit immediately.

Full error catalog lives in the [Config Guide](CONFIG_GUIDE.md#troubleshooting).

## Next steps

- Read the [Config Guide](CONFIG_GUIDE.md) for the complete option reference,
  validation rules, and troubleshooting.
- See [config/examples/config_loader_example.exs](../../config/examples/config_loader_example.exs)
  for more examples.
- Check [tests](../../test/config_loader/) for usage patterns.
