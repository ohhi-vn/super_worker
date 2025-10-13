# ConfigLoader Quick Start Guide

Get started with SuperWorker's ConfigLoader in 5 minutes.

## What is ConfigLoader?

ConfigLoader automatically starts SuperWorker supervisors from your application configuration files. Define your supervision tree in `config.exs` instead of writing code.

## Basic Setup

### 1. Add Configuration

In `config/config.exs`:

```elixir
import Config

config :super_worker, :my_app_supervisor,
  options: [
    number_of_partitions: 2,
    link: false
  ],
  groups: [
    [
      id: :api_workers,
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

### 2. That's It!

The supervisor starts automatically when your application boots. No additional code needed.

## Common Patterns

### Pattern 1: Background Job Workers

```elixir
config :super_worker, :job_supervisor,
  options: [number_of_partitions: 4, link: false],
  groups: [
    [
      id: :job_workers,
      restart_strategy: :one_for_one,
      workers: [
        [mfa: {MyApp.Jobs.EmailWorker, :start_link, []}, options: [id: :email]],
        [mfa: {MyApp.Jobs.ImageWorker, :start_link, []}, options: [id: :image]]
      ]
    ]
  ]
```

### Pattern 2: Processing Pipeline (Chain)

```elixir
config :super_worker, :pipeline_supervisor,
  options: [number_of_partitions: 2, link: false],
  chains: [
    [
      id: :data_pipeline,
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

### Pattern 3: Mixed Workers

```elixir
config :super_worker, :app_supervisor,
  options: [number_of_partitions: 4, link: false],
  groups: [
    [id: :api_group, restart_strategy: :one_for_one, workers: [...]]
  ],
  chains: [
    [id: :processing_chain, restart_strategy: :rest_for_one, workers: [...]]
  ],
  workers: [
    # Standalone workers with individual restart strategies
    [
      mfa: {MyApp.Cache, :start_link, []},
      options: [id: :cache, restart_strategy: :permanent]
    ]
  ]
```

## Worker Requirements

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

## Runtime Control

Load supervisors manually:

```elixir
# Load all configured supervisors
SuperWorker.ConfigLoader.ConfigWrapper.load()

# Load a specific supervisor
SuperWorker.ConfigLoader.ConfigWrapper.load_one(:my_app_supervisor)
```

Check if running:

```elixir
SuperWorker.Supervisor.is_running?(:my_app_supervisor)
```

Stop a supervisor:

```elixir
SuperWorker.Supervisor.stop(:my_app_supervisor)
```

## Configuration Options

### Supervisor Options

- `number_of_partitions` - Number of partitions (default: CPU cores)
- `link` - Link to caller process (true/false)
- `report_to` - List of PIDs for event reporting

### Group Options

- `id` - **Required**. Unique atom identifier
- `restart_strategy` - `:one_for_one` or `:one_for_all`

### Chain Options

- `id` - **Required**. Unique atom identifier
- `restart_strategy` - `:one_for_one`, `:one_for_all`, `:rest_for_one`, `:before_for_one`
- `send_type` - `:broadcast`, `:random`, `:partition`, `:round_robin`
- `queue_length` - Max queue size

### Worker Options

- `id` - **Required**. Unique atom identifier
- `restart_strategy` - (Standalone only) `:permanent`, `:transient`, `:temporary`

## Environment-Specific Config

```elixir
# config/dev.exs
import Config
config :super_worker, :my_supervisor,
  options: [number_of_partitions: 1, link: false]

# config/prod.exs
import Config
config :super_worker, :my_supervisor,
  options: [number_of_partitions: 8, link: false]
```

## Troubleshooting

**Supervisor not starting?**
- Check logs for parsing errors
- Ensure all required fields (`:id`) are present
- Verify worker modules exist and are compiled

**Workers crashing?**
- Ensure worker `start_link` returns `{:ok, pid}`
- Check worker implementation doesn't exit immediately
- Review restart strategy settings

## Next Steps

- Read [README.md](README.md) for complete documentation
- See [config/examples/config_loader_example.exs](../../config/examples/config_loader_example.exs) for more examples
- Check [tests](../../../test/config_loader/) for usage patterns

## Quick Reference

| Component | Config Key | Required Fields |
|-----------|-----------|-----------------|
| Supervisor | Top-level atom | `:options` with `:id` |
| Group | `:groups` list | `:id` |
| Chain | `:chains` list | `:id` |
| Standalone | `:workers` list | `:id`, `:restart_strategy` |
| Worker (in group/chain) | `:workers` list | `:mfa` or `:fun`, `:options` with `:id` |

## Help

Questions? Check the [full README](README.md) or the test files for examples.