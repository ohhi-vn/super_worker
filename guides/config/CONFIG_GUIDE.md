# SuperWorker ConfigLoader Guide

The ConfigLoader module provides a declarative way to configure and automatically start SuperWorker supervisors from the application environment. Instead of manually starting supervisors and adding workers programmatically, you can define your entire supervision tree in configuration files.

For the 5-minute path, start with the
[Config Quickstart](CONFIG_QUICKSTART.md). This guide is the full format
reference. For what groups, chains, and restart strategies mean, see the
[Supervisor Guide](../supervisor/SUPERVISOR_GUIDE.md).

## Overview

The ConfigLoader system consists of three modules:

- **`ConfigParser`** — main entry point; loads configurations from the
  application environment (`load/0` loads all, `load_one/1` loads one).
- **`Parser`** — validates and transforms raw configurations into structured data.
- **`Bootstrap`** — starts supervisors and their children from parsed configurations.

## When to use

Use config declaration when your supervision tree is **static and known at
deploy time**: declare it in `config/config.exs` or `config/runtime.exs` and
it boots with the application. Use the runtime API (see the
[Supervisor Guide](../supervisor/SUPERVISOR_GUIDE.md#quick-start)) when workers
must be added dynamically in response to runtime events.

## Quick start

### 1. Define configuration

Add supervisor configurations to your `config/config.exs` or `config/runtime.exs`:

```elixir
import Config

config :super_worker, :my_supervisor,
  options: [
    num_partitions: 2,
    link: false
  ],
  groups: [
    worker_pool: [
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

### 2. Automatic loading

Supervisors are automatically loaded when your application starts via the `SuperWorker.Application` module:

```elixir
def start(_type, _args) do
  SuperWorker.ConfigLoader.ConfigParser.load()

  children = []
  Supervisor.start_link(children, strategy: :one_for_one, name: SuperWorker.MainAppSupervisor)
end
```

All configured supervisors are started before the main application supervisor.

### 3. Manual loading

You can also load supervisors manually:

```elixir
# Load all configured supervisors
SuperWorker.ConfigLoader.ConfigParser.load()

# Load a specific supervisor
SuperWorker.ConfigLoader.ConfigParser.load_one(:my_supervisor)
```

`load/0` returns `:ok`; `load_one/1` returns `{:ok, pid}` or `{:error, reason}`.

## Core concepts

Each supervisor has one master process plus a number of partition processes.
When a supervisor API is called, the master picks a partition and forwards the
request to it, so a high volume of workers/requests does not bottleneck a single
process.

Worker, group, chain and runtime information is kept in an ETS table shared by
the partitions. See the [Supervisor Guide](../supervisor/SUPERVISOR_GUIDE.md)
for the full picture.

## Reference

### Supervisor options

```elixir
config :super_worker, :my_supervisor,
  options: [
    num_partitions: 2,  # Number of partitions (default: number of schedulers)
    link: false,              # Whether to link supervisor to caller
    report_to: [],            # List of PIDs to report events to
    strategy: :one_for_one    # Supervisor strategy (optional)
  ]
```

### Groups

Groups are collections of workers that share a restart strategy:

```elixir
config :super_worker, :my_supervisor,
  groups: [
    my_group: [
      restart_strategy: :one_for_one,   # :one_for_one or :one_for_all
      type: :normal,                    # Optional: group type
      max_restarts: 3,                  # Optional: max restarts
      max_seconds: 5,                   # Optional: time window for restarts
      workers: [
        # Worker specifications (see below)
      ]
    ]
  ]
```

### Chains

Chains enable sequential data processing where output flows from one worker to the next:

```elixir
config :super_worker, :my_supervisor,
  chains: [
    processing_chain: [
      restart_strategy: :rest_for_one,    # :one_for_one, :one_for_all, :rest_for_one
      send_type: :round_robin,            # :broadcast, :random, :partition, :round_robin
      queue_length: 100,                  # Optional: max queue size
      finished_callback: {M, :f, [a]},    # Optional: callback when chain completes
      workers: [
        # Worker specifications (see below)
      ]
    ]
  ]
```

### Standalone workers

Independent workers with their own restart strategies:

```elixir
config :super_worker, :my_supervisor,
  workers: [
    [
      mfa: {MyModule, :start_link, [arg1, arg2]},  # Worker MFA
      options: [
        id: :my_worker,                # Required: Unique atom identifier
        restart_strategy: :permanent,  # :permanent, :transient, :temporary
        max_restarts: 5,               # Optional
        max_seconds: 10                # Optional
      ]
    ]
  ]
```

### Worker specifications

Workers can be specified in two ways:

```elixir
# Using MFA (Module, Function, Arguments)
[
  mfa: {MyApp.Workers.EmailWorker, :start_link, []},
  options: [id: :email_worker]
]
```

```elixir
# Using an anonymous function (must have 0 arity)
[
  fun: fn ->
    # Worker loop
    receive do
      msg -> IO.puts("Received: #{inspect(msg)}")
    end
  end,
  options: [id: :function_worker]
]
```

### Complete example

```elixir
config :super_worker, :app_supervisor,
  options: [
    num_partitions: 4,
    link: false
  ],
  groups: [
    api_handlers: [
      restart_strategy: :one_for_one,
      workers: [
        [mfa: {MyApp.API.Handler, :start_link, []}, options: [id: :handler_1]],
        [mfa: {MyApp.API.Handler, :start_link, []}, options: [id: :handler_2]]
      ]
    ]
  ],
  chains: [
    order_pipeline: [
      restart_strategy: :rest_for_one,
      send_type: :partition,
      workers: [
        [mfa: {MyApp.Orders.Validator, :start_link, []}, options: [id: :validator]],
        [mfa: {MyApp.Orders.Processor, :start_link, []}, options: [id: :processor]],
        [mfa: {MyApp.Orders.Notifier, :start_link, []}, options: [id: :notifier]]
      ]
    ]
  ],
  workers: [
    [
      mfa: {MyApp.HealthChecker, :start_link, []},
      options: [id: :health, restart_strategy: :permanent]
    ]
  ]
```

### Loading and validation

`Parser.parse/1` validates raw configuration and returns
`{:ok, parsed_config}` or `{:error, reason}`. It checks that all required
fields are present, restart strategies and chain send types are valid, IDs are
atoms, and MFA tuples are well-formed.

`Bootstrap.start_supervisor/1` starts the supervisor process, adds all groups,
chains, and workers, and performs cleanup if children fail to start. It
returns `{:ok, pid}` or `{:error, reason}`.

## Error handling

Process restart strategies are defined in the
[Supervisor Guide](../supervisor/SUPERVISOR_GUIDE.md#error-handling):
`:one_for_one` / `:one_for_all` for groups, plus `:rest_for_one` for chains,
and `:permanent` / `:transient` / `:temporary` for standalone workers. Use
those names verbatim in config; invalid values fall back to defaults with a
parser warning (see Troubleshooting below).

### Troubleshooting

#### Configuration not found

```
{:error, :config_not_found}
```

**Solution:** Ensure configuration exists in application environment:
```elixir
Application.get_env(:super_worker, :my_supervisor)
```

#### Invalid configuration format

```
{:error, :invalid_config_format}
```

**Solution:** Configuration must be a keyword list. Check syntax in config file.

#### Missing required field

```
{:error, {:missing_id, ...}}
```

**Solution:** All groups, chains, and workers must have an `:id` field.

#### Invalid restart strategy

Configuration will use defaults, but check logs for warnings:
```
SuperWorker, Parser, invalid restart_strategy: ...
```

**Solution:** Use valid restart strategies (see above).

#### Supervisor already running

```
{:error, :already_running}
```

**Solution:** Stop the supervisor first or use a different ID:
```elixir
SuperWorker.Supervisor.stop(:my_supervisor)
```

#### Worker module not found

```
{:error, {:workers_parsing_errors, ...}}
```

**Solution:** Ensure all worker modules exist and are compiled before the supervisor starts.

## Introspection and observability

Runtime control after boot:

```elixir
# Load all configured supervisors / a specific one
SuperWorker.ConfigLoader.ConfigParser.load()
SuperWorker.ConfigLoader.ConfigParser.load_one(:my_supervisor)

# Check if running, then stop
SuperWorker.Supervisor.running?(:my_supervisor)
SuperWorker.Supervisor.stop(:my_supervisor)
```

Monitor startup logs to confirm supervisors start correctly, and test configs
in development — invalid configurations are caught during parsing.

### Testing

Run the ConfigLoader tests:

```bash
mix test test/config_loader/
```

Specific test files:
- `test/config_loader/parser_test.exs` - Parser validation tests
- `test/config_loader/bootstrap_test.exs` - Bootstrap functionality tests
- `test/config_loader/config_wrapper_test.exs` - Integration tests

## Next steps

- [Config Quickstart](CONFIG_QUICKSTART.md) — the 5-minute path.
- [Supervisor Guide](../supervisor/SUPERVISOR_GUIDE.md) — groups, chains and standalone workers.
- See `config/examples/config_loader_example.exs` for comprehensive examples covering
  simple supervisors, chain pipelines, standalone workers, and dev/test configs.
- For detailed module API documentation, run `mix docs` and open `doc/index.html`.
