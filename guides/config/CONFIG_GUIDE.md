# SuperWorker ConfigLoader

The ConfigLoader module provides a declarative way to configure and automatically start SuperWorker supervisors from the application environment. Instead of manually starting supervisors and adding workers programmatically, you can define your entire supervision tree in configuration files.

## Overview

The ConfigLoader system consists of three main modules:

- **`ConfigParser`** - Main entry point that loads configurations from the application environment
- **`Parser`** - Validates and transforms raw configurations into structured data
- **`Bootstrap`** - Starts supervisors and their children from parsed configurations

## Quick Start

### 1. Define Configuration

Add supervisor configurations to your `config/config.exs` or `config/runtime.exs`:

```elixir
import Config

config :super_worker, :my_supervisor,
  options: [
    number_of_partitions: 2,
    link: false
  ],
  groups: [
    [
      id: :worker_pool,
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

### 2. Automatic Loading

Supervisors are automatically loaded when your application starts via the `SuperWorker.Application` module.

### 3. Manual Loading

You can also load supervisors manually:

```elixir
# Load all configured supervisors
SuperWorker.ConfigLoader.ConfigParser.load()

# Load a specific supervisor
SuperWorker.ConfigLoader.ConfigParser.load_one(:my_supervisor)
```

## Configuration Format

### Supervisor Options

```elixir
options: [
  number_of_partitions: 2,  # Number of partitions (default: number of schedulers)
  link: false,              # Whether to link supervisor to caller
  report_to: [],            # List of PIDs to report events to
  strategy: :one_for_one    # Supervisor strategy (optional)
]
```

### Groups

Groups are collections of workers that share a restart strategy:

```elixir
groups: [
  [
    id: :my_group,                    # Required: Unique atom identifier
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
chains: [
  [
    id: :processing_chain,              # Required: Unique atom identifier
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

### Standalone Workers

Independent workers with their own restart strategies:

```elixir
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

### Worker Specifications

Workers can be specified in two ways:

#### Using MFA (Module, Function, Arguments)

```elixir
[
  mfa: {MyApp.Workers.EmailWorker, :start_link, []},
  options: [id: :email_worker]
]
```

#### Using Anonymous Functions

```elixir
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

**Note:** Anonymous functions must have 0 arity.

## Complete Example

```elixir
config :super_worker, :app_supervisor,
  options: [
    number_of_partitions: 4,
    link: false
  ],
  groups: [
    [
      id: :api_handlers,
      restart_strategy: :one_for_one,
      workers: [
        [mfa: {MyApp.API.Handler, :start_link, []}, options: [id: :handler_1]],
        [mfa: {MyApp.API.Handler, :start_link, []}, options: [id: :handler_2]]
      ]
    ]
  ],
  chains: [
    [
      id: :order_pipeline,
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

## Module Details

### ConfigParser

Main entry point for loading configurations.

**Functions:**

- `load/0` - Loads all supervisor configurations from application environment
- `load_one(sup_id)` - Loads a specific supervisor configuration

**Returns:**

- `load/0` returns `:ok`
- `load_one/1` returns `{:ok, pid}` or `{:error, reason}`

### Parser

Validates and transforms raw configuration into structured format.

**Functions:**

- `parse(config)` - Parses and validates a configuration

**Returns:**

- `{:ok, parsed_config}` - Successfully parsed
- `{:error, reason}` - Validation failed

**Validation:**

- Ensures all required fields are present
- Validates restart strategies
- Validates send types for chains
- Checks that IDs are atoms
- Verifies MFA tuples are well-formed

### Bootstrap

Starts supervisors from parsed configurations.

**Functions:**

- `start_supervisor(parsed_config)` - Starts a supervisor with parsed config

**Returns:**

- `{:ok, pid}` - Supervisor started successfully
- `{:error, reason}` - Failed to start

**Behavior:**

- Starts the supervisor process
- Adds all groups, chains, and workers
- Performs cleanup if children fail to start

## Restart Strategies

### Group Restart Strategies

- **`:one_for_one`** - Only restart the failed worker
- **`:one_for_all`** - Restart all workers in the group when one fails

### Chain Restart Strategies

- **`:one_for_one`** - Only restart the failed worker
- **`:one_for_all`** - Restart all workers in the chain
- **`:rest_for_one`** - Restart the failed worker and all workers after it

### Standalone Restart Strategies

- **`:permanent`** - Always restart the worker
- **`:transient`** - Restart only on abnormal termination
- **`:temporary`** - Never restart the worker

## Chain Send Types

- **`:broadcast`** - Send data to all workers at the current level
- **`:random`** - Send to a randomly selected worker
- **`:partition`** - Use consistent hashing based on data
- **`:round_robin`** - Distribute messages evenly across workers

## Best Practices

1. **Use Descriptive IDs**: Choose clear, meaningful atom IDs for all supervisors, groups, chains, and workers

2. **Start Simple**: Begin with basic configurations and add complexity as needed

3. **Environment-Specific Configs**: Use different configurations for dev/test/prod environments:
   ```elixir
   # config/dev.exs
   config :super_worker, :my_sup, options: [number_of_partitions: 1]

   # config/prod.exs
   config :super_worker, :my_sup, options: [number_of_partitions: 8]
   ```

4. **Validate Early**: Invalid configurations are caught during parsing, so test your configs in development

5. **Use Groups for Related Workers**: Group workers that need to work together or share restart behavior

6. **Use Chains for Pipelines**: Chains are ideal for data processing pipelines with sequential steps

7. **Monitor Startup**: Check logs during application startup to ensure supervisors start correctly

## Troubleshooting

### Configuration Not Found

```
{:error, :config_not_found}
```

**Solution:** Ensure configuration exists in application environment:
```elixir
Application.get_env(:super_worker, :my_supervisor)
```

### Invalid Configuration Format

```
{:error, :invalid_config_format}
```

**Solution:** Configuration must be a keyword list. Check syntax in config file.

### Missing Required Field

```
{:error, {:missing_id, ...}}
```

**Solution:** All groups, chains, and workers must have an `:id` field.

### Invalid Restart Strategy

Configuration will use defaults, but check logs for warnings:
```
SuperWorker, Parser, invalid restart_strategy: ...
```

**Solution:** Use valid restart strategies (see above).

### Supervisor Already Running

```
{:error, :already_running}
```

**Solution:** Stop the supervisor first or use a different ID:
```elixir
SuperWorker.Supervisor.stop(:my_supervisor)
```

### Worker Module Not Found

```
{:error, {:workers_parsing_errors, ...}}
```

**Solution:** Ensure all worker modules exist and are compiled before the supervisor starts.

## Examples

See `config/examples/config_loader_example.exs` for comprehensive examples covering:

- Simple supervisors with groups
- Chain-based processing pipelines
- Standalone workers
- Complex multi-type supervisors
- Development/testing configurations

## Testing

Run the ConfigLoader tests:

```bash
mix test test/config_loader/
```

Specific test files:
- `test/config_loader/parser_test.exs` - Parser validation tests
- `test/config_loader/bootstrap_test.exs` - Bootstrap functionality tests
- `test/config_loader/config_wrapper_test.exs` - Integration tests

## API Reference

For detailed API documentation, see:

```bash
mix docs
```

Then open `doc/index.html` and navigate to the ConfigLoader modules.

## Integration with SuperWorker.Application

The ConfigLoader is automatically invoked in `SuperWorker.Application.start/2`:

```elixir
def start(_type, _args) do
  SuperWorker.ConfigLoader.ConfigParser.load()

  children = []
  Supervisor.start_link(children, strategy: :one_for_one, name: SuperWorker.Supervisor)
end
```

All configured supervisors are started before the main application supervisor.

## License

This module is part of SuperWorker, licensed under MPL-2.0.
