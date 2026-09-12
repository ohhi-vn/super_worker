# SuperWorker FunctionChain Guide

`SuperWorker.FunctionChain` is a composable function pipeline library. Each
step is a function (MFA, anonymous function, or captured function with bound
args) that receives the previous step's result and returns
`{:ok, result} | {:error, reason}`.

Unlike the process-based `Chain` of `SuperWorker.Supervisor`, a FunctionChain
runs **in the calling process** — no supervision tree to design. It supports
per-step retry/ignore/skip policies, runtime-overridable arguments,
parallel/branching execution, conditional skipping, telemetry, optional
tracing, and optional checkpoint/resume.

## Overview

- **Steps** — functions receiving the previous result, in order.
- **Error strategies** — `:halt`, `:retry` or `:ignore`, per step or chain-wide.
- **Parallel & branches** — fan-out/fan-in and exclusive routing sub-chains.
- **Dynamic args** — override step arguments at run time without touching the
  chain definition.
- **Checkpoint/resume** — persist progress on failure and resume mid-retry.

## Quick start

```elixir
chain =
  SuperWorker.FunctionChain.new(log: true, telemetry: true)
  |> SuperWorker.FunctionChain.add(:fetch_user, {MyApp.Users, :fetch, []})
  |> SuperWorker.FunctionChain.add(:normalize, fn user -> {:ok, normalize(user)} end)
  |> SuperWorker.FunctionChain.add(:enrich, {&MyApp.Enricher.call/2, [source: :external]},
       on_error: :retry, max_retries: 3, retry_delay: 200
  )
  |> SuperWorker.FunctionChain.add(:save, {MyApp.Repo, :insert, []}, on_error: :ignore)

{:ok, result} = SuperWorker.FunctionChain.run(chain, %{id: 123})
```

Chain options (`new/1`):

| Option             | Default | Meaning                                              |
|--------------------|---------|------------------------------------------------------|
| `:id`              | ref     | Chain identifier                                     |
| `:log`             | `false` | Debug logging for steps                              |
| `:telemetry`       | `false` | Emit `[:function_chain, *]` telemetry events         |
| `:on_error`        | `:halt` | Chain-wide error strategy                            |
| `:max_retries`     | `3`     | Default retry budget per step                        |
| `:retry_delay`     | `0`     | Milliseconds between retry attempts                  |
| `:rescue_exceptions` | `true`| Rescue raised exceptions into `{:error, ...}`        |
| `:return_context`  | `false` | Return `{:error, reason, meta}` on failure           |
| `:tracing`         | `false` | Create spans per step (see `Tracer`)                 |
| `:store`           | `nil`   | Checkpoint store module for resumable chains         |

## Function representation

The incoming value is always prepended as the first argument. Everything else
in a step's `args` is "extra" and is exactly what runtime `arg_overrides`
replace.

| Form             | Example                        | Invocation                            |
|------------------|--------------------------------|---------------------------------------|
| MFA              | `{MyMod, :fun, [extra1]}`      | `apply(MyMod, :fun, [value, extra1])` |
| Anonymous fun    | `fn value -> {:ok, value} end` | `fun.(value)`                         |
| Fun + bound args | `{&MyMod.fun/2, [extra1]}`     | `apply(fun, [value, extra1])`         |

Arity is validated at `add/3` time so a mismatch fails fast.

## Step options

```elixir
SuperWorker.FunctionChain.add(chain, :step_name, fun_spec,
  args: [],                                # static extra args
  on_error: :halt | :retry | :ignore,      # inherits chain default when nil
  max_retries: 3, retry_delay: 200,        # inherit chain defaults when nil
  on_retry_exhausted: :halt | :ignore,     # default: :halt
  when: fn value -> ... end,               # skip step when predicate is falsy
  unless: fn value -> ... end,             # skip step when predicate is truthy
  on_skip: :pass_through | :error | {:replace, fun},  # default: :pass_through
  log: true, telemetry: true               # nil inherits chain default
)
```

Setting both `:when` and `:unless` raises.

## Error handling strategies

| Strategy          | Behavior on failure                                                  |
|-------------------|----------------------------------------------------------------------|
| `:halt` (default) | Stop chain, return `{:error, reason}`                                |
| `:retry`          | Retry the same step with its original input up to `max_retries`, waiting `retry_delay` ms between attempts, then fall back to `on_retry_exhausted`. A step runs at most `1 + max_retries` times |
| `:ignore`         | Forward the previous value unchanged, continue the chain             |

Raised exceptions are rescued by default (`rescue_exceptions: true`) and
normalized to `{:error, {:exception, exception, stacktrace}}`, then handled
through the same `on_error` logic. Any return other than `{:ok, _}` /
`{:error, _}` becomes `{:error, {:invalid_return, value}}`.

Return value contract:

- Success: `{:ok, final_result}`
- Failure: `{:error, reason}`
- Failure with `return_context: true`: `{:error, reason, %{step: id, index: i, attempts: n}}`

## Runtime argument overrides

Per-run overrides of a step's extra (non-data) arguments, without touching the
chain definition — safe for concurrent runs of a shared chain:

```elixir
SuperWorker.FunctionChain.run(chain, order,
  arg_overrides: %{
    charge_card: {:replace, [gateway: :stripe_test]},
    send_email:  {:merge,   [subject: "Reactivation"]}
  })
```

- A bare list (`send_email: [subject: "..."]`) is shorthand for `:replace`.
- `:merge` is only valid when both the step's static args and the override are
  keyword lists (`Keyword.merge/2` with the override winning); it falls back to
  `:replace` when either side is not a keyword list.
- Unknown keys are logged as a warning and silently ignored — they never raise.

There is also a build-time variant that returns a new chain, leaving the
original untouched:

```elixir
staging_chain = SuperWorker.FunctionChain.override_args(chain, :charge_card, [gateway: :stripe_test])
```

## Dynamic argument markers

Static `args` entries may be dynamic markers, resolved just before invocation:

- `{:context, key}` — value from the run-level context map (`run_opts[:context]`)
- `{:from_step, step_id}` — a prior step's result
- `{:call, fun}` — `fun.(context)` or `fun.(value, context)` computed lazily

```elixir
chain =
  SuperWorker.FunctionChain.new()
  |> SuperWorker.FunctionChain.add(:fetch, {MyApp.Repo, :get_user, []})
  |> SuperWorker.FunctionChain.add(:email,
       {MyApp.Mailer, :send, []},
       args: [template: {:context, :template}, user_id: {:from_step, :fetch}])

{:ok, _} = SuperWorker.FunctionChain.run(chain, 42, context: %{template: :reactivation})
```

## Parallel & branch steps

Fan the same input out to several sub-chains concurrently and join the results:

```elixir
fetch_a = SuperWorker.FunctionChain.new() |> SuperWorker.FunctionChain.add(:a, {MyApp.API, :a, []})
fetch_b = SuperWorker.FunctionChain.new() |> SuperWorker.FunctionChain.add(:b, {MyApp.API, :b, []})

chain =
  SuperWorker.FunctionChain.new()
  |> SuperWorker.FunctionChain.add_parallel(:fanout,
       [api_a: fetch_a, api_b: fetch_b],
       join: :map,                        # :list | :map | custom fun/1
       on_branch_error: :ignore_failed,   # :halt_all | :ignore_failed | :collect_errors
       max_concurrency: 4, timeout: 5_000
     )
  |> SuperWorker.FunctionChain.add(:merge, fn results -> {:ok, Map.merge(results["api_a"], results["api_b"])} end)
```

Exclusive routing — the first matching route runs, or `:default`:

```elixir
chain =
  SuperWorker.FunctionChain.new()
  |> SuperWorker.FunctionChain.add_branch(:router,
       [
         {&(&1.amount > 100), big_order_chain},
         {&(&1.amount <= 100), small_order_chain}
       ],
       default: fallback_chain
     )
```

If no route matches and there is no `:default`, the step fails with
`{:no_matching_branch, value}`.

## Checkpoint / resume

Configure a `store` (e.g. `SuperWorker.FunctionChain.Store.ETS`) and checkpoints
are written only on a `:halt` failure, and deleted on eventual success:

```elixir
chain =
  SuperWorker.FunctionChain.new(store: SuperWorker.FunctionChain.Store.ETS)
  |> SuperWorker.FunctionChain.add(:charge, {MyApp.Billing, :charge, []}, on_error: :halt)
  |> SuperWorker.FunctionChain.add(:email, {MyApp.Mailer, :send, []})

{:error, reason} =
  SuperWorker.FunctionChain.run(chain, order, run_id: "order-123")

# Later — resumes from the failed step (retry attempt count is part of the
# checkpoint, so resume continues mid-retry correctly).
{:ok, result} =
  SuperWorker.FunctionChain.resume(chain, run_id: "order-123", store: SuperWorker.FunctionChain.Store.ETS)
```

Only MFA-only chains are resumable by default — chains with closures raise
`SuperWorker.FunctionChain.NotResumableError` on `resume/2` unless a
`:step_resolver` maps each non-MFA step id to a serializable MFA fun_spec:

```elixir
SuperWorker.FunctionChain.resume(chain, run_id: "order-123",
  store: SuperWorker.FunctionChain.Store.ETS,
  step_resolver: fn
    :charge_card -> {MyApp.Billing, :charge, []}
    _ -> nil
  end)
```

`resume/2` returns `{:error, {:no_checkpoint, run_id}}` when the store has no
checkpoint for the given run.

## Telemetry & tracing

Set `telemetry: true` to emit `[:function_chain, :run, :start | :stop]` events;
set `tracing: true` for per-step spans via `SuperWorker.FunctionChain.Tracer`.

## Running chains through the pool / supervisor

Chains run in the calling process by default, but the library's process
machinery can fan them out:

- **Through the Pool** — `SuperWorker.Pool.FunctionChain` runs the chain for
  every pool job, distributing runs over partitions with the pool's retries,
  backpressure and `:on_failure`:

  ```elixir
  {:ok, _} =
    SuperWorker.Pool.start_link(
      name: MyChainPool,
      worker: SuperWorker.Pool.FunctionChain,
      worker_opts: [chain: chain, on_error: :retry],
      size: 20
    )

  {:ok, result} = SuperWorker.Pool.run(MyChainPool, order)
  ```

- **In the Supervisor** — `SuperWorker.Supervisor.FunctionChain`:
  `chain_node_fun/2` runs the chain as one node of a process chain
  (`Sup.add_chain_worker/4`), and `job_loop/2` turns a standalone/group
  worker into a chain-serving loop (`Sup.send_to_standalone_worker/3` with
  `{:run, ref, job, from}` for request/response).

See the [Pool Guide](../pool/POOL_GUIDE.md) and
[Supervisor Guide](../supervisor/SUPERVISOR_GUIDE.md) for the full context.

## Next steps

- [Supervisor Guide](../supervisor/SUPERVISOR_GUIDE.md) — process-based chains and groups.
- [Pool Guide](../pool/POOL_GUIDE.md) — a partitioned job pool for submitting jobs.
