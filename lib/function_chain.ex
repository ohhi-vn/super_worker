defmodule SuperWorker.FunctionChain do
  @moduledoc """
  A composable function pipeline library.

  Each step is a function (MFA, anonymous function, or captured function with
  bound args) that receives the previous step's result and returns
  `{:ok, result} | {:error, reason}`. Supports per-step retry / ignore / skip
  policies, runtime-overridable arguments, parallel/branching execution,
  conditional skipping, telemetry, optional tracing, and optional
  checkpoint/resume.

  ## Quick start

      chain =
        SuperWorker.FunctionChain.new(log: true, telemetry: true)
        |> SuperWorker.FunctionChain.add(:fetch_user, {MyApp.Users, :fetch, []})
        |> SuperWorker.FunctionChain.add(:normalize, fn user -> {:ok, normalize(user)} end)
        |> SuperWorker.FunctionChain.add(:enrich, {&MyApp.Enricher.call/2, [source: :external]},
             on_error: :retry, max_retries: 3, retry_delay: 200
        )
        |> SuperWorker.FunctionChain.add(:save, {MyApp.Repo, :insert, []}, on_error: :ignore)

      SuperWorker.FunctionChain.run(chain, %{id: 123})

  ## Function representation

  The incoming value is always prepended as the first argument. Everything else
  in a step's `args` is "extra" and is exactly what `arg_overrides` lets you
  replace at run time.

  | Form             | Example                        | Invocation                            |
  |------------------|--------------------------------|---------------------------------------|
  | MFA              | `{MyMod, :fun, [extra1]}`      | `apply(MyMod, :fun, [value, extra1])` |
  | Anonymous fun    | `fn value -> {:ok, value} end` | `fun.(value)`                         |
  | Fun + bound args | `{&MyMod.fun/2, [extra1]}`     | `apply(fun, [value, extra1])`         |

  Arity is validated at `add/3` time so a mismatch fails fast.

  ## Error handling strategies

  | Strategy          | Behavior on failure                                                                                                                                 |
  |-------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------|
  | `:halt` (default) | Stop chain, return `{:error, reason}`                                                                                                               |
  | `:retry`          | Retry the same step with its original input up to `max_retries`, waiting `retry_delay` ms between attempts, then fall back to `on_retry_exhausted`. The budget counts retries after the initial attempt, so a step runs at most `1 + max_retries` times  |
  | `:ignore`         | Forward the previous value unchanged, continue the chain                                                                                            |

  Raised exceptions are rescued by default (`rescue_exceptions: true`) and
  normalized to `{:error, {:exception, exception, stacktrace}}`, then handled
  through the same `on_error` logic. Any return other than
  `{:ok, _}`/`{:error, _}` becomes `{:error, {:invalid_return, value}}`.

  ## Return value contract

  - Success: `{:ok, final_result}`
  - Failure: `{:error, reason}`
  - Failure with `return_context: true`: `{:error, reason, %{step: id, index: i, attempts: n}}`

  ## Runtime argument overrides

  Per-run overrides of a step's extra (non-data) arguments, without touching
  the chain definition — safe for concurrent runs of a shared chain:

      SuperWorker.FunctionChain.run(chain, order,
        arg_overrides: %{
          charge_card: {:replace, [gateway: :stripe_test]},
          send_email:  {:merge,   [subject: "Reactivation"]}
        })

  A bare list (`send_email: [subject: "..."]`) is shorthand for `:replace`.
  `:merge` is only valid when both the step's static args and the override are
  keyword lists (`Keyword.merge/2` with the override winning on conflicts); it
  falls back to `:replace` when either side is not a keyword list.
  Unknown keys are logged as a warning and silently ignored — they never raise.

  Static `args` entries may also be dynamic markers, resolved just before
  invocation (after whichever of the above supplied the raw value):

  - `{:context, key}` — value from the run-level context map (`run_opts[:context]`)
  - `{:from_step, step_id}` — a prior step's result
  - `{:call, fun}` — `fun.(context)` or `fun.(value, context)` computed lazily

  ## Persistence / resumability

  Checkpoints are written only on a `:halt` failure when a `store` is
  configured, and deleted on eventual success. Only MFA-only chains are
  resumable by default — chains with closures raise
  `SuperWorker.FunctionChain.NotResumableError` on `resume/2` unless a
  `:step_resolver` (see `resume/2`) maps each non-MFA step id to a
  serializable MFA fun_spec. The retry attempt count is part of the
  checkpoint, so resume continues mid-retry correctly.
  """

  alias __MODULE__.{Branch, BranchStep, Checkpoint, ParallelStep, Step}

  require Logger

  defstruct [
    :id,
    steps: [],
    # MapSet of every step id in the chain (incl. nested parallel branches and
    # branch routes), maintained at construction so per-run override validation
    # does not walk the whole chain.
    step_ids: MapSet.new(),
    opts: %{
      log: false,
      telemetry: false,
      on_error: :halt,
      max_retries: 3,
      retry_delay: 0,
      rescue_exceptions: true,
      return_context: false,
      tracing: false,
      store: nil
    }
  ]

  @type t :: %__MODULE__{id: term(), steps: [struct()], step_ids: MapSet.t(), opts: map()}

  @strategies [:halt, :retry, :ignore]
  @on_branch_errors [:halt_all, :ignore_failed, :collect_errors]
  @joins [:list, :map]

  @max_checkable_arity 32

  # ══════════════════════════════════════════════════════════════════════
  # Construction
  # ══════════════════════════════════════════════════════════════════════

  @doc """
  Creates a new, empty chain.

  ## Options

    * `:id` — chain identifier (defaults to a fresh reference)
    * `:log` — enable debug logging for steps (default `false`)
    * `:telemetry` — emit `[:function_chain, *]` telemetry events (default `false`)
    * `:on_error` — chain-wide error strategy, one of `:halt | :retry | :ignore` (default `:halt`)
    * `:max_retries` — default retry budget per step (default `3`)
    * `:retry_delay` — constant delay in ms between retries (default `0`)
    * `:rescue_exceptions` — rescue raised exceptions into errors (default `true`)
    * `:return_context` — return `{:error, reason, context}` on failure (default `false`)
    * `:tracing` — `false` or a module implementing `SuperWorker.FunctionChain.Tracer`
    * `:store` — `nil` or a module implementing `SuperWorker.FunctionChain.Store`
  """
  @spec new(keyword()) :: t()
  def new(opts \\ []) when is_list(opts) do
    {id, opts} = Keyword.pop(opts, :id, make_ref())

    %__MODULE__{
      id: id,
      step_ids: MapSet.new(),
      opts: Map.merge(default_opts(), Map.new(opts))
    }
  end

  @doc """
  Adds a step.

  ## Unnamed form

      add(chain, fun_spec, step_opts \\ [])

  ## Named form

      add(chain, :step_name, fun_spec, step_opts \\ [])

  The 3-argument form is dispatched by shape: if the third argument is a valid
  `fun_spec`, the second argument is the step name; if the second argument is a
  valid `fun_spec` and the third a keyword list of step options, it is the
  unnamed form.

  ## Step options

    * `:args` — static extra args (defaults to the args embedded in `fun_spec`)
    * `:on_error` — `:halt | :retry | :ignore`, inherits chain default when `nil`
    * `:max_retries`, `:retry_delay` — inherit chain defaults when `nil`
    * `:on_retry_exhausted` — `:halt | :ignore` (default `:halt`)
    * `:when` / `:unless` — a `fun/1` predicate over the piped value; the step
      is skipped when `when` is falsy or `unless` is truthy. Setting both raises.
    * `:on_skip` — `:pass_through | :error | {:replace, fun}` (default `:pass_through`)
    * `:log`, `:telemetry` — `nil` (inherit chain default), `true`, or `false`
  """
  @spec add(t(), term(), keyword()) :: t()
  def add(chain = %__MODULE__{}, fun_spec_or_name, fun_spec_or_opts \\ []) do
    cond do
      fun_spec?(fun_spec_or_opts) ->
        do_add(chain, fun_spec_or_name, fun_spec_or_opts, [])

      fun_spec?(fun_spec_or_name) ->
        do_add(chain, make_ref(), fun_spec_or_name, fun_spec_or_opts)

      true ->
        raise ArgumentError,
              "unrecognized fun_spec: #{inspect(fun_spec_or_name)} — expected {mod, fun, args}, " <>
                "a 1-arity fun, or {fun, args}"
    end
  end

  @doc "See `add/3`."
  @spec add(t(), term(), term(), keyword()) :: t()
  def add(chain = %__MODULE__{}, name, fun_spec, step_opts) when is_list(step_opts) do
    do_add(chain, name, fun_spec, step_opts)
  end

  @doc """
  Adds a parallel fan-out/fan-in step: the same input is fanned out to all
  sub-chains concurrently via `Task.async_stream/3` and the results are joined.

  ## Options

    * `:join` — `:list` (default) | `:map` | a `fun/1` receiving
      `[{branch_id, {:ok, value} | {:error, reason}}]`
    * `:on_branch_error` — `:halt_all` (default) | `:ignore_failed` | `:collect_errors`
    * `:max_concurrency` — default `System.schedulers_online/0`
    * `:timeout` — per-branch timeout in ms (default `5_000`)
  """
  @spec add_parallel(t(), term(), [{term(), t()}], keyword()) :: t()
  def add_parallel(chain = %__MODULE__{}, name, branches, opts \\ []) when is_list(opts) do
    branches =
      Enum.map(branches, fn
        {id, sub_chain = %__MODULE__{}} -> %Branch{id: id, name: id, chain: sub_chain}
        other -> raise ArgumentError, "invalid parallel branch: #{inspect(other)}"
      end)

    join = Keyword.get(opts, :join, :list)
    validate_join!(join)

    on_branch_error = Keyword.get(opts, :on_branch_error, :halt_all)
    validate_on_branch_error!(on_branch_error)

    step = %ParallelStep{
      id: name,
      name: name,
      branches: branches,
      join: join,
      on_branch_error: on_branch_error,
      max_concurrency: Keyword.get(opts, :max_concurrency, System.schedulers_online()),
      timeout: Keyword.get(opts, :timeout, 5_000)
    }

    append_step(chain, step)
  end

  @doc """
  Adds an exclusive routing step: the first `{predicate, sub_chain}` route
  whose predicate is truthy on the value runs; the `:default` chain runs if no
  route matches; otherwise the step fails with `{:no_matching_branch, value}`.
  """
  @spec add_branch(t(), term(), [{(term() -> boolean()), t()}], keyword()) :: t()
  def add_branch(chain = %__MODULE__{}, name, routes, opts \\ []) when is_list(opts) do
    Enum.each(routes, fn
      {pred, %__MODULE__{}} when is_function(pred, 1) -> :ok
      other -> raise ArgumentError, "invalid branch route: #{inspect(other)}"
    end)

    step = %BranchStep{
      id: name,
      name: name,
      routes: routes,
      default: Keyword.get(opts, :default)
    }

    %{chain | steps: chain.steps ++ [step]}
  end

  @doc """
  Build-time argument override — returns a new chain, the original untouched.

      staging_chain = FunctionChain.override_args(chain, :charge_card, [gateway: :stripe_test])
  """
  @spec override_args(t(), term(), [term()], :replace | :merge) :: t()
  def override_args(chain = %__MODULE__{}, step_id, args, strategy \\ :replace)
      when is_list(args) and strategy in [:replace, :merge] do
    steps =
      Enum.map(chain.steps, fn
        step = %Step{id: ^step_id} -> %{step | args: apply_override(step.args, {strategy, args})}
        other -> other
      end)

    %{chain | steps: steps}
  end

  # ══════════════════════════════════════════════════════════════════════
  # Run / Resume
  # ══════════════════════════════════════════════════════════════════════

  @doc """
  Runs the chain with `data` as the initial value.

  ## Run options

    * `:run_id` — identifies the run for checkpointing (defaults to a fresh reference)
    * `:context` — run-level context map read by `{:context, key}` / `{:call, fun}` arg markers
    * `:arg_overrides` — `%{step_id => {:replace | :merge, args} | args}` runtime overrides
    * `:store` — checkpoint store module (falls back to `chain.opts.store`)

  Internal resume keys (`:__resume_index__`, `:__resume_results__`,
  `:__resume_attempts__`) are set by `resume/2`.
  """
  @spec run(t(), term(), keyword()) ::
          {:ok, term()} | {:error, term()} | {:error, term(), map()}
  def run(chain = %__MODULE__{}, data, run_opts \\ []) when is_list(run_opts) do
    run_id = Keyword.get(run_opts, :run_id, make_ref())
    store = chain.opts.store || Keyword.get(run_opts, :store)
    warn_unmatched_overrides(chain, run_opts)

    acc = %{
      index: Keyword.get(run_opts, :__resume_index__, 0),
      results: Keyword.get(run_opts, :__resume_results__, %{}),
      attempt: Keyword.get(run_opts, :__resume_attempts__, 0),
      context: Keyword.get(run_opts, :context, %{}),
      run_id: run_id
    }

    emit(chain, [:run, :start], %{chain_id: chain.id, run_id: run_id}, %{})
    chain_span = maybe_span(chain, "function_chain.run", %{chain_id: chain.id, run_id: run_id})

    steps =
      case acc.index do
        0 -> chain.steps
        index -> Enum.drop(chain.steps, index)
      end

    case run_steps(steps, data, chain, run_opts, acc) do
      {:ok, final} ->
        end_span(chain_span, %{chain_id: chain.id, run_id: run_id, result: :ok})
        emit(chain, [:run, :stop], %{chain_id: chain.id, run_id: run_id, result: :ok}, %{})
        maybe_delete_checkpoint(store, run_id)
        {:ok, final}

      {:error, reason, meta} ->
        end_span(chain_span, %{chain_id: chain.id, run_id: run_id, result: :error})

        emit(
          chain,
          [:run, :stop],
          %{chain_id: chain.id, run_id: run_id, result: :error, reason: reason},
          %{}
        )

        maybe_checkpoint(store, chain, run_id, meta)

        if chain.opts.return_context do
          {:error, reason, %{step: meta.step, index: meta.index, attempts: meta.attempts}}
        else
          {:error, reason}
        end
    end
  end

  @doc """
  Resumes a halted run from its persisted checkpoint.

      FunctionChain.resume(chain, run_id: "order-123", store: FunctionChain.Store.ETS)

  Only MFA-only chains are resumable by default: if the chain (including
  nested parallel branches and branch routes) contains non-MFA steps
  (closures cannot be serialized), `NotResumableError` is raised — unless a
  `:step_resolver` is supplied. The resolver is a `fun/1` called with each
  non-MFA step's id and must return an MFA fun_spec `{mod, fun, args}` that
  replaces the closure:

      FunctionChain.resume(chain, run_id: "order-123", store: ETS,
        step_resolver: fn
          :charge_card -> {MyApp.Billing, :charge, []}
          _ -> nil
        end)

  Returns `{:error, {:no_checkpoint, run_id}}` when the store has no
  checkpoint for the given run.
  """
  @spec resume(t(), keyword()) :: {:ok, term()} | {:error, term()} | {:error, term(), map()}
  def resume(chain = %__MODULE__{}, opts) when is_list(opts) do
    store = Keyword.fetch!(opts, :store)
    run_id = Keyword.fetch!(opts, :run_id)

    case store.get(run_id) do
      {:ok, cp = %Checkpoint{}} ->
        chain = resolve_chain_steps!(chain, Keyword.get(opts, :step_resolver))
        _ = store.delete(run_id)

        run(chain, cp.input_at_failure,
          run_id: run_id,
          store: store,
          __resume_index__: cp.step_index,
          __resume_results__: cp.accumulated_context,
          __resume_attempts__: cp.attempt_count,
          __resume_reason__: cp.failed_reason
        )

      {:error, :not_found} ->
        {:error, {:no_checkpoint, run_id}}
    end
  end

  # ══════════════════════════════════════════════════════════════════════
  # Step loop
  # ══════════════════════════════════════════════════════════════════════

  defp run_steps([], data, _chain, _run_opts, _acc), do: {:ok, data}

  defp run_steps([step = %ParallelStep{} | rest], data, chain, run_opts, acc) do
    run_composite_step(
      step,
      fn -> run_parallel(step, data, chain, run_opts) end,
      data,
      rest,
      chain,
      run_opts,
      acc
    )
  end

  defp run_steps([step = %BranchStep{} | rest], data, chain, run_opts, acc) do
    run_composite_step(
      step,
      fn -> run_branch(step, data, chain, run_opts) end,
      data,
      rest,
      chain,
      run_opts,
      acc
    )
  end

  defp run_steps([step = %Step{} | rest], data, chain, run_opts, acc) do
    case maybe_skip(step, data) do
      :run ->
        if step_retries_exhausted?(step, chain, acc) do
          # Resumed run whose retry budget was already spent when the
          # checkpoint was written — fail without invoking the step again.
          reason = Keyword.get(run_opts, :__resume_reason__) || {:retry_exhausted, step.id}
          handle_step_error(step, reason, data, rest, chain, run_opts, acc)
        else
          args = resolve_args(step, data, run_opts, acc)
          do_invoke(step, data, args, rest, chain, run_opts, acc)
        end

      {:skip, forwarded} ->
        emit_step(chain, step, acc, run_opts, [:skip], [], %{})
        run_steps(rest, forwarded, chain, run_opts, bump(acc, step, forwarded))

      {:predicate_error, reason} ->
        handle_step_error(step, reason, data, rest, chain, run_opts, acc)
    end
  end

  defp step_retries_exhausted?(step = %Step{}, chain, acc) do
    max_retries = step.max_retries || chain.opts.max_retries
    acc.attempt > max_retries
  end

  # Fan-out/fan-in and routing steps share the same lifecycle: span +
  # telemetry around a runner that yields `{:ok, value} | {:error, reason}`.
  defp run_composite_step(step, runner, data, rest, chain, run_opts, acc) do
    span_kind =
      case step do
        %ParallelStep{} -> "function_chain.parallel"
        %BranchStep{} -> "function_chain.branch"
      end

    span = maybe_span(chain, "#{span_kind}.#{inspect(step.id)}", span_meta(chain, step, acc))
    step_meta_struct = %{id: step.id, name: step.name, telemetry: nil}
    emit_step(chain, step_meta_struct, acc, run_opts, [:start], [], %{})
    started = System.monotonic_time()

    case runner.() do
      {:ok, value} ->
        duration = System.monotonic_time() - started
        end_span(span, %{chain_id: chain.id, result: :ok})

        emit_step(chain, step_meta_struct, acc, run_opts, [:stop], [result: :ok], %{
          duration: duration
        })

        run_steps(rest, value, chain, run_opts, bump(acc, step, value))

      {:error, reason} ->
        duration = System.monotonic_time() - started
        end_span(span, %{chain_id: chain.id, result: :error})

        emit_step(chain, step_meta_struct, acc, run_opts, [:exception], [reason: reason], %{
          duration: duration
        })

        {:error, reason, failure_meta(acc, step, reason, data)}
    end
  end

  defp do_invoke(step, data, args, rest, chain, run_opts, acc) do
    # acc.attempt counts invocations of the current step so far.
    acc = %{acc | attempt: acc.attempt + 1}

    span =
      maybe_span(chain, "function_chain.step.#{inspect(step.id)}", span_meta(chain, step, acc))

    emit_step(chain, step, acc, run_opts, [:start], [attempt: acc.attempt], %{})
    started = System.monotonic_time()

    result =
      try do
        invoke(step, data, args)
      rescue
        e ->
          if chain.opts.rescue_exceptions do
            {:error, {:exception, e, __STACKTRACE__}}
          else
            reraise e, __STACKTRACE__
          end
      end

    duration = System.monotonic_time() - started
    result = normalize_step_result(result)
    end_span(span, %{result: result_tag(result)})

    case result do
      {:ok, value} ->
        emit_step(chain, step, acc, run_opts, [:stop], [result: :ok, attempt: acc.attempt], %{
          duration: duration
        })

        log_step(chain, step, run_opts, data, result, duration, acc.attempt)
        run_steps(rest, value, chain, run_opts, bump(acc, step, value))

      {:error, reason} ->
        emit_step(
          chain,
          step,
          acc,
          run_opts,
          [:exception],
          [reason: reason, attempt: acc.attempt],
          %{
            duration: duration
          }
        )

        log_step(chain, step, run_opts, data, result, duration, acc.attempt)
        handle_step_error(step, reason, data, rest, chain, run_opts, acc)
    end
  end

  defp normalize_step_result(result = {:ok, _}), do: result
  defp normalize_step_result(result = {:error, _}), do: result
  defp normalize_step_result(other), do: {:error, {:invalid_return, other}}

  defp handle_step_error(step, reason, data, rest, chain, run_opts, acc) do
    strategy = step.on_error || chain.opts.on_error
    max_retries = step.max_retries || chain.opts.max_retries
    retry_delay = step.retry_delay || chain.opts.retry_delay

    case strategy do
      :halt ->
        {:error, reason, failure_meta(acc, step, reason, data)}

      :ignore ->
        run_steps(rest, data, chain, run_opts, bump(acc, step, data))

      :retry when acc.attempt <= max_retries ->
        emit_step(chain, step, acc, run_opts, [:retry], [attempt: acc.attempt + 1], %{})

        if retry_delay > 0, do: Process.sleep(retry_delay)

        args = resolve_args(step, data, run_opts, acc)
        do_invoke(step, data, args, rest, chain, run_opts, acc)

      :retry ->
        case step.on_retry_exhausted do
          :ignore -> run_steps(rest, data, chain, run_opts, bump(acc, step, data))
          _halt -> {:error, reason, failure_meta(acc, step, reason, data)}
        end
    end
  end

  # ══════════════════════════════════════════════════════════════════════
  # Parallel / Branch execution
  # ══════════════════════════════════════════════════════════════════════

  defp run_parallel(step = %ParallelStep{}, data, chain, run_opts) do
    results =
      step.branches
      |> Task.async_stream(
        fn %Branch{id: id, chain: sub_chain} ->
          {id,
           try do
             run(sub_chain, data, inherit_opts(chain, run_opts))
           rescue
             e -> {:error, {:exception, e, __STACKTRACE__}}
           end}
        end,
        max_concurrency: step.max_concurrency,
        timeout: step.timeout
      )
      |> Enum.map(fn
        {:ok, pair} -> pair
        {:exit, reason} -> {nil, {:error, {:branch_exit, reason}}}
      end)

    join_results(step, results)
  end

  defp join_results(%ParallelStep{join: :list, on_branch_error: :halt_all}, results) do
    case Enum.find(results, &match?({_, {:error, _}}, &1)) do
      nil -> {:ok, for({_, {:ok, value}} <- results, do: value)}
      {_, {:error, reason}} -> {:error, reason}
    end
  end

  defp join_results(%ParallelStep{join: :list, on_branch_error: :ignore_failed}, results) do
    {:ok, for({_, {:ok, value}} <- results, do: value)}
  end

  defp join_results(%ParallelStep{join: :list, on_branch_error: :collect_errors}, results) do
    errors = Map.new(for({id, {:error, reason}} <- results, do: {id, reason}))

    if map_size(errors) == 0 do
      {:ok, for({_, {:ok, value}} <- results, do: value)}
    else
      {:error, {:branch_errors, errors}}
    end
  end

  defp join_results(%ParallelStep{join: :map, on_branch_error: :halt_all}, results) do
    case Enum.find(results, &match?({_, {:error, _}}, &1)) do
      nil -> {:ok, Map.new(for({id, {:ok, value}} <- results, do: {id, value}))}
      {_, {:error, reason}} -> {:error, reason}
    end
  end

  defp join_results(%ParallelStep{join: :map, on_branch_error: :ignore_failed}, results) do
    {:ok, Map.new(for({id, {:ok, value}} <- results, do: {id, value}))}
  end

  defp join_results(%ParallelStep{join: :map, on_branch_error: :collect_errors}, results) do
    {:ok,
     Map.new(results, fn
       {id, {:ok, value}} -> {id, value}
       {id, {:error, reason}} -> {id, {:error, reason}}
     end)}
  end

  defp join_results(%ParallelStep{join: fun}, results) when is_function(fun, 1) do
    case fun.(results) do
      {:error, reason} -> {:error, reason}
      value -> {:ok, value}
    end
  end

  defp run_branch(%BranchStep{routes: routes, default: default}, data, chain, run_opts) do
    case match_route(routes, data) do
      {:match, sub_chain} ->
        normalize_result(run(sub_chain, data, inherit_opts(chain, run_opts)))

      :no_match when default != nil ->
        normalize_result(run(default, data, inherit_opts(chain, run_opts)))

      :no_match ->
        {:error, {:no_matching_branch, data}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp match_route([], _data), do: :no_match

  defp match_route([{pred, sub_chain} | rest], data) when is_function(pred, 1) do
    case safe_predicate(pred, data) do
      true -> {:match, sub_chain}
      false -> match_route(rest, data)
      {:error, reason} -> {:error, reason}
    end
  end

  defp normalize_result({:ok, value}), do: {:ok, value}
  defp normalize_result({:error, reason}), do: {:error, reason}
  defp normalize_result({:error, reason, _meta}), do: {:error, reason}

  # Nested chains (branches/parallel sub-chains) inherit the parent's
  # log/telemetry flags unless overridden (A6).
  defp inherit_opts(chain, run_opts) do
    prev = Keyword.get(run_opts, :__inherit__, %{})

    inherit = %{
      log: chain.opts.log == true or Map.get(prev, :log, false),
      telemetry: chain.opts.telemetry == true or Map.get(prev, :telemetry, false)
    }

    Keyword.put(run_opts, :__inherit__, inherit)
  end

  # Predicates are assumed pure; a raising predicate is treated like a
  # raising step (see §10 / A7 of the design assumptions).
  defp safe_predicate(pred, data) do
    pred.(data)
  rescue
    e -> {:error, {:exception, e, __STACKTRACE__}}
  end

  # ══════════════════════════════════════════════════════════════════════
  # Conditional skipping
  # ══════════════════════════════════════════════════════════════════════

  defp maybe_skip(%Step{when: nil, unless: nil}, _data), do: :run

  defp maybe_skip(step = %Step{when: pred}, data) when is_function(pred, 1) do
    case safe_predicate(pred, data) do
      true -> :run
      false -> skip_result(step, data)
      {:error, reason} -> {:predicate_error, reason}
    end
  end

  defp maybe_skip(step = %Step{unless: pred}, data) when is_function(pred, 1) do
    case safe_predicate(pred, data) do
      true -> skip_result(step, data)
      false -> :run
      {:error, reason} -> {:predicate_error, reason}
    end
  end

  defp skip_result(%Step{on_skip: :pass_through}, data), do: {:skip, data}

  defp skip_result(%Step{on_skip: :error, id: id}, _data),
    do: {:predicate_error, {:step_skipped, id}}

  defp skip_result(%Step{on_skip: {:replace, fun}}, data) do
    {:skip, fun.(data)}
  rescue
    e -> {:predicate_error, {:exception, e, __STACKTRACE__}}
  end

  # ══════════════════════════════════════════════════════════════════════
  # Argument resolution
  # ══════════════════════════════════════════════════════════════════════

  defp resolve_args(step = %Step{}, value, run_opts, acc) do
    override =
      run_opts
      |> Keyword.get(:arg_overrides, %{})
      |> Map.get(step.id)

    step.args
    |> apply_override(override)
    |> Enum.map(&resolve_dynamic(&1, value, acc))
  end

  defp apply_override(base_args, nil), do: base_args
  defp apply_override(_base_args, {:replace, new_args}), do: new_args

  defp apply_override(base_args, {:merge, new_args}) do
    if Keyword.keyword?(base_args) and Keyword.keyword?(new_args) do
      Keyword.merge(base_args, new_args)
    else
      new_args
    end
  end

  defp apply_override(_base_args, new_args) when is_list(new_args), do: new_args

  defp apply_override(base_args, override) do
    Logger.warning("FunctionChain, invalid arg override #{inspect(override)}, keeping base args")
    base_args
  end

  defp resolve_dynamic({:context, key}, _value, acc),
    do: Map.get(acc.context, key)

  defp resolve_dynamic({:from_step, step_id}, _value, acc),
    do: Map.get(acc.results, step_id)

  defp resolve_dynamic({:call, fun}, _value, acc) when is_function(fun, 1),
    do: fun.(acc.context)

  defp resolve_dynamic({:call, fun}, value, acc) when is_function(fun, 2),
    do: fun.(value, acc.context)

  # Markers nested in a keyword-style arg pair (e.g. `[gateway: {:context, :gateway}]`)
  # are resolved in place: the pair tuple spreads as a positional arg on
  # invocation, so the pair itself is kept with its value resolved.
  defp resolve_dynamic({key, {:context, k}}, _value, acc) when is_atom(key),
    do: {key, Map.get(acc.context, k)}

  defp resolve_dynamic({key, {:from_step, step_id}}, _value, acc) when is_atom(key),
    do: {key, Map.get(acc.results, step_id)}

  defp resolve_dynamic({key, {:call, fun}}, _value, acc)
       when is_atom(key) and is_function(fun, 1),
       do: {key, fun.(acc.context)}

  defp resolve_dynamic({key, {:call, fun}}, value, acc) when is_atom(key) and is_function(fun, 2),
    do: {key, fun.(value, acc.context)}

  defp resolve_dynamic(other, _value, _acc), do: other

  # ══════════════════════════════════════════════════════════════════════
  # Invocation
  # ══════════════════════════════════════════════════════════════════════

  defp invoke(%Step{type: :mfa, fun_spec: {mod, fun, _}}, value, args),
    do: apply(mod, fun, [value | args])

  defp invoke(%Step{type: :fun, fun_spec: fun}, value, _args) when is_function(fun, 1),
    do: fun.(value)

  defp invoke(%Step{type: :fun_with_args, fun_spec: {fun, _}}, value, args),
    do: apply(fun, [value | args])

  # ══════════════════════════════════════════════════════════════════════
  # Logging / Telemetry / Tracing
  # ══════════════════════════════════════════════════════════════════════

  defp log_step(chain, step, run_opts, input, result, duration, attempt) do
    enabled =
      if is_nil(step.log), do: log_on?(chain, run_opts), else: step.log == true

    if enabled do
      duration_us = System.convert_time_unit(duration, :native, :microsecond)

      Logger.debug(
        "FunctionChain step chain_id=#{inspect(chain.id)} step=#{inspect(step.id)} " <>
          "attempt=#{attempt} input=#{inspect(input)} result=#{inspect(result)} " <>
          "duration_us=#{duration_us}"
      )
    end
  end

  defp log_on?(chain, run_opts) do
    chain.opts.log == true or inherit_flag(run_opts, :log)
  end

  defp telemetry_on?(chain, run_opts) do
    chain.opts.telemetry == true or inherit_flag(run_opts, :telemetry)
  end

  defp inherit_flag(run_opts, key) do
    run_opts
    |> Keyword.get(:__inherit__, %{})
    |> Map.get(key, false)
  end

  defp emit(chain, event, meta, measurements) do
    if chain.opts.telemetry do
      :telemetry.execute([:function_chain | event], measurements, meta)
    end
  end

  defp emit_step(chain, step, acc, run_opts, event, extra, measurements) do
    enabled =
      if is_nil(step.telemetry), do: telemetry_on?(chain, run_opts), else: step.telemetry == true

    if enabled do
      :telemetry.execute(
        [:function_chain, :step | event],
        measurements,
        step_meta(chain, step, acc, extra)
      )
    end
  end

  defp step_meta(chain, step, acc, extra) do
    Map.merge(
      %{
        chain_id: chain.id,
        step_id: step.id,
        step_name: step.name,
        index: acc.index,
        attempt: acc.attempt
      },
      Map.new(extra)
    )
  end

  defp span_meta(chain, step, acc) do
    %{chain_id: chain.id, step_id: step.id, index: acc.index}
  end

  defp maybe_span(%__MODULE__{opts: %{tracing: tracer}}, name, meta)
       when is_atom(tracer) and tracer not in [false, nil] do
    {tracer, tracer.start_span(name, meta)}
  end

  defp maybe_span(_chain, _name, _meta), do: nil

  defp end_span(nil, _meta), do: :ok
  defp end_span({tracer, ctx}, meta), do: tracer.end_span(ctx, meta)

  # ══════════════════════════════════════════════════════════════════════
  # Checkpoint helpers
  # ══════════════════════════════════════════════════════════════════════

  defp maybe_checkpoint(nil, _chain, _run_id, _meta), do: :ok

  defp maybe_checkpoint(store, chain, run_id, meta) do
    checkpoint = %Checkpoint{
      chain_id: chain.id,
      run_id: run_id,
      step_index: meta.index,
      input_at_failure: meta.input,
      accumulated_context: meta.results,
      attempt_count: meta.attempts,
      failed_reason: meta.reason,
      inserted_at: DateTime.utc_now()
    }

    case store.put(checkpoint) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.error("FunctionChain, failed to store checkpoint: #{inspect(reason)}")
    end
  end

  defp maybe_delete_checkpoint(nil, _run_id), do: :ok

  defp maybe_delete_checkpoint(store, run_id) do
    _ = store.delete(run_id)
    :ok
  end

  # Unmatched override keys are non-fatal (A13): warn only when logging is on.
  defp warn_unmatched_overrides(chain = %__MODULE__{}, run_opts) do
    if chain.opts.log do
      case Keyword.get(run_opts, :arg_overrides) do
        overrides when is_map(overrides) and map_size(overrides) > 0 ->
          unmatched =
            Enum.reject(Map.keys(overrides), &MapSet.member?(chain.step_ids, &1))

          if unmatched != [] do
            Logger.warning(
              "FunctionChain, unmatched arg_overrides keys #{inspect(unmatched)} ignored"
            )
          end

        _ ->
          :ok
      end
    end

    :ok
  end

  defp step_ids_of(%Step{id: id}), do: [id]

  defp step_ids_of(%ParallelStep{id: id, branches: branches}) do
    [
      id
      | for(%Branch{id: branch_id, chain: sub} <- branches, do: [branch_id | step_ids(sub)])
    ]
  end

  defp step_ids_of(%BranchStep{id: id, routes: routes, default: default}) do
    [id | Enum.flat_map(routes, fn {_pred, sub} -> step_ids(sub) end) ++ step_ids(default)]
  end

  defp step_ids(chain = %__MODULE__{}) do
    Enum.flat_map(chain.steps, &step_ids_of/1)
  end

  defp step_ids(nil), do: []

  # Construction-time maintenance of the cached step-id set: overrides matching
  # at run time then never walks the chain.
  defp append_step(chain, step) do
    %{
      chain
      | steps: chain.steps ++ [step],
        step_ids: MapSet.union(chain.step_ids, MapSet.new(step_ids_of(step)))
    }
  end

  defp result_tag({:ok, _}), do: :ok
  defp result_tag({:error, _}), do: :error

  defp bump(acc, step, result) do
    %{acc | index: acc.index + 1, attempt: 0, results: Map.put(acc.results, step.id, result)}
  end

  defp failure_meta(acc, step, reason, input) do
    %{
      step: step.id,
      index: acc.index,
      reason: reason,
      attempts: acc.attempt,
      input: input,
      results: acc.results
    }
  end

  # ══════════════════════════════════════════════════════════════════════
  # Construction internals & validation
  # ══════════════════════════════════════════════════════════════════════

  defp default_opts do
    %__MODULE__{}.opts
  end

  defp do_add(chain, name, fun_spec, step_opts) do
    if Keyword.has_key?(step_opts, :when) and Keyword.has_key?(step_opts, :unless) do
      raise ArgumentError, "a step cannot define both :when and :unless"
    end

    type = classify!(fun_spec)
    on_error = Keyword.get(step_opts, :on_error)
    on_skip = Keyword.get(step_opts, :on_skip, :pass_through)

    unless on_error == nil or on_error in @strategies do
      raise ArgumentError, "invalid on_error strategy: #{inspect(on_error)}"
    end

    unless valid_on_skip?(on_skip) do
      raise ArgumentError, "invalid on_skip policy: #{inspect(on_skip)}"
    end

    args = Keyword.get(step_opts, :args, default_args(fun_spec))
    validate_arity!(type, fun_spec, args)

    step = %Step{
      id: name,
      name: name,
      fun_spec: fun_spec,
      type: type,
      args: args,
      on_error: on_error,
      max_retries: Keyword.get(step_opts, :max_retries),
      retry_delay: Keyword.get(step_opts, :retry_delay),
      on_retry_exhausted: Keyword.get(step_opts, :on_retry_exhausted, :halt),
      when: Keyword.get(step_opts, :when),
      unless: Keyword.get(step_opts, :unless),
      on_skip: on_skip,
      log: Keyword.get(step_opts, :log),
      telemetry: Keyword.get(step_opts, :telemetry)
    }

    append_step(chain, step)
  end

  defp valid_on_skip?(:pass_through), do: true
  defp valid_on_skip?(:error), do: true
  defp valid_on_skip?({:replace, fun}) when is_function(fun, 1), do: true
  defp valid_on_skip?(_), do: false

  defp classify!({mod, fun, args}) when is_atom(mod) and is_atom(fun) and is_list(args), do: :mfa
  defp classify!(fun) when is_function(fun, 1), do: :fun
  defp classify!({fun, args}) when is_function(fun) and is_list(args), do: :fun_with_args

  defp classify!(other),
    do: raise(ArgumentError, "unrecognized fun_spec: #{inspect(other)}")

  defp default_args({_mod, _fun, args}), do: args
  defp default_args({fun, args}) when is_function(fun), do: args
  defp default_args(_fun), do: []

  defp validate_arity!(:mfa, {mod, fun, _args}, args) do
    arity = 1 + length(args)

    cond do
      function_exported?(mod, fun, arity) ->
        :ok

      Enum.any?(0..@max_checkable_arity, &function_exported?(mod, fun, &1)) ->
        raise ArgumentError,
              "arity mismatch: #{inspect(mod)}.#{inspect(fun)}/#{arity} not found " <>
                "(the function is exported with a different arity)"

      true ->
        # Likely a private function; cannot be verified at build time.
        :ok
    end
  end

  # 1-arity funs are validated by `classify!/1`; nothing further to check.
  defp validate_arity!(:fun, _fun, _args), do: :ok

  defp validate_arity!(:fun_with_args, {fun, _args}, args) do
    {:arity, fun_arity} = :erlang.fun_info(fun, :arity)
    expected = 1 + length(args)

    if fun_arity != expected do
      raise ArgumentError,
            "arity mismatch: fun has arity #{fun_arity}, but will be invoked with " <>
              "#{expected} arguments (value + #{inspect(args)})"
    end

    :ok
  end

  defp fun_spec?({mod, fun, args}) when is_atom(mod) and is_atom(fun) and is_list(args), do: true
  defp fun_spec?(fun) when is_function(fun), do: true
  defp fun_spec?({fun, args}) when is_function(fun) and is_list(args), do: true
  defp fun_spec?(_), do: false

  defp validate_join!(join) when join in @joins, do: :ok
  defp validate_join!(fun) when is_function(fun, 1), do: :ok

  defp validate_join!(other),
    do: raise(ArgumentError, "invalid join: #{inspect(other)}")

  defp validate_on_branch_error!(value) when value in @on_branch_errors, do: :ok

  defp validate_on_branch_error!(other),
    do: raise(ArgumentError, "invalid on_branch_error: #{inspect(other)}")

  defp resolve_chain_steps!(chain, nil) do
    unless chain_resumable?(chain) do
      raise SuperWorker.FunctionChain.NotResumableError,
            "chain contains non-MFA steps #{inspect(non_mfa_step_ids(chain))}; " <>
              "only MFA-only chains are resumable, or supply a :step_resolver"
    end

    chain
  end

  defp resolve_chain_steps!(chain, resolver) when is_function(resolver, 1) do
    %{chain | steps: Enum.map(chain.steps, &resolve_step!(&1, resolver))}
  end

  defp resolve_chain_steps!(_chain, other) do
    raise SuperWorker.FunctionChain.NotResumableError,
          ":step_resolver must be a fun/1 returning MFA fun_specs, got: #{inspect(other)}"
  end

  defp resolve_step!(step = %Step{type: :mfa}, _resolver), do: step

  defp resolve_step!(step = %Step{type: type, id: id}, resolver) do
    case resolver.(id) do
      {mod, fun, args} = mfa when is_atom(mod) and is_atom(fun) and is_list(args) ->
        validate_arity!(:mfa, mfa, args)
        %{step | type: :mfa, fun_spec: mfa, args: args}

      other ->
        raise SuperWorker.FunctionChain.NotResumableError,
              "step #{inspect(id)} (type #{inspect(type)}) is not resumable and the " <>
                ":step_resolver did not return a valid MFA fun_spec: #{inspect(other)}"
    end
  end

  defp resolve_step!(step = %ParallelStep{branches: branches}, resolver) do
    branches =
      for branch = %Branch{} <- branches,
          do: %{branch | chain: resolve_chain_steps!(branch.chain, resolver)}

    %{step | branches: branches}
  end

  defp resolve_step!(step = %BranchStep{routes: routes, default: default}, resolver) do
    routes =
      for {pred, sub_chain} <- routes, do: {pred, resolve_chain_steps!(sub_chain, resolver)}

    %{step | routes: routes, default: resolve_chain_steps!(default, resolver)}
  end

  defp chain_resumable?(nil), do: true
  defp chain_resumable?(chain), do: Enum.all?(chain.steps, &mfa_step?/1)

  defp mfa_step?(%Step{type: :mfa}), do: true
  defp mfa_step?(%Step{}), do: false

  defp mfa_step?(%ParallelStep{branches: branches}),
    do: Enum.all?(branches, &chain_resumable?(&1.chain))

  defp mfa_step?(%BranchStep{routes: routes, default: default}) do
    Enum.all?(routes, fn {_pred, sub_chain} -> chain_resumable?(sub_chain) end) and
      chain_resumable?(default)
  end

  defp non_mfa_step_ids(chain = %__MODULE__{}) do
    Enum.flat_map(chain.steps, fn
      %Step{type: :mfa} -> []
      %Step{id: id} -> [id]
      step -> non_mfa_step_ids(step)
    end)
  end

  defp non_mfa_step_ids(nil), do: []

  defp non_mfa_step_ids(%ParallelStep{id: id, branches: branches}) do
    [id | Enum.flat_map(branches, fn %Branch{chain: sub} -> non_mfa_step_ids(sub) end)]
  end

  defp non_mfa_step_ids(%BranchStep{id: id, routes: routes, default: default}) do
    [
      id
      | Enum.flat_map(routes, fn {_pred, sub} -> non_mfa_step_ids(sub) end) ++
          non_mfa_step_ids(default)
    ]
  end
end
