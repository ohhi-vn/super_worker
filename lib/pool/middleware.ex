defmodule SuperWorker.Pool.Middleware do
  @moduledoc """
  Middleware behaviour for `SuperWorker.Pool` — a Plug-style pipeline around
  job execution.

  Middleware modules are composed at pool start (`middleware: [Mod1, Mod2]`).
  `call/3` wraps job execution and runs **in the worker process**; a crash
  inside a middleware is treated as a worker crash (the job is requeued).
  Built-ins ship with the pool:

  - `SuperWorker.Pool.Middleware.Telemetry` — `:telemetry` events on
    start/stop/exception of each execution.
  - `SuperWorker.Pool.Middleware.CircuitBreaker` — trips after N consecutive
    failures per partition and short-circuits new submissions.

  Users compose their own the same way — a rate limiter, request-id
  propagation, metrics, whatever — without touching pool internals:

      defmodule MyMiddleware do
        @behaviour SuperWorker.Pool.Middleware

        @impl true
        def call(job, meta, next) do
          Logger.debug("job \#{inspect(meta.job_id)} started")
          next.(job, meta)
        end
      end

  ## Callbacks

  - `call/3` (required) — wraps execution. Invoke `next.(job, meta)` to run
    the rest of the pipeline (and eventually the worker's `handle_job/2`).
  - `check_enqueue/2` (optional) — invoked by the partition process before a
    job is queued; return `:ok` to accept or `{:error, reason}` to reject the
    submission with that reason (used by the circuit breaker to fail fast
    with `{:error, :circuit_open}`).
  - `notify/3` (optional) — invoked by the partition when a job reaches a
    final outcome: `:ok` or `{:error, reason}` (expected error, exhausted
    retries or a final worker crash). Retries in flight are not reported.
  """

  @callback call(job :: term(), meta :: map(), next :: (term(), map() -> term())) :: term()

  @callback check_enqueue(job :: term(), meta :: map()) :: :ok | {:error, term()}

  @callback notify(outcome :: :ok | {:error, term()}, job :: term(), meta :: map()) :: :ok

  @optional_callbacks [check_enqueue: 2, notify: 3]

  require Logger

  alias SuperWorker.Supervisor.Utils

  @doc """
  Runs the job through the middleware pipeline.

  `executor` is the zero-middleware tail: it receives the job and meta and
  returns `{outcome, new_worker_state}` where `outcome` is
  `{:ok, result} | {:error, reason} | {:retry, reason}`.
  """
  @spec run_pipeline([module()], term(), map(), (term(), map() -> term())) :: term()
  def run_pipeline([], job, meta, executor), do: executor.(job, meta)

  def run_pipeline([module | rest], job, meta, executor) do
    module.call(job, meta, fn job, meta -> run_pipeline(rest, job, meta, executor) end)
  end

  @doc """
  Runs the optional `check_enqueue/2` of every middleware, in order.

  The first rejection (or a middleware crash — fail-open, logged) wins.
  """
  @spec check_enqueue([module()], term(), map()) :: :ok | {:error, term()}
  def check_enqueue(modules, job, meta) do
    Enum.reduce_while(modules, :ok, fn module, :ok ->
      case safe_check(module, job, meta) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  @doc """
  Reports a final job outcome (`:ok` or `{:error, reason}`) to every
  middleware implementing the optional `notify/3`. Errors inside middleware
  are logged and swallowed — reporting must never break the pool.
  """
  @spec notify([module()], :ok | {:error, term()}, term(), map()) :: :ok
  def notify(modules, outcome, job, meta) do
    Enum.each(modules, fn module ->
      if function_exported?(module, :notify, 3) do
        safe_notify(module, outcome, job, meta)
      end
    end)

    :ok
  end

  defp safe_check(module, job, meta) do
    if function_exported?(module, :check_enqueue, 2) do
      case Utils.safe_call(module, :check_enqueue, [job, meta]) do
        {:ok, result} ->
          result

        {:error, {kind, reason}} ->
          Logger.warning(
            "SuperWorker, Pool, middleware #{inspect(module)} crashed in check_enqueue " <>
              "(failing open): #{kind}: #{inspect(reason)}"
          )

          :ok
      end
    else
      :ok
    end
  end

  defp safe_notify(module, outcome, job, meta) do
    case Utils.safe_call(module, :notify, [outcome, job, meta]) do
      {:ok, _} ->
        :ok

      {:error, {kind, reason}} ->
        Logger.warning(
          "SuperWorker, Pool, middleware #{inspect(module)} crashed in notify: #{kind}: #{inspect(reason)}"
        )
    end
  end
end
