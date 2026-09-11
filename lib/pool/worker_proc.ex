defmodule SuperWorker.Pool.WorkerProc do
  @moduledoc """
  A pool worker process.

  One process per pool worker (started by the partition's supervisor). On
  start it runs the worker implementation's `init/1` (user behaviour or the
  auto-generated `SuperWorker.Pool.TaskWorker`) and then registers itself
  with its partition, which queues it as idle and dispatches jobs to it.

  The worker state is kept **in the process** between jobs; when the process
  crashes, the state is lost and `init/1` runs again on restart (the
  in-flight job is requeued by the partition and charged to its retry
  budget). This is the crash-isolation boundary: a bug in `handle_job/2` or
  in a middleware kills only this worker, never the pool or other workers.

  All user code runs through the middleware pipeline of the pool. Result
  classification (`{:ok, ...}` / `{:error, ...}` / `{:retry, ...}`) happens
  here; scheduling decisions (backoff, dead-letter) belong to the partition.
  """

  use GenServer

  require Logger
  require SuperWorker.Log

  alias SuperWorker.Pool.Middleware

  defstruct [
    :pool,
    :partition,
    :impl,
    :middleware,
    # internal state of the worker implementation (behaviour state)
    :impl_state
  ]

  @doc false
  def child_spec(arg = %{index: index}) do
    %{
      id: {:worker, index},
      start: {__MODULE__, :start_link, [arg]},
      restart: :permanent
    }
  end

  @doc false
  def start_link(arg) when is_map(arg) do
    GenServer.start_link(__MODULE__, arg)
  end

  @impl true
  def init(arg = %{pool: pool, partition: partition}) do
    case init_impl(arg.impl, arg.impl_opts) do
      {:ok, impl_state} ->
        SuperWorker.Log.debug(fn ->
          "SuperWorker, Pool, worker started, pool: #{inspect(pool)}, partition: #{inspect(partition)}"
        end)

        safe_send(pool, partition, {:worker_ready, self()})

        {:ok,
         %__MODULE__{
           pool: pool,
           partition: partition,
           impl: arg.impl,
           middleware: arg.middleware,
           impl_state: impl_state
         }}

      {:error, reason} ->
        # A worker whose implementation cannot start is a real failure: at
        # pool start it aborts `start_link`, at runtime the supervisor
        # restarts it (bounded by the partition supervisor's intensity).
        {:stop, {:worker_init_failed, reason}}
    end
  end

  @impl true
  def handle_info({:run_job, job_id, job, meta}, state) do
    SuperWorker.Log.debug(fn ->
      "SuperWorker, Pool, worker (pool #{inspect(state.pool)}) running job #{inspect(job_id)}"
    end)

    {outcome, impl_state} = execute(state, job, meta)

    safe_send(state.pool, state.partition, {:job_result, self(), job_id, outcome})

    {:noreply, %{state | impl_state: impl_state}}
  end

  def handle_info(_message, state), do: {:noreply, state}

  ## Private functions

  defp init_impl(impl, opts) do
    case impl.init(opts) do
      {:ok, impl_state} -> {:ok, impl_state}
      {:error, _reason} = error -> error
      other -> {:error, {:invalid_init_return, other}}
    end
  end

  # Runs the middleware pipeline around the worker implementation.
  # An exception here is deliberate: it crashes this worker process, which
  # the partition observes via :DOWN and requeues the job (crash isolation).
  defp execute(state, job, meta) do
    executor = fn job, _meta ->
      classify(state.impl.handle_job(job, state.impl_state), state.impl_state)
    end

    Middleware.run_pipeline(state.middleware, job, meta, executor)
  end

  defp classify({:ok, result, new_state}, _old_state), do: {{:ok, result}, new_state}
  defp classify({:error, reason, new_state}, _old_state), do: {{:error, reason}, new_state}
  defp classify({:retry, reason, new_state}, _old_state), do: {{:retry, reason}, new_state}

  defp classify(other, old_state),
    do: {{:error, {:invalid_handle_job_return, other}}, old_state}

  # Sends to the partition via the Registry (resolve by name, send by pid —
  # via tuples are not used here so a restarting partition never breaks us).
  defp safe_send(pool, partition, message) do
    case Registry.lookup(SuperWorker.Pool.Registry, {pool, {:partition, partition}}) do
      [{pid, _}] ->
        send(pid, message)

      [] ->
        # The partition is down/restarting: drop the message. Either the pool
        # is stopping or the partition will re-adopt this worker shortly.
        Logger.warning("SuperWorker, Pool, worker could not reach its partition, message dropped")
    end

    :ok
  end
end
