defmodule SuperWorker.Pool do
  @moduledoc """
  A simple, partitioned process pool for Elixir.

  `SuperWorker.Pool` gives you a worker pool without designing any
  supervision tree and without any OTP knowledge: hand it a function, an MFA
  or a module implementing the `SuperWorker.Pool.Worker` behaviour, then
  submit jobs with `run/2`, `run_async/2` or `cast/2`.

      # 1. A bare function
      {:ok, _pid} =
        SuperWorker.Pool.start_link(
          name: MyPool,
          task: fn job -> do_work(job) end,
          size: 20
        )

      # 2. An MFA
      {:ok, _pid} =
        SuperWorker.Pool.start_link(name: MyPool, task: {MyMod, :process, []}, size: 20)

      # 3. A module implementing the behaviour (stateful workers, e.g.
      #    holding a connection)
      {:ok, _pid} =
        SuperWorker.Pool.start_link(name: MyPool, worker: MyApp.ImageResizer, size: 20)

      {:ok, result} = SuperWorker.Pool.run(MyPool, job)    # sync: blocks until done
      {:ok, result} = SuperWorker.Pool.run(MyPool, job, timeout: 5_000)
      {:ok, ref} = SuperWorker.Pool.run_async(MyPool, job) # Task-like ref
      {:ok, result} = SuperWorker.Pool.await(ref)
      {:ok, result} = SuperWorker.Pool.await(ref, 5_000)   # custom timeout
      :ok = SuperWorker.Pool.cast(MyPool, job)             # fire-and-forget

  This is a **single-node, in-memory pool**: queued jobs survive worker
  crashes and partition restarts, but not a node crash, and there is no
  cross-node distribution. For durable, cross-node jobs use a persisted
  queue (e.g. Oban) instead — or put a persisted layer behind `:on_failure`.

  ## Architecture

  The pool is partitioned: workers and the job queue are split over
  `partitions` (default: `System.schedulers_online()`) independent
  bottlenecks, each holding `ceil(size / partitions)` workers. A job is
  routed to a partition by round robin or by hashing the job term
  (`:erlang.phash2`) — hash routing gives partition affinity for
  order-sensitive jobs. Each worker is a supervised process, so a crash
  restarts just that worker.

      Pool supervisor (one_for_one)
      ├── Pool.Server (config housekeeping)
      ├── Partition 1 supervisor (rest_for_one)
      │   ├── [CircuitBreaker]  ├── Partition  ├── Worker 1  ├── Worker 2 ...
      ├── Partition 2 supervisor
      └── ...

  The **queue lives in the `Partition` GenServer** — a process sibling of the
  workers under each partition supervisor, not in the supervisor itself. It
  owns the bounded FIFO queue, dispatches jobs to idle workers, schedules
  retries, and handles crash requeues and dead-lettering. See
  `SuperWorker.Pool.Partition`.

  ## Options

  - `:name` (required) — atom, the registered name of the pool.
  - `:task` or `:worker` (required, mutually exclusive) — fun/0, fun/1,
    `{module, function, args}` (job prepended to args), or a
    `SuperWorker.Pool.Worker` behaviour module.
  - `:worker_opts` — keyword passed to a `:worker` module's `init/1`
    (default: `[]`). The library ships a ready-made chain worker:
    `SuperWorker.Pool.FunctionChain` runs a `SuperWorker.FunctionChain` per
    job (`worker_opts: [chain: my_chain, ...]`).
  - `:size` — total number of workers (default: `System.schedulers_online()`).
  - `:partitions` — number of partitions (default: `System.schedulers_online()`).
    When `size < partitions`, workers are rounded up (`ceil(size/partitions)`,
    minimum 1) so no partition is dead.
  - `:routing` — `:round_robin` (default) or `:hash`; hash routes
    `:erlang.phash2(job, partitions) + 1`.
  - `:max_queue` — per-partition queue bound; submissions beyond it
    immediately fail with `{:error, :overloaded}` (default: `1_000`).
  - `:retry` — `max_attempts:` (default: `5`) and `backoff:`
    (`{:fixed, ms}` or `{:exponential, base:, max:, jitter:}`, default:
    `{:exponential, base: 200, max: 10_000, jitter: true}`).
  - `:on_failure` — `fun/2` or `{module, function, args}` invoked as
    `(job, reason)` after retries are exhausted (the reason is not silently
    dropped — dead-letter it, log it or alert on it).
  - `:on_result` — `fun/2` or `{module, function, args}` invoked as
    `(job, {:ok, result} | {:error, reason})` for `cast` jobs whose caller
    cannot await a result. The payload covers **both outcomes** — success,
    expected failures, and permanent failures (exhausted retries or a crash):
    the error reason is the same one `:on_failure` would receive. Without
    `:on_result`, cast results are dropped.
  - `:middleware` — list of `SuperWorker.Pool.Middleware` modules
    (default: `[]`).
  - `:circuit_breaker` — options for
    `SuperWorker.Pool.Middleware.CircuitBreaker` when it is in `:middleware`:
    `failure_threshold:` (default 5), `reset_timeout:` ms (default 10_000).
  - `:max_restarts` — restart intensity ceiling shared by the pool supervisor
    and each partition supervisor (default: 10). A worker that crash-loops in
    `init/1` burns this budget and then takes its partition down instead of
    spinning forever.
  - `:max_seconds` — the window for `:max_restarts` (default: 10).

  ## Error handling & durability

  Two separate concerns, handled differently:

  - **Worker crashes** (bugs, `raise`, exits): the worker is restarted by its
    partition supervisor and its in-flight job is **requeued once** — at the
    **front** of its partition queue — and charged against its own retry
    budget; a poison-pill job cannot crash-loop the pool. A crash of the
    worker loses its behaviour state (`init/1` runs again).
  - **Expected failures** (`{:error, ...}` / `{:retry, ...}`): never crash
    anything. `{:retry, ...}` reschedules via `Process.send_after` after a
    backoff delay (sleeping inside a worker would hold it hostage); after
    `max_attempts` the `:on_failure` callback fires.

  **Retry ordering**: a retried job re-enters the **front** of its partition
  queue when the backoff delay elapses, so it is not starved behind fresh
  submissions — but the backoff delay itself is inherent to retries, so a
  later job submitted during the delay can still complete first. `:hash`
  routing keeps a job and its retries on the same partition, which is what
  preserves relative order for order-sensitive jobs; it does not promise
  strict FIFO across retries.

  `run/3` returns `{:error, reason}` where reason is the job's error, or one
  of `:overloaded`, `:timeout`, `{:retries_exhausted, reason}`,
  `{:worker_crashed, reason}`, `:circuit_open`, `:pool_not_found`,
  `:pool_unavailable`. `cast/2` drops the result unless `:on_result` is
  configured (overloaded casts are dropped with a warning log).

  **Timeouts**: `run/3` and `await/2` take a `timeout` in ms (default
  30_000). When the caller gives up, the job keeps running — only the result
  is dropped.

  **`:circuit_open` vs `:on_failure`**: a job rejected by the circuit
  breaker (`check_enqueue/2`) never enters the queue and never runs, so
  `:on_failure` does **not** fire for it — the rejection is returned to the
  caller (and dropped, with a warning, for casts). `:on_failure` is only the
  source of truth for jobs that ran and finally failed (exhausted retries or
  a final worker crash).

  **Durability is in-memory only**: queued jobs survive worker crashes and
  partition restarts, but NOT a node crash. If jobs must survive `kill -9`,
  put a persisted layer behind `:on_failure` (or use a durable queue such as
  Oban for the pipeline instead of this pool).

  ## Telemetry

  See `SuperWorker.Pool.Middleware.Telemetry` for the full event list.

  ## Testing

  For deterministic tests start a single-worker pool and run jobs
  synchronously:

      {:ok, _} =
        SuperWorker.Pool.start_link(
          name: TestPool,
          task: fn job -> ... end,
          size: 1,
          partitions: 1
        )

      {:ok, result} = SuperWorker.Pool.run(TestPool, job)

  Avoid `cast/2` and multi-worker pools in assertions on ordering: jobs may
  run on any worker, in any order.
  """

  require Logger

  alias SuperWorker.Pool.Backoff
  alias SuperWorker.Pool.PartitionSupervisor
  alias SuperWorker.Pool.Server
  alias SuperWorker.TermStorage

  defstruct [
    :name,
    # task spec: {:fun, fun/1} | {:fun0, fun/0} | {:mfa, {m, f, a}}
    :task,
    # module implementing SuperWorker.Pool.Worker (alternative to :task)
    :worker,
    :worker_opts,
    :size,
    :partitions,
    # :round_robin | :hash
    :routing,
    :max_queue,
    # %{max_attempts: pos_integer(), backoff: Backoff.t()}
    :retry,
    :on_failure,
    :on_result,
    :middleware,
    :circuit_breaker,
    # restart intensity shared by the pool supervisor and each partition
    # supervisor
    :max_restarts,
    :max_seconds,
    # atomics counter for round-robin partition routing
    :counter
  ]

  @type t :: %__MODULE__{}

  @registry SuperWorker.Pool.Registry
  @routing_options [:round_robin, :hash]

  @default_size System.schedulers_online()
  @default_partitions System.schedulers_online()
  @default_max_queue 1_000
  @default_max_attempts 5
  @default_backoff {:exponential, base: 200, max: 10_000, jitter: true}
  @default_run_timeout 30_000
  @default_enqueue_timeout 5_000
  @default_max_restarts 10
  @default_max_seconds 10

  ## Public API

  @doc """
  Starts a partitioned worker pool.

  Returns `{:ok, pid}` of the pool supervisor, or `{:error, reason}`
  (`{:error, {:already_started, pid}}` if the name is taken, or a
  configuration error — including `{:error, {:worker_init_failed, reason}}`
  when a worker implementation cannot initialize).
  """
  @spec start_link(keyword()) :: {:ok, pid()} | {:error, term()}
  def start_link(opts) when is_list(opts) do
    with {:ok, config} <- validate(opts) do
      children = [
        {Server, config}
        | Enum.map(1..config.partitions, fn i ->
            %{
              id: {:partition_supervisor, i},
              start: {PartitionSupervisor, :start_link, [[config, i]]},
              type: :supervisor
            }
          end)
      ]

      opts = [
        strategy: :one_for_one,
        max_restarts: config.max_restarts,
        max_seconds: config.max_seconds,
        name: config.name
      ]

      # Trap exits while starting so a worker that cannot initialize surfaces
      # as a clean `{:error, reason}` instead of an exit signal in the caller;
      # the previous trap state is restored right after, so normal link
      # semantics are unaffected once the pool is up.
      trap_saved = Process.flag(:trap_exit, true)

      try do
        Supervisor.start_link(children, opts)
      else
        {:ok, pid} -> {:ok, pid}
        {:error, reason} -> {:error, extract_init_failure(reason)}
      after
        Process.flag(:trap_exit, trap_saved)
      end
    end
  end

  # A worker init failure surfaces as a nested supervisor shutdown; dig the
  # `{:worker_init_failed, reason}` leaf out of the nested structure so
  # callers get the actionable reason.
  defp extract_init_failure(reason) do
    case find_init_failure(reason) do
      nil -> reason
      found -> found
    end
  end

  defp find_init_failure(found = {:worker_init_failed, _}), do: found

  defp find_init_failure(tuple) when is_tuple(tuple) do
    tuple
    |> Tuple.to_list()
    |> Enum.find_value(&find_init_failure/1)
  end

  defp find_init_failure(_other), do: nil

  @doc """
  Submits a job and blocks until it completes (or finally fails).

  Returns `{:ok, result}` or `{:error, reason}` — the job's own error, or
  `:overloaded`, `:timeout`, `{:retries_exhausted, reason}`,
  `{:worker_crashed, reason}`, `:circuit_open`, `:pool_not_found`,
  `:pool_unavailable`.

  ## Options

  - `:timeout` — how long the caller blocks, in ms (default: 30_000). The
    job keeps running even if the caller times out; its result is then
    dropped.
  """
  @spec run(atom(), term(), keyword()) :: {:ok, term()} | {:error, term()}
  def run(pool, job, opts \\ []) when is_atom(pool) do
    timeout = Keyword.get(opts, :timeout, @default_run_timeout)

    case partition_pid(pool, job) do
      {:ok, pid} ->
        try do
          GenServer.call(pid, {:submit, job}, timeout)
        catch
          # GenServer.call timeout exits as {:timeout, {GenServer, :call, _}}
          :exit, {:timeout, {GenServer, :call, _}} -> {:error, :timeout}
          :exit, _reason -> {:error, :pool_unavailable}
        end

      error ->
        error
    end
  end

  @doc """
  Submits a job without blocking and returns `{:ok, ref}`.

  Await the result with `await/2` from the same process. If the pool or the
  partition dies before the job completes, `await/2` returns
  `{:error, :timeout}` (in-memory queue: the job is lost).
  """
  @spec run_async(atom(), term()) :: {:ok, reference()} | {:error, term()}
  def run_async(pool, job) when is_atom(pool) do
    case partition_pid(pool, job) do
      {:ok, pid} ->
        try do
          GenServer.call(pid, {:submit_async, job}, @default_enqueue_timeout)
        catch
          :exit, _reason -> {:error, :pool_unavailable}
        end

      error ->
        error
    end
  end

  @doc """
  Awaits the result of a job submitted with `run_async/2`.

  Returns the same shapes as `run/2`, or `{:error, :timeout}` after
  `timeout` ms (default: 30_000).
  """
  @spec await(reference(), non_neg_integer()) :: {:ok, term()} | {:error, term()}
  def await(ref, timeout \\ @default_run_timeout) when is_reference(ref) do
    receive do
      {:super_worker_pool_result, ^ref, result} -> result
    after
      timeout -> {:error, :timeout}
    end
  end

  @doc """
  Fire-and-forget submission. Returns `:ok`, or `{:error, :overloaded}` /
  `{:error, :pool_not_found}` when the submission could not even be queued.

  Results are dropped unless the pool is configured with `:on_result`.
  """
  @spec cast(atom(), term()) :: :ok | {:error, term()}
  def cast(pool, job) when is_atom(pool) do
    case partition_pid(pool, job) do
      {:ok, pid} -> GenServer.cast(pid, {:submit_cast, job})
      error -> error
    end
  end

  @doc """
  Stops the pool (default: graceful, waits up to `timeout` ms).

  Queued jobs are dropped — the pool is in-memory only.
  """
  @spec stop(atom(), non_neg_integer()) :: :ok | {:error, :not_running}
  def stop(pool, timeout \\ 5_000) when is_atom(pool) do
    case Process.whereis(pool) do
      nil -> {:error, :not_running}
      _pid -> Supervisor.stop(pool, :normal, timeout)
    end
  end

  @doc """
  Inspects a running pool: configuration summary and per-partition live
  counters (queue length, busy and idle workers).
  """
  @spec info(atom()) :: {:ok, map()} | {:error, :pool_not_found}
  def info(pool) when is_atom(pool) do
    case TermStorage.get({:pool, pool}) do
      {:ok, %__MODULE__{} = config} ->
        {:ok,
         %{
           name: config.name,
           size: config.size,
           partitions: config.partitions,
           workers_per_partition: max(1, ceil(config.size / config.partitions)),
           routing: config.routing,
           max_queue: config.max_queue,
           middleware: config.middleware,
           partition_details: Enum.map(1..config.partitions, &partition_info(pool, &1))
         }}

      {:error, :not_found} ->
        {:error, :pool_not_found}
    end
  end

  @doc false
  @spec via(atom(), term()) :: {:via, Registry, {atom(), term()}}
  def via(pool, key) do
    {:via, Registry, {@registry, {pool, key}}}
  end

  ## Configuration

  @doc false
  @spec validate(keyword()) :: {:ok, %__MODULE__{}} | {:error, term()}
  def validate(opts) when is_list(opts) do
    with {:ok, name} <- validate_name(opts),
         {:ok, task_spec} <- validate_task_or_worker(opts),
         {:ok, size} <- validate_positive(opts, :size, @default_size),
         {:ok, partitions} <- validate_positive(opts, :partitions, @default_partitions),
         {:ok, routing} <- validate_routing(opts),
         {:ok, max_queue} <- validate_non_negative(opts, :max_queue, @default_max_queue),
         {:ok, retry} <- validate_retry(opts),
         {:ok, middleware} <- validate_middleware(opts),
         {:ok, max_restarts} <- validate_positive(opts, :max_restarts, @default_max_restarts),
         {:ok, max_seconds} <- validate_positive(opts, :max_seconds, @default_max_seconds) do
      {:ok,
       %__MODULE__{
         name: name,
         task: task_spec.task,
         worker: task_spec.worker,
         worker_opts: Keyword.get(opts, :worker_opts, []),
         size: size,
         partitions: partitions,
         routing: routing,
         max_queue: max_queue,
         retry: retry,
         on_failure: validate_callback(opts, :on_failure),
         on_result: validate_callback(opts, :on_result),
         middleware: middleware,
         circuit_breaker: Keyword.get(opts, :circuit_breaker, []),
         max_restarts: max_restarts,
         max_seconds: max_seconds,
         counter: :atomics.new(1, [])
       }}
    end
  end

  defp validate_name(opts) do
    case Keyword.fetch(opts, :name) do
      {:ok, name} when is_atom(name) and name != nil -> {:ok, name}
      {:ok, name} -> {:error, {:invalid, {:name, name}}}
      :error -> {:error, {:missing, :name}}
    end
  end

  defp validate_task_or_worker(opts) do
    task = Keyword.get(opts, :task)
    worker = Keyword.get(opts, :worker)

    cond do
      not is_nil(task) and not is_nil(worker) ->
        {:error, {:invalid, {:task, "give either :task or :worker, not both"}}}

      is_function(task, 1) ->
        {:ok, %{task: {:fun, task}, worker: nil}}

      is_function(task, 0) ->
        {:ok, %{task: {:fun0, task}, worker: nil}}

      match?(
        {module, function, args} when is_atom(module) and is_atom(function) and is_list(args),
        task
      ) ->
        {:ok, %{task: {:mfa, task}, worker: nil}}

      behaviour_module?(worker) ->
        {:ok, %{task: nil, worker: worker}}

      true ->
        {:error, {:invalid, {:task, task || worker}}}
    end
  end

  defp behaviour_module?(module) when is_atom(module) and module != nil do
    Code.ensure_loaded(module) == {:module, module} and
      function_exported?(module, :init, 1) and
      function_exported?(module, :handle_job, 2)
  end

  defp behaviour_module?(_other), do: false

  defp validate_positive(opts, key, default) do
    value = Keyword.get(opts, key, default)

    if is_integer(value) and value > 0 do
      {:ok, value}
    else
      {:error, {:invalid, {key, value}}}
    end
  end

  defp validate_non_negative(opts, key, default) do
    value = Keyword.get(opts, key, default)

    if is_integer(value) and value >= 0 do
      {:ok, value}
    else
      {:error, {:invalid, {key, value}}}
    end
  end

  defp validate_routing(opts) do
    routing = Keyword.get(opts, :routing, :round_robin)

    if routing in @routing_options do
      {:ok, routing}
    else
      {:error, {:invalid, {:routing, routing}}}
    end
  end

  defp validate_retry(opts) do
    retry = Keyword.get(opts, :retry, [])
    backoff = Keyword.get(retry, :backoff, @default_backoff)

    with {:ok, max_attempts} <- validate_positive(retry, :max_attempts, @default_max_attempts) do
      if Backoff.valid?(backoff) do
        {:ok, %{max_attempts: max_attempts, backoff: backoff}}
      else
        {:error, {:invalid, {:retry, backoff}}}
      end
    end
  end

  defp validate_middleware(opts) do
    middleware = Keyword.get(opts, :middleware, [])

    if is_list(middleware) and Enum.all?(middleware, &valid_middleware?/1) do
      {:ok, middleware}
    else
      {:error, {:invalid, {:middleware, middleware}}}
    end
  end

  defp valid_middleware?(module) when is_atom(module) and module != nil do
    Code.ensure_loaded(module) == {:module, module} and
      function_exported?(module, :call, 3)
  end

  defp valid_middleware?(_other), do: false

  defp validate_callback(opts, key) do
    callback = Keyword.get(opts, key)

    cond do
      is_nil(callback) ->
        nil

      is_function(callback, 2) ->
        callback

      match?({module, _function, args} when is_atom(module) and is_list(args), callback) ->
        callback

      true ->
        Logger.warning(
          "SuperWorker, Pool, invalid #{inspect(key)} callback #{inspect(callback)} (want fun/2 or {m, f, a}), ignoring"
        )

        nil
    end
  end

  ## Routing

  defp partition_pid(pool, job) do
    case TermStorage.get({:pool, pool}) do
      {:ok, %__MODULE__{} = config} ->
        case Registry.lookup(@registry, {pool, {:partition, partition_index(config, job)}}) do
          [{pid, _}] -> {:ok, pid}
          [] -> {:error, :pool_unavailable}
        end

      {:error, :not_found} ->
        {:error, :pool_not_found}
    end
  end

  defp partition_index(%__MODULE__{partitions: partitions, routing: :hash}, job) do
    :erlang.phash2(job, partitions) + 1
  end

  defp partition_index(%__MODULE__{partitions: partitions, counter: counter}, _job) do
    sequence = :atomics.add_get(counter, 1, 1)
    rem(sequence - 1, partitions) + 1
  end

  defp partition_info(pool, id) do
    case Registry.lookup(@registry, {pool, {:partition, id}}) do
      [{pid, _}] ->
        try do
          GenServer.call(pid, :info)
        catch
          :exit, _ -> %{id: id, alive?: false}
        end

      [] ->
        %{id: id, alive?: false}
    end
  end
end
