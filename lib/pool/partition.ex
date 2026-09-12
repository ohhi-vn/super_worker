defmodule SuperWorker.Pool.Partition do
  @moduledoc """
  One partition of a `SuperWorker.Pool`.

  A pool runs `config.partitions` independent partitions (default: the number
  of online schedulers). Each partition is a GenServer that owns:

  - a bounded FIFO job queue (`:max_queue` submissions in flight per
    partition — beyond that callers immediately get `{:error, :overloaded}`);
  - the pool workers of this partition (static children of the partition's
    supervisor, registered here via `worker_ready`);
  - retry scheduling (`Process.send_after` + `SuperWorker.Pool.Backoff` — a
    retrying job never holds its worker);
  - crash handling: a worker crash requeues its in-flight job **once** and
    charges it against its retry budget, so poison-pill jobs cannot crash-
    loop the pool;
  - dead-lettering: after retries are exhausted, the `:on_failure` callback
    fires so jobs are never silently dropped.

  Jobs are routed to a partition by the pool (`:round_robin` or `:hash`);
  within the partition they are dispatched first-come-first-served to idle
  workers. Each partition is an independent bottleneck: a busy partition
  never slows the others down.
  """

  use GenServer

  require Logger
  require SuperWorker.Log

  alias SuperWorker.Pool
  alias SuperWorker.Pool.{Backoff, Middleware}
  alias SuperWorker.Supervisor.Utils

  defstruct [
    # pool configuration (the %SuperWorker.Pool{} struct)
    :config,
    # 1-based partition id
    :id,
    # workers wanted on this partition: ceil(size / partitions), min 1
    :workers_wanted,
    # idle worker pids (list; monitored in :monitors, so no duplicates)
    :idle,
    # %{job_id => worker_pid}
    :busy,
    # %{worker_pid => monitor_ref}
    :monitors,
    # FIFO queue of job_ids (:queue)
    :queue,
    # %{job_id => entry}
    :jobs,
    # cached :queue length (O(1) overload checks; :queue.len/1 is O(n))
    :queue_len,
    # %{job_id => retry timer ref}
    :timers,
    # last allocated job id
    :seq
  ]

  # Job entry: job bookkeeping between submissions and completion.
  # `reply_to`: {:sync, GenServer.from} | {:async, caller_pid, ref} | :none
  defp entry(job_id, job, reply_to) do
    %{
      job_id: job_id,
      job: job,
      reply_to: reply_to,
      # number of executions so far (incl. crash requeues)
      attempts: 0,
      # crash requeue is allowed once per job (poison-pill protection)
      crash_requeued: false,
      # retry timer ref while the job waits for its backoff to elapse
      timer: nil
    }
  end

  ## API (called by SuperWorker.Pool / PartitionSupervisor)

  @doc false
  def start_link([config, id]) do
    GenServer.start_link(__MODULE__, [config, id], name: Pool.via(config.name, {:partition, id}))
  end

  @impl true
  def init([config, id]) do
    SuperWorker.Log.debug(fn ->
      "SuperWorker, Pool, partition #{inspect(id)} starting, pool: #{inspect(config.name)}"
    end)

    {:ok,
     %__MODULE__{
       config: config,
       id: id,
       workers_wanted: max(1, ceil(config.size / config.partitions)),
       idle: [],
       busy: %{},
       monitors: %{},
       queue: :queue.new(),
       queue_len: 0,
       jobs: %{},
       timers: %{},
       seq: 0
     }}
  end

  ## Submissions

  @impl true
  def handle_call({:submit, job}, from, state) do
    case enqueue(state, job, {:sync, from}) do
      {:ok, state} -> {:noreply, state}
      {:error, _reason} = error -> {:reply, error, state}
    end
  end

  def handle_call({:submit_async, job}, from, state) do
    ref = make_ref()

    case enqueue(state, job, {:async, elem(from, 0), ref}) do
      {:ok, state} -> {:reply, {:ok, ref}, state}
      {:error, _reason} = error -> {:reply, error, state}
    end
  end

  def handle_call(:info, _from, state) do
    {:reply,
     %{
       id: state.id,
       alive?: true,
       queue: state.queue_len,
       busy: map_size(state.busy),
       idle: length(state.idle)
     }, state}
  end

  @impl true
  def handle_cast({:submit_cast, job}, state) do
    case enqueue(state, job, :none) do
      {:ok, state} ->
        {:noreply, state}

      # Fire-and-forget: an overloaded cast is dropped and logged.
      {:error, reason} ->
        Logger.warning(
          "SuperWorker, Pool, partition #{inspect(state.id)} dropped cast job: #{inspect(reason)}"
        )

        {:noreply, state}
    end
  end

  ## Worker messages

  @impl true
  def handle_info({:worker_ready, pid}, state) do
    if Map.has_key?(state.monitors, pid) do
      # Already tracked (duplicate ready message).
      {:noreply, state}
    else
      SuperWorker.Log.debug(fn ->
        "SuperWorker, Pool, partition #{inspect(state.id)} registered worker #{inspect(pid)}"
      end)

      ref = Process.monitor(pid)

      state =
        state
        |> Map.put(:monitors, Map.put(state.monitors, pid, ref))
        |> Map.put(:idle, state.idle ++ [pid])
        |> dispatch()

      {:noreply, state}
    end
  end

  # Job outcome from a worker: {:ok, result} | {:error, reason} | {:retry, reason}
  def handle_info({:job_result, pid, job_id, outcome}, state) do
    with {:ok, job_id} <- validate_sender(state, pid, job_id),
         {:ok, entry} <- fetch_entry(state, job_id) do
      state =
        state
        |> release_worker(pid, job_id)
        |> process_outcome(entry, outcome)
        |> dispatch()

      {:noreply, state}
    else
      _ -> {:noreply, state}
    end
  end

  # A monitored worker died: requeue its in-flight job (once) — the worker
  # itself is restarted by the partition supervisor, not by this process.
  def handle_info({:DOWN, ref, :process, pid, reason}, state) do
    case Map.get(state.monitors, pid) do
      ^ref ->
        SuperWorker.Log.debug(fn ->
          "SuperWorker, Pool, partition #{inspect(state.id)} worker #{inspect(pid)} died: #{inspect(reason)}"
        end)

        state =
          state
          |> Map.put(:monitors, Map.delete(state.monitors, pid))
          |> Map.put(:idle, List.delete(state.idle, pid))
          |> handle_worker_down(pid, reason)
          |> dispatch()

        {:noreply, state}

      _other ->
        {:noreply, state}
    end
  end

  # A retry backoff elapsed: put the job back into the queue (front, so
  # retried jobs are not starved behind fresh submissions) and dispatch.
  def handle_info({:retry_due, job_id}, state) do
    case Map.get(state.jobs, job_id) do
      %{timer: timer} = entry when timer != nil ->
        state =
          state
          |> Map.put(:timers, Map.delete(state.timers, job_id))
          |> Map.put(:jobs, Map.put(state.jobs, job_id, %{entry | timer: nil}))
          |> Map.put(:queue, :queue.in_r(job_id, state.queue))
          |> Map.put(:queue_len, state.queue_len + 1)
          |> dispatch()

        {:noreply, state}

      _other ->
        # Unknown job or no timer running (stale message): ignore.
        {:noreply, state}
    end
  end

  def handle_info(_message, state), do: {:noreply, state}

  ## Enqueue / dispatch

  # Common path for all three submission forms. The circuit breaker (when
  # configured) rejects here, before the job ever touches the queue.
  defp enqueue(state, job, reply_to) do
    case check_middleware(state, job) do
      :ok -> accept(state, job, reply_to)
      {:error, reason} -> {:error, reason}
    end
  end

  # No middleware configured: skip the pipeline and the meta map entirely.
  defp check_middleware(%{config: %{middleware: []}}, _job), do: :ok

  defp check_middleware(state, job) do
    meta = %{pool: state.config.name, partition: state.id, job_id: nil, attempts: 0}
    Middleware.check_enqueue(state.config.middleware, job, meta)
  end

  # Overload when the backlog is full AND no idle worker can take the job
  # straight away (max_queue 0 therefore means "never queue").
  defp accept(state = %{config: %{max_queue: max_queue}, idle: []}, _job, _reply_to)
       when state.queue_len >= max_queue do
    :telemetry.execute(
      [:super_worker, :pool, :overloaded],
      %{},
      %{pool: state.config.name, partition: state.id}
    )

    {:error, :overloaded}
  end

  defp accept(state, job, reply_to), do: {:ok, submit(state, job, reply_to)}

  defp submit(state, job, reply_to) do
    job_id = state.seq + 1

    state = %{
      state
      | seq: job_id,
        jobs: Map.put(state.jobs, job_id, entry(job_id, job, reply_to)),
        queue: :queue.in(job_id, state.queue),
        queue_len: state.queue_len + 1
    }

    dispatch(state)
  end

  # Assigns queued jobs to idle workers while both are available.
  defp dispatch(state = %{idle: [worker_pid | rest_idle], queue_len: queue_len})
       when queue_len > 0 do
    {{:value, job_id}, queue} = :queue.out(state.queue)
    entry = Map.fetch!(state.jobs, job_id)

    meta = %{
      pool: state.config.name,
      partition: state.id,
      job_id: job_id,
      attempts: entry.attempts + 1
    }

    SuperWorker.Log.debug(fn ->
      "SuperWorker, Pool, partition #{inspect(state.id)} dispatching job #{inspect(job_id)} to #{inspect(worker_pid)}"
    end)

    send(worker_pid, {:run_job, job_id, entry.job, meta})

    dispatch(%{
      state
      | queue: queue,
        idle: rest_idle,
        queue_len: queue_len - 1,
        busy: Map.put(state.busy, job_id, worker_pid)
    })
  end

  defp dispatch(state), do: state

  ## Outcome handling

  defp process_outcome(state, entry, outcome) do
    case outcome do
      {:ok, result} ->
        state
        |> complete(entry, {:ok, result})
        |> notify_middleware(:ok, entry)

      {:error, reason} ->
        # An expected, final failure: dead-letter it like exhausted retries
        # and worker crashes so `:on_failure` is the single source of truth
        # for "this job did not complete" (the Worker behaviour contract).
        # The bare reason keeps the caller's shape: `{:error, reason}`.
        dead_letter(state, entry, reason, reason)

      {:retry, reason} ->
        handle_retry(state, entry, reason)
    end
  end

  defp handle_retry(state, entry, reason) do
    attempts = entry.attempts + 1

    if attempts >= state.config.retry.max_attempts do
      dead_letter(state, %{entry | attempts: attempts}, {:retries_exhausted, reason}, reason)
    else
      delay = Backoff.delay(state.config.retry.backoff, attempts)

      :telemetry.execute(
        [:super_worker, :pool, :job, :retry],
        %{},
        meta_of(state, %{entry | attempts: attempts}, %{delay: delay, reason: reason})
      )

      SuperWorker.Log.debug(fn ->
        "SuperWorker, Pool, partition #{inspect(state.id)} retrying job #{inspect(entry.job_id)} " <>
          "(attempt #{attempts}) in #{delay}ms, reason: #{inspect(reason)}"
      end)

      timer = Process.send_after(self(), {:retry_due, entry.job_id}, delay)

      state
      |> Map.put(
        :jobs,
        Map.put(state.jobs, entry.job_id, %{entry | attempts: attempts, timer: timer})
      )
      |> Map.put(:timers, Map.put(state.timers, entry.job_id, timer))
      |> dispatch()
    end
  end

  defp handle_worker_down(state, pid, reason) do
    case Enum.find(state.busy, fn {_job_id, worker_pid} -> worker_pid == pid end) do
      {job_id, ^pid} ->
        entry = Map.fetch!(state.jobs, job_id)
        attempts = entry.attempts + 1

        :telemetry.execute(
          [:super_worker, :pool, :job, :crash],
          %{},
          meta_of(state, %{entry | attempts: attempts}, %{reason: crash_reason(reason)})
        )

        state = %{state | busy: Map.delete(state.busy, job_id)}

        cond do
          attempts >= state.config.retry.max_attempts ->
            dead_letter(
              state,
              %{entry | attempts: attempts},
              {:worker_crashed, crash_reason(reason)},
              crash_reason(reason)
            )

          entry.crash_requeued ->
            # Already crash-requeued once: a poison-pill job, dead-letter it
            # instead of queueing it again — no crash loop.
            dead_letter(
              state,
              %{entry | attempts: attempts},
              {:worker_crashed, crash_reason(reason)},
              crash_reason(reason)
            )

          true ->
            SuperWorker.Log.debug(fn ->
              "SuperWorker, Pool, partition #{inspect(state.id)} requeued job #{inspect(job_id)} " <>
                "after worker crash (once)"
            end)

            %{
              state
              | jobs:
                  Map.put(state.jobs, job_id, %{
                    entry
                    | attempts: attempts,
                      crash_requeued: true
                  }),
                queue: :queue.in_r(job_id, state.queue),
                queue_len: state.queue_len + 1
            }
        end

      nil ->
        # An idle (or unknown) worker died — the restart will arrive as a
        # fresh `worker_ready`. Nothing to requeue.
        state
    end
  end

  # `reason_tagged` is what the caller receives ({:retries_exhausted, reason}
  # or {:worker_crashed, reason}); `reason_raw` is what `:on_failure` gets.
  defp dead_letter(state, entry, reason_tagged, reason_raw) do
    :telemetry.execute(
      [:super_worker, :pool, :job, :dead_letter],
      %{},
      meta_of(state, entry, %{reason: reason_tagged})
    )

    invoke_on_failure(state.config, entry.job, reason_raw)
    notify_middleware(state, {:error, reason_raw}, entry)

    complete(state, entry, {:error, reason_tagged})
  end

  # A worker crash reason is normally {exception, stacktrace}; the
  # stacktrace is internal noise for users, unwrap the exception.
  defp crash_reason({exception, stacktrace}) when is_list(stacktrace), do: exception
  defp crash_reason(reason), do: reason

  ## Completion helpers

  defp complete(state, entry, result) do
    case entry.reply_to do
      {:sync, from} ->
        GenServer.reply(from, result)

      {:async, caller_pid, ref} ->
        send(caller_pid, {:super_worker_pool_result, ref, result})

      :none ->
        invoke_on_result(state.config, entry.job, result)
    end

    state
    |> Map.put(:jobs, Map.delete(state.jobs, entry.job_id))
    |> Map.put(:timers, Map.delete(state.timers, entry.job_id))
  end

  defp invoke_on_failure(config, job, reason) do
    case config.on_failure do
      nil ->
        Logger.error(
          "SuperWorker, Pool, job #{inspect(job)} failed permanently (reason: #{inspect(reason)}) " <>
            "with no :on_failure callback configured — configure on_failure or dead-letter it yourself"
        )

      fun when is_function(fun, 2) ->
        log_on_failure_result(Utils.safe_call(fn -> fun.(job, reason) end))

      {module, function, args} ->
        log_on_failure_result(Utils.safe_call(module, function, [job, reason | args]))
    end

    :ok
  end

  defp log_on_failure_result({:ok, _}), do: :ok

  defp log_on_failure_result({:error, {kind, reason}}) do
    Logger.error("SuperWorker, Pool, :on_failure callback crashed: #{kind}: #{inspect(reason)}")
  end

  defp invoke_on_result(config, job, result) do
    case config.on_result do
      nil ->
        # Fire-and-forget cast without on_result: the result is dropped.
        :ok

      fun when is_function(fun, 2) ->
        log_on_result_result(Utils.safe_call(fn -> fun.(job, result) end))

      {module, function, args} ->
        log_on_result_result(Utils.safe_call(module, function, [job, result | args]))
    end

    :ok
  end

  defp log_on_result_result({:ok, _}), do: :ok

  defp log_on_result_result({:error, {kind, reason}}) do
    Logger.error("SuperWorker, Pool, :on_result callback crashed: #{kind}: #{inspect(reason)}")
  end

  # No middleware configured: skip the notify pipeline and the meta map build.
  defp notify_middleware(state = %{config: %{middleware: []}}, _outcome, _entry), do: state

  defp notify_middleware(state, outcome, entry) do
    Middleware.notify(
      state.config.middleware,
      outcome,
      entry.job,
      meta_of(state, entry)
    )

    state
  end

  ## Small helpers

  defp meta_of(state, entry, extra \\ %{}) do
    %{
      pool: state.config.name,
      partition: state.id,
      job_id: entry.job_id,
      attempts: entry.attempts
    }
    |> Map.merge(extra)
  end

  # The job_id is already known (validated against :busy by validate_sender/3),
  # so releasing a worker is O(1) — no scan of the busy map.
  defp release_worker(state, pid, job_id) do
    %{state | idle: [pid | state.idle], busy: Map.delete(state.busy, job_id)}
  end

  # The job must still be in flight on this exact worker; results from
  # restarted workers for jobs the new partition instance does not know
  # about are stale and dropped.
  defp validate_sender(state, pid, job_id) do
    if Map.get(state.busy, job_id) == pid, do: {:ok, job_id}, else: {:error, :stale_result}
  end

  defp fetch_entry(state, job_id) do
    case Map.get(state.jobs, job_id) do
      nil -> {:error, :unknown_job}
      entry -> {:ok, entry}
    end
  end
end
