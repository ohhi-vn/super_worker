defmodule SuperWorker.Supervisor.Worker do
  @moduledoc """
  Documentation for `SuperWorker.Supervisor.Worker`.
  """

  alias :ets, as: Ets

  @worker_params [:id, :type, :fun]

  @standalone_params [:restart_strategy, :max_restarts, :max_seconds, :auto_restart_time]

  @standalone_restart_strategies [:permanent, :transient, :temporary]

  @group_params [:group_id]

  @chain_params [:chain_id, :num_workers]

  @enforce_keys [:id]
  defstruct [
    :id, # worker id, unique in supervior.
    :pid, # current pid of worker.
    :ref, # reference created when spawning the worker.
    :start_time, # start time of worker. if worker is restarted, this value is updated.
    restart_strategy: :transient, # restart strategy of worker. Affected by the supervisor restart strategy.
    type: :standalone, # type of worker. :standalone, :group, :chain
    restart_count: 0, # restart counter.
    fun: nil, # anonymous function {:fun, fun} or  {function, module, arguments} of worker.
    supervisor: nil, # supervisor pid.
    partition: nil, # partition id.
    num_workers: 1, # number of workers in chain.
    parent: nil, # parent(group/chain) id.
  ]

  import SuperWorker.Supervisor.Utils

  def check_group_options(opts) do
    with {:ok, opts} <- normalize_opts(opts, @worker_params ++ @group_params),
         {:ok, opts} <- validate_opts(opts),
         {:ok, opts} <- default_opts(opts),
         {:ok, opts} <- map_to_struct(opts) do
      {:ok, opts}
    end
  end

  def check_chain_options(opts) do
    with {:ok, opts} <- normalize_opts(opts, @worker_params ++ @chain_params),
         {:ok, opts} <- validate_opts(opts),
         {:ok, opts} <- default_opts(opts),
         {:ok, opts} <- map_to_struct(opts)  do
      {:ok, opts}
    end
  end

  def check_standalone_options(opts) do
    with {:ok, opts} <- normalize_opts(opts, @worker_params ++ @standalone_params),
          {:ok, opts} <- validate_restart_strategy(opts),
         {:ok, opts} <- validate_opts(opts),
         {:ok, opts} <- default_opts(opts),
         {:ok, opts} <- map_to_struct(opts)  do
      {:ok, opts}
    end
  end

  defp validate_restart_strategy(opts) do
    if opts.restart_strategy in @standalone_restart_strategies do
      {:ok, opts}
    else
      {:error, "Invalid group restart strategy, #{inspect opts.restart_strategy}"}
    end
  end

  defp validate_opts(opts) do
    # TO-DO: Implement the validation.
    {:ok, opts}
  end

  defp default_opts(opts) do
    opts =
      opts
      |> Map.put(:start_time, DateTime.utc_now())

    {:ok, opts}
  end

  def save(worker) do
    get_table_name(worker.supervisor)
    |> Ets.insert({{:worker, worker.id}, worker})
  end

  def get(supervisor, worker_id) do
    get_table_name(supervisor)
    |> Ets.lookup({{:worker, worker_id}})
    |> case do
      [{_, worker}] -> {:ok, worker}
      [] -> {:error, :not_found}
    end
  end

  def remove(supervisor, worker_id) do
    get_table_name(supervisor)
    |> Ets.delete({{:worker, worker_id}})
  end

  # Update the worker information.
  def update_process(supervisor, {:group, group_id}, worker, pid, ref) do
    update_process_info(supervisor, {:worker, {:group, group_id}, worker.id}, pid, ref)
  end

  def update_process(supervisor, {:chain, chain_id}, worker, pid, ref) do
    update_process_info(supervisor, {:worker, {:chain, chain_id}, worker.id}, pid, ref)
  end

  def update_process(supervisor, worker, pid, ref) do
    update_process_info(supervisor, {:worker, worker.id}, pid, ref)
  end

  defp update_process_info(supervisor, key, pid, ref) do
    table = get_table_name(supervisor)
    case Ets.lookup(table, key) do
      [{_, worker}] ->
        worker =
          worker
          |> Map.put(:pid, pid)
          |> Map.put(:ref, ref)

        Ets.insert(table, {key, worker})
      [] -> {:error, :not_found}
    end
  end

  defp map_to_struct(opts) when is_map(opts) do
    {:ok, struct(__MODULE__, opts)}
  end
end
