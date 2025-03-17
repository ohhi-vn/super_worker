defmodule SuperWorker.Supervisor.Worker do
  @moduledoc """
  Documentation for `SuperWorker.Supervisor.Worker`.
  """

  @worker_params [:id, :type, :name, :fun]

  @standalone_params [:restart_strategy, :max_restarts, :max_seconds, :auto_restart_time]

  @standalone_restart_strategies [:permanent, :transient, :temporary]

  # @worker_restart_strategies [:permanent, :transient, :temporary]

  @group_params [:group_id]

  @chain_params [:chain_id, :num_workers]

  alias __MODULE__

  @enforce_keys [:id, :fun]
  defstruct [
    :id, # worker id, unique in supervior.
    :pid, # current pid of worker.
    :name, # name of worker.
    :ref, # reference created when spawning the worker.
    :start_time, # start time of worker. if worker is restarted, this value is updated.
    restart_strategy: :transient, # restart strategy of worker. Affected by the supervisor restart strategy.
    type: :standalone, # type of worker. :standalone, :group, :chain
    restart_count: 0, # restart counter.
    fun: nil, # anonymous function {:fun, fun} or  {function, module, arguments} of worker.
    supervisor: nil, # supervisor id.
    partition: nil, # partition id.
    num_workers: 1, # number of workers in chain.
    parent: nil, # parent(group/chain) id.
    order: nil, # order in chain.
    first_worker_id: nil, # first worker id in chain.
  ]

  @type t :: %__MODULE__{
    id: any,
    pid: pid,
    ref: reference,
    start_time: DateTime.t,
    restart_strategy: atom,
    type: atom,
    restart_count: non_neg_integer,
    fun: nil | {:fun, fun} | {module, atom, [any]},
    supervisor: atom,
    partition: atom,
    num_workers: non_neg_integer,
    parent: any,
    order: non_neg_integer | nil
  }

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
    Registry.put_meta(worker.supervisor, {:worker, worker.id}, worker)
  end

  def get(supervisor, worker_id) do
    case Registry.meta(supervisor, {:worker, worker_id}) do
      :error -> {:error, :worker_not_found}
      {:ok, worker}  -> worker
    end
  end

  def remove(supervisor, worker_id) do
    Registry.delete_meta(supervisor, {:worker, worker_id})
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
    case Worker.get(supervisor, {:worker, key}) do
      {:error, _} = error ->
        error
      worker ->
        worker =
          worker
          |> Map.put(:pid, pid)
          |> Map.put(:ref, ref)

        Worker.save(worker)
    end
  end

  defp map_to_struct(opts) when is_map(opts) do
    {:ok, struct(__MODULE__, opts)}
  end
end
