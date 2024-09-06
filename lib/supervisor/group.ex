defmodule SuperWorker.Supervisor.Group do
  @moduledoc """
  Documentation for `SuperWorker.Supervisor.Group`.
  """

  @group_params [:id, :restart_strategy, :type, :max_restarts, :max_seconds, :auto_restart_time]

  @group_restart_strategies [:one_for_one, :one_for_all]

  alias SuperWorker.Supervisor.Worker

  @enforce_keys [:id]
  defstruct [
    :id, # group id, unique in supervior.
    restart_strategy: :one_for_all,
    workers: MapSet.new(),
    supervisor: nil,
    partition: nil,
  ]

  import SuperWorker.Supervisor.Utils

  alias :ets, as: Ets

  require Logger

  ## Public functions

  def check_options(opts) do
    with {:ok, opts} <- normalize_opts(opts, @group_params),
         {:ok, opts} <- validate_restart_strategy(opts),
         {:ok, opts} <- validate_opts(opts),
         {:ok, group} <- map_to_struct(opts) do
      {:ok, group}
    end
  end

  def get_worker(group, worker_id) do
    Logger.debug("get_worker: #{inspect group.supervisor}, #{inspect worker_id}")
    case Ets.lookup(get_table_name(group.supervisor), {:worker, {:group, group.id}, worker_id}) do
      [{_, worker}] -> {:ok, worker}
      [] -> {:error, :not_found}
    end
  end

  def worker_exists?(group, worker_id) do
    MapSet.member?(group.workers, worker_id)
  end

  def add_worker(group, %Worker{} = worker)  do
    case get_worker(group, worker.id) do
      {:ok, _} -> {:error, :worker_exists}
      {:error, _} ->
        table = get_table_name(group.supervisor)
        Ets.insert(table, {{:worker, {:group, group.id}, worker.id}, worker})
        group = Map.put(group, :workers, MapSet.put(group.workers, worker.id))

        with {:ok, group} <- spawn_worker(group, worker.id)  do
          Ets.insert(table, {{:group, group.id}, group})
          {:ok, group}
        end
    end
  end

  def restart_worker(group, worker_id) do
    if worker_exists?(group, worker_id) do
      kill_worker(group, worker_id)
      spawn_worker(group, worker_id)
    else
      {:error, :not_found}
    end
  end

  def remove_worker(group, worker_id) do
    if worker_exists?(group, worker_id) do
      Ets.delete(get_table_name(group.supervisor), {:worker, {:group, group.id}, worker_id})
      workers = MapSet.delete(group.workers, worker_id)

      {:ok, %{group | workers: workers}}
    else
      {:error, :not_found}
    end
  end

  def kill_worker(group, worker_id) do
    case get_worker(group, worker_id) do
      {:ok, worker} ->
        if Process.alive?(worker.pid) do
          Process.exit(worker.pid, :kill)
        end
      {:error, _} -> {:error, :not_found}
    end
  end

  def kill_all_workers(group, reason \\ :kill) do
    Enum.each(group.workers, fn worker_id ->
      {:ok, worker} = get_worker(group, worker_id)
      if  Process.alive?(worker.pid) do
        Process.exit(worker.pid, reason)
      end
    end)
  end

  defp spawn_worker(group, worker_id) do
    {:ok, worker} = get_worker(group, worker_id)
    worker = do_spawn_worker(group, worker)
    table = get_table_name(group.supervisor)
    Ets.insert(table, {{:worker, {:group, group.id}, worker_id}, worker})
    Ets.insert(table, {{:worker, :ref, worker.ref}, worker.id, worker.pid, {:group, group.id}})

    {:ok, group}
  end

  def broadcast(group, message) do
    Enum.each(group.workers, fn  worker_id ->
      worker = get_worker(group, worker_id)
      send(worker.pid, message)
    end)
  end

  ## Private functions

  defp do_spawn_worker(group, %Worker{} = worker) do
    {pid, ref} = spawn_monitor(fn ->
      Registry.register(group.supervisor, {:worker, worker.id}, [])
      Registry.register(group.supervisor, {:group, group.id}, :worker)
      Registry.update_value(group.supervisor, {:group, group.id}, fn workers ->
        [worker.id | workers]
      end)

      Process.put({:supervisor, :sup_id}, group.supervisor)
      Process.put({:supervisor, :group_id}, group.id)
      Process.put({:supervisor, :worker_id}, worker.id)

      case worker.fun do
        {m, f, a} ->
          apply(m, f, a)

        {:fun, fun} ->
          fun.()
      end
    end)

    worker
    |> Map.put(:pid, pid)
    |> Map.put(:ref, ref)
  end

  defp do_spawn_worker(group, worker_id) do
    with {:ok, worker} <- get_worker(group, worker_id) do
      do_spawn_worker(group, worker)
    end

  end

  defp validate_restart_strategy(opts) do
    if opts.restart_strategy in @group_restart_strategies do
      {:ok, opts}
    else
      {:error, "Invalid group restart strategy, #{inspect opts.restart_strategy}"}
    end
  end

  defp validate_opts(opts) do
    # TO-DO: Implement the validation
    {:ok, opts}
  end

  defp map_to_struct(opts) when is_map(opts) do
    {:ok, struct(__MODULE__, opts)}
  end
end
