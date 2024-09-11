defmodule SuperWorker.Supervisor.Group do
  @moduledoc """
  Documentation for `SuperWorker.Supervisor.Group`.
  """

  # Parameters for group.
  @group_params [:id, :restart_strategy, :type, :max_restarts, :max_seconds, :auto_restart_time]

  # Restart strategies for group.
  @group_restart_strategies [:one_for_one, :one_for_all]

  alias SuperWorker.Supervisor.Worker
  alias :ets, as: Ets
  alias __MODULE__

  @enforce_keys [:id]
  defstruct [
    :id, # group id, unique in supervior.
    restart_strategy: :one_for_all, # default restart strategy for group is :one_for_all.
    supervisor: nil, # supervisor id (atom)
    partition: nil, # partition id holding the group.
    data_table: nil, # ets table of supervisor.
  ]

  import SuperWorker.Supervisor.Utils

  require Logger

  ## Public functions

  @doc """
  Check, validate and convert key-value pairs to struct.
  """
  def check_options(opts) do
    with {:ok, opts} <- normalize_opts(opts, @group_params),
         {:ok, opts} <- validate_restart_strategy(opts),
         {:ok, opts} <- validate_opts(opts),
         {:ok, group} <- map_to_struct(opts) do
      {:ok, group}
    end
  end

  @doc """
  Get worker from the group.
  """
  def get_worker(%Group{} = group, worker_id) do
    Logger.debug("get_worker: #{inspect group.supervisor}, #{inspect worker_id}")
    case Ets.lookup(group.data_table, {:worker, {:group, group.id}, worker_id}) do
      [{_, worker}] -> {:ok, worker}
      [] -> {:error, :not_found}
    end
  end

  @doc """
  Get all workers from the group.
  """
  def get_all_workers(%Group{} = group) do
    Logger.debug("get_all_workers: #{inspect group.supervisor}")
    Ets.match(group.data_table, {{:worker, {:group, group.id}, :_}, :"$1"})
    |> List.flatten()
  end

  @doc """
  Check if worker exists in the group.
  """
  def worker_exists?(group, worker_id) do
    case get_worker(group, worker_id) do
      {:ok, _} -> true
      {:error, _} -> false
    end
  end

  @doc """
  A internal function. Add a worker to the group.
  """
  def add_worker(group, %Worker{} = worker)  do
    case get_worker(group, worker.id) do
      {:ok, _} -> {:error, :worker_exists}
      {:error, _} ->
        worker = Map.put(worker, :parent, group.id)

        with {:ok, group} <- spawn_worker(group, worker)  do
          Ets.insert(group.data_table, {{:group, group.id}, group})
          {:ok, group}
        end
    end
  end

  @doc """
  A internal function. Restart a worker in the group.
  """
  def restart_worker(group, worker_id) do
    if worker_exists?(group, worker_id) do
      with {:ok, worker} <- get_worker(group, worker_id) do
        kill_worker(group, worker, :restart)
        spawn_worker(group, worker)
      end
    else
      {:error, :not_found}
    end
  end

  def remove_worker(group, worker_id) do
    if worker_exists?(group, worker_id) do
      Ets.delete(group.data_table, {:worker, {:group, group.id}, worker_id})
      # TO-DO: Clean other info

      {:ok, group}
    else
      {:error, :not_found}
    end
  end

  def kill_worker(group, worker = %Worker{}, reason) do
    if Process.alive?(worker.pid) do
      Logger.debug("group: #{inspect group.id}, kill_worker: #{inspect worker}, reason: #{inspect reason}")
      Ets.delete(group.data_table, {:worker, :ref, worker.ref})
      Process.exit(worker.pid, reason)
    end
  end
  def kill_worker(group, worker_id, reason) do
    case get_worker(group, worker_id) do
      {:ok, worker} ->
       kill_worker(group, worker, reason)
      {:error, _} -> {:error, :not_found}
    end
  end

  def kill_all_workers(group, reason \\ :kill) do
    Group.get_all_workers(group)
    |> Enum.each(fn %Worker{} = worker ->
      kill_worker(group, worker, reason)
    end)
  end

  defp spawn_worker(group, %Worker{} = worker) do
    worker = do_spawn_worker(group, worker)
    Logger.debug("spawn_worker: #{inspect worker}")

    # add or update data, ref, pid
    Ets.insert(group.data_table, {{:worker, {:group, group.id}, worker.id}, worker})
    Ets.insert(group.data_table, {{:worker, :ref, worker.ref}, worker.id, worker.pid, {:group, group.id}})

    {:ok, group}
  end

  def broadcast(group, message) do
    Group.get_all_workers(group)
    |> Enum.each(fn %Worker{id: worker_id} ->
      {:ok, worker} = get_worker(group, worker_id)
      send(worker.pid, message)
    end)
  end

  ## Private functions

  defp do_spawn_worker(group, %Worker{} = worker) do
    {pid, ref} = spawn_monitor(fn ->
      Registry.register(group.supervisor, {:worker, {:group, group.id}, worker.id}, [])
      Registry.register(group.supervisor, {:group, group.id}, :worker)

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
