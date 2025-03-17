defmodule SuperWorker.Supervisor.Group do
  @moduledoc """
  Documentation for `SuperWorker.Supervisor.Group`.
  """

  # Parameters for group.
  @group_params [:id, :restart_strategy, :type, :max_restarts, :max_seconds, :auto_restart_time]

  # Restart strategies for group.
  @group_restart_strategies [:one_for_one, :one_for_all]

  alias SuperWorker.Supervisor.Worker

  alias __MODULE__

  @enforce_keys [:id]
  defstruct [
    :id, # group id, unique in supervior.
    restart_strategy: :one_for_all, # default restart strategy for group is :one_for_all.
    supervisor: nil, # supervisor id (atom)
    partition: nil, # partition id holding the group.
  ]

  @type t :: %__MODULE__{
    id: any,
    restart_strategy: atom,
    supervisor: atom,
    partition: atom,
  }

  import SuperWorker.Supervisor.Utils

  require Logger

  ## Public functions

  @doc """
  Check, validate and convert key-value pairs to struct.
  """
  @spec check_options([ keyword]) :: {:ok, term} | {:error, atom | {atom, any}}
  def check_options(opts) do
    with {:ok, opts} <- normalize_opts(opts, @group_params),
         {:ok, opts} <- validate_restart_strategy(opts),
         {:ok, opts} <- validate_opts(opts),
         {:ok, group} <- map_to_struct(opts) do
      {:ok, group}
    else
      {:error, reason} = error ->
        Logger.error("SuperWorker, Group, incorrect options, #{inspect reason}")
        error
    end
  end

  @doc """
  Get worker from the group.
  """
  def get_worker(%Group{} = group, worker_id) do
    Logger.debug("SuperWorker, Group, supervisor #{inspect group.supervisor}, group #{inspect group.id}, get_worker: #{inspect worker_id}")
    case Registry.meta(group.supervisor, {:worker, {:group, group.id}, worker_id}) do
      {:ok, worker} -> {:ok, worker}
      :error -> {:error, :worker_not_found}
    end
  end

  @doc """
  Get all workers from the group.
  """
  def get_all_workers(%Group{} = group) do
    Logger.debug("SuperWorker, Group, get_all_workers: #{inspect group.supervisor}")

    result = Registry.lookup(group.supervisor, {:group, group.id})

    {:ok, result}
  end

  def count_workers(%Group{} = group) do
    {:ok, workers} = get_all_workers(group)

    length(workers)
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
  def add_worker(group = %Group{}, %Worker{} = worker)  do
    case get_worker(group, worker.id) do
      {:ok, _} -> {:error, :worker_exists}
      {:error, _} ->
        worker = %Worker{worker | parent: group.id}

        worker  =
          if !worker.id do
            %Worker{worker | id: Uniq.UUID.uuid4()}
          else
            worker
          end

        spawn_worker(group, worker)
    end
  end

  @doc """
  A internal function. Restart a worker in the group.
  """
  def restart_worker(group, worker) do
    kill_worker(group, worker, :restart)
    spawn_worker(group, worker)
  end

  def remove_worker(group, worker_id) do
    if worker_exists?(group, worker_id) do
      with {:ok, worker} <- get_worker(group, worker_id),
        {:ok, _} <- kill_worker(group, worker, :remove) do
          Registry.delete_meta(group.supervisor, {:worker, {:group, group.id}, worker_id})
          {:ok, :worker_removed}
      else
        {:error, reason} = error ->
          Logger.error("SuperWorker, Group, failed to kill worker #{inspect(worker_id)} in group #{inspect group.id}, error: #{inspect reason}")
          error
      end

      {:ok, group}
    else
      {:error, :worker_not_found}
    end
  end

  def kill_worker(group, worker = %Worker{}, reason) do
    if Process.alive?(worker.pid) do
      Logger.debug("SuperWorker, Group, group: #{inspect group.id}, kill_worker: #{inspect worker}, reason: #{inspect reason}")

      Process.exit(worker.pid, reason)
      {:ok, :killed}
    else
      {:error, :not_alive}
    end
  end
  def kill_worker(group, worker_id, reason) do
    case get_worker(group, worker_id) do
      {:ok, worker} ->
        kill_worker(group, worker, reason)
      {:error, _} -> {:error, :worker_not_found}
    end
  end

  def kill_all_workers(group, reason \\ :kill) do
    get_all_workers(group)
    |> Enum.each(fn %Worker{} = worker ->
      kill_worker(group, worker, reason)
    end)
  end

  defp spawn_worker(group, %Worker{} = worker) do
    Logger.debug("SuperWorker, Group, spawn_worker: #{inspect worker}")
    worker = do_spawn_worker(group, worker)

    # add or update data, ref, pid
    Registry.put_meta(group.supervisor, {:worker, {:group, group.id}, worker.id}, worker)
    Registry.register(group.supervisor, {:worker, :ref, worker.ref}, {{:group, group.id}, worker.id})

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
      Registry.register(group.supervisor, {:group, group.id}, worker.id)

      Process.put({:supervisor, :sup_id}, group.supervisor)
      Process.put({:supervisor, :group_id}, group.id)
      Process.put({:supervisor, :worker_id}, worker.id)

      if worker.name do
        if Process.whereis(worker.name) do
          Logger.warning("SuperWorker, Group, worker name already registered: #{inspect worker.name}")
        else
          Process.register(self(), worker.name)
        end
      end

      case worker.fun do
        {m, f, a} ->
          apply(m, f, a)

        {:fun, fun} ->
          fun.()
      end
    end)

    # Link to child for case supervisor is down.
    # TO-DO: Improve case worker crash immediately.
    Process.link(pid)

    worker
    |> Map.put(:pid, pid)
    |> Map.put(:ref, ref)
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
