defmodule SuperWorker.Supervisor.Group do
  @moduledoc """
  Documentation for `SuperWorker.Supervisor.Group`.
  """

  @group_params [:id, :restart_strategy, :type, :max_restarts, :max_seconds, :auto_restart_time]

  @group_restart_strategies [:one_for_one, :one_for_all]

  @child_params [:id, :group_id]

  @enforce_keys [:id]
  defstruct [
    :id, # group id, unique in supervior.
    restart_strategy: :one_for_all,
    workers: %{},
    supervisor: nil,
  ]

  import SuperWorker.Supervisor.Utils

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

  def check_worker_opts(opts) do
    with {:ok, opts} <- normalize_opts(opts, @child_params),
     {:ok, opts} <- validate_opts(opts) do
      {:ok, opts}
    end
  end

  def get_worker(group, worker_id) do
    get_item(group.workers, worker_id)
  end

  def worker_exists?(group, worker_id) do
    Map.has_key?(group.workers, worker_id)
  end

  def add_worker(group, worker) when is_map(worker) do
    case get_worker(group, worker.id) do
      {:ok, _} -> {:error, "Worker already exists"}
      {:error, _} ->
        workers = Map.put(group.workers, worker.id, worker)
        group
        |> Map.put(:workers, workers)
        |> spawn_worker(worker.id)
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
    case get_worker(group, worker_id) do
      {:ok, _} ->
        workers = Map.delete(group.workers, worker_id)
        {:ok, %{group | workers: workers}}
      {:error, _} -> {:error, :not_found}
    end
  end

  def kill_worker(group, worker_id) do
    case get_worker(group, worker_id) do
      {:ok, worker} ->
        case Process.whereis(worker.pid) do
          nil -> {:error, :not_running}
          pid -> Process.exit(pid, :kill)
        end
      {:error, _} -> {:error, :not_found}
    end
  end

  def kill_all_workers(group) do
    Enum.each(group.workers, fn {_id, worker} ->
      case Process.whereis(worker.pid) do
        nil -> :ok
        pid -> Process.exit(pid, :kill)
      end
    end)
  end

  def restart_all_workers(group) do
    workers =
      Enum.map(group.workers, fn {id, worker} ->
        Logger.info("Restarting worker #{id}, pid: #{inspect worker.pid}")
        Process.exit(worker.pid, :kill)
        new_worker = do_spawn_worker(group, worker)
        {id, new_worker}
      end)

    {:ok, %{group | workers: workers}}
  end

  defp spawn_worker(group, worker_id) do
    {:ok, worker} = get_worker(group, worker_id)
    worker = do_spawn_worker(group, worker)
    workers = Map.put(group.workers, worker.id, worker)

    {:ok, %{group | workers: workers}}
  end

  def broadcast(group, message) do
    Enum.each(group.workers, fn {_id, worker} ->
      send(worker.pid, message)
    end)
  end

  ## Private functions

  defp do_spawn_worker(group, worker) do
    {pid, ref} = spawn_monitor(fn ->
      Process.put(:supervisor, group.supervisor)
      Process.put(:group, group.id)

      case worker.mfa do
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
