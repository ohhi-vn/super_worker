defmodule SuperWorker.Supervisor.Group do
  @moduledoc """
  Documentation for `SuperWorker.Supervisor.Group`.
  """

  @group_params [:id, :restart_strategy, :type, :max_restarts, :max_seconds, :auto_restart_time]

  @group_restart_strategies [:one_for_one, :one_for_all]

  @child_params [:id, :group_id]

  @enforce_keys [:id]
  defstruct id: nil, restart_strategy: :one_for_all, workers: %{}

  import SuperWorker.Supervisor.Utils

  ## Public functions

  def check_options(opts) do
    with {:ok, opts} <- normalize_opts(opts, @group_params),
         {:ok, opts} <- validate_restart_strategy(opts),
         {:ok, opts} <- validate_opts(opts) do
      {:ok, opts}
    end
  end

  def check_worker_opts(opts) do
    with {:ok, opts} <- normalize_opts(opts, @child_params),
     {:ok, opts} <- validate_opts(opts) do
      {:ok, opts}
    end
  end

  def get_worker(group, worker_id) do
    case Map.get(group.workers, worker_id) do
      nil -> {:error, "Worker not found"}
      worker -> {:ok, worker}
    end
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
      {:error, _} -> {:error, "Worker not found"}
    end
  end

  def kill_worker(group, worker_id) do
    case get_worker(group, worker_id) do
      {:ok, worker} ->
        case Process.whereis(worker.pid) do
          nil -> {:error, "Worker is not running"}
          pid -> Process.exit(pid, :kill)
        end
      {:error, _} -> {:error, "Worker not found"}
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

  defp spawn_worker(group, worker_id) do
    {:ok, worker} = get_worker(group, worker_id)
    case worker.mfa do
      {module, function, args} ->
        {pid, ref} = spawn_monitor(module, function, args)
        worker =
          worker
          |> Map.put(:pid, pid)
          |> Map.put(:ref, ref)

        workers = Map.put(group.workers, worker.id, worker)
        {:ok, %{group | workers: workers}}

      {:fun, fun} ->
        {pid, ref} = spawn_link(fun)
        worker =
          worker
          |> Map.put(:pid, pid)
          |> Map.put(:ref, ref)

        workers = Map.put(group.workers, worker.id, worker)
        {:ok, %{group | workers: workers}}
    end
  end

  ## Private functions

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
end
