defmodule SuperWorker.Supervisor.Group do
  @moduledoc """
  Documentation for `SuperWorker.Supervisor.Group`.
  """

  # Parameters for group.
  @group_params [:id, :restart_strategy, :type, :max_restarts, :max_seconds, :auto_restart_time]

  # Restart strategies for group.
  @group_restart_strategies [:one_for_one, :one_for_all]

  alias SuperWorker.Supervisor.{Worker, Db, Validator}

  alias __MODULE__

  @enforce_keys [:id]
  defstruct [
    # group id, unique in supervior.
    :id,
    # default restart strategy for group is :one_for_all.
    restart_strategy: :one_for_all,
    # supervisor id (atom)
    supervisor: nil,
    # partition id holding the group.
    partition: nil
  ]

  @type t :: %__MODULE__{
          id: any,
          restart_strategy: atom,
          supervisor: atom,
          partition: atom
        }

  require Logger

  ## Public functions

  @doc """
  Check, validate and convert key-value pairs to struct.
  """
  @spec check_options([keyword]) :: {:ok, %Group{}} | {:error, atom | {atom, any}}
  def check_options(opts) do
    with {:ok, opts} <- Validator.normalize_options(opts, @group_params),
         {:ok, opts} <- validate_restart_strategy(opts),
         {:ok, opts} <- validate_opts(opts),
         {:ok, group} <- to_struct(opts) do
      {:ok, group}
    else
      {:error, reason} = error ->
        Logger.error("SuperWorker, Group, incorrect options, #{inspect(reason)}")
        error
    end
  end

  @doc """
  Get worker from the group.
  """
  def get_worker(%Group{} = group, worker_id) do
    Logger.debug(
      "SuperWorker, Group, supervisor #{inspect(group.supervisor)}, group #{inspect(group.id)}, get_worker: #{inspect(worker_id)}"
    )

    worker_id =
      case worker_id do
        {_, id} -> id
        _ -> worker_id
      end

    Db.get_worker_info(group.supervisor, worker_id, {:group, group.id})
  end

  @doc """
  Get all workers from the group.
  """
  def get_all_workers(%Group{} = group) do
    Logger.debug(
      "SuperWorker, Group, get_all_workers for supervisor #{inspect(group.supervisor)}"
    )

    Db.get_worker_infos_by_parent(group.supervisor, {:group, group.id})
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
  def add_worker(group = %Group{}, %Worker{} = worker) do
    case get_worker(group, worker.id) do
      {:ok, _} ->
        {:error, :worker_exists}

      {:error, _} ->
        worker = %Worker{worker | parent: group.id}

        worker =
          if !worker.id do
            %Worker{worker | id: SuperWorker.Supervisor.Utils.random_id()}
          else
            worker
          end

        Db.put_worker_info(group.supervisor, worker)

        spawn_worker(group, worker)
    end
  end

  @doc """
  A internal function. Restart a worker in the group.
  """
  def restart_worker(group = %Group{}, worker = %Worker{}) do
    Logger.debug("SuperWoker, Group, restart worker #{inspect(worker)}")
    kill_worker(group, worker, :restart)
    spawn_worker(group, worker)
  end

  def restart_worker(group = %Group{}, worker_id) do
    Logger.debug("SuperWoker, Group, restart worker by id #{inspect(worker_id)}")

    case get_worker(group, worker_id) do
      {:ok, worker} ->
        restart_worker(group, worker)

      {:error, _} = error ->
        Logger.error(
          "SuperWorker, Group, cannot get worker #{inspect(worker_id)}, #{inspect(error)}"
        )

        {:error, :worker_not_found}
    end
  end

  def remove_worker(group = %Group{}, worker_id) do
    if worker_exists?(group, worker_id) do
      with {:ok, worker} <- get_worker(group, worker_id),
           {:ok, _} <- kill_worker(group, worker, :removed) do
        table = group.supervisor
        parent = {:group, group.id}
        Db.delete_worker_by_id(table, worker_id, parent)
        Db.delete_worker_info(table, worker_id, parent)

        {:ok, :worker_removed}
      else
        {:error, reason} = error ->
          Logger.error(
            "SuperWorker, Group, failed to kill worker #{inspect(worker_id)} in group #{inspect(group.id)}, error: #{inspect(reason)}"
          )

          error
      end
    else
      {:error, :worker_not_found}
    end
  end

  def kill_worker(group = %Group{}, worker = %Worker{}, reason) do
    with {:ok, {_, pid}} <- Db.get_worker_by_id(group.supervisor, worker.id, {:group, group.id}) do
      if Process.alive?(pid) do
        Logger.debug(
          "SuperWorker, Group, group: #{inspect(group.id)}, kill_worker: #{inspect(worker)}, reason: #{inspect(reason)}"
        )

        Process.exit(pid, reason)
        {:ok, :killed}
      else
        {:error, :not_alive}
      end
    else
      _ ->
        {:error, :not_found}
    end
  end

  def kill_worker(group = %Group{}, worker_id, reason) do
    case get_worker(group, worker_id) do
      {:ok, worker} ->
        kill_worker(group, worker, reason)

      {:error, _} ->
        {:error, :worker_not_found}
    end
  end

  def kill_all_workers(group = %Group{}, reason \\ :kill) do
    {:ok, list_worker} = get_all_workers(group)

    Enum.each(list_worker, fn worker ->
      kill_worker(group, worker, reason)
    end)
  end

  defp spawn_worker(group = %Group{}, %Worker{} = worker) do
    Logger.debug("SuperWorker, Group, spawn_worker: #{inspect(worker)}")
    do_spawn_worker(group, worker)

    {:ok, group}
  end

  def broadcast(group = %Group{}, message) do
    Group.get_all_workers(group)
    |> Enum.each(fn %Worker{id: worker_id} ->
      {:ok, worker} = get_worker(group, worker_id)
      send(worker.pid, message)
    end)
  end

  def send_message(group = %Group{}, worker_id, message) do
    with {:ok, {_, pid}} <- Db.get_worker_by_id(group.supervisor, worker_id, {:group, group.id}) do
      send(pid, message)
    else
      error ->
        Logger.error(
          "SuperWorker, Group, send to worker #{inspect(worker_id)} failed, #{inspect(error)}"
        )

        {:error, :cannot_send}
    end
  end

  ## Private functions

  defp do_spawn_worker(group, %Worker{} = worker) do
    {pid, ref} =
      case worker.fun do
        {:gen_server, {m, f, a}} ->
          {:ok, pid} = apply(m, f, a)

          ref = Process.monitor(pid)

          {pid, ref}

        _ ->
          spawn_monitor(fn ->
            Process.put({:supervisor, :sup_id}, group.supervisor)
            Process.put({:supervisor, :group_id}, group.id)
            Process.put({:supervisor, :worker_id}, worker.id)

            if worker.name do
              if Process.whereis(worker.name) do
                Logger.warning(
                  "SuperWorker, Group, worker name already registered: #{inspect(worker.name)}"
                )
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
      end

    Db.put_worker(group.supervisor, ref, worker.id, {:group, group.id}, pid)

    Logger.debug(
      "SuperWorker, Group, spawned worker #{inspect(worker.id)}, pid: #{inspect(pid)}, ref: #{inspect(ref)}"
    )

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
      {:error, "Invalid group restart strategy, #{inspect(opts.restart_strategy)}"}
    end
  end

  defp validate_opts(opts) do
    # TO-DO: Implement the validation
    {:ok, opts}
  end

  defp to_struct(opts) when is_map(opts) do
    fields =
      %Group{id: nil}
      |> Map.from_struct()
      |> Map.keys()

    result =
      %Group{} =
      Enum.reduce(fields, %Group{id: nil}, fn field, acc ->
        if Map.has_key?(opts, field) do
          %{acc | field => Map.get(opts, field)}
        else
          acc
        end
      end)

    {:ok, result}
  end
end
