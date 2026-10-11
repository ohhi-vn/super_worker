defmodule SuperWorker.Supervisor.Group do
  @moduledoc """
  Documentation for `SuperWorker.Supervisor.Group`.
  """

  # Parameters for group.
  @group_params [:id, :restart_strategy, :type, :max_restarts, :max_seconds, :auto_restart_time]

  @enforce_keys [:id]
  defstruct [
    # group id, unique in supervior.
    :id,
    # default restart strategy for group is :one_for_all.
    restart_strategy: :one_for_all,
    # supervisor id (atom)
    supervisor: nil,

    # table data of supervisors.
    table: nil
  ]

  @type t :: %__MODULE__{
          id: any(),
          restart_strategy: atom(),
          supervisor: atom() | nil,
          table: atom() | nil
        }

  @type check_options_result :: {:ok, t()} | {:error, atom() | {atom(), any()}}
  @type worker_operation_result :: {:ok, t()} | {:error, atom() | {atom(), any()}}
  @type worker_ref_result :: {:ok, :killed} | {:error, atom() | {atom(), any()}}

  alias __MODULE__
  alias SuperWorker.Supervisor.{Constants, Db, Validator, Worker}
  alias SuperWorker.Supervisor.ErrorHandler
  alias SuperWorker.Supervisor.Utils

  require Logger
  require SuperWorker.Log

  ## Public functions

  @doc """
  Check, validate and convert key-value pairs to struct.
  """
  @spec check_options([atom() | keyword()]) :: check_options_result()
  def check_options(options) do
    with {:ok, options} <- Validator.normalize_options(options, @group_params),
         {:ok, options} <- validate_options(options),
         {:ok, group} <- to_struct(options) do
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
  @spec get_worker(t(), any()) :: {:ok, Worker.t()} | {:error, atom() | {atom(), any()}}
  def get_worker(group = %Group{}, worker_id) do
    SuperWorker.Log.debug(fn ->
      "SuperWorker, Group, supervisor #{inspect(group.supervisor)}, group #{inspect(group.id)}, get_worker: #{inspect(worker_id)}"
    end)

    worker_id = normalize_worker_id(worker_id)

    case Db.get_worker_info(group.table, worker_id, {:group, group.id}) do
      {:ok, _} = ok -> ok
      {:error, _} -> ErrorHandler.not_found(:worker)
    end
  end

  # Worker ids may arrive as {_, id} tuples from partition routing.
  # Unwrap consistently so every entry point resolves the same logical id.
  defp normalize_worker_id({_, id}), do: id
  defp normalize_worker_id(id), do: id

  @doc """
  Get all workers from the group.
  """
  @spec get_all_workers(t()) :: {:ok, [Worker.t()]}
  def get_all_workers(group = %Group{}) do
    SuperWorker.Log.debug(fn ->
      "SuperWorker, Group, get_all_workers for supervisor #{inspect(group.supervisor)}"
    end)

    Db.get_worker_infos_by_parent(group.table, {:group, group.id})
  end

  @spec count_workers(t()) :: non_neg_integer()
  def count_workers(group = %Group{}) do
    {:ok, count} = Db.count_workers_by_parent(group.table, {:group, group.id})
    count
  end

  @doc """
  Check if worker exists in the group.
  """
  @spec worker_exists?(t(), any()) :: boolean()
  def worker_exists?(group, worker_id) do
    case get_worker(group, worker_id) do
      {:ok, _} -> true
      {:error, _} -> false
    end
  end

  @doc """
  A internal function. Add a worker to the group.
  """
  @spec add_worker(t(), Worker.t()) :: worker_operation_result()
  def add_worker(group = %Group{}, worker = %Worker{}) do
    worker = %Worker{worker | parent: group.id}

    worker =
      case normalize_worker_id(worker.id) do
        nil -> %Worker{worker | id: Utils.random_id()}
        false -> %Worker{worker | id: Utils.random_id()}
        id -> %Worker{worker | id: id}
      end

    # Single info-row read: no separate exists? round-trip.
    case Db.get_worker_info(group.table, worker.id, {:group, group.id}) do
      {:ok, _} ->
        ErrorHandler.already_exists(:worker)

      {:error, _} ->
        case spawn_worker(group, worker) do
          {:ok, _} = ok ->
            Db.put_worker_info(group.table, worker)
            ok

          {:error, _} = error ->
            # Spawn failed: ensure no orphan rows remain.
            Db.delete_worker_by_id(group.table, worker.id, {:group, group.id})
            Db.delete_worker_info(group.table, worker.id, {:group, group.id})
            error
        end
    end
  end

  @doc """
  A internal function. Restart a worker in the group.
  """
  @spec restart_worker(t(), Worker.t() | any()) :: worker_operation_result()
  def restart_worker(group = %Group{}, worker = %Worker{}) do
    SuperWorker.Log.debug(fn -> "SuperWorker, Group, restart worker #{inspect(worker)}" end)

    worker = %Worker{worker | parent: group.id, id: normalize_worker_id(worker.id)}

    case kill_worker(group, worker.id, :restart) do
      {:ok, _} ->
        # Worker was alive and killed, now spawn a new one
        spawn_worker(group, worker)

      {:error, :worker_not_found} ->
        # Stale or missing ref row: clean both row kinds, then respawn.
        SuperWorker.Log.debug(fn ->
          "SuperWorker, Group, worker #{inspect(worker.id)} dead or missing, cleaning stale rows and spawning new one"
        end)

        Db.delete_worker_by_id(group.table, worker.id, {:group, group.id})
        Db.delete_worker_info(group.table, worker.id, {:group, group.id})
        spawn_worker(group, worker)
    end
  end

  def restart_worker(group = %Group{}, worker_id) do
    SuperWorker.Log.debug(fn ->
      "SuperWorker, Group, restart worker by id #{inspect(worker_id)}"
    end)

    worker_id = normalize_worker_id(worker_id)

    case Db.get_worker_info(group.table, worker_id, {:group, group.id}) do
      {:ok, worker} ->
        restart_worker(group, worker)

      {:error, _} ->
        Logger.error(
          "SuperWorker, Group, cannot get worker #{inspect(worker_id)} for restart"
        )

        ErrorHandler.not_found(:worker)
    end
  end

  def remove_worker(group = %Group{}, worker_id) do
    worker_id = normalize_worker_id(worker_id)
    parent = {:group, group.id}

    # Single info read + single ref read; no exists? pre-check round-trip.
    with {:ok, _worker} <- Db.get_worker_info(group.table, worker_id, parent),
         {:ok, {ref, pid}} <- Db.get_worker_by_id(group.table, worker_id, parent) do
      if Process.alive?(pid) do
        Process.exit(pid, :removed)
        # Delete by key: no extra lookup round-trip.
        Db.delete_worker(group.table, ref)
        Db.delete_worker_info(group.table, worker_id, parent)

        {:ok, :worker_removed}
      else
        Logger.error(
          "SuperWorker, Group, failed to remove dead worker #{inspect(worker_id)} in group #{inspect(group.id)}"
        )

        ErrorHandler.not_found(:worker)
      end
    else
      {:error, _} ->
        ErrorHandler.not_found(:worker)
    end
  end

  def kill_worker(group = %Group{}, worker = %Worker{}, reason) do
    do_kill_worker(group, normalize_worker_id(worker.id), reason)
  end

  def kill_worker(group = %Group{}, worker_id, reason) do
    do_kill_worker(group, normalize_worker_id(worker_id), reason)
  end

  defp do_kill_worker(group = %Group{}, worker_id, reason) do
    case Db.get_worker_by_id(group.table, worker_id, {:group, group.id}) do
      {:ok, {_, pid}} ->
        if Process.alive?(pid) do
          SuperWorker.Log.debug(fn ->
            "SuperWorker, Group, group: #{inspect(group.id)}, kill_worker: #{inspect(worker_id)}, reason: #{inspect(reason)}"
          end)

          Process.exit(pid, reason)
          {:ok, :killed}
        else
          ErrorHandler.not_found(:worker)
        end

      {:error, _} ->
        ErrorHandler.not_found(:worker)
    end
  end

  def kill_all_workers(group = %Group{}, reason \\ :kill) do
    # Pid-only select: one ETS scan, no per-worker ref lookups, no full structs.
    {:ok, worker_pids} = Db.get_worker_pids_by_parent(group.table, {:group, group.id})

    results =
      Enum.map(worker_pids, fn {worker_id, pid} ->
        if Process.alive?(pid) do
          Process.exit(pid, reason)
          :ok
        else
          Logger.warning(
            "SuperWorker, Group, failed to kill worker #{inspect(worker_id)} in group #{inspect(group.id)}, reason: :worker_not_found"
          )

          {:error, worker_id, :worker_not_found}
        end
      end)

    errors = Enum.filter(results, &match?({:error, _, _}, &1))

    if Enum.empty?(errors) do
      :ok
    else
      {:error, errors}
    end
  end

  defp spawn_worker(group = %Group{}, worker = %Worker{}) do
    SuperWorker.Log.debug(fn -> "SuperWorker, Group, spawn_worker: #{inspect(worker)}" end)

    try do
      case do_spawn_worker(group, worker) do
        {:ok, _worker} -> {:ok, group}
      end
    catch
      # A failed GenServer start throws {:spawn_failed, reason}; convert it to
      # an error tuple instead of crashing the partition process.
      # All failure shapes preserve the cause and clean the ref row so no
      # orphan state remains for restart/add callers to compensate.
      :throw, {:spawn_failed, reason} ->
        Logger.error(
          "SuperWorker, Group, failed to spawn worker #{inspect(worker.id)}: #{inspect(reason)}"
        )

        Db.delete_worker_by_id(group.table, worker.id, {:group, group.id})
        {:error, {:spawn_failed, reason}}

      :throw, {:link_failed, reason} ->
        Logger.error(
          "SuperWorker, Group, failed to link worker #{inspect(worker.id)}: #{inspect(reason)}"
        )

        Db.delete_worker_by_id(group.table, worker.id, {:group, group.id})
        {:error, {:spawn_failed, {:link_failed, reason}}}

      :exit, reason ->
        Logger.error(
          "SuperWorker, Group, failed to spawn worker #{inspect(worker.id)}: #{inspect(reason)}"
        )

        Db.delete_worker_by_id(group.table, worker.id, {:group, group.id})
        {:error, {:spawn_failed, {:exit, reason}}}

      error, reason ->
        Logger.error(
          "SuperWorker, Group, unexpected error spawning worker #{inspect(worker.id)}: #{inspect(error)}: #{inspect(reason)}"
        )

        Db.delete_worker_by_id(group.table, worker.id, {:group, group.id})
        {:error, {:spawn_failed, {error, reason}}}
    end
  end

  @spec broadcast(t(), any()) :: :ok | {:error, list()}
  def broadcast(group = %Group{}, message) do
    case Db.get_worker_pids_by_parent(group.table, {:group, group.id}) do
      {:ok, worker_pids} ->
        errors =
          Enum.map(worker_pids, fn {_worker_id, pid} ->
            if Process.alive?(pid) do
              try do
                send(pid, message)
                :ok
              catch
                :exit, reason ->
                  Logger.error(
                    "SuperWorker, Group, failed to send to pid #{inspect(pid)}: #{inspect(reason)}"
                  )

                  {:error, pid, reason}
              end
            else
              {:error, pid, :worker_not_found}
            end
          end)
          |> Enum.filter(&match?({:error, _, _}, &1))

        if Enum.empty?(errors) do
          :ok
        else
          {:error, errors}
        end
    end
  end

  def send_message(group = %Group{}, worker_id, message) do
    worker_id = normalize_worker_id(worker_id)

    case Db.get_worker_by_id(group.table, worker_id, {:group, group.id}) do
      {:ok, {_, pid}} ->
        if Process.alive?(pid) do
          send(pid, message)
          :ok
        else
          Logger.error(
            "SuperWorker, Group, send to dead worker #{inspect(worker_id)} failed"
          )

          ErrorHandler.not_found(:worker)
        end

      {:error, _} ->
        Logger.error(
          "SuperWorker, Group, send to worker #{inspect(worker_id)} failed: :worker_not_found"
        )

        ErrorHandler.not_found(:worker)
    end
  end

  ## Private functions

  defp register_worker_name(nil), do: :ok

  defp register_worker_name(name) do
    try do
      Process.register(self(), name)
      :ok
    rescue
      ArgumentError ->
        Logger.warning("SuperWorker, Group, worker name already registered: #{inspect(name)}")
        :ok
    end
  end

  defp do_spawn_worker(group, worker = %Worker{}) do
    {pid, ref} =
      case worker.fun do
        {:gen_server, {m, f, a}} ->
          case apply(m, f, a) do
            {:ok, pid} ->
              ref = Process.monitor(pid)
              {pid, ref}

            error ->
              Logger.error("SuperWorker, Group, GenServer start failed: #{inspect(error)}")
              throw({:spawn_failed, error})
          end

        _ ->
          spawn_monitor(fn ->
            Process.put({:supervisor, :sup_id}, group.supervisor)
            Process.put({:supervisor, :group_id}, group.id)
            Process.put({:supervisor, :worker_id}, worker.id)

            register_worker_name(worker.name)

            SuperWorker.Log.debug(fn ->
              "SuperWorker, Group, worker #{inspect(worker.id)} started, fun: #{inspect(worker.fun)}"
            end)

            case worker.fun do
              {m, f, a} ->
                apply(m, f, a)

              {:fun, fun} ->
                fun.()
            end
          end)
      end

    Db.put_worker(group.table, ref, worker.id, {:group, group.id}, pid)

    SuperWorker.Log.debug(fn ->
      "SuperWorker, Group, spawned worker #{inspect(worker.id)}, pid: #{inspect(pid)}, ref: #{inspect(ref)}"
    end)

    try do
      Process.link(pid)
    catch
      :exit, reason ->
        Logger.error(
          "SuperWorker, Group, failed to link worker #{inspect(worker.id)}: #{inspect(reason)}"
        )

        Process.exit(pid, :kill)
        throw({:link_failed, reason})
    end

    {:ok, worker}
  end

  defp validate_restart_strategy(options) do
    if options.restart_strategy in Constants.Strategies.group_restart_strategies() do
      {:ok, options}
    else
      {:error,
       {:invalid_options, {:invalid_restart_strategy, options.restart_strategy}}}
    end
  end

  defp validate_id(options) do
    case Map.get(options, :id) do
      nil -> {:error, {:invalid_options, {:missing_id, nil}}}
      _ -> {:ok, options}
    end
  end

  defp validate_options(options) do
    with {:ok, options} <- validate_id(options) do
      validate_restart_strategy(options)
    end
  end

  defp to_struct(options) when is_map(options) do
    fields = Map.keys(%Group{id: nil})

    {:ok, struct(Group, Map.take(options, fields))}
  end
end
