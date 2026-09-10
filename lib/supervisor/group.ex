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
  @type worker_operation_result :: {:ok, t()} | {:error, atom()}

  alias __MODULE__
  alias SuperWorker.Supervisor.{Constants, Db, Validator, Worker}
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
  @spec get_worker(t(), any()) :: {:ok, Worker.t()} | {:error, atom()}
  def get_worker(group = %Group{}, worker_id) do
    SuperWorker.Log.debug(fn ->
      "SuperWorker, Group, supervisor #{inspect(group.supervisor)}, group #{inspect(group.id)}, get_worker: #{inspect(worker_id)}"
    end)

    worker_id =
      case worker_id do
        {_, id} -> id
        _ -> worker_id
      end

    Db.get_worker_info(group.table, worker_id, {:group, group.id})
  end

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
    {:ok, workers} = get_all_workers(group)
    Enum.count(workers)
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
    case get_worker(group, worker.id) do
      {:ok, _} ->
        {:error, :worker_exists}

      {:error, _} ->
        worker = %Worker{worker | parent: group.id}

        worker =
          case worker.id do
            nil -> %Worker{worker | id: Utils.random_id()}
            false -> %Worker{worker | id: Utils.random_id()}
            _ -> worker
          end

        Db.put_worker_info(group.table, worker)

        case spawn_worker(group, worker) do
          {:ok, _} = ok -> ok
          {:error, _} = error -> error
        end
    end
  end

  @doc """
  A internal function. Restart a worker in the group.
  """
  @spec restart_worker(t(), Worker.t() | any()) :: worker_operation_result()
  def restart_worker(group = %Group{}, worker = %Worker{}) do
    SuperWorker.Log.debug(fn -> "SuperWorker, Group, restart worker #{inspect(worker)}" end)

    case kill_worker(group, worker, :restart) do
      {:ok, _} ->
        # Worker was alive and killed, now spawn a new one
        spawn_worker(group, worker)

      {:error, :not_alive} ->
        # Worker process is already dead, clean up ETS and spawn new one
        SuperWorker.Log.debug(fn ->
          "SuperWorker, Group, worker #{inspect(worker.id)} already dead, spawning new one"
        end)

        Db.delete_worker_info(group.table, worker.id, {:group, group.id})
        spawn_worker(group, worker)

      {:error, :not_found} ->
        # Worker not in ETS, just spawn a new one
        SuperWorker.Log.debug(fn ->
          "SuperWorker, Group, worker #{inspect(worker.id)} not found in ETS, spawning new one"
        end)

        spawn_worker(group, worker)
    end
  end

  def restart_worker(group = %Group{}, worker_id) do
    SuperWorker.Log.debug(fn ->
      "SuperWorker, Group, restart worker by id #{inspect(worker_id)}"
    end)

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
        table = group.table
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
    case Db.get_worker_by_id(group.table, worker.id, {:group, group.id}) do
      {:ok, {_, pid}} ->
        if Process.alive?(pid) do
          SuperWorker.Log.debug(fn ->
            "SuperWorker, Group, group: #{inspect(group.id)}, kill_worker: #{inspect(worker)}, reason: #{inspect(reason)}"
          end)

          Process.exit(pid, reason)
          {:ok, :killed}
        else
          {:error, :not_alive}
        end

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

    results =
      Enum.map(list_worker, fn worker ->
        case kill_worker(group, worker, reason) do
          {:ok, _} ->
            :ok

          {:error, kill_reason} ->
            Logger.warning(
              "SuperWorker, Group, failed to kill worker #{inspect(worker.id)} in group #{inspect(group.id)}, reason: #{inspect(kill_reason)}"
            )

            {:error, worker.id, kill_reason}
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
      :throw, {:spawn_failed, reason} ->
        Logger.error(
          "SuperWorker, Group, failed to spawn worker #{inspect(worker.id)}: #{inspect(reason)}"
        )

        {:error, :spawn_failed}

      :exit, reason ->
        Logger.error(
          "SuperWorker, Group, failed to spawn worker #{inspect(worker.id)}: #{inspect(reason)}"
        )

        {:error, :spawn_failed}

      error, reason ->
        Logger.error(
          "SuperWorker, Group, unexpected error spawning worker #{inspect(worker.id)}: #{inspect(error)}: #{inspect(reason)}"
        )

        {:error, :spawn_failed}
    end
  end

  @spec broadcast(t(), any()) :: :ok | {:error, list()}
  def broadcast(group = %Group{}, message) do
    case Db.get_worker_pids_by_parent(group.table, {:group, group.id}) do
      {:ok, worker_pids} ->
        # Use Enum.each for side effects (sending messages)
        errors =
          Enum.map(worker_pids, fn {_worker_id, pid} ->
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
    case Db.get_worker_by_id(group.table, worker_id, {:group, group.id}) do
      {:ok, {_, pid}} ->
        send(pid, message)
        :ok

      error ->
        Logger.error(
          "SuperWorker, Group, send to worker #{inspect(worker_id)} failed, #{inspect(error)}"
        )

        {:error, :cannot_send}
    end
  end

  ## Private functions

  defp register_worker_name(nil), do: :ok

  defp register_worker_name(name) do
    case Process.whereis(name) do
      nil ->
        Process.register(self(), name)

      _pid ->
        Logger.warning("SuperWorker, Group, worker name already registered: #{inspect(name)}")
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
      {:error, "Invalid group restart strategy, #{inspect(options.restart_strategy)}"}
    end
  end

  defp validate_options(options) do
    validate_restart_strategy(options)
  end

  defp to_struct(options) when is_map(options) do
    fields =
      %Group{id: nil}
      |> Map.from_struct()
      |> Map.keys()

    result =
      %Group{} =
      Enum.reduce(fields, %Group{id: nil}, fn field, acc ->
        if Map.has_key?(options, field) do
          %{acc | field => Map.get(options, field)}
        else
          acc
        end
      end)

    {:ok, result}
  end
end
