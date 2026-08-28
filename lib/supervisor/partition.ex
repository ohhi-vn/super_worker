defmodule SuperWorker.Supervisor.Partition do
  @moduledoc """
  Partition processes of a `SuperWorker.Supervisor`.

  A supervisor spreads its workers over one or more partitions. Each
  partition is a plain process running `Looper.main_loop/1`, owns the worker
  processes hashed to it, and traps exits so crashing workers never kill the
  partition itself.

  Partitions are monitored (not linked) by the supervisor master, so a
  crashed partition is restarted individually without affecting the rest of
  the supervisor (see `restart_partition/2`). In turn, every partition
  monitors its master and stops when the master goes away.
  """

  alias SuperWorker.Supervisor
  alias SuperWorker.Supervisor.{Db, Looper}

  alias __MODULE__

  require Logger
  require SuperWorker.Log

  @doc """
  Entry point of a partition process.

  Registers the partition in ETS, notifies the supervisor master, turns the
  process into a system process (traps exits), starts monitoring the master
  process and enters the main loop.
  """
  def start_partition(state) do
    Db.put_sup_pid(state.table, state.id, self())

    send(state.master, {:partition_started, state.id})

    # Turn partition process to system process.
    Process.flag(:trap_exit, true)

    state =
      case master_pid(state) do
        nil ->
          Logger.error(
            "SuperWorker, Supervisor, partition #{inspect(state.id)} cannot find master #{inspect(state.master)}, it will not be tied to the supervisor lifetime"
          )

          state

        pid ->
          %{state | master_monitor: Process.monitor(pid)}
      end

    Looper.main_loop(state)
  end

  def init_additional_partitions(supervisor = %Supervisor{}) do
    SuperWorker.Log.debug(fn ->
      "SuperWorker, Supervisor, [#{inspect(supervisor.id)}] init additional partitions, options: #{inspect(supervisor)}"
    end)

    Enum.map(1..supervisor.num_partitions, fn i ->
      SuperWorker.Log.debug(fn ->
        "SuperWorker, Supervisor, [#{inspect(supervisor.id)}] add partition: #{inspect(i)}"
      end)

      {:ok, partition_id, pid, ref} = restart_partition(supervisor, i)
      {partition_id, {pid, ref}}
    end)
    |> Enum.into(%{})
    |> then(fn partitions_by_ref ->
      {
        Map.new(partitions_by_ref, fn {id, {pid, _ref}} -> {id, pid} end),
        Map.new(partitions_by_ref, fn {id, {_pid, ref}} -> {ref, id} end)
      }
    end)
  end

  @doc """
  Start a single partition process for the given partition id.

  Used at supervisor init and to restart a crashed partition without
  touching other partitions.
  """
  @spec restart_partition(Supervisor.t(), pos_integer()) ::
          {:ok, pos_integer(), pid(), reference()} | {:error, term()}
  def restart_partition(supervisor = %Supervisor{}, partition_id)
      when is_integer(partition_id) and partition_id > 0 do
    supervisor =
      %{supervisor | master: supervisor.id, master_pid: self(), id: partition_id}

    # self/0 is the supervisor master process here; partitions need the
    # actual pid because the master name is not registered yet while it is
    # still inside its own init callback.

    init_partition(supervisor)
  end

  ## Private functions

  # Partitions are started from the master's own init callback, before the
  # master name is registered, so resolve the master by the pid stored in
  # the config instead of Process.whereis/1.
  defp init_partition(partition = %Supervisor{}) do
    # Start the main loop. Monitored (not linked): a crashed partition must
    # not take down the supervisor master.
    {pid, ref} = spawn_monitor(Partition, :start_partition, [partition])

    SuperWorker.Log.debug(fn ->
      "SuperWorker, Supervisor, #{inspect(partition.id)} initialized, pid: #{inspect(pid)}"
    end)

    {:ok, partition.id, pid, ref}
  end

  defp master_pid(%Supervisor{master_pid: pid}) when is_pid(pid), do: pid

  defp master_pid(_state), do: nil
end
