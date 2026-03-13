defmodule SuperWorker.Supervisor.Partition do
  alias SuperWorker.Supervisor
  alias Supervisor.{Db, Looper}

  alias __MODULE__

  require Logger
  require SuperWorker.Log

  def start_partition(state) do
    Db.put_sup_pid(state.table, state.id, self())

    send(state.master, {:partition_started, state.id})

    # Turn partition process to system process.
    Process.flag(:trap_exit, true)

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

      supervisor =
        supervisor
        |> Map.put(:master, supervisor.id)
        |> Map.put(:id, i)

      {:ok, partition, pid} = init_partition(supervisor)
      {partition, pid}
    end)
    |> Enum.into(%{})
  end

  ## Private functions

  defp init_partition(partition = %Supervisor{}) do
    # Start the main loop
    pid = spawn_link(Partition, :start_partition, [partition])

    SuperWorker.Log.debug(fn ->
      "SuperWorker, Supervisor, #{inspect(partition.id)} initialized, pid: #{inspect(pid)}"
    end)

    {:ok, partition.id, pid}
  end
end
