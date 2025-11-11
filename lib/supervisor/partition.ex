defmodule SuperWorker.Supervisor.Partition do
  alias SuperWorker.Supervisor.Db

  import SuperWorker.Supervisor.Utils, only: [get_hash_order: 2]

  require Logger

  @spec get_host_partition(atom, any) :: {:error, atom} | {:ok, atom, pid}
  def get_host_partition(sup_id, data) do
    with {:ok, sup} <- Db.get_sup_info(sup_id, :master),
         partition_id <- get_target_partition(sup_id, data, sup.number_of_partitions),
         {:ok, pid} <- get_partition_pid(sup_id, partition_id) do
      {:ok, partition_id, pid}
    else
      error ->
        Logger.error(
          "SuperWorker, Supervisor, get partition pid failed: #{inspect(error)}, data: #{inspect(data)}, sup_id: #{inspect(sup_id)}"
        )

        error
    end
  end

  @spec get_target_partition(atom(), any(), integer()) :: atom()
  defp get_target_partition(prefix, data, num_partitions) when is_integer(num_partitions) do
    partition_id = get_hash_order(data, num_partitions)
    get_partition_id(prefix, partition_id)
  end

  @spec get_partition_pid(atom(), atom()) :: {:error, atom()} | {:ok, pid()}
  defp get_partition_pid(sup_id, partition_id) do
    case Db.get_sup_pid(sup_id, partition_id) do
      {:ok, pid} ->
        {:ok, pid}

      _ ->
        Logger.error(
          "SuperWorker, Supervisor, supervisor #{inspect(sup_id)} partition not found: #{inspect(partition_id)}"
        )

        {:error, {:partition_not_found, partition_id}}
    end
  end

  @spec get_partition_id(atom(), integer()) :: atom()
  defp get_partition_id(sup_id, partition_id) do
    if partition_id < 0 do
      sup_id
    else
      String.to_atom("#{Atom.to_string(sup_id)}_#{inspect(partition_id)}")
    end
  end
end
