defmodule SuperWorker.Supervisor.Db do
  alias :ets, as: Ets

  alias SuperWorker.Supervisor.{Worker, Group, Chain}

  require Logger

  def init(sup_name) when is_atom(sup_name) do
    ^sup_name =
      Ets.new(sup_name, [
        :set,
        :public,
        :named_table,
        {:write_concurrency, true},
        {:read_concurrency, true}
      ])

    Logger.debug("SuperWorker, Db, created table for supervisor #{inspect(sup_name)}")

    sup_name
  end

  def put_worker(table, ref, worker_id, parent, pid) do
    Ets.insert_new(table, {{:ref, ref}, worker_id, parent, pid})
  end

  def get_worker(table, ref) do
    with {:ok, {_, worker_id, parent, pid}} <- lookup(table, {:ref, ref}) do
      {worker_id, parent, pid}
    end
  end

  def get_worker_by_id(table, worker_id, parent) do
    case Ets.match_object(table, {{:ref, :_}, worker_id, parent, :_}) do
      [{{_, ref}, _, _, pid}] -> {:ok, {ref, pid}}
      [] -> {:error, :not_found}
    end
  end

  def delete_worker(table, ref) do
    Ets.delete(table, {:ref, ref})
  end

  def delete_worker_by_id(table, worker_id, parent) do
    with {:ok, {ref, _}} <- get_worker_by_id(table, worker_id, parent) do
      delete_worker(table, ref)
    end
  end

  def get_worker_info_by_ref(table, ref) do
    with {:ok, {{:ref, _ref}, worker_id, parent, _pid}} <- get_worker(table, ref) do
      get_worker_info(table, worker_id, parent)
    end
  end

  def put_chain_order(table, worker_id, chain_id, order, pid) do
    Ets.insert_new(table, {{:chain_order, chain_id, order}, {worker_id, pid}})
  end

  def get_chain_order(table, chain_id, order) do
    with {:ok, {_, data}} <- lookup(table, {:chain, chain_id, order}) do
      {:ok, data}
    else
      _ ->
        Logger.info("SuperWorker, Db, chain order not found")
        {:error, :not_found}
    end
  end

  def delete_chain_order(table, chain_id, order) do
    Ets.delete(table, {:chain_order, chain_id, order})
  end

  def get_workers_by_parent(table, parent) do
    Ets.match_object(table, {:_, :_, parent, :_})
    |> Enum.map(fn {_, worker_id, _, pid} -> {worker_id, pid} end)
  end

  def put_worker_info(table, %Worker{} = worker_info) do
    Ets.insert_new(
      table,
      {{:worker, worker_info.id, {worker_info.type, worker_info.parent}}, worker_info}
    )
  end

  def get_worker_info(table, worker_id, parent) do
    with {:ok, {_, worker_info}} <- lookup(table, {:worker, worker_id, parent}) do
      {:ok, worker_info}
    end
  end

  def delete_worker_info(table, worker_id, parent) do
    Ets.delete(table, {:worker, worker_id, parent})
  end

  def get_worker_infos_by_parent(table, parent) do
    result =
      Ets.match_object(table, {{:worker, :_, parent}, :_})
      |> Enum.map(fn {_, worker} -> worker end)

    {:ok, result}
  end

  def get_all_workers(table) do
    result =
      Ets.match_object(table, {{:worker, :_, :_}, :_})
      |> Enum.map(fn {_, worker_info} -> worker_info end)

    {:ok, result}
  end

  def put_group(table, %Group{} = group) do
    Ets.insert_new(table, {{:group, group.id}, group})
  end

  def delete_group(table, group_id) do
    Ets.delete(table, {:group, group_id})
  end

  def get_group(table, group_id) do
    with {:ok, {_, group}} <- lookup(table, {:group, group_id}) do
      {:ok, group}
    end
  end

  def put_chain(table, %Chain{} = chain) do
    Ets.insert_new(table, {{:chain, chain.id}, chain})
  end

  def get_chain(table, chain_id) do
    with {:ok, {_, chain}} <- lookup(table, {:chain, chain_id}) do
      {:ok, chain}
    end
  end

  def delete_chain(table, chain_id) do
    Ets.delete(table, {:chain, chain_id})
  end

  def put_sup_info(table, partition_id, opts) do
    Ets.insert_new(table, {{:supervisor, partition_id}, opts})
  end

  def get_sup_info(table, partition_id) do
    with {:ok, {_, opts}} <- lookup(table, {:supervisor, partition_id}) do
      {:ok, opts}
    end
  end

  def delete_sup_info(table, partition_id) do
    Ets.delete(table, {:supervisor, partition_id})
  end

  def put_sup_pid(table, partition_id, pid) do
    Ets.insert_new(table, {{:supervisor_pid, partition_id}, pid})
  end

  def get_sup_pid(table, partition_id) do
    with {:ok, {_, pid}} <- lookup(table, {:supervisor_pid, partition_id}) do
      {:ok, pid}
    end
  end

  def get_all_sup_pids(table) do
    result =
      Ets.match_object(table, {{:supervisor_pid, :_}, :_})
      |> Enum.map(fn {{_, partition_id}, pid} -> {partition_id, pid} end)

    {:ok, result}
  end

  def delete_sup_pid(table, partition_id) do
    Ets.delete(table, {:supervisor_pid, partition_id})
  end

  ## private functions ##

  defp lookup(table, key) do
    case Ets.lookup(table, key) do
      [] ->
        {:error, :not_found}

      [data] ->
        {:ok, data}

      _ ->
        Logger.error("wrong table type for #{inspect(table)}")
        {:error, :wrong_table_type}
    end
  end
end
