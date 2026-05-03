defmodule SuperWorker.Supervisor.Db do
  @moduledoc false

  alias :ets, as: Ets

  alias SuperWorker.Supervisor.{Worker, Group, Chain}

  require Logger
  require SuperWorker.Log

  def init(sup_name) when is_atom(sup_name) do
    SuperWorker.Log.debug(fn ->
      "SuperWorker, Db, creating table for supervisor #{inspect(sup_name)}"
    end)

    table =
      Ets.new(sup_name, [
        :set,
        :public,
        #   :named_table,
        {:write_concurrency, true},
        {:read_concurrency, true}
      ])

    SuperWorker.Log.debug(fn ->
      "SuperWorker, Db, created table for supervisor #{inspect(sup_name)}"
    end)

    table
  end

  def put_worker(table, ref, worker_id, parent, pid) do
    # Remove every old ref-row that belongs to this logical worker.
    stale =
      Ets.match_object(table, {{:ref, :_}, worker_id, parent, :_})

    Enum.each(stale, fn {{_, old_ref}, _, _, _} ->
      Ets.delete(table, {:ref, old_ref})
    end)

    # Now insert the single authoritative row.
    Ets.insert(table, {{:ref, ref}, worker_id, parent, pid})
  end

  def get_worker(table, ref) do
    with {:ok, {_, worker_id, parent, pid}} <- lookup(table, {:ref, ref}) do
      {:ok, {worker_id, parent, pid}}
    end
  end

  def get_worker_by_id(table, worker_id, parent) do
    # Using match_object to find entries matching worker_id and parent
    case Ets.match_object(table, {{:ref, :_}, worker_id, parent, :_}) do
      # Happy path – exactly one entry.
      [{{_, ref}, _, _, pid}] ->
        {:ok, {ref, pid}}

      # No entry found.
      [] ->
        {:error, :not_found}

      # Multiple stale entries exist (left behind by an incomplete restart cycle).
      # Keep the entry whose pid is still alive; purge the rest.
      # If none are alive, purge all and return :not_found.
      entries ->
        Logger.warning(
          "SuperWorker, Db, get_worker_by_id found #{length(entries)} entries " <>
            "for worker #{inspect(worker_id)}, parent #{inspect(parent)}. " <>
            "Cleaning up stale entries."
        )

        {alive, dead} =
          Enum.split_with(entries, fn {{_, _ref}, _, _, pid} -> Process.alive?(pid) end)

        # Purge every stale / duplicate ref row.
        Enum.each(dead, fn {{_, ref}, _, _, _} -> Ets.delete(table, {:ref, ref}) end)

        case alive do
          # Exactly one alive pid — also drop any extra alive duplicates to be safe.
          [{{_, ref}, _, _, pid} | extras] ->
            if extras != [] do
              Logger.warning(
                "SuperWorker, Db, found duplicate alive entries for worker #{inspect(worker_id)}, cleaning up"
              )

              Enum.each(extras, fn {{_, r}, _, _, _} -> Ets.delete(table, {:ref, r}) end)
            end

            {:ok, {ref, pid}}

          [] ->
            {:error, :not_found}
        end
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

  def get_worker_pids_by_parent(table, parent) do
    result =
      Ets.match_object(table, {{:ref, :_}, :_, parent, :_})
      |> Enum.map(fn {_, worker_id, _, pid} -> {worker_id, pid} end)

    {:ok, result}
  end

  def get_workers_by_parent(table, parent) do
    result =
      Ets.match_object(table, {{:ref, :_}, :_, parent, :_})
      |> Enum.map(fn {_, worker_id, _, pid} -> {worker_id, pid} end)
      # Remove duplicates efficiently
      |> :lists.usort()

    {:ok, result}
  end

  def get_worker_info_by_ref(table, ref) do
    with {:ok, {worker_id, parent, _pid}} <- get_worker(table, ref) do
      get_worker_info(table, worker_id, parent)
    end
  end

  def put_worker_info(table, %Worker{} = worker_info) do
    key = {:worker, worker_info.id, {worker_info.type, worker_info.parent}}

    # Use insert (upsert) instead of insert_new to handle restart scenarios
    # where worker info may already exist from a previous lifecycle.
    Ets.insert(table, {key, worker_info})
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
    # The ETS table stores: {{:worker, worker_id, {type, parent_value}}, worker_info}
    # The parent parameter is {type, parent_value}, e.g. {:group, reference}
    # We need to match all entries where the parent matches
    result =
      Ets.match_object(table, {{:worker, :_, {:_, :_}}, :_})
      |> Enum.filter(fn {{_, _, {type, p}}, _} -> {type, p} == parent end)
      |> Enum.map(fn {_, worker} -> worker end)

    {:ok, result}
  end

  def get_all_standalone_worker_infos(table) do
    result =
      Ets.match_object(table, {{:worker, :_, {:standalone, nil}}, :_})
      |> Enum.map(fn {_, worker} -> worker end)

    {:ok, result}
  end

  def get_all_workers(table) do
    result =
      Ets.match_object(table, {{:worker, :_, :_}, :_})
      |> Enum.map(fn {_, worker_info} -> worker_info end)

    {:ok, result}
  end

  # Optimized version using select for better performance on large datasets
  def get_all_workers_select(table) do
    result =
      Ets.match_object(table, {{:worker, :_, :_}, :_})
      |> Enum.map(fn {_, worker_info} -> worker_info end)

    {:ok, result}
  end

  def put_group(table, %Group{} = group) do
    key = {:group, group.id}

    if Ets.insert_new(table, {key, group}) do
      :ok
    else
      Logger.warning("SuperWorker, Db, group #{inspect(group.id)} already exists in table")
      {:error, :already_exists}
    end
  end

  def delete_group(table, group_id) do
    Ets.delete(table, {:group, group_id})
  end

  def get_group(table, group_id) do
    with {:ok, {_, group}} <- lookup(table, {:group, group_id}) do
      {:ok, group}
    end
  end

  def get_all_groups(table) do
    groups =
      Ets.match_object(table, {{:group, :_}, :_})
      |> Enum.map(fn {_, group} -> group end)

    {:ok, groups}
  end

  def put_chain_order(table, worker_id, chain_id, order, pid) do
    key = {:chain_order, chain_id, order}

    # Use insert (upsert) to handle chain worker restarts
    Ets.insert(table, {key, {worker_id, pid}})
  end

  def get_chain_order(table, chain_id, order) do
    with {:ok, {_, data}} <- lookup(table, {:chain_order, chain_id, order}) do
      {:ok, data}
    end
  end

  def delete_chain_order(table, chain_id, order) do
    Ets.delete(table, {:chain_order, chain_id, order})
  end

  def put_chain(table, %Chain{} = chain) do
    key = {:chain, chain.id}

    if Ets.insert_new(table, {key, chain}) do
      :ok
    else
      Logger.warning("SuperWorker, Db, chain #{inspect(chain.id)} already exists in table")
      {:error, :already_exists}
    end
  end

  def get_chain(table, chain_id) do
    with {:ok, {_, chain}} <- lookup(table, {:chain, chain_id}) do
      {:ok, chain}
    end
  end

  def get_all_chains(table) do
    chains =
      Ets.match_object(table, {{:chain, :_}, :_})
      |> Enum.map(fn {_, chain} -> chain end)

    {:ok, chains}
  end

  def delete_chain(table, chain_id) do
    Ets.delete(table, {:chain, chain_id})
  end

  def put_sup_info(table, partition_id, opts) do
    key = {:supervisor, partition_id}

    # Use insert (upsert) to handle supervisor restart scenarios
    Ets.insert(table, {key, opts})
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
    key = {:supervisor_pid, partition_id}

    # Use insert (upsert) to handle supervisor restart scenarios
    Ets.insert(table, {key, pid})
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
