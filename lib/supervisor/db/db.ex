defmodule SuperWorker.Supervisor.Db do
  @moduledoc false

  alias :ets, as: Ets

  alias SuperWorker.Supervisor.{Chain, Group, Worker}

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

  @doc """
  Inserts the ref row of a worker that has no ref row yet (a new add). Unlike
  `put_worker/5` it does not scan the table for stale rows, so adding N workers
  costs O(N) in total.
  """
  def put_new_worker(table, ref, worker_id, parent, pid) do
    Ets.insert(table, {{:ref, ref}, worker_id, parent, pid})
    :ok
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
            cleanup_duplicate_workers(table, worker_id, extras)
            {:ok, {ref, pid}}

          [] ->
            {:error, :not_found}
        end
    end
  end

  defp cleanup_duplicate_workers(_table, _worker_id, []), do: :ok

  defp cleanup_duplicate_workers(table, worker_id, extras) do
    Logger.warning(
      "SuperWorker, Db, found duplicate alive entries for worker #{inspect(worker_id)}, cleaning up"
    )

    Enum.each(extras, fn {{_, ref}, _, _, _} -> Ets.delete(table, {:ref, ref}) end)
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
    # Pid/id-only select: avoids materializing full Worker structs and
    # copying whole ref tuples into the caller.
    match_spec = [{{{:ref, :_}, :"$1", parent, :"$3"}, [], [{{:"$1", :"$3"}}]}]

    {:ok, Ets.select(table, match_spec)}
  end

  def get_workers_by_parent(table, parent) do
    match_spec = [{{{:ref, :_}, :"$1", parent, :"$3"}, [], [{{:"$1", :"$3"}}]}]

    result =
      Ets.select(table, match_spec)
      # Remove duplicates efficiently
      |> :lists.usort()

    {:ok, result}
  end

  def get_worker_info_by_ref(table, ref) do
    with {:ok, {worker_id, parent, _pid}} <- get_worker(table, ref) do
      get_worker_info(table, worker_id, parent)
    end
  end

  def put_worker_info(table, worker_info = %Worker{}) do
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

  def get_worker_infos_by_parent(table, {type, parent_value}) do
    # The ETS table stores: {{:worker, worker_id, {type, parent_value}}, worker_info}
    # Filter inside ETS with a match spec instead of scanning the whole
    # table into the caller process.
    match_spec = [
      {{{:worker, :_, {:"$1", :"$2"}}, :"$3"},
       [
         {:"=:=", :"$1", {:const, type}},
         {:"=:=", :"$2", {:const, parent_value}}
       ], [:"$3"]}
    ]

    {:ok, Ets.select(table, match_spec)}
  end

  def count_workers_by_parent(table, {type, parent_value}) do
    # Count-only query: never materializes Worker structs into the caller.
    match_spec = [{{{:worker, :_, {type, parent_value}}, :_}, [], [true]}]

    {:ok, Ets.select_count(table, match_spec)}
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

  def put_group(table, group = %Group{}) do
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

  # Routing: {:chain_order, chain_id, order} -> [{worker_id, pid}] holds every
  # replica of the node at `order`. Orders are kept contiguous 1..n by the
  # chain module; `{:chain_nodes, chain_id}` holds n.

  @doc "Adds or replaces (by worker_id) one replica in the node's routing row."
  def put_chain_order(table, worker_id, chain_id, order, pid) do
    key = {:chain_order, chain_id, order}

    replicas =
      case Ets.lookup(table, key) do
        [{^key, existing}] -> Enum.reject(existing, fn {id, _} -> id == worker_id end)
        [] -> []
      end

    Ets.insert(table, {key, [{worker_id, pid} | replicas]})
    :ok
  end

  @doc "Returns every replica `[{worker_id, pid}]` routed for the node at `order`."
  def get_chain_order(table, chain_id, order) do
    with {:ok, {_, replicas}} <- lookup(table, {:chain_order, chain_id, order}) do
      {:ok, replicas}
    end
  end

  @doc """
  Removes one replica from the node's routing row. Returns `:last` when that
  was the final replica (the row is deleted), `:more` when others remain, or
  `:not_found` when the replica was not routed.
  """
  def delete_chain_replica(table, chain_id, order, worker_id) do
    key = {:chain_order, chain_id, order}

    case Ets.lookup(table, key) do
      [{^key, replicas}] ->
        case Enum.reject(replicas, fn {id, _} -> id == worker_id end) do
          [] ->
            Ets.delete(table, key)
            :last

          remaining ->
            Ets.insert(table, {key, remaining})
            :more
        end

      [] ->
        :not_found
    end
  end

  @doc "Moves a node's routing row from `from` to `to` (used to close order gaps)."
  def move_chain_order(table, chain_id, from, to) do
    with {:ok, replicas} <- get_chain_order(table, chain_id, from) do
      Ets.delete(table, {:chain_order, chain_id, from})
      Ets.insert(table, {{:chain_order, chain_id, to}, replicas})
      :ok
    end
  end

  def delete_chain_order(table, chain_id, order) do
    Ets.delete(table, {:chain_order, chain_id, order})
  end

  @doc "Atomically allocates the next node order (count + 1) for a chain."
  def next_chain_order(table, chain_id) do
    Ets.update_counter(table, {:chain_nodes, chain_id}, {2, 1}, {{:chain_nodes, chain_id}, 0})
  end

  @doc "Releases the last allocated node order (decrements the node count)."
  def release_chain_order(table, chain_id) do
    Ets.update_counter(table, {:chain_nodes, chain_id}, {2, -1}, {{:chain_nodes, chain_id}, 0})
  end

  @doc "Number of nodes currently allocated in the chain."
  def get_chain_node_count(table, chain_id) do
    case Ets.lookup(table, {:chain_nodes, chain_id}) do
      [{_, count}] -> count
      [] -> 0
    end
  end

  @doc """
  Returns the round-robin replica index in `0..n-1` for a node, advancing
  the shared per-node counter atomically.
  """
  def next_round_robin(table, chain_id, order, n) when n > 0 do
    key = {:chain_rr, chain_id, order}
    count = Ets.update_counter(table, key, {2, 1}, {key, 0})
    rem(count - 1, n)
  end

  def put_chain(table, chain = %Chain{}) do
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

  @doc "Deletes a chain row together with all of its routing and counter rows."
  def delete_chain(table, chain_id) do
    Ets.match_delete(table, {{:chain_order, chain_id, :_}, :_})
    Ets.match_delete(table, {{:chain_rr, chain_id, :_}, :_})
    Ets.delete(table, {:chain_nodes, chain_id})
    Ets.delete(table, {:chain, chain_id})
    :ok
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
