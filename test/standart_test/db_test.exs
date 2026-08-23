defmodule SuperWorker.Supervisor.DbTest do
  use ExUnit.Case, async: true

  alias SuperWorker.Supervisor.{Db, Group, Chain}

  doctest Db

  @moduletag :capture_log

  setup do
    # Use a unique table name per test to avoid ETS conflicts with async: true
    table_name = :"db_test_#{System.unique_integer([:positive])}"
    table = Db.init(table_name)

    on_exit(fn ->
      # Safely delete ETS table, ignoring errors if already deleted
      try do
        :ets.delete(table)
      catch
        :error, _ -> :ok
      end
    end)

    %{table: table}
  end

  @doc """
  Test for put/get worker.
  """
  @tag :db_test_add_data
  test "put/get worker", %{table: table} do
    ref = make_ref()
    worker_id = ref

    Db.put_worker(table, ref, worker_id, {:standalone, nil}, self())
    {:ok, {^worker_id, {:standalone, nil}, pid}} = Db.get_worker(table, ref)

    assert self() == pid
  end

  @doc """
  Test for get by worker id
  """
  @tag :db_test_get_worker_by_id
  test "get worker by id", %{table: table} do
    ref = make_ref()
    worker_id = ref
    parent = {:standalone, nil}

    Db.put_worker(table, ref, worker_id, parent, self())
    {:ok, {ref2, pid}} = Db.get_worker_by_id(table, worker_id, parent)

    assert self() == pid and ref == ref2
  end

  @doc """
  Test for get workers by parent
  """
  @tag :db_get_workers_by_parent
  test "get workers by parent", %{table: table} do
    ref = make_ref()
    worker_id = ref
    parent = {:group, make_ref()}

    Db.put_worker(table, ref, worker_id, parent, self())
    {:ok, [{id, pid}]} = Db.get_workers_by_parent(table, parent)

    assert self() == pid and ref == id
  end

  describe "get_worker_infos_by_parent/2" do
    test "returns only worker infos of the given parent", %{table: table} do
      group_a = {:group, make_ref()}
      group_b = {:group, make_ref()}

      wa1 = %SuperWorker.Supervisor.Worker{
        id: :a1,
        fun: {:fun, fn -> :ok end},
        type: :group,
        parent: elem(group_a, 1)
      }

      wa2 = %SuperWorker.Supervisor.Worker{
        id: :a2,
        fun: {:fun, fn -> :ok end},
        type: :group,
        parent: elem(group_a, 1)
      }

      wb = %SuperWorker.Supervisor.Worker{
        id: :b1,
        fun: {:fun, fn -> :ok end},
        type: :group,
        parent: elem(group_b, 1)
      }

      ws = %SuperWorker.Supervisor.Worker{id: :s1, fun: {:fun, fn -> :ok end}, type: :standalone}

      Db.put_worker_info(table, wa1)
      Db.put_worker_info(table, wa2)
      Db.put_worker_info(table, wb)
      Db.put_worker_info(table, ws)

      assert {:ok, infos} = Db.get_worker_infos_by_parent(table, group_a)
      assert MapSet.new(infos, & &1.id) == MapSet.new([:a1, :a2])

      assert {:ok, infos} = Db.get_worker_infos_by_parent(table, group_b)
      assert Enum.map(infos, & &1.id) == [:b1]

      assert {:ok, infos} = Db.get_worker_infos_by_parent(table, {:standalone, nil})
      assert Enum.map(infos, & &1.id) == [:s1]
    end

    test "returns empty list when no worker matches", %{table: table} do
      assert {:ok, []} = Db.get_worker_infos_by_parent(table, {:chain, :missing})
    end

    test "matches reference parents exactly", %{table: table} do
      parent = {:chain, make_ref()}
      other = {:chain, make_ref()}

      w = %SuperWorker.Supervisor.Worker{
        id: :c1,
        fun: {:fun, fn -> :ok end},
        type: :chain,
        parent: elem(parent, 1)
      }

      Db.put_worker_info(table, w)

      assert {:ok, [%{id: :c1}]} = Db.get_worker_infos_by_parent(table, parent)
      assert {:ok, []} = Db.get_worker_infos_by_parent(table, other)
    end
  end

  @doc """
  Test for get worker by id with multiple entries
  """
  @tag :db_test_get_worker_by_id_2
  test "get worker by id 2", %{table: table} do
    ref = make_ref()
    worker_id = ref
    parent = {:standalone, nil}

    Db.put_worker(table, ref, worker_id, parent, self())
    {:ok, {ref2, pid}} = Db.get_worker_by_id(table, worker_id, parent)

    assert self() == pid and ref == ref2
  end

  @doc """
  Test delete worker
  """
  @tag :db_test_delete
  test "delete worker", %{table: table} do
    ref = make_ref()
    worker_id = ref
    parent = {:standalone, nil}

    Db.put_worker(table, ref, worker_id, parent, self())

    {:ok, _} = Db.get_worker(table, ref)
    Db.delete_worker(table, ref)
    result = Db.get_worker(table, ref)

    assert match?({:error, :not_found}, result)
  end

  @doc """
  Test delete worker by id
  """
  @tag :db_test_delete_by_id
  test "delete worker by id", %{table: table} do
    ref = make_ref()
    worker_id = ref
    parent = {:standalone, nil}

    Db.put_worker(table, ref, worker_id, parent, self())

    {:ok, _} = Db.get_worker(table, ref)
    Db.delete_worker_by_id(table, worker_id, parent)
    result = Db.get_worker(table, ref)

    assert match?({:error, :not_found}, result)
  end

  @doc """
  Test put/delete/get group
  """
  @tag :db_test_group
  test "test put/get/delete group", %{table: table} do
    id = make_ref()
    group = %Group{id: id}

    Db.put_group(table, group)

    {:ok, result} = Db.get_group(table, id)
    assert result.id == id
    Db.delete_group(table, id)
    result = Db.get_group(table, id)

    assert match?({:error, _}, result)
  end

  @doc """
  Test put/delete/get group
  """
  @tag :db_test_get_all_groups
  test "test get all groups", %{table: table} do
    group1 = %Group{id: make_ref()}
    group2 = %Group{id: make_ref()}

    Db.put_group(table, group1)
    Db.put_group(table, group2)

    {:ok, groups} = Db.get_all_groups(table)

    groups = Enum.map(groups, fn group -> group.id end)

    assert group1.id in groups
    assert group2.id in groups
  end

  @doc """
  Test put/delete/get chain
  """
  @tag :db_test_chain
  test "test put/get/delete chain", %{table: table} do
    id = make_ref()
    chain = %Chain{id: id}

    Db.put_chain(table, chain)

    {:ok, result} = Db.get_chain(table, id)
    assert result.id == id
    Db.delete_chain(table, id)
    result = Db.get_chain(table, id)

    assert match?({:error, _}, result)
  end

  @doc """
  Test get all chains
  """
  @tag :db_test_get_all_chains
  test "test get all chains", %{table: table} do
    chain1 = %Chain{id: make_ref()}
    chain2 = %Chain{id: make_ref()}

    Db.put_chain(table, chain1)
    Db.put_chain(table, chain2)

    {:ok, chains} = Db.get_all_chains(table)

    chains = Enum.map(chains, fn chain -> chain.id end)

    assert chain1.id in chains
    assert chain2.id in chains
  end

  @doc """
  Test get/put sup info
  """
  @tag :db_test_put_get_delete_sup_info
  test "test get/put/delete sup info", %{table: table} do
    options = [test: 1]
    partition_id = make_ref()

    Db.put_sup_info(table, partition_id, options)

    {:ok, options2} = Db.get_sup_info(table, partition_id)
    assert match?(^options, options2)

    Db.delete_sup_info(table, partition_id)

    result = Db.get_sup_info(table, partition_id)
    assert match?({:error, _}, result)
  end

  @doc """
  Test get/put sup pid
  """
  @tag :db_test_put_get_delete_sup_pid
  test "test get/put/delete sup pid", %{table: table} do
    partition_id = make_ref()

    Db.put_sup_pid(table, partition_id, self())

    {:ok, pid2} = Db.get_sup_pid(table, partition_id)
    assert self() == pid2

    Db.delete_sup_pid(table, partition_id)

    result = Db.get_sup_pid(table, partition_id)
    assert match?({:error, _}, result)
  end

  describe "misc accessors" do
    test "get_all_workers returns every worker info", %{table: table} do
      w1 = %SuperWorker.Supervisor.Worker{id: 1, fun: {:fun, fn -> :ok end}, type: :group}
      w2 = %SuperWorker.Supervisor.Worker{id: 2, fun: {:fun, fn -> :ok end}, type: :chain}

      Db.put_worker_info(table, w1)
      Db.put_worker_info(table, w2)

      assert {:ok, infos} = Db.get_all_workers(table)
      assert MapSet.new(infos, & &1.id) == MapSet.new([1, 2])
    end

    test "get_worker_info_by_ref resolves through the ref row", %{table: table} do
      ref = make_ref()

      w = %SuperWorker.Supervisor.Worker{
        id: :by_ref,
        fun: {:fun, fn -> :ok end},
        type: :standalone
      }

      Db.put_worker_info(table, w)
      Db.put_worker(table, ref, w.id, {:standalone, nil}, self())

      assert {:ok, %{id: :by_ref}} = Db.get_worker_info_by_ref(table, ref)
      assert {:error, :not_found} = Db.get_worker_info_by_ref(table, make_ref())
    end

    test "put_group/put_chain reject duplicates", %{table: table} do
      group = %SuperWorker.Supervisor.Group{id: :dup_group}
      chain = %SuperWorker.Supervisor.Chain{id: :dup_chain}

      assert :ok = Db.put_group(table, group)
      assert {:error, :already_exists} = Db.put_group(table, group)

      assert :ok = Db.put_chain(table, chain)
      assert {:error, :already_exists} = Db.put_chain(table, chain)

      assert {:ok, [%{id: :dup_group}]} = Db.get_all_groups(table)
      assert {:ok, [%{id: :dup_chain}]} = Db.get_all_chains(table)
    end

    test "get_all_sup_pids lists every partition pid", %{table: table} do
      Db.put_sup_pid(table, :p1, self())
      Db.put_sup_pid(table, :p2, spawn(fn -> :ok end))

      assert {:ok, pids} = Db.get_all_sup_pids(table)
      assert MapSet.new(pids, fn {id, _} -> id end) == MapSet.new([:p1, :p2])
    end

    test "chain order roundtrip", %{table: table} do
      Db.put_chain_order(table, :w1, :chain_x, 1, self())

      assert {:ok, {:w1, pid}} = Db.get_chain_order(table, :chain_x, 1)
      assert pid == self()

      Db.delete_chain_order(table, :chain_x, 1)
      assert {:error, :not_found} = Db.get_chain_order(table, :chain_x, 1)
    end

    test "delete_worker_by_id removes the ref row", %{table: table} do
      ref = make_ref()
      Db.put_worker(table, ref, :doomed, {:standalone, nil}, self())

      assert true = Db.delete_worker_by_id(table, :doomed, {:standalone, nil})
      assert {:error, :not_found} = Db.get_worker_by_id(table, :doomed, {:standalone, nil})
      # Missing worker also reports not found.
      assert {:error, :not_found} = Db.delete_worker_by_id(table, :doomed, {:standalone, nil})
    end

    test "get_worker_pids_by_parent keeps duplicates", %{table: table} do
      parent = {:group, :pids_group}
      Db.put_worker(table, make_ref(), :x1, parent, self())
      Db.put_worker(table, make_ref(), :x2, parent, self())

      assert {:ok, pids} = Db.get_worker_pids_by_parent(table, parent)
      assert length(pids) == 2
    end

    test "stale duplicate rows are purged on lookup", %{table: table} do
      dead_pid = spawn(fn -> :ok end)
      Process.sleep(10)

      # Two rows for the same logical worker: one pointing to a dead process.
      stale_ref = make_ref()
      fresh_ref = make_ref()

      Db.put_worker(table, stale_ref, :duplicated, {:standalone, nil}, dead_pid)

      :ets.insert(table, {{:ref, fresh_ref}, :duplicated, {:standalone, nil}, self()})

      assert {:ok, {found_ref, pid}} =
               Db.get_worker_by_id(table, :duplicated, {:standalone, nil})

      assert found_ref == fresh_ref
      assert pid == self()

      # The stale row is gone.
      assert [] = :ets.match_object(table, {{:ref, stale_ref}, :_, :_, :_})
    end
  end
end
