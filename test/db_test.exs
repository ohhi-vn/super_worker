defmodule SuperWorker.Supervisor.DbTest do
  use ExUnit.Case, async: true

  alias SuperWorker.Supervisor.{Db, Group, Chain}

  doctest Db

  @table :db_test

  setup_all do
    @table = Db.init(@table)
    :ok
  end

  setup do
    :ok
  end

  @doc """
  Test for put/get worker.
  """
  @tag :db_test_add_data
  test "put/get worker" do
    ref = make_ref()
    worker_id = ref

    Db.put_worker(@table, ref, worker_id, {:standalone, nil}, self())
    {:ok, {worker_id, {:standalone, nil}, pid}} = Db.get_worker(@table, ref)

    assert self() == pid
  end

  @doc """
  Test for get by worker id
  """
  @tag :db_test_add_data2
  test "get worker by id" do
    ref = make_ref()
    worker_id = ref
    parent = {:standalone, nil}

    Db.put_worker(@table, ref, worker_id, parent, self())
    {:ok, {ref2, pid}} = Db.get_worker_by_id(@table, worker_id, parent)

    assert self() == pid and ref == ref2
  end

  @doc """
  Test for get by workers by parent
  """
  @tag :db_get_workers_by_parent
  test "get workers by parent" do
    ref = make_ref()
    worker_id = ref
    parent = {:group, make_ref()}

    Db.put_worker(@table, ref, worker_id, parent, self())
    {:ok, [{id, pid}]} = Db.get_workers_by_parent(@table, parent)

    assert self() == pid and ref == id
  end

  @doc """
  Test for get by worker id
  """
  @tag :db_test_add_data2
  test "get worker by id 2" do
    ref = make_ref()
    worker_id = ref
    parent = {:standalone, nil}

    Db.put_worker(@table, ref, worker_id, parent, self())
    {:ok, {ref2, pid}} = Db.get_worker_by_id(@table, worker_id, parent)

    assert self() == pid and ref == ref2
  end

  @doc """
  Test delete worker
  """
  @tag :db_test_delete
  test "delete worker " do
    ref = make_ref()
    worker_id = ref
    parent = {:standalone, nil}

    Db.put_worker(@table, ref, worker_id, parent, self())

    {:ok, _} = Db.get_worker(@table, ref)
    Db.delete_worker(@table, ref)
    result = Db.get_worker(@table, ref)

    assert match?({:error, :not_found}, result)
  end

  @doc """
  Test delete worker by id
  """
  @tag :db_test_delete_by_id
  test "delete worker by id" do
    ref = make_ref()
    worker_id = ref
    parent = {:standalone, nil}

    Db.put_worker(@table, ref, worker_id, parent, self())

    {:ok, _} = Db.get_worker(@table, ref)
    Db.delete_worker_by_id(@table, worker_id, parent)
    result = Db.get_worker(@table, ref)

    assert match?({:error, :not_found}, result)
  end

  @doc """
  Test put/delete/get group
  """
  @tag :db_test_group
  test "test put/get/delete group" do
    id = make_ref()
    group = %Group{id: id}

    Db.put_group(@table, group)

    {:ok, result} = Db.get_group(@table, id)
    assert(result.id == id)
    Db.delete_group(@table, id)
    result = Db.get_group(@table, id)

    assert match?({:error, _}, result)
  end

  @doc """
  Test put/delete/get group
  """
  @tag :db_test_get_all_groups
  test "test get all groups" do
    group1 = %Group{id: make_ref()}
    group2 = %Group{id: make_ref()}

    Db.put_group(@table, group1)
    Db.put_group(@table, group2)

    {:ok, groups} = Db.get_all_groups(@table)

    groups = Enum.map(groups, fn group -> group.id end)

    assert group1.id in groups
    assert group2.id in groups
  end

  @doc """
  Test put/delete/get chain
  """
  @tag :db_test_chain
  test "test put/get/delete chain" do
    id = make_ref()
    chain = %Chain{id: id}

    Db.put_chain(@table, chain)

    {:ok, result} = Db.get_chain(@table, id)
    assert(result.id == id)
    Db.delete_chain(@table, id)
    result = Db.get_chain(@table, id)

    assert match?({:error, _}, result)
  end

  @doc """
  Test get all chains
  """
  @tag :db_test_get_all_chains
  test "test get all chains" do
    chain1 = %Chain{id: make_ref()}
    chain2 = %Chain{id: make_ref()}

    Db.put_chain(@table, chain1)
    Db.put_chain(@table, chain2)

    {:ok, chains} = Db.get_all_chains(@table)

    chains = Enum.map(chains, fn chain -> chain.id end)

    assert chain1.id in chains
    assert chain2.id in chains
  end

  @doc """
  Test get/put sup info
  """
  @tag :db_test_put_get_delete_sup_info
  test "test get/put/delete sup info" do
    options = [test: 1]
    partition_id = make_ref()

    Db.put_sup_info(@table, partition_id, options)

    {:ok, options2} = Db.get_sup_info(@table, partition_id)
    assert match?(options, options2)

    Db.delete_sup_info(@table, partition_id)

    result = Db.get_sup_info(@table, partition_id)
    assert match?({:error, _}, result)
  end

  @doc """
  Test get/put sup pid
  """
  @tag :db_test_put_get_delete_sup_pid
  test "test get/put/delete sup pid" do
    options = [test: 1]
    partition_id = make_ref()

    Db.put_sup_pid(@table, partition_id, self())

    {:ok, pid2} = Db.get_sup_pid(@table, partition_id)
    assert self() == pid2

    Db.delete_sup_pid(@table, partition_id)

    result = Db.get_sup_pid(@table, partition_id)
    assert match?({:error, _}, result)
  end
end
