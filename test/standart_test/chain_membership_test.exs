defmodule SuperWorker.Supervisor.ChainMembershipTest do
  # Membership verification for improve-chain-perf-and-errors (tasks 3.2 - 3.5).
  use ExUnit.Case, async: false

  alias SuperWorker.Supervisor, as: Sup
  alias SuperWorker.Supervisor.{Chain, Db, Worker}

  @moduletag :capture_log

  setup do
    sup_id = :"sup_chain_mem_#{System.unique_integer([:positive])}"
    {:ok, _} = Sup.start_with_config(link: false, id: sup_id, num_partitions: 1)

    on_exit(fn ->
      if Sup.running?(sup_id), do: Sup.stop(sup_id)
    end)

    %{sup_id: sup_id}
  end

  defp chain_table do
    table = Db.init(:"chain_mem_#{System.unique_integer([:positive])}")
    %Chain{id: make_ref(), table: table}
  end

  defp chain_node(id), do: %Worker{id: id, type: :chain, fun: {:fun, fn d -> {:next, d} end}}

  # 3.2 Removing a middle node keeps orders contiguous and the add after it does not collide.
  test "removing a middle node shifts later nodes down and keeps orders contiguous" do
    # Workers are linked to the test process; trap exits as the partition does.
    Process.flag(:trap_exit, true)
    chain = chain_table()

    Enum.reduce([1, 2, 3], chain, fn id, acc ->
      {:ok, acc} = Chain.add_worker(acc, chain_node(id))
      acc
    end)

    assert :ok = Chain.remove_worker(chain, 2)

    assert {:ok, %Worker{order: 1}} = Chain.get_worker(chain, 1)
    assert {:ok, %Worker{order: 2}} = Chain.get_worker(chain, 3)
    assert {:error, :not_found} = Db.get_chain_order(chain.table, chain.id, 3)
    assert Db.get_chain_node_count(chain.table, chain.id) == 2

    assert {:ok, chain} = Chain.add_worker(chain, chain_node(4))
    assert {:ok, %Worker{order: 3}} = Chain.get_worker(chain, 4)
  end

  # 3.3 The supervisor API reports the removal result.
  test "remove_chain_worker returns :ok for an existing worker and an error otherwise", %{
    sup_id: sup_id
  } do
    chain_id = make_ref()
    {:ok, _} = Sup.add_chain(sup_id, id: chain_id)
    {:ok, _} = Sup.add_chain_worker(sup_id, chain_id, fn d -> {:next, d} end, id: 1)

    assert :ok = Sup.remove_chain_worker(sup_id, chain_id, 1)
    assert {:error, :worker_not_found} = Sup.remove_chain_worker(sup_id, chain_id, 1)
  end

  # 3.4 Removing a chain leaves no routing rows; a re-added chain starts clean.
  test "remove_chain leaves no routing rows and a re-added chain starts at order 1", %{
    sup_id: sup_id
  } do
    chain_id = make_ref()
    {:ok, _} = Sup.add_chain(sup_id, id: chain_id)

    for id <- 1..3 do
      {:ok, _} = Sup.add_chain_worker(sup_id, chain_id, fn d -> {:next, d} end, id: id)
    end

    table = :sys.get_state(sup_id).supervisor.table

    assert :ok = Sup.remove_chain(sup_id, chain_id)
    assert [] == :ets.match_object(table, {{:chain_order, chain_id, :_}, :_})
    assert [] == :ets.match_object(table, {{:chain_nodes, chain_id}, :_})

    {:ok, _} = Sup.add_chain(sup_id, id: chain_id)
    {:ok, _} = Sup.add_chain_worker(sup_id, chain_id, fn d -> {:next, d} end, id: 1)

    assert {:ok, [{1, _}]} = Db.get_chain_order(table, chain_id, 1)
    assert {:error, :not_found} = Db.get_chain_order(table, chain_id, 2)
  end

  # 3.5 kill keeps the routing entry so a restart can replace the pid.
  test "kill keeps the routing entry and restart replaces its pid" do
    chain = chain_table()
    {:ok, chain} = Chain.add_worker(chain, chain_node(1))

    {:ok, [{1, old_pid}]} = Db.get_chain_order(chain.table, chain.id, 1)

    # The partition process traps exits; the test does the same since workers are linked here.
    Process.flag(:trap_exit, true)

    assert {:ok, _} = Chain.kill_worker(chain, 1)
    assert {:ok, [{1, ^old_pid}]} = Db.get_chain_order(chain.table, chain.id, 1)
    assert {:error, :not_found} = Chain.kill_worker(chain, 1)

    assert {:ok, _} = Chain.restart_worker(chain, 1)
    assert {:ok, [{1, new_pid}]} = Db.get_chain_order(chain.table, chain.id, 1)
    assert new_pid != old_pid
    assert Process.alive?(new_pid)
  end
end
