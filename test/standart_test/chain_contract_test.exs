defmodule SuperWorker.Supervisor.ChainContractTest do
  # One happy-path and one error-path assertion for every public Chain function
  # (improve-chain-perf-and-errors task 7.1).
  use ExUnit.Case, async: false

  alias SuperWorker.Supervisor.{Chain, Db, Worker}

  @moduletag :capture_log

  setup do
    # Workers are linked to the test process; trap exits as the partition does.
    Process.flag(:trap_exit, true)
    table = Db.init(:"chain_contract_#{System.unique_integer([:positive])}")
    %{chain: %Chain{id: make_ref(), table: table}}
  end

  defp chain_node(id), do: %Worker{id: id, type: :chain, fun: {:fun, fn d -> {:next, d} end}}

  defp add!(chain, id) do
    {:ok, chain} = Chain.add_worker(chain, chain_node(id))
    chain
  end

  test "check_options/1 accepts valid options and rejects an invalid send type" do
    assert {:ok, %Chain{id: :c1, send_type: :round_robin}} =
             Chain.check_options(id: :c1, send_type: :round_robin)

    assert {:error, {:invalid_send_type, :smoke_signals}} =
             Chain.check_options(id: :c1, send_type: :smoke_signals)
  end

  test "get_worker/2 returns the worker or :not_found", %{chain: chain} do
    chain = add!(chain, 1)

    assert {:ok, %Worker{id: 1, order: 1}} = Chain.get_worker(chain, 1)
    assert {:error, :not_found} = Chain.get_worker(chain, :missing)
  end

  test "worker_exists?/2 is true for members and false otherwise", %{chain: chain} do
    chain = add!(chain, 1)

    assert Chain.worker_exists?(chain, 1)
    refute Chain.worker_exists?(chain, :missing)
  end

  test "get_all_workers/1 lists every node and is empty for a new chain", %{chain: chain} do
    assert {:ok, []} = Chain.get_all_workers(chain)

    chain = chain |> add!(1) |> add!(2)
    assert {:ok, workers} = Chain.get_all_workers(chain)
    assert workers |> Enum.map(& &1.id) |> Enum.sort() == [1, 2]
  end

  test "add_worker/2 appends a node and rejects a duplicate id", %{chain: chain} do
    assert {:ok, chain} = Chain.add_worker(chain, chain_node(1))
    assert {:error, :already_exists} = Chain.add_worker(chain, chain_node(1))
    assert Chain.count_workers(chain) == 1
  end

  test "restart_worker/2 respawns a known worker and rejects an unknown id", %{chain: chain} do
    chain = add!(chain, 1)
    {:ok, old} = Chain.get_worker(chain, 1)
    {:ok, [{1, old_pid}]} = Db.get_chain_order(chain.table, chain.id, old.order)

    assert {:ok, _} = Chain.restart_worker(chain, 1)
    assert {:ok, [{1, new_pid}]} = Db.get_chain_order(chain.table, chain.id, old.order)
    assert new_pid != old_pid

    assert {:error, :worker_not_found} = Chain.restart_worker(chain, :missing)
  end

  test "restart_all_workers/1 succeeds on a populated chain", %{chain: chain} do
    chain = chain |> add!(1) |> add!(2)
    assert {:ok, ^chain} = Chain.restart_all_workers(chain)
    assert Chain.count_workers(chain) == 2
  end

  test "restart_from_worker/2 restarts the tail and rejects an unknown id", %{chain: chain} do
    chain = chain |> add!(1) |> add!(2)
    assert {:ok, _} = Chain.restart_from_worker(chain, 2)
    assert {:error, :worker_not_found} = Chain.restart_from_worker(chain, :missing)
  end

  test "count_workers/1 counts nodes and is zero for a new chain", %{chain: chain} do
    assert Chain.count_workers(chain) == 0
    assert Chain.count_workers(add!(chain, 1)) == 1
  end

  test "remove_worker/2 removes a member and rejects an unknown id", %{chain: chain} do
    chain = add!(chain, 1)

    assert :ok = Chain.remove_worker(chain, 1)
    assert Chain.count_workers(chain) == 0
    assert {:error, :worker_not_found} = Chain.remove_worker(chain, 1)
  end

  test "kill_worker/2 kills a live worker and rejects an unknown id", %{chain: chain} do
    chain = add!(chain, 1)

    assert {:ok, ^chain} = Chain.kill_worker(chain, 1)
    assert {:error, :not_found} = Chain.kill_worker(chain, 1)
  end

  test "remove_all_workers/1 removes every node", %{chain: chain} do
    chain = chain |> add!(1) |> add!(2)

    assert :ok = Chain.remove_all_workers(chain)
    assert Chain.count_workers(chain) == 0
  end
end
