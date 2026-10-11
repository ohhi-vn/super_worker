defmodule SuperWorker.Supervisor.ChainDbTest do
  # Routing storage used by chains (improve-chain-perf-and-errors tasks 2.1 - 2.4).
  use ExUnit.Case, async: true

  alias SuperWorker.Supervisor.{Chain, Db}

  setup do
    table = Db.init(:"chain_db_#{System.unique_integer([:positive])}")
    %{table: table, chain_id: make_ref()}
  end

  describe "replica routing rows" do
    test "put appends replicas to the node row", %{table: t, chain_id: c} do
      :ok = Db.put_chain_order(t, :a, c, 1, self())
      :ok = Db.put_chain_order(t, :b, c, 1, self())

      assert {:ok, replicas} = Db.get_chain_order(t, c, 1)
      assert Enum.sort(Enum.map(replicas, &elem(&1, 0))) == [:a, :b]
    end

    test "put replaces the pid of a restarted replica", %{table: t, chain_id: c} do
      old = spawn(fn -> :ok end)
      new = spawn(fn -> :ok end)

      :ok = Db.put_chain_order(t, :a, c, 1, old)
      :ok = Db.put_chain_order(t, :a, c, 1, new)

      assert {:ok, [{:a, ^new}]} = Db.get_chain_order(t, c, 1)
    end

    test "delete_chain_replica reports :more, then :last and removes the row", %{
      table: t,
      chain_id: c
    } do
      :ok = Db.put_chain_order(t, :a, c, 1, self())
      :ok = Db.put_chain_order(t, :b, c, 1, self())

      assert :more = Db.delete_chain_replica(t, c, 1, :a)
      assert {:ok, [{:b, _}]} = Db.get_chain_order(t, c, 1)

      assert :last = Db.delete_chain_replica(t, c, 1, :b)
      assert {:error, :not_found} = Db.get_chain_order(t, c, 1)
    end

    test "delete_chain_replica on a missing replica returns :not_found", %{table: t, chain_id: c} do
      assert :not_found = Db.delete_chain_replica(t, c, 1, :nobody)
    end

    test "move_chain_order relocates a node row", %{table: t, chain_id: c} do
      :ok = Db.put_chain_order(t, :a, c, 3, self())

      assert :ok = Db.move_chain_order(t, c, 3, 2)
      assert {:error, :not_found} = Db.get_chain_order(t, c, 3)
      assert {:ok, [{:a, _}]} = Db.get_chain_order(t, c, 2)
    end

    test "move_chain_order on a missing row returns an error", %{table: t, chain_id: c} do
      assert {:error, :not_found} = Db.move_chain_order(t, c, 5, 4)
    end
  end

  describe "node counter" do
    test "next_chain_order allocates 1, 2, 3 and release decrements", %{table: t, chain_id: c} do
      assert Db.get_chain_node_count(t, c) == 0
      assert Db.next_chain_order(t, c) == 1
      assert Db.next_chain_order(t, c) == 2
      assert Db.next_chain_order(t, c) == 3
      assert Db.get_chain_node_count(t, c) == 3

      assert Db.release_chain_order(t, c) == 2
      assert Db.get_chain_node_count(t, c) == 2
    end
  end

  describe "round-robin counter" do
    test "cycles through 0..n-1 per node", %{table: t, chain_id: c} do
      picks = for _ <- 1..6, do: Db.next_round_robin(t, c, 1, 3)
      assert picks == [0, 1, 2, 0, 1, 2]
    end

    test "is independent per node order", %{table: t, chain_id: c} do
      assert Db.next_round_robin(t, c, 1, 2) == 0
      assert Db.next_round_robin(t, c, 2, 2) == 0
      assert Db.next_round_robin(t, c, 1, 2) == 1
    end
  end

  describe "delete_chain/2" do
    test "removes the chain row, routing rows, counter and round-robin rows", %{
      table: t,
      chain_id: c
    } do
      :ok = Db.put_chain(t, %Chain{id: c, table: t})
      :ok = Db.put_chain_order(t, :a, c, 1, self())
      :ok = Db.put_chain_order(t, :b, c, 2, self())
      _ = Db.next_chain_order(t, c)
      _ = Db.next_round_robin(t, c, 1, 2)

      :ok = Db.delete_chain(t, c)

      assert {:error, :not_found} = Db.get_chain(t, c)
      assert {:error, :not_found} = Db.get_chain_order(t, c, 1)
      assert {:error, :not_found} = Db.get_chain_order(t, c, 2)
      assert Db.get_chain_node_count(t, c) == 0
      assert Enum.empty?(:ets.match_object(t, {{:chain_rr, c, :_}, :_}))
    end
  end
end
