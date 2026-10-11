defmodule SuperWorker.Supervisor.ChainRoutingTest do
  # Replica routing, confirmations, queue release and callbacks
  # (improve-chain-perf-and-errors tasks 4.1, 4.3 - 4.6).
  use ExUnit.Case, async: false

  alias SuperWorker.Supervisor, as: Sup
  alias SuperWorker.Supervisor.{Chain, Db, Message, Worker}
  alias SuperWorker.Supervisor.Chain.Messaging

  @moduletag :capture_log

  setup do
    sup_id = :"sup_chain_route_#{System.unique_integer([:positive])}"
    {:ok, _} = Sup.start_with_config(link: false, id: sup_id, num_partitions: 1)

    on_exit(fn ->
      if Sup.running?(sup_id), do: Sup.stop(sup_id)
    end)

    %{sup_id: sup_id}
  end

  # Chain-level helpers. Workers are linked to the test process, which traps exits.
  defp chain_with(send_type, extra \\ []) do
    table = Db.init(:"chain_route_#{System.unique_integer([:positive])}")
    struct(%Chain{id: make_ref(), table: table, send_type: send_type}, extra)
  end

  defp reporting_fun(parent) do
    {:fun,
     fn data ->
       send(parent, {:seen, self(), data})
       {:drop, :seen}
     end}
  end

  defp replicated_node(chain, id, n, fun) do
    {:ok, chain} =
      Chain.add_worker(chain, %Worker{id: id, type: :chain, num_workers: n, fun: fun})

    chain
  end

  defp msg(data, from), do: %Message{id: make_ref(), data: data, type: :new_data, from: from}

  defp collect_seen(count, acc \\ []) do
    if length(acc) == count do
      Enum.reverse(acc)
    else
      receive do
        {:seen, pid, data} -> collect_seen(count, [{pid, data} | acc])
      after
        2_000 -> flunk("expected #{count} replica deliveries, got #{length(acc)}")
      end
    end
  end

  defp refute_more_seen do
    receive do
      {:seen, _, _} -> flunk("unexpected extra delivery")
    after
      150 -> :ok
    end
  end

  describe "send types across replicas (4.3, 4.4)" do
    setup do
      Process.flag(:trap_exit, true)
      :ok
    end

    test "round_robin spreads six messages evenly over three replicas" do
      parent = self()
      chain = chain_with(:round_robin) |> replicated_node(:src, 3, reporting_fun(parent))

      for i <- 1..6, do: Messaging.new_data(chain, msg(i, parent))

      seen = collect_seen(6)
      counts = seen |> Enum.frequencies_by(&elem(&1, 0))
      assert map_size(counts) == 3
      assert Enum.all?(Map.values(counts), &(&1 == 2))
      refute_more_seen()
    end

    test "broadcast reaches every replica once per message" do
      parent = self()
      chain = chain_with(:broadcast) |> replicated_node(:src, 3, reporting_fun(parent))

      assert {:ok, :sent_to_many} = Messaging.new_data(chain, msg(:hello, parent))

      seen = collect_seen(3)
      assert seen |> Enum.map(&elem(&1, 0)) |> Enum.uniq() |> length() == 3
      assert Enum.all?(seen, fn {_pid, data} -> data == :hello end)
      refute_more_seen()
    end

    test "partition sends the same data to the same replica" do
      parent = self()
      chain = chain_with(:partition) |> replicated_node(:src, 3, reporting_fun(parent))

      for _ <- 1..2, do: Messaging.new_data(chain, msg(:same, parent))

      [{first, :same}, {second, :same}] = collect_seen(2)
      assert first == second
      refute_more_seen()
    end

    test "random sends each message to exactly one replica" do
      parent = self()
      chain = chain_with(:random) |> replicated_node(:src, 3, reporting_fun(parent))

      for i <- 1..20, do: Messaging.new_data(chain, msg(i, parent))

      assert length(collect_seen(20)) == 20
      refute_more_seen()
    end

    test "a single replica receives every message under every send type" do
      parent = self()

      for send_type <- [:broadcast, :random, :partition, :round_robin] do
        chain = chain_with(send_type) |> replicated_node(:solo, 1, reporting_fun(parent))

        for i <- 1..3, do: Messaging.new_data(chain, msg(i, parent))

        seen = collect_seen(3)
        assert seen |> Enum.map(&elem(&1, 0)) |> Enum.uniq() |> length() == 1
      end
    end
  end

  describe "confirmations (4.1)" do
    test "a node after the first confirms its upstream message; the first node does not" do
      parent = self()
      chain = chain_with(:random)

      first = replicated_node(chain, :first, 1, {:fun, fn d -> {:next, d} end})
      second = replicated_node(first, :second, 1, {:fun, fn d -> {:drop, d} end})

      # Entering at order 2: its upstream is the test process, so it confirms.
      Messaging.send_next(second, 2, msg(:a, parent))
      assert_receive {:processed, _, :second}, 2_000

      # Entering at order 1: no upstream chain node, so no confirmation.
      Messaging.new_data(second, msg(:b, parent))
      refute_receive {:processed, _, :first}, 150
    end
  end

  describe "queue release and no-consumer forwards (4.5)" do
    test "forwards with no downstream release their queue entry", %{sup_id: sup_id} do
      parent = self()
      chain_id = make_ref()

      {:ok, _} =
        Sup.add_chain(sup_id,
          id: chain_id,
          queue_length: 1,
          finished_callback: {:fun, fn d -> send(parent, {:finished, d}) end}
        )

      {:ok, _} = Sup.add_chain_worker(sup_id, chain_id, fn d -> {:next, d} end, id: 1)
      {:ok, pid} = Sup.get_pid_chain_worker(sup_id, chain_id, 1)

      # With queue_length 1, a leaked queue entry would block the next message for 5 s
      # and then restart the worker.
      for i <- 1..5, do: {:ok, _} = Sup.send_to_chain(sup_id, chain_id, i)

      for i <- 1..5, do: assert_receive({:finished, ^i}, 1_000)
      assert {:ok, ^pid} = Sup.get_pid_chain_worker(sup_id, chain_id, 1)
    end
  end

  describe "finished callback (4.6)" do
    test "a broadcast into two last-node replicas runs the callback once per copy" do
      parent = self()

      chain =
        chain_with(:broadcast,
          finished_callback: {:fun, fn d -> send(parent, {:fin, d}) end}
        )

      chain =
        chain
        |> replicated_node(:first, 1, {:fun, fn d -> {:next, d} end})
        |> replicated_node(:last, 2, {:fun, fn d -> {:next, d} end})

      Messaging.new_data(chain, msg(:payload, parent))

      assert_receive {:fin, :payload}, 2_000
      assert_receive {:fin, :payload}, 2_000
      refute_receive {:fin, _}, 150
    end

    test "mfa callback receives the final data" do
      parent = self()

      chain =
        chain_with(:random,
          finished_callback: {__MODULE__, :notify, [parent]}
        )
        |> replicated_node(:only, 1, {:fun, fn d -> {:next, d} end})

      Messaging.new_data(chain, msg(:mfa_payload, parent))
      assert_receive {:mfa_finished, :mfa_payload}, 2_000
    end

    test "a raising callback returns the real reason" do
      chain = chain_with(:random, finished_callback: {:fun, fn _ -> raise "cb boom" end})

      assert {:error, {:error, %RuntimeError{message: "cb boom"}}} =
               Messaging.send_next(chain, 9, msg(:x, self()))
    end
  end

  @doc false
  def notify(data, parent), do: send(parent, {:mfa_finished, data})
end
