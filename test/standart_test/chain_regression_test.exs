defmodule SuperWorker.Supervisor.ChainRegressionTest do
  # Regression tests for improve-chain-perf-and-errors (tasks 1.2 - 1.8).
  use ExUnit.Case, async: false

  alias SuperWorker.Supervisor, as: Sup
  alias SuperWorker.Supervisor.{Chain, Db, Worker}
  alias SuperWorker.Supervisor.Chain.Messaging
  alias SuperWorker.Supervisor.Message

  @moduletag :capture_log

  setup do
    sup_id = :"sup_chain_reg_#{System.unique_integer([:positive])}"
    {:ok, _} = Sup.start_with_config(link: false, id: sup_id, num_partitions: 1)

    on_exit(fn ->
      if Sup.running?(sup_id), do: Sup.stop(sup_id)
    end)

    %{sup_id: sup_id}
  end

  # Chain-level helper: a standalone table with the chain row stored.
  defp chain_table(chain) do
    table = Db.init(:"chain_reg_#{System.unique_integer([:positive])}")
    :ok = Db.put_chain(table, %{chain | table: table})
    %{chain | table: table}
  end

  defp msg(id, data, from), do: %Message{id: id, data: data, type: :new_data, from: from}

  # 1.2 A multi-replica node must make every replica routable.
  @tag :chain_regression_replicas
  test "a multi-replica node delivers to every replica" do
    parent = self()
    chain = chain_table(%Chain{id: :rep, send_type: :broadcast})

    worker = %Worker{
      id: :src,
      type: :chain,
      num_workers: 3,
      fun:
        {:fun,
         fn data ->
           send(parent, {:seen, self(), data})
           {:drop, :seen}
         end}
    }

    {:ok, chain} = Chain.add_worker(chain, worker)
    _ = Messaging.new_data(chain, msg(1, :hello, parent))

    pids =
      for _ <- 1..3 do
        assert_receive {:seen, pid, :hello}, 2_000
        pid
      end

    assert length(Enum.uniq(pids)) == 3
  end

  # 1.3 Removing a middle node must not strand the nodes after it.
  @tag :chain_regression_remove_middle
  test "removing a middle node still reaches the remaining nodes", %{sup_id: sup_id} do
    chain_id = make_ref()
    parent = self()

    {:ok, _} =
      Sup.add_chain(sup_id,
        id: chain_id,
        finished_callback: {:fun, fn data -> send(parent, {:finished, data}) end}
      )

    {:ok, _} = Sup.add_chain_worker(sup_id, chain_id, fn d -> {:next, d} end, id: 1)
    {:ok, _} = Sup.add_chain_worker(sup_id, chain_id, fn d -> {:next, d} end, id: 2)

    {:ok, _} =
      Sup.add_chain_worker(
        sup_id,
        chain_id,
        fn d ->
          send(parent, {:n3, d})
          {:next, d}
        end,
        id: 3
      )

    :ok = Sup.remove_chain_worker(sup_id, chain_id, 2)

    {:ok, _} = Sup.send_to_chain(sup_id, chain_id, :x)

    assert_receive {:n3, :x}, 2_000
    assert_receive {:finished, :x}, 2_000
  end

  # 1.4 Forwarding to a killed replica must not report success.
  @tag :chain_regression_dead_route
  test "a message routed to a killed replica is not reported as sent" do
    # Workers are linked to the caller; trap exits as the partition process does.
    Process.flag(:trap_exit, true)
    parent = self()
    chain = chain_table(%Chain{id: :dead})

    worker = %Worker{id: :only, type: :chain, fun: {:fun, fn d -> {:next, d} end}}
    {:ok, chain} = Chain.add_worker(chain, worker)
    {:ok, _} = Chain.kill_worker(chain, :only)

    assert {:error, :no_live_replica} = Messaging.send_next(chain, 1, msg(1, :lost, parent))
  end

  # 1.5 :rest_for_one must restart the tail, not crash the partition.
  @tag :chain_regression_rest_for_one
  test "rest_for_one restarts the failed node and those after it", %{sup_id: sup_id} do
    chain_id = make_ref()
    {:ok, _} = Sup.add_chain(sup_id, id: chain_id, restart_strategy: :rest_for_one)

    for id <- 1..3 do
      {:ok, _} = Sup.add_chain_worker(sup_id, chain_id, fn d -> {:next, d} end, id: id)
    end

    {:ok, p1} = Sup.get_pid_chain_worker(sup_id, chain_id, 1)
    {:ok, p2} = Sup.get_pid_chain_worker(sup_id, chain_id, 2)
    {:ok, p3} = Sup.get_pid_chain_worker(sup_id, chain_id, 3)

    send(p2, {:kill, :boom})

    wait_until(fn ->
      match?({:ok, pid} when pid != p2, Sup.get_pid_chain_worker(sup_id, chain_id, 2))
    end)

    wait_until(fn ->
      match?({:ok, pid} when pid != p3, Sup.get_pid_chain_worker(sup_id, chain_id, 3))
    end)

    assert {:ok, ^p1} = Sup.get_pid_chain_worker(sup_id, chain_id, 1)
  end

  # 1.6 remove_worker/2 must report a missing worker.
  @tag :chain_regression_remove_missing
  test "remove_worker on a missing id returns worker_not_found" do
    chain = chain_table(%Chain{id: :rm})
    assert {:error, :worker_not_found} = Chain.remove_worker(chain, :missing)
  end

  # 1.7 A conflicting replica id must not leave partially spawned replicas.
  @tag :chain_regression_add_conflict
  test "a multi-replica add with a conflicting replica id spawns no replica" do
    chain = chain_table(%Chain{id: :conflict})

    {:ok, chain} =
      Chain.add_worker(chain, %Worker{
        id: {:multi_workers, :m, 2},
        type: :chain,
        fun: {:fun, fn d -> {:next, d} end}
      })

    assert {:error, :already_exists} =
             Chain.add_worker(chain, %Worker{
               id: :m,
               type: :chain,
               num_workers: 3,
               fun: {:fun, fn d -> {:next, d} end}
             })

    assert Chain.count_workers(chain) == 1
  end

  # 1.8 Idle chain workers must not exit on a timer.
  @tag :slow
  @tag :chain_regression_idle
  test "a chain worker idle for 31 seconds keeps the same pid", %{sup_id: sup_id} do
    chain_id = make_ref()
    {:ok, _} = Sup.add_chain(sup_id, id: chain_id)
    {:ok, _} = Sup.add_chain_worker(sup_id, chain_id, fn d -> {:next, d} end, id: 1)
    {:ok, pid} = Sup.get_pid_chain_worker(sup_id, chain_id, 1)

    Process.sleep(31_000)

    assert {:ok, ^pid} = Sup.get_pid_chain_worker(sup_id, chain_id, 1)
  end

  defp wait_until(fun, tries \\ 100) do
    if fun.() do
      :ok
    else
      if tries <= 0, do: flunk("wait_until timed out")
      Process.sleep(20)
      wait_until(fun, tries - 1)
    end
  end
end
