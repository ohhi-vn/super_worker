defmodule SuperWorker.Supervisor.ChainRestartTest do
  # Restart strategies and restart reporting (improve-chain-perf-and-errors tasks 5.1 - 5.4).
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias SuperWorker.Supervisor, as: Sup
  alias SuperWorker.Supervisor.{Chain, Db, Worker}

  setup do
    sup_id = :"sup_chain_restart_#{System.unique_integer([:positive])}"
    {:ok, _} = Sup.start_with_config(link: false, id: sup_id, num_partitions: 1)

    on_exit(fn ->
      if Sup.running?(sup_id), do: Sup.stop(sup_id)
    end)

    %{sup_id: sup_id}
  end

  defp chain_table do
    table = Db.init(:"chain_restart_#{System.unique_integer([:positive])}")
    %Chain{id: make_ref(), table: table}
  end

  defp chain_node(id), do: %Worker{id: id, type: :chain, fun: {:fun, fn d -> {:next, d} end}}

  defp chain_of(n) do
    Enum.reduce(1..n, chain_table(), fn id, acc ->
      {:ok, acc} = Chain.add_worker(acc, chain_node(id))
      acc
    end)
  end

  defp pid_of(chain, id) do
    {:ok, [{^id, pid}]} = Db.get_chain_order(chain.table, chain.id, order_of(chain, id))
    pid
  end

  defp order_of(chain, id) do
    {:ok, %Worker{order: order}} = Chain.get_worker(chain, id)
    order
  end

  setup do
    Process.flag(:trap_exit, true)
    :ok
  end

  # 5.1 one_for_all: every worker is respawned, and a restart of an empty chain succeeds.
  test "restart_all_workers respawns every node with the same id and order" do
    chain = chain_of(3)
    before = Enum.map(1..3, &pid_of(chain, &1))

    assert {:ok, ^chain} = Chain.restart_all_workers(chain)

    after_pids = Enum.map(1..3, &pid_of(chain, &1))
    assert Enum.zip(before, after_pids) |> Enum.all?(fn {b, a} -> a != b end)
    assert Enum.all?(1..3, &(order_of(chain, &1) == &1))
  end

  test "restart_all_workers on an empty chain succeeds without spawning" do
    chain = chain_table()
    assert {:ok, ^chain} = Chain.restart_all_workers(chain)
    assert Chain.count_workers(chain) == 0
  end

  # 5.2 rest_for_one: the failed node and later ones restart; earlier ones keep their pid.
  test "restart_from_worker keeps earlier nodes and restarts the tail" do
    chain = chain_of(4)
    [p1, p2, p3, p4] = Enum.map(1..4, &pid_of(chain, &1))

    assert {:ok, _} = Chain.restart_from_worker(chain, 2)

    assert pid_of(chain, 1) == p1
    assert pid_of(chain, 2) != p2
    assert pid_of(chain, 3) != p3
    assert pid_of(chain, 4) != p4
  end

  test "restart_from_worker on a missing id returns worker_not_found" do
    assert {:error, :worker_not_found} = Chain.restart_from_worker(chain_of(1), :missing)
  end

  # 5.3 Normal exits are not restarted and produce no error log.
  test "a normal exit under rest_for_one is not restarted and logs no error", %{sup_id: sup_id} do
    chain_id = make_ref()
    {:ok, _} = Sup.add_chain(sup_id, id: chain_id, restart_strategy: :rest_for_one)
    {:ok, _} = Sup.add_chain_worker(sup_id, chain_id, fn d -> {:next, d} end, id: 1)
    {:ok, pid} = Sup.get_pid_chain_worker(sup_id, chain_id, 1)

    log =
      capture_log(fn ->
        send(pid, {:stop, chain_id})
        Process.sleep(200)
      end)

    refute log =~ "[error]"
    assert {:error, _} = Sup.get_pid_chain_worker(sup_id, chain_id, 1)
  end

  # 5.4 restart_worker respawns even after the ref row was already cleaned up.
  test "restart_worker respawns after the ref row was cleaned up" do
    chain = chain_of(1)
    old = pid_of(chain, 1)

    assert {:ok, _} = Chain.kill_worker(chain, 1)
    assert {:ok, _} = Chain.restart_worker(chain, 1)

    new = pid_of(chain, 1)
    assert new != old
    assert Process.alive?(new)
  end

  # 5.1 failure list: writes from a non-owner into a :protected table fail, so each
  # respawn fails and restart_all reports those ids instead of raising.
  test "restart_all_workers reports the ids it could not respawn" do
    parent = self()

    owner =
      spawn(fn ->
        table =
          :ets.new(:"protected_chain_#{System.unique_integer([:positive])}", [:set, :protected])

        chain = %Chain{id: make_ref(), table: table}

        {:ok, chain} = Chain.add_worker(chain, chain_node(1))
        {:ok, chain} = Chain.add_worker(chain, chain_node(2))
        send(parent, {:chain, chain})

        receive do
          :stop -> :ok
        end
      end)

    assert_receive {:chain, chain}, 2_000
    assert {:error, {:restart_failed, [1, 2]}} = Chain.restart_all_workers(chain)

    send(owner, :stop)
  end

  test "restart_worker on an unknown id returns worker_not_found" do
    assert {:error, :worker_not_found} = Chain.restart_worker(chain_of(1), :missing)
  end
end
