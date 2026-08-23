defmodule SuperWorker.Supervisor.ChainTest do
  use ExUnit.Case, async: false

  require Logger
  alias SuperWorker.Supervisor, as: Sup
  alias SuperWorker.Supervisor.{Chain, Db}

  doctest Chain

  @moduletag :capture_log

  setup do
    # Use a unique supervisor ID per test to avoid state pollution
    sup_id = :"sup_chain_#{System.unique_integer([:positive])}"
    {:ok, _} = Sup.start_with_config(link: false, id: sup_id, num_partitions: 1)

    on_exit(fn ->
      if Sup.running?(sup_id) do
        Sup.stop(sup_id)
      end
    end)

    %{sup_id: sup_id}
  end

  @tag :chain_add_workers
  test "add workers to chain", %{sup_id: sup_id} do
    id = make_ref()
    {:ok, _} = Sup.add_chain(sup_id, id: id, restart_strategy: :one_for_one)

    for index <- 1..3 do
      {:ok, _} = Sup.add_chain_worker(sup_id, id, {MyTest, :loop, [index]}, id: index)
    end

    assert {:ok, 3} = Sup.count_workers_in_chain(sup_id, id)
  end

  @tag :chain_remove_worker
  test "remove worker in chain", %{sup_id: sup_id} do
    chain_id = make_ref()
    worker_id = 1
    {:ok, _} = Sup.add_chain(sup_id, id: chain_id, restart_strategy: :one_for_one)

    {:ok, _} =
      Sup.add_chain_worker(sup_id, chain_id, {MyTest, :loop, [worker_id]}, id: worker_id)

    {:ok, pid} = Sup.get_pid_chain_worker(sup_id, chain_id, worker_id)
    assert is_pid(pid)

    Sup.remove_chain_worker(sup_id, chain_id, worker_id)

    assert {:error, _} = Sup.get_pid_chain_worker(sup_id, chain_id, worker_id)
  end

  @tag :remove_chain
  test "remove chain", %{sup_id: sup_id} do
    chain_id = make_ref()
    worker_id = 1
    {:ok, _} = Sup.add_chain(sup_id, id: chain_id, restart_strategy: :one_for_one)

    {:ok, _} =
      Sup.add_chain_worker(sup_id, chain_id, {MyTest, :loop, [worker_id]}, id: worker_id)

    assert {:ok, 1} = Sup.count_workers_in_chain(sup_id, chain_id)

    Sup.remove_chain(sup_id, chain_id)

    assert {:error, _} = Sup.get_pid_chain_worker(sup_id, chain_id, worker_id)
  end

  @tag :chain_add_workers_2
  test "add workers to multiple chains", %{sup_id: sup_id} do
    ref = make_ref()
    num_chains = 5
    num_workers = 5

    for index <- 1..num_chains do
      chain_id = {ref, index}
      {:ok, _} = Sup.add_chain(sup_id, id: chain_id, restart_strategy: :one_for_one)

      for worker_index <- 1..num_workers do
        {:ok, _} =
          Sup.add_chain_worker(sup_id, chain_id, {MyTest, :loop, [worker_index]},
            id: worker_index
          )
      end
    end

    for index <- 1..num_chains do
      chain_id = {ref, index}
      assert {:ok, ^num_workers} = Sup.count_workers_in_chain(sup_id, chain_id)
    end
  end

  @tag :chain_add_workers_parallel
  test "add workers to chain parallel", %{sup_id: sup_id} do
    num_chains = 3
    num_workers = 20
    me = self()
    ref = make_ref()

    f = fn index ->
      chain_id = {ref, index}
      {:ok, _} = Sup.add_chain(sup_id, id: chain_id, restart_strategy: :one_for_one)

      for worker_index <- 1..num_workers do
        {:ok, _} =
          Sup.add_chain_worker(sup_id, chain_id, {MyTest, :loop, [worker_index]},
            id: worker_index
          )
      end

      send(me, {:ok, index})
    end

    for index <- 1..num_chains do
      spawn(fn -> f.(index) end)
    end

    for index <- 1..num_chains do
      assert_receive {:ok, ^index}, 10_000
    end

    for index <- 1..num_chains do
      chain_id = {ref, index}
      assert {:ok, ^num_workers} = Sup.count_workers_in_chain(sup_id, chain_id)
    end
  end

  @tag :chain_send_data
  test "send data to chain", %{sup_id: sup_id} do
    chain_id = make_ref()
    parent = self()

    fun = fn result ->
      send(parent, {:processed, result})
    end

    {:ok, _} =
      Sup.add_chain(sup_id,
        id: chain_id,
        restart_strategy: :one_for_one,
        finished_callback: {:fun, fun}
      )

    {:ok, _} = Sup.add_chain_worker(sup_id, chain_id, {MyTest, :task, [10]}, id: 1)

    Logger.debug("send data to chain: #{inspect(chain_id)}")
    {:ok, _} = Sup.send_to_chain(sup_id, chain_id, 3)

    assert_receive {:processed, result}, 5_000
    assert is_integer(result)
  end

  @tag :chain_send_data_multi_worker
  test "send data to chain with multiple workers", %{sup_id: sup_id} do
    chain_id = make_ref()
    parent = self()
    num_workers = 5

    fun = fn result ->
      send(parent, {:processed, result})
    end

    {:ok, _} =
      Sup.add_chain(sup_id,
        id: chain_id,
        restart_strategy: :one_for_one,
        finished_callback: {:fun, fun}
      )

    for i <- 1..num_workers do
      {:ok, _} = Sup.add_chain_worker(sup_id, chain_id, {MyTest, :task, [5]}, id: i)
    end

    Logger.debug("send data to chain: #{inspect(chain_id)}")
    {:ok, _} = Sup.send_to_chain(sup_id, chain_id, 3)

    assert_receive {:processed, result}, 5_000
    assert is_integer(result)
  end

  @tag :chain_result_protocol
  test "worker returning plain data passes it through the chain", %{sup_id: sup_id} do
    chain_id = make_ref()
    parent = self()

    {:ok, _} =
      Sup.add_chain(sup_id,
        id: chain_id,
        finished_callback: {:fun, fn data -> send(parent, {:finished, data}) end}
      )

    # Default branch forwards the raw return value to the next worker.
    {:ok, _} = Sup.add_chain_worker(sup_id, chain_id, fn _data -> :first_output end, id: 1)
    {:ok, _} = Sup.add_chain_worker(sup_id, chain_id, fn data -> {:next, data} end, id: 2)

    {:ok, _} = Sup.send_to_chain(sup_id, chain_id, :input)

    # The default branch wraps plain data as {msg_id, data}; the next worker
    # forwards it via {:next, ...}, so the callback receives the wrapped pair.
    assert_receive {:finished, {msg_id, :first_output}}, 5_000
    assert is_integer(msg_id)
  end

  @tag :chain_result_protocol_drop
  test "worker returning {:drop, reason} stops forwarding", %{sup_id: sup_id} do
    chain_id = make_ref()
    parent = self()

    {:ok, _} =
      Sup.add_chain(sup_id,
        id: chain_id,
        finished_callback: {:fun, fn data -> send(parent, {:finished, data}) end}
      )

    {:ok, _} = Sup.add_chain_worker(sup_id, chain_id, fn _data -> {:drop, :filtered} end, id: 1)

    {:ok, _} = Sup.send_to_chain(sup_id, chain_id, :input)

    refute_receive {:finished, _}, 500
  end

  @tag :chain_result_protocol_error
  test "worker returning {:error, reason} stops forwarding but keeps worker alive", %{
    sup_id: sup_id
  } do
    chain_id = make_ref()
    parent = self()

    {:ok, _} =
      Sup.add_chain(sup_id,
        id: chain_id,
        finished_callback: {:fun, fn data -> send(parent, {:finished, data}) end}
      )

    {:ok, _} =
      Sup.add_chain_worker(sup_id, chain_id, fn _data -> {:error, :bad_data} end, id: 1)

    {:ok, pid1} = Sup.get_pid_chain_worker(sup_id, chain_id, 1)

    {:ok, _} = Sup.send_to_chain(sup_id, chain_id, :input)
    refute_receive {:finished, _}, 500

    # Worker survived the error result.
    Process.sleep(50)
    assert {:ok, ^pid1} = Sup.get_pid_chain_worker(sup_id, chain_id, 1)
  end

  @tag :chain_mfa_callback
  test "mfa finished_callback receives final data", %{sup_id: sup_id} do
    chain_id = make_ref()
    parent = self()

    {:ok, _} =
      Sup.add_chain(sup_id,
        id: chain_id,
        finished_callback: {__MODULE__, :capture_callback, [parent]}
      )

    {:ok, _} = Sup.add_chain_worker(sup_id, chain_id, {MyTest, :task, [1]}, id: 1)

    {:ok, _} = Sup.send_to_chain(sup_id, chain_id, 7)

    assert_receive {:mfa_finished, _data}, 5_000
  end

  @doc "MFA callback target used by chain tests."
  def capture_callback(data, parent) do
    send(parent, {:mfa_finished, data})
    :ok
  end

  @tag :chain_unknown_message
  test "chain worker ignores unknown messages", %{sup_id: sup_id} do
    chain_id = make_ref()

    {:ok, _} = Sup.add_chain(sup_id, id: chain_id)
    {:ok, _} = Sup.add_chain_worker(sup_id, chain_id, {MyTest, :task, [1]}, id: 1)

    {:ok, pid} = Sup.get_pid_chain_worker(sup_id, chain_id, 1)
    send(pid, :some_unexpected_message)
    Process.sleep(50)

    assert Process.alive?(pid)
    assert {:ok, ^pid} = Sup.get_pid_chain_worker(sup_id, chain_id, 1)
  end

  @tag :chain_restart_on_crash
  test "one_for_one: killed chain worker is restarted", %{sup_id: sup_id} do
    chain_id = make_ref()
    parent = self()

    {:ok, _} =
      Sup.add_chain(sup_id,
        id: chain_id,
        restart_strategy: :one_for_one,
        finished_callback: {:fun, fn data -> send(parent, {:finished, data}) end}
      )

    {:ok, _} =
      Sup.add_chain_worker(sup_id, chain_id, fn data -> {:next, data} end, id: 1)

    {:ok, pid} = Sup.get_pid_chain_worker(sup_id, chain_id, 1)

    # Ask the chain worker to terminate; the supervisor sees an abnormal
    # exit (:restart is reserved for internal restart cycles) and restarts it.
    send(pid, {:kill, :simulated_crash})

    wait_until(fn ->
      match?({:ok, p} when p != pid, Sup.get_pid_chain_worker(sup_id, chain_id, 1))
    end)

    # The restarted worker still processes data.
    {:ok, _} = Sup.send_to_chain(sup_id, chain_id, :fine)
    assert_receive {:finished, :fine}, 5_000
  end

  @tag :chain_restart_worker_missing
  test "restart missing chain worker returns error", %{sup_id: sup_id} do
    chain_id = make_ref()
    {:ok, _} = Sup.add_chain(sup_id, id: chain_id)

    table = :sys.get_state(sup_id).supervisor.table
    {:ok, chain} = Db.get_chain(table, chain_id)

    assert {:error, :worker_not_found} = Chain.restart_worker(chain, :missing)
    assert {:error, :not_found} = Chain.kill_worker(chain, :missing)
    assert false == Chain.worker_exists?(chain, :missing)
  end

  @tag :chain_restart_all_on_crash
  test "one_for_all: killing one chain worker restarts all of them", %{sup_id: sup_id} do
    chain_id = make_ref()

    {:ok, _} = Sup.add_chain(sup_id, id: chain_id, restart_strategy: :one_for_all)
    {:ok, _} = Sup.add_chain_worker(sup_id, chain_id, fn data -> {:next, data} end, id: 1)
    {:ok, _} = Sup.add_chain_worker(sup_id, chain_id, fn data -> {:next, data} end, id: 2)

    {:ok, old_pid1} = Sup.get_pid_chain_worker(sup_id, chain_id, 1)
    {:ok, old_pid2} = Sup.get_pid_chain_worker(sup_id, chain_id, 2)

    send(old_pid1, {:kill, :simulated_crash})

    wait_until(fn ->
      match?({:ok, p1} when p1 != old_pid1, Sup.get_pid_chain_worker(sup_id, chain_id, 1)) and
        match?({:ok, p2} when p2 != old_pid2, Sup.get_pid_chain_worker(sup_id, chain_id, 2))
    end)
  end

  @tag :chain_multi_workers_per_node
  test "adding a chain node with num_workers > 1 spawns several workers" do
    table = Db.init(:"chain_unit_#{System.unique_integer([:positive])}")

    chain = %Chain{id: :chain_multi, table: table}

    worker = %SuperWorker.Supervisor.Worker{
      id: :multi,
      fun: {:fun, fn _data -> {:next, :ok} end},
      type: :chain,
      num_workers: 3
    }

    assert {:ok, chain} = Chain.add_worker(chain, worker)
    assert {:ok, workers} = Chain.get_all_workers(chain)
    assert length(workers) == 3

    assert Enum.all?(workers, &(&1.parent == :chain_multi))

    # All replicas of one node share a single order slot.
    assert Enum.count(Enum.uniq(Enum.map(workers, & &1.order))) == 1
    assert {:ok, {_, _}} = Db.get_chain_order(table, :chain_multi, hd(workers).order)
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
