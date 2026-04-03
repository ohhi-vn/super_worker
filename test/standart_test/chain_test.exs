defmodule SuperWorker.Supervisor.ChainTest do
  use ExUnit.Case, async: false

  require Logger
  alias SuperWorker.Supervisor, as: Sup
  alias SuperWorker.Supervisor.{Chain, Worker, Db}

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
end
