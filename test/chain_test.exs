defmodule SuperWorker.Supervisor.ChainTest do
  use ExUnit.Case, async: false

  require Logger
  alias SuperWorker.Supervisor, as: Sup
  alias SuperWorker.Supervisor.{Chain, Worker, Db}

  doctest Chain

  @sup_id :sup_test_chain

  setup_all do
    {:ok, _} = Sup.start_with_config(link: false, id: @sup_id)
    :ok
  end

  setup do
    if Sup.running?(@sup_id) do
      :ok
    else
      raise "Supervisor is not running"
    end
  end

  @tag :chain_add_workers
  test "add workers to chain" do
    id = make_ref()
    {:ok, _} = Sup.add_chain(@sup_id, id: id, restart_strategy: :one_for_one)

    list =
      for index <- 1..3 do
        {:ok, _} = Sup.add_chain_worker(@sup_id, id, {MyTest, :loop, [index]}, id: index)
      end

    {:ok, count} = Sup.count_workers_in_chain(@sup_id, id)
    assert(3 == count)
  end

  @tag :chain_remove_worker
  test "remove worker in chain" do
    chain_id = make_ref()
    worker_id = 1
    {:ok, _} = Sup.add_chain(@sup_id, id: chain_id, restart_strategy: :one_for_one)

    {:ok, _} =
      Sup.add_chain_worker(@sup_id, chain_id, {MyTest, :loop, [worker_id]}, id: worker_id)

    {:ok, pid} = Sup.get_pid_chain_worker(@sup_id, chain_id, worker_id)

    Sup.remove_chain_worker(@sup_id, chain_id, worker_id)

    result = Sup.get_pid_chain_worker(@sup_id, chain_id, worker_id)
    assert match?({:error, _}, result)
  end

  @tag :remove_chain
  test "remove chain" do
    chain_id = make_ref()
    worker_id = 1
    {:ok, _} = Sup.add_chain(@sup_id, id: chain_id, restart_strategy: :one_for_one)

    {:ok, _} =
      Sup.add_chain_worker(@sup_id, chain_id, {MyTest, :loop, [worker_id]}, id: worker_id)

    {:ok, count} = Sup.count_workers_in_chain(@sup_id, chain_id)

    assert(1 == count)

    Sup.remove_chain(@sup_id, chain_id)

    result = Sup.get_pid_chain_worker(@sup_id, chain_id, worker_id)
    assert match?({:error, _}, result)
  end

  @tag :chain_add_workers_2
  test "add workers to chain 2" do
    ref = make_ref()
    num_chains = 10
    num_workers = 10

    for index <- 1..num_chains do
      chain_id = {ref, index}
      {:ok, _} = Sup.add_chain(@sup_id, id: chain_id, restart_strategy: :one_for_one)

      list =
        for worker_index <- 1..num_workers do
          {:ok, _} =
            Sup.add_chain_worker(@sup_id, chain_id, {MyTest, :loop, [worker_index]},
              id: worker_index
            )
        end
    end

    for index <- 1..num_chains do
      chain_id = {ref, index}
      {:ok, count} = Sup.count_workers_in_chain(@sup_id, chain_id)
      assert(count == num_workers)
    end
  end

  @tag :chain_add_workers_parallel
  test "add workers to chain parallel" do
    num_chains = 1
    num_workers = 100
    me = self()
    ref = make_ref()

    f = fn index ->
      chain_id = {ref, index}
      {:ok, _} = Sup.add_chain(@sup_id, id: chain_id, restart_strategy: :one_for_one)

      list =
        for worker_index <- 1..num_workers do
          {:ok, _} =
            Sup.add_chain_worker(@sup_id, chain_id, {MyTest, :loop, [worker_index]},
              id: worker_index
            )
        end

      send(me, {:ok, index})
    end

    for index <- 1..num_chains do
      spawn(fn -> f.(index) end)
    end

    for index <- 1..num_chains do
      receive do
        {:ok, index} -> true
      after
        10_000 -> raise "timeout for adding chains and workers"
      end
    end

    for index <- 1..num_chains do
      chain_id = {ref, index}
      {:ok, count} = Sup.count_workers_in_chain(@sup_id, chain_id)
      assert(num_workers == count)
    end
  end

  @tag :chain_send_data
  test "send data to chain" do
    chain_id = make_ref()
    parent = self()

    fun = fn result ->
      IO.puts("chain finished, result: #{inspect(result)}")
      send(parent, {:processed, result})
    end

    {:ok, _} =
      Sup.add_chain(@sup_id,
        id: chain_id,
        restart_strategy: :one_for_one,
        finished_callback: {:fun, fun}
      )

    {:ok, _} = Sup.add_chain_worker(@sup_id, chain_id, {MyTest, :task, [100]}, id: 1)

    Logger.debug("send data to chain: #{inspect(chain_id)}")
    {:ok, _} = Sup.send_to_chain(@sup_id, chain_id, 3)

    result =
      receive do
        {:processed, result} ->
          IO.puts("received result: #{inspect(result)}")
          true

        other ->
          IO.inspect(other)
          false
      after
        15_000 -> :timeout
      end

    assert(true == result)
  end

  test "send data to chain 2" do
    chain_id = make_ref()
    parent = self()
    num_workers = 10

    fun = fn result ->
      IO.puts("chain finished, result: #{inspect(result)}")
      send(parent, {:processed, result})
    end

    {:ok, _} =
      Sup.add_chain(@sup_id,
        id: chain_id,
        restart_strategy: :one_for_one,
        finished_callback: {:fun, fun}
      )

    for i <- 1..num_workers do
      {:ok, _} = Sup.add_chain_worker(@sup_id, chain_id, {MyTest, :task, [num_workers]}, id: i)
    end

    Logger.debug("send data to chain: #{inspect(chain_id)}")
    {:ok, _} = Sup.send_to_chain(@sup_id, chain_id, 3)

    result =
      receive do
        {:processed, result} ->
          IO.puts("received result: #{inspect(result)}")
          true

        other ->
          IO.inspect(other)
          false
      after
        15_000 -> :timeout
      end

    assert(true == result)
  end
end
