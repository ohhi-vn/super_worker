defmodule SuperWorker.Supervisor.ChainWorkloadTest do
  use ExUnit.Case, async: false

  require Logger
  alias SuperWorker.Supervisor, as: Sup
  alias SuperWorker.Supervisor.{Chain, Worker, Db}

  doctest Chain

  @sup_id :test_workload_chain

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

  @tag timeout: 300_000
  test "add workers to chain" do
    id = make_ref()
    num_workers = 10_000
    {:ok, _} = Sup.add_chain(@sup_id, id: id, restart_strategy: :one_for_one)

    for index <- 1..num_workers do
      {:ok, _} = Sup.add_chain_worker(@sup_id, id, {MyTest, :loop, [index]}, id: index)
    end

    {:ok, chain} = Sup.get_chain(@sup_id, id)
    # wait for workers to be added, need to adjust for slow machines.
    # TO-DO: Improve code for add worker (wait for worker to be added).
    Process.sleep(100)

    {:ok, workers} = Chain.get_all_workers(chain)

    assert(num_workers == length(workers))
  end

  @tag timeout: 300_000
  test "remove worker in chain" do
    chain_id = make_ref()
    num_workers = 10_000

    {:ok, _} = Sup.add_chain(@sup_id, id: chain_id, restart_strategy: :one_for_one)

    for index <- 1..num_workers do
      {:ok, _} =
        Sup.add_chain_worker(@sup_id, chain_id, {MyTest, :loop, [index]}, id: index)
    end

    {:ok, chain} = Sup.get_chain(@sup_id, chain_id)
    {:ok, workers} = Chain.get_all_workers(chain)

    assert(num_workers == length(workers))

    for index <- 1..num_workers do
      Sup.remove_chain_worker(@sup_id, chain_id, index)
    end

    for index <- 1..num_workers do
      result = Sup.get_pid_chain_worker(@sup_id, chain_id, index)
      assert match?({:error, _}, result)
    end
  end

  @tag timeout: 300_000
  test "add workers to chain parallel" do
    num_chains = 100
    num_workers = 1_000
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
      {:ok, chain} = Sup.get_chain(@sup_id, chain_id)
      {:ok, workers} = Chain.get_all_workers(chain)
      assert(num_workers == length(workers))
    end

    for index <- 1..num_chains do
      chain_id = {ref, index}
      Sup.remove_chain(@sup_id, chain_id)
    end
  end

  @tag timeout: 300_000
  test "send data to chain" do
    chain_id = make_ref()
    num_workers = 10_000
    {:ok, _} = Sup.add_chain(@sup_id, id: chain_id, restart_strategy: :one_for_one)

    for index <- 1..num_workers do
      {:ok, _} = Sup.add_chain_worker(@sup_id, chain_id, {MyTest, :ping_pong, []}, id: index)
    end

    Logger.debug("send data to chain: #{inspect(chain_id)}")
    {:ok, _} = Sup.send_to_chain(@sup_id, chain_id, {:ping, self()}, 1_000)

    result =
      receive do
        {:pong, _} ->
          true

        other ->
          IO.inspect(other)
          false
      after
        5_000 -> :timeout
      end

    assert(true == result)
  end
end
