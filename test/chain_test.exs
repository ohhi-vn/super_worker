defmodule SuperWorker.Supervisor.ChainTest do
  use ExUnit.Case, async: false

  require Logger
  alias SuperWorker.Supervisor, as: Sup
  alias SuperWorker.Supervisor.{Chain, Worker}

  doctest Chain

  @sup_id :sup_test_chain

  setup_all do
    {:ok, _} = Sup.start(link: false, id: @sup_id)
    :ok
  end

  setup do
    :ok
  end

  @tag :chain_verify_strategy
  test "add chain & verify strategy" do
    id1 = make_ref()
    id2 = make_ref()
    {:ok, _} = Sup.add_chain(@sup_id, id: id1, restart_strategy: :one_for_one)
    {:ok, _} = Sup.add_chain(@sup_id, id: id2, restart_strategy: :one_for_all)
    {:ok, chain1} = Sup.get_chain(@sup_id, id1)
    {:ok, chain2} = Sup.get_chain(@sup_id, id2)
    assert :one_for_one == chain1.restart_strategy
    assert :one_for_all == chain2.restart_strategy
  end

  @tag :chain_add_workers
  test "add workers to chain" do
    id = make_ref()
    {:ok, _} = Sup.add_chain(@sup_id, id: id, restart_strategy: :one_for_one)

    list =
      for index <- 1..3 do
        {:ok, _} = Sup.add_chain_worker(@sup_id, id, {__MODULE__, :loop, [index]}, id: index)
      end

    {:ok, chain} = Sup.get_chain(@sup_id, id)
    # wait for workers to be added, need to adjust for slow machines.
    # TO-DO: Improve code for add worker (wait for worker to be added).
    Process.sleep(100)

    {:ok, workers} = Chain.get_all_workers(chain)

    assert(length(list) == length(workers))
  end

  @tag :chain_add_workers_2
  test "add workers to chain 2" do
    ref = make_ref()

    for index <- 1..10 do
      chain_id = {ref, index}
      {:ok, _} = Sup.add_chain(@sup_id, id: chain_id, restart_strategy: :one_for_one)

      list =
        for worker_index <- 1..10 do
          {:ok, _} =
            Sup.add_chain_worker(@sup_id, chain_id, {__MODULE__, :loop, [worker_index]},
              id: worker_index
            )
        end

      {:ok, chain} = Sup.get_chain(@sup_id, chain_id)

      {:ok, workers} = Chain.get_all_workers(chain)

      assert(length(list) == length(workers))
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
            Sup.add_chain_worker(@sup_id, chain_id, {__MODULE__, :loop, [worker_index]},
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
  end

  @tag :chain_send_data
  test "send data to chain" do
    chain_id = make_ref()
    {:ok, _} = Sup.add_chain(@sup_id, id: chain_id, restart_strategy: :one_for_one)
    {:ok, _} = Sup.add_chain_worker(@sup_id, chain_id, {__MODULE__, :ping_pong, []}, id: 1)

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
        1500 -> :timeout
      end

    assert(true == result)
  end

  ## Helper functions

  # TO-DO: Support for loop function in chain?
  def loop(id) do
    prefix = "[#{inspect(Process.get({:supervisor, :worker_id}))}, #{inspect(self())}]"

    receive do
      {:ping, sender} ->
        IO.puts(prefix <> " Pong to #{inspect(sender)}")
        send(sender, {:pong, self()})

      msg ->
        IO.puts(prefix <> " task received: #{inspect(msg)}")
    end

    loop(id)
  end

  def ping_pong({:ping, sender}) do
    IO.puts("ping_pong(#{inspect(self())}), new task")

    IO.puts(" Pong to #{inspect(sender)}")
    send(sender, {:pong, self()})
  end

  def task(n, sleep \\ 100) do
    prefix = "[#{inspect(Process.get({:supervisor, :worker_id}))}, #{inspect(self())}]"
    IO.puts(prefix <> " Task is started, param: #{n}")

    sum =
      Enum.reduce(1..n, 0, fn i, acc ->
        :timer.sleep(sleep)
        acc + i
      end)

    IO.puts(IO.puts(prefix <> " Task done, #{sum}"))

    {:next, n + 1}
  end

  def task_crash(n, at, sleep \\ 100) do
    prefix = "[#{inspect(Process.get({:supervisor, :worker_id}))}, #{inspect(self())}]"
    IO.puts(prefix <> " Task is started, param: #{n}")

    sum =
      Enum.reduce(1..n, 0, fn i, acc ->
        if i == at,
          do:
            raise(
              "Task #{inspect(Process.get({:supervisor, :worker_id}))} raised an error at #{i}"
            )

        :timer.sleep(sleep)
        acc + i
      end)

    IO.puts(prefix <> " Task done, #{sum}")

    {:next, n + 1}
  end

  # return a anonymous function.
  def anonymous do
    fn ->
      prefix = "[#{inspect(Process.get({:supervisor, :worker_id}))}, #{inspect(self())}]"
      IO.puts(prefix <> " Anonymous function")

      for i <- 1..5 do
        IO.puts(prefix <> " Task #{i}")
        :timer.sleep(100)
      end
    end
  end
end
