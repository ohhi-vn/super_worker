defmodule SuperWorker.Supervisor.ChainTest do
  use ExUnit.Case, async: false

  alias SuperWorker.Supervisor, as: Sup
  alias SuperWorker.Supervisor.{Chain, Worker}

  @sup_id :sup_test_chain

  setup_all do
    {:ok, _} = Sup.start([link: false, id: @sup_id])
    :ok
  end

  setup do
    :ok
  end

  test "add chain & verify strategy" do
    {:ok, _} = Sup.add_chain(@sup_id, [id: :chain2, restart_strategy: :one_for_one])
    {:ok, _} = Sup.add_chain(@sup_id, [id: :chain3, restart_strategy: :one_for_all])
    {:ok, chain2} = Sup.get_chain(@sup_id, :chain2)
    {:ok, chain3} = Sup.get_chain(@sup_id, :chain3)
    assert :one_for_one == chain2.restart_strategy
    assert :one_for_all == chain3.restart_strategy
  end

  test "add workers to chain" do
    {:ok,_} = Sup.add_chain(@sup_id, [id: :chain1, restart_strategy: :one_for_one])
    list =
    for index <- 1..3 do
      {:ok, _} = Sup.add_chain_worker(@sup_id, :chain1, {__MODULE__, :loop, [index]}, [id: index])
    end

    {:ok, chain} = Sup.get_chain(@sup_id, :chain1)
    # wait for workers to be added, need to adjust for slow machines.
    # TO-DO: Improve code for add worker (wait for worker to be added).
    Process.sleep(100)

    {:ok, workers} = Chain.get_all_workers(chain)

    assert(length(list) == length(workers))
  end

  test "send data to worker in chain" do
    chain_id = :chain_loop_send
    {:ok,_} = Sup.add_chain(@sup_id, [id: chain_id, restart_strategy: :one_for_one])
    {:ok, _} = Sup.add_chain_worker(@sup_id, chain_id, {__MODULE__, :loop, []}, [id: 1])

    Process.sleep(100)
    Sup.send_to_chain(@sup_id, chain_id, 1, {:ping, self()})

    result =
      receive do
        {:pong, _} -> true
      after 1_000 -> :timeout
      end

    assert(true == result )
  end

  ## Helper functions

  # TO-DO: Support for loop function in chain?
  def loop(id) do
    prefix = "[#{inspect Process.get({:supervisor, :worker_id})}, #{inspect self()}]"
    receive do
      {:ping, sender} ->
        IO.puts prefix <> " Pong to #{inspect sender}"
        send(sender, {:pong, self()})

      msg -> IO.puts prefix <> " task received: #{inspect msg}"
    end

    loop(id)
  end

  def task(n, sleep \\ 100) do
    prefix = "[#{inspect Process.get({:supervisor, :worker_id})}, #{inspect self()}]"
    IO.puts prefix <> " Task is started, param: #{n}"

    sum = Enum.reduce(1..n, 0, fn i, acc ->
      :timer.sleep(sleep)
      acc + i
    end)
    IO.puts  IO.puts prefix <> " Task done, #{sum}"

    {:next, n + 1}
  end

  def task_crash(n, at, sleep \\ 100) do
    prefix = "[#{inspect Process.get({:supervisor, :worker_id})}, #{inspect self()}]"
    IO.puts prefix <> " Task is started, param: #{n}"

    sum = Enum.reduce(1..n, 0, fn i, acc ->
      if i == at, do: raise "Task #{inspect Process.get({:supervisor, :worker_id})} raised an error at #{i}"
      :timer.sleep(sleep)
      acc + i
    end)
    IO.puts prefix <> " Task done, #{sum}"

    {:next, n + 1}
  end

  # return a anonymous function.
  def anonymous do
    fn ->
      prefix = "[#{inspect Process.get({:supervisor, :worker_id})}, #{inspect self()}]"
      IO.puts prefix <> " Anonymous function"
      for i <- 1..5 do
        IO.puts prefix <> " Task #{i}"
        :timer.sleep(100)
      end
    end
  end

end
