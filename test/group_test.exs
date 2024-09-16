defmodule SuperWorker.Supervisor.GroupTest do
  use ExUnit.Case

  alias SuperWorker.Supervisor, as: Sup
  alias SuperWorker.Supervisor.Group
  alias SuperWorker.Supervisor.Worker
  alias SuperWorker.Supervisor.Chain

  @group {:group1, "test group"}
  @sup_id :sup_test

  setup_all do
    :ok = Sup.start([link: false, id: :sup_test])
  end

  setup do
    :ok
  end


  @doc """
  Test for adding group strategy.
  """
  test "add group & verify strategy" do
    {:ok, _} = Sup.add_group(@sup_id, [id: :group2, restart_strategy: :one_for_one])
    {:ok, _} = Sup.add_group(@sup_id, [id: :group3, restart_strategy: :one_for_all])
    {:ok, group2} = Sup.get_group(@sup_id, :group2)
    {:ok, group3} = Sup.get_group(@sup_id, :group3)
    assert(:one_for_one == group2.restart_strategy && :one_for_all == group3.restart_strategy)
  end

  test "add workers to group" do
    {:ok,_} = Sup.add_group(@sup_id, [id: @group, restart_strategy: :one_for_one])
    list =
    for index <- 1..3 do
      {:ok, _} = Sup.add_group_worker(@sup_id, @group, {__MODULE__, :loop, [index]}, [id: index])
    end

    {:ok, group} = Sup.get_group(@sup_id, @group)
    # wait for workers to be added, need to adjust for slow machines.
    # TO-DO: Improve code for add worker (wait for worker to be added).
    Process.sleep(100)

    assert(length(list) == length(Group.get_all_workers(group)))
  end

  test "send data to worker in group" do
    group_id = :group_loop_send
    {:ok,_} = Sup.add_group(@sup_id, [id: group_id, restart_strategy: :one_for_one])
    {:ok, _} = Sup.add_group_worker(@sup_id, group_id, {__MODULE__, :loop, [1]}, [id: 1])

    Process.sleep(100)
    Sup.send_to_group(@sup_id, group_id, 1, {:ping, self()})

    result =
      receive do
        {:pong, sender} -> true
      after 1_000 -> false
      end

    assert(true == result )
  end

  test "remove worker from group" do
    group_id = :group_loop_remove
    {:ok,_} = Sup.add_group(@sup_id, [id: group_id, restart_strategy: :one_for_one])
    {:ok, _} = Sup.add_group_worker(@sup_id, group_id, {__MODULE__, :loop, [1]}, [id: 1])

    Process.sleep(100)
    :ok = Sup.send_to_group(@sup_id, group_id, 1, {:ping, self()})

    result =
      receive do
        {:pong, sender} -> true
      after 1_000 -> false
      end

    assert(true == result)

    :ok == Sup.remove_group_worker(@sup_id, group_id, 1)
    {:error, _ } = Sup.send_to_group(@sup_id, group_id, 1, {:ping, self()})
  end

  ## Helper functions

  # Basic loop, receive messages and print them.
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

  def send_to_chain(sup_id, chain_id, data \\ 10) do
    Sup.send_to_chain(sup_id, chain_id, data)
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
