defmodule SuperWorker.Supervisor.GroupTest do
  use ExUnit.Case, async: true

  alias SuperWorker.Supervisor, as: Sup
  alias SuperWorker.Supervisor.{Group}

  doctest Group

  @sup_id :sup_group_test

  setup_all do
    {:ok, _} = Sup.start(link: false, id: @sup_id, number_of_partitions: 2)
    :ok
  end

  setup do
    if Sup.is_running?(@sup_id) do
      :ok
    else
      raise "Supervisor is not running"
    end
  end

  @doc """
  Test for adding group strategy.
  """
  @tag :group_verify_strategy
  test "add group & verify strategy" do
    {:ok, _} = Sup.add_group(@sup_id, id: :group2, restart_strategy: :one_for_one)
    {:ok, _} = Sup.add_group(@sup_id, id: :group3, restart_strategy: :one_for_all)
    {:ok, group2} = Sup.get_group(@sup_id, :group2)
    {:ok, group3} = Sup.get_group(@sup_id, :group3)

    assert(:one_for_one == group2.restart_strategy && :one_for_all == group3.restart_strategy)
  end

  @tag :group_add_workers
  test "add workers to group" do
    group_id = :test_add_workers

    {:ok, _} = Sup.add_group(@sup_id, id: group_id, restart_strategy: :one_for_one)

    list =
      for index <- 1..5 do
        {:ok, _} =
          Sup.add_group_worker(@sup_id, group_id, {__MODULE__, :loop, [index]}, id: index)
      end

    {:ok, group} = Sup.get_group(@sup_id, group_id)

    {:ok, workers} = Group.get_all_workers(group)

    assert(length(list) == length(workers))
  end

  @tag :group_add_mixed_workers
  test "add mixed workers to group" do
    group_id = :test_add_mixed_workers
    num_workers = 5
    {:ok, _} = Sup.add_group(@sup_id, id: group_id, restart_strategy: :one_for_one)

    list =
      for index <- 1..num_workers do
        {:ok, _} =
          Sup.add_group_worker(@sup_id, group_id, {__MODULE__, :loop, [index]}, id: index)
      end

    Sup.add_group_worker(@sup_id, group_id, MyGenServer, [])
    Process.sleep(10)

    {:ok, group} = Sup.get_group(@sup_id, group_id)

    {:ok, workers} = Group.get_all_workers(group)

    assert(num_workers + 1 == length(workers))
  end

  @tag :group_add_workers_2
  test "add workers to group 2" do
    Enum.each(1..10, fn index ->
      group_id = {:test_add_workers_parallel, index}

      {:ok, _} = Sup.add_group(@sup_id, id: group_id, restart_strategy: :one_for_one)

      list =
        for worker_index <- 1..10 do
          {:ok, _} =
            Sup.add_group_worker(@sup_id, group_id, {__MODULE__, :loop, [worker_index]},
              id: {index, worker_index}
            )
        end

      Process.sleep(100)

      {:ok, group} = Sup.get_group(@sup_id, group_id)

      {:ok, workers} = Group.get_all_workers(group)

      assert(10 == length(workers))
    end)
  end

  @tag :group_send_data
  test "send data to worker in group" do
    group_id = :group_loop_send

    {:ok, _} = Sup.add_group(@sup_id, id: group_id, restart_strategy: :one_for_one)
    {:ok, _} = Sup.add_group_worker(@sup_id, group_id, {__MODULE__, :loop, [1]}, id: 1)

    Process.sleep(100)
    Sup.send_to_group(@sup_id, group_id, 1, {:ping, self()})

    result =
      receive do
        {:pong, _sender} -> true
      after
        1_000 -> false
      end

    assert(true == result)
  end

  @tag :group_broadcast_data
  test "broadcadts data to all workers in group" do
    group_id = make_ref()

    num_workers = 5

    {:ok, _} = Sup.add_group(@sup_id, id: group_id, restart_strategy: :one_for_one)

    for index <- 1..num_workers do
      {:ok, _} = Sup.add_group_worker(@sup_id, group_id, {__MODULE__, :loop, [index]}, id: index)
    end

    Process.sleep(100)
    Sup.broadcast_to_group(@sup_id, group_id, {:ping, self()})

    results =
      for _ <- 1..num_workers do
        receive do
          {:pong, _sender} -> true
        after
          1_000 -> false
        end
      end

    assert Enum.all?(results)
  end

  @tag :group_remove_worker
  test "remove worker from group" do
    group_id = :group_test_remove_worker

    {:ok, _} = Sup.add_group(@sup_id, id: group_id, restart_strategy: :one_for_one)
    {:ok, _} = Sup.add_group_worker(@sup_id, group_id, {__MODULE__, :loop, [1]}, id: 1)

    :ok = Sup.send_to_group(@sup_id, group_id, 1, {:ping, self()})

    result =
      receive do
        {:pong, _sender} -> true
      after
        1_000 -> false
      end

    assert(true == result)

    {:ok, _} = Sup.remove_group_worker(@sup_id, group_id, 1)
    result = Sup.send_to_group(@sup_id, group_id, 1, {:ping, self()})

    assert result == {:error, :not_found}
  end

  @tag :group_restart_one_worker
  test "restart one for on  in a group" do
    group_id = :group_restart_one

    {:ok, _} = Sup.add_group(@sup_id, id: group_id, restart_strategy: :one_for_one)
    {:ok, _} = Sup.add_group_worker(@sup_id, group_id, {__MODULE__, :loop, [1]}, id: 1)
    {:ok, _} = Sup.add_group_worker(@sup_id, group_id, {__MODULE__, :loop, [2]}, id: 2)

    Process.sleep(100)
    Sup.send_to_group(@sup_id, group_id, 1, {:ping, self()})

    result =
      receive do
        {:pong, _sender} -> true
        other -> other
      after
        1_000 -> "verify the worker is started"
      end

    assert(true == result)

    Sup.send_to_group(@sup_id, group_id, 2, {:store, :test, :hello})

    Sup.send_to_group(@sup_id, group_id, 2, {:get, :test, self()})

    result =
      receive do
        {:result, :hello} ->
          true

        other ->
          other
      after
        1_000 -> "incorrect result from worker 2"
      end

    assert(true == result)

    Sup.send_to_group(@sup_id, group_id, 1, {:raise, "Restart all workers"})

    Process.sleep(100)
    Sup.send_to_group(@sup_id, group_id, 2, {:get, :test, self()})

    result =
      receive do
        {:result, :hello} ->
          true

        other ->
          other
      after
        1_000 -> "get data failed, timeout"
      end

    assert(true == result)
  end

  @tag :group_restart_all_workers
  test "restart all workers in a group" do
    group_id = :group_restart_all

    {:ok, _} = Sup.add_group(@sup_id, id: group_id, restart_strategy: :one_for_all)
    {:ok, _} = Sup.add_group_worker(@sup_id, group_id, {__MODULE__, :loop, [1]}, id: "w_1")
    {:ok, _} = Sup.add_group_worker(@sup_id, group_id, {__MODULE__, :loop, [2]}, id: "w_2")

    Process.sleep(100)
    Sup.send_to_group(@sup_id, group_id, "w_1", {:ping, self()})

    result =
      receive do
        {:pong, _sender} -> true
      after
        1_000 -> :check_1_failed
      end

    assert(true == result)

    ref = make_ref()
    Sup.send_to_group(@sup_id, group_id, "w_2", {:store, :test, ref})
    Process.sleep(100)
    Sup.send_to_group(@sup_id, group_id, "w_2", {:get, :test, self()})

    result =
      receive do
        {:result, ^ref} -> true
      after
        1_000 -> :check_2_failed
      end

    assert(true == result)

    Sup.send_to_group(@sup_id, group_id, "w_1", {:raise, "Test restart all of group strategy"})
    Process.sleep(100)
    Sup.send_to_group(@sup_id, group_id, "w_2", {:get, :test, self()})

    result =
      receive do
        {:result, nil} -> true
        other -> other
      after
        1_000 -> :verify_after_restart
      end

    assert(true == result)
  end

  @tag :group_restart_all_workers2
  test "restart all workers in a group 2" do
    group_id = :group_restart_all

    {:ok, _} = Sup.add_group(@sup_id, id: {group_id, 1}, restart_strategy: :one_for_all)
    {:ok, _} = Sup.add_group(@sup_id, id: {group_id, 2}, restart_strategy: :one_for_all)
    {:ok, _} = Sup.add_group(@sup_id, id: {group_id, 3}, restart_strategy: :one_for_all)

    {:ok, _} = Sup.add_group_worker(@sup_id, {group_id, 1}, {__MODULE__, :loop, [1]}, id: 1)
    {:ok, _} = Sup.add_group_worker(@sup_id, {group_id, 1}, {__MODULE__, :loop, [2]}, id: 2)

    {:ok, _} = Sup.add_group_worker(@sup_id, {group_id, 2}, {__MODULE__, :loop, [1]}, id: 1)
    {:ok, _} = Sup.add_group_worker(@sup_id, {group_id, 2}, {__MODULE__, :loop, [2]}, id: 2)

    {:ok, _} = Sup.add_group_worker(@sup_id, {group_id, 3}, {__MODULE__, :loop, [1]}, id: 1)
    {:ok, _} = Sup.add_group_worker(@sup_id, {group_id, 3}, {__MODULE__, :loop, [2]}, id: 2)

    fun = fn group_id, parent ->
      Process.sleep(100)
      Sup.send_to_group(@sup_id, group_id, 1, {:ping, self()})

      result =
        receive do
          {:pong, _sender} -> true
        after
          1_000 -> raise "timeout for restarting all workers 2"
        end

      Sup.send_to_group(@sup_id, group_id, 2, {:store, :test, :hello})
      Process.sleep(100)

      Sup.send_to_group(@sup_id, group_id, 2, {:get, :test, self()})

      result =
        receive do
          {:result, :hello} -> true
        after
          1_000 -> raise "timeout for restarting all workers 2"
        end

      Sup.send_to_group(@sup_id, group_id, 1, {:raise, "Restart all workers"})

      Process.sleep(100)
      Sup.send_to_group(@sup_id, group_id, 2, {:get, :test, self()})

      result =
        receive do
          {:result, nil} -> true
        after
          1_000 -> raise "timeout for restarting all workers 2"
        end

      send(parent, :success)
    end

    parent = self()
    spawn(fn -> fun.({group_id, 1}, parent) end)
    spawn(fn -> fun.({group_id, 2}, parent) end)
    spawn(fn -> fun.({group_id, 3}, parent) end)

    for _ <- 1..3 do
      receive do
        :success -> true
      after
        4_000 -> raise "timeout for restarting all workers"
      end
    end
  end

  ## Helper functions

  # Basic loop, receive messages and print them.
  def loop(id) do
    prefix = "[#{inspect(Process.get({:supervisor, :worker_id}))}, #{inspect(self())}]"

    receive do
      {:ping, sender} ->
        IO.puts(prefix <> " Pong to #{inspect(sender)}")
        send(sender, {:pong, self()})

      {:store, key, data} ->
        IO.puts(prefix <> " Store data: #{inspect(data)}")
        Process.put(key, data)

      {:get, key, from} ->
        IO.puts(prefix <> " Get data: #{inspect(Process.get(key))}")
        send(from, {:result, Process.get(key)})

      {:raise, reason} ->
        IO.puts(prefix <> " Raise an error: #{inspect(reason)}")
        raise reason

      msg ->
        IO.puts(prefix <> " task received: #{inspect(msg)}")
    end

    loop(id)
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

  def send_to_chain(sup_id, chain_id, data \\ 10) do
    Sup.send_to_chain(sup_id, chain_id, data)
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
