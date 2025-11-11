defmodule SuperWorker.Supervisor.StandaloneTest do
  use ExUnit.Case, async: true

  alias SuperWorker.Supervisor, as: Sup
  alias SuperWorker.Supervisor.{Worker}

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

  @tag :add_workers
  test "add workers to group" do
    list =
      for index <- 1..5 do
        {:ok, _} =
          Sup.add_standalone_worker(@sup_id, {__MODULE__, :loop, [index]}, id: index)
      end

    {:ok, workers} = Sup.get_all_standalone_workers(@sup_id)

    assert(length(list) == length(workers))
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

    Sup.send_to_group(@sup_id, group_id, "w_2", {:store, :test, :hello})
    Process.sleep(100)
    Sup.send_to_group(@sup_id, group_id, "w_2", {:get, :test, self()})

    result =
      receive do
        {:result, :hello} -> true
      after
        1_000 -> :check_2_failed
      end

    assert(true == result)

    Sup.send_to_group(@sup_id, group_id, "w_1", {:raise, "Test restart all strategy"})
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
