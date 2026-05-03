defmodule SuperWorker.Supervisor.GroupTest do
  use ExUnit.Case, async: true

  alias SuperWorker.Supervisor, as: Sup
  alias SuperWorker.Supervisor.{Group, Db}

  doctest Group

  @sup_id :sup_group_test

  setup_all do
    {:ok, _} = Sup.start_with_config(link: false, id: @sup_id, num_partitions: 2)
    :ok
  end

  setup do
    if Sup.running?(@sup_id) do
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
    result = Sup.add_group(@sup_id, id: :group2, restart_strategy: :one_for_one)
    match?({:ok, _}, result)

    result = Sup.add_group(@sup_id, id: :group3, restart_strategy: :one_for_all)
    match?({:ok, _}, result)
  end

  @tag :group_add_workers
  test "add workers to group" do
    group_id = make_ref()

    {:ok, _} = Sup.add_group(@sup_id, id: group_id, restart_strategy: :one_for_one)

    range = 1..5

    for index <- range do
      {:ok, _} =
        Sup.add_group_worker(@sup_id, group_id, {MyTest, :loop, [index]}, id: index)
    end

    list_pid =
      Enum.reduce(range, [], fn index, acc ->
        case Sup.get_pid_group_worker(@sup_id, group_id, index) do
          {:ok, pid} -> [pid | acc]
          _ -> acc
        end
      end)

    assert(length(list_pid) == Enum.count(range))
  end

  @tag :worker_get_pid
  test "get pid from worker in supervisor" do
    group_id = make_ref()
    worker_id = 1

    {:ok, _} = Sup.add_group(@sup_id, id: group_id, restart_strategy: :one_for_one)

    {:ok, _} =
      Sup.add_group_worker(@sup_id, group_id, {MyTest, :loop, [worker_id]}, id: worker_id)

    Process.sleep(100)

    Sup.send_to_group_worker(@sup_id, group_id, worker_id, {:store, :test, :hello})
    Sup.send_to_group_worker(@sup_id, group_id, worker_id, {:get, :test, self()})

    result =
      receive do
        {:result, :hello} ->
          true

        other ->
          other
      after
        1_000 -> "incorrect result from worker"
      end

    assert(true == result)

    Sup.send_to_group_worker(@sup_id, group_id, worker_id, {:get_pid, self()})

    pid =
      receive do
        {:pid, pid} -> pid
      after
        1_000 -> raise "cannot get pid of worker"
      end

    send(pid, {:get, :test, self()})

    result =
      receive do
        {:result, :hello} ->
          true

        other ->
          other
      after
        1_000 -> "incorrect result from worker"
      end

    assert(true == result)
  end

  test "broadcast message to group" do
    group_id = make_ref()
    num_workers = 3

    {:ok, _} = Sup.add_group(@sup_id, id: group_id, restart_strategy: :one_for_one)

    for i <- 1..num_workers do
      {:ok, _} =
        Sup.add_group_worker(@sup_id, group_id, {MyTest, :loop, [i]}, id: i)
    end

    Process.sleep(100)

    Sup.broadcast_to_group(@sup_id, group_id, {:store, :test, :hello})

    for i <- 1..num_workers do
      Sup.send_to_group_worker(@sup_id, group_id, i, {:get, :test, self()})

      result =
        receive do
          {:result, :hello} ->
            true

          other ->
            other
        after
          1_000 -> "incorrect result from worker"
        end

      assert(true == result)
    end

    Sup.remove_group(@sup_id, group_id)
  end

  test "send random to group" do
    group_id = make_ref()
    num_workers = 3

    {:ok, _} = Sup.add_group(@sup_id, id: group_id, restart_strategy: :one_for_one)

    for i <- 1..num_workers do
      {:ok, _} =
        Sup.add_group_worker(@sup_id, group_id, {MyTest, :loop, [i]}, id: i)
    end

    Process.sleep(100)

    Sup.broadcast_to_group(@sup_id, group_id, {:store, :test, :nothing})

    Sup.send_to_group_random(@sup_id, group_id, {:store, :test, :hello})

    results =
      Enum.map(1..num_workers, fn i ->
        Sup.send_to_group_worker(@sup_id, group_id, i, {:get, :test, self()})

        result =
          receive do
            {:result, :hello} ->
              true

            {:result, :nothing} ->
              false
          after
            1_000 -> "incorrect result from worker"
          end
      end)

    assert 1 == Enum.count(results, &(&1 == true))
    assert 2 == Enum.count(results, &(&1 == false))

    Sup.remove_group(@sup_id, group_id)
  end

  test "restart worker in group" do
    group_id = make_ref()
    num_workers = 2

    {:ok, _} = Sup.add_group(@sup_id, id: group_id, restart_strategy: :one_for_one)

    for i <- 1..num_workers do
      {:ok, _} =
        Sup.add_group_worker(@sup_id, group_id, {MyTest, :loop, [i]}, id: i)
    end

    Process.sleep(100)

    Sup.broadcast_to_group(@sup_id, group_id, {:store, :test, :hello})

    Sup.restart_group_worker(@sup_id, group_id, 1)

    Sup.send_to_group_worker(@sup_id, group_id, 1, {:get, :test, self()})

    result =
      receive do
        {:result, nil} ->
          true

        other ->
          other
      after
        1_000 -> "incorrect result from worker"
      end

    assert(true == result)

    Sup.send_to_group_worker(@sup_id, group_id, 2, {:get, :test, self()})

    result =
      receive do
        {:result, :hello} ->
          true

        other ->
          other
      after
        1_000 -> "incorrect result from worker"
      end

    assert(true == result)

    Sup.remove_group(@sup_id, group_id)
  end

  test "restart all workers in group" do
    group_id = make_ref()
    num_workers = 5

    {:ok, _} = Sup.add_group(@sup_id, id: group_id, restart_strategy: :one_for_one)

    for i <- 1..num_workers do
      {:ok, _} =
        Sup.add_group_worker(@sup_id, group_id, {MyTest, :loop, [i]}, id: i)
    end

    Process.sleep(100)

    Sup.broadcast_to_group(@sup_id, group_id, {:store, :test, :hello})

    Sup.restart_group(@sup_id, group_id)

    Process.sleep(2000)

    for i <- 1..num_workers do
      Sup.send_to_group_worker(@sup_id, group_id, i, {:get, :test, self()})

      result =
        receive do
          {:result, nil} ->
            true

          other ->
            other
        after
          1000 -> "incorrect result from worker"
        end

      assert(true == result)
    end

    Sup.remove_group(@sup_id, group_id)
  end

  @tag :group_add_mixed_workers
  test "add mixed workers to group" do
    group_id = make_ref()
    num_workers = 5
    {:ok, _} = Sup.add_group(@sup_id, id: group_id, restart_strategy: :one_for_one)

    list =
      for index <- 1..num_workers do
        {:ok, _} =
          Sup.add_group_worker(@sup_id, group_id, {MyTest, :loop, [index]}, id: index)
      end

    Sup.add_group_worker(@sup_id, group_id, MyGenServer, [])
    Process.sleep(10)

    counter =
      Enum.reduce(1..num_workers, [], fn index, acc ->
        case Sup.get_pid_group_worker(@sup_id, group_id, index) do
          {:ok, pid} -> [pid | acc]
          _ -> acc
        end
      end)
      |> Enum.count()

    counter =
      case Sup.get_pid_group_worker(@sup_id, group_id, MyGenServer) do
        {:ok, _} -> counter + 1
        _ -> counter
      end

    assert(num_workers + 1 == counter)
  end

  @tag :group_add_workers_multi_groups
  test "add workers to groups" do
    num_workers = 10
    num_groups = 5

    Enum.each(1..num_groups, fn index ->
      group_id = {make_ref(), index}

      {:ok, _} = Sup.add_group(@sup_id, id: group_id, restart_strategy: :one_for_one)

      list =
        for worker_index <- 1..num_workers do
          {:ok, _} =
            Sup.add_group_worker(@sup_id, group_id, {MyTest, :loop, [worker_index]},
              id: {index, worker_index}
            )
        end

      Process.sleep(10)

      counter =
        Enum.reduce(1..num_workers, 0, fn worker_index, acc ->
          case Sup.get_pid_group_worker(@sup_id, group_id, {index, worker_index}) do
            {:ok, pid} -> acc + 1
            _ -> acc
          end
        end)

      assert(counter == num_workers)
    end)
  end

  @tag :group_send_data
  test "send data to worker in group" do
    group_id = make_ref()

    {:ok, _} = Sup.add_group(@sup_id, id: group_id, restart_strategy: :one_for_one)
    {:ok, _} = Sup.add_group_worker(@sup_id, group_id, {MyTest, :loop, [1]}, id: 1)

    Process.sleep(100)
    Sup.send_to_group_worker(@sup_id, group_id, 1, {:ping, self()})

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
      {:ok, _} = Sup.add_group_worker(@sup_id, group_id, {MyTest, :loop, [index]}, id: index)
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
    group_id = make_ref()
    worker_id = 1

    {:ok, _} = Sup.add_group(@sup_id, id: group_id, restart_strategy: :one_for_one)

    {:ok, _} =
      Sup.add_group_worker(@sup_id, group_id, {MyTest, :loop, [worker_id]}, id: worker_id)

    :ok = Sup.send_to_group_worker(@sup_id, group_id, worker_id, {:ping, self()})

    result =
      receive do
        {:pong, _sender} -> true
      after
        1_000 -> false
      end

    assert result

    {:ok, _} = Sup.remove_group_worker(@sup_id, group_id, worker_id)
    result = Sup.send_to_group_worker(@sup_id, group_id, worker_id, {:ping, self()})

    assert match?(result, {:error, :not_found})
  end

  @tag :remove_group
  test "remove group" do
    group_id = make_ref()
    worker_id = 1

    {:ok, _} = Sup.add_group(@sup_id, id: group_id, restart_strategy: :one_for_one)

    {:ok, _} =
      Sup.add_group_worker(@sup_id, group_id, {MyTest, :loop, [worker_id]}, id: worker_id)

    :ok = Sup.send_to_group_worker(@sup_id, group_id, worker_id, {:ping, self()})

    result =
      receive do
        {:pong, _sender} -> true
      after
        1_000 -> false
      end

    assert result

    {:ok, _} = Sup.remove_group(@sup_id, group_id)

    result = Sup.get_pid_group_worker(@sup_id, group_id, worker_id)

    assert match?({:error, _}, result)
  end

  @tag :group_restart_one_worker
  test "restart one for on  in a group" do
    group_id = make_ref()

    {:ok, _} = Sup.add_group(@sup_id, id: group_id, restart_strategy: :one_for_one)
    {:ok, _} = Sup.add_group_worker(@sup_id, group_id, {MyTest, :loop, [1]}, id: 1)
    {:ok, _} = Sup.add_group_worker(@sup_id, group_id, {MyTest, :loop, [2]}, id: 2)

    Process.sleep(100)
    Sup.send_to_group_worker(@sup_id, group_id, 1, {:ping, self()})

    result =
      receive do
        {:pong, _sender} -> true
        other -> other
      after
        1_000 -> "verify the worker is started"
      end

    assert(true == result)

    Sup.send_to_group_worker(@sup_id, group_id, 2, {:store, :test, :hello})

    Sup.send_to_group_worker(@sup_id, group_id, 2, {:get, :test, self()})

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

    Sup.send_to_group_worker(@sup_id, group_id, 1, {:raise, "Restart all workers"})

    Process.sleep(100)
    Sup.send_to_group_worker(@sup_id, group_id, 2, {:get, :test, self()})

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
    group_id = make_ref()

    {:ok, _} = Sup.add_group(@sup_id, id: group_id, restart_strategy: :one_for_all)
    {:ok, _} = Sup.add_group_worker(@sup_id, group_id, {MyTest, :loop, [1]}, id: "w_1")
    {:ok, _} = Sup.add_group_worker(@sup_id, group_id, {MyTest, :loop, [2]}, id: "w_2")

    Process.sleep(100)
    Sup.send_to_group_worker(@sup_id, group_id, "w_1", {:ping, self()})

    result =
      receive do
        {:pong, _sender} -> true
      after
        1_000 -> :check_1_failed
      end

    assert(true == result)

    ref = make_ref()
    Sup.send_to_group_worker(@sup_id, group_id, "w_2", {:store, :test, ref})
    Process.sleep(100)
    Sup.send_to_group_worker(@sup_id, group_id, "w_2", {:get, :test, self()})

    result =
      receive do
        {:result, ^ref} -> true
      after
        1_000 -> :check_2_failed
      end

    assert(true == result)

    Sup.send_to_group_worker(
      @sup_id,
      group_id,
      "w_1",
      {:raise, "Test restart all of group strategy"}
    )

    Process.sleep(100)
    Sup.send_to_group_worker(@sup_id, group_id, "w_2", {:get, :test, self()})

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
    group_id = make_ref()

    {:ok, _} = Sup.add_group(@sup_id, id: {group_id, 1}, restart_strategy: :one_for_all)
    {:ok, _} = Sup.add_group(@sup_id, id: {group_id, 2}, restart_strategy: :one_for_all)
    {:ok, _} = Sup.add_group(@sup_id, id: {group_id, 3}, restart_strategy: :one_for_all)

    {:ok, _} = Sup.add_group_worker(@sup_id, {group_id, 1}, {MyTest, :loop, [1]}, id: 1)
    {:ok, _} = Sup.add_group_worker(@sup_id, {group_id, 1}, {MyTest, :loop, [2]}, id: 2)

    {:ok, _} = Sup.add_group_worker(@sup_id, {group_id, 2}, {MyTest, :loop, [1]}, id: 1)
    {:ok, _} = Sup.add_group_worker(@sup_id, {group_id, 2}, {MyTest, :loop, [2]}, id: 2)

    {:ok, _} = Sup.add_group_worker(@sup_id, {group_id, 3}, {MyTest, :loop, [1]}, id: 1)
    {:ok, _} = Sup.add_group_worker(@sup_id, {group_id, 3}, {MyTest, :loop, [2]}, id: 2)

    fun = fn group_id, parent ->
      Process.sleep(100)
      Sup.send_to_group_worker(@sup_id, group_id, 1, {:ping, self()})

      result =
        receive do
          {:pong, _sender} -> true
        after
          1_000 -> raise "timeout for restarting all workers 2"
        end

      Sup.send_to_group_worker(@sup_id, group_id, 2, {:store, :test, :hello})
      Process.sleep(100)

      Sup.send_to_group_worker(@sup_id, group_id, 2, {:get, :test, self()})

      result =
        receive do
          {:result, :hello} -> true
        after
          1_000 -> raise "timeout for restarting all workers 2"
        end

      Sup.send_to_group_worker(@sup_id, group_id, 1, {:raise, "Restart all workers"})

      Process.sleep(100)
      Sup.send_to_group_worker(@sup_id, group_id, 2, {:get, :test, self()})

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

  @tag :group_restart_worker_by_api
  test "restart group worker by api" do
    group_id = make_ref()

    {:ok, _} = Sup.add_group(@sup_id, id: group_id, restart_strategy: :one_for_one)
    {:ok, _} = Sup.add_group_worker(@sup_id, group_id, {MyTest, :loop, [2]}, id: 2)

    Sup.send_to_group_worker(@sup_id, group_id, 2, {:store, :test, :hello})
    Sup.send_to_group_worker(@sup_id, group_id, 2, {:get, :test, self()})

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

    Sup.restart_group_worker(@sup_id, group_id, 2)

    Process.sleep(100)
    Sup.send_to_group_worker(@sup_id, group_id, 2, {:get, :test, self()})

    result =
      receive do
        {:result, nil} ->
          true

        other ->
          other
      after
        1_000 -> "get data failed, timeout"
      end

    assert(true == result)
  end

  @tag :group_restart_all_workers_by_api
  test "restart all group worker by api" do
    group_id = make_ref()

    {:ok, _} = Sup.add_group(@sup_id, id: group_id, restart_strategy: :one_for_one)
    {:ok, _} = Sup.add_group_worker(@sup_id, group_id, {MyTest, :loop, [1]}, id: 1)
    {:ok, _} = Sup.add_group_worker(@sup_id, group_id, {MyTest, :loop, [2]}, id: 2)

    Sup.send_to_group_worker(@sup_id, group_id, 1, {:store, :test, :hello})
    Sup.send_to_group_worker(@sup_id, group_id, 1, {:get, :test, self()})

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

    Sup.send_to_group_worker(@sup_id, group_id, 2, {:store, :test, :hello})
    Sup.send_to_group_worker(@sup_id, group_id, 2, {:get, :test, self()})

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

    Sup.restart_group(@sup_id, group_id)

    Process.sleep(100)
    Sup.send_to_group_worker(@sup_id, group_id, 1, {:get, :test, self()})

    result =
      receive do
        {:result, nil} ->
          true

        other ->
          other
      after
        1_000 -> "get data failed, timeout"
      end

    assert(true == result)

    Sup.send_to_group_worker(@sup_id, group_id, 2, {:get, :test, self()})

    result =
      receive do
        {:result, nil} ->
          true

        other ->
          other
      after
        1_000 -> "get data failed, timeout"
      end

    assert(true == result)
  end
end
