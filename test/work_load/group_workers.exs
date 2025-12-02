defmodule SuperWorker.Supervisor.GroupWorkloadTest do
  use ExUnit.Case, async: false

  alias SuperWorker.Supervisor, as: Sup
  alias SuperWorker.Supervisor.{Group, Db}

  @sup_id :test_workload_group

  setup_all do
    {:ok, _} = Sup.start_with_config(link: false, id: @sup_id, num_partitions: 20)
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
  test "add amount of workers to group" do
    group_id = make_ref()

    {:ok, _} = Sup.add_group(@sup_id, id: group_id, restart_strategy: :one_for_one)
    assert Sup.group_exists?(@sup_id, group_id)

    range = 1..20_000

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

    assert Sup.group_exists?(@sup_id, group_id)

    Sup.remove_group(@sup_id, group_id, 300_000)

    assert false == Sup.group_exists?(@sup_id, group_id)
  end

  @tag timeout: 300_000
  test "send message to workers in group" do
    group_id = make_ref()

    {:ok, _} = Sup.add_group(@sup_id, id: group_id, restart_strategy: :one_for_one)

    num_workers = 1_000
    range = 1..num_workers

    for index <- range do
      {:ok, _} =
        Sup.add_group_worker(@sup_id, group_id, {MyTest, :loop, [index]}, id: index)
    end

    Process.sleep(100)
    parent = self()

    list_pids =
      Enum.map(range, fn index ->
        spawn(fn -> send_message(parent, group_id, index, 100) end)
      end)

    result = receive_loop(num_workers, [])

    assert length(result) == length(list_pids)
  end

  @tag timeout: 300_000
  test "broadcadts data to all workers in group" do
    group_id = make_ref()

    num_workers = 5_000

    {:ok, _} = Sup.add_group(@sup_id, id: group_id, restart_strategy: :one_for_one)

    for index <- 1..num_workers do
      {:ok, _} = Sup.add_group_worker(@sup_id, group_id, {MyTest, :loop, [index]}, id: index)
    end

    Process.sleep(100)

    for _ <- 1..10 do
      Sup.broadcast_to_group(@sup_id, group_id, {:ping, self()})

      results =
        for _ <- 1..num_workers do
          receive do
            {:pong, _sender} -> true
          after
            5_000 -> false
          end
        end

      assert Enum.all?(results)
    end
  end

  @tag timeout: 300_000
  test "remove workers from group" do
    group_id = make_ref()

    num_workers = 50_000

    {:ok, _} = Sup.add_group(@sup_id, id: group_id, restart_strategy: :one_for_one)

    for index <- 1..num_workers do
      {:ok, _} = Sup.add_group_worker(@sup_id, group_id, {MyTest, :loop, [index]}, id: index)
    end

    for index <- 1..num_workers do
      {:ok, _} = Sup.remove_group_worker(@sup_id, group_id, index)
    end
  end

  @tag timeout: 300_000
  test "restart one for on  in a group" do
    group_id = make_ref()
    num_workers = 10_000

    {:ok, _} = Sup.add_group(@sup_id, id: group_id, restart_strategy: :one_for_all)
    {:ok, _} = Sup.add_group_worker(@sup_id, group_id, {MyTest, :loop, [1]}, id: :crash_worker)

    for index <- 1..num_workers do
      {:ok, _} = Sup.add_group_worker(@sup_id, group_id, {MyTest, :loop, [index]}, id: index)
    end

    Process.sleep(1_000)
    Sup.send_to_group_worker(@sup_id, group_id, :crash_worker, {:ping, self()})

    result =
      receive do
        {:pong, _sender} -> true
        other -> other
      after
        1_000 -> "verify the worker is started"
      end

    assert(true == result)

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

    Sup.send_to_group_worker(@sup_id, group_id, :crash_worker, {:raise, "Restart workers"})

    Process.sleep(1_000)

    wait_worker_restart(group_id, 1)
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

    Sup.remove_group(@sup_id, group_id)
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
    num_workers = 5000

    {:ok, _} = Sup.add_group(@sup_id, id: group_id, restart_strategy: :one_for_one)

    for index <- 1..num_workers do
      {:ok, _} = Sup.add_group_worker(@sup_id, group_id, {MyTest, :loop, [index]}, id: index)
    end

    Process.sleep(1_000)

    for index <- 1..num_workers do
      Sup.send_to_group_worker(@sup_id, group_id, index, {:store, :test, :hello})
      Sup.send_to_group_worker(@sup_id, group_id, index, {:get, :test, self()})

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
    end

    Sup.restart_group(@sup_id, group_id)

    Process.sleep(1_000)

    for index <- 1..num_workers do
      Sup.send_to_group_worker(@sup_id, group_id, index, {:get, :test, self()})

      result =
        receive do
          {:result, nil} ->
            true

          other ->
            other
        after
          1_000 -> "incorrect result from worker 2"
        end

      assert(true == result)
    end
  end

  def send_message(parent, group_id, worker_id, 0) do
    send(parent, {:result, worker_id})
  end

  def send_message(parent, group_id, worker_id, times) do
    Sup.send_to_group_worker(@sup_id, group_id, worker_id, {:store, :test, :hello})
    Sup.send_to_group_worker(@sup_id, group_id, worker_id, {:get, :test, self()})

    result =
      receive do
        {:result, :hello} ->
          send_message(parent, group_id, worker_id, times - 1)
      after
        5_000 ->
          {:error, :timeout}
      end
  end

  defp receive_loop(0, acc) do
    acc
  end

  defp receive_loop(count, acc) do
    receive do
      {:result, worker_id} ->
        receive_loop(count - 1, [worker_id | acc])

      {:error, _} = error ->
        error
    after
      6_000 ->
        receive_loop(count - 1, acc)
    end
  end

  defp wait_worker_restart(group_id, worker_id) do
    wait_worker_restart(group_id, worker_id, 10)
  end

  defp wait_worker_restart(_group_id, _worker_id, 0) do
    false
  end

  defp wait_worker_restart(group_id, worker_id, count) do
    case Sup.get_pid_group_worker(@sup_id, group_id, 2) do
      {:ok, pid} ->
        true

      {:error, _} ->
        Process.sleep(1000)
        wait_worker_restart(group_id, worker_id, count - 1)
    end
  end
end
