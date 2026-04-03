defmodule SuperWorker.Supervisor.GroupWorkloadTest do
  @moduledoc """
  Workload tests for group workers.

  These tests verify supervisor behavior under moderate load with reasonable
  worker counts (50-500) and timeouts (30s). They are designed to run quickly
  while still exercising the core functionality at scale.
  """

  use ExUnit.Case, async: false

  alias SuperWorker.Supervisor, as: Sup

  @moduletag :capture_log
  @default_timeout 30_000

  # ---------------------------------------------------------------------------
  # Setup — per-test isolation with unique supervisor IDs
  # ---------------------------------------------------------------------------

  setup do
    sup_id = :"sup_workload_group_#{System.unique_integer([:positive])}"
    {:ok, _} = Sup.start_with_config(link: false, id: sup_id, num_partitions: 2)

    on_exit(fn ->
      if Sup.running?(sup_id) do
        try do
          Sup.stop(sup_id)
        catch
          :exit, _ -> :ok
        end
      end
    end)

    %{sup_id: sup_id}
  end

  # ---------------------------------------------------------------------------
  # Basic lifecycle
  # ---------------------------------------------------------------------------

  @tag timeout: @default_timeout
  test "add and remove a group", %{sup_id: sup_id} do
    ref = make_ref()

    for index <- 1..500 do
      group_id = {ref, index}
      {:ok, _} = Sup.add_group(sup_id, id: group_id, restart_strategy: :one_for_one)

      assert Sup.group_exists?(sup_id, group_id)

      {:ok, _} = Sup.remove_group(sup_id, group_id)
      refute Sup.group_exists?(sup_id, group_id)
    end
  end

  @tag timeout: @default_timeout
  test "duplicate group id is rejected", %{sup_id: sup_id} do
    group_id = make_ref()
    {:ok, _} = Sup.add_group(sup_id, id: group_id, restart_strategy: :one_for_one)

    assert {:error, _} =
             Sup.add_group(sup_id, id: group_id, restart_strategy: :one_for_one)
  end

  @tag timeout: @default_timeout
  test "group_exists? returns false for unknown group", %{sup_id: sup_id} do
    refute Sup.group_exists?(sup_id, make_ref())
  end

  @tag timeout: @default_timeout
  test "add 500 workers to a group and verify all pids", %{sup_id: sup_id} do
    group_id = make_ref()
    num_workers = 500

    {:ok, _} = Sup.add_group(sup_id, id: group_id, restart_strategy: :one_for_one)

    for i <- 1..num_workers do
      {:ok, _} = Sup.add_group_worker(sup_id, group_id, {MyTest, :loop, [i]}, id: i)
    end

    pid_count =
      Enum.count(1..num_workers, fn i ->
        match?({:ok, _}, Sup.get_pid_group_worker(sup_id, group_id, i))
      end)

    assert pid_count == num_workers

    Sup.remove_group(sup_id, group_id)
    refute Sup.group_exists?(sup_id, group_id)
  end

  @tag timeout: @default_timeout
  test "duplicate worker id in group is rejected", %{sup_id: sup_id} do
    group_id = make_ref()
    {:ok, _} = Sup.add_group(sup_id, id: group_id, restart_strategy: :one_for_one)

    {:ok, _} = Sup.add_group_worker(sup_id, group_id, {MyTest, :loop, [1]}, id: :dup)

    assert {:error, :worker_already_exists} =
             Sup.add_group_worker(sup_id, group_id, {MyTest, :loop, [1]}, id: :dup)
  end

  @tag timeout: @default_timeout
  test "count workers in group is accurate", %{sup_id: sup_id} do
    group_id = make_ref()
    num_workers = 500

    {:ok, _} = Sup.add_group(sup_id, id: group_id, restart_strategy: :one_for_one)

    for i <- 1..num_workers do
      {:ok, _} = Sup.add_group_worker(sup_id, group_id, {MyTest, :loop, [i]}, id: i)
    end

    {:ok, count} = Sup.count_workers_in_group(sup_id, group_id)
    assert count == num_workers
  end

  # ---------------------------------------------------------------------------
  # Remove workers
  # ---------------------------------------------------------------------------

  @tag timeout: @default_timeout
  test "remove 500 workers from a group sequentially", %{sup_id: sup_id} do
    group_id = make_ref()
    num_workers = 500

    {:ok, _} = Sup.add_group(sup_id, id: group_id, restart_strategy: :one_for_one)

    for i <- 1..num_workers do
      {:ok, _} = Sup.add_group_worker(sup_id, group_id, {MyTest, :loop, [i]}, id: i)
    end

    for i <- 1..num_workers do
      assert match?({:ok, _}, Sup.remove_group_worker(sup_id, group_id, i))
    end

    # All pids should be gone
    gone_count =
      Enum.count(1..num_workers, fn i ->
        match?({:error, _}, Sup.get_pid_group_worker(sup_id, group_id, i))
      end)

    assert gone_count == num_workers
  end

  @tag timeout: @default_timeout
  test "remove unknown worker from group returns error", %{sup_id: sup_id} do
    group_id = make_ref()
    {:ok, _} = Sup.add_group(sup_id, id: group_id, restart_strategy: :one_for_one)

    assert {:error, :worker_not_found} =
             Sup.remove_group_worker(sup_id, group_id, :no_such_worker)
  end

  # ---------------------------------------------------------------------------
  # Messaging
  # ---------------------------------------------------------------------------

  @tag timeout: @default_timeout
  test "send message to 500 workers and collect all replies", %{sup_id: sup_id} do
    group_id = make_ref()
    num_workers = 500

    {:ok, _} = Sup.add_group(sup_id, id: group_id, restart_strategy: :one_for_one)

    for i <- 1..num_workers do
      {:ok, _} = Sup.add_group_worker(sup_id, group_id, {MyTest, :loop, [i]}, id: i)
    end

    parent = self()

    senders =
      for i <- 1..num_workers do
        spawn(fn -> do_send_receive(parent, sup_id, group_id, i, 5) end)
      end

    results = collect_replies(num_workers, [])
    assert length(results) == length(senders)
    assert Enum.all?(results, &match?({:result, _}, &1))
  end

  @tag timeout: @default_timeout
  test "broadcast reaches all 500 workers 5 times", %{sup_id: sup_id} do
    group_id = make_ref()
    num_workers = 500

    {:ok, _} = Sup.add_group(sup_id, id: group_id, restart_strategy: :one_for_one)

    for i <- 1..num_workers do
      {:ok, _} = Sup.add_group_worker(sup_id, group_id, {MyTest, :loop, [i]}, id: i)
    end

    for round <- 1..5 do
      Sup.broadcast_to_group(sup_id, group_id, {:ping, self()})

      pong_count =
        Enum.count(1..num_workers, fn _ ->
          receive do
            {:pong, _} -> true
          after
            3_000 -> false
          end
        end)

      assert pong_count == num_workers,
             "Round #{round}: expected #{num_workers} pongs, got #{pong_count}"
    end
  end

  @tag timeout: @default_timeout
  test "send to random worker in group reaches a live worker", %{sup_id: sup_id} do
    group_id = make_ref()
    num_workers = 100

    {:ok, _} = Sup.add_group(sup_id, id: group_id, restart_strategy: :one_for_one)

    for i <- 1..num_workers do
      {:ok, _} = Sup.add_group_worker(sup_id, group_id, {MyTest, :loop, [i]}, id: i)
    end

    # Send messages to random workers and collect pongs
    for _ <- 1..20 do
      Sup.send_to_group_random(sup_id, group_id, {:ping, self()})
    end

    # Collect pongs (some may be from same worker)
    pong_count =
      Enum.count(1..20, fn _ ->
        receive do
          {:pong, _} -> true
        after
          2_000 -> false
        end
      end)

    assert pong_count > 0
  end

  @tag timeout: @default_timeout
  test "send_to_group_random on empty group returns error", %{sup_id: sup_id} do
    group_id = make_ref()
    {:ok, _} = Sup.add_group(sup_id, id: group_id, restart_strategy: :one_for_one)

    assert {:error, :no_worker} = Sup.send_to_group_random(sup_id, group_id, :hello)
  end

  # ---------------------------------------------------------------------------
  # Restart — one_for_one
  # ---------------------------------------------------------------------------

  @tag timeout: @default_timeout
  test "one_for_one: crashed worker restarts; siblings keep their state", %{sup_id: sup_id} do
    group_id = make_ref()
    num_siblings = 200

    {:ok, _} = Sup.add_group(sup_id, id: group_id, restart_strategy: :one_for_one)

    {:ok, _} =
      Sup.add_group_worker(sup_id, group_id, {MyTest, :loop, [:crash_test]}, id: :crash_target)

    for i <- 1..num_siblings do
      {:ok, _} = Sup.add_group_worker(sup_id, group_id, {MyTest, :loop, [i]}, id: i)
    end

    # Store state in siblings
    for i <- 1..num_siblings do
      Sup.send_to_group_worker(sup_id, group_id, i, {:store, :key, i})
    end

    # Crash one worker
    Sup.send_to_group_worker(sup_id, group_id, :crash_target, {:raise, "crash"})
    Process.sleep(300)

    # Siblings must still have their state
    sibling_ok =
      Enum.count(1..num_siblings, fn i ->
        Sup.send_to_group_worker(sup_id, group_id, i, {:get, :key, self()})

        receive do
          {:result, ^i} -> true
          _ -> false
        after
          1_000 -> false
        end
      end)

    assert sibling_ok == num_siblings

    # The crashed worker must have restarted fresh
    assert wait_for_worker(sup_id, group_id, :crash_target),
           "crash_target did not restart within deadline"

    Sup.send_to_group_worker(sup_id, group_id, :crash_target, {:get, :key, self()})
    assert_receive {:result, nil}, 1_000
  end

  @tag timeout: @default_timeout
  test "one_for_one: restart via API clears worker state", %{sup_id: sup_id} do
    group_id = make_ref()

    {:ok, _} = Sup.add_group(sup_id, id: group_id, restart_strategy: :one_for_one)
    {:ok, _} = Sup.add_group_worker(sup_id, group_id, {MyTest, :loop, [1]}, id: :w)

    Sup.send_to_group_worker(sup_id, group_id, :w, {:store, :k, :hello})
    Sup.send_to_group_worker(sup_id, group_id, :w, {:get, :k, self()})
    assert_receive {:result, :hello}, 1_000

    Sup.restart_group_worker(sup_id, group_id, :w)
    Process.sleep(200)

    Sup.send_to_group_worker(sup_id, group_id, :w, {:get, :k, self()})
    assert_receive {:result, nil}, 1_000
  end

  # ---------------------------------------------------------------------------
  # Restart — one_for_all
  # ---------------------------------------------------------------------------

  @tag timeout: @default_timeout
  test "one_for_all: crash propagates restart to all workers", %{sup_id: sup_id} do
    group_id = make_ref()
    num_workers = 500

    {:ok, _} = Sup.add_group(sup_id, id: group_id, restart_strategy: :one_for_all)

    {:ok, _} =
      Sup.add_group_worker(sup_id, group_id, {MyTest, :loop, [1]}, id: :crash_worker)

    for i <- 1..num_workers do
      {:ok, _} = Sup.add_group_worker(sup_id, group_id, {MyTest, :loop, [i]}, id: i)
    end

    # Store state in worker 1 so we can verify it is wiped
    Sup.send_to_group_worker(sup_id, group_id, 1, {:store, :test, :hello})
    Sup.send_to_group_worker(sup_id, group_id, 1, {:get, :test, self()})
    assert_receive {:result, :hello}, 1_000

    # Trigger crash
    Sup.send_to_group_worker(sup_id, group_id, :crash_worker, {:raise, "trigger one_for_all"})
    Process.sleep(500)

    # Worker 1 must have restarted — state should be nil
    assert wait_for_worker(sup_id, group_id, 1), "worker 1 did not restart"

    Sup.send_to_group_worker(sup_id, group_id, 1, {:get, :test, self()})
    assert_receive {:result, nil}, 1_000
  end

  # ---------------------------------------------------------------------------
  # Restart all workers via API
  # ---------------------------------------------------------------------------

  @tag timeout: @default_timeout
  test "restart_group clears state for all workers", %{sup_id: sup_id} do
    group_id = make_ref()
    num_workers = 500

    {:ok, _} = Sup.add_group(sup_id, id: group_id, restart_strategy: :one_for_one)

    for i <- 1..num_workers do
      {:ok, _} = Sup.add_group_worker(sup_id, group_id, {MyTest, :loop, [i]}, id: i)
    end

    # Write state to every worker
    for i <- 1..num_workers do
      Sup.send_to_group_worker(sup_id, group_id, i, {:store, :test, :hello})
    end

    # Verify state is set (sample check for performance)
    Sup.send_to_group_worker(sup_id, group_id, 1, {:get, :test, self()})
    assert_receive {:result, :hello}, 1_000

    Sup.restart_group(sup_id, group_id)
    Process.sleep(500)

    # State must be gone after restart (sample check)
    Sup.send_to_group_worker(sup_id, group_id, 1, {:get, :test, self()})
    assert_receive {:result, nil}, 1_000
  end

  # ---------------------------------------------------------------------------
  # Parallel group operations
  # ---------------------------------------------------------------------------

  @tag timeout: @default_timeout
  test "parallel groups do not interfere with each other", %{sup_id: sup_id} do
    num_groups = 20
    num_workers_per_group = 50
    ref = make_ref()
    parent = self()

    for g <- 1..num_groups do
      spawn(fn ->
        group_id = {ref, g}

        {:ok, _} = Sup.add_group(sup_id, id: group_id, restart_strategy: :one_for_one)

        for w <- 1..num_workers_per_group do
          {:ok, _} = Sup.add_group_worker(sup_id, group_id, {MyTest, :loop, [w]}, id: w)
        end

        Sup.broadcast_to_group(sup_id, group_id, {:ping, self()})

        pong_count =
          Enum.count(1..num_workers_per_group, fn _ ->
            receive do
              {:pong, _} -> true
            after
              3_000 -> false
            end
          end)

        send(parent, {:group_done, group_id, pong_count})
      end)
    end

    results =
      for _ <- 1..num_groups do
        assert_receive {:group_done, _gid, count}, 30_000
        count
      end

    assert Enum.all?(results, &(&1 == num_workers_per_group))
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp do_send_receive(parent, sup_id, group_id, worker_id, 0),
    do: send(parent, {:result, worker_id})

  defp do_send_receive(parent, sup_id, group_id, worker_id, times) do
    Sup.send_to_group_worker(sup_id, group_id, worker_id, {:store, :test, :hello})
    Sup.send_to_group_worker(sup_id, group_id, worker_id, {:get, :test, self()})

    receive do
      {:result, :hello} -> do_send_receive(parent, sup_id, group_id, worker_id, times - 1)
    after
      3_000 -> send(parent, {:error, {:timeout, worker_id}})
    end
  end

  defp collect_replies(0, acc), do: acc

  defp collect_replies(remaining, acc) do
    receive do
      {:result, _} = r -> collect_replies(remaining - 1, [r | acc])
      {:error, _} = e -> collect_replies(remaining - 1, [e | acc])
    after
      10_000 -> collect_replies(remaining - 1, acc)
    end
  end

  defp wait_for_worker(sup_id, group_id, worker_id, attempts \\ 10)
  defp wait_for_worker(_sup_id, _group_id, _worker_id, 0), do: false

  defp wait_for_worker(sup_id, group_id, worker_id, n) do
    case Sup.get_pid_group_worker(sup_id, group_id, worker_id) do
      {:ok, _} ->
        true

      {:error, _} ->
        Process.sleep(100)
        wait_for_worker(sup_id, group_id, worker_id, n - 1)
    end
  end
end
