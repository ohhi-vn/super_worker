defmodule SuperWorker.Supervisor.GroupWorkloadTest do
  use ExUnit.Case, async: false

  alias SuperWorker.Supervisor, as: Sup

  @sup_id :test_workload_group
  @default_timeout 300_000

  # ---------------------------------------------------------------------------
  # Setup
  # ---------------------------------------------------------------------------

  setup_all do
    {:ok, _} = Sup.start_with_config(link: false, id: @sup_id, num_partitions: 20)
    :ok
  end

  setup do
    unless Sup.running?(@sup_id), do: raise("Supervisor #{@sup_id} is not running")
    :ok
  end

  # ---------------------------------------------------------------------------
  # Basic lifecycle
  # ---------------------------------------------------------------------------

  @tag timeout: @default_timeout
  test "add and remove a group" do
    ref = make_ref()

    for index <- 1..10_000 do
      group_id = {ref, index}
      {:ok, _} = Sup.add_group(@sup_id, id: group_id, restart_strategy: :one_for_one)

      assert Sup.group_exists?(@sup_id, group_id)

      {:ok, _} = Sup.remove_group(@sup_id, group_id)
      refute Sup.group_exists?(@sup_id, group_id)
    end
  end

  @tag timeout: @default_timeout
  test "duplicate group id is rejected" do
    group_id = make_ref()
    {:ok, _} = Sup.add_group(@sup_id, id: group_id, restart_strategy: :one_for_one)

    assert {:error, _} =
             Sup.add_group(@sup_id, id: group_id, restart_strategy: :one_for_one)
  end

  @tag timeout: @default_timeout
  test "group_exists? returns false for unknown group" do
    refute Sup.group_exists?(@sup_id, make_ref())
  end

  @tag timeout: @default_timeout
  test "add 20_000 workers to a group and verify all pids" do
    group_id = make_ref()
    num_workers = 20_000

    {:ok, _} = Sup.add_group(@sup_id, id: group_id, restart_strategy: :one_for_one)

    for i <- 1..num_workers do
      {:ok, _} = Sup.add_group_worker(@sup_id, group_id, {MyTest, :loop, [i]}, id: i)
    end

    pid_count =
      Enum.count(1..num_workers, fn i ->
        match?({:ok, _}, Sup.get_pid_group_worker(@sup_id, group_id, i))
      end)

    assert pid_count == num_workers

    Sup.remove_group(@sup_id, group_id, @default_timeout)
    refute Sup.group_exists?(@sup_id, group_id)
  end

  @tag timeout: @default_timeout
  test "duplicate worker id in group is rejected" do
    group_id = make_ref()
    {:ok, _} = Sup.add_group(@sup_id, id: group_id, restart_strategy: :one_for_one)

    {:ok, _} = Sup.add_group_worker(@sup_id, group_id, {MyTest, :loop, [1]}, id: :dup)

    assert {:error, :worker_already_exists} =
             Sup.add_group_worker(@sup_id, group_id, {MyTest, :loop, [1]}, id: :dup)
  end

  @tag timeout: @default_timeout
  test "count workers in group is accurate" do
    group_id = make_ref()
    num_workers = 500

    {:ok, _} = Sup.add_group(@sup_id, id: group_id, restart_strategy: :one_for_one)

    for i <- 1..num_workers do
      {:ok, _} = Sup.add_group_worker(@sup_id, group_id, {MyTest, :loop, [i]}, id: i)
    end

    {:ok, count} = Sup.count_workers_in_group(@sup_id, group_id)
    assert count == num_workers
  end

  # ---------------------------------------------------------------------------
  # Remove workers
  # ---------------------------------------------------------------------------

  @tag timeout: @default_timeout
  test "remove 50_000 workers from a group sequentially" do
    group_id = make_ref()
    num_workers = 50_000

    {:ok, _} = Sup.add_group(@sup_id, id: group_id, restart_strategy: :one_for_one)

    for i <- 1..num_workers do
      {:ok, _} = Sup.add_group_worker(@sup_id, group_id, {MyTest, :loop, [i]}, id: i)
    end

    for i <- 1..num_workers do
      assert match?({:ok, _}, Sup.remove_group_worker(@sup_id, group_id, i))
    end

    # All pids should be gone
    gone_count =
      Enum.count(1..num_workers, fn i ->
        match?({:error, _}, Sup.get_pid_group_worker(@sup_id, group_id, i))
      end)

    assert gone_count == num_workers
  end

  @tag timeout: @default_timeout
  test "remove unknown worker from group returns error" do
    group_id = make_ref()
    {:ok, _} = Sup.add_group(@sup_id, id: group_id, restart_strategy: :one_for_one)

    assert {:error, :worker_not_found} =
             Sup.remove_group_worker(@sup_id, group_id, :no_such_worker)
  end

  # ---------------------------------------------------------------------------
  # Messaging
  # ---------------------------------------------------------------------------

  @tag timeout: @default_timeout
  test "send message to 1_000 workers and collect all replies" do
    group_id = make_ref()
    num_workers = 1_000

    {:ok, _} = Sup.add_group(@sup_id, id: group_id, restart_strategy: :one_for_one)

    for i <- 1..num_workers do
      {:ok, _} = Sup.add_group_worker(@sup_id, group_id, {MyTest, :loop, [i]}, id: i)
    end

    Process.sleep(300)
    parent = self()

    senders =
      for i <- 1..num_workers do
        spawn(fn -> do_send_receive(parent, group_id, i, 100) end)
      end

    results = collect_replies(num_workers, [])
    assert length(results) == length(senders)
  end

  @tag timeout: @default_timeout
  test "broadcast reaches all 5_000 workers 10 times" do
    group_id = make_ref()
    num_workers = 5_000

    {:ok, _} = Sup.add_group(@sup_id, id: group_id, restart_strategy: :one_for_one)

    for i <- 1..num_workers do
      {:ok, _} = Sup.add_group_worker(@sup_id, group_id, {MyTest, :loop, [i]}, id: i)
    end

    Process.sleep(200)

    for round <- 1..10 do
      Sup.broadcast_to_group(@sup_id, group_id, {:ping, self()})

      pong_count =
        Enum.count(1..num_workers, fn _ ->
          receive do
            {:pong, _} -> true
          after
            5_000 -> false
          end
        end)

      assert pong_count == num_workers,
             "Round #{round}: expected #{num_workers} pongs, got #{pong_count}"
    end
  end

  @tag timeout: @default_timeout
  test "send to random worker in group reaches a live worker" do
    group_id = make_ref()
    num_workers = 100

    {:ok, _} = Sup.add_group(@sup_id, id: group_id, restart_strategy: :one_for_one)

    for i <- 1..num_workers do
      {:ok, _} = Sup.add_group_worker(@sup_id, group_id, {MyTest, :loop, [i]}, id: i)
    end

    Process.sleep(200)

    for _ <- 1..50 do
      :ok = Sup.send_to_group_random(@sup_id, group_id, {:ping, self()})

      assert_receive {:pong, _}, 3_000
    end
  end

  @tag timeout: @default_timeout
  test "send_to_group_random on empty group returns error" do
    group_id = make_ref()
    {:ok, _} = Sup.add_group(@sup_id, id: group_id, restart_strategy: :one_for_one)

    assert {:error, :no_worker} = Sup.send_to_group_random(@sup_id, group_id, :hello)
  end

  # ---------------------------------------------------------------------------
  # Restart — one_for_one
  # ---------------------------------------------------------------------------

  @tag timeout: @default_timeout
  test "one_for_one: crashed worker restarts; siblings keep their state" do
    group_id = make_ref()
    num_siblings = 200

    {:ok, _} = Sup.add_group(@sup_id, id: group_id, restart_strategy: :one_for_one)

    {:ok, _} =
      Sup.add_group_worker(@sup_id, group_id, {MyTest, :loop, [:crash_test]}, id: :crash_target)

    for i <- 1..num_siblings do
      {:ok, _} = Sup.add_group_worker(@sup_id, group_id, {MyTest, :loop, [i]}, id: i)
    end

    Process.sleep(200)

    # Store state in siblings
    for i <- 1..num_siblings do
      Sup.send_to_group_worker(@sup_id, group_id, i, {:store, :key, i})
    end

    # Crash one worker
    Sup.send_to_group_worker(@sup_id, group_id, :crash_target, {:raise, "crash"})
    Process.sleep(800)

    # Siblings must still have their state
    sibling_ok =
      Enum.count(1..num_siblings, fn i ->
        Sup.send_to_group_worker(@sup_id, group_id, i, {:get, :key, self()})

        receive do
          {:result, ^i} -> true
          _ -> false
        after
          2_000 -> false
        end
      end)

    assert sibling_ok == num_siblings

    # The crashed worker must have restarted fresh
    assert wait_for_worker(group_id, :crash_target),
           "crash_target did not restart within deadline"

    Sup.send_to_group_worker(@sup_id, group_id, :crash_target, {:get, :key, self()})

    assert_receive {:result, nil}, 2_000
  end

  @tag timeout: @default_timeout
  test "one_for_one: restart via API clears worker state" do
    group_id = make_ref()

    {:ok, _} = Sup.add_group(@sup_id, id: group_id, restart_strategy: :one_for_one)
    {:ok, _} = Sup.add_group_worker(@sup_id, group_id, {MyTest, :loop, []}, id: :w)

    Sup.send_to_group_worker(@sup_id, group_id, :w, {:store, :k, :hello})
    Sup.send_to_group_worker(@sup_id, group_id, :w, {:get, :k, self()})
    assert_receive {:result, :hello}, 2_000

    Sup.restart_group_worker(@sup_id, group_id, :w)
    Process.sleep(300)

    Sup.send_to_group_worker(@sup_id, group_id, :w, {:get, :k, self()})
    assert_receive {:result, nil}, 2_000
  end

  # ---------------------------------------------------------------------------
  # Restart — one_for_all
  # ---------------------------------------------------------------------------

  @tag timeout: @default_timeout
  test "one_for_all: crash propagates restart to all workers" do
    group_id = make_ref()
    num_workers = 10_000

    {:ok, _} = Sup.add_group(@sup_id, id: group_id, restart_strategy: :one_for_all)

    {:ok, _} =
      Sup.add_group_worker(@sup_id, group_id, {MyTest, :loop, []}, id: :crash_worker)

    for i <- 1..num_workers do
      {:ok, _} = Sup.add_group_worker(@sup_id, group_id, {MyTest, :loop, [i]}, id: i)
    end

    Process.sleep(500)

    # Store state in worker 1 so we can verify it is wiped
    Sup.send_to_group_worker(@sup_id, group_id, 1, {:store, :test, :hello})

    Sup.send_to_group_worker(@sup_id, group_id, 1, {:get, :test, self()})
    assert_receive {:result, :hello}, 2_000

    # Trigger crash
    Sup.send_to_group_worker(@sup_id, group_id, :crash_worker, {:raise, "trigger one_for_all"})
    Process.sleep(1_500)

    # Worker 1 must have restarted — state should be nil
    assert wait_for_worker(group_id, 1), "worker 1 did not restart"

    Sup.send_to_group_worker(@sup_id, group_id, 1, {:get, :test, self()})
    assert_receive {:result, nil}, 2_000

    Sup.remove_group(@sup_id, group_id)
  end

  # ---------------------------------------------------------------------------
  # Restart all workers via API
  # ---------------------------------------------------------------------------

  @tag timeout: @default_timeout
  test "restart_group clears state for all workers" do
    group_id = make_ref()
    num_workers = 5_000

    {:ok, _} = Sup.add_group(@sup_id, id: group_id, restart_strategy: :one_for_one)

    for i <- 1..num_workers do
      {:ok, _} = Sup.add_group_worker(@sup_id, group_id, {MyTest, :loop, [i]}, id: i)
    end

    Process.sleep(500)

    # Write state to every worker
    for i <- 1..num_workers do
      Sup.send_to_group_worker(@sup_id, group_id, i, {:store, :test, :hello})
    end

    # Verify state is set
    all_set =
      Enum.all?(1..num_workers, fn i ->
        Sup.send_to_group_worker(@sup_id, group_id, i, {:get, :test, self()})

        receive do
          {:result, :hello} -> true
          _ -> false
        after
          1_000 -> false
        end
      end)

    assert all_set

    Sup.restart_group(@sup_id, group_id)
    Process.sleep(1_500)

    # State must be gone after restart
    all_nil =
      Enum.count(1..num_workers, fn i ->
        Sup.send_to_group_worker(@sup_id, group_id, i, {:get, :test, self()})

        receive do
          {:result, nil} -> true
          _ -> false
        after
          2_000 -> false
        end
      end)

    assert all_nil == num_workers
  end

  # ---------------------------------------------------------------------------
  # Parallel group operations
  # ---------------------------------------------------------------------------

  @tag timeout: @default_timeout
  test "parallel groups do not interfere with each other" do
    num_groups = 50
    num_workers_per_group = 100
    ref = make_ref()
    parent = self()

    for g <- 1..num_groups do
      spawn(fn ->
        group_id = {ref, g}

        {:ok, _} = Sup.add_group(@sup_id, id: group_id, restart_strategy: :one_for_one)

        for w <- 1..num_workers_per_group do
          {:ok, _} = Sup.add_group_worker(@sup_id, group_id, {MyTest, :loop, [w]}, id: w)
        end

        Sup.broadcast_to_group(@sup_id, group_id, {:ping, self()})

        pong_count =
          Enum.count(1..num_workers_per_group, fn _ ->
            receive do
              {:pong, _} -> true
            after
              5_000 -> false
            end
          end)

        send(parent, {:group_done, group_id, pong_count})
      end)
    end

    results =
      for _ <- 1..num_groups do
        assert_receive {:group_done, _gid, count}, 60_000
        count
      end

    assert Enum.all?(results, &(&1 == num_workers_per_group))
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp do_send_receive(parent, _group_id, worker_id, 0),
    do: send(parent, {:result, worker_id})

  defp do_send_receive(parent, group_id, worker_id, times) do
    Sup.send_to_group_worker(@sup_id, group_id, worker_id, {:store, :test, :hello})
    Sup.send_to_group_worker(@sup_id, group_id, worker_id, {:get, :test, self()})

    receive do
      {:result, :hello} -> do_send_receive(parent, group_id, worker_id, times - 1)
    after
      5_000 -> send(parent, {:error, {:timeout, worker_id}})
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

  defp wait_for_worker(group_id, worker_id, attempts \\ 10)
  defp wait_for_worker(_group_id, _worker_id, 0), do: false

  defp wait_for_worker(group_id, worker_id, n) do
    case Sup.get_pid_group_worker(@sup_id, group_id, worker_id) do
      {:ok, _} ->
        true

      {:error, _} ->
        Process.sleep(500)
        wait_for_worker(group_id, worker_id, n - 1)
    end
  end
end
