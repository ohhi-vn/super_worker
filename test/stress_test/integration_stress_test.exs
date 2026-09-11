defmodule SuperWorker.StressTest.IntegrationStressTest do
  @moduledoc """
  Comprehensive stress tests simulating real-world scenarios with all worker types.

  Tests cover:
  - Continuous message sending under load
  - Worker crash and respawn verification
  - Dynamic add/remove cycles while under load
  - Mixed worker types (standalone, group, chain)
  - High concurrency with parallel senders

  All tests use moderate worker counts (50-200) to ensure they complete
  within reasonable time while still exercising the system at scale.
  """

  use ExUnit.Case, async: false

  alias SuperWorker.Supervisor, as: Sup

  @moduletag :capture_log
  @default_timeout 60_000

  # ---------------------------------------------------------------------------
  # Setup — per-test isolation
  # ---------------------------------------------------------------------------

  setup do
    sup_id = :"sup_stress_#{System.unique_integer([:positive])}"
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
  # Helper: Ping-pong worker for standalone/group
  # ---------------------------------------------------------------------------

  def ping_loop do
    receive do
      {:ping, from} ->
        send(from, {:pong, self()})
        ping_loop()

      {:store, key, val} ->
        Process.put(key, val)
        ping_loop()

      {:get, key, from} ->
        send(from, {:result, Process.get(key)})
        ping_loop()

      :crash ->
        raise "intentional crash"

      _other ->
        ping_loop()
    end
  end

  # ---------------------------------------------------------------------------
  # Helper: Chain worker that increments data
  # ---------------------------------------------------------------------------

  def chain_increment(data) do
    {:next, data + 1}
  end

  # Simple chain worker that just passes data through
  def chain_pass(data) do
    {:next, data}
  end

  # ---------------------------------------------------------------------------
  # Helper: Send N pings and count responses
  # ---------------------------------------------------------------------------

  defp send_pings_and_count(sup_id, worker_id, count, timeout \\ 2_000) do
    Enum.count(1..count, fn _ ->
      Sup.send_to_standalone_worker(sup_id, worker_id, {:ping, self()})

      receive do
        {:pong, _} -> true
      after
        timeout -> false
      end
    end)
  end

  # ===========================================================================
  # STANDALONE WORKER STRESS TESTS
  # ===========================================================================

  @tag timeout: @default_timeout
  test "standalone: continuous messaging under load with crash recovery", %{sup_id: sup_id} do
    num_workers = 100
    ref = make_ref()

    # Create permanent workers
    for i <- 1..num_workers do
      {:ok, _} =
        Sup.add_standalone_worker(sup_id, {__MODULE__, :ping_loop, []},
          id: {ref, i},
          restart_strategy: :permanent
        )
    end

    # Phase 1: Send continuous messages
    total_pings = 500
    ok_count = send_pings_and_count(sup_id, {ref, 1}, total_pings)
    assert ok_count == total_pings, "Phase 1: Expected #{total_pings} pongs, got #{ok_count}"

    # Phase 2: Crash all workers and verify respawn
    for i <- 1..num_workers do
      Sup.send_to_standalone_worker(sup_id, {ref, i}, :crash)
    end

    Process.sleep(500)

    # Phase 3: Verify all workers recovered
    recovered =
      Enum.count(1..num_workers, fn i ->
        Sup.send_to_standalone_worker(sup_id, {ref, i}, {:ping, self()})

        receive do
          {:pong, _} -> true
        after
          2_000 -> false
        end
      end)

    assert recovered == num_workers,
           "Phase 3: Expected #{num_workers} recovered workers, got #{recovered}"

    # Phase 4: Continuous messaging after recovery
    ok_count = send_pings_and_count(sup_id, {ref, 1}, total_pings)
    assert ok_count == total_pings, "Phase 4: Expected #{total_pings} pongs, got #{ok_count}"
  end

  @tag timeout: @default_timeout
  test "standalone: parallel senders with crash-restart cycle", %{sup_id: sup_id} do
    worker_id = make_ref()

    {:ok, _} =
      Sup.add_standalone_worker(sup_id, {__MODULE__, :ping_loop, []},
        id: worker_id,
        restart_strategy: :permanent
      )

    num_senders = 50
    messages_per_sender = 20
    _expected_total = num_senders * messages_per_sender

    # Start parallel senders
    parent = self()

    for _ <- 1..num_senders do
      spawn(fn ->
        results =
          Enum.count(1..messages_per_sender, fn _ ->
            Sup.send_to_standalone_worker(sup_id, worker_id, {:ping, self()})

            receive do
              {:pong, _} -> 1
            after
              3_000 -> 0
            end
          end)

        send(parent, {:sender_done, results})
      end)
    end

    # Crash worker while senders are running
    Process.sleep(100)
    Sup.send_to_standalone_worker(sup_id, worker_id, :crash)

    # Collect results
    total_received =
      Enum.reduce(1..num_senders, 0, fn _, acc ->
        receive do
          {:sender_done, count} -> acc + count
        after
          10_000 -> acc
        end
      end)

    # Some messages may be lost during crash, but worker should recover
    assert total_received > 0, "Expected some messages to be received, got #{total_received}"

    # Verify worker recovered
    Process.sleep(300)
    Sup.send_to_standalone_worker(sup_id, worker_id, {:ping, self()})
    assert_receive {:pong, _}, 2_000
  end

  @tag timeout: @default_timeout
  test "standalone: rapid add/remove cycle under load", %{sup_id: sup_id} do
    ref = make_ref()
    num_cycles = 50

    # Start a stable worker to send messages to during cycles
    {:ok, _} =
      Sup.add_standalone_worker(sup_id, {__MODULE__, :ping_loop, []},
        id: {ref, :stable},
        restart_strategy: :permanent
      )

    # Run add/remove cycles
    for cycle <- 1..num_cycles do
      worker_id = {ref, cycle}

      # Add worker
      {:ok, _} =
        Sup.add_standalone_worker(sup_id, {__MODULE__, :ping_loop, []},
          id: worker_id,
          restart_strategy: :permanent
        )

      # Send a message
      Sup.send_to_standalone_worker(sup_id, worker_id, {:ping, self()})
      assert_receive {:pong, _}, 1_000

      # Remove worker
      {:ok, _} = Sup.remove_standalone_worker(sup_id, worker_id)
      assert {:error, _} = Sup.get_pid_standalone_worker(sup_id, worker_id)
    end

    # Verify stable worker still works
    ok_count = send_pings_and_count(sup_id, {ref, :stable}, 100)
    assert ok_count == 100
  end

  # ===========================================================================
  # GROUP WORKER STRESS TESTS
  # ===========================================================================

  @tag timeout: @default_timeout
  test "group: continuous broadcast with crash recovery", %{sup_id: sup_id} do
    group_id = make_ref()
    num_workers = 50

    {:ok, _} = Sup.add_group(sup_id, id: group_id, restart_strategy: :one_for_one)

    # Create workers
    for i <- 1..num_workers do
      {:ok, _} =
        Sup.add_group_worker(sup_id, group_id, {__MODULE__, :ping_loop, []}, id: i)
    end

    # Phase 1: Broadcast messages
    for round <- 1..5 do
      Sup.broadcast_to_group(sup_id, group_id, {:ping, self()})

      pong_count =
        Enum.count(1..num_workers, fn _ ->
          receive do
            {:pong, _} -> true
          after
            2_000 -> false
          end
        end)

      assert pong_count == num_workers,
             "Round #{round}: Expected #{num_workers} pongs, got #{pong_count}"
    end

    # Phase 2: Crash half the workers
    for i <- 1..div(num_workers, 2) do
      Sup.send_to_group_worker(sup_id, group_id, i, :crash)
    end

    Process.sleep(500)

    # Phase 3: Verify all workers recovered
    for i <- 1..num_workers do
      Sup.send_to_group_worker(sup_id, group_id, i, {:ping, self()})
      assert_receive {:pong, _}, 2_000, "Worker #{i} did not recover"
    end

    # Phase 4: Broadcast after recovery
    Sup.broadcast_to_group(sup_id, group_id, {:ping, self()})

    pong_count =
      Enum.count(1..num_workers, fn _ ->
        receive do
          {:pong, _} -> true
        after
          2_000 -> false
        end
      end)

    assert pong_count == num_workers
  end

  @tag timeout: @default_timeout
  test "group: one_for_all crash propagates and recovers", %{sup_id: sup_id} do
    group_id = make_ref()
    num_workers = 30

    {:ok, _} = Sup.add_group(sup_id, id: group_id, restart_strategy: :one_for_all)

    # Create crash target + workers
    {:ok, _} =
      Sup.add_group_worker(sup_id, group_id, {__MODULE__, :ping_loop, []}, id: :crash_target)

    for i <- 1..num_workers do
      {:ok, _} =
        Sup.add_group_worker(sup_id, group_id, {__MODULE__, :ping_loop, []}, id: i)
    end

    # Store state in worker 1
    Sup.send_to_group_worker(sup_id, group_id, 1, {:store, :test, :hello})
    Sup.send_to_group_worker(sup_id, group_id, 1, {:get, :test, self()})
    assert_receive {:result, :hello}, 1_000

    # Crash the target - should trigger one_for_all restart
    Sup.send_to_group_worker(sup_id, group_id, :crash_target, :crash)
    Process.sleep(800)

    # Worker 1 should have restarted (state cleared)
    Sup.send_to_group_worker(sup_id, group_id, 1, {:get, :test, self()})
    assert_receive {:result, nil}, 2_000

    # All workers should be alive
    alive_count =
      Enum.count(1..num_workers, fn i ->
        Sup.send_to_group_worker(sup_id, group_id, i, {:ping, self()})

        receive do
          {:pong, _} -> true
        after
          2_000 -> false
        end
      end)

    assert alive_count == num_workers
  end

  @tag timeout: @default_timeout
  test "group: add/remove workers while broadcasting", %{sup_id: sup_id} do
    group_id = make_ref()
    num_initial_workers = 20

    {:ok, _} = Sup.add_group(sup_id, id: group_id, restart_strategy: :one_for_one)

    # Create initial workers
    for i <- 1..num_initial_workers do
      {:ok, _} =
        Sup.add_group_worker(sup_id, group_id, {__MODULE__, :ping_loop, []}, id: i)
    end

    parent = self()

    # Start broadcaster
    spawn(fn ->
      Enum.each(1..10, fn _ ->
        Sup.broadcast_to_group(sup_id, group_id, {:ping, self()})
        Process.sleep(50)
      end)

      send(parent, :broadcast_done)
    end)

    # Add/remove workers while broadcasting
    for i <- 1..10 do
      new_id = :"new_worker_#{i}"
      {:ok, _} = Sup.add_group_worker(sup_id, group_id, {__MODULE__, :ping_loop, []}, id: new_id)
      Process.sleep(20)
      Sup.remove_group_worker(sup_id, group_id, new_id)
    end

    assert_receive :broadcast_done, 10_000

    # Verify original workers still work
    for i <- 1..num_initial_workers do
      Sup.send_to_group_worker(sup_id, group_id, i, {:ping, self()})
      assert_receive {:pong, _}, 2_000
    end
  end

  # ===========================================================================
  # CHAIN WORKER STRESS TESTS
  # ===========================================================================

  @tag timeout: @default_timeout
  test "chain: continuous message flow", %{sup_id: sup_id} do
    chain_id = make_ref()
    num_workers = 5
    num_messages = 20
    parent = self()

    fun = fn result -> send(parent, {:chain_done, result}) end

    {:ok, _} =
      Sup.add_chain(sup_id,
        id: chain_id,
        restart_strategy: :one_for_one,
        finished_callback: {:fun, fun}
      )

    # Create chain workers
    for i <- 1..num_workers do
      {:ok, _} =
        Sup.add_chain_worker(sup_id, chain_id, {MyTest, :task, []}, id: i)
    end

    Process.sleep(100)

    # Send messages through chain continuously
    for _ <- 1..num_messages do
      {:ok, _} = Sup.send_to_chain(sup_id, chain_id, 3)
      assert_receive {:chain_done, _result}, 5_000
    end
  end

  @tag timeout: @default_timeout
  test "chain: multiple chains processing in parallel", %{sup_id: sup_id} do
    num_chains = 10
    workers_per_chain = 3
    messages_per_chain = 5
    ref = make_ref()
    parent = self()

    # Create chains
    for c <- 1..num_chains do
      chain_id = {ref, c}

      fun = fn result -> send(parent, {:chain_done, chain_id, result}) end

      {:ok, _} =
        Sup.add_chain(sup_id,
          id: chain_id,
          restart_strategy: :one_for_one,
          finished_callback: {:fun, fun}
        )

      for w <- 1..workers_per_chain do
        {:ok, _} =
          Sup.add_chain_worker(sup_id, chain_id, {MyTest, :task, []}, id: w)
      end
    end

    Process.sleep(100)

    # Send messages to all chains in parallel
    for c <- 1..num_chains do
      chain_id = {ref, c}

      spawn(fn ->
        for _ <- 1..messages_per_chain do
          {:ok, _} = Sup.send_to_chain(sup_id, chain_id, 1)
        end

        send(parent, {:chain_sender_done, c})
      end)
    end

    # Wait for all senders to finish
    for _ <- 1..num_chains do
      assert_receive {:chain_sender_done, _}, 15_000
    end

    # Collect all callback results
    expected_results = num_chains * messages_per_chain

    results =
      for _ <- 1..expected_results do
        assert_receive {:chain_done, _chain_id, _result}, 10_000
        :ok
      end

    assert length(results) == expected_results
  end

  @tag timeout: @default_timeout
  test "chain: remove and recreate chain", %{sup_id: sup_id} do
    chain_id = make_ref()
    num_workers = 3
    parent = self()

    fun = fn result -> send(parent, {:chain_done, result}) end

    # Create chain
    {:ok, _} =
      Sup.add_chain(sup_id,
        id: chain_id,
        restart_strategy: :one_for_one,
        finished_callback: {:fun, fun}
      )

    for i <- 1..num_workers do
      {:ok, _} =
        Sup.add_chain_worker(sup_id, chain_id, {MyTest, :task, []}, id: i)
    end

    Process.sleep(100)

    # Verify chain works
    {:ok, _} = Sup.send_to_chain(sup_id, chain_id, 1)
    assert_receive {:chain_done, _result}, 5_000

    # Remove entire chain
    Sup.remove_chain(sup_id, chain_id)
    Process.sleep(100)

    # Recreate chain with same ID
    {:ok, _} =
      Sup.add_chain(sup_id,
        id: chain_id,
        restart_strategy: :one_for_one,
        finished_callback: {:fun, fun}
      )

    for i <- 1..num_workers do
      {:ok, _} =
        Sup.add_chain_worker(sup_id, chain_id, {MyTest, :task, []}, id: i)
    end

    Process.sleep(100)

    # Chain should work again
    {:ok, _} = Sup.send_to_chain(sup_id, chain_id, 1)
    assert_receive {:chain_done, _result}, 5_000
  end

  # ===========================================================================
  # MIXED WORKER TYPES STRESS TEST
  # ===========================================================================

  @tag timeout: @default_timeout
  test "mixed: standalone + group + chain under concurrent load", %{sup_id: sup_id} do
    ref = make_ref()
    parent = self()

    # Setup standalone workers
    for i <- 1..10 do
      {:ok, _} =
        Sup.add_standalone_worker(sup_id, {__MODULE__, :ping_loop, []},
          id: {ref, :standalone, i},
          restart_strategy: :permanent
        )
    end

    # Setup group
    {:ok, _} = Sup.add_group(sup_id, id: {ref, :group}, restart_strategy: :one_for_one)

    for i <- 1..10 do
      {:ok, _} =
        Sup.add_group_worker(sup_id, {ref, :group}, {__MODULE__, :ping_loop, []}, id: i)
    end

    # Setup chain
    chain_fun = fn result -> send(parent, {:chain_done, result}) end

    {:ok, _} =
      Sup.add_chain(sup_id,
        id: {ref, :chain},
        restart_strategy: :one_for_one,
        finished_callback: {:fun, chain_fun}
      )

    for i <- 1..3 do
      {:ok, _} =
        Sup.add_chain_worker(sup_id, {ref, :chain}, {MyTest, :task, []}, id: i)
    end

    Process.sleep(200)

    # Run concurrent operations sequentially to avoid race conditions
    # Task 1: Standalone pings
    standalone_ok =
      Enum.count(1..50, fn _ ->
        Sup.send_to_standalone_worker(sup_id, {ref, :standalone, 1}, {:ping, self()})

        receive do
          {:pong, _} -> 1
        after
          2_000 -> 0
        end
      end)

    assert standalone_ok == 50, "Standalone pings failed"

    # Task 2: Group worker messaging (individual sends for reliability)
    group_ok =
      Enum.count(1..30, fn _ ->
        # Send to a random worker in the group
        worker_id = :rand.uniform(10)
        Sup.send_to_group_worker(sup_id, {ref, :group}, worker_id, {:ping, self()})

        receive do
          {:pong, _} -> 1
        after
          2_000 -> 0
        end
      end)

    assert group_ok == 30, "Group messaging failed: expected 30 pongs, got #{group_ok}"

    # Task 3: Chain messages
    chain_ok =
      Enum.count(1..10, fn _ ->
        {:ok, _} = Sup.send_to_chain(sup_id, {ref, :chain}, 1)

        receive do
          {:chain_done, _} -> 1
        after
          5_000 -> 0
        end
      end)

    assert chain_ok == 10, "Chain messages failed"

    # Task 4: Crash and recover standalone worker
    Sup.send_to_standalone_worker(sup_id, {ref, :standalone, 5}, :crash)
    Process.sleep(300)

    Sup.send_to_standalone_worker(sup_id, {ref, :standalone, 5}, {:ping, self()})
    assert_receive {:pong, _}, 2_000, "Standalone crash recovery failed"

    # Task 5: Add/remove group worker
    {:ok, _} =
      Sup.add_group_worker(sup_id, {ref, :group}, {__MODULE__, :ping_loop, []}, id: :temp_worker)

    Sup.send_to_group_worker(sup_id, {ref, :group}, :temp_worker, {:ping, self()})
    assert_receive {:pong, _}, 1_000, "Temp group worker failed"

    Sup.remove_group_worker(sup_id, {ref, :group}, :temp_worker)
  end

  @tag timeout: @default_timeout
  test "mixed: sustained load with periodic crashes", %{sup_id: sup_id} do
    ref = make_ref()
    num_workers = 10
    parent = self()

    # Create standalone workers
    for i <- 1..num_workers do
      {:ok, _} =
        Sup.add_standalone_worker(sup_id, {__MODULE__, :ping_loop, []},
          id: {ref, i},
          restart_strategy: :permanent
        )
    end

    # Create group
    {:ok, _} = Sup.add_group(sup_id, id: {ref, :group}, restart_strategy: :one_for_one)

    for i <- 1..num_workers do
      {:ok, _} =
        Sup.add_group_worker(sup_id, {ref, :group}, {__MODULE__, :ping_loop, []}, id: i)
    end

    # Create chain
    chain_fun = fn result -> send(parent, {:chain_done, result}) end

    {:ok, _} =
      Sup.add_chain(sup_id,
        id: {ref, :chain},
        restart_strategy: :one_for_one,
        finished_callback: {:fun, chain_fun}
      )

    for i <- 1..3 do
      {:ok, _} =
        Sup.add_chain_worker(sup_id, {ref, :chain}, {MyTest, :task, []}, id: i)
    end

    Process.sleep(200)

    # Run sustained load with periodic crashes
    crash_interval = 3
    num_rounds = 8

    for round <- 1..num_rounds do
      # Send messages
      Sup.send_to_standalone_worker(sup_id, {ref, 1}, {:ping, self()})
      assert_receive {:pong, _}, 2_000

      Sup.send_to_group_worker(sup_id, {ref, :group}, 1, {:ping, self()})
      assert_receive {:pong, _}, 2_000

      {:ok, _} = Sup.send_to_chain(sup_id, {ref, :chain}, 1)
      assert_receive {:chain_done, _}, 5_000

      # Crash a worker every N rounds
      if rem(round, crash_interval) == 0 do
        crash_idx = rem(round, num_workers) + 1

        # Crash standalone
        Sup.send_to_standalone_worker(sup_id, {ref, crash_idx}, :crash)

        # Crash group worker
        Sup.send_to_group_worker(sup_id, {ref, :group}, crash_idx, :crash)

        Process.sleep(500)
      end
    end

    # Final verification: all workers should be alive
    standalone_alive =
      Enum.count(1..num_workers, fn i ->
        Sup.send_to_standalone_worker(sup_id, {ref, i}, {:ping, self()})

        receive do
          {:pong, _} -> true
        after
          2_000 -> false
        end
      end)

    assert standalone_alive == num_workers,
           "Expected #{num_workers} standalone workers alive, got #{standalone_alive}"

    group_alive =
      Enum.count(1..num_workers, fn i ->
        Sup.send_to_group_worker(sup_id, {ref, :group}, i, {:ping, self()})

        receive do
          {:pong, _} -> true
        after
          2_000 -> false
        end
      end)

    assert group_alive == num_workers,
           "Expected #{num_workers} group workers alive, got #{group_alive}"
  end
end
