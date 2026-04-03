defmodule SuperWorker.Supervisor.StandaloneWorkloadTest do
  @moduledoc """
  Workload tests for standalone workers.

  These tests verify supervisor behavior under moderate load with reasonable
  worker counts (100-500) and timeouts (30s). They are designed to run quickly
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
    sup_id = :"sup_workload_standalone_#{System.unique_integer([:positive])}"
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
  # Spawn / basic lifecycle
  # ---------------------------------------------------------------------------

  @tag timeout: @default_timeout
  test "spawn workers and verify all are alive", %{sup_id: sup_id} do
    num_workers = 500
    ref = make_ref()

    for i <- 1..num_workers do
      {:ok, _} =
        Sup.add_standalone_worker(sup_id, MyGenServer,
          id: {ref, i},
          restart_strategy: :permanent
        )
    end

    # Workers start immediately; verify all respond
    alive_count =
      Enum.count(1..num_workers, fn i ->
        try do
          Sup.send_to_standalone_worker(sup_id, {ref, i}, {:ping, self()})
          assert_receive {:pong, _}, 2_000
          true
        catch
          _kind, _reason -> false
        end
      end)

    assert alive_count == num_workers
  end

  @tag timeout: @default_timeout
  test "duplicate worker id is rejected", %{sup_id: sup_id} do
    ref = make_ref()
    id = {ref, :dup}

    {:ok, _} =
      Sup.add_standalone_worker(sup_id, MyGenServer, id: id, restart_strategy: :permanent)

    assert {:error, :worker_already_exists} =
             Sup.add_standalone_worker(sup_id, MyGenServer,
               id: id,
               restart_strategy: :permanent
             )
  end

  @tag timeout: @default_timeout
  test "get pid returns correct pid for standalone worker", %{sup_id: sup_id} do
    ref = make_ref()
    id = {ref, :pid_test}

    {:ok, _} =
      Sup.add_standalone_worker(sup_id, MyGenServer, id: id, restart_strategy: :permanent)

    {:ok, pid} = Sup.get_pid_standalone_worker(sup_id, id)
    assert is_pid(pid)
    assert Process.alive?(pid)
  end

  @tag timeout: @default_timeout
  test "get pid returns error for unknown worker", %{sup_id: sup_id} do
    assert {:error, _} = Sup.get_pid_standalone_worker(sup_id, make_ref())
  end

  # ---------------------------------------------------------------------------
  # Restart strategies — crash and verify
  # ---------------------------------------------------------------------------

  @tag timeout: @default_timeout
  test "permanent workers restart after crash", %{sup_id: sup_id} do
    num_workers = 200
    ref = make_ref()

    for i <- 1..num_workers do
      {:ok, _} =
        Sup.add_standalone_worker(sup_id, MyGenServer,
          id: {ref, i},
          restart_strategy: :permanent
        )
    end

    # Crash all workers
    for i <- 1..num_workers do
      Sup.send_to_standalone_worker(sup_id, {ref, i}, :crash)
    end

    # Wait for restarts
    Process.sleep(500)

    # Every permanent worker must have restarted
    alive_count =
      Enum.count(1..num_workers, fn i ->
        Sup.send_to_standalone_worker(sup_id, {ref, i}, {:ping, self()})

        receive do
          {:pong, _} -> true
        after
          1_000 -> false
        end
      end)

    assert alive_count == num_workers
  end

  @tag timeout: @default_timeout
  test "transient workers restart after abnormal exit, not after :normal", %{sup_id: sup_id} do
    ref = make_ref()

    {:ok, _} =
      Sup.add_standalone_worker(sup_id, MyGenServer,
        id: {ref, :transient},
        restart_strategy: :transient
      )

    # Crash (abnormal) → should restart
    Sup.send_to_standalone_worker(sup_id, {ref, :transient}, :crash)
    Process.sleep(300)

    Sup.send_to_standalone_worker(sup_id, {ref, :transient}, {:ping, self()})
    assert_receive {:pong, _}, 1_000

    # Normal exit → should NOT restart
    Sup.send_to_standalone_worker(sup_id, {ref, :transient}, :stop_normal)
    Process.sleep(300)

    assert {:error, _} = Sup.get_pid_standalone_worker(sup_id, {ref, :transient})
  end

  @tag timeout: @default_timeout
  test "temporary workers never restart", %{sup_id: sup_id} do
    num_workers = 200
    ref = make_ref()

    for i <- 1..num_workers do
      {:ok, _} =
        Sup.add_standalone_worker(sup_id, MyGenServer,
          id: {ref, i},
          restart_strategy: :temporary
        )
    end

    for i <- 1..num_workers do
      Sup.send_to_standalone_worker(sup_id, {ref, i}, :crash)
    end

    Process.sleep(300)

    # No temporary worker should be alive
    dead_count =
      Enum.count(1..num_workers, fn i ->
        match?({:error, _}, Sup.get_pid_standalone_worker(sup_id, {ref, i}))
      end)

    assert dead_count == num_workers
  end

  @tag timeout: @default_timeout
  test "mixed restart strategies — only eligible workers come back", %{sup_id: sup_id} do
    num_workers = 300
    ref = make_ref()

    # Distribute strategies evenly
    strategy_for = fn i ->
      case rem(i, 3) do
        0 -> :permanent
        1 -> :transient
        _ -> :temporary
      end
    end

    for i <- 1..num_workers do
      {:ok, _} =
        Sup.add_standalone_worker(sup_id, MyGenServer,
          id: {ref, i},
          restart_strategy: strategy_for.(i)
        )
    end

    parent = self()

    # Crash all three categories in parallel
    Enum.each([:permanent, :transient, :temporary], fn strat ->
      spawn(fn ->
        for i <- 1..num_workers, strategy_for.(i) == strat do
          Sup.send_to_standalone_worker(sup_id, {ref, i}, :crash)
        end

        send(parent, {:crashed, strat})
      end)
    end)

    for _ <- 1..3, do: assert_receive({:crashed, _}, 10_000)

    Process.sleep(500)

    permanent_alive =
      Enum.count(1..num_workers, fn i ->
        strategy_for.(i) == :permanent &&
          match?({:ok, _}, Sup.get_pid_standalone_worker(sup_id, {ref, i}))
      end)

    transient_alive =
      Enum.count(1..num_workers, fn i ->
        strategy_for.(i) == :transient &&
          match?({:ok, _}, Sup.get_pid_standalone_worker(sup_id, {ref, i}))
      end)

    temporary_alive =
      Enum.count(1..num_workers, fn i ->
        strategy_for.(i) == :temporary &&
          match?({:ok, _}, Sup.get_pid_standalone_worker(sup_id, {ref, i}))
      end)

    permanent_total = Enum.count(1..num_workers, &(strategy_for.(&1) == :permanent))
    transient_total = Enum.count(1..num_workers, &(strategy_for.(&1) == :transient))

    assert permanent_alive == permanent_total
    assert transient_alive == transient_total
    assert temporary_alive == 0
  end

  # ---------------------------------------------------------------------------
  # Remove
  # ---------------------------------------------------------------------------

  @tag timeout: @default_timeout
  test "removed worker is no longer reachable", %{sup_id: sup_id} do
    ref = make_ref()
    id = {ref, :to_remove}

    {:ok, _} =
      Sup.add_standalone_worker(sup_id, MyGenServer, id: id, restart_strategy: :permanent)

    {:ok, _} = Sup.remove_standalone_worker(sup_id, id)
    Process.sleep(100)

    assert {:error, _} = Sup.get_pid_standalone_worker(sup_id, id)
  end

  @tag timeout: @default_timeout
  test "removing unknown worker returns error", %{sup_id: sup_id} do
    assert {:error, _} = Sup.remove_standalone_worker(sup_id, make_ref())
  end

  # ---------------------------------------------------------------------------
  # Messaging correctness under load
  # ---------------------------------------------------------------------------

  @tag timeout: @default_timeout
  test "concurrent sends to same worker preserve all responses", %{sup_id: sup_id} do
    ref = make_ref()
    id = {ref, :msg_target}

    {:ok, _} =
      Sup.add_standalone_worker(sup_id, MyGenServer, id: id, restart_strategy: :permanent)

    num_senders = 100
    parent = self()

    for _ <- 1..num_senders do
      spawn(fn ->
        Sup.send_to_standalone_worker(sup_id, id, {:ping, self()})

        result =
          receive do
            {:pong, _} -> :ok
          after
            2_000 -> :timeout
          end

        send(parent, result)
      end)
    end

    results = for(_ <- 1..num_senders, do: assert_receive(_, 3_000))
    assert Enum.count(results, &(&1 == :ok)) == num_senders
  end

  @tag timeout: @default_timeout
  test "crash-restart cycle workers recover and respond", %{sup_id: sup_id} do
    num_workers = 100
    ref = make_ref()

    for i <- 1..num_workers do
      {:ok, _} =
        Sup.add_standalone_worker(sup_id, MyGenServer,
          id: {ref, i},
          restart_strategy: :permanent
        )
    end

    # Verify all workers respond before crash
    before_count =
      Enum.count(1..num_workers, fn i ->
        Sup.send_to_standalone_worker(sup_id, {ref, i}, {:ping, self()})

        receive do
          {:pong, _} -> true
        after
          1_000 -> false
        end
      end)

    assert before_count == num_workers

    # Crash all workers
    for i <- 1..num_workers do
      Sup.send_to_standalone_worker(sup_id, {ref, i}, :crash)
    end

    # Wait for restarts
    Process.sleep(500)

    # Verify all workers recovered and respond
    after_count =
      Enum.count(1..num_workers, fn i ->
        Sup.send_to_standalone_worker(sup_id, {ref, i}, {:ping, self()})

        receive do
          {:pong, _} -> true
        after
          2_000 -> false
        end
      end)

    assert after_count == num_workers
  end

  # ---------------------------------------------------------------------------
  # High-churn resilience
  # ---------------------------------------------------------------------------

  @tag timeout: @default_timeout
  test "rapid add-crash-restart cycle stays consistent", %{sup_id: sup_id} do
    num_workers = 200
    ref = make_ref()

    for i <- 1..num_workers do
      {:ok, _} =
        Sup.add_standalone_worker(sup_id, MyGenServer,
          id: {ref, i},
          restart_strategy: :permanent
        )
    end

    Process.sleep(100)
    parent = self()

    spawn(fn ->
      for _ <- 1..3 do
        for i <- 1..num_workers do
          Sup.send_to_standalone_worker(sup_id, {ref, i}, :crash)
        end

        Process.sleep(300)
      end

      send(parent, :churn_done)
    end)

    assert_receive :churn_done, 15_000

    Process.sleep(500)

    alive_count =
      Enum.count(1..num_workers, fn i ->
        Sup.send_to_standalone_worker(sup_id, {ref, i}, {:ping, self()})

        receive do
          {:pong, _} -> true
        after
          1_000 -> false
        end
      end)

    assert alive_count == num_workers
  end

  @tag timeout: @default_timeout
  test "workers survive sustained load then respond correctly", %{sup_id: sup_id} do
    num_workers = 200
    iterations = 5
    ref = make_ref()

    for i <- 1..num_workers do
      {:ok, _} =
        Sup.add_standalone_worker(sup_id, MyGenServer,
          id: {ref, i},
          restart_strategy: :permanent
        )
    end

    Process.sleep(100)
    parent = self()

    for i <- 1..num_workers do
      spawn(fn ->
        result = send_with_retry(sup_id, {ref, i}, iterations)
        send(parent, result)
      end)
    end

    results = for(_ <- 1..num_workers, do: assert_receive(_, 15_000))
    ok_count = Enum.count(results, &(&1 == :ok))
    assert ok_count == num_workers
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp send_with_retry(_sup_id, _id, 0), do: :ok

  defp send_with_retry(sup_id, id, remaining) do
    Sup.send_to_standalone_worker(sup_id, id, {:ping, self()})

    case receive_with_timeout() do
      :ok ->
        send_with_retry(sup_id, id, remaining - 1)

      :timeout ->
        # Back off briefly and retry
        Process.sleep(100)
        send_with_retry(sup_id, id, remaining - 1)
    end
  end

  defp receive_with_timeout do
    receive do
      {:pong, _} -> :ok
    after
      1_000 -> :timeout
    end
  end
end
