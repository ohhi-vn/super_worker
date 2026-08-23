defmodule SuperWorker.Supervisor.ChainWorkloadTest do
  @moduledoc """
  Workload tests for chain workers.

  These tests verify supervisor behavior under moderate load with reasonable
  worker counts (50-200) and timeouts (30s). They focus on basic chain
  creation, worker management, and simple message flow.
  """

  use ExUnit.Case, async: false
  require Logger

  alias SuperWorker.Supervisor, as: Sup

  @moduletag :capture_log
  @default_timeout 30_000

  # ---------------------------------------------------------------------------
  # Setup — per-test isolation with unique supervisor IDs
  # ---------------------------------------------------------------------------

  setup do
    sup_id = :"sup_workload_chain_#{System.unique_integer([:positive])}"
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
  # Basic chain lifecycle
  # ---------------------------------------------------------------------------

  @tag timeout: @default_timeout
  test "add and remove a chain", %{sup_id: sup_id} do
    chain_id = make_ref()
    {:ok, _} = Sup.add_chain(sup_id, id: chain_id, restart_strategy: :one_for_one)

    # Verify chain was created by checking worker count
    assert {:ok, 0} = Sup.count_workers_in_chain(sup_id, chain_id)

    assert :ok = Sup.remove_chain(sup_id, chain_id)

    # After removal, count should return error
    assert {:error, _} = Sup.count_workers_in_chain(sup_id, chain_id)
  end

  @tag timeout: @default_timeout
  test "duplicate chain id is rejected", %{sup_id: sup_id} do
    chain_id = make_ref()
    {:ok, _} = Sup.add_chain(sup_id, id: chain_id, restart_strategy: :one_for_one)

    assert {:error, _} =
             Sup.add_chain(sup_id, id: chain_id, restart_strategy: :one_for_one)
  end

  @tag timeout: @default_timeout
  test "add workers to chain and verify count", %{sup_id: sup_id} do
    chain_id = make_ref()
    num_workers = 200

    {:ok, _} = Sup.add_chain(sup_id, id: chain_id, restart_strategy: :one_for_one)

    for i <- 1..num_workers do
      {:ok, _} = Sup.add_chain_worker(sup_id, chain_id, {MyTest, :loop, [i]}, id: i)
    end

    {:ok, count} = Sup.count_workers_in_chain(sup_id, chain_id)
    assert count == num_workers
  end

  @tag timeout: @default_timeout
  test "duplicate worker id in chain is rejected", %{sup_id: sup_id} do
    chain_id = make_ref()
    {:ok, _} = Sup.add_chain(sup_id, id: chain_id, restart_strategy: :one_for_one)

    {:ok, _} = Sup.add_chain_worker(sup_id, chain_id, {MyTest, :loop, [1]}, id: :dup)

    assert {:error, :worker_already_exists} =
             Sup.add_chain_worker(sup_id, chain_id, {MyTest, :loop, [1]}, id: :dup)
  end

  @tag timeout: @default_timeout
  test "remove workers from chain, all pids disappear", %{sup_id: sup_id} do
    chain_id = make_ref()
    num_workers = 200

    {:ok, _} = Sup.add_chain(sup_id, id: chain_id, restart_strategy: :one_for_one)

    for i <- 1..num_workers do
      {:ok, _} = Sup.add_chain_worker(sup_id, chain_id, {MyTest, :loop, [i]}, id: i)
    end

    for i <- 1..num_workers do
      Sup.remove_chain_worker(sup_id, chain_id, i)
    end

    # All pids should be gone
    gone_count =
      Enum.count(1..num_workers, fn i ->
        match?({:error, _}, Sup.get_pid_chain_worker(sup_id, chain_id, i))
      end)

    assert gone_count == num_workers
  end

  @tag timeout: @default_timeout
  test "parallel chain creation", %{sup_id: sup_id} do
    num_chains = 30
    num_workers = 30
    parent = self()
    ref = make_ref()

    for i <- 1..num_chains do
      spawn(fn ->
        chain_id = {ref, i}
        {:ok, _} = Sup.add_chain(sup_id, id: chain_id, restart_strategy: :one_for_one)

        for w <- 1..num_workers do
          {:ok, _} = Sup.add_chain_worker(sup_id, chain_id, {MyTest, :loop, [w]}, id: w)
        end

        send(parent, {:chain_done, i})
      end)
    end

    for _ <- 1..num_chains do
      assert_receive {:chain_done, _}, 15_000
    end

    # Verify all chains have correct worker count
    for i <- 1..num_chains do
      chain_id = {ref, i}
      {:ok, count} = Sup.count_workers_in_chain(sup_id, chain_id)
      assert count == num_workers
    end
  end

  # ---------------------------------------------------------------------------
  # Message flow through chains
  # ---------------------------------------------------------------------------

  @tag timeout: @default_timeout
  test "message flows through a 10-worker chain and triggers callback", %{sup_id: sup_id} do
    chain_id = make_ref()
    num_workers = 10
    parent = self()

    fun = fn result -> send(parent, {:chain_done, result}) end

    {:ok, _} =
      Sup.add_chain(sup_id,
        id: chain_id,
        restart_strategy: :one_for_one,
        finished_callback: {:fun, fun}
      )

    for i <- 1..num_workers do
      {:ok, _} = Sup.add_chain_worker(sup_id, chain_id, {MyTest, :task, []}, id: i)
    end

    {:ok, _} = Sup.send_to_chain(sup_id, chain_id, 3)

    assert_receive {:chain_done, _result}, 15_000
  end

  @tag timeout: @default_timeout
  test "message through single-worker chain triggers callback", %{sup_id: sup_id} do
    chain_id = make_ref()
    parent = self()

    fun = fn result -> send(parent, {:chain_done, result}) end

    {:ok, _} =
      Sup.add_chain(sup_id,
        id: chain_id,
        restart_strategy: :one_for_one,
        finished_callback: {:fun, fun}
      )

    {:ok, _} = Sup.add_chain_worker(sup_id, chain_id, {MyTest, :task, []}, id: 1)

    {:ok, _} = Sup.send_to_chain(sup_id, chain_id, 5)

    assert_receive {:chain_done, _}, 5_000
  end

  @tag timeout: @default_timeout
  test "multiple messages through a 5-worker chain all reach callback", %{sup_id: sup_id} do
    chain_id = make_ref()
    num_messages = 50
    parent = self()

    fun = fn result -> send(parent, {:chain_done, result}) end

    {:ok, _} =
      Sup.add_chain(sup_id,
        id: chain_id,
        restart_strategy: :one_for_one,
        finished_callback: {:fun, fun}
      )

    for i <- 1..5 do
      {:ok, _} = Sup.add_chain_worker(sup_id, chain_id, {MyTest, :task, []}, id: i)
    end

    for _ <- 1..num_messages do
      {:ok, _} = Sup.send_to_chain(sup_id, chain_id, 1)
    end

    results =
      for _ <- 1..num_messages do
        assert_receive {:chain_done, _}, 5_000
        :ok
      end

    assert length(results) == num_messages
  end

  @tag timeout: @default_timeout
  test "parallel senders to same chain — all messages processed", %{sup_id: sup_id} do
    chain_id = make_ref()
    num_senders = 30
    parent = self()

    fun = fn result -> send(parent, {:chain_done, result}) end

    {:ok, _} =
      Sup.add_chain(sup_id,
        id: chain_id,
        restart_strategy: :one_for_one,
        finished_callback: {:fun, fun}
      )

    for i <- 1..3 do
      {:ok, _} = Sup.add_chain_worker(sup_id, chain_id, {MyTest, :task, []}, id: i)
    end

    for _ <- 1..num_senders do
      spawn(fn -> {:ok, _} = Sup.send_to_chain(sup_id, chain_id, 1) end)
    end

    results =
      for _ <- 1..num_senders do
        assert_receive {:chain_done, _}, 5_000
        :ok
      end

    assert length(results) == num_senders
  end

  # ---------------------------------------------------------------------------
  # Chain worker management
  # ---------------------------------------------------------------------------

  @tag timeout: @default_timeout
  test "chain worker count is accurate after add and remove", %{sup_id: sup_id} do
    chain_id = make_ref()

    {:ok, _} =
      Sup.add_chain(sup_id,
        id: chain_id,
        restart_strategy: :one_for_one
      )

    # Add 3 workers
    for i <- 1..3 do
      {:ok, _} = Sup.add_chain_worker(sup_id, chain_id, {MyTest, :task, []}, id: i)
    end

    assert {:ok, 3} = Sup.count_workers_in_chain(sup_id, chain_id)

    # Remove worker 2
    Sup.remove_chain_worker(sup_id, chain_id, 2)
    Process.sleep(100)

    assert {:ok, 2} = Sup.count_workers_in_chain(sup_id, chain_id)
  end

  @tag timeout: @default_timeout
  test "chain worker management with multiple workers", %{sup_id: sup_id} do
    chain_id = make_ref()
    num_workers = 5
    parent = self()

    fun = fn result -> send(parent, {:chain_done, result}) end

    {:ok, _} =
      Sup.add_chain(sup_id,
        id: chain_id,
        restart_strategy: :one_for_one,
        finished_callback: {:fun, fun}
      )

    for i <- 1..num_workers do
      {:ok, _} = Sup.add_chain_worker(sup_id, chain_id, {MyTest, :task, []}, id: i)
    end

    assert {:ok, ^num_workers} = Sup.count_workers_in_chain(sup_id, chain_id)

    # Remove all workers
    for i <- 1..num_workers do
      Sup.remove_chain_worker(sup_id, chain_id, i)
    end

    Process.sleep(100)
    assert {:ok, 0} = Sup.count_workers_in_chain(sup_id, chain_id)
  end

  # ---------------------------------------------------------------------------
  # Callback variations
  # ---------------------------------------------------------------------------

  @tag timeout: @default_timeout
  test "MFA finished_callback is invoked correctly", %{sup_id: sup_id} do
    chain_id = make_ref()
    parent = self()

    {:ok, _} =
      Sup.add_chain(sup_id,
        id: chain_id,
        restart_strategy: :one_for_one,
        finished_callback: {__MODULE__, :mfa_callback, [parent]}
      )

    {:ok, _} = Sup.add_chain_worker(sup_id, chain_id, {MyTest, :task, []}, id: 1)

    {:ok, _} = Sup.send_to_chain(sup_id, chain_id, 7)

    assert_receive {:mfa_result, _}, 5_000
  end

  @tag timeout: @default_timeout
  test "chain without callback still processes messages", %{sup_id: sup_id} do
    chain_id = make_ref()

    {:ok, _} =
      Sup.add_chain(sup_id,
        id: chain_id,
        restart_strategy: :one_for_one
      )

    {:ok, _} = Sup.add_chain_worker(sup_id, chain_id, {MyTest, :task, []}, id: 1)

    # Should not raise
    {:ok, _} = Sup.send_to_chain(sup_id, chain_id, 5)
    Process.sleep(200)
  end

  # ---------------------------------------------------------------------------
  # Error handling
  # ---------------------------------------------------------------------------

  @tag timeout: @default_timeout
  test "send to non-existent chain returns error", %{sup_id: sup_id} do
    assert {:error, _} = Sup.send_to_chain(sup_id, make_ref(), :data)
  end

  @tag timeout: @default_timeout
  test "remove non-existent chain returns error", %{sup_id: sup_id} do
    assert {:error, _} = Sup.remove_chain(sup_id, make_ref())
  end

  @tag timeout: @default_timeout
  test "count workers in non-existent chain returns error", %{sup_id: sup_id} do
    assert {:error, _} = Sup.count_workers_in_chain(sup_id, make_ref())
  end

  @tag timeout: @default_timeout
  test "get pid of non-existent chain worker returns error", %{sup_id: sup_id} do
    assert {:error, _} = Sup.get_pid_chain_worker(sup_id, make_ref(), 1)
  end

  # ---------------------------------------------------------------------------
  # Stress: many chains simultaneously
  # ---------------------------------------------------------------------------

  @tag timeout: @default_timeout
  test "20 independent chains each process 2 messages concurrently", %{sup_id: sup_id} do
    num_chains = 20
    messages_per_chain = 2
    parent = self()
    ref = make_ref()

    for i <- 1..num_chains do
      spawn(fn ->
        chain_id = {ref, i}

        fun = fn result -> send(parent, {:chain_done, chain_id, result}) end

        {:ok, _} =
          Sup.add_chain(sup_id,
            id: chain_id,
            restart_strategy: :one_for_one,
            finished_callback: {:fun, fun}
          )

        for w <- 1..3 do
          {:ok, _} = Sup.add_chain_worker(sup_id, chain_id, {MyTest, :task, []}, id: w)
        end

        for _ <- 1..messages_per_chain do
          {:ok, _} = Sup.send_to_chain(sup_id, chain_id, 1)
        end

        send(parent, {:chain_setup_done, i})
      end)
    end

    # Wait for all chains to be set up
    for _ <- 1..num_chains do
      assert_receive {:chain_setup_done, _}, 10_000
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

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  @doc false
  def mfa_callback(data, caller) do
    send(caller, {:mfa_result, data})
  end
end
