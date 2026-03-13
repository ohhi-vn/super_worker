defmodule SuperWorker.Supervisor.ChainWorkloadTest do
  use ExUnit.Case, async: false
  require Logger

  alias SuperWorker.Supervisor, as: Sup

  @sup_id :test_workload_chain
  @default_timeout 300_000

  # ---------------------------------------------------------------------------
  # Setup
  # ---------------------------------------------------------------------------

  setup_all do
    {:ok, _} = Sup.start_with_config(link: false, id: @sup_id)
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
  test "add and remove a chain" do
    chain_id = make_ref()
    {:ok, _} = Sup.add_chain(@sup_id, id: chain_id, restart_strategy: :one_for_one)

    {:ok, 0} = Sup.count_workers_in_chain(@sup_id, chain_id)

    :ok = Sup.remove_chain(@sup_id, chain_id)

    assert {:error, _} = Sup.count_workers_in_chain(@sup_id, chain_id)
  end

  @tag timeout: @default_timeout
  test "duplicate chain id is rejected" do
    chain_id = make_ref()
    {:ok, _} = Sup.add_chain(@sup_id, id: chain_id, restart_strategy: :one_for_one)

    assert {:error, _} =
             Sup.add_chain(@sup_id, id: chain_id, restart_strategy: :one_for_one)
  end

  @tag timeout: @default_timeout
  test "add 10_000 workers to chain and verify count" do
    chain_id = make_ref()
    num_workers = 10_000

    {:ok, _} = Sup.add_chain(@sup_id, id: chain_id, restart_strategy: :one_for_one)

    for i <- 1..num_workers do
      {:ok, _} = Sup.add_chain_worker(@sup_id, chain_id, {MyTest, :loop, [i]}, id: i)
    end

    Process.sleep(200)

    {:ok, count} = Sup.count_workers_in_chain(@sup_id, chain_id)
    assert count == num_workers
  end

  @tag timeout: @default_timeout
  test "duplicate worker id in chain is rejected" do
    chain_id = make_ref()
    {:ok, _} = Sup.add_chain(@sup_id, id: chain_id, restart_strategy: :one_for_one)

    {:ok, _} = Sup.add_chain_worker(@sup_id, chain_id, {MyTest, :loop, []}, id: :dup)

    assert {:error, :worker_already_exists} =
             Sup.add_chain_worker(@sup_id, chain_id, {MyTest, :loop, []}, id: :dup)
  end

  # ---------------------------------------------------------------------------
  # Worker removal
  # ---------------------------------------------------------------------------

  @tag timeout: @default_timeout
  test "remove 10_000 workers from chain, all pids disappear" do
    chain_id = make_ref()
    num_workers = 10_000

    {:ok, _} = Sup.add_chain(@sup_id, id: chain_id, restart_strategy: :one_for_one)

    for i <- 1..num_workers do
      {:ok, _} = Sup.add_chain_worker(@sup_id, chain_id, {MyTest, :loop, [i]}, id: i)
    end

    {:ok, count} = Sup.count_workers_in_chain(@sup_id, chain_id)
    assert count == num_workers

    for i <- 1..num_workers do
      Sup.remove_chain_worker(@sup_id, chain_id, i)
    end

    Process.sleep(200)

    gone_count =
      Enum.count(1..num_workers, fn i ->
        match?({:error, _}, Sup.get_pid_chain_worker(@sup_id, chain_id, i))
      end)

    assert gone_count == num_workers
  end

  # ---------------------------------------------------------------------------
  # Parallel chain creation
  # ---------------------------------------------------------------------------

  @tag timeout: @default_timeout
  test "100 chains × 100 workers each added in parallel" do
    num_chains = 100
    num_workers = 100
    ref = make_ref()
    parent = self()

    for c <- 1..num_chains do
      spawn(fn ->
        chain_id = {ref, c}
        {:ok, _} = Sup.add_chain(@sup_id, id: chain_id, restart_strategy: :one_for_one)

        for w <- 1..num_workers do
          {:ok, _} =
            Sup.add_chain_worker(@sup_id, chain_id, {MyTest, :loop, [w]}, id: w)
        end

        send(parent, {:chain_ready, chain_id})
      end)
    end

    for _ <- 1..num_chains do
      assert_receive {:chain_ready, _}, 30_000
    end

    # Verify every chain has exactly num_workers workers
    counts =
      for c <- 1..num_chains do
        chain_id = {ref, c}
        {:ok, count} = Sup.count_workers_in_chain(@sup_id, chain_id)
        count
      end

    assert Enum.all?(counts, &(&1 == num_workers))

    for c <- 1..num_chains do
      Sup.remove_chain(@sup_id, {ref, c})
    end
  end

  # ---------------------------------------------------------------------------
  # Data flow — single message through the chain
  # ---------------------------------------------------------------------------

  @tag timeout: @default_timeout
  test "message flows through a 10_000-worker chain and triggers callback" do
    chain_id = make_ref()
    num_workers = 10_000
    parent = self()

    finished = fn data ->
      send(parent, {:finished, data})
    end

    {:ok, _} =
      Sup.add_chain(@sup_id,
        id: chain_id,
        restart_strategy: :one_for_one,
        finished_callback: {:fun, finished}
      )

    for i <- 1..num_workers do
      {:ok, _} = Sup.add_chain_worker(@sup_id, chain_id, {MyTest, :ping_pong, []}, id: i)
    end

    Process.sleep(500)

    {:ok, _} = Sup.send_to_chain(@sup_id, chain_id, {:ping, self()}, 5_000)

    assert_receive {:finished, _}, 30_000
  end

  @tag timeout: @default_timeout
  test "3-worker chain passes and transforms data in order" do
    chain_id = make_ref()
    parent = self()

    # Each worker appends its index to the list
    step = fn idx ->
      fn data ->
        new = data ++ [idx]
        {:next, new}
      end
    end

    finished = fn data -> send(parent, {:result, data}) end

    {:ok, _} =
      Sup.add_chain(@sup_id,
        id: chain_id,
        restart_strategy: :one_for_one,
        finished_callback: {:fun, finished}
      )

    for i <- 1..3 do
      {:ok, _} =
        Sup.add_chain_worker(@sup_id, chain_id, {:fun, step.(i)}, id: i)
    end

    Process.sleep(200)

    {:ok, _} = Sup.send_to_chain(@sup_id, chain_id, [], 2_000)

    assert_receive {:result, [1, 2, 3]}, 5_000
  end

  @tag timeout: @default_timeout
  test "chain worker :drop stops propagation" do
    chain_id = make_ref()
    parent = self()

    finished = fn _data -> send(parent, :should_not_arrive) end

    dropper = fn _data -> {:drop, :intentional} end

    receiver = fn data ->
      send(parent, {:received, data})
      {:next, data}
    end

    {:ok, _} =
      Sup.add_chain(@sup_id,
        id: chain_id,
        restart_strategy: :one_for_one,
        finished_callback: {:fun, finished}
      )

    {:ok, _} = Sup.add_chain_worker(@sup_id, chain_id, {:fun, dropper}, id: 1)
    {:ok, _} = Sup.add_chain_worker(@sup_id, chain_id, {:fun, receiver}, id: 2)

    Process.sleep(200)

    {:ok, _} = Sup.send_to_chain(@sup_id, chain_id, :payload, 2_000)

    # Worker 2 must not receive anything; no finished callback must fire
    refute_receive :should_not_arrive, 1_000
    refute_receive {:received, _}, 500
  end

  @tag timeout: @default_timeout
  test "message through single-worker chain triggers callback immediately" do
    chain_id = make_ref()
    parent = self()

    finished = fn data -> send(parent, {:done, data}) end

    {:ok, _} =
      Sup.add_chain(@sup_id,
        id: chain_id,
        restart_strategy: :one_for_one,
        finished_callback: {:fun, finished}
      )

    {:ok, _} =
      Sup.add_chain_worker(@sup_id, chain_id, {MyTest, :ping_pong, []}, id: 1)

    Process.sleep(200)

    {:ok, _} = Sup.send_to_chain(@sup_id, chain_id, {:ping, self()}, 2_000)

    assert_receive {:done, _}, 5_000
  end

  # ---------------------------------------------------------------------------
  # Throughput — many messages
  # ---------------------------------------------------------------------------

  @tag timeout: @default_timeout
  test "1_000 messages through a 5-worker chain all reach callback" do
    chain_id = make_ref()
    num_messages = 1_000
    num_workers = 5
    parent = self()

    {:ok, counter} = Agent.start_link(fn -> 0 end)

    finished = fn _data ->
      Agent.update(counter, &(&1 + 1))

      if Agent.get(counter, & &1) == num_messages do
        send(parent, :all_done)
      end
    end

    {:ok, _} =
      Sup.add_chain(@sup_id,
        id: chain_id,
        restart_strategy: :one_for_one,
        finished_callback: {:fun, finished},
        queue_length: num_messages + 10
      )

    for i <- 1..num_workers do
      {:ok, _} =
        Sup.add_chain_worker(@sup_id, chain_id, {MyTest, :ping_pong, []}, id: i)
    end

    Process.sleep(300)

    for _ <- 1..num_messages do
      {:ok, _} = Sup.send_to_chain(@sup_id, chain_id, {:ping, self()}, 2_000)
    end

    assert_receive :all_done, 60_000

    final_count = Agent.get(counter, & &1)
    assert final_count == num_messages

    Agent.stop(counter)
  end

  @tag timeout: @default_timeout
  test "parallel senders to same chain — all messages processed" do
    chain_id = make_ref()
    num_senders = 50
    msgs_per_sender = 20
    total = num_senders * msgs_per_sender
    parent = self()

    {:ok, counter} = Agent.start_link(fn -> 0 end)

    finished = fn _data ->
      n = Agent.get_and_update(counter, fn c -> {c + 1, c + 1} end)
      if n == total, do: send(parent, :all_done)
    end

    {:ok, _} =
      Sup.add_chain(@sup_id,
        id: chain_id,
        restart_strategy: :one_for_one,
        finished_callback: {:fun, finished},
        queue_length: total + 10
      )

    for i <- 1..3 do
      {:ok, _} =
        Sup.add_chain_worker(@sup_id, chain_id, {MyTest, :ping_pong, []}, id: i)
    end

    Process.sleep(300)

    for _ <- 1..num_senders do
      spawn(fn ->
        for _ <- 1..msgs_per_sender do
          Sup.send_to_chain(@sup_id, chain_id, {:ping, self()}, 5_000)
        end
      end)
    end

    assert_receive :all_done, 60_000
    Agent.stop(counter)
  end

  # ---------------------------------------------------------------------------
  # Restart strategies
  # ---------------------------------------------------------------------------

  @tag timeout: @default_timeout
  test "one_for_one: only crashed worker restarts, chain resumes" do
    chain_id = make_ref()
    parent = self()

    finished = fn data -> send(parent, {:done, data}) end

    {:ok, _} =
      Sup.add_chain(@sup_id,
        id: chain_id,
        restart_strategy: :one_for_one,
        finished_callback: {:fun, finished}
      )

    for i <- 1..3 do
      {:ok, _} =
        Sup.add_chain_worker(@sup_id, chain_id, {MyTest, :ping_pong, []}, id: i)
    end

    Process.sleep(200)

    # Get pids before crash
    {:ok, pid1_before} = Sup.get_pid_chain_worker(@sup_id, chain_id, 1)
    {:ok, pid2_before} = Sup.get_pid_chain_worker(@sup_id, chain_id, 2)
    {:ok, pid3_before} = Sup.get_pid_chain_worker(@sup_id, chain_id, 3)

    # Crash worker 2
    Process.exit(pid2_before, :kill)
    Process.sleep(800)

    # Worker 2 should have a new pid
    {:ok, pid2_after} = Sup.get_pid_chain_worker(@sup_id, chain_id, 2)
    assert pid2_after != pid2_before

    # Workers 1 and 3 must be the same
    {:ok, ^pid1_before} = Sup.get_pid_chain_worker(@sup_id, chain_id, 1)
    {:ok, ^pid3_before} = Sup.get_pid_chain_worker(@sup_id, chain_id, 3)

    # Chain must still work end-to-end
    {:ok, _} = Sup.send_to_chain(@sup_id, chain_id, {:ping, self()}, 5_000)
    assert_receive {:done, _}, 10_000
  end

  @tag timeout: @default_timeout
  test "one_for_all: all workers restart when one crashes" do
    chain_id = make_ref()
    num_workers = 5

    {:ok, _} =
      Sup.add_chain(@sup_id,
        id: chain_id,
        restart_strategy: :one_for_all
      )

    for i <- 1..num_workers do
      {:ok, _} =
        Sup.add_chain_worker(@sup_id, chain_id, {MyTest, :ping_pong, []}, id: i)
    end

    Process.sleep(200)

    pids_before =
      for i <- 1..num_workers do
        {:ok, pid} = Sup.get_pid_chain_worker(@sup_id, chain_id, i)
        {i, pid}
      end

    # Kill worker 1
    {1, pid1} = List.keyfind(pids_before, 1, 0)
    Process.exit(pid1, :kill)
    Process.sleep(1_500)

    pids_after =
      for i <- 1..num_workers do
        {:ok, pid} = Sup.get_pid_chain_worker(@sup_id, chain_id, i)
        {i, pid}
      end

    # Every pid must have changed
    changed =
      Enum.count(1..num_workers, fn i ->
        before_pid = elem(List.keyfind(pids_before, i, 0), 1)
        after_pid = elem(List.keyfind(pids_after, i, 0), 1)
        before_pid != after_pid
      end)

    assert changed == num_workers
  end

  # ---------------------------------------------------------------------------
  # Finished callback variants
  # ---------------------------------------------------------------------------

  @tag timeout: @default_timeout
  test "MFA finished_callback is invoked correctly" do
    chain_id = make_ref()
    parent = self()

    {:ok, _} =
      Sup.add_chain(@sup_id,
        id: chain_id,
        restart_strategy: :one_for_one,
        finished_callback: {__MODULE__, :mfa_callback, [parent]}
      )

    {:ok, _} =
      Sup.add_chain_worker(@sup_id, chain_id, {MyTest, :ping_pong, []}, id: 1)

    Process.sleep(200)

    {:ok, _} = Sup.send_to_chain(@sup_id, chain_id, {:ping, self()}, 2_000)

    assert_receive {:mfa_cb, _data}, 5_000
  end

  @tag timeout: @default_timeout
  test "chain without callback still processes messages without error" do
    chain_id = make_ref()

    {:ok, _} =
      Sup.add_chain(@sup_id, id: chain_id, restart_strategy: :one_for_one)

    {:ok, _} =
      Sup.add_chain_worker(@sup_id, chain_id, {MyTest, :ping_pong, []}, id: 1)

    Process.sleep(200)

    # Should not crash
    {:ok, _} = Sup.send_to_chain(@sup_id, chain_id, {:ping, self()}, 2_000)

    # Give it time to process without a callback
    Process.sleep(500)
  end

  # ---------------------------------------------------------------------------
  # Edge cases
  # ---------------------------------------------------------------------------

  @tag timeout: @default_timeout
  test "send to non-existent chain returns error" do
    assert {:error, _} = Sup.send_to_chain(@sup_id, make_ref(), :data)
  end

  @tag timeout: @default_timeout
  test "remove non-existent chain returns error" do
    assert {:error, _} = Sup.remove_chain(@sup_id, make_ref())
  end

  @tag timeout: @default_timeout
  test "count workers in non-existent chain returns error" do
    assert {:error, _} = Sup.count_workers_in_chain(@sup_id, make_ref())
  end

  @tag timeout: @default_timeout
  test "get pid of non-existent chain worker returns error" do
    chain_id = make_ref()
    {:ok, _} = Sup.add_chain(@sup_id, id: chain_id, restart_strategy: :one_for_one)

    assert {:error, _} = Sup.get_pid_chain_worker(@sup_id, chain_id, :no_such_worker)
  end

  @tag timeout: @default_timeout
  test "empty chain processes messages without crashing" do
    chain_id = make_ref()
    parent = self()

    finished = fn data -> send(parent, {:done, data}) end

    {:ok, _} =
      Sup.add_chain(@sup_id,
        id: chain_id,
        finished_callback: {:fun, finished}
      )

    # Sending to an empty chain should invoke the callback immediately
    # (no workers = data falls straight through to finished_callback)
    {:ok, _} = Sup.send_to_chain(@sup_id, chain_id, :hello, 2_000)

    assert_receive {:done, :hello}, 3_000
  end

  # ---------------------------------------------------------------------------
  # Stress: many chains simultaneously
  # ---------------------------------------------------------------------------

  @tag timeout: @default_timeout
  test "200 independent chains each process 10 messages concurrently" do
    num_chains = 200
    msgs_per_chain = 10
    num_workers_per_chain = 3
    ref = make_ref()
    parent = self()

    for c <- 1..num_chains do
      chain_id = {ref, c}
      {:ok, counter} = Agent.start_link(fn -> 0 end, name: {__MODULE__, chain_id})

      finished = fn _data ->
        n = Agent.get_and_update({__MODULE__, chain_id}, fn x -> {x + 1, x + 1} end)
        if n == msgs_per_chain, do: send(parent, {:chain_done, chain_id})
      end

      {:ok, _} =
        Sup.add_chain(@sup_id,
          id: chain_id,
          restart_strategy: :one_for_one,
          finished_callback: {:fun, finished},
          queue_length: msgs_per_chain + 5
        )

      for w <- 1..num_workers_per_chain do
        {:ok, _} =
          Sup.add_chain_worker(@sup_id, chain_id, {MyTest, :ping_pong, []}, id: w)
      end
    end

    Process.sleep(500)

    # Send all messages in parallel
    for c <- 1..num_chains do
      chain_id = {ref, c}

      spawn(fn ->
        for _ <- 1..msgs_per_chain do
          Sup.send_to_chain(@sup_id, chain_id, {:ping, self()}, 10_000)
        end
      end)
    end

    # Wait for all chains to finish
    completed =
      Enum.count(1..num_chains, fn _ ->
        receive do
          {:chain_done, _} -> true
        after
          60_000 -> false
        end
      end)

    assert completed == num_chains

    for c <- 1..num_chains do
      Agent.stop({__MODULE__, {ref, c}})
    end
  end

  # ---------------------------------------------------------------------------
  # Exported MFA callback (must be public)
  # ---------------------------------------------------------------------------

  @doc false
  def mfa_callback(data, caller), do: send(caller, {:mfa_cb, data})
end
