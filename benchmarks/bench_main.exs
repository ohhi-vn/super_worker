#!/usr/bin/env elixir

# Benchmark script for SuperWorker
# Run with: mix run benchmarks/bench_main.exs

alias SuperWorker.Supervisor, as: Sup
alias SuperWorker.Supervisor.{Worker, Group, Chain, Db, MapQueue}

require Logger

# Helper to generate unique supervisor ID
defmodule BenchHelper do
  def unique_sup_id do
    :"bench_sup_#{System.unique_integer([:positive])}"
  end

  def wait_for_completion(sup_id, timeout \\ 5000) do
    ref = make_ref()

    receive do
      {^ref, :done} -> :ok
    after
      timeout -> {:error, :timeout}
    end
  end
end

IO.puts("\n=== SuperWorker Performance Benchmarks ===\n")

Benchee.run(
  %{
    "ETS insert operations" => fn ->
      table =
        :ets.new(:bench_table, [
          :set,
          :public,
          {:write_concurrency, true},
          {:read_concurrency, true}
        ])

      # Benchmark insert operations
      for i <- 1..1000 do
        :ets.insert(table, {{:ref, make_ref()}, i, {:standalone, nil}, self()})
      end

      :ets.delete(table)
    end,
    "ETS lookup operations" => fn ->
      table =
        :ets.new(:bench_table, [
          :set,
          :public,
          {:write_concurrency, true},
          {:read_concurrency, true}
        ])

      # Insert test data
      ref = make_ref()
      :ets.insert(table, {{:ref, ref}, 1, {:standalone, nil}, self()})

      # Benchmark lookup
      for _ <- 1..1000 do
        :ets.lookup(table, {:ref, ref})
      end

      :ets.delete(table)
    end,
    "ETS match_object operations" => fn ->
      table =
        :ets.new(:bench_table, [
          :set,
          :public,
          {:write_concurrency, true},
          {:read_concurrency, true}
        ])

      # Insert test data
      for i <- 1..100 do
        :ets.insert(table, {{:ref, make_ref()}, i, {:standalone, nil}, self()})
      end

      # Benchmark match_object
      for _ <- 1..100 do
        :ets.match_object(table, {{:ref, :_}, :_, {:standalone, nil}, :_})
      end

      :ets.delete(table)
    end,
    "MapQueue add operations" => fn ->
      queue = MapQueue.new(:bench_queue, queue_length: 1000)

      # Benchmark adding messages
      for i <- 1..1000 do
        {:ok, _, _} = MapQueue.add(queue, "message_#{i}")
      end
    end,
    "MapQueue add and remove operations" => fn ->
      queue = MapQueue.new(:bench_queue, queue_length: 1000)

      # Add messages
      {queue, msg_ids} =
        Enum.reduce(1..100, {queue, []}, fn i, {q, ids} ->
          {:ok, new_q, msg_id} = MapQueue.add(q, "message_#{i}")
          {new_q, [msg_id | ids]}
        end)

      # Benchmark removing messages
      Enum.each(msg_ids, fn msg_id ->
        {:ok, _} = MapQueue.remove(queue, msg_id)
      end)
    end,
    "Worker creation (minimal)" => fn ->
      sup_id = BenchHelper.unique_sup_id()

      {:ok, _} = Sup.start_with_config(link: false, id: sup_id, num_partitions: 1)

      # Add workers
      for i <- 1..10 do
        {:ok, _} =
          Sup.add_standalone_worker(
            sup_id,
            fn -> Process.sleep(10) end,
            id: :"worker_#{i}"
          )
      end

      Sup.stop(sup_id)
    end,
    "Group worker broadcast" => fn ->
      sup_id = BenchHelper.unique_sup_id()
      group_id = :"group_#{System.unique_integer([:positive])}"

      {:ok, _} = Sup.start_with_config(link: false, id: sup_id, num_partitions: 1)
      {:ok, _} = Sup.add_group(sup_id, id: group_id, restart_strategy: :one_for_one)

      # Add workers to group
      for i <- 1..5 do
        {:ok, _} =
          Sup.add_group_worker(
            sup_id,
            group_id,
            fn ->
              receive do
                msg -> msg
              end
            end,
            id: :"g_worker_#{i}"
          )
      end

      # Benchmark broadcast
      for _ <- 1..10 do
        Sup.broadcast_to_group(sup_id, group_id, {:test_msg, self()})
      end

      Sup.stop(sup_id)
    end,
    "Chain worker message passing" => fn ->
      sup_id = BenchHelper.unique_sup_id()
      chain_id = :"chain_#{System.unique_integer([:positive])}"

      {:ok, _} = Sup.start_with_config(link: false, id: sup_id, num_partitions: 1)

      {:ok, _} =
        Sup.add_chain(sup_id, id: chain_id, restart_strategy: :one_for_one, queue_length: 100)

      # Add chain workers
      for i <- 1..3 do
        {:ok, _} =
          Sup.add_chain_worker(
            sup_id,
            chain_id,
            fn data ->
              {:next, "#{data}_processed_#{i}"}
            end,
            id: i
          )
      end

      # Benchmark sending data to chain
      for i <- 1..50 do
        Sup.send_to_chain(sup_id, chain_id, "data_#{i}")
      end

      # Wait for processing to complete
      Process.sleep(500)
      Sup.stop(sup_id)
    end
  },
  time: 5,
  memory_time: 2,
  reduction_time: 2,
  formatters: [
    {Benchee.Formatters.Console, extended_statistics: true}
  ]
)

IO.puts("\n=== Error Handling Benchmarks ===\n")

# Test error handling scenarios
Benchee.run(
  %{
    "Handle worker crash" => fn ->
      sup_id = BenchHelper.unique_sup_id()

      {:ok, _} = Sup.start_with_config(link: false, id: sup_id, num_partitions: 1)

      # Add a worker that will crash
      {:ok, _} =
        Sup.add_standalone_worker(
          sup_id,
          fn ->
            # Simulate some work then crash
            Process.sleep(5)
            raise "simulated crash"
          end,
          id: :crash_worker,
          restart_strategy: :temporary
        )

      Process.sleep(50)

      Sup.stop(sup_id)
    end,
    "Handle invalid worker config" => fn ->
      # Try to add worker with invalid config
      try do
        Worker.from_config(type: :invalid_type, id: make_ref())
      rescue
        _ -> :ok
      end
    end,
    "ETS cleanup stale entries" => fn ->
      table = :ets.new(:bench_table, [:set, :public])

      # Insert multiple entries for same worker (simulating stale entries)
      worker_id = make_ref()
      parent = {:standalone, nil}

      for i <- 1..10 do
        ref = make_ref()
        pid = spawn(fn -> Process.sleep(1000) end)
        :ets.insert(table, {{:ref, ref}, worker_id, parent, pid})
      end

      # Now cleanup - get_worker_by_id will clean stale entries
      # (This would be called in real scenario)
      entries = :ets.match_object(table, {{:ref, :_}, worker_id, parent, :_})

      # Clean up dead entries
      Enum.each(entries, fn {{_, ref}, _, _, pid} ->
        if not Process.alive?(pid) do
          :ets.delete(table, {:ref, ref})
        end
      end)

      :ets.delete(table)
    end
  },
  time: 3,
  formatters: [
    {Benchee.Formatters.Console, extended_statistics: true}
  ]
)

IO.puts("\n=== Benchmark Complete ===\n")
