#!/usr/bin/env elixir

# Quick benchmark script - simpler version
# Run with: mix run benchmarks/bench_quick.exs

alias SuperWorker.Supervisor, as: Sup
alias SuperWorker.Supervisor.{MapQueue, Db}

IO.puts("\n=== Quick Performance Benchmarks ===\n")

# Benchmark 1: ETS Operations
IO.puts("Benchmarking ETS operations...")

{time_ets_insert, _} =
  :timer.tc(fn ->
    table = :ets.new(:bench_table, [:set, :public])

    for i <- 1..10000 do
      :ets.insert(table, {{:ref, make_ref()}, i, {:standalone, nil}, self()})
    end

    :ets.delete(table)
  end)

IO.puts("  ETS insert (10k ops): #{time_ets_insert / 1000} ms")

{time_ets_lookup, _} =
  :timer.tc(fn ->
    table = :ets.new(:bench_table, [:set, :public])
    ref = make_ref()
    :ets.insert(table, {{:ref, ref}, 1, {:standalone, nil}, self()})

    for _ <- 1..10000 do
      :ets.lookup(table, {:ref, ref})
    end

    :ets.delete(table)
  end)

IO.puts("  ETS lookup (10k ops): #{time_ets_lookup / 1000} ms")

# Benchmark 2: MapQueue Operations
IO.puts("\nBenchmarking MapQueue operations...")

{time_queue_add, _} =
  :timer.tc(fn ->
    queue = MapQueue.new(:bench_queue, queue_length: 10000)

    for i <- 1..10000 do
      {:ok, _, _} = MapQueue.add(queue, "message_#{i}")
    end
  end)

IO.puts("  MapQueue add (10k ops): #{time_queue_add / 1000} ms")

{time_queue_add_remove, _} =
  :timer.tc(fn ->
    queue = MapQueue.new(:bench_queue, queue_length: 10000)
    msg_ids = []

    {queue, msg_ids} =
      Enum.reduce(1..1000, {queue, []}, fn i, {q, ids} ->
        {:ok, new_q, msg_id} = MapQueue.add(q, "message_#{i}")
        {new_q, [msg_id | ids]}
      end)

    Enum.each(msg_ids, fn msg_id ->
      {:ok, _} = MapQueue.remove(queue, msg_id)
    end)
  end)

IO.puts("  MapQueue add+remove (1k ops): #{time_queue_add_remove / 1000} ms")

# Benchmark 3: Supervisor Operations
IO.puts("\nBenchmarking Supervisor operations...")

{time_sup_start, _} =
  :timer.tc(fn ->
    for _ <- 1..10 do
      sup_id = :"bench_#{System.unique_integer([:positive])}"
      {:ok, _} = Sup.start_with_config(link: false, id: sup_id, num_partitions: 1)
      Sup.stop(sup_id)
    end
  end)

IO.puts("  Supervisor start+stop (10 ops): #{time_sup_start / 1000} ms")

{time_workers, _} =
  :timer.tc(fn ->
    sup_id = :"bench_workers_#{System.unique_integer([:positive])}"
    {:ok, _} = Sup.start_with_config(link: false, id: sup_id, num_partitions: 1)

    for i <- 1..20 do
      {:ok, _} =
        Sup.add_standalone_worker(
          sup_id,
          fn -> Process.sleep(1) end,
          id: :"w_#{i}"
        )
    end

    Sup.stop(sup_id)
  end)

IO.puts("  Add 20 workers: #{time_workers / 1000} ms")

IO.puts("\n=== Quick Benchmarks Complete ===\n")
