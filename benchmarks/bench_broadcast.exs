alias SuperWorker.Supervisor, as: Sup
alias SuperWorker.Supervisor.{Group, Db}

# Config
num_workers = 100
num_broadcasts = 1000

IO.puts("=== BEFORE Optimization ===")
IO.puts("Workers: #{num_workers}, Broadcasts: #{num_broadcasts}")

# Start supervisor
{:ok, _} = Sup.start_with_config(link: false, id: :bench_sup, num_partitions: 1)

# Add group
{:ok, _} = Sup.add_group(:bench_sup, id: :bench_group, restart_strategy: :one_for_one)

# Add workers
for i <- 1..num_workers do
  {:ok, _} = Sup.add_group_worker(:bench_sup, :bench_group, fn ->
    receive do
      msg -> msg
    after
      60000 -> :timeout
    end
  end, id: :"w_#{i}")
end

IO.puts("Workers added. Starting benchmark...")

# Benchmark broadcast
{bench_time, :ok} = :timer.tc(fn ->
  for _ <- 1..num_broadcasts do
    Sup.broadcast_to_group(:bench_sup, :bench_group, {:test_msg, self()})
  end
end)

IO.puts("Total time: #{bench_time / 1000} ms")
IO.puts("Time per broadcast: #{bench_time / num_broadcasts} μs")

# Cleanup
Sup.stop(:bench_sup)
