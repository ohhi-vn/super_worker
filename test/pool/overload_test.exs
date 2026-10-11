defmodule SuperWorker.Pool.OverloadTest do
  @moduledoc false

  use ExUnit.Case, async: true

  alias SuperWorker.Pool

  test "submissions beyond max_queue are rejected with :overloaded" do
    name = TestPool.unique_name()

    {:ok, _} =
      Pool.start_link(
        name: name,
        worker: TestPool.SlowWorker,
        worker_opts: [ms: 300],
        size: 1,
        partitions: 1,
        max_queue: 1
      )

    # Let the worker register with the partition, then fill it up.
    assert Pool.run(name, :warmup) == {:ok, :warmup}

    # One running + one queued fills the partition.
    {:ok, ref1} = Pool.run_async(name, :job1)
    {:ok, ref2} = Pool.run_async(name, :job2)

    assert Pool.run(name, :job3) == {:error, :overloaded}
    # cast overloads are dropped (logged) rather than blocking the caller.
    assert Pool.cast(name, :job4) == :ok

    assert Pool.await(ref1) == {:ok, :job1}
    assert Pool.await(ref2) == {:ok, :job2}

    Pool.stop(name)
  end

  test "overload rejection stays prompt with a large backlog" do
    name = TestPool.unique_name()

    {:ok, _} =
      Pool.start_link(
        name: name,
        worker: TestPool.SlowWorker,
        worker_opts: [ms: 500],
        size: 1,
        partitions: 1,
        max_queue: 2_000
      )

    assert Pool.run(name, :warmup) == {:ok, :warmup}

    {:ok, running} = Pool.run_async(name, :running)

    # Fill the backlog to the bound: 1 running + 2_000 queued.
    Enum.each(1..2_000, fn i -> {:ok, _} = Pool.run_async(name, {:fill, i}) end)

    # The partition is full: rejections must stay prompt even with a deep
    # backlog (per-job partition work is bounded, not linear in backlog).
    {micros, results} = :timer.tc(fn -> Enum.map(1..50, fn _ -> Pool.run(name, :extra) end) end)

    assert Enum.all?(results, &(&1 == {:error, :overloaded}))
    assert micros < 1_000_000

    assert Pool.await(running) == {:ok, :running}

    Pool.stop(name)
  end

  test "the queue drains and accepts jobs again" do
    name = TestPool.unique_name()

    {:ok, _} =
      Pool.start_link(
        name: name,
        worker: TestPool.SlowWorker,
        worker_opts: [ms: 50],
        size: 1,
        partitions: 1,
        max_queue: 0
      )

    assert Pool.run(name, :warmup) == {:ok, :warmup}
    {:ok, ref} = Pool.run_async(name, :first)
    # max_queue: 0 — nothing may queue while the worker is busy.
    assert Pool.run(name, :second) == {:error, :overloaded}
    assert Pool.await(ref) == {:ok, :first}
    assert Pool.run(name, :after_drain) == {:ok, :after_drain}

    Pool.stop(name)
  end
end
