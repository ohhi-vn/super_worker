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
