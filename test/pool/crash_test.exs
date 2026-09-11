defmodule SuperWorker.Pool.CrashTest do
  @moduledoc false

  use ExUnit.Case, async: true

  alias SuperWorker.Pool

  test "a worker crash requeues the job once and the second attempt succeeds" do
    name = TestPool.unique_name()
    counter = TestPool.unique_counter()
    {:ok, _agent} = TestPool.Counter.start(counter)

    {:ok, _} =
      Pool.start_link(
        name: name,
        worker: TestPool.CrashOnceWorker,
        worker_opts: [counter: counter],
        size: 1,
        partitions: 1,
        retry: [max_attempts: 5]
      )

    # First execution raises (crashes the worker process), the job is
    # requeued once, the restarted worker succeeds on the second attempt.
    assert Pool.run(name, :job) == {:ok, {:done, :job, 2}}

    Pool.stop(name)
  end

  test "a poison-pill job is dead-lettered instead of crash-looping" do
    name = TestPool.unique_name()
    test_pid = self()

    {:ok, _} =
      Pool.start_link(
        name: name,
        worker: TestPool.PoisonWorker,
        size: 1,
        partitions: 1,
        retry: [max_attempts: 5],
        on_failure: fn job, reason -> send(test_pid, {:dead_letter, job, reason}) end
      )

    # First crash requeues once; the second crash dead-letters the job
    # instead of queueing it again — no crash loop.
    assert {:error, {:worker_crashed, %RuntimeError{}}} = Pool.run(name, :poison)
    assert_receive {:dead_letter, :poison, %RuntimeError{}}, 1_000

    Pool.stop(name)
  end

  test "a crashing worker does not take down the rest of the pool" do
    name = TestPool.unique_name()
    counter = TestPool.unique_counter()
    {:ok, _agent} = TestPool.Counter.start(counter)

    {:ok, _} =
      Pool.start_link(
        name: name,
        worker: TestPool.CrashOnceWorker,
        worker_opts: [counter: counter],
        size: 2,
        partitions: 2,
        retry: [max_attempts: 5]
      )

    assert {:ok, {:done, :crashing, _}} = Pool.run(name, :crashing)
    # Other workers (and the restarted worker) still serve jobs.
    assert {:ok, _} = Pool.run(name, :after_crash)
    {:ok, info} = Pool.info(name)
    assert Enum.all?(info.partition_details, & &1.alive?)

    Pool.stop(name)
  end
end
