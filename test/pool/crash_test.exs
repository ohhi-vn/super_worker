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

  test "busy index mirrors busy map through crash, retry, and completion" do
    name = TestPool.unique_name()
    counter = TestPool.unique_counter()
    {:ok, _agent} = TestPool.Counter.start(counter)

    {:ok, _} =
      Pool.start_link(
        name: name,
        worker: TestPool.CrashOnceWorker,
        worker_opts: [counter: counter],
        size: 2,
        partitions: 1,
        retry: [max_attempts: 5, backoff: {:fixed, 10}]
      )

    # Crash → requeue → success, then a plain completion.
    assert {:ok, {:done, :crashing, _}} = Pool.run(name, :crashing)
    assert {:ok, _} = Pool.run(name, :after_crash)

    # Quiescent: nothing in flight, index fully drained.
    state = partition_state(name, 1)
    assert state.busy == %{}
    assert state.busy_by_worker == %{}

    Pool.stop(name)
  end

  test "busy index is the exact inverse of busy while jobs are in flight" do
    name = TestPool.unique_name()

    {:ok, _} =
      Pool.start_link(
        name: name,
        worker: TestPool.SlowWorker,
        worker_opts: [ms: 200],
        size: 2,
        partitions: 1,
        max_queue: 10
      )

    assert Pool.run(name, :warmup) == {:ok, :warmup}

    {:ok, ref1} = Pool.run_async(name, :job1)
    {:ok, ref2} = Pool.run_async(name, :job2)
    {:ok, ref3} = Pool.run_async(name, :job3)

    state = partition_state(name, 1)
    assert map_size(state.busy) == 2

    assert Map.new(state.busy, fn {job_id, worker} -> {worker, job_id} end) ==
             state.busy_by_worker

    assert Pool.await(ref1) == {:ok, :job1}
    assert Pool.await(ref2) == {:ok, :job2}
    assert Pool.await(ref3) == {:ok, :job3}

    Pool.stop(name)
  end

  test "cascade shutdown never dead-letters a healthy job" do
    # Regression: with several workers on one partition, a worker crash
    # rest_for_one-restarts younger siblings; a sibling holding the requeued
    # job then exits :shutdown, which must requeue uncharged, not dead-letter.
    results =
      Enum.map(1..20, fn _ ->
        name = TestPool.unique_name()
        counter = TestPool.unique_counter()
        {:ok, _agent} = TestPool.Counter.start(counter)

        {:ok, _} =
          Pool.start_link(
            name: name,
            worker: TestPool.CrashOnceWorker,
            worker_opts: [counter: counter],
            size: 2,
            partitions: 1,
            retry: [max_attempts: 5, backoff: {:fixed, 1}]
          )

        result = Pool.run(name, :job)
        Pool.stop(name)
        result
      end)

    assert Enum.all?(results, &match?({:ok, _}, &1)), "dead-lettered: #{inspect(results)}"
  end

  defp partition_state(pool, id) do
    [{pid, _}] = Registry.lookup(SuperWorker.Pool.Registry, {pool, {:partition, id}})
    :sys.get_state(pid)
  end
end
