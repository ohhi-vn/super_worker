defmodule SuperWorker.Pool.RetryTest do
  @moduledoc false

  use ExUnit.Case, async: true

  alias SuperWorker.Pool

  test "a retrying job eventually succeeds" do
    name = TestPool.unique_name()

    {:ok, _} =
      Pool.start_link(
        name: name,
        worker: TestPool.RetryWorker,
        worker_opts: [fails: 2],
        size: 1,
        partitions: 1,
        retry: [max_attempts: 3, backoff: {:fixed, 10}]
      )

    # Two `{:retry, ...}` results, then success.
    assert Pool.run(name, :job) == {:ok, {:done, :job}}

    Pool.stop(name)
  end

  test "a worker may succeed on the very first retry attempt" do
    name = TestPool.unique_name()

    {:ok, _} =
      Pool.start_link(
        name: name,
        worker: TestPool.RetryWorker,
        worker_opts: [fails: 1],
        size: 1,
        partitions: 1,
        retry: [max_attempts: 5, backoff: {:fixed, 10}]
      )

    assert Pool.run(name, :job) == {:ok, {:done, :job}}

    Pool.stop(name)
  end

  test "exhausted retries return {:error, {:retries_exhausted, reason}}" do
    name = TestPool.unique_name()

    {:ok, _} =
      Pool.start_link(
        name: name,
        worker: TestPool.RetryWorker,
        worker_opts: [fails: 99],
        size: 1,
        partitions: 1,
        retry: [max_attempts: 2, backoff: {:fixed, 10}]
      )

    assert Pool.run(name, :job) == {:error, {:retries_exhausted, :busy}}
    # The worker stayed free and healthy.
    assert Pool.info(name) |> elem(1) |> Map.get(:partition_details) |> Enum.all?(& &1.alive?)

    Pool.stop(name)
  end

  test "on_failure fires with the job and the retry reason when retries are exhausted" do
    name = TestPool.unique_name()
    test_pid = self()

    {:ok, _} =
      Pool.start_link(
        name: name,
        worker: TestPool.RetryWorker,
        worker_opts: [fails: 99],
        size: 1,
        partitions: 1,
        retry: [max_attempts: 2, backoff: {:fixed, 10}],
        on_failure: fn job, reason -> send(test_pid, {:dead_letter, job, reason}) end
      )

    assert Pool.run(name, :poison) == {:error, {:retries_exhausted, :busy}}
    assert_receive {:dead_letter, :poison, :busy}, 1_000
    Pool.stop(name)
  end

  test "an expected error is final: no retries, immediate result" do
    name = TestPool.unique_name()
    test_pid = self()

    {:ok, _} =
      Pool.start_link(
        name: name,
        worker: TestPool.ErrorWorker,
        size: 1,
        partitions: 1,
        retry: [max_attempts: 5, backoff: {:fixed, 10}],
        on_failure: fn job, reason -> send(test_pid, {:dead_letter, job, reason}) end
      )

    start_time = System.monotonic_time()
    assert Pool.run(name, :job) == {:error, :boom}
    # No backoff delay: the error is returned immediately.
    assert System.monotonic_time() - start_time <
             System.convert_time_unit(100, :millisecond, :native)

    # Expected errors are delivered to the caller immediately (no backoff),
    # and dead-lettered: on_failure is the single source of truth for
    # "this job did not complete" (see SuperWorker.Pool.Worker).
    assert_receive {:dead_letter, :job, :boom}, 1_000

    Pool.stop(name)
  end

  test "exhausted retries deliver the error to await and on_result for casts" do
    name = TestPool.unique_name()
    test_pid = self()

    {:ok, _} =
      Pool.start_link(
        name: name,
        worker: TestPool.RetryWorker,
        worker_opts: [fails: 99],
        size: 1,
        partitions: 1,
        retry: [max_attempts: 2, backoff: {:fixed, 10}],
        on_result: fn job, result -> send(test_pid, {:on_result, job, result}) end
      )

    {:ok, ref} = Pool.run_async(name, :job)
    assert Pool.await(ref, 1_000) == {:error, {:retries_exhausted, :busy}}

    Pool.cast(name, :cast_job)
    assert_receive {:on_result, :cast_job, {:error, {:retries_exhausted, :busy}}}, 1_000

    Pool.stop(name)
  end
end
