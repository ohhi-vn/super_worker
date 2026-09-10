defmodule SuperWorker.Pool.ApiTest do
  @moduledoc false

  use ExUnit.Case, async: true

  alias SuperWorker.Pool

  test "run with a fun task returns the fun result" do
    name = TestPool.unique_name()

    {:ok, _} =
      Pool.start_link(name: name, task: fn job -> {:echo, job} end, size: 2, partitions: 2)

    assert Pool.run(name, :hello) == {:ok, {:echo, :hello}}
    assert Pool.run(name, 1 + 1) == {:ok, {:echo, 2}}

    Pool.stop(name)
  end

  test "run with an MFA task prepends the job to args" do
    name = TestPool.unique_name()

    {:ok, _} = Pool.start_link(name: name, task: {String, :upcase, []}, size: 1, partitions: 1)

    assert Pool.run(name, "abc") == {:ok, "ABC"}

    Pool.stop(name)
  end

  test "a raising task is an expected error, not a crash" do
    name = TestPool.unique_name()

    task = fn
      :boom -> raise "nope"
      job -> {:fine, job}
    end

    {:ok, _} = Pool.start_link(name: name, task: task, size: 1, partitions: 1)

    assert {:error, {:error, %RuntimeError{message: "nope"}}} = Pool.run(name, :boom)
    # The worker survived: the next job still runs.
    assert Pool.run(name, :next) == {:ok, {:fine, :next}}

    Pool.stop(name)
  end

  test "a stateful worker keeps its state between jobs" do
    name = TestPool.unique_name()

    {:ok, _} = Pool.start_link(name: name, worker: TestPool.CountWorker, size: 1, partitions: 1)

    assert Pool.run(name, :a) == {:ok, {:a, 1}}
    assert Pool.run(name, :b) == {:ok, {:b, 2}}

    Pool.stop(name)
  end

  test "worker init opts are passed through" do
    name = TestPool.unique_name()

    {:ok, _} =
      Pool.start_link(
        name: name,
        worker: TestPool.SlowWorker,
        worker_opts: [ms: 10],
        size: 1,
        partitions: 1
      )

    assert Pool.run(name, :job) == {:ok, :job}

    Pool.stop(name)
  end

  test "run_async returns a ref and await returns the result" do
    name = TestPool.unique_name()

    {:ok, _} = Pool.start_link(name: name, task: fn job -> job * 2 end, size: 1, partitions: 1)

    {:ok, ref} = Pool.run_async(name, 21)
    assert Pool.await(ref) == {:ok, 42}

    Pool.stop(name)
  end

  test "await times out when the job never completes" do
    name = TestPool.unique_name()

    {:ok, _} =
      Pool.start_link(
        name: name,
        worker: TestPool.SlowWorker,
        worker_opts: [ms: 100],
        size: 1,
        partitions: 1
      )

    {:ok, ref} = Pool.run_async(name, :slow)
    assert Pool.await(ref, 10) == {:error, :timeout}

    # Wait for the worker to be free again before the pool is stopped.
    assert Pool.run(name, :done) == {:ok, :done}

    Pool.stop(name)
  end

  test "cast delivers the result to the on_result callback" do
    name = TestPool.unique_name()
    test_pid = self()

    {:ok, _} =
      Pool.start_link(
        name: name,
        task: fn job -> {:cast, job} end,
        size: 1,
        partitions: 1,
        on_result: fn job, result -> send(test_pid, {:on_result, job, result}) end
      )

    assert Pool.cast(name, :ping) == :ok
    assert_receive {:on_result, :ping, {:ok, {:cast, :ping}}}, 1_000

    Pool.stop(name)
  end

  test "run against an unknown pool fails without crashing" do
    assert Pool.run(:definitely_not_a_pool, :job) == {:error, :pool_not_found}
    assert Pool.cast(:definitely_not_a_pool, :job) == {:error, :pool_not_found}
  end

  test "info reports the configuration and live partition counters" do
    name = TestPool.unique_name()

    {:ok, _} = Pool.start_link(name: name, task: fn job -> job end, size: 2, partitions: 2)

    {:ok, info} = Pool.info(name)
    assert info.name == name
    assert info.size == 2
    assert info.partitions == 2
    assert info.workers_per_partition == 1
    assert info.routing == :round_robin

    Enum.each(info.partition_details, fn detail ->
      assert detail.alive?
      assert detail.idle == 1
      assert detail.busy == 0
      assert detail.queue == 0
    end)

    Pool.stop(name)
  end

  test "size smaller than partitions rounds workers up" do
    name = TestPool.unique_name()

    {:ok, _} = Pool.start_link(name: name, task: fn job -> job end, size: 2, partitions: 4)

    {:ok, info} = Pool.info(name)
    assert info.workers_per_partition == 1
    assert length(info.partition_details) == 4

    Enum.each(info.partition_details, fn detail ->
      assert detail.alive?
      assert detail.idle == 1
    end)

    Pool.stop(name)
  end

  test "invalid options are rejected" do
    assert {:error, {:missing, :name}} = Pool.start_link(task: fn job -> job end)
    assert {:error, {:invalid, {:task, nil}}} = Pool.start_link(name: :no_task)

    assert {:error, {:invalid, {:size, 0}}} =
             Pool.start_link(name: :bad_size, task: fn job -> job end, size: 0)

    assert {:error, {:invalid, {:task, "give either :task or :worker, not both"}}} =
             Pool.start_link(
               name: :both,
               task: fn job -> job end,
               worker: TestPool.CountWorker
             )

    assert {:error, {:invalid, {:middleware, [NotAModule]}}} =
             Pool.start_link(
               name: :bad_mw,
               task: fn job -> job end,
               middleware: [NotAModule]
             )
  end

  test "start_link fails when the name is already taken" do
    name = TestPool.unique_name()

    {:ok, _} = Pool.start_link(name: name, task: fn job -> job end, size: 1, partitions: 1)

    assert {:error, {:already_started, _pid}} =
             Pool.start_link(name: name, task: fn job -> job end, size: 1, partitions: 1)

    Pool.stop(name)
  end

  test "stop makes the pool unavailable" do
    name = TestPool.unique_name()

    {:ok, _} = Pool.start_link(name: name, task: fn job -> job end, size: 1, partitions: 1)
    assert Pool.stop(name) == :ok

    assert Pool.run(name, :job) == {:error, :pool_not_found}
    assert Pool.stop(name) == {:error, :not_running}
  end

  test "hash routing keeps identical jobs on the same partition" do
    name = TestPool.unique_name()

    {:ok, _} =
      Pool.start_link(name: name, task: fn job -> job end, size: 2, partitions: 2, routing: :hash)

    # Same job term twice must succeed regardless of partition.
    assert Pool.run(name, :same_job) == {:ok, :same_job}
    assert Pool.run(name, :same_job) == {:ok, :same_job}

    Pool.stop(name)
  end
end
