defmodule SuperWorker.Pool.CoverageTest do
  @moduledoc false

  # Tests targeting the edge/defensive paths of the pool modules that the
  # happy-path suites do not exercise: invalid worker returns, middleware
  # crash handling (fail-open), circuit breaker state transitions, timer and
  # monitor leftovers, MFA-form callbacks and validation corner cases.

  use ExUnit.Case, async: true

  alias SuperWorker.Pool
  alias SuperWorker.Pool.Middleware.CircuitBreaker
  alias SuperWorker.Pool.TaskWorker

  @meta %{pool: :any_pool, partition: 1, job_id: nil, attempts: 0}

  describe "TaskWorker (unit)" do
    test "init accepts fun/1, fun/0 and MFA" do
      fun = fn job -> job end
      assert {:ok, {:fun, ^fun}} = TaskWorker.init({:fun, fun})

      fun0 = fn -> :zero end
      assert {:ok, {:fun0, ^fun0}} = TaskWorker.init({:fun0, fun0})

      assert {:ok, {:mfa, {String, :upcase, []}}} = TaskWorker.init({:mfa, {String, :upcase, []}})
    end

    test "init rejects anything else" do
      assert TaskWorker.init("nope") == {:error, {:invalid_task, "nope"}}

      assert {:error, {:invalid_task, {:fun, fun}}} =
               TaskWorker.init({:fun, fn a, b -> a + b end})

      assert is_function(fun, 2)
    end

    test "handle_job runs an arity-0 task and ignores the job" do
      state = {:fun0, fn -> :zero end}
      assert TaskWorker.handle_job(:anything, state) == {:ok, :zero, state}
    end

    test "handle_job converts task exceptions to expected errors (fun/1)" do
      state = {:fun, fn _ -> raise "nope" end}

      assert {:error, {:error, %RuntimeError{message: "nope"}}, ^state} =
               TaskWorker.handle_job(:job, state)
    end

    test "handle_job converts task exceptions to expected errors (fun/0)" do
      state = {:fun0, fn -> raise "zero nope" end}

      assert {:error, {:error, %RuntimeError{message: "zero nope"}}, ^state} =
               TaskWorker.handle_job(:job, state)
    end

    test "handle_job converts task exceptions to expected errors (MFA)" do
      state = {:mfa, {Kernel, :apply, []}}

      assert {:error, {:error, _}, ^state} = TaskWorker.handle_job(:job, state)
    end

    test "handle_job rejects a corrupted state" do
      assert {:error, {:invalid_task_state, :garbage}, :garbage} =
               TaskWorker.handle_job(:job, :garbage)
    end
  end

  describe "Telemetry middleware (exception path)" do
    test "emits an exception event before the worker crashes" do
      name = TestPool.unique_name()

      handler =
        String.to_atom("pool_exc_" <> Integer.to_string(System.unique_integer([:positive])))

      test_pid = self()

      handler_fun = fn event, _measurements, meta, _config ->
        send(test_pid, {:telemetry, event, meta})
      end

      :telemetry.attach(handler, [:super_worker, :pool, :job, :exception], handler_fun, nil)
      on_exit(fn -> :telemetry.detach(handler) end)

      {:ok, _} =
        Pool.start_link(
          name: name,
          worker: TestPool.PoisonWorker,
          size: 1,
          partitions: 1,
          retry: [max_attempts: 1],
          middleware: [SuperWorker.Pool.Middleware.Telemetry]
        )

      assert {:error, {:worker_crashed, %RuntimeError{}}} = Pool.run(name, :poison)

      assert_receive {:telemetry, [:super_worker, :pool, :job, :exception], meta}, 1_000
      assert %RuntimeError{} = meta.error
      assert meta.kind == :error

      Pool.stop(name)
    end
  end

  describe "worker process edge paths" do
    test "a worker whose init fails aborts pool start" do
      name = TestPool.unique_name()

      assert {:error, {:worker_init_failed, :cannot_start}} =
               Pool.start_link(
                 name: name,
                 worker: TestPool.FailingInitWorker,
                 size: 1,
                 partitions: 1
               )

      assert Pool.run(name, :job) == {:error, :pool_not_found}
    end

    test "a worker with an invalid init return aborts pool start" do
      name = TestPool.unique_name()

      assert {:error, {:worker_init_failed, {:invalid_init_return, {:weird, :return}}}} =
               Pool.start_link(
                 name: name,
                 worker: TestPool.WeirdInitWorker,
                 size: 1,
                 partitions: 1
               )
    end

    test "an invalid handle_job return is an expected error" do
      name = TestPool.unique_name()

      {:ok, _} =
        Pool.start_link(name: name, worker: TestPool.WeirdReturnWorker, size: 1, partitions: 1)

      assert Pool.run(name, :job) == {:error, {:invalid_handle_job_return, :just_an_atom}}
      # The worker survived and can still take jobs.
      assert Pool.run(name, :job2) == {:error, {:invalid_handle_job_return, :just_an_atom}}

      Pool.stop(name)
    end

    test "unknown messages do not disturb a worker" do
      name = TestPool.unique_name()

      {:ok, _} = Pool.start_link(name: name, task: fn job -> job end, size: 1, partitions: 1)

      assert Pool.run(name, :warmup) == {:ok, :warmup}
      send(worker_pid(name), :nonsense_message)
      assert Pool.run(name, :still_fine) == {:ok, :still_fine}

      Pool.stop(name)
    end

    test "a worker started without a reachable partition just logs and idles" do
      arg = %{
        pool: :no_such_pool_test,
        partition: 1,
        index: 1,
        impl: TaskWorker,
        impl_opts: {:fun, fn job -> job end},
        middleware: []
      }

      {:ok, worker} = Pool.WorkerProc.start_link(arg)
      on_exit(fn -> Process.exit(worker, :kill) end)

      assert Process.alive?(worker)
      Process.exit(worker, :normal)
    end
  end

  describe "middleware crash handling (fail-open)" do
    test "a crashing check_enqueue does not block submissions" do
      name = TestPool.unique_name()

      {:ok, _} =
        Pool.start_link(
          name: name,
          task: fn job -> job end,
          size: 1,
          partitions: 1,
          middleware: [TestPool.CrashingCheckMiddleware]
        )

      assert Pool.run(name, :job) == {:ok, :job}

      Pool.stop(name)
    end

    test "a crashing notify does not break the pool" do
      name = TestPool.unique_name()

      {:ok, _} =
        Pool.start_link(
          name: name,
          worker: TestPool.ErrorWorker,
          size: 1,
          partitions: 1,
          middleware: [TestPool.CrashingCheckMiddleware]
        )

      assert Pool.run(name, :job) == {:error, :boom}
      assert Pool.run(name, :job) == {:error, :boom}

      Pool.stop(name)
    end
  end

  describe "CircuitBreaker edge paths (unit)" do
    test "fails open when no breaker is registered for the partition" do
      meta = %{pool: :no_breaker_here, partition: 1}

      assert CircuitBreaker.check_enqueue(:job, meta) == :ok
      assert CircuitBreaker.notify({:error, :boom}, :job, meta) == :ok
      assert CircuitBreaker.notify(:ok, :job, meta) == :ok
    end

    test "fails open when the registered breaker is not a real breaker" do
      key = {:cb_fake_pool, {:breaker, 1}}
      {:ok, _fake} = TestPool.FakeBreaker.start(key)
      on_exit(fn -> Registry.unregister(SuperWorker.Pool.Registry, key) end)

      meta = %{pool: :cb_fake_pool, partition: 1}

      assert CircuitBreaker.check_enqueue(:job, meta) == :ok
      assert CircuitBreaker.notify({:error, :boom}, :job, meta) == :ok
    end

    test "full state machine: trip, probe limit, re-open, close" do
      meta = %{pool: :cb_unit_pool, partition: 1}

      {:ok, pid} =
        CircuitBreaker.start_link(
          pool: :cb_unit_pool,
          partition: 1,
          failure_threshold: 2,
          reset_timeout: 100
        )

      # closed: calls pass, first failure does not trip (threshold 2)
      assert CircuitBreaker.check_enqueue(:job, meta) == :ok
      CircuitBreaker.notify({:error, :boom}, :job, meta)
      Process.sleep(20)
      assert CircuitBreaker.check_enqueue(:job, meta) == :ok

      # second failure trips it
      CircuitBreaker.notify({:error, :boom}, :job, meta)
      Process.sleep(20)
      assert CircuitBreaker.check_enqueue(:job, meta) == {:error, :circuit_open}

      # failures reported while already open are ignored, timer keeps running
      CircuitBreaker.notify({:error, :boom}, :job, meta)
      Process.sleep(20)
      assert CircuitBreaker.check_enqueue(:job, meta) == {:error, :circuit_open}

      # after the reset timeout one probe is allowed, the next is rejected
      Process.sleep(150)
      assert CircuitBreaker.check_enqueue(:job, meta) == :ok
      assert CircuitBreaker.check_enqueue(:job, meta) == {:error, :circuit_open}

      # the probe fails: circuit re-opens
      CircuitBreaker.notify({:error, :boom}, :job, meta)
      Process.sleep(20)
      assert CircuitBreaker.check_enqueue(:job, meta) == {:error, :circuit_open}

      # after another reset the probe succeeds: circuit closes
      Process.sleep(150)
      assert CircuitBreaker.check_enqueue(:job, meta) == :ok
      CircuitBreaker.notify(:ok, :job, meta)
      Process.sleep(20)
      assert CircuitBreaker.check_enqueue(:job, meta) == :ok

      send(pid, :unknown_message)
      Process.sleep(10)
      assert CircuitBreaker.check_enqueue(:job, meta) == :ok

      GenServer.stop(pid)
    end
  end

  describe "partition edge paths" do
    test "duplicate worker_ready, bogus DOWN and unknown messages are ignored" do
      name = TestPool.unique_name()

      {:ok, _} = Pool.start_link(name: name, task: fn job -> job end, size: 1, partitions: 1)

      worker = worker_pid(name)
      partition = partition_pid(name)

      # Duplicate ready (already tracked), unknown DOWN, unknown message,
      # stale retry timer for a job this partition never saw.
      send(partition, {:worker_ready, worker})
      send(partition, {:DOWN, make_ref(), :process, self(), :normal})
      send(partition, :bogus)
      send(partition, {:retry_due, 999_999})

      assert Pool.run(name, :job) == {:ok, :job}

      Pool.stop(name)
    end

    test "a crash consumes the retry budget: max_attempts 1 dead-letters on first crash" do
      name = TestPool.unique_name()

      {:ok, _} =
        Pool.start_link(
          name: name,
          worker: TestPool.PoisonWorker,
          size: 1,
          partitions: 1,
          retry: [max_attempts: 1]
        )

      assert {:error, {:worker_crashed, %RuntimeError{}}} = Pool.run(name, :poison)

      Pool.stop(name)
    end

    test "an exit(:halt) crash reason is passed through unwrapped" do
      name = TestPool.unique_name()
      counter = TestPool.unique_counter()
      {:ok, _agent} = TestPool.Counter.start(counter)

      {:ok, _} =
        Pool.start_link(
          name: name,
          worker: TestPool.ExitWorker,
          worker_opts: [counter: counter],
          size: 1,
          partitions: 1,
          retry: [max_attempts: 5]
        )

      # :halt is not a {exception, stacktrace} pair — it stays as-is.
      assert Pool.run(name, :job) == {:ok, {:done, :job, 2}}

      Pool.stop(name)
    end

    test "on_failure via MFA and a crashing on_failure callback both stay safe" do
      name = TestPool.unique_name()
      on_exit(fn -> TestPool.Recorder.clean(:job) end)

      {:ok, _} =
        Pool.start_link(
          name: name,
          worker: TestPool.RetryWorker,
          worker_opts: [fails: 99],
          size: 1,
          partitions: 1,
          retry: [max_attempts: 2, backoff: {:fixed, 10}],
          on_failure: {TestPool.Recorder, :record_failure, []}
        )

      assert Pool.run(name, :job) == {:error, {:retries_exhausted, :busy}}
      assert TestPool.Recorder.failure(:job) == {:failure, :busy}

      Pool.stop(name)
    end

    test "a crashing on_failure callback does not break the partition" do
      name = TestPool.unique_name()

      {:ok, _} =
        Pool.start_link(
          name: name,
          worker: TestPool.RetryWorker,
          worker_opts: [fails: 99],
          size: 1,
          partitions: 1,
          retry: [max_attempts: 2, backoff: {:fixed, 10}],
          on_failure: fn _job, _reason -> raise "dead letter boom" end
        )

      assert Pool.run(name, :job) == {:error, {:retries_exhausted, :busy}}
      # Partition still healthy.
      assert Pool.run(name, :next) == {:error, {:retries_exhausted, :busy}}

      Pool.stop(name)
    end

    test "on_result via MFA and a crashing on_result callback both stay safe" do
      name = TestPool.unique_name()
      on_exit(fn -> TestPool.Recorder.clean(:cast_job) end)

      {:ok, _} =
        Pool.start_link(
          name: name,
          worker: TestPool.ErrorWorker,
          size: 1,
          partitions: 1,
          on_result: {TestPool.Recorder, :record_result, []}
        )

      assert Pool.cast(name, :cast_job) == :ok

      wait_until(1_000, fn -> TestPool.Recorder.result(:cast_job) != :none end)
      assert TestPool.Recorder.result(:cast_job) == {:result, {:error, :boom}}

      Pool.stop(name)
    end

    test "a crashing on_result callback does not break the partition" do
      name = TestPool.unique_name()

      {:ok, _} =
        Pool.start_link(
          name: name,
          task: fn job -> {:ok_r, job} end,
          size: 1,
          partitions: 1,
          on_result: fn _job, _result -> raise "result boom" end
        )

      assert Pool.cast(name, :job) == :ok
      # Partition still healthy after the crashing callback.
      assert Pool.run(name, :after) == {:ok, {:ok_r, :after}}

      Pool.stop(name)
    end

    test "a cast without on_result drops the result silently" do
      name = TestPool.unique_name()

      {:ok, _} =
        Pool.start_link(name: name, task: fn job -> {:cast_r, job} end, size: 1, partitions: 1)

      assert Pool.cast(name, :job) == :ok
      assert Pool.run(name, :still_alive) == {:ok, {:cast_r, :still_alive}}

      Pool.stop(name)
    end

    test "a pool whose partition is killed is rejected with :pool_unavailable" do
      name = TestPool.unique_name()

      {:ok, _} = Pool.start_link(name: name, task: fn job -> job end, size: 1, partitions: 1)

      assert Pool.run(name, :warmup) == {:ok, :warmup}
      Process.exit(partition_pid(name), :kill)

      # The top supervisor restarts the partition; depending on timing the
      # call either lands on the dead process or on the fresh one — both are
      # handled without crashing the caller.
      result = Pool.run(name, :after_kill)
      assert result == {:ok, :after_kill} or result == {:error, :pool_unavailable}

      Pool.stop(name)
    end

    test "a published config without live partitions routes to :pool_unavailable" do
      name = TestPool.unique_name()

      # Simulate the state left behind by an ungraceful pool death (e.g. a
      # node crash lost the process tree but not :persistent_term): the
      # config is published but no partition is registered.
      {:ok, config} = Pool.validate(name: name, task: fn job -> job end, size: 1, partitions: 1)
      SuperWorker.TermStorage.put({:pool, name}, config)

      assert Pool.run(name, :job) == {:error, :pool_unavailable}
      assert Pool.cast(name, :job) == {:error, :pool_unavailable}

      SuperWorker.TermStorage.delete({:pool, name})
      assert Pool.run(name, :job) == {:error, :pool_not_found}
    end
  end

  describe "Pool validation edge cases" do
    test "rejects a non-atom name" do
      assert {:error, {:invalid, {:name, "not an atom"}}} =
               Pool.start_link(name: "not an atom", task: fn job -> job end)
    end

    test "rejects a worker module that is not loaded" do
      assert {:error, {:invalid, {:task, UndefinedWorkerModule}}} =
               Pool.start_link(name: :bad_worker, worker: UndefinedWorkerModule)
    end

    test "rejects middleware entries that are not middleware modules" do
      assert {:error, {:invalid, {:middleware, ["not a module"]}}} =
               Pool.start_link(
                 name: :bad_mw2,
                 task: fn job -> job end,
                 middleware: ["not a module"]
               )
    end

    test "rejects an invalid on_failure / on_result shape by ignoring it" do
      name = TestPool.unique_name()

      {:ok, _} =
        Pool.start_link(
          name: name,
          task: fn job -> job end,
          size: 1,
          partitions: 1,
          on_failure: :not_a_callback,
          on_result: :also_not
        )

      assert Pool.run(name, :job) == {:ok, :job}

      Pool.stop(name)
    end

    test "accepts an arity-0 task" do
      name = TestPool.unique_name()

      {:ok, _} = Pool.start_link(name: name, task: fn -> :zero end, size: 1, partitions: 1)

      assert Pool.run(name, :anything) == {:ok, :zero}

      Pool.stop(name)
    end
  end

  describe "Pool runtime edge paths" do
    test "run returns {:error, :timeout} when the job outlives the caller timeout" do
      name = TestPool.unique_name()

      {:ok, _} =
        Pool.start_link(
          name: name,
          worker: TestPool.SlowWorker,
          worker_opts: [ms: 200],
          size: 1,
          partitions: 1
        )

      assert Pool.run(name, :slow, timeout: 10) == {:error, :timeout}

      # Wait for the job to finish, the pool is still healthy.
      Process.sleep(250)
      assert Pool.run(name, :next) == {:ok, :next}

      Pool.stop(name)
    end

    test "run_async against an unknown pool returns :pool_not_found" do
      assert Pool.run_async(:definitely_not_a_pool_2, :job) == {:error, :pool_not_found}
    end

    test "info against an unknown pool returns :pool_not_found" do
      assert Pool.info(:definitely_not_a_pool_3) == {:error, :pool_not_found}
    end

    test "info reports unresponsive and unregistered partitions as down" do
      name = TestPool.unique_name()

      # Ghost pool: config published, only a fake (non-responsive) partition
      # registered for partition 1; partition 2 was never registered.
      {:ok, config} = Pool.validate(name: name, task: fn job -> job end, size: 1, partitions: 2)
      SuperWorker.TermStorage.put({:pool, name}, config)
      on_exit(fn -> SuperWorker.TermStorage.delete({:pool, name}) end)

      {:ok, _fake} = TestPool.FakePartition.start({name, {:partition, 1}})

      {:ok, info} = Pool.info(name)

      assert %{id: 1, alive?: false} =
               Enum.find(info.partition_details, &(&1.id == 1))

      assert %{id: 2, alive?: false} =
               Enum.find(info.partition_details, &(&1.id == 2))
    end

    test "the pool server serves its configuration on :config" do
      name = TestPool.unique_name()

      {:ok, _} = Pool.start_link(name: name, task: fn job -> job end, size: 1, partitions: 1)

      server_pid =
        Supervisor.which_children(name)
        |> Enum.find_value(fn
          {SuperWorker.Pool.Server, pid, :worker, _modules} -> pid
          _other -> nil
        end)

      assert is_pid(server_pid)
      assert {:ok, %Pool{name: ^name}} = GenServer.call(server_pid, :config)

      Pool.stop(name)
    end
  end

  defp worker_pid(pool_name) do
    {_, worker_pid, _, _} =
      Supervisor.which_children(Pool.via(pool_name, {:sup, 1}))
      |> Enum.find(fn {id, _, _, _} -> id == {:worker, 1} end)

    worker_pid
  end

  defp partition_pid(pool_name) do
    [{pid, _}] = Registry.lookup(SuperWorker.Pool.Registry, {pool_name, {:partition, 1}})
    pid
  end

  test "worker_ready for an unknown pid is ignored and the pool keeps working" do
    name = TestPool.unique_name()

    {:ok, _} = Pool.start_link(name: name, task: fn job -> job end, size: 1, partitions: 1)

    # A worker that never registered itself: partition must ignore it.
    orphan = spawn(fn -> Process.sleep(:infinity) end)

    send(partition_pid(name), {:worker_ready, orphan})

    # Now a real registration still works and jobs flow.
    assert Pool.run(name, :job) == {:ok, :job}

    Process.exit(orphan, :kill)
    Pool.stop(name)
  end

  defp wait_until(timeout, fun) do
    if fun.() do
      :ok
    else
      if timeout <= 0, do: flunk("condition not met in time")

      Process.sleep(10)
      wait_until(timeout - 10, fun)
    end
  end
end
