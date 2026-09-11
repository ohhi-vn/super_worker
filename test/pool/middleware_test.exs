defmodule SuperWorker.Pool.MiddlewareTest do
  @moduledoc false

  use ExUnit.Case, async: true

  alias SuperWorker.Pool

  describe "Telemetry middleware" do
    test "emits start/stop events around a successful job" do
      name = TestPool.unique_name()

      handler =
        String.to_atom("pool_tlm_" <> Integer.to_string(System.unique_integer([:positive])))

      test_pid = self()

      handler_fun = fn event, measurements, meta, _config ->
        send(test_pid, {:telemetry, event, measurements, meta})
      end

      :telemetry.attach_many(
        handler,
        [
          [:super_worker, :pool, :job, :start],
          [:super_worker, :pool, :job, :stop]
        ],
        handler_fun,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler) end)

      {:ok, _} =
        Pool.start_link(
          name: name,
          task: fn job -> {:ok_res, job} end,
          size: 1,
          partitions: 1,
          middleware: [SuperWorker.Pool.Middleware.Telemetry]
        )

      assert Pool.run(name, :job) == {:ok, {:ok_res, :job}}

      assert_receive {:telemetry, [:super_worker, :pool, :job, :start], _, meta}, 1_000
      assert meta.pool == name and meta.partition == 1 and meta.job_id != nil

      assert_receive {:telemetry, [:super_worker, :pool, :job, :stop], %{duration: duration},
                      meta},
                     1_000

      assert is_integer(duration) and duration >= 0
      assert meta.status == :ok

      Pool.stop(name)
    end

    test "emits a retry event when the worker asks for a retry" do
      name = TestPool.unique_name()

      handler =
        String.to_atom("pool_retry_" <> Integer.to_string(System.unique_integer([:positive])))

      test_pid = self()

      handler_fun = fn event, _measurements, meta, _config ->
        send(test_pid, {:telemetry, event, meta})
      end

      :telemetry.attach(handler, [:super_worker, :pool, :job, :retry], handler_fun, nil)
      on_exit(fn -> :telemetry.detach(handler) end)

      {:ok, _} =
        Pool.start_link(
          name: name,
          worker: TestPool.RetryWorker,
          worker_opts: [fails: 1],
          size: 1,
          partitions: 1,
          retry: [max_attempts: 3, backoff: {:fixed, 10}],
          middleware: [SuperWorker.Pool.Middleware.Telemetry]
        )

      assert Pool.run(name, :job) == {:ok, {:done, :job}}

      assert_receive {:telemetry, [:super_worker, :pool, :job, :retry], meta}, 1_000
      assert meta.attempts == 1
      assert meta.delay == 10
      assert meta.reason == :busy

      Pool.stop(name)
    end

    test "emits a dead_letter event when retries are exhausted" do
      name = TestPool.unique_name()

      handler =
        String.to_atom("pool_dl_" <> Integer.to_string(System.unique_integer([:positive])))

      test_pid = self()

      handler_fun = fn event, _measurements, meta, _config ->
        send(test_pid, {:telemetry, event, meta})
      end

      :telemetry.attach(handler, [:super_worker, :pool, :job, :dead_letter], handler_fun, nil)
      on_exit(fn -> :telemetry.detach(handler) end)

      {:ok, _} =
        Pool.start_link(
          name: name,
          worker: TestPool.RetryWorker,
          worker_opts: [fails: 99],
          size: 1,
          partitions: 1,
          retry: [max_attempts: 1, backoff: {:fixed, 10}],
          on_failure: fn _job, _reason -> :logged end
        )

      assert Pool.run(name, :job) == {:error, {:retries_exhausted, :busy}}

      assert_receive {:telemetry, [:super_worker, :pool, :job, :dead_letter], meta}, 1_000
      assert meta.reason == {:retries_exhausted, :busy}

      Pool.stop(name)
    end
  end

  describe "CircuitBreaker middleware" do
    test "opens after consecutive failures and rejects submissions" do
      name = TestPool.unique_name()

      {:ok, _} =
        Pool.start_link(
          name: name,
          worker: TestPool.ErrorWorker,
          size: 1,
          partitions: 1,
          middleware: [SuperWorker.Pool.Middleware.CircuitBreaker],
          circuit_breaker: [failure_threshold: 2, reset_timeout: 10_000]
        )

      assert Pool.run(name, :job1) == {:error, :boom}
      assert Pool.run(name, :job2) == {:error, :boom}

      # Circuit tripped: the job is rejected before it is queued.
      assert Pool.run(name, :job3) == {:error, :circuit_open}
      assert Pool.cast(name, :job4) == :ok
      assert Pool.run_async(name, :job5) == {:error, :circuit_open}

      Pool.stop(name)
    end

    test "the circuit recovers after the reset timeout" do
      name = TestPool.unique_name()

      {:ok, _} =
        Pool.start_link(
          name: name,
          worker: TestPool.ErrorWorker,
          size: 1,
          partitions: 1,
          middleware: [SuperWorker.Pool.Middleware.CircuitBreaker],
          circuit_breaker: [failure_threshold: 1, reset_timeout: 200]
        )

      assert Pool.run(name, :job1) == {:error, :boom}
      assert Pool.run(name, :job2) == {:error, :circuit_open}

      Process.sleep(300)

      # Half-open: one probe is let through and the job result comes back.
      assert Pool.run(name, :job3) == {:error, :boom}
      # The probe failed, so the circuit opened again.
      assert Pool.run(name, :job4) == {:error, :circuit_open}

      Pool.stop(name)
    end

    test "a success closes the circuit again" do
      name = TestPool.unique_name()
      fail = :atomics.new(1, [])

      {:ok, _} =
        Pool.start_link(
          name: name,
          task: fn job ->
            if :atomics.get(fail, 1) > 0 do
              :atomics.sub(fail, 1, 1)
              raise "boom"
            end

            {:fine, job}
          end,
          size: 1,
          partitions: 1,
          middleware: [SuperWorker.Pool.Middleware.CircuitBreaker],
          circuit_breaker: [failure_threshold: 1, reset_timeout: 200]
        )

      :atomics.add(fail, 1, 1)
      assert {:error, {:error, %RuntimeError{}}} = Pool.run(name, :job1)
      assert Pool.run(name, :job2) == {:error, :circuit_open}

      Process.sleep(250)

      # The probe succeeds this time (the flag was consumed): circuit closes.
      assert Pool.run(name, :job3) == {:ok, {:fine, :job3}}
      assert Pool.run(name, :job4) == {:ok, {:fine, :job4}}

      Pool.stop(name)
    end
  end

  test "middleware wraps execution and sees the job" do
    name = TestPool.unique_name()
    on_exit(fn -> :persistent_term.erase({:pool_test_tag, name}) end)

    {:ok, _} =
      Pool.start_link(
        name: name,
        task: fn job -> job end,
        size: 1,
        partitions: 1,
        middleware: [TestPool.TagMiddleware]
      )

    assert Pool.run(name, :job) == {:ok, :job}
    assert [:job] = :persistent_term.get({:pool_test_tag, name})

    Pool.stop(name)
  end
end
