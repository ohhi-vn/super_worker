defmodule SuperWorker.FunctionChainTest do
  use ExUnit.Case, async: true

  alias SuperWorker.FunctionChain, as: FC
  alias SuperWorker.FunctionChain.Store.ETS

  @moduletag :capture_log

  # ── Serial execution ──────────────────────────────────────────────────

  test "runs steps in order, threading results" do
    chain =
      FC.new()
      |> FC.add(:double, fn n -> {:ok, n * 2} end)
      |> FC.add(:inc, fn n -> {:ok, n + 1} end)

    assert {:ok, 11} = FC.run(chain, 5)
  end

  test "invokes MFA steps with value prepended to args" do
    chain = FC.new() |> FC.add(:sum, {__MODULE__, :add, [10]})

    assert {:ok, 15} = FC.run(chain, 5)
  end

  test "invokes fun with bound args" do
    chain = FC.new() |> FC.add(:sum, {&__MODULE__.add/2, [7]})

    assert {:ok, 12} = FC.run(chain, 5)
  end

  test "validates arity of fun_with_args at add time" do
    assert_raise ArgumentError, ~r/arity mismatch/, fn ->
      FC.new() |> FC.add(:bad, {&__MODULE__.add/2, [1, 2]})
    end
  end

  test "validates arity of exported MFA at add time" do
    assert_raise ArgumentError, ~r/arity mismatch/, fn ->
      FC.new() |> FC.add(:bad, {__MODULE__, :add, [1, 2]})
    end
  end

  test "raises on unrecognized fun_spec" do
    assert_raise ArgumentError, ~r/unrecognized fun_spec/, fn ->
      FC.new() |> FC.add(:bad, :not_a_fun_spec)
    end
  end

  test "unnamed steps get a generated id" do
    chain = FC.new() |> FC.add(fn n -> {:ok, n} end)

    assert [%{id: id}] = chain.steps
    assert is_reference(id)
  end

  test "3-argument named form without opts" do
    chain = FC.new() |> FC.add(:named, {__MODULE__, :add, [1]})

    assert [%{id: :named, args: [1]}] = chain.steps
    assert {:ok, 6} = FC.run(chain, 5)
  end

  # ── Error handling ────────────────────────────────────────────────────

  test ":halt stops the chain with {:error, reason}" do
    chain =
      FC.new()
      |> FC.add(:ok_step, fn n -> {:ok, n} end)
      |> FC.add(:fail, fn _ -> {:error, :boom} end)
      |> FC.add(:never, fn n -> {:ok, n} end)

    assert {:error, :boom} = FC.run(chain, 1)
  end

  test ":ignore forwards the previous value and continues" do
    chain =
      FC.new()
      |> FC.add(:first, fn n -> {:ok, n + 1} end)
      |> FC.add(:fail, fn _ -> {:error, :boom} end, on_error: :ignore)
      |> FC.add(:last, fn n -> {:ok, n * 10} end)

    assert {:ok, 20} = FC.run(chain, 1)
  end

  test ":ignore falls back to the chain default and can be overridden per step" do
    chain =
      FC.new(on_error: :ignore)
      |> FC.add(:fail, fn _ -> {:error, :boom} end)
      |> FC.add(:add, {__MODULE__, :add, [1]})

    assert {:ok, 2} = FC.run(chain, 1)
  end

  test "retry retries with the original input until success" do
    {:ok, counter} = Agent.start_link(fn -> 0 end)

    chain =
      FC.new()
      |> FC.add(
        :flaky,
        fn n ->
          Agent.update(counter, &(&1 + 1))

          if Agent.get(counter, & &1) < 3 do
            {:error, :transient}
          else
            {:ok, n + 100}
          end
        end,
        on_error: :retry,
        max_retries: 5,
        retry_delay: 0
      )

    assert {:ok, 101} = FC.run(chain, 1)
    assert 3 == Agent.get(counter, & &1)
  end

  test "retry exhaustion with on_retry_exhausted: :halt returns the error" do
    chain =
      FC.new()
      |> FC.add(:fail, fn _ -> {:error, :always} end, on_error: :retry, max_retries: 2)

    assert {:error, :always} = FC.run(chain, 1)
  end

  test "retry exhaustion with on_retry_exhausted: :ignore continues" do
    chain =
      FC.new()
      |> FC.add(:fail, fn _ -> {:error, :always} end,
        on_error: :retry,
        max_retries: 2,
        on_retry_exhausted: :ignore
      )
      |> FC.add(:add, {__MODULE__, :add, [1]})

    assert {:ok, 2} = FC.run(chain, 1)
  end

  test "invalid returns become {:error, {:invalid_return, value}}" do
    chain =
      FC.new(return_context: true)
      |> FC.add(:bad, fn _ -> :plain end)

    assert {:error, {:invalid_return, :plain}, %{step: :bad, attempts: 1}} = FC.run(chain, 1)
  end

  test "exceptions are rescued and normalized" do
    chain =
      FC.new(return_context: true)
      |> FC.add(:raise, fn _ -> raise "boom" end)

    assert {:error, {:exception, %RuntimeError{message: "boom"}, _stack}, %{step: :raise}} =
             FC.run(chain, 1)
  end

  test "exceptions can propagate with rescue_exceptions: false" do
    chain =
      FC.new(rescue_exceptions: false)
      |> FC.add(:raise, fn _ -> raise "boom" end)

    assert_raise RuntimeError, "boom", fn -> FC.run(chain, 1) end
  end

  test "return_context: false (default) returns 2-tuple errors" do
    chain = FC.new() |> FC.add(:fail, fn _ -> {:error, :boom} end)

    assert {:error, :boom} = FC.run(chain, 1)
  end

  test "return_context: true includes step context" do
    chain =
      FC.new(return_context: true)
      |> FC.add(:a, fn n -> {:ok, n} end)
      |> FC.add(:fail, fn _ -> {:error, :boom} end)

    assert {:error, :boom, %{step: :fail, index: 1, attempts: 1}} =
             FC.run(chain, 1)
  end

  # ── Conditional skipping ──────────────────────────────────────────────

  test "when: skips the step and passes the value through" do
    chain =
      FC.new()
      |> FC.add(:add, {__MODULE__, :add, [1]}, when: &(&1 > 0))
      |> FC.add(:double, fn n -> {:ok, n * 2} end)

    assert {:ok, 6} = FC.run(chain, 2)
    assert {:ok, 0} = FC.run(chain, 0)
  end

  test "unless: skips when predicate is truthy" do
    chain =
      FC.new()
      |> FC.add(:add, {__MODULE__, :add, [1]}, unless: &(&1 > 0))

    assert {:ok, 1} = FC.run(chain, 0)
    assert {:ok, 1} = FC.run(chain, 1)
  end

  test "on_skip: :replace substitutes the output" do
    chain =
      FC.new()
      |> FC.add(:add, {__MODULE__, :add, [1]},
        when: fn n -> n > 0 end,
        on_skip: {:replace, fn _ -> :replaced end}
      )

    assert {:ok, :replaced} = FC.run(chain, -1)
  end

  test "on_skip: :error flows through the normal on_error path" do
    chain =
      FC.new(return_context: true)
      |> FC.add(:add, {__MODULE__, :add, [1]}, when: fn n -> n > 0 end, on_skip: :error)

    assert {:error, {:step_skipped, :add}, %{step: :add}} = FC.run(chain, -1)
  end

  test "a raising predicate is rescued like a raising step" do
    chain =
      FC.new(return_context: true)
      |> FC.add(:add, {__MODULE__, :add, [1]}, when: fn _ -> raise "bad predicate" end)

    assert {:error, {:exception, %RuntimeError{message: "bad predicate"}, _}, %{step: :add}} =
             FC.run(chain, 1)
  end

  test "setting both :when and :unless raises at add time" do
    assert_raise ArgumentError, ~r/cannot define both/, fn ->
      FC.new()
      |> FC.add(:bad, fn n -> {:ok, n} end,
        when: fn _ -> true end,
        unless: fn _ -> false end
      )
    end
  end

  # ── Argument overrides ────────────────────────────────────────────────

  test "build-time override_args replaces args and keeps the original chain" do
    base = FC.new() |> FC.add(:add, {__MODULE__, :add, [1]})

    variant = FC.override_args(base, :add, [100])
    assert {:ok, 6} = FC.run(base, 5)
    assert {:ok, 105} = FC.run(variant, 5)
  end

  test "run-time arg_overrides do not mutate the chain" do
    chain = FC.new() |> FC.add(:add, {__MODULE__, :add, [1]})

    assert {:ok, 105} = FC.run(chain, 5, arg_overrides: %{add: {:replace, [100]}})
    assert {:ok, 6} = FC.run(chain, 5)
  end

  test "bare list override is shorthand for :replace" do
    chain = FC.new() |> FC.add(:add, {__MODULE__, :add, [1]})

    assert {:ok, 105} = FC.run(chain, 5, arg_overrides: %{add: [100]})
  end

  test ":merge strategy merges keyword args with override winning" do
    chain =
      FC.new()
      |> FC.add(:opts, {__MODULE__, :opts, [a: 1, b: 2]})

    assert {:ok, [a: 1, b: 3]} =
             FC.run(chain, 0, arg_overrides: %{opts: {:merge, [b: 3]}})
  end

  test ":merge falls back to :replace when either side is not a keyword list" do
    chain = FC.new() |> FC.add(:add, {__MODULE__, :add, [1]})

    assert {:ok, 105} = FC.run(chain, 5, arg_overrides: %{add: {:merge, [100]}})
  end

  test "unknown override keys are ignored, never raise" do
    chain = FC.new() |> FC.add(:add, {__MODULE__, :add, [1]})

    assert {:ok, 6} = FC.run(chain, 5, arg_overrides: %{unknown_step: [99], add: {:replace, [1]}})
  end

  test "dynamic markers: {:context, key} and {:call, fun}" do
    chain =
      FC.new()
      |> FC.add(
        :charge,
        {__MODULE__, :charge,
         [gateway: {:context, :gateway}, key: {:call, fn ctx -> ctx.request_id end}]}
      )

    assert {:ok, %{gateway: :stripe_test, key: "req-1", amount: 5}} =
             FC.run(chain, 5, context: %{gateway: :stripe_test, request_id: "req-1"})
  end

  test "dynamic marker {:from_step, step_id} resolves a prior step's result" do
    chain =
      FC.new()
      |> FC.add(:first, fn n -> {:ok, n * 2} end)
      |> FC.add(:second, {__MODULE__, :add, [{:from_step, :first}]})

    assert {:ok, 20} = FC.run(chain, 5)
  end

  test "dynamic markers resolve after runtime overrides" do
    chain =
      FC.new() |> FC.add(:charge2, {__MODULE__, :charge2, [gateway: {:context, :gateway}]})

    assert {:ok, %{gateway: :adyen, amount: 5}} =
             FC.run(chain, 5,
               context: %{gateway: :stripe_test},
               arg_overrides: %{charge2: {:merge, [gateway: :adyen]}}
             )
  end

  # ── Parallel execution ────────────────────────────────────────────────

  test "parallel fan-out joins as a list by default" do
    chain =
      FC.new()
      |> FC.add_parallel(:fanout, [
        {:double, FC.new() |> FC.add(:double, fn n -> {:ok, n * 2} end)},
        {:inc, FC.new() |> FC.add(:inc, fn n -> {:ok, n + 1} end)}
      ])

    assert {:ok, [10, 6]} = FC.run(chain, 5)
  end

  test "parallel join: :map keys results by branch id" do
    chain =
      FC.new()
      |> FC.add_parallel(
        :fanout,
        [
          {:double, FC.new() |> FC.add(:double, fn n -> {:ok, n * 2} end)},
          {:inc, FC.new() |> FC.add(:inc, fn n -> {:ok, n + 1} end)}
        ],
        join: :map
      )

    assert {:ok, %{double: 10, inc: 6}} = FC.run(chain, 5)
  end

  test "parallel join with a custom function" do
    chain =
      FC.new()
      |> FC.add_parallel(
        :fanout,
        [
          {:a, FC.new() |> FC.add(:a, fn n -> {:ok, n} end)},
          {:b, FC.new() |> FC.add(:b, fn n -> {:ok, n} end)}
        ],
        join: fn results -> Enum.reduce(results, 0, fn {_, {:ok, v}}, acc -> acc + v end) end
      )

    assert {:ok, 10} = FC.run(chain, 5)
  end

  test "parallel with on_branch_error: :halt_all stops on first branch error" do
    chain =
      FC.new()
      |> FC.add_parallel(:fanout, [
        {:ok_branch, FC.new() |> FC.add(:a, fn n -> {:ok, n} end)},
        {:bad_branch, FC.new() |> FC.add(:b, fn _ -> {:error, :branch_boom} end)}
      ])

    assert {:error, :branch_boom} = FC.run(chain, 5)
  end

  test "parallel with on_branch_error: :ignore_failed drops failed branches" do
    chain =
      FC.new()
      |> FC.add_parallel(
        :fanout,
        [
          {:ok_branch, FC.new() |> FC.add(:a, fn n -> {:ok, n} end)},
          {:bad_branch, FC.new() |> FC.add(:b, fn _ -> {:error, :branch_boom} end)}
        ],
        on_branch_error: :ignore_failed
      )

    assert {:ok, [5]} = FC.run(chain, 5)
  end

  test "parallel with on_branch_error: :collect_errors gathers all branch errors" do
    chain =
      FC.new()
      |> FC.add_parallel(
        :fanout,
        [
          {:bad1, FC.new() |> FC.add(:a, fn _ -> {:error, :e1} end)},
          {:bad2, FC.new() |> FC.add(:b, fn _ -> {:error, :e2} end)}
        ],
        on_branch_error: :collect_errors
      )

    assert {:error, {:branch_errors, %{bad1: :e1, bad2: :e2}}} = FC.run(chain, 5)
  end

  test "parallel step failures halt the chain with context" do
    chain =
      FC.new(return_context: true)
      |> FC.add_parallel(:fanout, [
        {:bad, FC.new() |> FC.add(:b, fn _ -> {:error, :boom} end)}
      ])
      |> FC.add(:never, fn n -> {:ok, n} end)

    assert {:error, :boom, %{step: :fanout, index: 0}} = FC.run(chain, 5)
  end

  # ── Branch execution ──────────────────────────────────────────────────

  test "branch routes exclusively on the first matching predicate" do
    big = FC.new() |> FC.add(:big, fn n -> {:ok, {:big, n}} end)
    small = FC.new() |> FC.add(:small, fn n -> {:ok, {:small, n}} end)

    chain =
      FC.new()
      |> FC.add_branch(:route, [
        {&(&1 > 10), big},
        {&(&1 > 0), small}
      ])

    assert {:ok, {:big, 100}} = FC.run(chain, 100)
    assert {:ok, {:small, 5}} = FC.run(chain, 5)
  end

  test "branch :default runs when no route matches" do
    default = FC.new() |> FC.add(:default, fn n -> {:ok, {:default, n}} end)

    chain =
      FC.new()
      |> FC.add_branch(:route, [{fn _ -> false end, FC.new()}], default: default)

    assert {:ok, {:default, 5}} = FC.run(chain, 5)
  end

  test "branch with no match and no default fails" do
    chain =
      FC.new(return_context: true)
      |> FC.add_branch(:route, [{fn _ -> false end, FC.new()}])

    assert {:error, {:no_matching_branch, 5}, %{step: :route}} = FC.run(chain, 5)
  end

  test "invalid add_branch routes raise at build time" do
    assert_raise ArgumentError, ~r/invalid branch route/, fn ->
      FC.new() |> FC.add_branch(:route, [:not_a_route])
    end
  end

  # ── Logging / Telemetry ───────────────────────────────────────────────

  test "emits telemetry events when enabled" do
    {:ok, collector} = Agent.start_link(fn -> [] end)
    ref = make_ref()

    :telemetry.attach(
      "fc-test-#{inspect(ref)}",
      [:function_chain, :run, :start],
      fn _event, _measurements, meta, _ ->
        Agent.update(collector, &[meta | &1])
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach("fc-test-#{inspect(ref)}") end)

    chain =
      FC.new(telemetry: true)
      |> FC.add(:a, fn n -> {:ok, n} end)
      |> FC.add(:b, fn _ -> {:error, :boom} end)

    assert {:error, :boom} = FC.run(chain, 1)

    metas = Agent.get(collector, & &1)
    assert [%{chain_id: id, run_id: _} | _] = metas
    assert id == chain.id
  end

  test "telemetry can be enabled per step" do
    {:ok, collector} = Agent.start_link(fn -> [] end)
    ref = make_ref()

    :telemetry.attach(
      "fc-step-test-#{inspect(ref)}",
      [:function_chain, :step, :start],
      fn _event, _measurements, meta, _ ->
        Agent.update(collector, &[meta | &1])
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach("fc-step-test-#{inspect(ref)}") end)

    chain =
      FC.new()
      |> FC.add(:a, fn n -> {:ok, n} end)
      |> FC.add(:b, fn n -> {:ok, n} end, telemetry: true)

    assert {:ok, _} = FC.run(chain, 1)

    assert [%{step_id: :b} | _] = Agent.get(collector, & &1)
  end

  test "disabled telemetry emits nothing" do
    {:ok, collector} = Agent.start_link(fn -> [] end)
    ref = make_ref()

    :telemetry.attach(
      "fc-off-test-#{inspect(ref)}",
      [:function_chain, :step, :start],
      fn _event, _measurements, _meta, _ -> Agent.update(collector, &[true | &1]) end,
      nil
    )

    on_exit(fn -> :telemetry.detach("fc-off-test-#{inspect(ref)}") end)

    chain = FC.new() |> FC.add(:a, fn n -> {:ok, n} end)
    assert {:ok, _} = FC.run(chain, 1)

    assert [] == Agent.get(collector, & &1)
  end

  # ── Persistence / resume ──────────────────────────────────────────────

  test "checkpoint written on halt, resume continues, deleted on success" do
    run_id = "resume-#{System.unique_integer([:positive])}"
    {:ok, gate} = Agent.start_link(fn -> :fail end)

    chain =
      FC.new()
      |> FC.add(:first, {__MODULE__, :add, [1]})
      |> FC.add(:guarded, {__MODULE__, :guarded, []}, on_error: :halt, args: [gate: gate])
      |> FC.add(:last, {__MODULE__, :mul10, []})

    assert {:error, :gate_closed} = FC.run(chain, 1, store: ETS, run_id: run_id)

    assert {:ok, %FC.Checkpoint{} = cp} = ETS.get(run_id)
    assert cp.step_index == 1
    assert cp.input_at_failure == 2
    assert cp.failed_reason == :gate_closed

    Agent.update(gate, fn _ -> :pass end)
    assert {:ok, 20} = FC.resume(chain, run_id: run_id, store: ETS)
    assert {:error, :not_found} = ETS.get(run_id)
  end

  test "resume mid-retry continues from the recorded attempt count" do
    run_id = "retry-resume-#{System.unique_integer([:positive])}"
    {:ok, counter} = Agent.start_link(fn -> 0 end)

    chain =
      FC.new()
      |> FC.add(:flaky, {__MODULE__, :flaky, [counter]},
        on_error: :retry,
        max_retries: 2
      )

    assert {:error, :still_failing} = FC.run(chain, 1, store: ETS, run_id: run_id)

    # 1 initial + 2 retries = 3 attempts, all failures so far.
    assert 3 == Agent.get(counter, & &1)
    assert {:ok, %FC.Checkpoint{attempt_count: 3}} = ETS.get(run_id)

    Agent.update(counter, fn _ -> 100 end)
    # Remaining budget is exhausted on resume, so the run still halts —
    # but no further invocation happens beyond the recorded attempts.
    assert {:error, :still_failing} = FC.resume(chain, run_id: run_id, store: ETS)
    assert 100 == Agent.get(counter, & &1)
    assert {:ok, %FC.Checkpoint{attempt_count: 3}} = ETS.get(run_id)
  end

  test "resume without a checkpoint returns an error" do
    chain = FC.new() |> FC.add(:a, fn n -> {:ok, n} end)

    assert {:error, {:no_checkpoint, :nope}} =
             FC.resume(chain, run_id: :nope, store: ETS)
  end

  test "resume raises for chains with non-MFA steps" do
    run_id = "closure-#{System.unique_integer([:positive])}"
    {:ok, gate} = Agent.start_link(fn -> :fail end)

    chain =
      FC.new()
      |> FC.add(:guarded, {__MODULE__, :guarded, []}, args: [gate: gate])
      |> FC.add(:closure, fn n -> {:ok, n} end)

    assert {:error, :gate_closed} = FC.run(chain, 1, store: ETS, run_id: run_id)

    assert_raise FC.NotResumableError, ~r/non-MFA steps/, fn ->
      FC.resume(chain, run_id: run_id, store: ETS)
    end
  end

  test "resume raises for nested non-MFA steps without a resolver" do
    run_id = "nested-closure-#{System.unique_integer([:positive])}"
    {:ok, gate} = Agent.start_link(fn -> :fail end)

    chain =
      FC.new()
      |> FC.add(:ok_mfa, {__MODULE__, :add, [1]})
      |> FC.add_parallel(:fanout, [
        {:branch, FC.new() |> FC.add(:closure, fn n -> {:ok, n} end)}
      ])
      |> FC.add(:guarded, {__MODULE__, :guarded, []}, args: [gate: gate])

    assert {:error, :gate_closed} = FC.run(chain, 1, store: ETS, run_id: run_id)

    assert_raise FC.NotResumableError, ~r/non-MFA steps \[:fanout, :closure\]/, fn ->
      FC.resume(chain, run_id: run_id, store: ETS)
    end
  end

  test "resume with :step_resolver rebuilds closure steps to MFA" do
    run_id = "resolver-#{System.unique_integer([:positive])}"
    {:ok, gate} = Agent.start_link(fn -> :fail end)

    chain =
      FC.new()
      |> FC.add(:first, {__MODULE__, :add, [1]})
      |> FC.add(:guarded, {__MODULE__, :guarded, []}, args: [gate: gate])
      |> FC.add(:closure, fn n -> {:ok, n + 100} end)

    assert {:error, :gate_closed} = FC.run(chain, 1, store: ETS, run_id: run_id)

    Agent.update(gate, fn _ -> :pass end)

    assert {:ok, 102} =
             FC.resume(chain,
               run_id: run_id,
               store: ETS,
               step_resolver: fn
                 :closure -> {__MODULE__, :add, [100]}
                 _ -> nil
               end
             )
  end

  test "resume with :step_resolver rebuilds nested parallel closures" do
    run_id = "nested-resolver-#{System.unique_integer([:positive])}"
    {:ok, gate} = Agent.start_link(fn -> :fail end)

    chain =
      FC.new()
      |> FC.add(:ok_mfa, {__MODULE__, :add, [1]})
      |> FC.add_parallel(:fanout, [
        {:closed, FC.new() |> FC.add(:closure, fn n -> {:ok, n * 3} end)},
        {:mfa, FC.new() |> FC.add(:inc, {__MODULE__, :add, [1]})}
      ])
      |> FC.add(:guarded, {__MODULE__, :guarded, []}, args: [gate: gate])

    assert {:error, :gate_closed} = FC.run(chain, 2, store: ETS, run_id: run_id)

    Agent.update(gate, fn _ -> :pass end)

    assert {:ok, [9, 4]} =
             FC.resume(chain,
               run_id: run_id,
               store: ETS,
               step_resolver: fn
                 :closure -> {__MODULE__, :triple, []}
                 _ -> nil
               end
             )
  end

  test "resume raises when :step_resolver returns an invalid fun_spec" do
    run_id = "bad-resolver-#{System.unique_integer([:positive])}"
    {:ok, gate} = Agent.start_link(fn -> :fail end)

    chain =
      FC.new()
      |> FC.add(:guarded, {__MODULE__, :guarded, []}, args: [gate: gate])
      |> FC.add(:closure, fn n -> {:ok, n} end)

    assert {:error, :gate_closed} = FC.run(chain, 1, store: ETS, run_id: run_id)

    assert_raise FC.NotResumableError, ~r/did not return a valid MFA/, fn ->
      FC.resume(chain, run_id: run_id, store: ETS, step_resolver: fn _ -> :not_an_mfa end)
    end
  end

  test "resume raises when :step_resolver is not a fun/1" do
    run_id = "fun-resolver-#{System.unique_integer([:positive])}"
    {:ok, gate} = Agent.start_link(fn -> :fail end)

    chain =
      FC.new()
      |> FC.add(:guarded, {__MODULE__, :guarded, []}, args: [gate: gate])
      |> FC.add(:closure, fn n -> {:ok, n} end)

    assert {:error, :gate_closed} = FC.run(chain, 1, store: ETS, run_id: run_id)

    assert_raise FC.NotResumableError, ~r/:step_resolver must be a fun\/1/, fn ->
      FC.resume(chain, run_id: run_id, store: ETS, step_resolver: :not_a_fun)
    end
  end

  test "no store configured means no checkpointing overhead" do
    chain = FC.new() |> FC.add(:fail, fn _ -> {:error, :boom} end)

    assert {:error, :boom} = FC.run(chain, 1, store: nil)
    assert {:error, :not_found} = ETS.get("never-written")
  end

  # ── Store.ETS ─────────────────────────────────────────────────────────

  test "ETS store put/get/delete" do
    ETS.ensure_table!()
    run_id = "ets-#{System.unique_integer([:positive])}"
    cp = %FC.Checkpoint{run_id: run_id, step_index: 2}

    assert :ok = ETS.put(cp)
    assert {:ok, ^cp} = ETS.get(run_id)
    assert :ok = ETS.delete(run_id)
    assert {:error, :not_found} = ETS.get(run_id)
  end

  # ── Test helpers ──────────────────────────────────────────────────────

  def add(value, extra), do: {:ok, value + extra}

  def opts(_value, {:a, a}, {:b, b}), do: {:ok, [a: a, b: b]}

  def charge(value, {:gateway, gateway}, {:key, key}) do
    {:ok, %{amount: value, gateway: gateway, key: key}}
  end

  def charge2(value, {:gateway, gateway}) do
    {:ok, %{gateway: gateway, amount: value}}
  end

  def guarded(value, {:gate, gate}) do
    case Agent.get(gate, & &1) do
      :pass -> {:ok, value}
      _ -> {:error, :gate_closed}
    end
  end

  def mul10(value), do: {:ok, value * 10}

  def triple(value), do: {:ok, value * 3}

  def flaky(_value, counter) do
    Agent.update(counter, &(&1 + 1))
    {:error, :still_failing}
  end
end
