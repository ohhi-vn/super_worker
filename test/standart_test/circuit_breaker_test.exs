defmodule SuperWorker.CircuitBreakerTest do
  use ExUnit.Case, async: true

  alias SuperWorker.CircuitBreaker

  @moduletag :capture_log

  defp unique_name do
    :"circuit_breaker_test_#{System.unique_integer([:positive])}"
  end

  describe "start/2" do
    test "starts in closed state with default options" do
      name = unique_name()
      {:ok, pid} = CircuitBreaker.start(name)
      on_exit(fn -> Process.exit(pid, :kill) end)

      {:ok, state} = CircuitBreaker.get_state(name)

      assert %CircuitBreaker{} = state
      assert state.state == :closed
      assert state.failure_count == 0
      assert state.success_count == 0
      assert state.in_flight == 0
      assert state.name == name
    end

    test "applies custom options" do
      name = unique_name()
      {:ok, pid} = CircuitBreaker.start(name, failure_threshold: 2, reset_timeout: 50)
      on_exit(fn -> Process.exit(pid, :kill) end)

      {:ok, state} = CircuitBreaker.get_state(name)

      assert state.failure_threshold == 2
      assert state.reset_timeout == 50
    end

    test "returns error when already started" do
      name = unique_name()
      {:ok, pid} = CircuitBreaker.start(name)
      on_exit(fn -> Process.exit(pid, :kill) end)

      assert {:error, {:already_started, ^pid}} = CircuitBreaker.start(name)
    end
  end

  describe "call/2 when closed" do
    setup do
      name = unique_name()
      {:ok, pid} = CircuitBreaker.start(name)
      on_exit(fn -> Process.exit(pid, :kill) end)
      %{name: name}
    end

    test "passes through ok results", %{name: name} do
      assert {:ok, :value} = CircuitBreaker.call(name, fn -> {:ok, :value} end)
    end

    test "passes through error results without opening the circuit below threshold", %{name: name} do
      assert {:error, :boom} = CircuitBreaker.call(name, fn -> {:error, :boom} end)
      {:ok, state} = CircuitBreaker.get_state(name)
      assert state.state == :closed
      assert state.failure_count == 1
    end

    test "converts raised exceptions into error tuples", %{name: name} do
      assert {:error, {:error, %RuntimeError{}}} =
               CircuitBreaker.call(name, fn -> raise "worker failed" end)

      assert {:error, {:throw, :thrown}} = CircuitBreaker.call(name, fn -> throw(:thrown) end)

      assert {:error, {:exit, :exited}} = CircuitBreaker.call(name, fn -> exit(:exited) end)

      {:ok, state} = CircuitBreaker.get_state(name)
      assert state.failure_count == 3
    end

    test "runs the function in the caller process", %{name: name} do
      caller = self()

      CircuitBreaker.call(name, fn ->
        send(caller, {:ran_in, self()})
        {:ok, :done}
      end)

      assert_receive {:ran_in, ^caller}
    end

    test "resets failure count after a success", %{name: name} do
      CircuitBreaker.call(name, fn -> {:error, :x} end)
      CircuitBreaker.call(name, fn -> {:ok, :y} end)

      {:ok, state} = CircuitBreaker.get_state(name)
      assert state.failure_count == 0
      assert is_nil(state.last_failure_time)
    end

    test "returns circuit_not_started for unknown breaker" do
      assert {:error, :circuit_not_started} =
               CircuitBreaker.call(unique_name(), fn -> {:ok, :x} end)
    end
  end

  describe "state machine" do
    test "opens after threshold failures and fails fast" do
      name = unique_name()
      {:ok, pid} = CircuitBreaker.start(name, failure_threshold: 3)
      on_exit(fn -> Process.exit(pid, :kill) end)

      # The call that trips the threshold returns the real error.
      Enum.each(1..2, fn _ ->
        assert {:error, :boom} = CircuitBreaker.call(name, fn -> {:error, :boom} end)
      end)

      assert {:error, :boom} = CircuitBreaker.call(name, fn -> {:error, :boom} end)
      {:ok, state} = CircuitBreaker.get_state(name)
      assert state.state == :open

      # Subsequent calls fail fast without executing the fun.
      refute_receive :executed

      assert {:error, :circuit_open} =
               CircuitBreaker.call(name, fn ->
                 send(self(), :executed)
                 {:ok, :never}
               end)

      {:ok, state} = CircuitBreaker.get_state(name)
      assert state.in_flight == 0
    end

    test "moves to half_open after reset timeout" do
      name = unique_name()
      {:ok, pid} = CircuitBreaker.start(name, failure_threshold: 1, reset_timeout: 100)
      on_exit(fn -> Process.exit(pid, :kill) end)

      CircuitBreaker.call(name, fn -> {:error, :boom} end)
      {:ok, open_state} = CircuitBreaker.get_state(name)
      assert open_state.state == :open

      # Still open before the reset timeout elapses.
      assert {:error, :circuit_open} = CircuitBreaker.call(name, fn -> {:ok, :nope} end)

      Process.sleep(120)

      # First successful call runs as a half_open probe.
      assert {:ok, :recovered} = CircuitBreaker.call(name, fn -> {:ok, :recovered} end)
      {:ok, state} = CircuitBreaker.get_state(name)

      # Default half_open_max_calls = 3: not closed until enough successes.
      assert state.state == :half_open
      assert state.success_count == 1
      assert state.in_flight == 0
    end

    test "closes after reaching half_open_max_calls successes" do
      name = unique_name()

      {:ok, pid} =
        CircuitBreaker.start(name,
          failure_threshold: 1,
          reset_timeout: 50,
          half_open_max_calls: 2
        )

      on_exit(fn -> Process.exit(pid, :kill) end)

      CircuitBreaker.call(name, fn -> {:error, :boom} end)
      Process.sleep(60)

      assert {:ok, :ok1} = CircuitBreaker.call(name, fn -> {:ok, :ok1} end)
      {:ok, state} = CircuitBreaker.get_state(name)
      assert state.state == :half_open

      assert {:ok, :ok2} = CircuitBreaker.call(name, fn -> {:ok, :ok2} end)
      {:ok, state} = CircuitBreaker.get_state(name)
      assert state.state == :closed
      assert state.failure_count == 0
      assert state.success_count == 0
    end

    test "re-opens immediately when a half_open probe fails" do
      name = unique_name()

      {:ok, pid} =
        CircuitBreaker.start(name,
          failure_threshold: 1,
          reset_timeout: 50,
          half_open_max_calls: 1
        )

      on_exit(fn -> Process.exit(pid, :kill) end)

      CircuitBreaker.call(name, fn -> {:error, :boom} end)
      Process.sleep(60)

      assert {:error, :still_broken} =
               CircuitBreaker.call(name, fn -> {:error, :still_broken} end)

      {:ok, state} = CircuitBreaker.get_state(name)
      assert state.state == :open
      assert state.in_flight == 0

      # Fails fast again while open.
      assert {:error, :circuit_open} = CircuitBreaker.call(name, fn -> {:ok, :x} end)
    end

    test "limits concurrent half_open probes to half_open_max_calls" do
      name = unique_name()

      {:ok, pid} =
        CircuitBreaker.start(name,
          failure_threshold: 1,
          reset_timeout: 50,
          half_open_max_calls: 1
        )

      on_exit(fn -> Process.exit(pid, :kill) end)

      CircuitBreaker.call(name, fn -> {:error, :boom} end)
      Process.sleep(60)

      # One slow probe takes the only half_open slot.
      task =
        Task.async(fn ->
          CircuitBreaker.call(name, fn ->
            Process.sleep(150)
            {:ok, :probe_ok}
          end)
        end)

      Process.sleep(20)

      # The second call cannot acquire a slot while the probe runs.
      assert {:error, :circuit_open} = CircuitBreaker.call(name, fn -> {:ok, :x} end)

      assert {:ok, :probe_ok} = Task.await(task)

      # After the successful probe the circuit stays half_open until enough
      # successes accumulate (half_open_max_calls = 1 here), so it is closed.
      {:ok, state} = CircuitBreaker.get_state(name)
      assert state.state == :closed
    end

    test "reset/1 forces the breaker back to closed" do
      name = unique_name()
      {:ok, pid} = CircuitBreaker.start(name, failure_threshold: 1)
      on_exit(fn -> Process.exit(pid, :kill) end)

      CircuitBreaker.call(name, fn -> {:error, :boom} end)
      {:ok, state} = CircuitBreaker.get_state(name)
      assert state.state == :open

      assert :ok = CircuitBreaker.reset(name)

      {:ok, state} = CircuitBreaker.get_state(name)
      assert state.state == :closed
      assert state.failure_count == 0

      assert {:ok, :works} = CircuitBreaker.call(name, fn -> {:ok, :works} end)
    end

    test "get_state/reset return not_found for unknown breakers" do
      name = unique_name()
      assert {:error, :not_found} = CircuitBreaker.get_state(name)
      assert {:error, :not_found} = CircuitBreaker.reset(name)
    end

    test "returns circuit_timeout when the breaker process does not reply" do
      name = unique_name()

      # A registered process that never handles GenServer calls.
      pid = spawn(fn -> Process.sleep(:infinity) end)
      Process.register(pid, name)
      on_exit(fn -> Process.exit(pid, :kill) end)

      assert {:error, :circuit_timeout} = CircuitBreaker.call(name, fn -> {:ok, :x} end)
    end

    test "ignores a late success report after the circuit was re-opened" do
      name = unique_name()
      {:ok, pid} = CircuitBreaker.start(name, failure_threshold: 1, reset_timeout: 50)
      on_exit(fn -> Process.exit(pid, :kill) end)

      assert {:error, :boom} = CircuitBreaker.call(name, fn -> {:error, :boom} end)
      {:ok, state} = CircuitBreaker.get_state(name)
      assert state.state == :open

      Process.sleep(60)

      me = self()

      blocker = fn tag, result ->
        receive do
          {:go, ^tag} -> result
        end
      end

      # Two half_open probes start before either finishes; the first will fail
      # and re-open the circuit, the second reports a late success.
      t1 =
        spawn(fn ->
          send(me, {:t1, CircuitBreaker.call(name, fn -> blocker.(1, {:error, :boom}) end)})
        end)

      t2 =
        spawn(fn ->
          send(me, {:t2, CircuitBreaker.call(name, fn -> blocker.(2, {:ok, :done}) end)})
        end)

      wait_until(fn ->
        match?({:ok, %{state: :half_open, in_flight: 2}}, CircuitBreaker.get_state(name))
      end)

      # The failing probe re-opens the circuit while the other is still running.
      send(t1, {:go, 1})
      assert_receive {:t1, {:error, :boom}}, 1_000

      # The late success report from the second probe is ignored.
      send(t2, {:go, 2})
      assert_receive {:t2, {:ok, :done}}, 1_000

      {:ok, state} = CircuitBreaker.get_state(name)
      assert state.state == :open
    end
  end

  defp wait_until(fun, tries \\ 50)

  defp wait_until(_fun, 0), do: flunk("condition was not met")

  defp wait_until(fun, tries) do
    if fun.() do
      :ok
    else
      Process.sleep(20)
      wait_until(fun, tries - 1)
    end
  end
end
