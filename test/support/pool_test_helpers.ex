defmodule TestPool do
  @moduledoc false

  # Helpers for SuperWorker.Pool tests: unique pool names and small,
  # deterministic worker implementations driven by a shared counter Agent.

  def unique_name do
    String.to_atom("pool_" <> Integer.to_string(System.unique_integer([:positive])))
  end

  def unique_counter do
    String.to_atom("pool_counter_" <> Integer.to_string(System.unique_integer([:positive])))
  end
end

defmodule TestPool.Counter do
  @moduledoc false

  def start(name), do: Agent.start_link(fn -> 0 end, name: name)

  def bump(name), do: Agent.get_and_update(name, fn n -> {n + 1, n + 1} end)

  def get(name), do: Agent.get(name, & &1)
end

defmodule TestPool.CountWorker do
  @moduledoc false

  @behaviour SuperWorker.Pool.Worker

  def init(_opts), do: {:ok, 0}
  def handle_job(job, count), do: {:ok, {job, count + 1}, count + 1}
end

defmodule TestPool.RetryWorker do
  @moduledoc false

  @behaviour SuperWorker.Pool.Worker

  def init(opts), do: {:ok, %{fails: Keyword.fetch!(opts, :fails)}}

  def handle_job(job, state = %{fails: 0}), do: {:ok, {:done, job}, state}

  def handle_job(_job, state = %{fails: n}), do: {:retry, :busy, %{state | fails: n - 1}}
end

defmodule TestPool.CrashOnceWorker do
  @moduledoc false

  @behaviour SuperWorker.Pool.Worker

  def init(opts), do: {:ok, %{counter: Keyword.fetch!(opts, :counter)}}

  def handle_job(job, state = %{counter: counter}) do
    case TestPool.Counter.bump(counter) do
      1 -> raise "boom on first attempt"
      _n -> {:ok, {:done, job, TestPool.Counter.get(counter)}, state}
    end
  end
end

defmodule TestPool.PoisonWorker do
  @moduledoc false

  @behaviour SuperWorker.Pool.Worker

  def init(_opts), do: {:ok, []}
  def handle_job(_job, _state), do: raise("poison pill")
end

defmodule TestPool.ErrorWorker do
  @moduledoc false

  @behaviour SuperWorker.Pool.Worker

  def init(_opts), do: {:ok, []}
  def handle_job(_job, state), do: {:error, :boom, state}
end

defmodule TestPool.SlowWorker do
  @moduledoc false

  @behaviour SuperWorker.Pool.Worker

  def init(opts), do: {:ok, %{ms: Keyword.fetch!(opts, :ms)}}

  def handle_job(job, state = %{ms: ms}) do
    Process.sleep(ms)
    {:ok, job, state}
  end
end

defmodule TestPool.TagMiddleware do
  @moduledoc false

  @behaviour SuperWorker.Pool.Middleware

  @impl true
  def call(job, meta, next) do
    seen = :persistent_term.get({:pool_test_tag, meta.pool}, [])
    :persistent_term.put({:pool_test_tag, meta.pool}, [job | seen])
    next.(job, meta)
  end
end

defmodule TestPool.FailingInitWorker do
  @moduledoc false

  @behaviour SuperWorker.Pool.Worker

  def init(_opts), do: {:error, :cannot_start}
  def handle_job(_job, state), do: {:ok, :never, state}
end

defmodule TestPool.WeirdInitWorker do
  @moduledoc false

  @behaviour SuperWorker.Pool.Worker

  def init(_opts), do: {:weird, :return}
  def handle_job(_job, state), do: {:ok, :never, state}
end

defmodule TestPool.WeirdReturnWorker do
  @moduledoc false

  @behaviour SuperWorker.Pool.Worker

  def init(_opts), do: {:ok, []}
  def handle_job(_job, state), do: :just_an_atom
end

defmodule TestPool.ExitWorker do
  @moduledoc false

  @behaviour SuperWorker.Pool.Worker

  def init(opts), do: {:ok, %{counter: Keyword.fetch!(opts, :counter)}}

  def handle_job(job, state = %{counter: counter}) do
    case TestPool.Counter.bump(counter) do
      1 -> Process.exit(self(), :halt)
      _n -> {:ok, {:done, job, TestPool.Counter.get(counter)}, state}
    end
  end
end

defmodule TestPool.Recorder do
  @moduledoc false

  # MFA-form callbacks for on_failure / on_result, recording into
  # :persistent_term keyed by job so tests can assert on them.
  def record_failure(job, reason) do
    :persistent_term.put({:pool_test_failure, job}, {:failure, reason})
  end

  def record_result(job, result) do
    :persistent_term.put({:pool_test_result, job}, {:result, result})
  end

  def failure(job), do: :persistent_term.get({:pool_test_failure, job}, :none)

  def result(job), do: :persistent_term.get({:pool_test_result, job}, :none)

  def clean(job) do
    :persistent_term.erase({:pool_test_failure, job})
    :persistent_term.erase({:pool_test_result, job})
    :ok
  end
end

defmodule TestPool.CrashingCheckMiddleware do
  @moduledoc false

  @behaviour SuperWorker.Pool.Middleware

  @impl true
  def call(job, meta, next), do: next.(job, meta)

  # Deliberately raises: check_enqueue crashes must fail open.
  @impl true
  def check_enqueue(_job, _meta), do: raise("check middleware exploded")

  @impl true
  def notify(_outcome, _job, _meta), do: raise("notify middleware exploded")
end

defmodule TestPool.FakeBreaker do
  @moduledoc false

  # A GenServer registered under a breaker key that does not handle :check —
  # calling it must be caught by the fail-open path in CircuitBreaker.
  use GenServer

  def start(key) do
    GenServer.start(__MODULE__, key, name: {:via, Registry, {SuperWorker.Pool.Registry, key}})
  end

  @impl true
  def init(key), do: {:ok, key}
end

defmodule TestPool.FakePartition do
  @moduledoc false

  # A GenServer registered under a partition key that does not handle :info —
  # calling it must be caught by the dead-partition path in Pool.info/1.
  use GenServer

  def start(key) do
    GenServer.start(__MODULE__, key, name: {:via, Registry, {SuperWorker.Pool.Registry, key}})
  end

  @impl true
  def init(key), do: {:ok, key}
end
