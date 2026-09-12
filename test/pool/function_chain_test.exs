defmodule SuperWorker.Pool.FunctionChainTest do
  @moduledoc false

  use ExUnit.Case, async: true

  alias SuperWorker.FunctionChain
  alias SuperWorker.Pool
  alias SuperWorker.Pool.FunctionChain, as: ChainWorker

  defp start_pool(worker_opts \\ []) do
    name = TestPool.unique_name()

    chain =
      FunctionChain.new()
      |> FunctionChain.add(:double, fn n -> {:ok, n * 2} end)
      |> FunctionChain.add(:stringify, fn n -> {:ok, "n=#{n}"} end)

    {:ok, _} =
      Pool.start_link(
        [name: name, worker: ChainWorker, worker_opts: [chain: chain], size: 2, partitions: 2] ++
          worker_opts
      )

    {name, chain}
  end

  test "runs a chain for every job" do
    {name, _} = start_pool()

    assert Pool.run(name, 2) == {:ok, "n=4"}
    assert Pool.run(name, 5) == {:ok, "n=10"}

    Pool.stop(name)
  end

  test "a failed chain run is an expected error and fires :on_failure" do
    name = TestPool.unique_name()
    job = {:failing, make_ref()}

    chain =
      FunctionChain.new()
      |> FunctionChain.add(:fail, fn _ -> {:error, :not_allowed} end)

    {:ok, _} =
      Pool.start_link(
        name: name,
        worker: ChainWorker,
        worker_opts: [chain: chain, run_opts: [return_context: true]],
        size: 1,
        partitions: 1,
        on_failure: {TestPool.Recorder, :record_failure, []}
      )

    # return_context meta is stripped: the caller sees a plain reason.
    assert {:error, :not_allowed} = Pool.run(name, job)
    assert {:failure, :not_allowed} = TestPool.Recorder.failure(job)

    Pool.stop(name)
  end

  test "on_error: :retry hands the failure to the pool's retry budget" do
    name = TestPool.unique_name()
    counter = TestPool.unique_counter()
    {:ok, _} = TestPool.Counter.start(counter)

    chain =
      FunctionChain.new()
      |> FunctionChain.add(:first_only, fn n ->
        if TestPool.Counter.bump(counter) == 1, do: {:error, :not_ready}, else: {:ok, n * 10}
      end)

    {:ok, _} =
      Pool.start_link(
        name: name,
        worker: ChainWorker,
        worker_opts: [chain: chain, on_error: :retry],
        size: 1,
        partitions: 1,
        retry: [max_attempts: 5, backoff: {:fixed, 10}]
      )

    # The first run fails, the second (a pool retry, not a chain retry — the
    # chain has no retry config) succeeds.
    assert {:ok, 30} = Pool.run(name, 3)
    assert TestPool.Counter.get(counter) == 2

    Pool.stop(name)
  end

  test "a raising chain is an expected error, not a worker crash" do
    name = TestPool.unique_name()

    chain =
      FunctionChain.new()
      |> FunctionChain.add(:boom, fn job ->
        if job == :crash, do: raise("kaboom")
        {:ok, {:handled, job}}
      end)

    {:ok, _} =
      Pool.start_link(
        name: name,
        worker: ChainWorker,
        worker_opts: [chain: chain],
        size: 1,
        partitions: 1
      )

    assert {:error, {:exception, %RuntimeError{message: "kaboom"}, _stacktrace}} =
             Pool.run(name, :crash)

    # The worker survived: further jobs still run.
    assert {:ok, {:handled, :fine}} = Pool.run(name, :fine)

    Pool.stop(name)
  end

  test "chain as a fun/0 builds a fresh chain per job" do
    name = TestPool.unique_name()

    chain_fun = fn ->
      FunctionChain.new() |> FunctionChain.add(:echo, fn x -> {:ok, {:echo, x}} end)
    end

    {:ok, _} =
      Pool.start_link(
        name: name,
        worker: ChainWorker,
        worker_opts: [chain: chain_fun],
        size: 1,
        partitions: 1
      )

    assert Pool.run(name, :a) == {:ok, {:echo, :a}}
    assert Pool.run(name, :b) == {:ok, {:echo, :b}}

    Pool.stop(name)
  end

  test "run_opts as a fun/1 provides per-job arg overrides" do
    name = TestPool.unique_name()

    chain =
      FunctionChain.new()
      |> FunctionChain.add(
        :greet,
        {fn value, greeting -> {:ok, "#{greeting}, #{value}!"} end, ["Hi"]}
      )

    {:ok, _} =
      Pool.start_link(
        name: name,
        worker: ChainWorker,
        worker_opts: [
          chain: chain,
          run_opts: fn _job -> [arg_overrides: %{greet: {:replace, ["Hello"]}}] end
        ],
        size: 1,
        partitions: 1
      )

    assert Pool.run(name, "Ada") == {:ok, "Hello, Ada!"}

    Pool.stop(name)
  end

  test "invalid worker_opts fail init" do
    assert {:error, {:missing, :chain}} = ChainWorker.init([])
    assert {:error, {:invalid, {:chain, :not_a_chain}}} = ChainWorker.init(chain: :not_a_chain)

    assert {:error, {:invalid, {:on_error, :boom}}} =
             ChainWorker.init(on_error: :boom, chain: FunctionChain.new())

    assert {:error, {:invalid, {:run_opts, :bad}}} =
             ChainWorker.init(chain: FunctionChain.new(), run_opts: :bad)
  end
end
