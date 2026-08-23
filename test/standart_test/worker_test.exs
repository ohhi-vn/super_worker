defmodule SuperWorker.Supervisor.WorkerTest do
  use ExUnit.Case, async: true

  alias SuperWorker.Supervisor.Worker

  describe "from_config/1" do
    test "creates a standalone worker from an mfa" do
      {:ok, worker} =
        Worker.from_config(type: :standalone, id: :w1, fun: {MyTest, :loop, [:w1]})

      assert %Worker{} = worker
      assert worker.id == :w1
      assert worker.fun == {MyTest, :loop, [:w1]}
      assert worker.type == :standalone
      # Defaults are applied.
      assert worker.restart_strategy == Worker.default_restart_strategy()
      assert worker.num_workers == 1
    end

    test "creates a group worker from an anonymous function" do
      {:ok, worker} = Worker.from_config(type: :group, parent: :g1, fun: {:fun, fn -> :ok end})

      assert worker.type == :group
      assert worker.parent == :g1
      assert match?({:fun, _}, worker.fun)

      # Missing ids get a generated random one.
      assert is_binary(worker.id) and byte_size(worker.id) == 32
    end

    test "creates a chain worker" do
      # Note: :order is not an accepted option; chains assign orders when the
      # node is added to the chain.
      {:ok, worker} =
        Worker.from_config(type: :chain, parent: :c1, fun: {MyTest, :task, [5]})

      assert worker.type == :chain
      assert worker.parent == :c1
    end

    test "accepts gen_server funs" do
      {:ok, worker} =
        Worker.from_config(
          type: :standalone,
          id: :gs,
          fun: {:gen_server, {MyGenServer, :start_link, [[]]}}
        )

      assert match?({:gen_server, {MyGenServer, :start_link, _}}, worker.fun)
    end

    test "rejects an unknown type" do
      assert {:error, "invalid type: " <> _} =
               Worker.from_config(type: :swarm, id: :w1, fun: {MyTest, :loop, [1]})
    end

    test "rejects unknown options per type" do
      assert {:error, {:invalid_options, [:bogus_option]}} =
               Worker.from_config(
                 type: :standalone,
                 id: :w1,
                 fun: {MyTest, :loop, [1]},
                 bogus_option: 1
               )
    end

    test "rejects invalid restart strategy" do
      assert {:error, "Invalid standalone restart strategy, :sometimes"} =
               Worker.from_config(
                 type: :standalone,
                 id: :w1,
                 fun: {MyTest, :loop, [1]},
                 restart_strategy: :sometimes
               )
    end

    test "rejects invalid field values" do
      assert {:error, [{:error, {:invalid, {:name, "not_an_atom"}}}]} =
               Worker.from_config(
                 type: :group,
                 id: :w1,
                 parent: :g1,
                 fun: {MyTest, :loop, [1]},
                 name: "not_an_atom"
               )
    end

    test "a missing fun silently produces a nil fun (struct/2 bypasses enforce_keys)" do
      # Documents current behaviour: validation does not require :fun.
      assert {:ok, %{fun: nil}} = Worker.from_config(type: :standalone, id: :no_fun)
    end

    test "rejects malformed funs" do
      for bad <- ["string", {123}, {:not_a_module, 1}] do
        assert {:error, _} = Worker.from_config(type: :standalone, id: :w1, fun: bad)
      end
    end
  end
end
