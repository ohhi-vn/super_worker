defmodule SuperWorker.Supervisor.ChainConfigTest do
  use ExUnit.Case, async: true

  alias SuperWorker.Supervisor.Chain
  alias SuperWorker.Supervisor.Chain.Config

  describe "new/1" do
    test "creates a chain struct from valid options" do
      {:ok, chain} =
        Config.new(
          id: :chain1,
          restart_strategy: :one_for_all,
          send_type: :broadcast,
          queue_length: 10,
          finished_callback: {:fun, fn _data -> :ok end}
        )

      assert %Chain{} = chain
      assert chain.id == :chain1
      assert chain.restart_strategy == :one_for_all
      assert chain.send_type == :broadcast
      assert chain.queue_length == 10
    end

    test "rejects invalid restart strategy" do
      assert {:error, {:invalid_restart_strategy, :bogus}} =
               Config.new(id: :chain1, restart_strategy: :bogus)
    end

    test "rejects invalid send type" do
      assert {:error, {:invalid_send_type, :smoke_signals}} =
               Config.new(id: :chain1, send_type: :smoke_signals)
    end

    test "rejects invalid callback" do
      assert {:error, {:invalid_callback, "not a callback"}} =
               Config.new(id: :chain1, finished_callback: "not a callback")
    end

    test "rejects out of range queue length" do
      assert {:error, {:invalid_queue_length, 0}} = Config.new(id: :chain1, queue_length: 0)
    end

    test "rejects unknown options" do
      assert {:error, {:invalid_options, [:nope]}} = Config.new(id: :chain1, nope: true)
    end

    test "accepts mfa callbacks" do
      {:ok, chain} = Config.new(id: :chain1, finished_callback: {String, :length, []})
      assert chain.finished_callback == {String, :length, []}
    end
  end
end
