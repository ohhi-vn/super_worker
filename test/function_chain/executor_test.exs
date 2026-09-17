defmodule SuperWorker.FunctionChain.ExecutorTest do
  use ExUnit.Case, async: true

  alias SuperWorker.FunctionChain.Executor

  describe "drop_meta/1" do
    test "drops failure metadata while preserving the complete reason" do
      reason = {:failed, %{step: :save}}
      assert Executor.drop_meta({:error, reason, %{context: :private}}) === {:error, reason}
    end

    test "preserves plain errors" do
      result = {:error, {:failed, %{step: :save}}}
      assert Executor.drop_meta(result) === result
    end

    test "preserves successful values including nested error tuples" do
      result = {:ok, {:error, :value, %{context: :data}}}
      assert Executor.drop_meta(result) === result
    end

    test "preserves other values" do
      for value <- [nil, :ok, %{}, {:ok, :value, :metadata}, {:error, :reason, :meta, :extra}] do
        assert Executor.drop_meta(value) === value
      end
    end
  end
end
