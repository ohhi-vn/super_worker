defmodule SuperWorker.FunctionChain.Store.ETS do
  @moduledoc """
  Reference in-memory checkpoint store — single node, dev/test use.
  """

  @behaviour SuperWorker.FunctionChain.Store
  @table :super_worker_function_chain_checkpoints

  @doc "Ensures the backing ETS table exists (idempotent)."
  @spec ensure_table!() :: :ok
  def ensure_table! do
    if :ets.whereis(@table) == :undefined do
      :ets.new(@table, [:named_table, :public, :set])
    end

    :ok
  end

  @impl true
  def put(checkpoint = %SuperWorker.FunctionChain.Checkpoint{run_id: run_id}) do
    ensure_table!()
    :ets.insert(@table, {run_id, checkpoint})
    :ok
  end

  @impl true
  def get(run_id) do
    ensure_table!()

    case :ets.lookup(@table, run_id) do
      [{^run_id, checkpoint}] -> {:ok, checkpoint}
      [] -> {:error, :not_found}
    end
  end

  @impl true
  def delete(run_id) do
    ensure_table!()
    :ets.delete(@table, run_id)
    :ok
  end
end
