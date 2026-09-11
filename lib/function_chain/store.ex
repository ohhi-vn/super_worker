defmodule SuperWorker.FunctionChain.Store do
  @moduledoc """
  Checkpoint store behaviour for `SuperWorker.FunctionChain` persistence.
  """

  @callback put(SuperWorker.FunctionChain.Checkpoint.t()) :: :ok | {:error, term()}
  @callback get(run_id :: term()) ::
              {:ok, SuperWorker.FunctionChain.Checkpoint.t()} | {:error, :not_found}
  @callback delete(run_id :: term()) :: :ok
end
