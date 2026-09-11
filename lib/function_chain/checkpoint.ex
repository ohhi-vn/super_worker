defmodule SuperWorker.FunctionChain.Checkpoint do
  @moduledoc """
  A persisted failure point of a run, used by `SuperWorker.FunctionChain.resume/2`.
  """
  defstruct [
    :chain_id,
    :run_id,
    :step_index,
    :input_at_failure,
    :accumulated_context,
    :attempt_count,
    :failed_reason,
    :inserted_at
  ]

  @type t :: %__MODULE__{}
end
