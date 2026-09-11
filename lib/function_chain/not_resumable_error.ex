defmodule SuperWorker.FunctionChain.NotResumableError do
  @moduledoc """
  Raised by `SuperWorker.FunctionChain.resume/2` when the chain is not
  resumable (contains non-MFA steps).
  """
  defexception [:message]
end
