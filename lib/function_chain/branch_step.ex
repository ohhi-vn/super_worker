defmodule SuperWorker.FunctionChain.BranchStep do
  @moduledoc false
  defstruct [:id, :name, routes: [], default: nil]
end
