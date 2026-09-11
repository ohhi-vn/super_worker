defmodule SuperWorker.FunctionChain.ParallelStep do
  @moduledoc false
  defstruct [
    :id,
    :name,
    branches: [],
    join: :list,
    on_branch_error: :halt_all,
    max_concurrency: System.schedulers_online(),
    timeout: 5_000
  ]
end
