defmodule SuperWorker.FunctionChain.Step do
  @moduledoc false
  defstruct [
    :id,
    :name,
    :fun_spec,
    :type,
    args: [],
    on_error: nil,
    max_retries: nil,
    retry_delay: nil,
    on_retry_exhausted: :halt,
    when: nil,
    unless: nil,
    on_skip: :pass_through,
    log: nil,
    telemetry: nil
  ]
end
