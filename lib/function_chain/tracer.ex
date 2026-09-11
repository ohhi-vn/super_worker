defmodule SuperWorker.FunctionChain.Tracer do
  @moduledoc """
  Distributed tracing adapter behaviour.

  Implementations (e.g. an OpenTelemetry adapter) create real spans from the
  chain/step/branch lifecycle. `tracing: false` (default) disables tracing
  while telemetry still fires for anyone else listening.

  Span names follow `function_chain.run`, `function_chain.step.<id>`,
  `function_chain.parallel.<name>`, `function_chain.branch.<id>`.
  """

  @callback start_span(name :: String.t(), meta :: map()) :: term()
  @callback end_span(span_ctx :: term(), result_meta :: map()) :: :ok
  @callback current_context() :: term()
end
