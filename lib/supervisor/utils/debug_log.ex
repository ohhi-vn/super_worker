defmodule SuperWorker.Log do
  @moduledoc """
  Conditional debug logging for SuperWorker.

  Debug output is controlled per environment via application config,
  evaluated **at compile time**:

      # config/config.exs      (production default)
      config :super_worker, debug_log: false

      # config/dev.exs or test.exs
      config :super_worker, debug_log: true

  All internal `Logger.debug/1` calls are routed through `SuperWorker.Log.debug/1`,
  which is a no-op when disabled so no message strings are built in production.
  """

  require Logger

  @enable_debug_log Application.compile_env(:super_worker, :debug_log, false)

  @doc """
  Emit a debug log if debug logging is enabled.

  Accepts the same forms as `Logger.debug/1`: a string, iodata, or a
  zero-arity anonymous function (lazy evaluation). When disabled the whole
  expression compiles away so nothing is evaluated.
  """
  defmacro debug(chardata_or_fun) do
    if @enable_debug_log do
      quote do
        Logger.debug(unquote(chardata_or_fun))
      end
    end
  end
end
