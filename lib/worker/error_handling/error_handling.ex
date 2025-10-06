defmodule SuperWorker.Worker.ErrorHandling do
  @moduledoc """
  Defines the error handling callback for a `SuperWorker` worker.
  This callback is invoked when a worker process encounters an error and exits.
  """

  @doc """
  Called when a worker process exits with a reason other than `:normal`,
  `:shutdown`, or `{:shutdown, term}`.

  This allows for custom error handling, logging, or cleanup actions.
  It receives the worker's configuration and its state at the time of the error.

  ### Return Values
  - `{:ok, state}`: Acknowledges the error. The supervisor will proceed with its configured restart strategy.
  - `{:error, reason}`: Indicates a failure within the error handling logic itself.
  - `{:stop, reason}`: Instructs the supervisor to stop the worker and not attempt a restart.
  """
  @callback error(config :: map, state :: map) ::
              {:ok, map} | {:error, reason :: any} | {:stop, reason :: any}
end
