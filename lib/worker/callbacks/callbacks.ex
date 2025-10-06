defmodule SuperWorker.Worker.Callbacks do
  @moduledoc """
  Defines the lifecycle callbacks for a `SuperWorker` worker.
  These callbacks are invoked by the worker's supervisor at different stages of its lifecycle.
  """

  @doc """
  Called when the worker is starting.

  It receives the configuration and the initial state.
  It should return `{:ok, state}` if the start-up is successful,
  or `{:error, reason}` to indicate a failure.
  """
  @callback start(config :: map, state :: map) :: {:ok, map} | {:error, reason :: any}

  @doc """
  Called to initialize the worker.

  This is typically the first callback invoked. It receives the initial
  configuration and should return `{:ok, initial_state}`.
  Returning `{:error, reason}` will cause the worker to fail to start.
  """
  @callback init(config :: map) :: {:ok, map} | {:error, reason :: any}

  @doc """
  Called when the worker is shutting down.

  This callback provides an opportunity for the worker to perform any
  necessary cleanup before it terminates. It receives the configuration
  and the final state of the worker. The return value is generally ignored.
  """
  @callback terminate(config :: map, state :: map) :: {:ok, map} | {:error, reason :: any}
end
