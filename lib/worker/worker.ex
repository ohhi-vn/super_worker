defmodule SuperWorker.Worker do
  @callback start(config :: map, state :: map) :: {:ok, map} | {:error, reason :: any}

  @callback start_link(config :: map, state :: map) :: {:ok, map} | {:error, reason :: any}

  @callback init(config :: map) :: {:ok, map} | {:error, reason :: any}

  @callback error(config :: map, state :: map) :: {:ok, map} | {:error, reason :: any} | {:stop, reason :: any}

  @callback spec(opts :: list()) :: {:ok, map} | {:error, reason :: any}

  @callback terminate(config :: map, state :: map) :: {:ok, map} | {:error, reason :: any}
end
