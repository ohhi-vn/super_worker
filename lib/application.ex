defmodule SuperWorker.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application
  require Logger
  require SuperWorker.Log

  alias SuperWorker.ConfigLoader.ConfigParser

  @impl true
  @spec start(any, any) :: {:error, any} | {:ok, pid} | {:ok, pid, any}
  def start(_type, _args) do
    SuperWorker.Log.debug(fn -> "SuperWorker, Application, start app" end)

    # Load supervisors' configuration and log results
    case ConfigParser.load() do
      :ok ->
        SuperWorker.Log.debug(fn ->
          "SuperWorker, Application, supervisor configurations loaded successfully"
        end)

      {:error, reason} ->
        Logger.error(
          "SuperWorker, Application, failed to load supervisor configurations: #{inspect(reason)}"
        )
    end

    # Pool Registry: partitions, their supervisors and circuit breakers
    # register here by {pool_name, kind, id} keys, so callers can resolve a
    # partition pid directly (no central dispatcher in the job path).
    children = [
      {Registry, keys: :unique, name: SuperWorker.Pool.Registry}
    ]

    SuperWorker.Log.debug(fn ->
      "SuperWorker, Application, load with children: #{inspect(children)}"
    end)

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: SuperWorker.MainAppSupervisor]
    Supervisor.start_link(children, opts)
  end
end
