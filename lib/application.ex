defmodule SuperWorker.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application
  require Logger

  alias SuperWorker.ConfigLoader.ConfigParser, as: Cfg

  @impl true
  @spec start(any, any) :: {:error, any} | {:ok, pid}
  def start(_type, _args) do
    Logger.debug("SuperWorker, Application, start app")

    Cfg.load()

    children = []

    Logger.debug("SuperWorker, Application, load with children: #{inspect(children)}")

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: SuperWorker.MainAppSupervisor]
    Supervisor.start_link(children, opts)
  end
end
