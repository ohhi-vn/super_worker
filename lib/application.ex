defmodule SuperWorker.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application
  require Logger

  alias SuperWorker.Supervisor.ConfigWrapper, as: Cfg

  @impl true
  @spec start(any, any) :: {:error, any} | {:ok, pid}
  def start(_type, _args) do
    Logger.debug("start SuperWorker app")

    config = Cfg.load()

    children = [
    ]

    Logger.debug("SuperWorker application load with children: #{inspect children}")

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: SuperWorker.Supervisor]
    Supervisor.start_link(children, opts)
  end

end
