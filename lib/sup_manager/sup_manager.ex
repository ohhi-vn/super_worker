defmodule SuperWorker.Supervisor.Manager do
  @doc false

  use GenServer

  alias __MODULE__


  require Logger

  ### Public API

  def start_link(opts \\ []) do
    GenServer.start_link(Manager, opts, name: __MODULE__)
  end

  ### Callbacks

  @impl true
  def init(_elements) do
    Logger.info("SuperWorker, Manager, init")

    {:ok, %{}}
  end

  @impl true
  def terminate(_reason, _state) do
    Logger.info("SuperWorker, Manager, terminate")

    :ok
  end
end
