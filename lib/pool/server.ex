defmodule SuperWorker.Pool.Server do
  @moduledoc """
  Housekeeping process of a `SuperWorker.Pool`.

  Holds the pool configuration and publishes it to `SuperWorker.TermStorage`
  so the public API (`run`/`cast`/`info`) can resolve partitions without
  calling into a central process — dispatch stays fully partitioned, with no
  single bottleneck in front of the pool. On graceful shutdown it deletes the
  published entry.

  This is not a dispatcher: it never sits in the job path.
  """

  use GenServer

  require SuperWorker.Log

  alias SuperWorker.TermStorage

  @doc false
  def start_link(config) do
    GenServer.start_link(__MODULE__, config)
  end

  @impl true
  def init(config) do
    # Trap exits so terminate/2 runs when the pool supervisor stops us:
    # it removes the published TermStorage entry.
    Process.flag(:trap_exit, true)

    TermStorage.put({:pool, config.name}, config)

    SuperWorker.Log.debug(fn ->
      "SuperWorker, Pool, server started, pool: #{inspect(config.name)}"
    end)

    {:ok, config}
  end

  @impl true
  def handle_call(:config, _from, config), do: {:reply, {:ok, config}, config}

  @impl true
  def terminate(_reason, config) do
    TermStorage.delete({:pool, config.name})
    :ok
  end
end
