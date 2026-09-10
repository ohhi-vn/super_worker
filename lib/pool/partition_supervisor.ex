defmodule SuperWorker.Pool.PartitionSupervisor do
  @moduledoc """
  The supervision subtree of one `SuperWorker.Pool.Partition`.

  Uses Elixir's built-in `Supervisor` with `:rest_for_one` — worker processes
  are static children (a pool has a fixed number of workers, decided at
  `start_link` time), so no `DynamicSupervisor` is needed:

      PartitionSupervisor (rest_for_one)
      ├── CircuitBreaker (optional, one per partition)
      ├── Partition (GenServer — queue, dispatch, retries)
      ├── WorkerProc 1
      ├── WorkerProc 2
      └── ...

  Restart semantics:

  - a **worker** crash restarts only that worker (its state is lost, its
    in-flight job is requeued once by the partition);
  - the **partition** crashing restarts only the partition; workers keep
    running and re-register with the new partition instance;
  - the **circuit breaker** crashing also restarts the partition after it
    (`:rest_for_one`), so both recover with consistent state;
  - the partition monitor the workers and never outlive their supervisor.
  """

  use Supervisor

  require SuperWorker.Log

  alias SuperWorker.Pool
  alias SuperWorker.Pool.{Middleware, Partition, WorkerProc}

  @doc false
  def start_link([config, id]) do
    Supervisor.start_link(__MODULE__, [config, id], name: Pool.via(config.name, {:sup, id}))
  end

  @impl true
  def init([config, id]) do
    SuperWorker.Log.debug(fn ->
      "SuperWorker, Pool, partition supervisor #{inspect(id)} starting, pool: #{inspect(config.name)}"
    end)

    children =
      breaker_children(config, id) ++
        [%{id: Partition, start: {Partition, :start_link, [[config, id]]}}] ++
        worker_children(config, id)

    Supervisor.init(children, strategy: :rest_for_one, max_restarts: 10, max_seconds: 5)
  end

  defp breaker_children(config, id) do
    if Middleware.CircuitBreaker in config.middleware do
      [
        %{
          id: Middleware.CircuitBreaker,
          start:
            {Middleware.CircuitBreaker, :start_link,
             [
               [pool: config.name, partition: id] ++ config.circuit_breaker
             ]}
        }
      ]
    else
      []
    end
  end

  defp worker_children(config, id) do
    workers = max(1, ceil(config.size / config.partitions))

    impl = impl_module(config)
    impl_opts = impl_opts(config)

    Enum.map(1..workers, fn index ->
      WorkerProc.child_spec(%{
        pool: config.name,
        partition: id,
        index: index,
        impl: impl,
        impl_opts: impl_opts,
        middleware: config.middleware
      })
    end)
  end

  defp impl_module(%{worker: module}) when is_atom(module) and module != nil, do: module
  defp impl_module(_config), do: SuperWorker.Pool.TaskWorker

  defp impl_opts(%{task: {:fun, fun}}), do: {:fun, fun}
  defp impl_opts(%{task: {:fun0, fun}}), do: {:fun0, fun}
  defp impl_opts(%{task: {:mfa, mfa}}), do: {:mfa, mfa}
  defp impl_opts(%{worker_opts: opts}) when is_list(opts), do: opts
end
