defmodule SuperWorker.Supervisor.Chain do
  @moduledoc """
  Documentation for `SuperWorker.Supervisor.Chain`.
  """

  @chain_params [:id, :restart_strategy]

  @chain_restart_strategies [:one_for_one, :one_for_all, :rest_for_one, :before_for_one]

  @child_params [:id, :chain_id, :restart_strategy]

  defstruct [
    :id, # chain id, unique in supervior.
    :first_worker_id, # first worker id in the chain. where the data is sent.
    :last_worker_id, # last worker id in the chain. where the data is received.
    restart_strategy: :one_for_one,
    workers: %{},
  ]

  import SuperWorker.Supervisor.Utils

  require Logger

  ## Public functions

  def check_options(opts) do
    with {:ok, opts} <- normalize_opts(opts, @chain_params),
         {:ok, opts} <- validate_restart_strategy(opts),
         {:ok, opts} <- validate_opts(opts) do
      {:ok, opts}
    end
  end

  def check_worker_options(opts) do
    with {:ok, opts} <- normalize_opts(opts, @child_params),
    {:ok, opts} <- validate_opts(opts) do
     {:ok, opts}
    end
  end

  def get_worker(chain, worker_id) do
    get_item(chain.workers, worker_id)
  end

  def worker_exists?(chain, worker_id) do
    Map.has_key?(chain.workers, worker_id)
  end

  def add_worker(chain, worker) when is_map(worker) do
    if worker_exists?(chain, worker.id) do
      {:error, :already_exists}
    else
      workers = Map.put(chain.workers, worker.id, worker)
      chain
      |> Map.put(:workers, workers)
      |> update_chain_first(worker)
      |> update_chain_last(worker)
      |> spawn_worker(worker.id)
    end
  end

  def restart_worker(chain, worker_id) do
    if worker_exists?(chain, worker_id) do

      chain
      |>  kill_worker(worker_id)
      |> spawn_worker(worker_id)
    else
      {:error, "Worker not found"}
    end
  end

  def restart_all_worker(chain) do
    workers = chain.workers
    Enum.each(workers, fn {_, worker} ->
      chain
      |> kill_worker(worker.id)
      |> spawn_worker(worker.id)
    end)
  end

  def remove_worker(chain, worker_id) do
    case get_worker(chain, worker_id) do
      {:ok, _} ->
        workers = Map.delete(chain.workers, worker_id)
        {:ok, %{chain | workers: workers}}
      {:error, _} ->
        {:error, "Worker not found"}
    end
  end

  def kill_worker(chain, worker_id) do
    case get_worker(chain, worker_id) do
      {:ok, worker} ->
        Process.exit(worker.pid, :kill)
        workers = Map.delete(chain.workers, worker_id)
        {:ok, %{chain | workers: workers}}
      {:error, _} ->
        {:error, "Worker not found"}
    end
  end

  # TO-DO: refactor this function, remove ref & pid from worker
  def kill_all_workers(chain) do
    workers = chain.workers
    Enum.each(workers, fn {_, worker} ->
      Process.exit(worker.pid, :kill)
    end)
    {:ok, %{chain | workers: %{}}}
  end

  def new_data(chain, data) do
    worker = Map.get(chain.workers, chain.first_worker_id)
    send(worker.pid, {:new_data, data})
  end

  ## Private functions

  defp update_chain_first(chain, worker) do
    if chain.first_worker_id do
      chain
    else
      Map.put(chain, :first_worker_id, worker.id)
    end
  end

  defp update_chain_last(chain, worker) do
    Map.put(chain, :last_worker_id, worker.id)
  end

  defp spawn_worker(chain, worker_id) do
    {:ok, worker} = get_worker(chain, worker_id)
    worker = do_spawn_worker(worker)
    workers = Map.put(chain.workers, worker_id, worker)

    {:ok, %{chain | workers: workers}}
  end

  defp do_spawn_worker(worker) do
    {pid, ref} = spawn_monitor(fn ->
      loop_chain(worker.mfa, worker.opts)
    end)

    worker
    |> Map.put(:pid, pid)
    |> Map.put(:ref, ref)
  end

  # Support receive data from the previous process in the chain and pass it to the next process.
  defp loop_chain(mfa, %{id: id, chain_id: chain_id} = opts) do
    receive do
      {:new_data, data} ->
        result =
          case mfa do
            {:fun, f} ->
              f.(data)
            {m, f, a} ->
              apply(m, f, [data | a])
          end
        case result do
          {:next, new_data} ->
            Logger.debug("Passing data to the next process(#{inspect(id)}), chain: #{inspect(chain_id)}")
            send(self(), {:new_data, new_data})
            loop_chain(mfa, opts)
          {:error, reason} ->
            Logger.error("Error in chain process(#{inspect(id)}), chain: #{inspect(chain_id)}: #{inspect(reason)}")
            # TO-DO: decide to ignore or stop the chain.
          {:drop, reason} ->
            Logger.debug("Dropping chain process(#{inspect(id)}), chain: #{inspect(chain_id)}: #{inspect(reason)}")
            loop_chain(mfa, opts)
          {:stop} ->
            Logger.debug("Stopping chain process(#{inspect(id)}), chain: #{inspect(chain_id)}")
        end

      {:stop, ^chain_id} ->
        Logger.debug("Stopping chain process(#{inspect(id)}), chain: #{inspect(chain_id)}")
    end
  end

  defp validate_restart_strategy(opts) do
    if opts.restart_strategy in @chain_restart_strategies do
      {:ok, opts}
    else
      {:error, "Invalid group restart strategy, #{inspect opts.restart_strategy}"}
    end
  end

  defp validate_opts(opts) do
    # TO-DO: Implement the validation
    {:ok, opts}
  end
end
