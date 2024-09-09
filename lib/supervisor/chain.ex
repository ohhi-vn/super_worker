defmodule SuperWorker.Supervisor.Chain do
  @moduledoc """
  Documentation for `SuperWorker.Supervisor.Chain`.
  """

  @chain_params [:id, :restart_strategy, :finished_callback]

  @chain_restart_strategies [:one_for_one, :one_for_all, :rest_for_one, :before_for_one]

  alias :ets, as: Ets

  defstruct [
    :id, # chain id, unique in supervior.
    :first_worker_id, # first worker id in the chain. where the data is sent.
    restart_strategy: :one_for_one,
    workers: MapSet.new(),
    supervisor: nil,
    partition: nil,
    finished_callback: nil,
    queue_length: 50,
    last_chain_order: 0,
    send_type: :random,
    data_table: nil, # ets table of supervisor.
  ]

  import SuperWorker.Supervisor.Utils

  alias SuperWorker.Supervisor, as: Sup
  alias SuperWorker.Supervisor.MapQueue
  alias SuperWorker.Supervisor.Worker

  require Logger

  ## Public functions

  def check_options(opts) do
    with {:ok, opts} <- normalize_opts(opts, @chain_params),
         {:ok, opts} <- validate_restart_strategy(opts),
         {:ok, opts} <- validate_opts(opts),
         {:ok, chain} <- map_to_struct(opts) do
      {:ok, chain}
    end
  end

  def get_worker(chain, worker_id) do
    case Ets.lookup(chain.data_table, {:worker, {:chain, chain.id}, worker_id}) do
      [{_, worker}] -> {:ok, worker}
      [] -> {:error, :not_found}
    end
  end

  def worker_exists?(chain, worker_id) do
    MapSet.member?(chain.workers, worker_id)
  end

  def add_worker(chain,  %Worker{} = worker) do
    if worker_exists?(chain, worker.id) do
      {:error, :already_exists}
    else
      if worker.num_workers == 1 do # has 1 worker per chain node.
        do_add_worker(chain, worker)
      else
        chain =
        Enum.reduce(1..worker.num_workers, chain, fn index, acc ->
          worker = Map.put(worker, :id, {:multi_workers, worker.id, index})
          {:ok, chain} = do_add_worker(acc, worker)
          chain
        end)
        Logger.debug("Added multi workers (#{inspect worker.id}) to the chain #{inspect chain.id}")
        {:ok, chain}
      end
    end
  end

  defp do_add_worker(chain, %Worker{} = worker) do
    Logger.debug("Adding worker #{inspect worker.id} to the chain #{inspect chain.id}")
    if worker_exists?(chain, worker.id) do
      {:error, :already_exists}
    else
      worker = Map.put(worker, :chain_order, chain.last_chain_order + 1)
      workers =
        case worker.id do
          {:multi_workers, root_id, 1} ->
            MapSet.put(chain.workers, root_id)
          {:multi_workers, _root_id, _index} ->
            chain.workers
          _ ->
            MapSet.put(chain.workers, worker.id)
        end

      Ets.insert(chain.data_table, {{:worker, {:chain, chain.id}, worker.id}, worker})

      chain
      |> Map.put(:workers, workers)
      |> Map.put(:last_chain_order, worker.chain_order)
      |> update_chain_first(worker)
      |> spawn_worker(worker.id)
    end
  end

  def restart_worker(chain, worker_id) do
    if worker_exists?(chain, worker_id) do
      kill_worker(chain, worker_id)
      spawn_worker(chain, worker_id)
    else
      {:error, "Worker not found"}
    end
  end

  @spec restart_all_workers(any()) :: {:error, list()} | {:ok, any()}
  def restart_all_workers(chain) do
    workers =
      Enum.map(chain.workers, fn worker_id ->
        with {:ok, worker} <- get_worker(chain, worker_id) do
          Logger.info("Restarting worker #{worker.id}, pid: #{worker.pid}")
          Process.exit(worker.pid, :kill)
          worker = do_spawn_worker(worker)
          worker.id
        end
      end)

    {:ok, %{chain | workers: workers}}
  end

  def remove_worker(chain, worker_id) do
    case get_worker(chain, worker_id) do
      {:ok, _} ->
        workers = MapSet.delete(chain.workers, worker_id)
        {:ok, %{chain | workers: workers}}
      {:error, _} ->
        {:error, "Worker not found"}
    end
  end

  def kill_worker(chain, worker_id) do
    case get_worker(chain, worker_id) do
      {:ok, worker} ->
        Process.exit(worker.pid, :kill)
        workers = MapSet.delete(chain.workers, worker_id)
        {:ok, %{chain | workers: workers}}
      {:error, _} ->
        {:error, "Worker not found"}
    end
  end

  # TO-DO: refactor this function, remove ref & pid from worker
  def kill_all_workers(chain) do
    workers = chain.workers

    Enum.each(workers, fn worker_id ->
      worker = get_worker(chain, worker_id)
      Process.exit(worker.pid, :kill)
    end)

    {:ok, %{chain | workers: %{}}}
  end

  def new_data(chain, data) do
    Registry.dispatch(chain.supervisor, {:worker, chain.first_worker_id},
    fn entries ->
      Enum.each(entries, fn {pid, _} ->
        send(pid, {:new_data, {nil, nil, data}})
      end)
    end)

  end

  ## Private functions

  defp send_next(chain, order, data) do
   if [] == Registry.lookup(chain.supervisor, {:chain_order, chain.id, order}) do
      Logger.debug("No next worker found for order #{order}, maybe the chain is finished.")

      # TO-DO: catch throw, error from outside.
      case chain.finished_callback do
        nil -> Logger.debug("No callback found for chain #{chain.id}")
        {:fun, fun} -> fun.()
        {m, f, a} -> apply(m, f, [data|a])
      end
    else
      Registry.dispatch(chain.supervisor, {:chain_order, chain.id, order},
        fn
          [{pid, _}] -> # just one worker doesn't check type.
            send(pid, {:new_data, data})
          entries ->
            case chain.send_type do
              :broadcast ->
                Enum.each(entries,
                  fn {pid, worker_id} ->
                    Logger.debug("Sending data to the next worker #{worker_id}")
                    send(pid, {:new_data, data})
                  end)
              :random ->
                {pid, _} = Enum.random(entries)
                send(pid, {:new_data, data})
            end
            Enum.each(entries, fn {pid, worker_id} ->
              Logger.debug("Sending data to the next worker #{worker_id}")
              send(pid, {:new_data, data})
            end)
        end)
      end
  end

  defp update_chain_first(chain, worker) do
    if chain.first_worker_id do
      chain
    else
      Map.put(chain, :first_worker_id, worker.id)
    end
  end

  defp spawn_worker(chain, worker_id) do
    {:ok, worker} = get_worker(chain, worker_id)
    worker =
      worker
      |> Map.put(:supervisor, chain.supervisor)
      |> Map.put(:chain_id, chain.id)
      |> Map.put(:first_worker_id, chain.first_worker_id)
      |> do_spawn_worker()

    Ets.insert(chain.data_table, {{:worker, {:chain, chain.id}, worker.id}, worker})
    Ets.insert(chain.data_table, {{:worker, :ref, worker.ref},  worker.id, worker.pid, {:chain, chain.id}})

    workers =
      case worker.id do
        {:multi_workers, root_id, 1} ->
          MapSet.put(chain.workers, root_id)
        {:multi_workers, _root_id, _index} ->
          chain.workers
        _ ->
          MapSet.put(chain.workers, worker.id)
      end

    {:ok, %{chain | workers: workers}}
  end

  defp do_spawn_worker(worker) when is_map(worker) do
    {pid, ref} = spawn_monitor(fn ->
      # Store for user can directly access to the worker.
      Process.put({:supervisor, :sup_id}, worker.supervisor)
      Process.put({:supervisor,:chain}, worker.chain_id)
      Process.put({:supervisor, :worker_id}, worker.id)

      Registry.register(worker.supervisor, {:chain, worker.chain_id}, :worker)
      Registry.register(worker.supervisor, {:chain_order, worker.chain_id, worker.chain_order}, worker.id)
      Registry.update_value(worker.supervisor, {:chain, worker.chain_id}, fn workers ->
        [worker.id | workers]
      end)

      case worker.id do
        {:multi_workers, root_id, index} ->
          # subsribe to the root worker id for get data.
          Registry.register(worker.supervisor, {:worker, root_id}, index)
          Registry.register(worker.supervisor, {:worker, worker.id}, 1)
        _ ->
          Registry.register(worker.supervisor, {:worker, worker.id}, 0)
      end

      loop_chain(%MapQueue{}, worker)
    end)

    worker
    |> Map.put(:pid, pid)
    |> Map.put(:ref, ref)
  end

  # Support receive data from the previous process in the chain and pass it to the next process.
  defp loop_chain(queue, %Worker{} = %{id: id, chain_id: chain_id} = worker) do
    receive do
      {:processed, msg_id, worker_id} ->
        Logger.debug("Worker #{inspect worker_id} processed the data, msg_id: #{msg_id}")
        {:ok, queue} = MapQueue.remove(queue, msg_id)
        loop_chain(queue, worker)
      {:new_data, {from, msg_id, data}} ->
        result =
          case worker.fun do
            {:fun, f} ->
              f.(data)
            {m, f, a} ->
              apply(m, f, [data | a])
          end
        if worker.first_worker_id != id do
          send(from, {:processed, msg_id, id})
        end
        case result do
          {:next, new_data} ->
            Logger.debug("Passing data to the next process(#{inspect(id)}), chain: #{inspect(chain_id)}")

            if MapQueue.is_full?(queue) do
              Logger.debug("Queue is full, go to loop waiting for consume last data.")
              loop_send(queue, worker)
            end

            {:ok, queue, msg_id} = MapQueue.add(queue, new_data)
            chain = Sup.get_chain(get_my_supervisor(), chain_id)

            send_next(chain, worker.chain_order + 1, {self(), msg_id, new_data})

            loop_chain(queue, worker)
          {:error, reason} ->
            Logger.error("Error in chain process(#{inspect(id)}), chain: #{inspect(chain_id)}: #{inspect(reason)}")
            # TO-DO: decide to ignore or stop the chain.
          {:drop, reason} ->
            Logger.info("Dropping chain process(#{inspect(id)}), chain: #{inspect(chain_id)}: #{inspect(reason)}")
            loop_chain(queue, worker)
          {:stop, reason} ->
            Logger.info("Stopping chain process(#{inspect(id)}), chain: #{inspect(chain_id)}")
            exit(reason)
          data ->
            Logger.debug("Passing data (default) to the next process(#{inspect(id)}), chain: #{inspect(chain_id)}")

            if MapQueue.is_full?(queue) do
              Logger.debug("Queue is full, go to loop waiting for consume last data.")
              loop_send(queue, worker)
            end

            {:ok, queue, msg_id} = MapQueue.add(queue, data)
            chain = Sup.get_chain(get_my_supervisor(), chain_id)

            send_next(chain, worker.chain_order + 1, {self(), msg_id, data})
            loop_chain(queue, worker)
        end

      {:kill, reason} ->
        Logger.debug("Killing chain process(#{inspect(id)}), chain: #{inspect(chain_id)}")
        exit(reason)

      {:stop, ^chain_id} ->
        Logger.debug("Stopping chain process(#{inspect(id)}), chain: #{inspect(chain_id)}")
    end
  end

  defp loop_send(queue, %{id: id, chain_id: chain_id} = _worker) do
    receive do
      {:processed, msg_id, worker_id} ->
        Logger.debug("Worker #{worker_id} processed the data, msg_id: #{msg_id}")
        {:ok, MapQueue.remove(queue, msg_id)}
      {:kill, reason} ->
        Logger.debug("Killing chain process(#{inspect(id)}), chain: #{inspect(chain_id)}")
        exit(reason)

      {:stop, ^chain_id} ->
        Logger.debug("Stopping chain process(#{inspect(id)}), chain: #{inspect(chain_id)}")
        :stop
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

  defp map_to_struct(opts) when is_map(opts) do
    {:ok, struct(__MODULE__, opts)}
  end

  defp get_my_chain_id() do
    Process.get({:supervisor, :chain_id})
  end

  defp get_my_supervisor() do
    Process.get({:supervisor, :sup_id})
  end
end
