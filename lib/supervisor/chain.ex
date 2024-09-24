defmodule SuperWorker.Supervisor.Chain do
  @moduledoc """
  Documentation for `SuperWorker.Supervisor.Chain`.
  """

  @chain_params [:id, :restart_strategy, :finished_callback]

  @chain_restart_strategies [:one_for_one, :one_for_all, :rest_for_one, :before_for_one]

  alias :ets, as: Ets
  alias __MODULE__

  defstruct [
    :id, # chain id, unique in supervior.
    :first_worker_id, # first worker id in the chain. where the data is sent.
    restart_strategy: :one_for_one,
    supervisor: nil,
    partition: nil,
    finished_callback: nil,
    queue_length: 50,
    send_type: :random,
    data_table: nil, # ets table of supervisor.
  ]

  @type t :: %__MODULE__{
    id: any,
    first_worker_id: any,
    restart_strategy: atom,
    supervisor: atom,
    partition: atom,
    finished_callback: nil | {:fun, fun} | {module, atom, [any]},
    queue_length: non_neg_integer,
    send_type: :broadcast | :random,
    data_table: atom
  }

  import SuperWorker.Supervisor.Utils

  alias SuperWorker.Supervisor, as: Sup
  alias SuperWorker.Supervisor.{Worker, Message, MapQueue}

  require Logger

  ## Public functions

  @spec check_options([atom() | keyword()]) :: {:error, atom | {atom, any}} | {:ok, Chain.t}
  def check_options(opts) do
    with {:ok, opts} <- normalize_opts(opts, @chain_params),
         {:ok, opts} <- validate_restart_strategy(opts),
         {:ok, opts} <- validate_opts(opts),
         {:ok, chain} <- map_to_struct(opts) do
      {:ok, chain}
    end
  end

  @spec get_worker(Chain.t, any()) :: {:error, :not_found} | {:ok, Worker.t}
  def get_worker(chain, worker_id) do
    case Ets.lookup(chain.data_table, {:worker, {:chain, chain.id}, worker_id}) do
      [{_, worker}] -> {:ok, worker}
      [] -> {:error, :not_found}
    end
  end

  @spec worker_exists?(Chain.t, any()) :: boolean()
  def worker_exists?(chain, worker_id) do
    case get_worker(chain, worker_id) do
      {:ok, _} -> true
      {:error, _} -> false
    end
  end

  @spec get_all_workers(Chain.t) :: {:ok, list(Worker.t)}
  def get_all_workers(chain) do
    result =
    Ets.match(chain.data_table, {{:worker, {:chain, chain.id}, :_}, :"$1"})
    |> List.flatten()
    {:ok, result}
  end

  @spec add_worker(Chain.t, Worker.t) :: {:error, :already_exists} | {:ok, Chain.t}
  def add_worker(chain,  %Worker{} = worker) do
    if worker_exists?(chain, worker.id) do
      {:error, :already_exists}
    else
      worker =
        worker
        |> Map.put(:order, get_and_update_chain_order(chain))
        |> Map.put(:parent, chain.id)

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

  @spec do_add_worker(Chain.t, Worker.t) :: {:error, :already_exists} | {:ok, Chain.t}
  defp do_add_worker(chain, %Worker{} = worker) do
    Logger.debug("Adding worker #{inspect worker.id} to the chain #{inspect chain.id}")
    if worker_exists?(chain, worker.id) do
      {:error, :already_exists}
    else
      Ets.insert(chain.data_table, {{:worker, {:chain, chain.id}, worker.id}, worker})

      chain
      |> update_chain_first(worker)
      |> spawn_worker(worker.id)
    end
  end

  @spec restart_worker(Chain.t, any()) :: {:error, any} | {:ok, Chain.t}
  def restart_worker(chain, worker_id) do
    if worker_exists?(chain, worker_id) do
      kill_worker(chain, worker_id)
      spawn_worker(chain, worker_id)
    else
      {:error, "Worker not found"}
    end
  end

  @spec restart_all_workers(Chain.t) :: {:ok, Chain.t}
  # TO-DO: support restart workers depend on host partition.
  def restart_all_workers(chain) do
      workers =
        Ets.match_object(chain.data_table, {:worker, {:chain, chain.id}, :_})
        |> Enum.map(fn {_, worker} -> worker end)

      Enum.map(workers,
        fn worker ->
          Logger.info("Restarting worker #{worker.id}, pid: #{worker.pid}")
          Process.exit(worker.pid, :kill)
          worker = do_spawn_worker(worker)
          worker.id
      end)

    {:ok, chain}
  end

  @spec remove_worker(Chain.t, any()) :: {:error, any} | {:ok, Chain.t}
  def remove_worker(chain, worker_id) do
    case get_worker(chain, worker_id) do
      {:ok, _} ->
        Ets.delete(chain.data_table, {:worker, {:chain, chain.id}, worker_id})
        # TO-DO: remove other info of worker.
        {:ok, chain}
      {:error, reason} = error ->
        Logger.error("Failed to remove worker #{inspect(worker_id)} in chain #{inspect chain.id}, error: #{inspect reason}")
        error
    end
  end


  @spec kill_worker(Chain.t, any()) :: {:error, any} | {:ok, Chain.t}
  def kill_worker(chain, worker_id) do
    case get_worker(chain, worker_id) do
      {:ok, worker} ->
        Process.exit(worker.pid, :kill)
        {:ok, chain}
      {:error, reason} = error ->
        Logger.error("Failed to kill worker #{inspect(worker_id)} in chain #{inspect chain.id}, error: #{inspect reason}")
        error
    end
  end

  @spec kill_all_workers(Chain.t) :: {:ok, Chain.t}
  # TO-DO: refactor this function, remove ref & pid from worker
  def kill_all_workers(chain) do
    workers = chain.workers

    Enum.each(workers, fn worker_id ->
      worker = get_worker(chain, worker_id)
      Process.exit(worker.pid, :kill)
    end)

    {:ok, chain}
  end

  @spec new_data(Chain.t, Message.t) :: any
  def new_data(chain = %Chain{}, msg = %Message{}) do
    send_next(chain, 1, msg)
  end

  ## Private functions

  @spec send_next(Chain.t, non_neg_integer, Message.t) :: any
  defp send_next(chain = %Chain{}, order, msg = %Message{}) do
   case Registry.lookup(chain.supervisor, {:chain_order, chain.id, order}) do
    [] ->
      Logger.debug("No next worker found for order #{order}, chain: #{chain.id}, go to finished callback.")
      # TO-DO: catch throw, error from outside.
      case chain.finished_callback do
        nil ->
          Logger.debug("No callback found for chain #{chain.id}")
          {:error, :no_worker_or_callback}
        {:fun, fun} ->
          fun.(msg.data)
          {:ok, :call_back}
        {m, f, a} ->
          apply(m, f, [msg.data|a])
          {:ok, :call_back}
      end

    [{pid, worker_id}] -> # just one worker doesn't check type.
     Logger.debug("#{inspect chain.id}, order: #{order}, found a next worker: #{inspect worker_id}, send msg #{inspect msg.id}")
      send(pid, {:new_data, msg})
      {:ok, :send_one}
    [_|_] = entries ->
      Logger.debug("#{inspect chain.id}, order: #{order}, found next workers: #{inspect entries}")

      case chain.send_type do
        :broadcast ->
          Enum.each(entries,
            fn {pid, worker_id} ->
              Logger.debug("Sending data to the next worker #{worker_id}")
              send(pid, {:new_data, msg})
            end)
          {:ok, :send_all}
        :random ->
          {pid, _} = Enum.random(entries)
          send(pid, {:new_data, msg})
          Logger.debug("Sending data to the next worker #{inspect pid} (random)")
          {:ok, :send_random}
        end
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
      |> Map.put(:first_worker_id, chain.first_worker_id)
      |> do_spawn_worker()

    Ets.insert(chain.data_table, {{:worker, {:chain, chain.id}, worker.id}, worker})
    Ets.insert(chain.data_table, {{:worker, :ref, worker.ref},  worker.id, worker.pid, {:chain, chain.id}})

    {:ok, chain}
  end

  defp do_spawn_worker(%Worker{} = worker) do
    {pid, ref} = spawn_monitor(fn ->
      # Store for user can directly access to the worker.
      Process.put({:supervisor, :sup_id}, worker.supervisor)
      Process.put({:supervisor,:chain}, worker.parent)
      Process.put({:supervisor, :worker_id}, worker.id)

      Registry.register(worker.supervisor, {:chain, worker.parent}, :worker)
      Registry.register(worker.supervisor, {:chain_order, worker.parent, worker.order}, worker.id)

      case worker.id do
        {:multi_workers, root_id, index} ->
          # subsribe to the root worker id for get data.
          Registry.register(worker.supervisor, {:worker, {:chain, worker.parent}, root_id}, index)
         # Registry.register(worker.supervisor, {:worker, worker.id}, 1)
        _ ->
          Registry.register(worker.supervisor, {:worker, {:chain, worker.parent}, worker.id}, 0)
      end

      loop_chain(%MapQueue{}, worker)
    end)

    # Link to child for case supervisor is down.
    # TO-DO: Improve case worker crash immediately.
    Process.link(pid)

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
      {:new_data, msg = %Message{}} ->
        result =
          # TO-DO: catch throw, error from outside.
          case worker.fun do
            {:fun, f} ->
              f.(msg.data)
            {m, f, a} ->
              apply(m, f, [msg.data | a])
          end

        if worker.first_worker_id != id do
          send(msg.from, {:processed, msg.id, id})
        end

        case result do
          {:next, new_data} ->
            if MapQueue.is_full?(queue) do
              Logger.debug("worker #{inspect(id)}, queue is full, go to loop waiting for consume last data.")
              loop_send(queue, worker)
            end

            Logger.debug("worker #{inspect(id)}, passing data to the next process, chain: #{inspect(chain_id)}")

            {:ok, queue, msg_id} = MapQueue.add(queue, new_data)
            {:ok,chain} = Sup.get_chain(get_my_supervisor(), chain_id)

            msg = Message.new(self(), nil, new_data, msg_id)
            send_next(chain, worker.order + 1, msg)

            loop_chain(queue, worker)
          {:error, reason} ->
            Logger.error("worker #{inspect(id)}, error in chain process, chain: #{inspect(chain_id)}: #{inspect(reason)}")
            # TO-DO: decide to ignore or stop the chain.
          {:drop, reason} ->
            Logger.info("worker #{inspect(id)}, dropping chain process, chain: #{inspect(chain_id)}: #{inspect(reason)}")
            loop_chain(queue, worker)
          {:stop, reason} ->
            Logger.info("worker #{inspect(id)}, stopping chain process, chain: #{inspect(chain_id)}")
            exit(reason)
          data ->
            Logger.debug("worker #{inspect(id)}, passing data (default) to the next process, chain: #{inspect(chain_id)}")

            if MapQueue.is_full?(queue) do
              Logger.debug("worker #{inspect(id)}, queue is full, go to loop waiting for consume last data.")
              loop_send(queue, worker)
            end

            {:ok, queue, msg_id} = MapQueue.add(queue, data)
            chain = Sup.get_chain(get_my_supervisor(), chain_id)

            msg = Message.new(self(), nil, data, msg_id)
            send_next(chain, worker.order + 1, msg)
            loop_chain(queue, worker)
        end

      {:kill, reason} ->
        Logger.debug("worker #{inspect(id)}, killing chain, chain: #{inspect(chain_id)}")
        exit(reason)

      {:stop, ^chain_id} ->
        Logger.debug("worker #{inspect(id)}, stopping chain, chain: #{inspect(chain_id)}")
    end
  end

  defp loop_send(queue, %{id: id, chain_id: chain_id} = _worker) do
    receive do
      {:processed, msg_id, worker_id} ->
        Logger.debug("Worker #{worker_id} processed the data, msg_id: #{msg_id}")
        {:ok, MapQueue.remove(queue, msg_id)}
      {:kill, reason} ->
        Logger.debug("Worker #{id}, killing chain, chain: #{inspect(chain_id)}")
        exit(reason)

      {:stop, ^chain_id} ->
        Logger.debug("Worker #{id}, stopping chain process, chain: #{inspect(chain_id)}")
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

  defp get_and_update_chain_order(chain) do
    Ets.update_counter(chain.data_table, {:last_chain_order, chain.id}, {2, 1}, {{:last_chain_order, chain.id}, 0})
  end
end
