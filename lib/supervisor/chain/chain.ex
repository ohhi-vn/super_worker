defmodule SuperWorker.Supervisor.Chain do
  @moduledoc """
  Documentation for `SuperWorker.Supervisor.Chain`.
  """

  @chain_params [:id, :restart_strategy, :finished_callback, :queue_length, :send_type]

  @chain_restart_strategies [:one_for_one, :one_for_all, :rest_for_one, :before_for_one]

  @send_types [:broadcast, :random, :partition, :round_robin]

  defstruct [
    # chain id, unique in supervior.
    :id,
    restart_strategy: :one_for_one,
    supervisor: nil,
    partition: nil,
    finished_callback: nil,
    queue_length: 50,
    # :broadcast, :random, :partition, :round_robin
    send_type: :random
  ]

  @type t :: %__MODULE__{
          id: any,
          restart_strategy: atom,
          supervisor: atom,
          partition: atom,
          finished_callback: nil | {:fun, fun} | {module, atom, [any]},
          queue_length: non_neg_integer,
          send_type: :broadcast | :random | :partition | :round_robin
        }

  alias SuperWorker.Supervisor, as: Sup
  alias SuperWorker.Supervisor.{Worker, Db, Validator, Message, MapQueue}

  alias __MODULE__

  require Logger

  ## Public functions

  @spec check_options([atom() | keyword()]) :: {:error, atom | {atom, any}} | {:ok, Chain.t()}
  def check_options(opts) do
    with {:ok, opts} <- Validator.normalize_options(opts, @chain_params),
         {:ok, chain} <- map_to_struct(opts),
         {:ok, chain} <- validate_opts(chain) do
      {:ok, chain}
    end
  end

  @spec get_worker(Chain.t(), any()) :: {:error, :worker_not_found} | {:ok, Worker.t()}
  def get_worker(chain = %Chain{}, worker_id) do
    Logger.debug(
      "SuperWorker, Chain, get_worker: #{inspect(chain.supervisor)}, #{inspect(worker_id)}"
    )

    Db.get_worker_info(chain.supervisor, worker_id, {:chain, chain.id})
  end

  @spec worker_exists?(Chain.t(), any()) :: boolean()
  def worker_exists?(chain = %Chain{}, worker_id) do
    case get_worker(chain, worker_id) do
      {:ok, _} -> true
      {:error, _} -> false
    end
  end

  @spec get_all_workers(Chain.t()) :: {:ok, list(Worker.t())}
  def get_all_workers(chain = %Chain{}) do
    Logger.debug("SuperWorker, Chain, get_all_workers: #{inspect(chain.supervisor)}")

    Db.get_worker_infos_by_parent(chain.supervisor, {:chain, chain.id})
  end

  @spec add_worker(Chain.t(), Worker.t()) :: {:error, :already_exists} | {:ok, Chain.t()}
  def add_worker(chain = %Chain{}, worker = %Worker{}) do
    if worker_exists?(chain, worker.id) do
      {:error, :already_exists}
    else
      worker =
        worker
        |> Map.put(:order, get_chain_order(chain))
        |> Map.put(:parent, chain.id)

      # has 1 worker per chain node.
      if worker.num_workers == 1 do
        do_add_worker(chain, worker)
      else
        chain =
          Enum.reduce(1..worker.num_workers, chain, fn index, acc ->
            worker = Map.put(worker, :id, {:multi_workers, worker.id, index})
            {:ok, chain} = do_add_worker(acc, worker)
            chain
          end)

        Logger.debug(
          "SuperWorker, Chain, added multi workers (#{inspect(worker.id)}) to the chain #{inspect(chain.id)}"
        )

        {:ok, chain}
      end
    end
  end

  @spec do_add_worker(Chain.t(), Worker.t()) :: {:error, :already_exists} | {:ok, Chain.t()}
  defp do_add_worker(chain = %Chain{}, %Worker{} = worker) do
    Logger.debug(
      "SuperWorker, Chain, adding worker #{inspect(worker.id)} to the chain #{inspect(chain.id)}"
    )

    if worker_exists?(chain, worker.id) do
      {:error, :already_exists}
    else
      chain
      |> spawn_worker(worker)
    end
  end

  @spec restart_worker(Chain.t(), any()) :: {:error, any} | {:ok, Chain.t()}
  def restart_worker(chain = %Chain{}, worker_id) do
    if worker_exists?(chain, worker_id) do
      kill_worker(chain, worker_id)
      spawn_worker(chain, worker_id)
    else
      {:error, "Worker not found"}
    end
  end

  @spec restart_all_workers(Chain.t()) :: {:ok, Chain.t()}
  # TO-DO: support restart workers depend on host partition.
  def restart_all_workers(chain = %Chain{}) do
    {:ok, workers} = Db.get_all_workers(chain)

    Enum.map(
      workers,
      fn worker ->
        Logger.info("SuperWorker, Chain, restarting worker #{worker.id}, pid: #{worker.pid}")
        Process.exit(worker.pid, :kill)
        worker = do_spawn_worker(worker)
        worker.id
      end
    )

    {:ok, chain}
  end

  @spec remove_worker(Chain.t(), any()) :: true
  def remove_worker(chain, worker_id) do
    Db.delete_worker_info(chain.supervisor, worker_id, {:chain, chain.id})
  end

  @spec kill_worker(Chain.t(), any()) :: {:error, any} | {:ok, Chain.t()}
  def kill_worker(chain, worker_id) do
    with {:ok, {_, _, pid}} <-
           Db.get_worker_by_id(chain.supervisor, worker_id, {:chain, chain.id}) do
      Process.exit(pid, :kill)
      {:ok, chain}
    else
      error ->
        Logger.error(
          "SuperWorker, Chain, failed to kill worker #{inspect(worker_id)} in chain #{inspect(chain.id)}, error: #{inspect(error)}"
        )

        error
    end
  end

  @spec kill_all_workers(Chain.t()) :: {:ok, Chain.t()}
  # TO-DO: refactor this function, remove ref & pid from worker
  def kill_all_workers(chain = %Chain{}) do
    workers = Db.get_workers_by_parent(chain.supervisor, {:chain, chain.id})

    Enum.each(workers, fn {worker_id, _, pid} ->
      Logger.debug("SuperWorker, Chain, kill #{inspect(worker_id)}, pid: #{inspect(pid)}")
      Process.exit(pid, :kill)
    end)

    {:ok, chain}
  end

  @spec new_data(Chain.t(), Message.t()) :: any
  def new_data(chain = %Chain{}, msg = %Message{}) do
    send_next(chain, 1, msg)
  end

  ## Private functions

  @spec send_next(Chain.t(), non_neg_integer, Message.t()) :: any
  defp send_next(chain = %Chain{}, order, msg = %Message{}) do
    case Db.get_chain_order(chain.supervisor, chain.id, order) do
      # TO-DO: Verify order is valid/process is killed
      {:error, :not_found} ->
        Logger.debug(
          "SuperWorker, Chain, not found next worker for order #{order}, chain: #{chain.id}, go to finished callback."
        )

        # TO-DO: catch throw, error from outside.
        case chain.finished_callback do
          nil ->
            Logger.debug("SuperWorker, Chain, not found callback for chain #{chain.id}")
            {:error, :no_worker_or_callback}

          {:fun, fun} ->
            fun.(msg.data)
            {:ok, :call_back}

          {m, f, a} ->
            apply(m, f, [msg.data | a])
            {:ok, :call_back}
        end

      # just one worker doesn't check type.
      {:ok, {worker_id, pid}} ->
        Logger.debug(
          "SuperWorker, Chain, chain #{inspect(chain.id)}, order: #{order}, found a next worker: #{inspect(worker_id)}, send msg #{inspect(msg.id)}"
        )

        send(pid, {:new_data, msg})
        {:ok, :send_one}
    end
  end

  defp get_next_round_robin_order(chain, worker_id, max_order) do
    Logger.debug(
      "SuperWorker, Chain, getting next round robin order for worker #{inspect(worker_id)}, max_order: #{max_order}"
    )
  end

  defp spawn_worker(chain = %Chain{}, worker = %Worker{}) do
    Logger.debug(
      "SuperWorker, Chain, spawning worker #{inspect(worker.id)} in chain #{inspect(chain.id)}"
    )

    Db.put_worker_info(chain.supervisor, worker)

    worker
    |> Map.put(:supervisor, chain.supervisor)
    |> do_spawn_worker()

    {:ok, chain}
  end

  defp do_spawn_worker(%Worker{} = worker) do
    {pid, ref} =
      spawn_monitor(fn ->
        # Store for user can directly access to the worker.
        Process.put({:supervisor, :sup_id}, worker.supervisor)
        Process.put({:supervisor, :chain}, worker.parent)
        Process.put({:supervisor, :worker_id}, worker.id)

        loop_chain(%MapQueue{}, worker)
      end)

    Db.put_worker(worker.supervisor, ref, worker.id, {worker.type, worker.parent}, pid)
    Db.put_chain_order(worker.supervisor, worker.id, worker.parent, worker.order, pid)

    # Link to child for case supervisor is down.
    # TO-DO: Improve case worker crash immediately.
    Process.link(pid)

    worker
  end

  # Support receive data from the previous process in the chain and pass it to the next process.
  defp loop_chain(queue, %Worker{id: id, parent: chain_id} = worker) do
    receive do
      {:processed, msg_id, worker_id} ->
        Logger.debug(
          "SuperWorker, Chain, worker #{inspect(worker_id)} processed the data, msg_id: #{msg_id}"
        )

        {:ok, queue} = MapQueue.remove(queue, msg_id)
        loop_chain(queue, worker)

      {:new_data, msg = %Message{}} ->
        # TO-DO: catch throw, error from outside.
        result =
          case worker.fun do
            {:fun, f} ->
              f.(msg.data)

            {m, f, a} ->
              apply(m, f, [msg.data | a])
          end

        with {:ok, {first_id, _}} <- Db.get_chain_order(worker.supervisor, chain_id, 1) do
          if first_id != id do
            send(msg.from, {:processed, msg.id, id})
          end
        end

        case result do
          {:next, new_data} ->
            if MapQueue.is_full?(queue) do
              Logger.debug(
                "SuperWorker, Chain, worker #{inspect(id)}, queue is full, go to loop waiting for consume last data."
              )

              loop_send(queue, worker)
            end

            Logger.debug(
              "SuperWorker, Chain, worker #{inspect(id)}, passing data to the next process, chain: #{inspect(chain_id)}"
            )

            {:ok, queue, msg_id} = MapQueue.add(queue, new_data)
            {:ok, chain} = Sup.get_chain(get_my_supervisor(), chain_id)

            msg = Message.new(self(), nil, new_data, msg_id)
            send_next(chain, worker.order + 1, msg)

            loop_chain(queue, worker)

          {:error, reason} ->
            Logger.error(
              "SuperWorker, Chain, worker #{inspect(id)}, error in chain process, chain: #{inspect(chain_id)}: #{inspect(reason)}"
            )

          # TO-DO: decide to ignore or stop the chain.
          {:drop, reason} ->
            Logger.info(
              "SuperWorker, Chain, worker #{inspect(id)}, dropping chain process, chain: #{inspect(chain_id)}: #{inspect(reason)}"
            )

            loop_chain(queue, worker)

          {:stop, reason} ->
            Logger.info(
              "SuperWorker, Chain, worker #{inspect(id)}, stopping chain process, chain: #{inspect(chain_id)}"
            )

            exit(reason)

          data ->
            Logger.debug(
              "SuperWorker, Chain, worker #{inspect(id)}, passing data (default) to the next process, chain: #{inspect(chain_id)}"
            )

            if MapQueue.is_full?(queue) do
              Logger.debug(
                "SuperWorker, Chain, worker #{inspect(id)}, queue is full, go to loop waiting for consume last data."
              )

              loop_send(queue, worker)
            end

            {:ok, queue, msg_id} = MapQueue.add(queue, data)
            {:ok, chain} = Sup.get_chain(get_my_supervisor(), chain_id)

            msg = Message.new(self(), nil, data, msg_id)
            send_next(chain, worker.order + 1, msg)
            loop_chain(queue, worker)
        end

      {:kill, reason} ->
        Logger.debug(
          "SuperWorker, Chain, worker #{inspect(id)}, killing chain, chain: #{inspect(chain_id)}"
        )

        exit(reason)

      {:stop, ^chain_id} ->
        Logger.debug(
          "SuperWorker, Chain, worker #{inspect(id)}, stopping chain, chain: #{inspect(chain_id)}"
        )
    end
  end

  defp loop_send(queue, %Worker{id: id, parent: chain_id} = _worker) do
    receive do
      {:processed, msg_id, worker_id} ->
        Logger.debug(
          "SuperWorker, Chain, worker #{worker_id} processed the data, msg_id: #{msg_id}"
        )

        {:ok, MapQueue.remove(queue, msg_id)}

      {:kill, reason} ->
        Logger.debug(
          "SuperWorker, Chain, worker #{id}, killing chain, chain: #{inspect(chain_id)}"
        )

        exit(reason)

      {:stop, ^chain_id} ->
        Logger.debug(
          "SuperWorker, Chain, worker #{id}, stopping chain process, chain: #{inspect(chain_id)}"
        )

        :stop
    end
  end

  defp validate_restart_strategy(opts) do
    if opts.restart_strategy in @chain_restart_strategies do
      {:ok, opts}
    else
      {:error, "Invalid group restart strategy, #{inspect(opts.restart_strategy)}"}
    end
  end

  defp validate_send_type(opts) do
    if opts.send_type in @send_types do
      {:ok, opts}
    else
      {:error, "Invalid send type, #{inspect(opts.send_type)}"}
    end
  end

  defp validate_callback(opts) do
    case opts.finished_callback do
      nil -> {:ok, opts}
      {:fun, fun} when is_function(fun) -> {:ok, opts}
      {m, f, a} when is_atom(m) and is_atom(f) and is_list(a) -> {:ok, opts}
      _ -> {:error, "Invalid callback"}
    end
  end

  defp validate_queue_length(opts) do
    case opts.queue_length do
      n when is_integer(n) and n > 0 -> {:ok, opts}
      _ -> {:error, "Invalid queue length"}
    end
  end

  defp validate_opts(chain) do
    with {:ok, chain} <- validate_restart_strategy(chain),
         {:ok, chain} <- validate_send_type(chain),
         {:ok, chain} <- validate_callback(chain),
         {:ok, chain} <- validate_queue_length(chain) do
      {:ok, chain}
    end
  end

  defp map_to_struct(opts) when is_map(opts) do
    {:ok, struct(__MODULE__, opts)}
  end

  defp get_my_supervisor() do
    Process.get({:supervisor, :sup_id})
  end

  defp get_chain_order(chain) do
    length(Db.get_workers_by_parent(chain.supervisor, {:chain, chain.id})) + 1
  end
end
