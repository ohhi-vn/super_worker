defmodule SuperWorker.Supervisor.Chain do
  @moduledoc """
  Documentation for `SuperWorker.Supervisor.Chain`.
  """

  @enforce_keys [:id]
  defstruct [
    # chain id, unique in supervior.
    :id,
    restart_strategy: :one_for_one,
    supervisor: nil,
    # partition_pid is deprecated and unused.
    partition_pid: nil,
    finished_callback: nil,
    queue_length: 50,
    # :broadcast, :random, :partition, :round_robin
    send_type: :random,
    table: nil
  ]

  @type t :: %__MODULE__{
          id: any(),
          restart_strategy: atom(),
          supervisor: atom() | nil,
          finished_callback: nil | {:fun, fun()} | {module(), atom(), [any()]},
          queue_length: non_neg_integer(),
          send_type: :broadcast | :random | :partition | :round_robin,
          table: atom() | nil
        }

  @type check_options_result :: {:error, atom() | {atom(), any()}} | {:ok, t()}
  @type worker_operation_result :: {:error, atom()} | {:ok, t()}

  alias SuperWorker.Supervisor.{Constants, Db, MapQueue, Message, Utils, Validator, Worker}

  alias __MODULE__
  alias Chain.Messaging

  require Logger
  require SuperWorker.Log

  ## Public functions

  @spec check_options([atom() | keyword()]) :: {:error, atom | {atom, any}} | {:ok, Chain.t()}
  @spec check_options([atom() | keyword()]) :: check_options_result()
  def check_options(options) do
    with {:ok, options} <- Validator.normalize_options(options, Constants.Types.chain_params()),
         {:ok, chain} <- to_struct(options) do
      validate_options(chain)
    end
  end

  @spec get_worker(Chain.t(), any()) :: {:error, :worker_not_found} | {:ok, Worker.t()}
  @spec get_worker(t(), any()) :: {:error, :worker_not_found} | {:ok, Worker.t()}
  def get_worker(chain = %Chain{}, worker_id) do
    SuperWorker.Log.debug(fn ->
      "SuperWorker, Chain, get_worker: #{inspect(chain.supervisor)}, #{inspect(worker_id)}"
    end)

    Db.get_worker_info(chain.table, worker_id, {:chain, chain.id})
  end

  @spec worker_exists?(Chain.t(), any()) :: boolean()
  @spec worker_exists?(t(), any()) :: boolean()
  def worker_exists?(chain = %Chain{}, worker_id) do
    case get_worker(chain, worker_id) do
      {:ok, _} -> true
      {:error, _} -> false
    end
  end

  @spec get_all_workers(Chain.t()) :: {:ok, list(Worker.t())}
  @spec get_all_workers(t()) :: {:ok, list(Worker.t())}
  def get_all_workers(chain = %Chain{}) do
    SuperWorker.Log.debug(fn ->
      "SuperWorker, Chain, get_all_workers: #{inspect(chain.supervisor)}"
    end)

    Db.get_worker_infos_by_parent(chain.table, {:chain, chain.id})
  end

  @spec add_worker(Chain.t(), Worker.t()) :: {:error, :already_exists} | {:ok, Chain.t()}
  @spec add_worker(t(), Worker.t()) :: {:error, :already_exists} | {:ok, t()}
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

        SuperWorker.Log.debug(fn ->
          "SuperWorker, Chain, added multi workers (#{inspect(worker.id)}) to the chain #{inspect(chain.id)}"
        end)

        {:ok, chain}
      end
    end
  end

  @spec do_add_worker(Chain.t(), Worker.t()) :: {:error, :already_exists} | {:ok, Chain.t()}
  defp do_add_worker(chain = %Chain{}, %Worker{} = worker) do
    SuperWorker.Log.debug(fn ->
      "SuperWorker, Chain, adding worker #{inspect(worker.id)} to the chain #{inspect(chain.id)}"
    end)

    if worker_exists?(chain, worker.id) do
      {:error, :already_exists}
    else
      chain
      |> spawn_worker(worker)
    end
  end

  @spec restart_worker(Chain.t(), any()) :: {:error, any} | {:ok, Chain.t()}
  @spec restart_worker(t(), any()) :: {:error, any()} | {:ok, t()}
  def restart_worker(chain = %Chain{}, worker_id) do
    case get_worker(chain, worker_id) do
      {:ok, worker} ->
        case kill_worker(chain, worker_id) do
          {:ok, _chain} ->
            spawn_worker(chain, worker)

          # The ref row may already be cleaned up by the crash handler;
          # respawn anyway so the node comes back.
          {:error, :not_found} ->
            spawn_worker(chain, worker)
        end

      {:error, _} ->
        {:error, :worker_not_found}
    end
  end

  @spec restart_all_workers(Chain.t()) :: {:ok, Chain.t()}
  # TO-DO: support restart workers depend on host partition.
  def restart_all_workers(chain = %Chain{}) do
    {:ok, workers} = Db.get_worker_infos_by_parent(chain.table, {:chain, chain.id})

    results =
      Enum.map(workers, fn worker ->
        Logger.info("SuperWorker, Chain, restarting worker #{inspect(worker.id)}")

        case Db.get_worker_by_id(chain.table, worker.id, {:chain, chain.id}) do
          {:ok, {ref, pid}} ->
            Process.exit(pid, :kill)
            Db.delete_worker(chain.table, ref)

          {:error, _reason} ->
            :ok
        end

        case do_spawn_worker(chain, worker) do
          %Worker{} -> {:ok, worker.id}
          other -> other
        end
      end)

    {failures, _successes} =
      Enum.split_with(results, fn
        {:ok, _} -> false
        _ -> true
      end)

    if Enum.any?(failures) do
      Logger.error(
        "SuperWorker, Chain, failed to restart #{Enum.count(failures)} workers in chain #{inspect(chain.id)}: #{inspect(failures)}"
      )
    end

    {:ok, chain}
  end

  @spec count_workers(Chain.t()) :: non_neg_integer()
  @spec count_workers(t()) :: non_neg_integer()
  def count_workers(chain = %Chain{}) do
    {:ok, workers} = Db.get_worker_infos_by_parent(chain.table, {:chain, chain.id})
    Enum.count(workers)
  end

  @spec remove_worker(Chain.t(), any()) :: true
  def remove_worker(chain, worker_id) do
    kill_worker(chain, worker_id)
    Db.delete_worker_info(chain.table, worker_id, {:chain, chain.id})
  end

  @spec kill_worker(Chain.t(), any()) :: {:error, any} | {:ok, Chain.t()}
  def kill_worker(chain, worker_id) do
    case Db.get_worker_by_id(chain.table, worker_id, {:chain, chain.id}) do
      {:ok, {ref, pid}} ->
        Process.exit(pid, :kill)
        Db.delete_worker(chain.table, ref)
        {:ok, chain}

      error ->
        Logger.error(
          "SuperWorker, Chain, failed to kill worker #{inspect(worker_id)} in chain #{inspect(chain.id)}, error: #{inspect(error)}"
        )

        error
    end
  end

  ## Private functions

  defp spawn_worker(chain = %Chain{}, worker = %Worker{}) do
    SuperWorker.Log.debug(fn ->
      "SuperWorker, Chain, spawning worker #{inspect(worker.id)} in chain #{inspect(chain.id)}"
    end)

    worker = %{worker | supervisor: chain.supervisor}

    Db.put_worker_info(chain.table, worker)

    do_spawn_worker(chain, worker)

    {:ok, chain}
  end

  defp do_spawn_worker(chain = %Chain{}, worker = %Worker{}) do
    queue =
      case Db.get_chain(chain.table, chain.id) do
        {:ok, %Chain{queue_length: queue_length}} ->
          %MapQueue{queue_length: queue_length}

        _ ->
          %MapQueue{}
      end

    {pid, ref} =
      spawn_monitor(fn ->
        # Store for user can directly access to the worker.
        Process.put({:supervisor, :sup_id}, chain.supervisor)
        Process.put({:supervisor, :chain}, worker.parent)
        Process.put({:supervisor, :worker_id}, worker.id)

        loop_chain(chain.table, queue, worker)
      end)

    Db.put_worker(chain.table, ref, worker.id, {worker.type, worker.parent}, pid)
    Db.put_chain_order(chain.table, worker.id, worker.parent, worker.order, pid)

    try do
      Process.link(pid)
    catch
      :exit, reason ->
        Logger.error(
          "SuperWorker, Chain, failed to link worker #{inspect(worker.id)}: #{inspect(reason)}"
        )

        Process.exit(pid, :kill)
        exit(reason)
    end

    worker
  end

  # Support receive data from the previous process in the chain and pass it to the next process.
  defp loop_chain(table, queue, worker = %Worker{id: id, parent: chain_id}) do
    receive do
      {:processed, msg_id, _worker_id} ->
        SuperWorker.Log.debug(fn ->
          "SuperWorker, Chain, worker processed the data, msg_id: #{msg_id}"
        end)

        {:ok, queue} = MapQueue.remove(queue, msg_id)
        loop_chain(table, queue, worker)

      {:new_data, msg = %Message{}} ->
        result =
          case Utils.safe_call(fn ->
                 case worker.fun do
                   {:fun, f} ->
                     f.(msg.data)

                   {m, f, a} ->
                     apply(m, f, [msg.data | a])
                 end
               end) do
            {:ok, result} ->
              result

            {:error, {kind, reason}} ->
              Logger.error(
                "SuperWorker, Chain, fail to call function in chain, worker_id: #{inspect(id)}, #{kind}: #{inspect(reason)}"
              )

              {:error, :fail_to_execute_func}
          end

        SuperWorker.Log.debug(fn ->
          "SuperWorker, Chain, worker #{inspect(worker.id)} processed the data, result: #{inspect(result)}"
        end)

        with {:ok, {first_id, _}} <- Db.get_chain_order(table, chain_id, 1) do
          if first_id != id do
            send(msg.from, {:processed, msg.id, id})
          end
        end

        case result do
          {:next, new_data} ->
            forward_data(table, chain_id, worker, queue, new_data, :new_data)

          {:error, reason} ->
            Logger.error(
              "SuperWorker, Chain, worker #{inspect(id)}, error in chain process, chain: #{inspect(chain_id)}: #{inspect(reason)}"
            )

            loop_chain(table, queue, worker)

          {:drop, reason} ->
            Logger.info(
              "SuperWorker, Chain, worker #{inspect(id)}, dropping chain process, chain: #{inspect(chain_id)}: #{inspect(reason)}"
            )

            loop_chain(table, queue, worker)

          {:stop, reason} ->
            Logger.info(
              "SuperWorker, Chain, worker #{inspect(id)}, stopping chain process, chain: #{inspect(chain_id)}"
            )

            exit(reason)

          data ->
            SuperWorker.Log.debug(fn ->
              "SuperWorker, Chain, worker #{inspect(id)}, passing data (default) to the next process, chain: #{inspect(chain_id)}"
            end)

            forward_data(table, chain_id, worker, queue, data, :chain_message)
        end

      {:kill, reason} ->
        SuperWorker.Log.debug(fn ->
          "SuperWorker, Chain, worker #{inspect(id)}, killing chain, chain: #{inspect(chain_id)}"
        end)

        exit(reason)

      {:stop, ^chain_id} ->
        SuperWorker.Log.debug(fn ->
          "SuperWorker, Chain, worker #{inspect(id)}, stopping chain, chain: #{inspect(chain_id)}"
        end)

        exit(:normal)

      unknown ->
        Logger.warning(
          "SuperWorker, Chain, worker #{inspect(id)} received unknown message: #{inspect(unknown)}"
        )

        loop_chain(table, queue, worker)
    after
      30_000 ->
        Logger.warning("SuperWorker, Chain, worker #{inspect(id)} timed out waiting for messages")
        exit(:timeout)
    end
  end

  defp forward_data(table, chain_id, worker, queue, data, message_type) do
    queue = ensure_queue_capacity(queue, worker)
    {:ok, queue, msg_id} = MapQueue.add(queue, data)
    {:ok, chain} = Db.get_chain(table, chain_id)

    message =
      case message_type do
        :new_data -> %Message{id: msg_id, data: data, type: :new_data, from: self()}
        :chain_message -> Message.new(:chain_message, nil, {msg_id, data})
      end

    result = Messaging.send_next(chain, worker.order + 1, message)

    queue =
      case result do
        # A downstream worker owns the message and will send the
        # {:processed, msg_id, ...} confirmation.
        {:ok, :sent_to_one} ->
          queue

        # No downstream consumed the message (no next worker: the finished
        # callback ran; or a send failure): nothing will ever confirm it,
        # so drop the queue entry instead of wedging the worker.
        _ ->
          {:ok, queue} = MapQueue.remove(queue, msg_id)
          queue
      end

    loop_chain(table, queue, worker)
  end

  defp ensure_queue_capacity(queue, %Worker{id: id} = worker) do
    case MapQueue.full?(queue) do
      false ->
        queue

      true ->
        SuperWorker.Log.debug(fn ->
          "SuperWorker, Chain, worker #{inspect(id)}, queue is full, go to loop waiting for consume last data."
        end)

        case loop_send(queue, worker, 5_000) do
          {:ok, updated_queue} ->
            updated_queue

          :stop ->
            Logger.error(
              "SuperWorker, Chain, worker #{inspect(id)}, queue full, exiting chain process"
            )

            exit(:queue_full)
        end
    end
  end

  defp loop_send(queue, %Worker{id: id, parent: chain_id} = worker, timeout) do
    # NOTE: there is deliberately no catch-all clause here. Non-matching
    # messages (e.g. {:new_data, _} arriving while the queue is full) must
    # stay in the mailbox so they are processed by loop_chain/3 once the
    # queue drains; consuming them here would silently drop chain data.
    receive do
      {:processed, msg_id, _worker_id} ->
        SuperWorker.Log.debug(fn ->
          "SuperWorker, Chain, worker processed the data, msg_id: #{msg_id}"
        end)

        MapQueue.remove(queue, msg_id)

      {:kill, reason} ->
        SuperWorker.Log.debug(fn ->
          "SuperWorker, Chain, worker #{id}, killing chain, chain: #{inspect(chain_id)}"
        end)

        exit(reason)

      {:stop, ^chain_id} ->
        SuperWorker.Log.debug(fn ->
          "SuperWorker, Chain, worker #{id}, stopping chain process, chain: #{inspect(chain_id)}"
        end)

        :stop
    after
      timeout ->
        Logger.error("SuperWorker, Chain, worker #{id} timed out in loop_send after #{timeout}ms")
        :stop
    end
  end

  defp validate_restart_strategy(opts) do
    if opts.restart_strategy in Constants.Strategies.chain_restart_strategies() do
      {:ok, opts}
    else
      {:error, "Invalid chain restart strategy, #{inspect(opts.restart_strategy)}"}
    end
  end

  defp validate_send_type(opts) do
    if opts.send_type in Constants.Types.chain_send_types() do
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

  defp validate_queue_length(options) do
    case options.queue_length do
      n when is_integer(n) and n > 0 -> {:ok, options}
      _ -> {:error, "Invalid queue length"}
    end
  end

  defp validate_options(chain) do
    with {:ok, chain} <- validate_restart_strategy(chain),
         {:ok, chain} <- validate_send_type(chain),
         {:ok, chain} <- validate_callback(chain) do
      validate_queue_length(chain)
    end
  end

  defp get_chain_order(chain) do
    {:ok, workers} = Db.get_workers_by_parent(chain.table, {:chain, chain.id})
    length(workers) + 1
  end

  defp to_struct(options) when is_map(options) do
    fields =
      %Chain{id: nil}
      |> Map.from_struct()
      |> Map.keys()

    result =
      %Chain{} =
      Enum.reduce(fields, %Chain{id: nil}, fn field, acc ->
        if Map.has_key?(options, field) do
          %{acc | field => Map.get(options, field)}
        else
          acc
        end
      end)

    {:ok, result}
  end
end
