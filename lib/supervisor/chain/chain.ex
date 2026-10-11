defmodule SuperWorker.Supervisor.Chain do
  @moduledoc """
  A chain: an ordered sequence of nodes that process data one after another.

  Each node has one or more replicas (`Worker.num_workers`). A message is
  delivered to the replica(s) of the next node, chosen by the chain
  `:send_type`:

    * `:broadcast` - every live replica.
    * `:random` - one uniformly chosen live replica.
    * `:round_robin` - live replicas in turn (counter shared per node).
    * `:partition` - the replica selected by a hash of the data.

  Orders are kept contiguous `1..n`. Removing the last replica of a node
  releases its order and shifts later nodes down.

  Restart strategies, applied when a chain worker terminates abnormally:

    * `:one_for_one` - restart only the failed worker.
    * `:one_for_all` - restart every worker of the chain.
    * `:rest_for_one` - restart the failed worker and every later node.

  Options are validated by `check_options/1` (see `SuperWorker.Supervisor.Chain.Config`):
  `:id`, `:restart_strategy`, `:send_type`, `:finished_callback`, `:queue_length`.
  """

  @enforce_keys [:id]
  defstruct [
    # chain id, unique in supervisor.
    :id,
    restart_strategy: :one_for_one,
    supervisor: nil,
    finished_callback: nil,
    queue_length: 50,
    # :broadcast, :random, :partition, :round_robin
    send_type: :random,
    table: nil
  ]

  @type t :: %__MODULE__{
          id: any(),
          restart_strategy: :one_for_one | :one_for_all | :rest_for_one,
          supervisor: atom() | nil,
          finished_callback: nil | {:fun, fun()} | {module(), atom(), [any()]},
          queue_length: pos_integer(),
          send_type: :broadcast | :random | :partition | :round_robin,
          table: atom() | reference() | nil
        }

  alias SuperWorker.Supervisor.{Chain.Config, Db, MapQueue, Message, Utils, Worker}

  alias __MODULE__
  alias Chain.Messaging

  require Logger
  require SuperWorker.Log

  ## Public functions

  @doc "Validates chain options and returns a chain struct."
  @spec check_options([atom() | keyword()]) :: {:ok, t()} | {:error, term()}
  def check_options(options), do: Config.new(options)

  @spec get_worker(t(), any()) :: {:ok, Worker.t()} | {:error, :not_found}
  def get_worker(chain = %Chain{}, worker_id) do
    SuperWorker.Log.debug(fn ->
      "SuperWorker, Chain, get_worker: #{inspect(chain.supervisor)}, #{inspect(worker_id)}"
    end)

    Db.get_worker_info(chain.table, worker_id, {:chain, chain.id})
  end

  @spec worker_exists?(t(), any()) :: boolean()
  def worker_exists?(chain = %Chain{}, worker_id) do
    match?({:ok, _}, get_worker(chain, worker_id))
  end

  @spec get_all_workers(t()) :: {:ok, [Worker.t()]}
  def get_all_workers(chain = %Chain{}) do
    SuperWorker.Log.debug(fn ->
      "SuperWorker, Chain, get_all_workers: #{inspect(chain.supervisor)}"
    end)

    Db.get_worker_infos_by_parent(chain.table, {:chain, chain.id})
  end

  @doc """
  Adds a node (one worker, or `num_workers` replicas) at the end of the chain.

  Returns `{:error, :already_exists}` when any replica id is already in the
  chain; in that case nothing is spawned.
  """
  @spec add_worker(t(), Worker.t()) :: {:ok, t()} | {:error, :already_exists}
  def add_worker(chain = %Chain{}, worker = %Worker{}) do
    ids = replica_ids(worker)

    if Enum.any?(ids, &worker_exists?(chain, &1)) do
      {:error, :already_exists}
    else
      node = %{worker | order: Db.next_chain_order(chain.table, chain.id), parent: chain.id}

      Enum.each(ids, fn id -> spawn_worker(chain, %{node | id: id}) end)

      SuperWorker.Log.debug(fn ->
        "SuperWorker, Chain, added node #{inspect(worker.id)} (#{length(ids)} replica(s)) at order #{node.order} in chain #{inspect(chain.id)}"
      end)

      {:ok, chain}
    end
  end

  @doc "Restarts one worker, keeping its id and order."
  @spec restart_worker(t(), any()) :: {:ok, t()} | {:error, :worker_not_found | term()}
  def restart_worker(chain = %Chain{}, worker_id) do
    case get_worker(chain, worker_id) do
      {:ok, worker} -> respawn_all(chain, [worker])
      {:error, _} -> {:error, :worker_not_found}
    end
  end

  @doc "Restarts every worker of the chain (`:one_for_all`)."
  @spec restart_all_workers(t()) :: {:ok, t()} | {:error, {:restart_failed, [any()]}}
  def restart_all_workers(chain = %Chain{}) do
    with {:ok, workers} <- get_all_workers(chain) do
      respawn_all(chain, workers)
    end
  end

  @doc """
  Restarts the worker and every worker of a later order (`:rest_for_one`).
  Earlier nodes keep running.
  """
  @spec restart_from_worker(t(), any()) :: {:ok, t()} | {:error, :worker_not_found | term()}
  def restart_from_worker(chain = %Chain{}, worker_id) do
    case get_worker(chain, worker_id) do
      {:ok, %Worker{order: order}} ->
        with {:ok, workers} <- get_all_workers(chain) do
          respawn_all(chain, Enum.filter(workers, &(&1.order >= order)))
        end

      {:error, _} ->
        {:error, :worker_not_found}
    end
  end

  @spec count_workers(t()) :: non_neg_integer()
  def count_workers(chain = %Chain{}) do
    {:ok, count} = Db.count_workers_by_parent(chain.table, {:chain, chain.id})
    count
  end

  @doc """
  Kills one worker, deletes it, and releases its routing. Returns
  `{:error, :worker_not_found}` when the id is not in the chain.
  """
  @spec remove_worker(t(), any()) :: :ok | {:error, :worker_not_found}
  def remove_worker(chain = %Chain{}, worker_id) do
    case get_worker(chain, worker_id) do
      {:ok, worker} ->
        remove_node_worker(chain, worker)
        :ok

      {:error, _} ->
        {:error, :worker_not_found}
    end
  end

  @doc "Kills and deletes every worker of the chain. Use before deleting the chain row."
  @spec remove_all_workers(t()) :: :ok
  def remove_all_workers(chain = %Chain{}) do
    with {:ok, workers} <- get_all_workers(chain) do
      Enum.each(workers, fn worker ->
        kill_process(chain, worker.id)
        Db.delete_worker_info(chain.table, worker.id, {:chain, chain.id})
      end)
    end

    :ok
  end

  @doc """
  Kills a worker process and deletes its ref row. The routing entry is kept so
  a restart can replace the pid.
  """
  @spec kill_worker(t(), any()) :: {:ok, t()} | {:error, :not_found}
  def kill_worker(chain, worker_id) do
    case kill_process(chain, worker_id) do
      :ok ->
        {:ok, chain}

      error ->
        Logger.error(
          "SuperWorker, Chain, failed to kill worker #{inspect(worker_id)} in chain #{inspect(chain.id)}, error: #{inspect(error)}"
        )

        error
    end
  end

  ## Private functions: membership

  # Replicas of a node get ids {:multi_workers, id, i}; a single worker keeps its id.
  defp replica_ids(%Worker{id: id, num_workers: n}) when is_integer(n) and n > 1 do
    for i <- 1..n, do: {:multi_workers, id, i}
  end

  defp replica_ids(%Worker{id: id}), do: [id]

  defp spawn_worker(chain = %Chain{}, worker = %Worker{}) do
    SuperWorker.Log.debug(fn ->
      "SuperWorker, Chain, spawning worker #{inspect(worker.id)} in chain #{inspect(chain.id)}"
    end)

    worker = %{worker | supervisor: chain.supervisor}
    Db.put_worker_info(chain.table, worker)
    do_spawn_worker(chain, worker, &Db.put_new_worker/5)
  end

  # Kills the old process of a worker (if any), then spawns a fresh one.
  # Failures are returned, not raised, so a restart cycle can report them.
  defp respawn(chain, worker) do
    _ = kill_process(chain, worker.id)
    do_spawn_worker(chain, worker, &Db.put_worker/5)
    :ok
  rescue
    error -> {:error, error}
  catch
    :exit, reason -> {:error, {:exit, reason}}
  end

  defp respawn_all(chain, workers) do
    failed =
      workers
      |> Enum.sort_by(& &1.order)
      |> Enum.flat_map(fn worker ->
        Logger.info("SuperWorker, Chain, restarting worker #{inspect(worker.id)}")

        case respawn(chain, worker) do
          :ok -> []
          {:error, reason} -> [{worker.id, reason}]
        end
      end)

    case failed do
      [] ->
        {:ok, chain}

      _ ->
        Logger.error(
          "SuperWorker, Chain, failed to restart #{length(failed)} workers in chain #{inspect(chain.id)}: #{inspect(failed)}"
        )

        {:error, {:restart_failed, Enum.map(failed, &elem(&1, 0))}}
    end
  end

  defp remove_node_worker(chain, worker = %Worker{id: worker_id}) do
    kill_process(chain, worker_id)
    Db.delete_worker_info(chain.table, worker_id, {:chain, chain.id})

    case Db.delete_chain_replica(chain.table, chain.id, worker.order, worker_id) do
      :last -> close_order_gap(chain, worker.order)
      _ -> :ok
    end
  end

  # Shifts every node after `order` down by one so orders stay contiguous.
  defp close_order_gap(chain, order) do
    last = Db.get_chain_node_count(chain.table, chain.id)

    Enum.each((order + 1)..last//1, fn from -> shift_node(chain, from) end)

    Db.release_chain_order(chain.table, chain.id)
  end

  defp shift_node(chain, from) do
    with {:ok, replicas} <- Db.get_chain_order(chain.table, chain.id, from),
         :ok <- Db.move_chain_order(chain.table, chain.id, from, from - 1) do
      Enum.each(replicas, fn {worker_id, _pid} ->
        set_worker_order(chain, worker_id, from - 1)
      end)
    else
      error ->
        Logger.error(
          "SuperWorker, Chain, failed to shift order #{from} in chain #{inspect(chain.id)}: #{inspect(error)}"
        )
    end
  end

  defp set_worker_order(chain, worker_id, order) do
    case Db.get_worker_info(chain.table, worker_id, {:chain, chain.id}) do
      {:ok, info} -> Db.put_worker_info(chain.table, %{info | order: order})
      _ -> :ok
    end
  end

  # Kills the process and deletes its ref row. Returns {:error, :not_found} when
  # the worker has no live ref row (already cleaned up by the crash handler).
  defp kill_process(chain, worker_id) do
    case Db.get_worker_by_id(chain.table, worker_id, {:chain, chain.id}) do
      {:ok, {ref, pid}} ->
        Process.exit(pid, :kill)
        Db.delete_worker(chain.table, ref)
        :ok

      error ->
        error
    end
  end

  # `put_ref` stores the ref row: `Db.put_new_worker/5` for a new add,
  # `Db.put_worker/5` (clears stale rows) for a restart.
  defp do_spawn_worker(chain = %Chain{}, worker = %Worker{}, put_ref) do
    queue = %MapQueue{queue_length: chain.queue_length}

    {pid, ref} =
      spawn_monitor(fn ->
        # Store for user can directly access to the worker.
        Process.put({:supervisor, :sup_id}, chain.supervisor)
        Process.put({:supervisor, :chain}, worker.parent)
        Process.put({:supervisor, :worker_id}, worker.id)

        loop_chain(chain, queue, worker)
      end)

    put_ref.(chain.table, ref, worker.id, {worker.type, worker.parent}, pid)
    Db.put_chain_order(chain.table, worker.id, worker.parent, worker.order, pid)
    Process.link(pid)

    worker
  end

  ## Private functions: worker loop

  # The worker reads its current order from ETS on every message, because
  # removals shift orders after it spawned.
  defp loop_chain(chain = %Chain{}, queue, worker = %Worker{id: id, parent: chain_id}) do
    receive do
      {:processed, msg_id, _worker_id} ->
        SuperWorker.Log.debug(fn ->
          "SuperWorker, Chain, worker processed the data, msg_id: #{msg_id}"
        end)

        {:ok, queue} = MapQueue.remove(queue, msg_id)
        loop_chain(chain, queue, worker)

      {:new_data, msg = %Message{}} ->
        result = call_worker_fun(worker, msg.data)

        SuperWorker.Log.debug(fn ->
          "SuperWorker, Chain, worker #{inspect(id)} processed the data, result: #{inspect(result)}"
        end)

        confirm_upstream(chain, worker, msg)

        case result do
          {:next, new_data} ->
            forward_data(chain, queue, worker, new_data, :new_data)

          {:error, reason} ->
            Logger.error(
              "SuperWorker, Chain, worker #{inspect(id)}, error in chain process, chain: #{inspect(chain.id)}: #{inspect(reason)}"
            )

            loop_chain(chain, queue, worker)

          {:drop, reason} ->
            Logger.info(
              "SuperWorker, Chain, worker #{inspect(id)}, dropping chain process, chain: #{inspect(chain.id)}: #{inspect(reason)}"
            )

            loop_chain(chain, queue, worker)

          {:stop, reason} ->
            Logger.info(
              "SuperWorker, Chain, worker #{inspect(id)}, stopping chain process, chain: #{inspect(chain.id)}"
            )

            exit(reason)

          data ->
            SuperWorker.Log.debug(fn ->
              "SuperWorker, Chain, worker #{inspect(id)}, passing data (default) to the next process, chain: #{inspect(chain.id)}"
            end)

            forward_data(chain, queue, worker, data, :chain_message)
        end

      {:kill, reason} ->
        SuperWorker.Log.debug(fn ->
          "SuperWorker, Chain, worker #{inspect(id)}, killing chain, chain: #{inspect(chain.id)}"
        end)

        exit(reason)

      {:stop, ^chain_id} ->
        SuperWorker.Log.debug(fn ->
          "SuperWorker, Chain, worker #{inspect(id)}, stopping chain, chain: #{inspect(chain.id)}"
        end)

        exit(:normal)

      unknown ->
        Logger.warning(
          "SuperWorker, Chain, worker #{inspect(id)} received unknown message: #{inspect(unknown)}"
        )

        loop_chain(chain, queue, worker)
    end
  end

  defp call_worker_fun(worker, data) do
    case Utils.safe_call(fn ->
           case worker.fun do
             {:fun, f} -> f.(data)
             {m, f, a} -> apply(m, f, [data | a])
           end
         end) do
      {:ok, result} ->
        result

      {:error, {kind, reason}} ->
        Logger.error(
          "SuperWorker, Chain, fail to call function in chain, worker_id: #{inspect(worker.id)}, #{kind}: #{inspect(reason)}"
        )

        {:error, :fail_to_execute_func}
    end
  end

  # Confirms the upstream message. The first node has no chain upstream to confirm.
  defp confirm_upstream(chain, worker, msg) do
    case current_order(chain, worker.id) do
      {:ok, order} when order > 1 -> send(msg.from, {:processed, msg.id, worker.id})
      _ -> :ok
    end
  end

  defp current_order(chain, worker_id) do
    case Db.get_worker_info(chain.table, worker_id, {:chain, chain.id}) do
      {:ok, %Worker{order: order}} -> {:ok, order}
      _ -> :error
    end
  end

  defp forward_data(chain, queue, worker, data, message_type) do
    queue = ensure_queue_capacity(queue, worker)
    {:ok, queue, msg_id} = MapQueue.add(queue, data)

    message =
      case message_type do
        :new_data ->
          %Message{id: msg_id, data: data, type: :new_data, from: self()}

        :chain_message ->
          %Message{id: msg_id, data: {msg_id, data}, type: :chain_message, from: self()}
      end

    queue =
      case forward(chain, worker, message) do
        {:ok, status} when status in [:sent_to_one, :sent_to_many] ->
          # A downstream replica owns the message and confirms it.
          queue

        _ ->
          # Nothing downstream will confirm this message (end of chain, no live
          # replica, or unknown worker): release its queue entry.
          {:ok, queue} = MapQueue.remove(queue, msg_id)
          queue
      end

    loop_chain(chain, queue, worker)
  end

  defp forward(chain, worker, message) do
    case current_order(chain, worker.id) do
      {:ok, order} -> Messaging.send_next(chain, order + 1, message)
      :error -> {:error, :unknown_worker}
    end
  end

  defp ensure_queue_capacity(queue, worker = %Worker{id: id}) do
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

  defp loop_send(queue, %Worker{id: id, parent: chain_id}, timeout) do
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
end
