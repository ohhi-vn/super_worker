defmodule SuperWorker.Supervisor.Chain.Lifecycle do
  @moduledoc """
  Manages the lifecycle of workers within a chain.

  This module handles creating, retrieving, updating, and terminating
  workers associated with a specific chain. It interacts closely
  with the `Registry` for state management.
  """

  require Logger

  alias SuperWorker.Supervisor.Chain
  alias SuperWorker.Supervisor.Chain.Messaging
  alias SuperWorker.Supervisor.Worker
  alias SuperWorker.Supervisor.Message
  alias SuperWorker.Supervisor.MapQueue
  alias SuperWorker.Supervisor.ErrorHandler
  alias SuperWorker.Supervisor, as: Sup

  # ============================================================================
  # Public API
  # ============================================================================

  @doc "Retrieves a worker from the chain by its ID."
  @spec get_worker(Chain.t(), any()) :: {:error, :worker_not_found} | {:ok, Worker.t()}
  def get_worker(%Chain{id: chain_id, supervisor: sup_id}, worker_id) do
    Logger.debug(
      "Chain.Lifecycle: Getting worker #{inspect(worker_id)} in chain #{inspect(chain_id)}"
    )

    case Registry.meta(sup_id, {:worker, {:chain, chain_id}, worker_id}) do
      {:ok, worker} -> {:ok, worker}
      :error -> ErrorHandler.not_found(:worker)
    end
  end

  @doc "Checks if a worker with the given ID exists in the chain."
  @spec worker_exists?(Chain.t(), any()) :: boolean()
  def worker_exists?(chain, worker_id) do
    match?({:ok, _}, get_worker(chain, worker_id))
  end

  @doc "Retrieves all workers currently in the chain."
  @spec get_all_workers(Chain.t()) :: {:ok, list(Worker.t())}
  def get_all_workers(%Chain{id: chain_id, supervisor: sup_id}) do
    Logger.debug("Chain.Lifecycle: Getting all workers for chain #{inspect(chain_id)}")
    workers = Registry.lookup(sup_id, {:chain, chain_id})
    {:ok, workers}
  end

  @doc "Adds a new worker to the chain."
  @spec add_worker(Chain.t(), Worker.t()) :: {:error, :already_exists} | {:ok, Chain.t()}
  def add_worker(chain, %Worker{} = worker) do
    if worker_exists?(chain, worker.id) do
      ErrorHandler.log_error(__MODULE__, "Worker already exists",
        worker_id: worker.id,
        chain_id: chain.id
      )

      ErrorHandler.already_exists(:worker)
    else
      worker =
        worker
        |> Map.put(:order, get_chain_order(chain))
        |> Map.put(:parent, chain.id)

      if worker.num_workers == 1 do
        do_add_worker(chain, worker)
      else
        # Handle adding multiple instances of a worker definition
        Enum.reduce(1..worker.num_workers, {:ok, chain}, fn index, {:ok, acc_chain} ->
          multi_worker = Map.put(worker, :id, {:multi_workers, worker.id, index})
          do_add_worker(acc_chain, multi_worker)
        end)
      end
    end
  end

  @doc "Restarts a specific worker in the chain."
  @spec restart_worker(Chain.t(), any()) :: {:error, any()} | {:ok, Chain.t()}
  def restart_worker(chain, worker_id) do
    with {:ok, worker} <- get_worker(chain, worker_id) do
      Logger.info(
        "Chain.Lifecycle: Restarting worker #{inspect(worker_id)} in chain #{inspect(chain.id)}"
      )

      kill_worker(chain, worker_id)
      # The supervisor's strategy will handle the actual restart.
      # For direct API calls, we manually respawn as per original logic.
      spawn_worker(chain, worker)
    else
      {:error, reason} = error ->
        ErrorHandler.log_error(__MODULE__, "Failed to restart worker",
          worker_id: worker_id,
          chain_id: chain.id,
          reason: reason
        )

        error
    end
  end

  @doc "Removes a worker from the chain."
  @spec remove_worker(Chain.t(), any()) :: {:error, any()} | {:ok, Chain.t()}
  def remove_worker(chain, worker_id) do
    with {:ok, worker} <- get_worker(chain, worker_id) do
      Logger.info(
        "Chain.Lifecycle: Removing worker #{inspect(worker_id)} from chain #{inspect(chain.id)}"
      )

      kill_worker(chain, worker_id)

      # Unregister all references to the worker
      Registry.unregister(chain.supervisor, {:worker, {:chain, chain.id}, worker.id})
      Registry.unregister(chain.supervisor, {:worker, :ref, worker.ref})

      {:ok, chain}
    else
      {:error, reason} = error ->
        ErrorHandler.log_error(__MODULE__, "Failed to remove worker",
          worker_id: worker_id,
          chain_id: chain.id,
          reason: reason
        )

        error
    end
  end

  @doc "Terminates a worker's process."
  @spec kill_worker(Chain.t(), any()) :: {:error, any()} | {:ok, Chain.t()}
  def kill_worker(chain, worker_id) do
    case get_worker(chain, worker_id) do
      {:ok, %{pid: pid}} when is_pid(pid) ->
        Logger.debug("Chain.Lifecycle: Killing worker process #{inspect(pid)}")
        Process.exit(pid, :kill)
        {:ok, chain}

      {:error, reason} = error ->
        ErrorHandler.log_error(__MODULE__, "Failed to kill worker",
          worker_id: worker_id,
          chain_id: chain.id,
          reason: reason
        )

        error

      _ ->
        {:error, :pid_not_found}
    end
  end

  # ============================================================================
  # Private Helpers
  # ============================================================================

  defp do_add_worker(chain, worker) do
    Logger.debug(
      "Chain.Lifecycle: Adding worker #{inspect(worker.id)} to chain #{inspect(chain.id)}"
    )

    chain
    |> update_chain_first(worker)
    |> spawn_worker(worker)
  end

  defp update_chain_first(chain, worker) do
    # The first worker added becomes the entry point for new data.
    if is_nil(chain.first_worker_id) do
      %{chain | first_worker_id: worker.id}
    else
      chain
    end
  end

  defp spawn_worker(chain, worker) do
    Logger.debug(
      "Chain.Lifecycle: Spawning worker #{inspect(worker.id)} in chain #{inspect(chain.id)}"
    )

    worker_with_sup = %{
      worker
      | supervisor: chain.supervisor,
        first_worker_id: chain.first_worker_id
    }

    {pid, ref} = spawn_monitor(__MODULE__, &worker_process_loop/2, [%MapQueue{}, worker_with_sup])
    Process.link(pid)

    final_worker = %{worker_with_sup | pid: pid, ref: ref}

    Registry.put_meta(
      chain.supervisor,
      {:worker, {:chain, chain.id}, final_worker.id},
      final_worker
    )

    Registry.register(
      chain.supervisor,
      {:worker, :ref, ref},
      {{:chain, chain.id}, final_worker.id}
    )

    {:ok, chain}
  end

  defp worker_process_loop(queue, %Worker{} = worker) do
    Process.put({:supervisor, :sup_id}, worker.supervisor)
    Process.put({:supervisor, :chain}, worker.parent)
    Process.put({:supervisor, :worker_id}, worker.id)

    Registry.register(worker.supervisor, {:chain, worker.parent}, :worker)
    Registry.register(worker.supervisor, {:chain_order, worker.parent, worker.order}, worker.id)

    case worker.id do
      {:multi_workers, root_id, index} ->
        Registry.register(worker.supervisor, {:worker, {:chain, worker.parent}, root_id}, index)

      _ ->
        Registry.register(worker.supervisor, {:worker, {:chain, worker.parent}, worker.id}, 0)
    end

    main_receive_loop(queue, worker)
  end

  defp main_receive_loop(queue, worker = %Worker{parent: parent}) do
    receive do
      {:new_data, msg = %Message{}} ->
        handle_new_data(msg, queue, worker)

      {:processed, msg_id, _worker_id} ->
        {:ok, new_queue} = MapQueue.remove(queue, msg_id)
        main_receive_loop(new_queue, worker)

      {:kill, reason} ->
        exit(reason)

      {:stop, ^parent} ->
        :ok
    end
  end

  defp handle_new_data(msg, queue, worker) do
    result =
      try do
        case worker.fun do
          {:fun, f} -> f.(msg.data)
          {m, f, a} -> apply(m, f, [msg.data | a])
        end
      catch
        kind, reason -> {:error, {kind, reason}}
      end

    if worker.first_worker_id != worker.id do
      send(msg.from, {:processed, msg.id, worker.id})
    end

    case result do
      {:next, new_data} ->
        process_next(new_data, queue, worker)

      {:error, reason} ->
        Logger.error("Worker #{inspect(worker.id)} error: #{inspect(reason)}")
        main_receive_loop(queue, worker)

      {:drop, reason} ->
        Logger.info("Worker #{inspect(worker.id)} dropped data: #{inspect(reason)}")
        main_receive_loop(queue, worker)

      {:stop, _reason} ->
        :ok

      data ->
        process_next(data, queue, worker)
    end
  end

  defp process_next(data, queue, worker) do
    new_queue =
      if MapQueue.is_full?(queue) do
        wait_for_queue_space_loop(queue, worker)
      else
        queue
      end

    {:ok, final_queue, msg_id} = MapQueue.add(new_queue, data)
    {:ok, chain} = Sup.get_chain(worker.supervisor, worker.parent)

    new_msg = Message.new(self(), nil, data, msg_id)
    Messaging.send_next(chain, worker.order + 1, new_msg)

    main_receive_loop(final_queue, worker)
  end

  defp wait_for_queue_space_loop(queue, _worker = %Worker{parent: parent}) do
    receive do
      {:processed, msg_id, _worker_id} ->
        {:ok, new_queue} = MapQueue.remove(queue, msg_id)
        new_queue

      {:kill, reason} ->
        exit(reason)

      {:stop, ^parent} ->
        exit(:normal)
    end
  end

  defp get_chain_order(chain) do
    length(Registry.lookup(chain.supervisor, {:chain, chain.id})) + 1
  end
end
