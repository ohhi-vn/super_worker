defmodule SuperWorker.Supervisor.Looper do
  @moduledoc """
  Main message loop for partition processes.

  Each `SuperWorker.Supervisor` runs one or more partitions; every partition
  executes `main_loop/1`, which receives and dispatches:

  - public API messages (`{:public_api, %Message{}}`) sent by users through
    the `SuperWorker.Supervisor` API;
  - internal API messages (`{:internal_api, %Message{}}`) exchanged between
    the supervisor master and its partitions (partition lists, restarts,
    shutdown);
  - worker `:DOWN` messages, which trigger restart handling according to the
    restart strategy of the worker's parent (standalone/group/chain);
  - `EXIT` messages from linked outside processes (ignored).

  The loop only exits when a deliberate shutdown is requested.
  """

  alias SuperWorker.Supervisor.{
    ApiHelper,
    Chain,
    Db,
    ErrorHandler,
    Group,
    Message,
    Utils,
    Worker
  }

  require Logger
  require SuperWorker.Log

  # Main loop for partition process.
  def main_loop(state) do
    # Ref of the supervisor master process (see Partition.start_partition/1);
    # when it fires, this partition must not outlive the supervisor.
    master_ref = Map.get(state, :master_monitor)

    receive do
      {:public_api, msg = %Message{}} ->
        SuperWorker.Log.debug(fn ->
          "SuperWorker, Supervisor, #{state.id} received a api message: #{inspect(msg)}"
        end)

        process_public_api_message(state, msg)

      {:internal_api, msg} ->
        SuperWorker.Log.debug(fn ->
          "SuperWorker, Supervisor, #{state.id} received an internal api message: #{inspect(msg)}"
        end)

        process_internal_api_message(state, msg)

      # The supervisor master went away; exit so all workers linked to this
      # partition die with the supervisor.
      {:DOWN, ^master_ref, :process, _pid, _reason} when master_ref != nil ->
        SuperWorker.Log.debug(fn ->
          "SuperWorker, Supervisor, #{state.id} master process is gone, stopping partition."
        end)

        exit(:normal)

      {:DOWN, _ref, :process, _pid, _reason} = msg ->
        SuperWorker.Log.debug(fn ->
          "SuperWorker, Supervisor, #{state.id} received a worker DOWN message."
        end)

        process_worker_down(state, msg)

      {:EXIT, from, reason} ->
        process_exit_message(state, from, reason)

      # Timer set by :auto_restart_time after the restart ceiling was hit.
      {:restart_worker, worker_id} ->
        state
        |> handle_auto_restart(worker_id)
        |> main_loop()

      unknown ->
        Logger.warning(
          "SuperWorker, Supervisor, partition #{state.id}, main_loop, unknown message: #{inspect(unknown)}"
        )

        main_loop(state)
    end
  end

  # TO-DO:
  #  - Implement graceful shutdown for worker processes.
  #  - Send to host partition for shutdown its workers.

  defp shutdown(state, :kill) do
    SuperWorker.Log.debug(fn ->
      "SuperWorker, Supervisor, shutting down partition: #{inspect(state.id)}"
    end)

    # The partition only kills the workers it owns.
    {:ok, groups} = Db.get_all_groups(state.table)

    Enum.each(groups, fn group ->
      kill_owned_workers(state, {:group, group.id})
    end)

    {:ok, chains} = Db.get_all_chains(state.table)

    Enum.each(chains, fn chain ->
      kill_owned_workers(state, {:chain, chain.id})
    end)

    {:ok, workers} = Db.get_worker_pids_by_parent(state.table, {:standalone, nil})

    Enum.each(workers, fn {worker_id, pid} ->
      # kill worker process on current partition only
      if partition_owns?(state, {nil, worker_id}) do
        Process.exit(pid, :kill)
      end
    end)

    {:ok, :brutal_kill}
  end

  # Graceful shutdown is not implemented yet; any other shutdown type falls
  # back to a brutal kill instead of crashing the partition.
  defp shutdown(state, type) do
    Logger.warning(
      "SuperWorker, Supervisor, partition #{state.id}, shutdown type #{inspect(type)} is not supported yet, falling back to :kill"
    )

    shutdown(state, :kill)
  end

  # Kill the workers of a group/chain parent that are owned by the current
  # partition. `state.id` is the 1-based partition order, so compare against
  # the hash order, not the partition pid.
  defp kill_owned_workers(state, parent) do
    {:ok, worker_pids} = Db.get_worker_pids_by_parent(state.table, parent)
    parent_id = elem(parent, 1)

    Enum.each(worker_pids, fn {worker_id, pid} ->
      # kill worker process on current partition only
      if partition_owns?(state, {parent_id, worker_id}) do
        Process.exit(pid, :kill)
      end
    end)
  end

  defp partition_owns?(state, key) do
    Utils.get_hash_order(key, map_size(state.partitions)) == state.id
  end

  # process exit message for outside processes.
  defp process_exit_message(state, _from, _reason) do
    SuperWorker.Log.debug(fn ->
      "SuperWorker, Supervisor, #{state.id} skipped an EXIT message for an outside process."
    end)

    main_loop(state)
  end

  # Add new worker to group/chain/standalone.
  defp process_public_api_message(
         state,
         message = %Message{type: :start_worker, data: worker = %Worker{}}
       ) do
    runable = worker_start_status(state, worker)

    # start child process.
    # not found group or chain, return error to the caller.
    case runable do
      true ->
        start_worker(state, message, worker)

      error ->
        Logger.error(
          "SuperWorker, Supervisor, #{state.id} Error when starting worker: #{inspect(error)}"
        )

        ApiHelper.api_response(message, {:error, error})
        state
    end

    main_loop(state)
  end

  # broadcast a data to all worker in group.
  defp process_public_api_message(
         state,
         message = %Message{type: :broadcast_to_group, data: {group_id, data}}
       ) do
    result =
      case Db.get_group(state.table, group_id) do
        {:ok, group} ->
          case Group.broadcast(group, data) do
            :ok -> {:ok, :sent}
            {:error, _} = error -> error
          end

        {:error, _} = error ->
          Logger.error(
            "SuperWorker, Supervisor, #{state.id} Not found group with id #{inspect(group_id)}"
          )

          error
      end

    ApiHelper.api_response(message, result)

    main_loop(state)
  end

  # send data directly to worker from api.
  defp process_public_api_message(
         state,
         message = %Message{type: :send_to_group, data: {group_id, worker_id, data}}
       ) do
    worker_id = normalize_group_worker_id(worker_id)

    case Db.get_worker_by_id(state.table, worker_id, {:group, group_id}) do
      {:ok, {_ref, pid}} ->
        send(pid, data)
        ApiHelper.api_response(message, :ok)

      {:error, _} ->
        Logger.error(
          "SuperWorker, Supervisor, supervisor #{state.id}, send to worker #{inspect(worker_id)} in group #{inspect(group_id)}, error: :worker_not_found"
        )

        ApiHelper.api_response(message, ErrorHandler.not_found(:worker))
    end

    main_loop(state)
  end

  # restart group worker from api.
  defp process_public_api_message(
         state,
         message = %Message{type: :restart_group_worker, data: {group_id, worker_id}}
       ) do
    result =
      case Db.get_group(state.table, group_id) do
        {:ok, group} ->
          case Group.restart_worker(group, worker_id) do
            # Keep the historical success reply.
            {:ok, _group} -> :ok
            {:error, _reason} = error -> error
          end

        {:error, _} ->
          Logger.error(
            "SuperWorker, Supervisor, supervisor #{state.id}, cannot restart worker #{inspect(worker_id)} in group #{inspect(group_id)}, error: :group_not_found"
          )

          ErrorHandler.not_found(:group)
      end

    ApiHelper.api_response(message, result)

    main_loop(state)
  end

  # restart all group workers from api.
  defp process_public_api_message(
         state,
         message = %Message{type: :restart_group, data: group_id}
       ) do
    case Db.get_group(state.table, group_id) do
      {:ok, group} ->
        case Group.get_all_workers(group) do
          {:ok, workers} ->
            Enum.each(workers, &Group.restart_worker(group, &1))
            ApiHelper.api_response(message, :ok)

          failed ->
            ApiHelper.api_response(message, failed)
        end

      {:error, _} ->
        Logger.error(
          "SuperWorker, Supervisor, supervisor #{state.id}, cannot restart group #{inspect(group_id)}, error: :group_not_found"
        )

        ApiHelper.api_response(message, ErrorHandler.not_found(:group))
    end

    main_loop(state)
  end

  # send data to random worker from api.
  defp process_public_api_message(
         state,
         message = %Message{type: :send_to_group_random, data: {group_id, data}}
       ) do
    result =
      with {:ok, group} <- Db.get_group(state.table, group_id),
           {:ok, workers} <- Db.get_workers_by_parent(state.table, {:group, group_id}) do
        if workers != [] do
          {worker_id, _} = Enum.random(workers)

          Group.send_message(group, worker_id, data)
        else
          {:error, :no_worker}
        end
      else
        {:error, _} = error ->
          Logger.error(
            "SuperWorker, Supervisor, #{state.id} Group not found: #{inspect(group_id)}, error: #{inspect(error)}"
          )

          error
      end

    ApiHelper.api_response(message, result)

    main_loop(state)
  end

  # add data to chain from api.
  defp process_public_api_message(
         state,
         message = %Message{type: :add_data_to_chain, data: {chain_id, data}}
       ) do
    result =
      case Db.get_chain(state.table, chain_id) do
        {:ok, chain} ->
          SuperWorker.Log.debug(fn ->
            "SuperWorker, Supervisor, #{state.id} Add data to chain: #{inspect(chain_id)}, message: #{inspect(message)}"
          end)

          message = %{message | data: data}

          Chain.Messaging.new_data(chain, message)

        error ->
          Logger.error(
            "SuperWorker, Supervisor, #{state.id} cannot send to chain, error: #{inspect(error)}"
          )

          {:error, :cannot_send}
      end

    SuperWorker.Log.debug(fn ->
      "SuperWorker, Supervisor, #{state.id} Added data to chain: #{inspect(chain_id)}, result: #{inspect(result)}"
    end)

    ApiHelper.api_response(message, result)

    main_loop(state)
  end

  defp process_public_api_message(
         state,
         message = %Message{type: :send_to_standalone_worker, data: {worker_id, data}}
       ) do
    result =
      case Db.get_worker_by_id(state.table, worker_id, {:standalone, nil}) do
        {:ok, {_ref, pid}} ->
          send(pid, data)
          :ok

        error ->
          Logger.warning(
            "SuperWorker, Supervisor, not found standalone worker #{inspect(worker_id)}"
          )

          worker_lookup_error({:standalone, nil}, error)
      end

    ApiHelper.api_response(message, result)

    main_loop(state)
  end

  # remove chain.
  defp process_public_api_message(
         state,
         message = %Message{type: :remove_chain, data: chain_id}
       ) do
    result =
      case Db.get_chain(state.table, chain_id) do
        {:ok, chain} ->
          :ok = Chain.remove_all_workers(chain)
          Db.delete_chain(state.table, chain_id)
          :ok

        error ->
          Logger.error(
            "SuperWorker, Supervisor, #{state.id} chain #{inspect(chain_id)}, something is wrong, #{inspect(error)}"
          )

          error
      end

    ApiHelper.api_response(message, result)

    main_loop(state)
  end

  # check group is existed
  defp process_public_api_message(
         state,
         message = %Message{type: :group_exists, data: group_id}
       ) do
    result =
      case Db.get_group(state.table, group_id) do
        {:ok, _group} ->
          true

        _other ->
          false
      end

    ApiHelper.api_response(message, result)

    main_loop(state)
  end

  # check group is existed
  defp process_public_api_message(
         state,
         message = %Message{type: :count_workers_in_group, data: group_id}
       ) do
    result =
      case Db.get_group(state.table, group_id) do
        {:ok, group} ->
          {:ok, Group.count_workers(group)}

        _other ->
          {:error, :group_not_found}
      end

    ApiHelper.api_response(message, result)

    main_loop(state)
  end

  # remove group.
  defp process_public_api_message(
         state,
         message = %Message{type: :remove_group, data: group_id}
       ) do
    result =
      case Db.get_group(state.table, group_id) do
        {:ok, group} ->
          case Group.get_all_workers(group) do
            {:ok, workers} ->
              Enum.each(workers, &Group.remove_worker(group, &1.id))

              Db.delete_group(state.table, group_id)
              {:ok, :deleted}

            error ->
              error
          end

        error ->
          Logger.error(
            "SuperWorker, Supervisor, #{state.id} group: #{inspect(group_id)}, something is wrong, #{inspect(error)}"
          )

          error
      end

    ApiHelper.api_response(message, result)

    main_loop(state)
  end

  # remove worker from group.
  defp process_public_api_message(
         state,
         message = %Message{type: :remove_group_worker, data: {worker_id, group_id}}
       ) do
    result =
      case Db.get_group(state.table, group_id) do
        {:ok, group} ->
          Group.remove_worker(group, worker_id)

        error ->
          Logger.error(
            "SuperWorker, Supervisor, #{state.id} Group not found: #{inspect(group_id)}"
          )

          error
      end

    ApiHelper.api_response(message, result)

    main_loop(state)
  end

  # get worker's pid.
  defp process_public_api_message(
         state,
         message = %Message{type: :get_worker_pid, data: {worker_id, parent}}
       ) do
    worker_id =
      case parent do
        {:group, _} -> normalize_group_worker_id(worker_id)
        _ -> worker_id
      end

    result =
      case Db.get_worker_by_id(state.table, worker_id, parent) do
        {:ok, {_ref, pid}} ->
          {:ok, pid}

        error ->
          Logger.error(
            "SuperWorker, Supervisor, #{state.id} worker not found, worker: #{inspect(worker_id)}, group/chain/standalone: #{inspect(parent)}"
          )

          worker_lookup_error(parent, error)
      end

    ApiHelper.api_response(message, result)

    main_loop(state)
  end

  # check group is existed
  defp process_public_api_message(
         state,
         message = %Message{type: :count_workers_in_chain, data: chain_id}
       ) do
    result =
      case Db.get_chain(state.table, chain_id) do
        {:ok, chain} ->
          {:ok, Chain.count_workers(chain)}

        _other ->
          {:error, :chain_not_found}
      end

    ApiHelper.api_response(message, result)

    main_loop(state)
  end

  # remove worker from chain.
  defp process_public_api_message(
         state,
         message = %Message{type: :remove_chain_worker, data: {worker_id, chain_id}}
       ) do
    result =
      case Db.get_chain(state.table, chain_id) do
        {:ok, chain} ->
          Chain.remove_worker(chain, worker_id)

        error ->
          Logger.error(
            "SuperWorker, Supervisor, #{state.id} chain not found: #{inspect(chain_id)}"
          )

          error
      end

    ApiHelper.api_response(message, result)

    main_loop(state)
  end

  # remove worker from group.
  defp process_public_api_message(
         state,
         message = %Message{type: :remove_standalone_worker, data: worker_id}
       ) do
    result =
      case Db.get_worker_info(state.table, worker_id, {:standalone, nil}) do
        {:ok, _info} ->
          # Kill a live process when present, then delete both rows. A worker
          # stopped by restart intensity has an info row but no ref row, and
          # must still be removable.
          case Db.get_worker_by_id(state.table, worker_id, {:standalone, nil}) do
            {:ok, {ref, pid}} ->
              Process.exit(pid, :kill)
              Db.delete_worker(state.table, ref)

            {:error, _} ->
              :ok
          end

          Db.delete_worker_info(state.table, worker_id, {:standalone, nil})
          {:ok, worker_id}

        {:error, _} = error ->
          Logger.error(
            "SuperWorker, Supervisor, #{state.id} standalone worker not found: #{inspect(worker_id)}"
          )

          worker_lookup_error({:standalone, nil}, error)
      end

    ApiHelper.api_response(message, result)

    main_loop(state)
  end

  # add group from api.
  defp process_public_api_message(state, message = %Message{type: :add_group, data: group}) do
    case Db.get_group(state.table, group.id) do
      {:error, :not_found} ->
        SuperWorker.Log.debug(fn ->
          "SuperWorker,Supervisor,  #{state.id} Adding group: #{inspect(group.id)}"
        end)

        state = add_new_group(state, group)
        # Send the response to the caller.
        ApiHelper.api_response(message, {:ok, group.id})

        state

      {:ok, _} ->
        Logger.error(
          "SuperWorker, Supervisor, partition #{state.id}, Group already exists: #{inspect(group.id)}"
        )

        ApiHelper.api_response(message, {:error, :already_exists})

        state
    end
    |> main_loop()
  end

  # add chain from api.
  defp process_public_api_message(state, message = %Message{type: :add_chain, data: chain}) do
    result =
      case Db.get_chain(state.table, chain.id) do
        {:error, :not_found} ->
          SuperWorker.Log.debug(fn ->
            "SuperWorker, Supervisor, #{state.id} Adding chain: #{inspect(chain.id)}"
          end)

          add_new_chain(state, chain)
          # Send the response to the caller.
          {:ok, chain.id}

        {:ok, _} ->
          Logger.error(
            "SuperWorker, Supervisor, #{state.id} Chain already exists: #{inspect(chain.id)}"
          )

          {:error, :already_exists}
      end

    ApiHelper.api_response(message, result)

    main_loop(state)
  end

  defp process_public_api_message(state, unknown = %Message{}) do
    Logger.warning(
      "SuperWorker, Supervisor, #{state.id} unknown api #{inspect(unknown.type)} message: #{inspect(unknown)}"
    )

    ApiHelper.api_response(unknown, {:error, {:unknown, unknown.data}})

    main_loop(state)
  end

  defp start_worker(state, message, worker) do
    SuperWorker.Log.debug(fn ->
      "SuperWorker, Supervisor, #{state.id} Everything is fine, starting worker: #{inspect(worker)}"
    end)

    result =
      case worker.type do
        :group ->
          case Db.get_group(state.table, worker.parent) do
            {:ok, group} ->
              case Group.add_worker(group, worker) do
                {:ok, _group} -> {:ok, worker.id}
                {:error, _reason} = error -> error
              end

            _ ->
              {:error, :not_found}
          end

        :chain ->
          case Db.get_chain(state.table, worker.parent) do
            {:ok, chain} ->
              case Chain.add_worker(chain, worker) do
                {:ok, _chain} -> {:ok, worker.id}
                {:error, _reason} = error -> error
              end

            _ ->
              {:error, :not_found}
          end

        :standalone ->
          start_standalone_worker(state, worker)
      end

    ApiHelper.api_response(message, result)
    state
  end

  defp start_standalone_worker(state, worker) do
    with {:ok, _pid} <- sup_start_child(state, worker, :fresh) do
      Db.put_worker_info(state.table, worker)
      {:ok, worker.id}
    end
  end

  defp worker_start_status(state, %Worker{type: :group, parent: parent, id: id}) do
    id = normalize_group_worker_id(id)

    case {has_group?(state, parent), has_group_worker?(state, parent, id)} do
      {false, _} -> :group_not_found
      {true, true} -> :worker_already_exists
      {true, false} -> true
    end
  end

  defp worker_start_status(state, %Worker{type: :chain, parent: parent, id: id}) do
    case {has_chain?(state, parent), has_chain_worker?(state, parent, id)} do
      {false, _} -> :chain_not_found
      {true, true} -> :worker_already_exists
      {true, false} -> true
    end
  end

  defp worker_start_status(state, %Worker{type: :standalone, id: id}) do
    case has_standalone_worker?(state, id) do
      true -> :worker_already_exists
      false -> true
    end
  end

  defp process_worker_down(state, {:DOWN, ref, :process, _pid, :restart}) do
    SuperWorker.Log.debug(fn ->
      "SuperWorker, Supervisor, #{state.id} ignored for died process (restarting)"
    end)

    Db.delete_worker(state.table, ref)

    main_loop(state)
  end

  defp process_worker_down(state, {:DOWN, ref, :process, _pid, :removed}) do
    SuperWorker.Log.debug(fn ->
      "SuperWorker, Supervisor, #{state.id} ignored for died process (removed by user)"
    end)

    Db.delete_worker(state.table, ref)

    main_loop(state)
  end

  defp process_worker_down(state, {:DOWN, ref, :process, pid, reason}) do
    SuperWorker.Log.debug(fn ->
      "SuperWorker, Supervisor, #{state.id}, process died: #{inspect(pid)}, ref: #{inspect(ref)}, reason: #{inspect(reason)}"
    end)

    state =
      case Db.get_worker(state.table, ref) do
        {:ok, {worker_id, parent, _info}} ->
          handle_tracked_worker_down(state, ref, worker_id, parent, pid, reason)

        # The ref row is already gone: the worker was removed (or its rows were
        # cleaned) before this DOWN arrived — nothing to restart, no warning.
        {:error, :not_found} ->
          SuperWorker.Log.debug(fn ->
            "SuperWorker, Supervisor, #{inspect(state.id)} ignored DOWN for already-cleaned ref #{inspect(ref)}"
          end)

          state

        other ->
          Logger.warning(
            "SuperWorker, Supervisor, #{inspect(state.id)} unexpected for getting worker data, reason: #{inspect(other)}"
          )

          state
      end

    main_loop(state)
  end

  defp handle_tracked_worker_down(state, ref, worker_id, parent, pid, reason) do
    case Db.get_worker_info(state.table, worker_id, parent) do
      {:ok, worker} ->
        SuperWorker.Log.debug(fn ->
          "SuperWorker, Supervisor, worker found: #{inspect({worker_id, parent})}, orig_pid: #{inspect(pid)}, restarting..."
        end)

        # clean up old data
        Db.delete_worker(state.table, ref)

        case parent do
          {:standalone, nil} ->
            restart_standalone(state, worker, {pid, reason})

          {:group, group_id} ->
            SuperWorker.Log.debug(fn ->
              "SuperWorker, Supervisor, restart worker for group #{inspect(group_id)}"
            end)

            with {:ok, group} <- Db.get_group(state.table, group_id) do
              restart_group(state, group, worker_id, {pid, reason})
            end

          {:chain, chain_id} ->
            SuperWorker.Log.debug(fn ->
              "SuperWorker, Supervisor, restart worker for chain #{inspect(chain_id)}"
            end)

            with {:ok, chain} <- Db.get_chain(state.table, chain_id) do
              restart_chain(state, chain, worker, {pid, reason})
            end
        end

      other ->
        # The ref row exists but its worker info is missing — real corruption.
        Logger.warning(
          "SuperWorker, Supervisor, #{inspect(state.id)} ref row present but worker info missing for #{inspect({worker_id, parent})}, reason: #{inspect(other)}"
        )

        Db.delete_worker(state.table, ref)
        state
    end
  end

  # get group info from api.
  defp process_internal_api_message(
         state,
         %Message{type: :restart_group_worker, data: {worker_id, group_id}}
       ) do
    case Db.get_group(state.table, group_id) do
      {:ok, group} ->
        Group.restart_worker(group, worker_id)

      other ->
        Logger.error(
          "SuperWorker, Looper, something is wrong for start group worker, #{inspect(other)}"
        )
    end

    main_loop(state)
  end

  defp process_internal_api_message(
         state,
         %Message{type: :partition_list, data: partitions}
       ) do
    state
    |> Map.put(:partitions, partitions)
    |> main_loop()
  end

  # Stop supervisor from api.
  defp process_internal_api_message(
         state,
         _message = %Message{type: :stop_supervisor, data: type}
       ) do
    SuperWorker.Log.debug(fn ->
      "SuperWorker, Supervisor, #{state.id} Stopping partition, request from master"
    end)

    # stop worker on master.
    shutdown(state, type)

    send(state.master, {:partition_stopped, state.id})
  end

  defp process_internal_api_message(state, unknown = %Message{}) do
    Logger.warning(
      "SuperWorker, Supervisor, partition #{state.id} received unknown internal api message type #{inspect(unknown.type)}, ignoring: #{inspect(unknown)}"
    )

    main_loop(state)
  end

  @spec has_group?(map(), any()) :: boolean()
  defp has_group?(state = %{}, group_id) do
    match?({:ok, _}, Db.get_group(state.table, group_id))
  end

  @spec has_chain?(map(), any()) :: boolean()
  defp has_chain?(state = %{}, chain_id) do
    match?({:ok, _}, Db.get_chain(state.table, chain_id))
  end

  @spec has_group_worker?(map(), any(), any()) :: boolean()
  defp has_group_worker?(state = %{}, group_id, worker_id) do
    worker_id = normalize_group_worker_id(worker_id)
    match?({:ok, _}, Db.get_worker_info(state.table, worker_id, {:group, group_id}))
  end

  # Group worker ids may arrive as {_, id} tuples; resolve to the same
  # canonical id Group stores.
  defp normalize_group_worker_id({_, id}), do: id
  defp normalize_group_worker_id(id), do: id

  # Group and standalone lookups use the documented worker-not-found contract;
  # chain lookups keep their existing reply shape.
  defp worker_lookup_error({:group, _}, _), do: ErrorHandler.not_found(:worker)
  defp worker_lookup_error({:standalone, nil}, _), do: ErrorHandler.not_found(:worker)
  defp worker_lookup_error(_parent, error), do: error

  @spec has_chain_worker?(map(), any(), any()) :: boolean()
  defp has_chain_worker?(state = %{}, chain_id, worker_id) do
    match?({:ok, _}, Db.get_worker_info(state.table, worker_id, {:chain, chain_id}))
  end

  @spec has_standalone_worker?(map(), any()) :: boolean()
  defp has_standalone_worker?(state = %{}, worker_id) do
    match?({:ok, _}, Db.get_worker_info(state.table, worker_id, {:standalone, nil}))
  end

  defp sup_start_child(state, worker = %Worker{id: id, type: :standalone}, mode) do
    # Start a child process
    SuperWorker.Log.debug(fn ->
      "SuperWorker, Supervisor, starting standalone worker process(#{inspect(id)})"
    end)

    case do_start_standalone(worker, state.master) do
      {:ok, {pid, ref}} ->
        try do
          Process.link(pid)
        rescue
          error ->
            Logger.error(
              "SuperWorker, Standalone, failed to link worker #{inspect(worker.id)}: #{inspect(error)}"
            )
        end

        persist_ref_row(state, worker, mode, ref, pid)
        {:ok, pid}

      {:error, cause} ->
        Logger.error(
          "SuperWorker, Looper, cannot start standalone worker #{inspect(id)}, reason: #{inspect(cause)}"
        )

        {:error, {:spawn_failed, cause}}
    end
  end

  # Fresh adds have no competing row, so skip the stale-row scan; restarts keep
  # the scanning write as a defensive purge (not on the add hot path).
  defp persist_ref_row(state, %Worker{id: id}, :fresh, ref, pid) do
    Db.put_new_worker(state.table, ref, id, {:standalone, nil}, pid)
  end

  defp persist_ref_row(state, %Worker{id: id}, :restart, ref, pid) do
    Db.put_worker(state.table, ref, id, {:standalone, nil}, pid)
  end

  # Starts the worker process itself. GenServer starts can fail (and may raise
  # or exit); function/mfa spawns cannot fail to spawn, so an immediately dying
  # process is a later DOWN, not a spawn failure.
  defp do_start_standalone(%Worker{fun: {:gen_server, {m, f, a}}}, _sup_id) do
    try do
      case apply(m, f, a) do
        {:ok, pid} -> {:ok, {pid, Process.monitor(pid)}}
        {:error, reason} -> {:error, reason}
        other -> {:error, {:unexpected_start, other}}
      end
    catch
      kind, reason -> {:error, {kind, reason}}
    end
  end

  defp do_start_standalone(%Worker{id: id, fun: fun}, sup_id) do
    result =
      spawn_monitor(fn ->
        # Store for user can directly access to the worker.
        # Store the supervisor id (not the partition id) so user code
        # can resolve its supervisor via get_my_supervisor/0.
        Process.put({:supervisor, :sup_id}, sup_id)
        Process.put({:supervisor, :worker_id}, id)

        case fun do
          {:fun, fun} -> fun.()
          {m, f, a} -> apply(m, f, a)
        end
      end)

    {:ok, result}
  end

  defp add_new_group(state, group = %Group{}) do
    group = %Group{group | supervisor: state.master, table: state.table}
    Db.put_group(state.table, group)

    state
  end

  defp add_new_chain(state, chain = %Chain{}) do
    chain = %Chain{chain | supervisor: state.master, table: state.table}
    Db.put_chain(state.table, chain)

    state
  end

  defp restart_standalone(state, child = %Worker{restart_strategy: :permanent}, {_pid, reason}) do
    restart_with_intensity(state, child, reason)
  end

  defp restart_standalone(state, %Worker{restart_strategy: :transient}, {_pid, :normal}) do
    SuperWorker.Log.debug(fn ->
      "SuperWorker, Supervisor, #{inspect(state.id)}, :transient, ignore restarting standalone worker"
    end)

    state
  end

  defp restart_standalone(state, child = %Worker{restart_strategy: :transient}, {_pid, reason}) do
    restart_with_intensity(state, child, reason)
  end

  defp restart_standalone(state, %Worker{restart_strategy: :temporary}, _child_exit) do
    SuperWorker.Log.debug(fn ->
      "SuperWorker, Supervisor, #{inspect(state.id)}, :temporary, ignore restarting standalone worker"
    end)

    state
  end

  # Restart the worker unless it has crashed :max_restarts times within
  # :max_seconds. Once the ceiling is exceeded the worker stays stopped and,
  # when :auto_restart_time is set, a timer resumes restart attempts later.
  defp restart_with_intensity(state, child = %Worker{}, reason) do
    now = System.monotonic_time(:millisecond)
    window_ms = child.max_seconds * 1_000

    recent =
      state.restarts
      |> Map.get(child.id, [])
      |> Enum.filter(&(&1 > now - window_ms))

    if length(recent) < child.max_restarts do
      state
      |> put_restart_timestamps(child.id, [now | recent])
      |> do_restart_standalone(child)
    else
      Logger.error(
        "SuperWorker, Supervisor, #{inspect(state.id)} standalone worker #{inspect(child.id)} exceeded max_restarts (#{child.max_restarts}) within #{child.max_seconds}s, stopping restarts. last reason: #{inspect(reason)}"
      )

      state
      |> put_restart_timestamps(child.id, recent)
      |> schedule_auto_restart(child)
    end
  end

  defp do_restart_standalone(state, child) do
    SuperWorker.Log.debug(fn ->
      "SuperWorker, Supervisor, #{inspect(state.id)}, restarting standalone worker #{inspect(child.id)}"
    end)

    case sup_start_child(state, child, :restart) do
      {:ok, _pid} ->
        :ok

      {:error, error} ->
        Logger.error(
          "SuperWorker, Supervisor, #{inspect(state.id)} failed to restart standalone worker #{inspect(child.id)}, reason: #{inspect(error)}"
        )
    end

    state
  end

  defp schedule_auto_restart(state, %Worker{auto_restart_time: nil}), do: state

  defp schedule_auto_restart(state, %Worker{id: id, auto_restart_time: ms})
       when is_integer(ms) and ms > 0 do
    Process.send_after(self(), {:restart_worker, id}, ms)
    state
  end

  defp put_restart_timestamps(state, worker_id, timestamps) do
    %{state | restarts: Map.put(state.restarts, worker_id, timestamps)}
  end

  # Resumes a worker whose restart ceiling was exceeded. The window is cleared
  # so the worker gets a fresh restart budget, and restart is skipped if the
  # worker was removed during the cooldown.
  defp handle_auto_restart(state, worker_id) do
    state = put_restart_timestamps(state, worker_id, [])

    case Db.get_worker_info(state.table, worker_id, {:standalone, nil}) do
      {:ok, worker} ->
        do_restart_standalone(state, worker)

      {:error, _} ->
        SuperWorker.Log.debug(fn ->
          "SuperWorker, Supervisor, #{inspect(state.id)} auto-restart skipped, worker #{inspect(worker_id)} no longer exists"
        end)

        state
    end
  end

  defp restart_group(
         state,
         group = %Group{restart_strategy: :one_for_one},
         worker,
         {_child_pid, child_reason}
       ) do
    case child_reason do
      :normal ->
        SuperWorker.Log.debug(
          "SuperWorker, Supervisor, #{inspect(state.id)}, Child process is shutdown with reason :normal, ignore restart phase."
        )

        state

      _other ->
        SuperWorker.Log.debug(
          "SuperWorker, Supervisor, #{inspect(state.id)}, Child process is down with reason: #{inspect(child_reason)}, restarting..."
        )

        Group.restart_worker(group, worker)

        state
    end
  end

  defp restart_group(
         state,
         group = %Group{restart_strategy: :one_for_all},
         _worker,
         {_child_pid, child_reason}
       ) do
    case child_reason do
      :normal ->
        SuperWorker.Log.debug(
          "SuperWorker, Supervisor, #{inspect(state.id)}, Child process is :normal shutdown, ignore restart phase."
        )

        state

      _other ->
        SuperWorker.Log.debug(
          "SuperWorker, Supervisor, #{inspect(state.id)}, Child process is down with reason #{inspect(child_reason)}, restarting..."
        )

        {:ok, workers} = Group.get_all_workers(group)

        # Clean up old process.
        # TO-DO: make sure pid, ref in worker struct is cleaned & correct after restart.
        Group.kill_all_workers(group, :restart)

        Enum.each(workers, fn %Worker{id: worker_id} ->
          pid = get_target_partition(state, {group.id, worker_id})

          SuperWorker.Log.debug(fn ->
            "SuperWorker, Supervisor, send restart signal to #{inspect(pid)} for group worker #{inspect(worker_id)}"
          end)

          message = Message.new(:restart_group_worker, pid, {worker_id, group.id})

          send(pid, {:internal_api, message})
        end)

        state
    end
  end

  # TO-DO: Follow restart strategy for child group & chain.

  defp restart_chain(state, %Chain{}, %Worker{}, {_child_pid, :normal}) do
    SuperWorker.Log.debug("SuperWorker, Supervisor, chain worker is normal, ignore restarting.")

    state
  end

  defp restart_chain(
         state,
         chain = %Chain{restart_strategy: :one_for_one},
         worker = %Worker{},
         _child_exit
       ) do
    SuperWorker.Log.debug(
      "SuperWorker, Supervisor, worker(#{inspect(worker.id)}) is down, restarting."
    )

    log_chain_restart(chain, Chain.restart_worker(chain, worker.id))
    state
  end

  defp restart_chain(
         state,
         chain = %Chain{restart_strategy: :one_for_all},
         %Worker{},
         _child_exit
       ) do
    SuperWorker.Log.debug("SuperWorker, Supervisor, child is down, restarting...")

    log_chain_restart(chain, Chain.restart_all_workers(chain))
    state
  end

  defp restart_chain(
         state,
         chain = %Chain{restart_strategy: :rest_for_one},
         worker = %Worker{},
         _child_exit
       ) do
    SuperWorker.Log.debug(
      "SuperWorker, Supervisor, worker(#{inspect(worker.id)}) is down, restarting it and later nodes."
    )

    log_chain_restart(chain, Chain.restart_from_worker(chain, worker.id))
    state
  end

  defp log_chain_restart(_chain, {:ok, _}), do: :ok

  defp log_chain_restart(chain, {:error, reason}) do
    Logger.error(
      "SuperWorker, Supervisor, chain #{inspect(chain.id)} restart failed: #{inspect(reason)}"
    )
  end

  defp get_target_partition(state, data) do
    order = Utils.get_hash_order(data, map_size(state.partitions))
    Map.get(state.partitions, order)
  end
end
