defmodule SuperWorker.Supervisor.Looper do
  alias SuperWorker.Supervisor
  alias Supervisor.{Group, Db, ApiHelper, Chain, Worker, Message, Utils}

  require Logger

  # Default timeout (miliseconds) for API calls.
  @default_time 3_000

  # Main loop for partition process.
  def main_loop(state) do
    receive do
      {:public_api, msg = %Message{}} ->
        Logger.debug(
          "SuperWorker, Supervisor, #{state.id} received a api message: #{inspect(msg)}"
        )

        process_public_api_message(state, msg)

      {:internal_api, msg} ->
        Logger.debug(
          "SuperWorker, Supervisor, #{state.id} received a api message: #{inspect(msg)}"
        )

        process_internal_api_message(state, msg)

      {:DOWN, _ref, :process, pid, reason} = msg ->
        Logger.debug(
          "SuperWorker, Supervisor, #{state.id} Worker died: #{inspect(pid)}, reason: #{inspect(reason)}"
        )

        process_worker_down(state, msg)

      {:EXIT, from, reason} ->
        process_exit_message(state, from, reason)

      unknown ->
        Logger.warning(
          "SuperWorker, Supervisor, #{state.id} main_loop, unknown message: #{inspect(unknown)}"
        )

        main_loop(state)
    end

    Logger.debug("SuperWorker, Supervisor, #{state.id} #{inspect(self())} main loop exited.")
  end

  defp shutdown(state, :kill) do
    Logger.debug("SuperWorker, Supervisor, shutting down partition: #{inspect(state.id)}")

    # TO-DO: Implement graceful shutdown for worker processes.
    #
    {:ok, groups} = Db.get_all_groups(state.table)

    Enum.each(groups, fn group ->
      Group.kill_all_workers(group)
    end)

    {:ok, chains} = Db.get_all_chains(state.table)

    Enum.each(chains, fn chain ->
      Chain.kill_all_workers(chain)
    end)

    {:ok, workers} = Db.get_all_standalone_worker_infos(state.table)

    Enum.each(workers, fn worker ->
      Process.exit(worker.pid, :kill)
    end)

    {:ok, :brutal_kill}
  end

  # process exit message for outside processes.
  defp process_exit_message(state, from, reason) do
    Logger.debug(
      "SuperWorker, Supervisor, #{state.id} skipped process exit msg for worker process: #{inspect(from)}, reason: #{inspect(reason)}"
    )

    main_loop(state)
  end

  # Add new worker to group/chain/standalone.
  defp process_public_api_message(
         state,
         message = %Message{type: :start_worker, data: worker = %Worker{}}
       ) do
    runable =
      case worker.type do
        :group ->
          cond do
            !has_group?(state, worker.parent) ->
              :group_not_found

            has_group_worker?(state, worker.parent, worker.id) ->
              :worker_already_exists

            true ->
              true
          end

        :chain ->
          cond do
            !has_chain?(state, worker.parent) ->
              :chain_not_found

            has_chain_worker?(state, worker.parent, worker.id) ->
              :worker_already_exists

            true ->
              true
          end

        :standalone ->
          if has_standalone_worker?(state, worker.id) do
            :worker_already_exists
          else
            true
          end
      end

    # start child process.
    # not found group or chain, return error to the caller.
    state =
      if runable == true do
        Logger.debug(
          "SuperWorker, Supervisor, #{state.id} Everything is fine, starting worker: #{inspect(worker)}"
        )

        worker = %{worker | table: state.table}

        result =
          case worker.type do
            :group ->
              with {:ok, group} <- Db.get_group(state.table, worker.parent) do
                Group.add_worker(group, worker)
                {:ok, worker.id}
              else
                _ ->
                  {:error, :not_found}
              end

            :chain ->
              with {:ok, chain} <- Db.get_chain(state.table, worker.parent) do
                Chain.add_worker(chain, worker)
                {:ok, worker.id}
              else
                _ ->
                  {:error, :not_found}
              end

            :standalone ->
              Db.put_worker_info(state.table, worker)
              sup_start_child(state, worker)
              {:ok, worker.id}
          end

        ApiHelper.api_response(message, result)

        state
      else
        Logger.error(
          "SuperWorker, Supervisor, #{state.id} Error when starting worker: #{inspect(runable)}"
        )

        ApiHelper.api_response(message, {:error, runable})
        state
      end

    main_loop(state)
  end

  # Get chain in supervisor and return to the caller.
  defp process_public_api_message(state, message = %Message{type: :get_chain, data: chain_id}) do
    result = Db.get_chain(state.table, chain_id)

    ApiHelper.api_response(message, result)

    main_loop(state)
  end

  # broadcast a data to all worker in group.
  defp process_public_api_message(
         state,
         message = %Message{type: :broadcast_to_group, data: {group_id, data}}
       ) do
    result =
      with {:ok, group} <- Db.get_group(state.table, group_id) do
        Group.broadcast(group, data)

        {:ok, :sent}
      else
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
    with {:ok, {_ref, pid}} <- Db.get_worker_by_id(state.table, worker_id, {:group, group_id}) do
      send(pid, data)
      ApiHelper.api_response(message, :ok)
    else
      failed ->
        Logger.error(
          "SuperWorker, Supervisor, supervisor #{state.id}, send to worker #{inspect(worker_id)} in group #{inspect(group_id)}, error: #{inspect(failed)}"
        )

        ApiHelper.api_response(message, failed)
    end

    main_loop(state)
  end

  # restart group worker from api.
  defp process_public_api_message(
         state,
         message = %Message{type: :restart_group_worker, data: {group_id, worker_id}}
       ) do
    with {:ok, group} <- Db.get_group(state.table, group_id) do
      Group.restart_worker(group, worker_id)

      ApiHelper.api_response(message, :ok)
    else
      failed ->
        Logger.error(
          "SuperWorker, Supervisor, supervisor #{state.id}, cannot restart worker #{inspect(worker_id)} in group #{inspect(group_id)}, error: #{inspect(failed)}"
        )

        ApiHelper.api_response(message, failed)
    end

    main_loop(state)
  end

  # restart all group workers from api.
  defp process_public_api_message(
         state,
         message = %Message{type: :restart_group, data: group_id}
       ) do
    with {:ok, group} <- Db.get_group(state.table, group_id) do
      with {:ok, workers} <- Group.get_all_workers(group) do
        Enum.each(workers, fn worker ->
          Group.restart_worker(group, worker)
        end)
      end

      ApiHelper.api_response(message, :ok)
    else
      failed ->
        Logger.error(
          "SuperWorker, Supervisor, supervisor #{state.id}, cannot restart group #{inspect(group_id)}, error: #{inspect(failed)}"
        )

        ApiHelper.api_response(message, failed)
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
        if length(workers) > 0 do
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
      with {:ok, chain} <- Db.get_chain(state.table, chain_id) do
        Logger.debug(
          "SuperWorker, Supervisor, #{state.id} Add data to chain: #{inspect(chain_id)}, message: #{inspect(message)}"
        )

        message = %{message | data: data}

        Chain.Messaging.new_data(chain, message)
      else
        error ->
          Logger.error(
            "SuperWorker, Supervisor, #{state.id} cannot send to chain, error: #{inspect(error)}"
          )

          {:error, :cannot_send}
      end

    Logger.debug(
      "SuperWorker, Supervisor, #{state.id} Added data to chain: #{inspect(chain_id)}, result: #{inspect(result)}"
    )

    ApiHelper.api_response(message, result)

    main_loop(state)
  end

  defp process_public_api_message(
         state,
         message = %Message{type: :send_to_worker, data: {worker_id, data}}
       ) do
    result =
      with {:ok, {_ref, pid}} <- Db.get_worker_by_id(state.table, worker_id, {:standalone, nil}) do
        send(pid, data)
        :ok
      else
        error ->
          Logger.warning(
            "SuperWorker, Supervisor, not found standalone worker #{inspect(worker_id)}"
          )

          error
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
      with {:ok, chain} <- Db.get_chain(state.table, chain_id) do
        with {:ok, workers} <- Chain.get_all_workers(chain) do
          Enum.map(workers, fn worker ->
            Chain.remove_worker(chain, worker.id)
          end)

          Db.delete_chain(state.table, chain_id)
        end
      else
        error ->
          Logger.error(
            "SuperWorker, Supervisor, #{state.id} chain #{inspect(chain_id)}, something is wrong, #{inspect(error)}"
          )

          error
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
      with {:ok, group} <- Db.get_group(state.table, group_id) do
        with {:ok, workers} <- Group.get_all_workers(group) do
          Enum.map(workers, fn worker ->
            Group.remove_worker(group, worker.id)
          end)

          Db.delete_group(state.table, group_id)
          {:ok, :deleted}
        end
      else
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
      with {:ok, group} <- Db.get_group(state.table, group_id) do
        Group.remove_worker(group, worker_id)
      else
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
    result =
      with {:ok, {_ref, pid}} <- Db.get_worker_by_id(state.table, worker_id, parent) do
        {:ok, pid}
      else
        error ->
          Logger.error(
            "SuperWorker, Supervisor, #{state.id} worker not found, worker: #{inspect(worker_id)}, group/chain/standalone: #{inspect(parent)}"
          )

          error
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
      with {:ok, chain} <- Db.get_chain(state.table, chain_id) do
        Chain.remove_worker(chain, worker_id)
      else
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
      with {:ok, {ref, pid}} <- Db.get_worker_by_id(state.table, worker_id, {:standalone, nil}) do
        Process.exit(pid, :kill)
        Db.delete_worker(state.table, ref)
        Db.delete_worker_info(state.table, worker_id, {:standalone, nil})
        {:ok, worker_id}
      else
        error ->
          Logger.error(
            "SuperWorker, Supervisor, #{state.id} standalone worker not found: #{inspect(worker_id)}"
          )

          error
      end

    ApiHelper.api_response(message, result)

    main_loop(state)
  end

  # add group from api.
  defp process_public_api_message(state, message = %Message{type: :add_group, data: group}) do
    case Db.get_group(state.table, group.id) do
      {:error, :not_found} ->
        Logger.debug("SuperWorker,Supervisor,  #{state.id} Adding group: #{inspect(group.id)}")

        state = add_new_group(state, group)
        # Send the response to the caller.
        ApiHelper.api_response(message, {:ok, group.id})

        state

      {:ok, _} ->
        Logger.error(
          "SuperWorker, Supervisor, #{state.id} Group already exists: #{inspect(group.id)}"
        )

        ApiHelper.api_response(message, {:error, :already_exists})

        state
    end
    |> main_loop()
  end

  # get group info from api.
  defp process_public_api_message(state, message = %Message{type: :get_group, data: group_id}) do
    result = Db.get_group(state.table, group_id)

    ApiHelper.api_response(message, result)
    main_loop(state)
  end

  # get group info from api.
  defp process_public_api_message(
         state,
         message = %Message{type: :get_all_standalone_workers}
       ) do
    result = Db.get_all_standalone_worker_infos(state.table)

    ApiHelper.api_response(message, result)
    main_loop(state)
  end

  # add chain from api.
  defp process_public_api_message(state, message = %Message{type: :add_chain, data: chain}) do
    result =
      case Db.get_chain(state.table, chain.id) do
        {:error, :not_found} ->
          Logger.debug("SuperWorker, Supervisor, #{state.id} Adding chain: #{inspect(chain.id)}")

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

  defp process_worker_down(state, {:DOWN, ref, :process, pid, :restart}) do
    Logger.debug(
      "SuperWorker, Supervisor, #{state.id} ignored for died process (restarting): #{inspect(pid)}"
    )

    Db.delete_worker(state.table, ref)

    main_loop(state)
  end

  defp process_worker_down(state, {:DOWN, ref, :process, pid, :removed}) do
    Logger.debug(
      "SuperWorker, Supervisor, #{state.id} ignored for died process (removed by user): #{inspect(pid)}"
    )

    Db.delete_worker(state.table, ref)

    main_loop(state)
  end

  defp process_worker_down(state, {:DOWN, ref, :process, pid, reason}) do
    Logger.debug(
      "SuperWorker, Supervisor, #{state.id}, process died: #{inspect(pid)}, ref: #{inspect(ref)}, reason: #{inspect(reason)}"
    )

    with {:ok, {worker_id, parent, _} = info} <- Db.get_worker(state.table, ref),
         {:ok, worker} <- Db.get_worker_info(state.table, worker_id, parent) do
      Logger.debug(
        "SuperWorker, Supervisor, worker found: #{inspect(info)}, orig_pid: #{inspect(pid)}, restarting..."
      )

      # clean up old data
      Db.delete_worker(state.table, ref)

      case parent do
        {:standalone, nil} ->
          restart_standalone(state, worker, {pid, reason})

        {:group, group_id} ->
          Logger.debug("SuperWorker, Supervisor, restart worker for group #{inspect(group_id)}")

          with {:ok, group} <- Db.get_group(state.table, group_id) do
            restart_group(state, group, worker_id, {pid, reason})
          end

        {:chain, chain_id} ->
          Logger.debug("SuperWorker, Supervisor, restart worker for chain #{inspect(chain_id)}")

          with {:ok, chain} <- Db.get_chain(state.table, chain_id) do
            restart_chain(state, chain, worker, {pid, reason})
          end
      end
    else
      other ->
        Logger.warning(
          "SuperWorker, Supervisor, #{inspect(state.id)} unexpected for getting worker data, reason: #{inspect(other)}"
        )
    end

    main_loop(state)
  end

  # get group info from api.
  defp process_internal_api_message(
         state,
         %Message{type: :restart_group_worker, data: {worker_id, group_id}}
       ) do
    with {:ok, group} <- Db.get_group(state.table, group_id) do
      Group.restart_worker(group, worker_id)
    else
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
  defp process_internal_api_message(state, message = %Message{type: :stop, data: type}) do
    Logger.info(
      "SuperWorker, Supervisor, #{state.id} Stopping partition, request from #{inspect(message.from)}"
    )

    # stop worker on master.
    shutdown(state, type)

    send(state.master, {:partition_stopped, state.id})

    exit(:normal)
  end

  defp process_internal_api_message(_, _) do
    raise "not implement"
  end

  @spec has_group?(map(), any()) :: boolean()
  defp has_group?(%{} = state, group_id) do
    match?({:ok, _}, Db.get_group(state.table, group_id))
  end

  @spec has_chain?(map(), any()) :: boolean()
  defp has_chain?(%{} = state, chain_id) do
    match?({:ok, _}, Db.get_chain(state.table, chain_id))
  end

  @spec has_group_worker?(map(), any(), any()) :: boolean()
  defp has_group_worker?(%{} = state, group_id, worker_id) do
    match?({:ok, _}, Db.get_worker_info(state.table, worker_id, {:group, group_id}))
  end

  @spec has_chain_worker?(map(), any(), any()) :: boolean()
  defp has_chain_worker?(%{} = state, chain_id, worker_id) do
    match?({:ok, _}, Db.get_worker_info(state.table, worker_id, {:chain, chain_id}))
  end

  @spec has_standalone_worker?(map(), any()) :: boolean()
  defp has_standalone_worker?(%{} = state, worker_id) do
    match?({:ok, _}, Db.get_worker_info(state.table, worker_id, {:standalone, nil}))
  end

  defp sup_start_child(state, %Worker{id: id, type: :standalone} = worker) do
    # Start a child process
    Logger.debug("SuperWorker, Supervisor, starting standalone worker process(#{inspect(id)})")

    result =
      case worker.fun do
        {:gen_server, {m, f, a}} ->
          case apply(m, f, a) do
            {:ok, pid} ->
              ref = Process.monitor(pid)
              {:ok, {pid, ref}}

            {:error, reason} ->
              Logger.error("Failed to start gen_server: #{inspect(reason)}")

              {:error, reason}
          end

        _ ->
          result =
            spawn_monitor(fn ->
              # Store for user can directly access to the worker.
              Process.put({:supervisor, :sup_id}, state.id)
              Process.put({:supervisor, :worker_id}, id)

              case worker.fun do
                {:fun, fun} ->
                  fun.()

                {m, f, a} ->
                  apply(m, f, a)
              end
            end)

          {:ok, result}
      end

    case result do
      {:ok, {pid, ref}} ->
        # Link to child for case supervisor is down.
        Process.link(pid)

        Db.put_worker(state.table, ref, worker.id, {:standalone, nil}, pid)

      # ignore failed worker
      failed ->
        Logger.error("SuperWorker, Looper, cannot start standalone worker, #{inspect(failed)}")
        :ok
    end

    state
  end

  defp add_new_group(state, %Group{} = group) do
    group = %Group{group | supervisor: state.master, table: state.table}
    Db.put_group(state.table, group)

    state
  end

  defp add_new_chain(state, %Chain{} = chain) do
    chain = %Chain{chain | supervisor: state.master, table: state.table}
    Db.put_chain(state.table, chain)

    state
  end

  defp restart_standalone(state, %Worker{} = child, {pid, reason}) do
    case child.restart_strategy do
      :permanent ->
        Logger.debug(
          "SuperWorker, Supervisor, #{inspect(state.id)}, :permanent, restarting #{inspect(pid)}"
        )

        # Restart the child process
        sup_start_child(state, child)

      :transient when reason != :normal ->
        Logger.debug(
          "SuperWorker, Supervisor, #{inspect(state.id)}, :transient, reason down: #{inspect(reason)} restarting #{inspect(pid)}"
        )

        # Restart the child process
        sup_start_child(state, child)

      :transient ->
        Logger.debug(
          "SuperWorker, Supervisor, #{inspect(state.id)}, :transient, ignore restarting #{inspect(pid)}"
        )

        state

      :temporary ->
        Logger.debug(
          "SuperWorker, Supervisor, #{inspect(state.id)}, :temporary, ignore restarting #{inspect(pid)}"
        )

        state
    end
  end

  defp restart_group(
         state,
         %Group{restart_strategy: :one_for_one} = group,
         worker,
         {pid, reason}
       ) do
    case reason do
      :normal ->
        Logger.debug(
          "SuperWorker, Supervisor, #{inspect(state.id)}, Child process(#{inspect(pid)}) is shutdown with reason :normal, ignore restart phase."
        )

        state

      reason ->
        Logger.debug(
          "SuperWorker, Supervisor, #{inspect(state.id)}, Child process(#{inspect(pid)}) is down with reason: #{inspect(reason)}, restarting..."
        )

        Group.restart_worker(group, worker)

        state
    end
  end

  defp restart_group(
         state,
         %Group{restart_strategy: :one_for_all} = group,
         _worker,
         {pid, reason}
       ) do
    case reason do
      :normal ->
        Logger.debug(
          "SuperWorker, Supervisor, #{inspect(state.id)}, Child process(#{inspect(pid)}) is :normal shutdown, ignore restart phase."
        )

        state

      reason ->
        Logger.debug(
          "SuperWorker, Supervisor, #{inspect(state.id)}, Child process(#{inspect(pid)}) is down with reason #{inspect(reason)}, restarting..."
        )

        {:ok, workers} = Group.get_all_workers(group)

        # Clean up old process.
        # TO-DO: make sure pid, ref in worker struct is cleaned & correct after restart.
        Group.kill_all_workers(group, :restart)

        Enum.each(workers, fn %Worker{id: worker_id} ->
          pid = get_target_partition(state, {group.id, worker_id})

          Logger.debug(
            "SuperWorker, Supervisor, send restart signal to #{inspect(pid)} for group worker #{inspect(worker_id)}"
          )

          message = Message.new(:restart_group_worker, pid, {worker_id, group.id})

          send(pid, {:internal_api, message})
        end)

        state
    end
  end

  # TO-DO: Follow restart strategy for child group & chain.

  defp restart_chain(
         state,
         %Chain{restart_strategy: :one_for_one} = chain,
         worker = %Worker{},
         {pid, reason}
       ) do
    case reason do
      :normal ->
        Logger.debug(
          "SuperWorker, Supervisor, worker(#{inspect(worker.id)}) process(#{inspect(pid)}) is normal, ignore restarting."
        )

        state

      _ ->
        Logger.debug(
          "SuperWorker, Supervisor, worker(#{inspect(worker.id)}) process(#{inspect(pid)}) is down, restarting."
        )

        Chain.restart_worker(chain, worker.id)
    end
  end

  defp restart_chain(
         state,
         %Chain{restart_strategy: :one_for_all} = chain,
         %Worker{},
         {pid, reason}
       ) do
    case reason do
      :normal ->
        Logger.debug(
          "SuperWorker, Supervisor, child process(#{inspect(pid)}) is normal, ignore restarting."
        )

        state

      _ ->
        Logger.debug(
          "SuperWorker, Supervisor, child process(#{inspect(pid)}) is down, restarting..."
        )

        Chain.restart_all_workers(chain)
    end
  end

  defp get_target_partition(state, data) do
    order = Utils.get_hash_order(data, map_size(state.partitions))
    Map.get(state.partitions, order)
  end
end
