defmodule SuperWorker.Supervisor.Chain.Messaging do
  @moduledoc """
  Handles message passing and data flow within a worker chain.

  This module is responsible for sending data to the correct worker(s)
  based on the chain's configuration (e.g., send type, order) and
  invoking the finished callback when data completes its journey
  through the chain.
  """

  require Logger

  alias SuperWorker.Supervisor.{Chain, Db, Message, ErrorHandler}

  # ============================================================================
  # Public API
  # ============================================================================

  @doc """
  Injects new data into the beginning of the chain.
  """
  @spec new_data(Chain.t(), Message.t()) :: :ok
  def new_data(chain = %Chain{}, msg = %Message{}) do
    Logger.debug("Chain.Messaging: Injecting new data into chain #{inspect(chain.id)}")
    send_next(chain, 1, msg)
  end

  @doc """
  Sends data to the next worker(s) in the chain based on the current order.
  """
  @spec send_next(Chain.t(), non_neg_integer(), Message.t()) ::
          {:ok, atom()} | {:error, atom()}
  def send_next(chain = %Chain{}, order, msg = %Message{}) do
    with {:ok, {worker_id, pid}} <-
           Db.get_chain_order(chain.table, chain.id, order) do
      Logger.debug(
        "Chain.Messaging: Chain #{inspect(chain.id)}, order #{order}, found next worker: #{inspect(worker_id)}. Sending message."
      )

      send(pid, {:new_data, msg})
      {:ok, :sent_to_one}
    else
      {:error, :not_found} ->
        handle_finished_callback(chain, msg)
    end
  end

  # ============================================================================
  # Private Helpers
  # ============================================================================

  defp route_to_multiple_workers(chain, workers, msg) do
    case chain.send_type do
      :broadcast ->
        Enum.each(workers, fn {pid, worker_id} ->
          Logger.debug("Chain.Messaging: Broadcasting to worker #{inspect(worker_id)}")
          send(pid, {:new_data, msg})
        end)

        {:ok, :sent_broadcast}

      :random ->
        {pid, worker_id} = Enum.random(workers)
        Logger.debug("Chain.Messaging: Sending randomly to worker #{inspect(worker_id)}")
        send(pid, {:new_data, msg})
        {:ok, :sent_random}

      :partition ->
        index = :erlang.phash2(msg.data, length(workers))
        {pid, worker_id} = Enum.at(workers, index)

        Logger.debug(
          "Chain.Messaging: Sending via partition to worker #{inspect(worker_id)} at index #{index}"
        )

        send(pid, {:new_data, msg})
        {:ok, :sent_partition}

      :round_robin ->
        [{_, {:multi_workers, worker_id, _}} | _] = workers
        index = get_next_round_robin_order(chain, worker_id, length(workers))
        {pid, _} = Enum.at(workers, index)
        Logger.debug("Chain.Messaging: Sending round-robin to worker at index #{index}")
        send(pid, {:new_data, msg})
        {:ok, :sent_round_robin}
    end
  end

  defp get_next_round_robin_order(chain, worker_id, max_order) do
    key = {:round_robin, {:chain, chain.id}, worker_id}
    # {pos_to_update, increment, threshold, set_on_threshold}
    # This will increment field 2. If it reaches max_order, it resets to 0.
    # The initial value is {key, 0}.
    # The return value is the value *before* the update.
    :ets.update_counter(chain.data_table, key, {2, 1, max_order, 0}, {key, 0})
  end

  defp handle_finished_callback(chain, msg) do
    case chain.finished_callback do
      nil ->
        Logger.debug(
          "Chain.Messaging: No finished_callback defined for chain #{inspect(chain.id)}"
        )

        {:ok, :no_callback}

      {:fun, fun} when is_function(fun, 1) ->
        try do
          fun.(msg.data)
          {:ok, :callback_executed}
        catch
          kind, reason ->
            ErrorHandler.log_error(__MODULE__, "Finished callback failed",
              chain_id: chain.id,
              kind: kind,
              reason: reason
            )

            {:error, :callback_failed}
        end

      {m, f, a} when is_atom(m) and is_atom(f) and is_list(a) ->
        try do
          apply(m, f, [msg.data | a])
          {:ok, :callback_executed}
        catch
          kind, reason ->
            ErrorHandler.log_error(__MODULE__, "Finished callback failed",
              chain_id: chain.id,
              module: m,
              function: f,
              kind: kind,
              reason: reason
            )

            {:error, :callback_failed}
        end

      invalid_callback ->
        ErrorHandler.log_error(__MODULE__, "Invalid finished_callback definition",
          chain_id: chain.id,
          callback: invalid_callback
        )

        {:error, :invalid_callback}
    end
  end
end
