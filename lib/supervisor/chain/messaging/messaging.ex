defmodule SuperWorker.Supervisor.Chain.Messaging do
  @moduledoc """
  Handles message passing and data flow within a worker chain.

  This module is responsible for sending data to the correct replica(s) of
  the next node, based on the chain's send type, and invoking the finished
  callback when data completes its journey through the chain.

  Routing results:

    * `{:ok, :sent_to_one}` - one live replica of the node was signalled.
    * `{:ok, :sent_to_many}` - several replicas were signalled (`:broadcast`).
    * `{:ok, :callback_executed}` / `{:ok, :no_callback}` - no node exists at
      the order, the finished callback ran (or is not configured).
    * `{:error, :no_live_replica}` - the node exists but none of its replicas
      is alive; nothing was sent.
    * `{:error, reason}` - the finished callback failed or is invalid.
  """

  require Logger
  require SuperWorker.Log

  alias SuperWorker.Supervisor.{Chain, Db, ErrorHandler, Message, Utils}

  # ============================================================================
  # Public API
  # ============================================================================

  @doc """
  Injects new data into the beginning of the chain.
  """
  @spec new_data(Chain.t(), Message.t()) ::
          {:ok, atom()} | {:error, atom() | {atom(), any()}}
  def new_data(chain = %Chain{}, msg = %Message{}) do
    send_next(chain, 1, msg)
  end

  @doc """
  Sends data to the replica(s) of the node at `order`, selected by the
  chain's send type. Runs the finished callback when no node exists there.
  """
  @spec send_next(Chain.t(), pos_integer(), Message.t()) ::
          {:ok, atom()} | {:error, atom() | {atom(), any()}}
  def send_next(chain = %Chain{}, order, msg = %Message{}) do
    case Db.get_chain_order(chain.table, chain.id, order) do
      {:ok, replicas} ->
        dispatch(chain, order, replicas, msg)

      {:error, :not_found} ->
        handle_finished_callback(chain, msg)

      other ->
        Logger.error(
          "SuperWorker, Chain.Messaging, send_next/3 failed unexpectedly: #{inspect(other)}"
        )

        {:error, :send_failed}
    end
  end

  # ============================================================================
  # Private Helpers
  # ============================================================================

  defp dispatch(chain, order, replicas, msg) do
    case Enum.filter(replicas, fn {_id, pid} -> Process.alive?(pid) end) do
      [] ->
        Logger.error(
          "SuperWorker, Chain.Messaging, no live replica at order #{order} in chain #{inspect(chain.id)}"
        )

        {:error, :no_live_replica}

      live ->
        targets = select_replicas(chain, order, live, msg)
        Enum.each(targets, fn {_id, pid} -> send(pid, {:new_data, msg}) end)

        {:ok, sent_status(targets)}
    end
  end

  defp sent_status([_single]), do: :sent_to_one
  defp sent_status(_many), do: :sent_to_many

  # A single live replica always receives the message, whatever the send type.
  defp select_replicas(_chain, _order, [single], _msg), do: [single]

  defp select_replicas(chain, order, live, msg) do
    case chain.send_type do
      :broadcast ->
        live

      :random ->
        [Enum.random(live)]

      :round_robin ->
        index = Db.next_round_robin(chain.table, chain.id, order, length(live))
        [Enum.at(live, index)]

      :partition ->
        index = Utils.get_hash_order(partition_key(msg), length(live)) - 1
        [Enum.at(live, index)]
    end
  end

  # Chain messages forwarded from a plain return carry {msg_id, data}; hash the
  # user-level data so the same input keeps reaching the same replica.
  defp partition_key(%Message{type: :chain_message, data: {_msg_id, data}}), do: data
  defp partition_key(%Message{data: data}), do: data

  defp handle_finished_callback(chain, msg) do
    case chain.finished_callback do
      nil ->
        SuperWorker.Log.debug(fn ->
          "Chain.Messaging: No finished_callback defined for chain #{inspect(chain.id)}"
        end)

        {:ok, :no_callback}

      {:fun, fun} when is_function(fun, 1) ->
        run_callback(chain, fn -> fun.(msg.data) end)

      {m, f, a} when is_atom(m) and is_atom(f) and is_list(a) ->
        run_callback(chain, fn -> apply(m, f, [msg.data | a]) end)

      invalid_callback ->
        ErrorHandler.log_error(__MODULE__, "Invalid finished_callback definition",
          chain_id: chain.id,
          callback: invalid_callback
        )

        {:error, :invalid_callback}
    end
  end

  defp run_callback(chain, callback) do
    case Utils.safe_call(callback) do
      {:ok, _result} ->
        {:ok, :callback_executed}

      {:error, reason} = error ->
        ErrorHandler.log_error(__MODULE__, "Finished callback failed",
          chain_id: chain.id,
          reason: reason
        )

        error
    end
  end
end
