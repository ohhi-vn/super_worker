defmodule SuperWorker.Supervisor.Chain.Messaging do
  @moduledoc """
  Handles message passing and data flow within a worker chain.

  This module is responsible for sending data to the correct worker(s)
  based on the chain's configuration (e.g., send type, order) and
  invoking the finished callback when data completes its journey
  through the chain.
  """

  require Logger
  require SuperWorker.Log

  alias SuperWorker.Supervisor.{Chain, Db, Message, ErrorHandler, Utils}

  # ============================================================================
  # Public API
  # ============================================================================

  @doc """
  Injects new data into the beginning of the chain.
  """
  @spec new_data(Chain.t(), Message.t()) :: :ok
  def new_data(chain = %Chain{}, msg = %Message{}) do
    SuperWorker.Log.debug(fn ->
      "Chain.Messaging: Injecting new data into chain #{inspect(chain.id)}"
    end)

    send_next(chain, 1, msg)
  end

  @doc """
  Sends data to the next worker(s) in the chain based on the current order.
  """
  @spec send_next(Chain.t(), non_neg_integer(), Message.t()) ::
          {:ok, atom()} | {:error, atom()}
  def send_next(chain = %Chain{}, order, msg = %Message{}) do
    with {:ok, {_worker_id, pid}} <-
           Db.get_chain_order(chain.table, chain.id, order) do
      SuperWorker.Log.debug(fn ->
        "Chain.Messaging: Chain #{inspect(chain.id)}, order #{order}, found next worker. Sending message."
      end)

      send(pid, {:new_data, msg})
      {:ok, :sent_to_one}
    else
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
