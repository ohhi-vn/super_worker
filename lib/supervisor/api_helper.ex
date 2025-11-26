defmodule SuperWorker.Supervisor.ApiHelper do
  @moduledoc """
  Utility functions for SuperWorker.Supervisor.

  This module provides common utility functions used across the supervisor
  implementation, including:

  - API call helpers
  """
  @type api_result :: {:ok, any()} | {:error, any()}
  @type timeout_ms :: non_neg_integer() | :infinity

  # List message from api.
  @api_types [
    :start_worker,
    :get_group,
    :remove_group_worker,
    :get_chain,
    :send_to_group,
    :send_to_group_random,
    :add_data_to_chain,
    :send_to_worker,
    :remove_group_worker,
    :add_group,
    :add_chain,
    :stop
  ]

  alias SuperWorker.Supervisor.Message

  require Logger

  # ============================================================================
  # API Communication Helpers
  # ============================================================================

  @doc """
  Waits for an API response matching the given reference.

  Blocks until a message with the matching reference is received
  or the timeout expires.

  ## Examples

      ref = response_ref()
      # ... send message with ref ...
      api_receiver(ref, 5000)

  """
  @spec api_receiver(reference(), timeout_ms()) :: any()
  def api_receiver(ref, timeout) when is_reference(ref) do
    receive do
      {^ref, result} -> result
    after
      timeout -> {:error, :api_timeout}
    end
  end

  @doc """
  Sends an API response to the caller.

  ## Examples

      def handle_api({from, ref}, params) do
        result = do_work(params)
        api_response({from, ref}, result)
      end

  """
  @spec api_response(Message.t(), any()) :: any()
  def api_response(message = %Message{}, result) do
    send(message.from, {message.id, result})
  end

  @doc """
  Makes a synchronous API call to a target process.

  Sends a message in the format `{api_name, {from, ref}, params}` and waits
  for a response with the matching reference.

  ## Parameters

    - `target` - The pid or registered name of the target process
    - `api` - The API function name (atom)
    - `params` - Parameters to pass to the API
    - `timeout` - Maximum time to wait for response in milliseconds

  ## Examples

      result = call_api(:my_supervisor, :get_worker, worker_id, 5000)

  """
  @spec call_api(atom() | pid(), atom(), any(), timeout_ms()) :: any()
  def call_api(target, api, params, timeout)
      when (is_atom(target) or is_pid(target)) and is_atom(api) do
    Logger.debug(
      "SuperWorker, Utils, call API: #{inspect(api)}, params: #{inspect(params)}, target: #{inspect(target)}"
    )

    message = Message.new(api, target, params)

    send(target, {:public_api, message})
    api_receiver(message.id, timeout)
  end

  @doc """
  Makes an asynchronous API call (fire-and-forget).

  Sends a message but does not wait for a response.

  ## Examples

      call_api_no_reply(:my_supervisor, :notify, :data_updated)

  """
  @spec call_api_no_reply(atom() | pid(), atom(), any()) :: reference()
  def call_api_no_reply(target, api, params)
      when (is_atom(target) or is_pid(target)) and is_atom(api) do
    message =
      Message.new(api, target, params)
      |> Map.put(:from, nil)

    send(message.to, {:public_api, message})

    message.id
  end

  @spec internal_call_api_no_reply(atom() | pid(), atom(), any()) :: reference()
  def internal_call_api_no_reply(target, api, params)
      when (is_atom(target) or is_pid(target)) and is_atom(api) do
    message =
      Message.new(api, target, params)
      |> Map.put(:from, nil)

    send(message.to, {:internal_api, message})

    message.id
  end

  def valid_type?(type), do: type in @api_types
end
