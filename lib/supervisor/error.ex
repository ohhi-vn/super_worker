defmodule SuperWorker.Error do
  @moduledoc """
  Exception module for SuperWorker errors.

  This module defines exceptions that can be raised by SuperWorker operations.
  """

  defexception [:reason, :message]

  @type t :: %__MODULE__{
          reason: atom() | String.t(),
          message: String.t()
        }

  @impl true
  def exception(opts) when is_list(opts) do
    reason = Keyword.get(opts, :reason, :unknown_error)
    message = Keyword.get(opts, :message, format_message(reason))

    %__MODULE__{
      reason: reason,
      message: message
    }
  end

  def exception(reason) when is_atom(reason) or is_binary(reason) do
    %__MODULE__{
      reason: reason,
      message: format_message(reason)
    }
  end

  def exception(opts) do
    exception(reason: opts)
  end

  @impl true
  def message(%__MODULE__{message: message}) when is_binary(message) do
    message
  end

  def message(%__MODULE__{reason: reason}) do
    format_message(reason)
  end

  @doc """
  Formats an error reason into a human-readable message.
  """
  @spec format_message(atom() | String.t()) :: String.t()
  def format_message(reason) when is_binary(reason), do: reason

  def format_message(:supervisor_not_running) do
    "Supervisor is not running"
  end

  def format_message(:already_running) do
    "Supervisor is already running"
  end

  def format_message(:worker_not_found) do
    "Worker not found"
  end

  def format_message(:worker_already_exists) do
    "Worker already exists"
  end

  def format_message(:group_not_found) do
    "Group not found"
  end

  def format_message(:chain_not_found) do
    "Chain not found"
  end

  def format_message(:invalid_options) do
    "Invalid options provided"
  end

  def format_message(:invalid_restart_strategy) do
    "Invalid restart strategy"
  end

  def format_message(:timeout) do
    "Operation timed out"
  end

  def format_message(:api_timeout) do
    "API call timed out"
  end

  def format_message(:queue_full) do
    "Queue is full"
  end

  def format_message(:partition_not_found) do
    "Partition not found"
  end

  def format_message(reason) when is_atom(reason) do
    reason
    |> Atom.to_string()
    |> String.replace("_", " ")
    |> String.capitalize()
  end

  def format_message(reason) do
    inspect(reason)
  end
end
