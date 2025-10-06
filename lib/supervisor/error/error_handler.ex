defmodule SuperWorker.Supervisor.ErrorHandler do
  @moduledoc """
  Provides consistent error handling for the SuperWorker system.

  This module defines a custom exception and helper functions to create
  standardized error tuples. This ensures that errors are handled uniformly
  across the application, making the system more robust and easier to debug.
  """

  defexception([:reason])

  def message(%{reason: reason}) do
    case reason do
      :not_found -> "Resource not found"
      :already_exists -> "Resource already exists"
      :already_running -> "Process is already running"
      :not_running -> "Process is not running"
      :api_timeout -> "API call timed out"
      :invalid_type -> "Invalid type provided"
      :worker_not_found -> "Worker not found"
      :group_not_found -> "Group not found"
      :chain_not_found -> "Chain not found"
      :supervisor_not_found -> "Supervisor not found"
      :worker_already_exists -> "Worker already exists"
      :group_already_exists -> "Group already exists"
      :chain_already_exists -> "Chain already exists"
      :supervisor_already_exists -> "Supervisor already exists"
      {:invalid_options, details} -> "Invalid options provided: #{inspect(details)}"
      {:invalid_config, details} -> "Invalid configuration: #{inspect(details)}"
      {:custom, details} -> "A custom error occurred: #{inspect(details)}"
      other -> "An unknown error occurred: #{inspect(other)}"
    end
  end

  @type error_reason ::
          :not_found
          | :already_exists
          | :already_running
          | :not_running
          | :api_timeout
          | :invalid_type
          # Not Found
          | :worker_not_found
          | :group_not_found
          | :chain_not_found
          | :supervisor_not_found
          # Already Exists
          | :worker_already_exists
          | :group_already_exists
          | :chain_already_exists
          | :supervisor_already_exists
          # With details
          | {:invalid_options, term()}
          | {:invalid_config, term()}
          | {:custom, term()}

  @type error_tuple :: {:error, error_reason}

  @doc """
  Creates a standardized not found error.
  """
  @spec not_found(atom) :: error_tuple
  def not_found(type \\ :generic) do
    case type do
      :worker -> {:error, :worker_not_found}
      :group -> {:error, :group_not_found}
      :chain -> {:error, :chain_not_found}
      :supervisor -> {:error, :supervisor_not_found}
      _ -> {:error, :not_found}
    end
  end

  @doc """
  Creates a standardized already exists error.
  """
  @spec already_exists(atom) :: error_tuple
  def already_exists(type \\ :generic) do
    case type do
      :worker -> {:error, :worker_already_exists}
      :group -> {:error, :group_already_exists}
      :chain -> {:error, :chain_already_exists}
      :supervisor -> {:error, :supervisor_already_exists}
      _ -> {:error, :already_exists}
    end
  end

  @doc """
  Creates a standardized invalid options error.
  """
  @spec invalid_options(term) :: error_tuple
  def invalid_options(details) do
    {:error, {:invalid_options, details}}
  end

  @doc """
  Creates a standardized invalid config error.
  """
  @spec invalid_config(term) :: error_tuple
  def invalid_config(details) do
    {:error, {:invalid_config, details}}
  end

  @doc """
  Creates a standardized API timeout error.
  """
  @spec api_timeout :: error_tuple
  def api_timeout do
    {:error, :api_timeout}
  end

  @doc """
  Creates a standardized "already running" error.
  """
  @spec already_running :: error_tuple
  def already_running do
    {:error, :already_running}
  end

  @doc """
  Creates a standardized "not running" error.
  """
  @spec not_running :: error_tuple
  def not_running do
    {:error, :not_running}
  end

  @doc """
  Logs an error message.
  """
  @spec log_error(module(), String.t(), keyword()) :: :ok
  def log_error(module, message, context \\ []) do
    require Logger
    context_str = Enum.map_join(context, ", ", fn {k, v} -> "#{k}: #{inspect(v)}" end)
    Logger.error("[#{inspect(module)}] #{message}. Context: #{context_str}")
    :ok
  end

  @doc """
  Raises a `SuperWorker.Error` exception.
  """
  @spec raise_error(term) :: no_return()
  def raise_error(reason) do
    raise SuperWorker.Error, reason: reason
  end
end
