defmodule SuperWorker.Supervisor.Chain.Config do
  @moduledoc """
  Handles the configuration and validation for a Chain.

  This module is responsible for taking a keyword list of options,
  validating them against predefined rules, and creating a
  `SuperWorker.Supervisor.Chain` struct.
  """

  alias SuperWorker.Supervisor.Chain
  alias SuperWorker.Supervisor.Utils
  alias SuperWorker.Supervisor.Constants.Validation
  alias SuperWorker.Supervisor.Constants.Types
  alias SuperWorker.Supervisor.ErrorHandler

  @chain_params Types.chain_params()

  @doc """
  Normalizes, validates, and creates a Chain struct from the given options.
  """
  @spec new(list()) :: {:ok, Chain.t()} | {:error, term()}
  def new(opts) do
    with {:ok, normalized_opts} <- Utils.normalize_opts(opts, @chain_params),
         chain_struct = struct(Chain, normalized_opts),
         :ok <- validate_opts(chain_struct) do
      {:ok, chain_struct}
    else
      {:error, reason} ->
        ErrorHandler.log_error(__MODULE__, "Failed to create chain config", reason: reason)
        {:error, reason}
    end
  end

  defp validate_opts(chain) do
    with :ok <- validate_restart_strategy(chain),
         :ok <- validate_send_type(chain),
         :ok <- validate_callback(chain),
         :ok <- validate_queue_length(chain) do
      :ok
    else
      {:error, reason} -> {:error, reason}
    end
  end

  defp validate_restart_strategy(%Chain{restart_strategy: strategy}) do
    if Validation.valid_restart_strategy?(strategy, :chain) do
      :ok
    else
      {:error, {:invalid_restart_strategy, strategy}}
    end
  end

  defp validate_send_type(%Chain{send_type: send_type}) do
    if Validation.valid_send_type?(send_type) do
      :ok
    else
      {:error, {:invalid_send_type, send_type}}
    end
  end

  defp validate_callback(%Chain{finished_callback: callback}) do
    case callback do
      nil -> :ok
      {:fun, fun} when is_function(fun) -> :ok
      {m, f, a} when is_atom(m) and is_atom(f) and is_list(a) -> :ok
      _ -> {:error, {:invalid_callback, callback}}
    end
  end

  defp validate_queue_length(%Chain{queue_length: length}) do
    if Validation.valid_queue_length?(length) do
      :ok
    else
      {:error, {:invalid_queue_length, length}}
    end
  end
end
