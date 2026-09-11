defmodule SuperWorker.Supervisor.Constants.Validation do
  @moduledoc """
  Provides validation functions for various constants and parameters in the SuperWorker system.
  """

  alias SuperWorker.Supervisor.Constants.Strategies
  alias SuperWorker.Supervisor.Constants.Types

  # Queue Configuration - moved here as it's primarily for validation
  @default_queue_length 50
  @min_queue_length 1
  @max_queue_length 10_000

  @doc "Default queue length for chains."
  @spec default_queue_length() :: non_neg_integer()
  def default_queue_length, do: @default_queue_length

  @doc "Minimum queue length for chains."
  @spec min_queue_length() :: non_neg_integer()
  def min_queue_length, do: @min_queue_length

  @doc "Maximum queue length for chains."
  @spec max_queue_length() :: non_neg_integer()
  def max_queue_length, do: @max_queue_length

  @doc """
  Calculates the default number of partitions based on the number of online schedulers.
  """
  @spec default_partitions() :: pos_integer()
  def default_partitions do
    System.schedulers_online()
  end

  @doc """
  Validates if a given restart strategy is valid for a specific worker type.
  """
  @spec valid_restart_strategy?(atom(), :group | :chain | :standalone) :: boolean()
  def valid_restart_strategy?(strategy, :group) do
    strategy in Strategies.group_restart_strategies()
  end

  def valid_restart_strategy?(strategy, :chain) do
    strategy in Strategies.chain_restart_strategies()
  end

  def valid_restart_strategy?(strategy, :standalone) do
    strategy in Strategies.standalone_restart_strategies()
  end

  def valid_restart_strategy?(_strategy, _type), do: false

  @doc """
  Validates if a given send type is valid for chains.
  """
  @spec valid_send_type?(atom()) :: boolean()
  def valid_send_type?(send_type) do
    send_type in Types.chain_send_types()
  end

  @doc """
  Validates if a given worker type is valid.
  """
  @spec valid_worker_type?(atom()) :: boolean()
  def valid_worker_type?(type) do
    type in Types.worker_types()
  end

  @doc """
  Validates if a given queue length is within the allowed range.
  """
  @spec valid_queue_length?(integer()) :: boolean()
  def valid_queue_length?(length) when is_integer(length) do
    length >= min_queue_length() and length <= max_queue_length()
  end

  def valid_queue_length?(_), do: false

  @doc """
  Validates if a given shutdown type is valid.
  """
  @spec valid_shutdown_type?(atom()) :: boolean()
  def valid_shutdown_type?(type) do
    type in Types.shutdown_types()
  end
end
