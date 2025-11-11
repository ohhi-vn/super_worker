defmodule SuperWorker.Supervisor.Utils do
  @moduledoc """
  Utility functions for SuperWorker.Supervisor.

  This module provides common utility functions used across the supervisor
  implementation, including:

  - Hashing and partitioning utilities
  - Process information helpers
  """

  require Logger

  # ============================================================================
  # Hashing and Partitioning
  # ============================================================================

  @doc """
  Computes a hash-based order for partitioning data.

  Uses Erlang's phash2 for consistent hashing across the cluster.

  ## Examples

      iex> order = get_hash_order("my_data", 10)
      iex> order >= 0 and order < 10
      true

  """
  @spec get_hash_order(term(), pos_integer()) :: non_neg_integer()
  def get_hash_order(term, num_partitions)
      when is_integer(num_partitions) and num_partitions > 0 do
    :erlang.phash2(term, num_partitions)
  end

  # ============================================================================
  # System Information
  # ============================================================================

  @doc """
  Returns the number of online schedulers in the system.

  This is typically used as the default number of partitions.
  """
  @spec get_default_schedulers() :: pos_integer()
  def get_default_schedulers do
    System.schedulers_online()
  end

  @doc """
  Counts the number of messages in a process's message queue.

  Returns 0 if the process is not alive.
  """
  @spec count_msgs(pid()) :: non_neg_integer()
  def count_msgs(pid) when is_pid(pid) do
    case Process.info(pid, :message_queue_len) do
      {:message_queue_len, n} when is_integer(n) -> n
      nil -> 0
    end
  end

  # ============================================================================
  # ID Generation
  # ============================================================================

  @doc """
  Generates a cryptographically secure random ID.

  Returns a 32-character hexadecimal string.

  ## Examples

      iex> id = random_id()
      iex> String.length(id)
      32

  """
  @spec random_id() :: String.t()
  def random_id do
    :crypto.strong_rand_bytes(16)
    |> Base.encode16()
  end
end
