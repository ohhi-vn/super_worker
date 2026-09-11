defmodule SuperWorker.Supervisor.Utils do
  @moduledoc """
  Utility functions for SuperWorker.Supervisor.

  This module provides common utility functions used across the supervisor
  implementation, including:

  - Hashing and partitioning utilities
  - Process information helpers
  """

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
    :erlang.phash2(term, num_partitions) + 1
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
  # Safe Invocation
  # ============================================================================

  @doc """
  Invokes a zero-arity function and never lets exceptions escape.

  Returns `{:ok, result}` on success or `{:error, {kind, reason}}` when the
  function throws, raises or exits. Used everywhere user supplied callbacks
  are invoked so a faulty callback cannot take down supervisor processes.

  ## Examples

      iex> safe_call(fn -> 1 + 1 end)
      {:ok, 2}

      iex> match?({:error, {:error, %RuntimeError{}}}, safe_call(fn -> raise "boom" end))
      true

      iex> match?({:error, {:exit, :halt}}, safe_call(fn -> exit(:halt) end))
      true

      iex> safe_call(Enum, :sum, [[1, 2, 3]])
      {:ok, 6}

  """
  @spec safe_call((-> term())) :: {:ok, term()} | {:error, {atom(), term()}}
  def safe_call(fun) when is_function(fun, 0) do
    {:ok, fun.()}
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  @doc """
  Like `safe_call/1` for remote calls: `apply(module, function, arguments)`.

  ## Examples

      iex> safe_call(Enum, :sum, [[1, 2, 3]])
      {:ok, 6}

  """
  @spec safe_call(module(), atom(), [term()]) :: {:ok, term()} | {:error, {atom(), term()}}
  def safe_call(module, function, arguments)
      when is_atom(module) and is_atom(function) and is_list(arguments) do
    safe_call(fn -> apply(module, function, arguments) end)
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
