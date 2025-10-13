defmodule SuperWorker.Supervisor.Utils do
  @moduledoc """
  Utility functions for SuperWorker.Supervisor.

  This module provides common utility functions used across the supervisor
  implementation, including:

  - Option normalization and validation
  - API call helpers
  - Hashing and partitioning utilities
  - Process information helpers
  """

  require Logger

  # ============================================================================
  # Type Definitions
  # ============================================================================

  @type api_result :: {:ok, any()} | {:error, any()}
  @type timeout_ms :: non_neg_integer() | :infinity

  # ============================================================================
  # Option Normalization
  # ============================================================================

  @doc """
  Normalizes options from keyword list or atom shorthand to a map.

  Validates that all provided options are in the allowed parameters list.
  Returns `{:ok, map}` with normalized options or `{:error, reason}` if
  invalid options are provided.

  ## Examples

      iex> normalize_opts([id: :my_sup, link: true], [:id, :link])
      {:ok, %{id: :my_sup, link: true}}

      iex> normalize_opts([id: :my_sup, invalid: :opt], [:id])
      {:error, {:invalid_options, [:invalid]}}

  """
  @spec normalize_opts([keyword() | atom()], [atom()]) :: api_result()
  def normalize_opts(opts, allowed_params) when is_list(opts) and is_list(allowed_params) do
    do_normalize_opts(opts, allowed_params, %{}, [])
  end

  @doc """
  Adds default owner to options if not present.

  The owner defaults to the calling process.
  """
  @spec generic_default_sup_opts(map()) :: map()
  def generic_default_sup_opts(opts) when is_map(opts) do
    Map.put_new(opts, :owner, self())
  end

  @doc """
  Checks that the value of a key in options passes a validation function.

  ## Examples

      iex> check_type(%{count: 5}, :count, &is_integer/1)
      {:ok, %{count: 5}}

      iex> check_type(%{count: "five"}, :count, &is_integer/1)
      {:error, :invalid_type}

  """
  @spec check_type(map(), atom(), (any() -> boolean())) :: api_result()
  def check_type(opts, key, validator_fun)
      when is_map(opts) and is_atom(key) and is_function(validator_fun, 1) do
    case Map.get(opts, key) do
      nil ->
        Logger.warning("SuperWorker, Utils, option #{inspect(key)} not found")
        {:error, :invalid_type}

      value ->
        if validator_fun.(value) do
          {:ok, opts}
        else
          Logger.warning("SuperWorker, Utils, option #{inspect(key)} has invalid type")
          {:error, :invalid_type}
        end
    end
  end

  # ============================================================================
  # Map/Option Helpers
  # ============================================================================

  @doc """
  Safely retrieves an item from a map.

  Returns `{:ok, value}` if the key exists, `{:error, {:not_found, key}}` otherwise.
  """
  @spec get_item(map(), any()) :: api_result()
  def get_item(map, key) when is_map(map) do
    case Map.get(map, key) do
      nil -> {:error, {:not_found, key}}
      value -> {:ok, value}
    end
  end

  @doc """
  Converts a keyword option shorthand to a full keyword tuple.

  Used for converting shorthand like `:group` to `{:type, :group}`.
  """
  @spec get_keyword(atom()) :: {:type, atom()} | {:error, :invalid_options}
  def get_keyword(:group), do: {:type, :group}
  def get_keyword(:chain), do: {:type, :chain}
  def get_keyword(:standalone), do: {:type, :standalone}
  def get_keyword(_), do: {:error, :invalid_options}

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

  # ============================================================================
  # API Communication Helpers
  # ============================================================================

  @doc """
  Creates a reference tuple for API responses.

  Returns `{pid, reference}` where pid is the calling process
  and reference is a unique reference for this call.
  """
  @spec response_ref() :: {pid(), reference()}
  def response_ref do
    {self(), make_ref()}
  end

  @doc """
  Waits for an API response matching the given reference.

  Blocks until a message with the matching reference is received
  or the timeout expires.

  ## Examples

      ref = response_ref()
      # ... send message with ref ...
      api_receiver(ref, 5000)

  """
  @spec api_receiver({pid(), reference()}, timeout_ms()) :: any()
  def api_receiver({_from, ref}, timeout) when is_reference(ref) do
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
  @spec api_response({pid(), reference()}, any()) :: any()
  def api_response({from, ref}, result) when is_pid(from) and is_reference(ref) do
    send(from, {ref, result})
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

    ref = response_ref()
    send(target, {api, ref, params})
    api_receiver(ref, timeout)
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
    {_, ref} = response_ref()
    send(target, {api, ref, params})
    ref
  end

  # ============================================================================
  # Private Functions
  # ============================================================================

  # Recursively processes options list, separating valid from invalid options
  @spec do_normalize_opts(
          [keyword() | atom()],
          [atom()],
          map(),
          [atom() | {atom(), any()}]
        ) :: api_result()
  defp do_normalize_opts([], _allowed_params, valid, invalid) do
    if Enum.empty?(invalid) do
      {:ok, valid}
    else
      {:error, {:invalid_options, Enum.reverse(invalid)}}
    end
  end

  # Handle keyword tuple option
  defp do_normalize_opts([{key, value} | rest], allowed_params, valid, invalid)
       when is_atom(key) do
    if key in allowed_params do
      do_normalize_opts(rest, allowed_params, Map.put(valid, key, value), invalid)
    else
      do_normalize_opts(rest, allowed_params, valid, [key | invalid])
    end
  end

  # Handle atom shorthand option (like :group, :chain)
  defp do_normalize_opts([short_opt | rest], allowed_params, valid, invalid)
       when is_atom(short_opt) do
    case get_keyword(short_opt) do
      {:type, value} ->
        if :type in allowed_params do
          do_normalize_opts(rest, allowed_params, Map.put(valid, :type, value), invalid)
        else
          do_normalize_opts(rest, allowed_params, valid, [short_opt | invalid])
        end

      {:error, _} ->
        if short_opt in allowed_params do
          # It's a valid boolean flag
          do_normalize_opts(rest, allowed_params, Map.put(valid, short_opt, true), invalid)
        else
          do_normalize_opts(rest, allowed_params, valid, [short_opt | invalid])
        end
    end
  end

  # Handle any other unexpected option format
  defp do_normalize_opts([opt | rest], allowed_params, valid, invalid) do
    do_normalize_opts(rest, allowed_params, valid, [opt | invalid])
  end
end
