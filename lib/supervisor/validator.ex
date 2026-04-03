defmodule SuperWorker.Supervisor.Validator do
  @moduledoc """
  Utility functions for SuperWorker.Supervisor.

  This module provides common utility functions used across the supervisor
  implementation, including:

  - Option normalization and validation
  - API call helpers
  - Hashing and partitioning utilities
  - Process information helpers
  """

  @sup_params [:id, :num_partitions, :link, :report_to, :children]

  alias SuperWorker.Supervisor

  require Logger

  # ============================================================================
  # Type Definitions
  # ============================================================================

  @type api_result :: {:ok, any()} | {:error, any()}
  @type timeout_ms :: non_neg_integer() | :infinity

  def validate_and_convert(options) do
    with {:ok, opts} <- normalize_options(options, @sup_params),
         {:ok, opts} <- default_sup_options(opts),
         {:ok, opts} <- validate_options(opts),
         {:ok, sup} <- to_struct(opts) do
      {:ok, sup}
    end
  end

  @spec normalize_options([keyword() | atom()], [atom()]) :: api_result()
  def normalize_options(opts, allowed_params) when is_list(opts) and is_list(allowed_params) do
    do_normalize_opts(opts, allowed_params, %{}, [])
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
    case Map.fetch(opts, key) do
      :error ->
        Logger.warning("SuperWorker, Utils, option #{inspect(key)} not found")
        {:error, :invalid_type}

      {:ok, value} ->
        if validator_fun.(value) do
          {:ok, opts}
        else
          Logger.warning("SuperWorker, Utils, option #{inspect(key)} has invalid type")
          {:error, :invalid_type}
        end
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

  # Set the default options if not provided.
  defp default_sup_options(options) do
    options =
      if Map.has_key?(options, :num_partitions) do
        options
      else
        Map.put(options, :num_partitions, :erlang.system_info(:schedulers_online))
      end

    options =
      if Map.has_key?(options, :link) do
        options
      else
        Map.put(options, :link, true)
      end

    options =
      if Map.has_key?(options, :name) do
        options
      else
        Map.put(options, :name, SuperWorker.Supervisor)
      end

    options =
      options
      |> Map.put(:master, options.id)
      |> Map.put(:owner, self())

    {:ok, options}
  end

  # Validate the type & value of options.
  defp validate_options(options) do
    with {:ok, opts} <- check_type(options, :id, &is_atom/1),
         {:ok, opts} <- check_type(opts, :num_partitions, &is_integer/1),
         {:ok, opts} <- check_type(opts, :num_partitions, &(&1 > 0)),
         {:ok, opts} <- check_type(opts, :owner, &is_pid/1),
         {:ok, opts} <- check_type(opts, :link, &is_boolean/1) do
      {:ok, opts}
    else
      {:error, reason} = error ->
        Logger.error("SuperWorker, Supervisor, error in validating options: #{inspect(reason)}")
        error
    end
  end

  defp to_struct(map_options) when is_map(map_options) do
    fields =
      %Supervisor{id: nil}
      |> Map.from_struct()
      |> Map.keys()

    result =
      Enum.reduce(fields, %Supervisor{id: nil}, fn field, acc ->
        if Map.has_key?(map_options, field) do
          %{acc | field => Map.get(map_options, field)}
        else
          acc
        end
      end)

    {:ok, result}
  end
end
