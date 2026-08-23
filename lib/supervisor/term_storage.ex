defmodule SuperWorker.TermStorage do
  @moduledoc """
  A thin wrapper around `:persistent_term` for storing terms that are rarely
  updated and read from many processes on the node.

  Keys are prefixed with this module's name so they cannot collide with other
  `:persistent_term` users. All operations run in the calling process — there
  is no server process, so reads are as fast as `:persistent_term` lookups.

  Keep in mind that updating a `:persistent_term` key triggers a global GC
  scan on all schedulers; use it for config-like data, not hot counters.

  ## Examples

      iex> TermStorage.put(:my_key, %{a: 1})
      iex> TermStorage.get(:my_key)
      {:ok, %{a: 1}}

      iex> TermStorage.get(:never_put)
      {:error, :not_found}

  """

  require Logger
  require SuperWorker.Log

  @me __MODULE__
  # Sentinel distinguishing "key absent" from a stored `nil` value.
  @not_set :"$super_worker_term_storage_not_set"

  @doc """
  Get the value of the key from the storage.

  Returns `{:ok, value}` (including stored `nil` values) or
  `{:error, :not_found}` when the key was never put.
  """
  @spec get(term()) :: {:ok, term()} | {:error, :not_found}
  def get(key) do
    SuperWorker.Log.debug(fn -> "SuperWorker, TermStorage, get key: #{inspect(key)}" end)

    case :persistent_term.get({@me, key}, @not_set) do
      @not_set -> {:error, :not_found}
      value -> {:ok, value}
    end
  end

  @doc """
  Put the value of the key to the storage.

  Returns `:ok`.
  """
  @spec put(term(), term()) :: :ok
  def put(key, value) do
    :persistent_term.put({@me, key}, value)
  end

  @doc """
  Delete the key from the storage.
  """
  @spec delete(term()) :: :ok
  def delete(key) do
    :persistent_term.erase({@me, key})
  end

  @doc """
  Get all key/value pairs stored by this module.

  Keys are returned as given to `put/2` (the internal module prefix is
  stripped).
  """
  @spec get_all() :: [{term(), term()}]
  def get_all do
    Enum.flat_map(
      :persistent_term.get(),
      fn
        {{@me, key}, value} -> [{key, value}]
        _ -> []
      end
    )
  end
end
