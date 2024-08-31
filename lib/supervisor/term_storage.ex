defmodule SuperWorker.TermStorage do
  @moduledoc """
  A wrapper module for storing terms in :persistent_term.
  Good choice for storing data that is rarely change.
  Data is can acessed by all processes in the same node.

  Key was prefixed with the module name to avoid key collision.

  Note:
  By default, TermStorage will be limited to 1GB of memory.
  If you need to store more data, you can increase the limit by setting the `:persistent_term` option in the Erlang VM.
  """

  require Logger

  @me __MODULE__

  @doc """
  Get the value of the key from the storage.
  """
  def get(key) do
    Logger.debug("Get key: #{inspect key}")
    case :persistent_term.get({@me, key}, nil) do
      nil -> {:error, :not_found}
      value -> {:ok, value}
    end
  end

  @doc """
  Put the value of the key to the storage.
  """
  def put(key, value) do
    :persistent_term.put({@me, key}, value)
  end

  @doc """
  Delete the key from the storage.
  """
  def delete(key) do
    :persistent_term.erase({@me, key})
  end

  @doc """
  Get all key/value in the storage.
  """
  def get_all do
    Enum.filter(:persistent_term.get(),
    fn
      {{mod, _}, _} -> mod == @me
      _ -> false
    end)
  end
end
