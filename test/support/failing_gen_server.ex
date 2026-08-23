defmodule FailingGenServer do
  @moduledoc """
  Test support GenServer whose start always fails with `{:error, :cannot_start}`.
  """

  use GenServer

  def child_spec(opts) do
    %{id: __MODULE__, start: {__MODULE__, :start_link, [opts]}}
  end

  def start_link(_opts), do: {:error, :cannot_start}

  @impl true
  def init(arg), do: {:ok, arg}
end
