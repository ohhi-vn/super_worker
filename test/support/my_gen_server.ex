defmodule MyGenServer do
  @moduledoc false
  use GenServer, restart: :permanent

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, :ok, opts)
  end

  def get(pid, key) do
    GenServer.call(pid, {:get, key})
  end

  def crash(pid) do
    GenServer.cast(pid, :crash)
  end

  def put(pid, key, value) do
    GenServer.cast(pid, {:put, key, value})
  end

  def delete(pid, key) do
    GenServer.cast(pid, {:delete, key})
  end

  @impl true
  def init(:ok) do
    {:ok, %{}}
  end

  @impl true
  def handle_call({:get, key}, _from, state) do
    {:reply, Map.get(state, key), state}
  end

  @impl true
  def handle_cast({:put, key, value}, state) do
    {:noreply, Map.put(state, key, value)}
  end

  def handle_cast({:delete, key}, state) do
    {:noreply, Map.delete(state, key)}
  end

  def handle_cast(:crash, state) do
    raise "I'm crashing"
    {:noreply, state}
  end

  @impl true
  def handle_info({:ping, from} = msg, state) do
    IO.puts("Received ping message: #{inspect(msg)}")
    send(from, {:pong, self()})
    {:noreply, state}
  end

  def handle_info(:crash, state) do
    IO.puts("Received raise an error message")
    raise "I'm raising an error"
    {:noreply, state}
  end

  def handle_info(:stop_normal, state) do
    IO.puts("Received a stop normal message")
    {:stop, :normal, state}
  end

  def handle_info(msg, state) do
    IO.puts("Received message: #{inspect(msg)}")
    {:noreply, state}
  end

  @impl true
  def terminate(reason, _state) do
    IO.puts("stop, reason: #{inspect(reason)}")
    :ok
  end
end
