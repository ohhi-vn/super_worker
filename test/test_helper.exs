ExUnit.start()

defmodule MyGenServer do
  use GenServer, restart: :permanent

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, :ok, opts)
  end

  def get(pid, key) do
    GenServer.call(pid, {:get, key})
  end

  def put(pid, key, value) do
    GenServer.cast(pid, {:put, key, value})
  end

  def delete(pid, key) do
    GenServer.cast(pid, {:delete, key})
  end

  def init(:ok) do
    {:ok, %{}}
  end

  def handle_call({:get, key}, _from, state) do
    {:reply, Map.get(state, key), state}
  end

  def handle_cast({:put, key, value}, state) do
    {:noreply, Map.put(state, key, value)}
  end

  def handle_cast({:delete, key}, state) do
    {:noreply, Map.delete(state, key)}
  end

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

  def handle_info(msg, state) do
    IO.puts("Received message: #{inspect(msg)}")
    {:noreply, state}
  end
end
