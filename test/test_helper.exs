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

defmodule MyTest do
  # Basic loop, receive messages and print them.
  def loop(id) do
    prefix = "[#{inspect(Process.get({:supervisor, :worker_id}))}, #{inspect(self())}]"

    receive do
      {:ping, sender} ->
        IO.puts(prefix <> " Pong to #{inspect(sender)}")
        send(sender, {:pong, self()})

      {:store, key, data} ->
        IO.puts(prefix <> " Store data: #{inspect(data)}")
        Process.put(key, data)

      {:get, key, from} ->
        IO.puts(prefix <> " Get data: #{inspect(Process.get(key))}")
        send(from, {:result, Process.get(key)})

      {:raise, reason} ->
        IO.puts(prefix <> " Raise an error: #{inspect(reason)}")
        raise reason

      {:get_pid, from} ->
        IO.puts(prefix <> " Get pid from: #{inspect(from)}")
        send(from, {:pid, self()})

      msg ->
        IO.puts(prefix <> " task received: #{inspect(msg)}")
    end

    loop(id)
  end

  def task(n, sleep \\ 100) do
    prefix = "[#{inspect(Process.get({:supervisor, :worker_id}))}, #{inspect(self())}]"
    IO.puts(prefix <> " Task is started, param: #{n}")

    sum =
      Enum.reduce(1..n, 0, fn i, acc ->
        :timer.sleep(sleep)
        acc + i
      end)

    IO.puts(IO.puts(prefix <> " Task done, #{sum}"))

    {:next, n + 1}
  end

  def task_crash(n, at, sleep \\ 100) do
    prefix = "[#{inspect(Process.get({:supervisor, :worker_id}))}, #{inspect(self())}]"
    IO.puts(prefix <> " Task is started, param: #{n}")

    sum =
      Enum.reduce(1..n, 0, fn i, acc ->
        if i == at,
          do:
            raise(
              "Task #{inspect(Process.get({:supervisor, :worker_id}))} raised an error at #{i}"
            )

        :timer.sleep(sleep)
        acc + i
      end)

    IO.puts(prefix <> " Task done, #{sum}")

    {:next, n + 1}
  end

  def send_to_chain(sup_id, chain_id, data \\ 10) do
    Sup.send_to_chain(sup_id, chain_id, data)
  end

  # return a anonymous function.
  def anonymous do
    fn ->
      prefix = "[#{inspect(Process.get({:supervisor, :worker_id}))}, #{inspect(self())}]"
      IO.puts(prefix <> " Anonymous function")

      for i <- 1..5 do
        IO.puts(prefix <> " Task #{i}")
        :timer.sleep(100)
      end
    end
  end

  def ping_pong({:ping, sender}) do
    IO.puts("ping_pong(#{inspect(self())}), new task")

    IO.puts(" Pong to #{inspect(sender)}")
    send(sender, {:pong, self()})
  end
end
