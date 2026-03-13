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

  def task(n, sleep \\ 100) when is_integer(n) do
    prefix = "[#{inspect(Process.get({:supervisor, :worker_id}))}, #{inspect(self())}]"
    IO.puts(prefix <> " Task is started, param: #{inspect(n)}")

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
    SuperWorker.Supervisor.send_to_chain(sup_id, chain_id, data)
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
