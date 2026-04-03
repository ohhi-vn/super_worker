defmodule MyTest do
  @moduledoc """
  Test support module providing worker functions for SuperWorker tests.

  This module provides simple loop workers, chain task workers, and utility
  functions used across the test suite. All IO output has been minimized
  to improve test performance.
  """

  # Basic loop, receive messages and handle them.
  def loop(id) do
    receive do
      {:ping, sender} ->
        send(sender, {:pong, self()})

      {:store, key, data} ->
        Process.put(key, data)

      {:get, key, from} ->
        send(from, {:result, Process.get(key)})

      {:raise, reason} ->
        raise reason

      {:get_pid, from} ->
        send(from, {:pid, self()})

      _msg ->
        :ok
    end

    loop(id)
  end

  @doc """
  A task function for chain workers. Processes numbers 1..n and returns {:next, n + 1}.

  ## Parameters

    * `n` - The upper bound of the range to process
    * `sleep` - Milliseconds to sleep per iteration (default: 1ms for fast tests)

  """
  def task(n, sleep \\ 1) when is_integer(n) do
    sum =
      Enum.reduce(1..n, 0, fn i, acc ->
        if sleep > 0, do: :timer.sleep(sleep)
        acc + i
      end)

    {:next, n + 1}
  end

  @doc """
  A task function that crashes at a specific iteration.

  ## Parameters

    * `n` - The upper bound of the range
    * `at` - The iteration number at which to crash
    * `sleep` - Milliseconds to sleep per iteration (default: 1ms)

  """
  def task_crash(n, at, sleep \\ 1) do
    Enum.reduce(1..n, 0, fn i, acc ->
      if i == at,
        do: raise("Task raised an error at #{i}")

      if sleep > 0, do: :timer.sleep(sleep)
      acc + i
    end)

    {:next, n + 1}
  end

  @doc """
  Sends data to a chain worker.
  """
  def send_to_chain(sup_id, chain_id, data \\ 10) do
    SuperWorker.Supervisor.send_to_chain(sup_id, chain_id, data)
  end

  @doc """
  Returns an anonymous function that simulates work.

  ## Parameters

    * `sleep` - Milliseconds to sleep per iteration (default: 1ms)

  """
  def anonymous(sleep \\ 1) do
    fn ->
      for _i <- 1..5 do
        if sleep > 0, do: :timer.sleep(sleep)
      end

      :done
    end
  end

  @doc """
  A simple ping-pong handler for testing message passing.
  """
  def ping_pong({:ping, sender}) do
    send(sender, {:pong, self()})
  end
end
