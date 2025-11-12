defmodule SuperWorker.Supervisor.StandaloneTest do
  use ExUnit.Case, async: true

  alias SuperWorker.Supervisor, as: Sup

  @sup_id :sup_group_test

  setup_all do
    {:ok, _} = Sup.start(link: false, id: @sup_id, number_of_partitions: 2)
    :ok
  end

  setup do
    if Sup.is_running?(@sup_id) do
      :ok
    else
      raise "Supervisor is not running"
    end
  end

  @tag :standalone_add_workers
  test "add standalone workers to supervisor" do
    ref = make_ref()

    ids =
      for index <- 1..5 do
        id = {ref, index}

        {:ok, _} =
          Sup.add_standalone_worker(@sup_id, {__MODULE__, :loop, [index]},
            id: id,
            restart_strategy: :permanent
          )

        id
      end

    running_list =
      with {:ok, workers} <- Sup.get_all_standalone_workers(@sup_id) do
        Enum.map(workers, fn worker -> worker.id end)
      else
        _ -> raise "unexpected result"
      end

    Enum.all?(ids, fn id -> Enum.member?(running_list, id) end)
  end

  @tag :standalone_send_data
  test "send data to worker in supervisor" do
    worker_id = make_ref()

    {:ok, _} =
      Sup.add_standalone_worker(@sup_id, {__MODULE__, :loop, [1]},
        id: worker_id,
        restart_strategy: :permanent
      )

    Process.sleep(100)
    Sup.send_to_standalone_worker(@sup_id, worker_id, {:ping, self()})

    result =
      receive do
        {:pong, _sender} -> true
      after
        1_000 -> false
      end

    assert(true == result)
  end

  @tag :standalone_send_data_gen_server
  test "send data to genserver worker in supervisor" do
    worker_id = make_ref()

    {:ok, _} =
      Sup.add_standalone_worker(@sup_id, MyGenServer, id: worker_id)

    Process.sleep(100)
    Sup.send_to_standalone_worker(@sup_id, worker_id, {:ping, self()})

    result =
      receive do
        {:pong, _sender} -> true
      after
        1_000 -> false
      end

    assert(true == result)
  end

  @tag :standalone_restart_gen_server_worker
  test "restart genserver worker in supervisor" do
    worker_id = make_ref()

    {:ok, _} =
      Sup.add_standalone_worker(@sup_id, MyGenServer, id: worker_id, restart_strategy: :permanent)

    Process.sleep(100)
    Sup.send_to_standalone_worker(@sup_id, worker_id, {:ping, self()})

    pid1 =
      receive do
        {:pong, sender} -> sender
      after
        1_000 -> raise "no data return from worker"
      end

    Sup.send_to_standalone_worker(@sup_id, worker_id, :crash)

    Process.sleep(100)
    Sup.send_to_standalone_worker(@sup_id, worker_id, {:ping, self()})

    result =
      receive do
        {:pong, pid2} -> pid1 != pid2
      after
        1_000 -> false
      end

    assert(true == result)
  end

  @tag :standalone_restart_gen_server_worker_2
  test "restart genserver worker in supervisor 2" do
    worker_id = make_ref()

    {:ok, _} =
      Sup.add_standalone_worker(@sup_id, MyGenServer, id: worker_id, restart_strategy: :transient)

    Process.sleep(100)
    Sup.send_to_standalone_worker(@sup_id, worker_id, {:ping, self()})

    pid1 =
      receive do
        {:pong, sender} -> sender
      after
        1_000 -> raise "no data return from worker"
      end

    Sup.send_to_standalone_worker(@sup_id, worker_id, :crash)

    Process.sleep(100)
    Sup.send_to_standalone_worker(@sup_id, worker_id, {:ping, self()})

    result =
      receive do
        {:pong, pid2} -> pid1 != pid2
      after
        1_000 -> false
      end

    assert(true == result)
  end

  @tag :standalone_doesnt_restart_gen_server_worker
  test "doesnt restart genserver worker in supervisor 2" do
    worker_id = make_ref()

    {:ok, _} =
      Sup.add_standalone_worker(@sup_id, MyGenServer, id: worker_id, restart_strategy: :temporary)

    Process.sleep(100)
    Sup.send_to_standalone_worker(@sup_id, worker_id, {:ping, self()})

    pid1 =
      receive do
        {:pong, sender} -> sender
      after
        1_000 -> raise "no data return from worker"
      end

    Sup.send_to_standalone_worker(@sup_id, worker_id, :crash)

    Process.sleep(100)
    Sup.send_to_standalone_worker(@sup_id, worker_id, {:ping, self()})

    result =
      receive do
        {:pong, pid2} -> false
      after
        1_000 -> true
      end

    assert(true == result)
  end

  @tag :standalone_remove_worker
  test "remove standalone worker from supervisor" do
    worker_id = make_ref()

    {:ok, _} =
      Sup.add_standalone_worker(@sup_id, {__MODULE__, :loop, [1]},
        id: worker_id,
        restart_strategy: :permanent
      )

    {:ok, _} = Sup.remove_standalone_worker(@sup_id, worker_id)
    result = Sup.send_to_standalone_worker(@sup_id, worker_id, {:ping, self()})

    assert result == {:error, :not_found}
  end

  @tag :standalone_reuse_id_worker
  test "reuse standalone worker id from supervisor" do
    worker_id = make_ref()

    {:ok, _} =
      Sup.add_standalone_worker(@sup_id, {__MODULE__, :loop, [1]},
        id: worker_id,
        restart_strategy: :permanent
      )

    {:ok, _} = Sup.remove_standalone_worker(@sup_id, worker_id)
    result = Sup.send_to_standalone_worker(@sup_id, worker_id, {:ping, self()})

    assert result == {:error, :not_found}

    {:ok, _} =
      Sup.add_standalone_worker(@sup_id, {__MODULE__, :loop, [1]},
        id: worker_id,
        restart_strategy: :permanent
      )

    Process.sleep(100)
    Sup.send_to_standalone_worker(@sup_id, worker_id, {:ping, self()})

    result =
      receive do
        {:pong, _sender} -> true
      after
        1_000 -> false
      end

    assert(true == result)
  end

  @tag :standalone_restart_worker
  test "restart a worker not affect to others" do
    worker1_id = make_ref()
    worker2_id = make_ref()

    {:ok, _} =
      Sup.add_standalone_worker(@sup_id, {__MODULE__, :loop, [1]},
        id: worker1_id,
        restart_strategy: :permanent
      )

    {:ok, _} =
      Sup.add_standalone_worker(@sup_id, {__MODULE__, :loop, [1]},
        id: worker2_id,
        restart_strategy: :permanent
      )

    Process.sleep(100)
    Sup.send_to_standalone_worker(@sup_id, worker1_id, {:ping, self()})

    result =
      receive do
        {:pong, _sender} -> true
        other -> other
      after
        1_000 -> "verify the worker is started"
      end

    assert(true == result)

    Sup.send_to_standalone_worker(@sup_id, worker2_id, {:store, :test, :hello})
    Sup.send_to_standalone_worker(@sup_id, worker2_id, {:get, :test, self()})

    result =
      receive do
        {:result, :hello} ->
          true

        other ->
          other
      after
        1_000 -> "incorrect result from worker 2"
      end

    assert(true == result)

    Sup.send_to_standalone_worker(@sup_id, worker1_id, {:raise, "Restart all workers"})

    Process.sleep(100)
    Sup.send_to_standalone_worker(@sup_id, worker2_id, {:get, :test, self()})

    result =
      receive do
        {:result, :hello} ->
          true

        other ->
          other
      after
        1_000 -> "get data failed, timeout"
      end

    assert(true == result)
  end

  @tag :standalone_restart_worker2
  test "restart a worker " do
    worker1_id = make_ref()

    {:ok, _} =
      Sup.add_standalone_worker(@sup_id, {__MODULE__, :loop, [1]},
        id: worker1_id,
        restart_strategy: :permanent
      )

    Process.sleep(100)
    Sup.send_to_standalone_worker(@sup_id, worker1_id, {:ping, self()})

    result =
      receive do
        {:pong, _sender} -> true
        other -> other
      after
        1_000 -> "verify the worker is started"
      end

    assert(true == result)

    Sup.send_to_standalone_worker(@sup_id, worker1_id, {:store, :test, :hello})
    Sup.send_to_standalone_worker(@sup_id, worker1_id, {:get, :test, self()})

    result =
      receive do
        {:result, :hello} ->
          true

        other ->
          other
      after
        1_000 -> "incorrect result from worker"
      end

    assert(true == result)

    Sup.send_to_standalone_worker(@sup_id, worker1_id, {:raise, "Restart all workers"})

    Process.sleep(100)
    Sup.send_to_standalone_worker(@sup_id, worker1_id, {:get, :test, self()})

    result =
      receive do
        {:result, nil} ->
          true

        other ->
          other
      after
        1_000 -> "get data failed, timeout"
      end

    assert(true == result)
  end

  ## Helper functions

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
end
