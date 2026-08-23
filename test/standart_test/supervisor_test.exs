defmodule SuperWorker.SupervisorTest do
  use ExUnit.Case, async: false

  alias SuperWorker.Supervisor, as: Sup

  @sup_id :sup_test

  doctest Sup

  setup_all do
    :ok
  end

  setup do
    # ensure sup with id is not running from last test case.
    if Sup.running?(@sup_id) do
      Sup.stop(@sup_id)
      # Wait for supervisor to fully shut down
      wait_until_stopped(@sup_id, 1000)
    end

    :ok
  end

  @tag :supervisor_start
  test "start/stop supervisor, no linked process" do
    {:ok, _} = Sup.start_with_config(link: false, id: @sup_id, num_partitions: 1)

    assert true == Sup.running?(@sup_id)
    Sup.stop(@sup_id)
    Process.sleep(10)

    assert false == Sup.running?(@sup_id)
  end

  @tag :supervisor_start2
  test "start/stop supervisor, no linked process - 2" do
    {:ok, _} = Sup.start_with_config(link: false, id: @sup_id, num_partitions: 10)

    assert true == Sup.running?(@sup_id)
    Sup.stop(@sup_id)
    Process.sleep(10)

    assert false == Sup.running?(@sup_id)
  end

  @tag :supervisor_start2
  test "start/stop multi times supervisor, no linked process - 2" do
    for _ <- 1..10 do
      {:ok, _} = Sup.start_with_config(link: false, id: @sup_id, num_partitions: 10)

      assert true == Sup.running?(@sup_id)
      Sup.stop(@sup_id)
      Process.sleep(10)

      assert false == Sup.running?(@sup_id)
    end
  end

  @tag :supervisor_check_duplicate_id
  test "check duplicated supervisor's id" do
    {:ok, _} = Sup.start_with_config(link: false, id: @sup_id, num_partitions: 1)
    {:error, _} = Sup.start_with_config(link: false, id: @sup_id, num_partitions: 1)
    assert true == Sup.running?(@sup_id)
  end

  @tag :supervisor_check_duplicate_id2
  test "check duplicated supervisor's id - 2" do
    {:ok, _} = Sup.start_with_config(link: false, id: @sup_id, num_partitions: 10)
    {:error, _} = Sup.start_with_config(link: false, id: @sup_id, num_partitions: 10)
    assert true == Sup.running?(@sup_id)
  end

  @tag :supervisor_start_link_1
  test "start supervisor with link" do
    {:ok, _} = Sup.start_with_config(link: true, id: @sup_id, num_partitions: 1)
    result = Sup.running?(@sup_id)

    assert true == result
    Sup.stop(@sup_id)
    Process.sleep(10)
    assert false == Sup.running?(@sup_id)
  end

  @tag :supervisor_start_link_1a
  test "start_link supervisor with - a" do
    {:ok, _} = Sup.start_with_config(link: true, id: @sup_id, num_partitions: 10)
    result = Sup.running?(@sup_id)

    assert true == result
    Sup.stop(@sup_id)
    Process.sleep(100)
    assert not Sup.running?(@sup_id)
  end

  @tag :supervisor_start_link_2
  test "start supervisor with link, process still alive if linked process exit :normal - a" do
    pid =
      spawn(fn ->
        {:ok, _} = Sup.start_with_config(link: true, id: @sup_id, num_partitions: 1)

        receive do
          {from, :exit, reason} ->
            send(from, {:ok, from})
            exit(reason)
        end
      end)

    me = self()
    send(pid, {me, :exit, :normal})

    send_result =
      receive do
        {:ok, ^me} -> :ok
      after
        1_000 -> :timed_out
      end

    assert send_result == :ok
    assert true == Sup.running?(@sup_id)
    Sup.stop(@sup_id)
    Process.sleep(1)
    assert false == Sup.running?(@sup_id)
  end

  @tag :supervisor_start_link_2a
  test "start supervisor with link, process still alive if linked process exit :normal - 2a" do
    pid =
      spawn(fn ->
        {:ok, _} = Sup.start_with_config(link: true, id: @sup_id, num_partitions: 3)

        receive do
          {from, :exit, reason} ->
            send(from, {:ok, from})
            exit(reason)
        end
      end)

    me = self()
    send(pid, {me, :exit, :normal})

    send_result =
      receive do
        {:ok, ^me} -> :ok
      after
        1_000 -> :timed_out
      end

    assert send_result == :ok
    assert true == Sup.running?(@sup_id)
    Sup.stop(@sup_id)
    Process.sleep(1)
    assert false == Sup.running?(@sup_id)
  end

  @tag :supervisor_start_link_3
  test "start supervisor with linked process, expected the supervisor is crashed follow crashed process" do
    pid =
      spawn(fn ->
        {:ok, _} = Sup.start_with_config(link: true, id: @sup_id, num_partitions: 1)

        receive do
          {from, :crash} ->
            IO.puts("#{inspect(self())}, receive crash command from #{inspect(from)}")
            send(from, {:ok, from})
            raise "#{inspect(self())}, receive crash command from #{inspect(from)}"
        end
      end)

    Process.sleep(100)

    send(pid, {self(), :crash})

    send_result =
      receive do
        {:ok, _} ->
          :ok

        msg ->
          IO.inspect(msg)
          msg
      after
        1_000 -> :timed_out
      end

    assert send_result == :ok
    Process.sleep(100)

    assert false == Sup.running?(@sup_id)
  end

  @tag :supervisor_start_link_3a
  test "start supervisor with linked process, expected the supervisor is crashed follow crashed process - 3a" do
    pid =
      spawn(fn ->
        {:ok, _} = Sup.start_with_config(link: true, id: @sup_id, num_partitions: 10)

        receive do
          {from, :crash} ->
            IO.puts("#{inspect(self())}, receive crash command from #{inspect(from)}")
            send(from, {:ok, from})
            raise "#{inspect(self())}, receive crash command from #{inspect(from)}"
        end
      end)

    Process.sleep(100)

    send(pid, {self(), :crash})

    send_result =
      receive do
        {:ok, _} ->
          :ok

        msg ->
          IO.inspect(msg)
          msg
      after
        1_000 -> :timed_out
      end

    assert send_result == :ok
    Process.sleep(100)

    assert false == Sup.running?(@sup_id)
  end

  @tag :supervisor_children_crash_follow_supervisor
  test "children crash follow supervisor" do
    pid =
      spawn(fn ->
        {:ok, _} = Sup.start_with_config(link: true, id: @sup_id, num_partitions: 1)
        {:ok, _} = Sup.add_group(@sup_id, id: :group1, restart_strategy: :one_for_one)
        {:ok, _} = Sup.add_group_worker(@sup_id, :group1, {__MODULE__, :loop, [:w1]}, id: :w1)

        receive do
          {from, :crash} ->
            send(from, {:ok, from})
            Process.sleep(100)
            raise "receive crash command from #{inspect(from)}"
        end
      end)

    # wait for spawned process to start supervisor and add group.
    Process.sleep(100)

    assert true == Sup.running?(@sup_id)

    me = self()
    send(pid, {me, :crash})
    send_result = loop_receiver()
    assert send_result == false

    # Wait for supervisor to fully crash and clean up
    wait_until_stopped(@sup_id, 2000)
    assert false == Sup.running?(@sup_id)
  end

  @tag :supervisor_children_crash_follow_supervisor2
  test "children crash follow supervisor - 2" do
    pid =
      spawn(fn ->
        {:ok, _} = Sup.start_with_config(link: true, id: @sup_id, num_partitions: 10)
        {:ok, _} = Sup.add_group(@sup_id, id: :group1, restart_strategy: :one_for_one)
        {:ok, _} = Sup.add_group_worker(@sup_id, :group1, {__MODULE__, :loop, [:w1]}, id: :w1)

        receive do
          {from, :crash} ->
            send(from, {:ok, from})
            Process.sleep(100)
            raise "receive crash command from #{inspect(from)}"
        end
      end)

    # wait for spawned process to start supervisor and add group.
    Process.sleep(100)

    assert true == Sup.running?(@sup_id)

    me = self()
    send(pid, {me, :crash})
    send_result = loop_receiver()
    assert send_result == false

    # Wait for supervisor to fully crash and clean up
    wait_until_stopped(@sup_id, 2000)
    assert false == Sup.running?(@sup_id)
  end

  @tag :supervisor_reuse_id_after_stop
  test "re-use id after stop" do
    sup_id = :test_reuse
    {:ok, _} = Sup.start_with_config(link: false, id: sup_id, num_partitions: 1)
    assert true == Sup.running?(sup_id)

    result = Sup.start_with_config(link: false, id: sup_id, num_partitions: 1)

    assert(match?({:error, _}, result))

    Sup.stop(sup_id)
    Process.sleep(1)
    assert false == Sup.running?(sup_id)

    {:ok, _} = Sup.start_with_config(link: false, id: sup_id, num_partitions: 1)
    assert true == Sup.running?(sup_id)
    Sup.stop(sup_id)
    Process.sleep(1)
    {:ok, _} = Sup.start_with_config(link: false, id: sup_id, num_partitions: 1)
    assert true == Sup.running?(sup_id)
    Sup.stop(sup_id)
    Process.sleep(1)
    assert false == Sup.running?(sup_id)
  end

  @tag :supervisor_reuse_id_after_stop2
  test "re-use id after stop - 2" do
    sup_id = :test_reuse
    {:ok, _} = Sup.start_with_config(link: false, id: sup_id, num_partitions: 10)
    assert true == Sup.running?(sup_id)

    result = Sup.start_with_config(link: false, id: sup_id, num_partitions: 10)

    assert(match?({:error, _}, result))

    Sup.stop(sup_id)
    Process.sleep(100)
    assert false == Sup.running?(sup_id)

    {:ok, _} = Sup.start_with_config(link: false, id: sup_id, num_partitions: 10)
    assert true == Sup.running?(sup_id)
    Sup.stop(sup_id)
    :ok = wait_until_stopped(sup_id, 3_000)

    {:ok, _} = Sup.start_with_config(link: false, id: sup_id, num_partitions: 10)
    assert true == Sup.running?(sup_id)
    Sup.stop(sup_id)
    :ok = wait_until_stopped(sup_id, 3_000)
    assert false == Sup.running?(sup_id)
  end

  # Basic loop, receive messages and print them.
  def loop(pid) do
    prefix = "[#{inspect(Process.get({:supervisor, :worker_id}))}, #{inspect(self())}]"

    receive do
      {:ping, sender} ->
        IO.puts(prefix <> " Pong to #{inspect(sender)}")
        send(sender, {:pong, self()})

      msg ->
        IO.puts(prefix <> " task received: #{inspect(msg)}")
    end

    loop(pid)
  end

  # Basic loop, receive messages and print them.
  def loop2(pid) do
    send(pid, {:ping, self()})
    Process.sleep(100)
    loop(pid)
  end

  # Continues receive message from sender.
  def loop_receiver() do
    receive do
      {:ping, sender} ->
        IO.puts("Ping from #{inspect(sender)}")
        loop_receiver()
    after
      1_000 -> false
    end
  end

  # Helper function to wait until supervisor is stopped
  defp wait_until_stopped(_sup_id, timeout) when timeout <= 0 do
    :timeout
  end

  defp wait_until_stopped(sup_id, timeout) do
    if Sup.running?(sup_id) do
      Process.sleep(50)
      wait_until_stopped(sup_id, timeout - 50)
    else
      :ok
    end
  end

  @tag :supervisor_default_start
  test "start/0 and start_link/0 use the default supervisor name" do
    default_id = SuperWorker.Supervisor

    assert {:ok, _} = Sup.start()
    assert true == Sup.running?(default_id)
    :ok = Sup.stop(default_id)
    Process.sleep(50)

    assert {:ok, _} = Sup.start_link()
    assert true == Sup.running?(default_id)
    :ok = Sup.stop(default_id)
    Process.sleep(50)

    assert false == Sup.running?(default_id)
  end
end
