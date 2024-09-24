defmodule SuperWorker.SupervisorTest do
  use ExUnit.Case, async: false

  alias SuperWorker.Supervisor, as: Sup
  alias SuperWorker.Supervisor.{Group, Chain, Worker}

  @group {:group1, "test group"}
  @sup_id :sup_test
  @chain :chain1

  setup_all do
    :ok
  end

  setup do
    # ensure sup with id is not running from last test case.
    if Sup.is_running?(@sup_id) do
      Sup.stop(@sup_id)

    end
    Process.sleep(100)
    :ok
  end

  @tag :supervisor_start
  test "start/stop supervisor, no linked process" do
    {:ok, _} = Sup.start([link: false, id: @sup_id])
    Process.sleep(100)
    assert true == Sup.is_running?(@sup_id)
    Sup.stop(@sup_id)
    Process.sleep(100)
    assert false == Sup.is_running?(@sup_id)
  end

  @tag :supervisor_check_duplicate_id
  test "duplicated supervisor's id" do
    {:ok, _} = Sup.start([link: false, id: @sup_id])
    Process.sleep(100)
    {:error, _} = Sup.start([link: false, id: @sup_id])
    assert true == Sup.is_running?(@sup_id)
  end

  @tag :supervisor_start_link_1
  test "start supervisor with link" do
    {:ok, _} = Sup.start([link: true, id: @sup_id])
    Process.sleep(100)
    assert true == Sup.is_running?(@sup_id)
  end

  @tag :supervisor_start_link_2
  test "start supervisor with link, process still alive if linked process exit :normal" do
    pid = spawn fn ->
      {:ok, _} = Sup.start([link: true, id: @sup_id])
      receive do
        {from, :exit, reason} ->
          send from, {:ok, from}
          exit(reason)
      end
    end

    me = self()
    send pid, {me, :exit, :normal}
    send_result =
    receive do
      {:ok, ^me} -> :ok
    after 1_000 -> :timed_out
    end

    assert send_result == :ok
    assert true == Sup.is_running?(@sup_id)
  end

  @tag :supervisor_start_link_3
  test "start supervisor with linked process, expect supervisor is crashed follow crashed process" do
    pid = spawn fn ->
      {:ok, _} = Sup.start([link: true, id: @sup_id])
      receive do
        {from, :crash} ->
          send(from, {:ok, from})
          Process.sleep(100)
          raise "receive crash command from #{inspect from}"
      end
    end

    Process.sleep(100)

    me = self()
    send pid, {me, :crash}
    send_result =
    receive do
      {:ok, ^me} -> :ok
      msg ->
        IO.inspect msg
        msg
    after 1_000 -> :timed_out
    end
    assert send_result == :ok
    Process.sleep(100)

    assert false == Sup.is_running?(@sup_id)
  end

  @tag :supervisor_children_crash_follow_supervisor
  test "children crash follow supervisor" do
    pid = spawn fn ->
      {:ok, _} = Sup.start([link: true, id: @sup_id])
      {:ok, _} = Sup.add_group(@sup_id, [id: :group1, restart_strategy: :one_for_one])
      {:ok, _} = Sup.add_group_worker(@sup_id, :group1, {__MODULE__, :loop, [:w1]}, [id: :w1])
      receive do
        {from, :crash} ->
          send(from, {:ok, from})
          Process.sleep(100)
          raise "receive crash command from #{inspect from}"
      end
    end

    Process.sleep(100)

    assert true == Sup.is_running?(@sup_id)

    me = self()
    send pid, {me, :crash}
    send_result = loop_receiver()
    assert send_result == false
    assert false == Sup.is_running?(@sup_id)
  end

  # Basic loop, receive messages and print them.
  def loop(pid) do
    prefix = "[#{inspect Process.get({:supervisor, :worker_id})}, #{inspect self()}]"
    receive do
      {:ping, sender} ->
        IO.puts prefix <> " Pong to #{inspect sender}"
        send(sender, {:pong, self()})

      msg -> IO.puts prefix <> " task received: #{inspect msg}"
    end

    loop(pid)
  end

  # Basic loop, receive messages and print them.
  def loop2(pid) do
    prefix = "[#{inspect Process.get({:supervisor, :worker_id})}, #{inspect self()}]"
    send(pid, {:ping, self()})
    Process.sleep(100)
    loop(pid)
  end

  # Continues receive message from sender.
  def loop_receiver() do
    receive do
      {:ping, sender} ->
        IO.puts "Ping from #{inspect sender}"
        loop_receiver()
    after 1_000 -> false
    end
  end
end
