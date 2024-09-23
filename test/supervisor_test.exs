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
      Process.sleep(100)
    end
    :ok
  end

  test "start/stop supervisor, no linked process" do
    {:ok, _} = Sup.start([link: false, id: @sup_id])
    Process.sleep(100)
    assert true == Sup.is_running?(@sup_id)
    Sup.stop(@sup_id)
    Process.sleep(100)
    assert false == Sup.is_running?(@sup_id)
  end

  test "duplicated supervisor's id" do
    {:ok, _} = Sup.start([link: false, id: @sup_id])
    Process.sleep(100)
    {:error, _} = Sup.start([link: false, id: @sup_id])
    assert true == Sup.is_running?(@sup_id)
  end

  test "start supervisor with link" do
    {:ok, _} = Sup.start([link: true, id: @sup_id])
    Process.sleep(100)
    assert true == Sup.is_running?(@sup_id)
  end

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

    assert false == Sup.is_running?(@sup_id)
  end
end
