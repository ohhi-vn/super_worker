defmodule SuperWorker.SupervisorTest do
  use ExUnit.Case, async: false

  alias SuperWorker.Supervisor, as: Sup
  alias SuperWorker.Supervisor.{Group, Chain, Worker}

  @group {:group1, "test group"}
  @sup_id :sup_test_group
  @chain :chain1

  setup_all do
    :ok
  end

  setup do
    Sup.stop(@sup_id)
    :ok
  end

  test "start supervisor" do
    {:ok, _} = Sup.start([link: false, id: @sup_id])
    assert true
  end

  test "duplicated supervisor's id" do
    {:ok, pid1} = Sup.start([link: false, id: @sup_id])
    {:error, {_, pid2}} = Sup.start([link: false, id: @sup_id])
    assert pid1 == pid2
  end

  test "start supervisor with link" do
    {:ok, _} = Sup.start([link: true, id: @sup_id])
    assert true
  end

  test "start supervisor with link 2" do
    pid = spawn fn ->
      {:ok, _} = Sup.start([link: true, id: @sup_id])
      receive do
        {from, :exit, reason} ->
          send from, {from, :ok}
          exit(reason)
      end
    end

    me = self()
    send pid, {me, :exit, :normal}
    send_result =
    receive do
      {me, :ok} -> :ok
    after 1_000 -> :timed_out
    end

    assert send_result == :ok
    assert !Sup.is_running?(@sup_id)
  end

  test "start supervisor with link 3" do
    pid = spawn fn ->
      {:ok, _} = Sup.start([link: true, id: @sup_id])
      receive do
        {from, :crash, reason} ->
          send from, {from, :ok}
          raise reason
      end
    end

    me = self()
    send pid, {me, :crash, "test crash linked process"}
    send_result =
    receive do
      {me, :ok} -> :ok
    after 1_000 -> :timed_out
    end

    assert send_result == :ok
    assert !Sup.is_running?(@sup_id)
  end
end
