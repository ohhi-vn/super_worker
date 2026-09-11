defmodule SuperWorker.Supervisor.WorkerContextTest do
  use ExUnit.Case, async: false

  alias SuperWorker.Supervisor, as: Sup

  @moduletag :capture_log

  setup do
    sup_id = :"worker_ctx_#{System.unique_integer([:positive])}"
    {:ok, _} = Sup.start_with_config(link: false, id: sup_id, num_partitions: 1)
    on_exit(fn -> if Sup.running?(sup_id), do: Sup.stop(sup_id) end)

    %{sup_id: sup_id}
  end

  test "standalone worker sees its supervisor but no group", %{sup_id: sup_id} do
    parent = self()

    fun = fn ->
      send(parent, {:context, Sup.get_my_supervisor(), Sup.get_my_group()})
      MyTest.loop(:ctx)
    end

    {:ok, _} = Sup.add_standalone_worker(sup_id, fun, id: :solo)

    assert_receive {:context, ^sup_id, nil}, 2_000
  end

  test "group worker sees its group and can talk to peers", %{sup_id: sup_id} do
    parent = self()
    group_id = :"g_ctx_#{System.unique_integer([:positive])}"

    {:ok, ^group_id} =
      Sup.add_group(sup_id, id: group_id, restart_strategy: :one_for_one)

    reporter = fn ->
      send(parent, {:group_context, Sup.get_my_supervisor(), Sup.get_my_group()})
      MyTest.loop(:reporter)
    end

    listener = fn ->
      receive do
        msg -> send(parent, {:peer_got, msg})
      after
        3_000 -> :timeout
      end

      MyTest.loop(:listener)
    end

    {:ok, _} = Sup.add_group_worker(sup_id, group_id, listener, id: :listener)
    {:ok, _} = Sup.add_group_worker(sup_id, group_id, reporter, id: :reporter)

    assert_receive {:group_context, ^sup_id, ^group_id}, 2_000
  end

  test "my-group helpers return errors outside of a worker" do
    assert {:error, :group_not_found} = Sup.broadcast_to_my_group(:data)
    assert {:error, :group_not_found} = Sup.send_to_my_group(:who, :data)
    assert {:error, :group_not_found} = Sup.send_to_my_group_random(:data)
    refute Sup.get_my_group()
    refute Sup.get_my_supervisor()
  end

  test "stop/3 with an unsupported shutdown type still terminates the supervisor", %{
    sup_id: sup_id
  } do
    group_id = :"g_stop_#{System.unique_integer([:positive])}"
    worker_id = :stopme

    {:ok, ^group_id} =
      Sup.add_group(sup_id, id: group_id, restart_strategy: :one_for_one)

    {:ok, ^worker_id} =
      Sup.add_group_worker(sup_id, group_id, {MyTest, :loop, [worker_id]}, id: worker_id)

    Process.sleep(50)

    assert :ok = Sup.stop(sup_id, :normal)
    Process.sleep(100)

    refute Sup.running?(sup_id)
  end
end
