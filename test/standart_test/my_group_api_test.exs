defmodule SuperWorker.Supervisor.MyGroupApiTest do
  @moduledoc """
  Tests for the `*_my_group` APIs and the link-to-pid supervisor option.

  The `*_my_group` APIs resolve the caller's group/supervisor from the process
  dictionary, which is exactly what group workers have set; here we simulate
  that from the test process.
  """

  use ExUnit.Case, async: false

  alias SuperWorker.Supervisor, as: Sup

  setup do
    sup_id = :"sup_my_group_#{System.unique_integer([:positive])}"
    {:ok, _} = Sup.start_with_config(link: false, id: sup_id, num_partitions: 2)

    group_id = make_ref()
    {:ok, _} = Sup.add_group(sup_id, id: group_id, restart_strategy: :one_for_one)

    {:ok, _} = Sup.add_group_worker(sup_id, group_id, {MyTest, :loop, [1]}, id: 1)
    {:ok, _} = Sup.add_group_worker(sup_id, group_id, {MyTest, :loop, [2]}, id: 2)

    on_exit(fn ->
      if Sup.running?(sup_id), do: Sup.stop(sup_id)
    end)

    %{sup_id: sup_id, group_id: group_id}
  end

  defp set_my_context(sup_id, group_id) do
    Process.put({:supervisor, :sup_id}, sup_id)
    Process.put({:supervisor, :group_id}, group_id)
  end

  defp clear_my_context do
    Process.delete({:supervisor, :sup_id})
    Process.delete({:supervisor, :group_id})
  end

  test "send_to_my_group delivers to the given worker", %{sup_id: sup_id, group_id: group_id} do
    set_my_context(sup_id, group_id)

    assert :ok = Sup.send_to_my_group(1, {:ping, self()})
    assert_receive {:pong, _pid}, 1_000
  end

  test "broadcast_to_my_group delivers to all workers", %{sup_id: sup_id, group_id: group_id} do
    set_my_context(sup_id, group_id)

    assert {:ok, :sent} = Sup.broadcast_to_my_group({:ping, self()})

    assert_receive {:pong, _}, 1_000
    assert_receive {:pong, _}, 1_000
  end

  test "send_to_my_group_random delivers to one worker", %{sup_id: sup_id, group_id: group_id} do
    set_my_context(sup_id, group_id)

    assert :ok = Sup.send_to_my_group_random({:ping, self()})
    assert_receive {:pong, _}, 1_000
  end

  test "repeated sends hit the cached partition lookup", %{sup_id: sup_id, group_id: group_id} do
    set_my_context(sup_id, group_id)

    assert :ok = Sup.send_to_group_worker(sup_id, group_id, 1, {:ping, self()})
    assert_receive {:pong, _}, 1_000

    # The second call resolves the target partition from the master cache.
    assert :ok = Sup.send_to_group_worker(sup_id, group_id, 1, {:ping, self()})
    assert_receive {:pong, _}, 1_000
  end

  test "my group APIs return error when group id is unknown" do
    clear_my_context()

    assert {:error, :group_not_found} = Sup.send_to_my_group(1, :data)
    assert {:error, :group_not_found} = Sup.broadcast_to_my_group(:data)
    assert {:error, :group_not_found} = Sup.send_to_my_group_random(:data)
  end

  test "my group APIs return error when supervisor id is unknown", %{group_id: group_id} do
    Process.put({:supervisor, :group_id}, group_id)
    Process.delete({:supervisor, :sup_id})

    assert {:error, :supervisor_not_found} = Sup.send_to_my_group(1, :data)
    assert {:error, :supervisor_not_found} = Sup.broadcast_to_my_group(:data)
    assert {:error, :supervisor_not_found} = Sup.send_to_my_group_random(:data)
  end

  test "send APIs return supervisor_not_found when supervisor is not running" do
    sup_id = :"sup_not_running_#{System.unique_integer([:positive])}"

    assert {:error, :supervisor_not_found} = Sup.broadcast_to_group(sup_id, :g, :data)
    assert {:error, :supervisor_not_found} = Sup.send_to_group_worker(sup_id, :g, :w, :data)
    # send_to_group_random short-circuits on running?/1 and returns the raw false
    assert false == Sup.send_to_group_random(sup_id, :g, :data)
  end

  test "supervisor can be linked to a pid" do
    sup_id = :"sup_link_pid_#{System.unique_integer([:positive])}"
    {:ok, _} = Sup.start_with_config(link: self(), id: sup_id, num_partitions: 1)

    assert Sup.running?(sup_id)

    Sup.stop(sup_id)
    wait_until(fn -> not Sup.running?(sup_id) end)
  end

  defp wait_until(fun, tries \\ 50)

  defp wait_until(_fun, 0), do: flunk("condition was not met")

  defp wait_until(fun, tries) do
    if fun.() do
      :ok
    else
      Process.sleep(20)
      wait_until(fun, tries - 1)
    end
  end
end
