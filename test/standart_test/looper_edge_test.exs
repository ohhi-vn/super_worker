defmodule SuperWorker.Supervisor.LooperTest do
  @moduledoc """
  Tests for partition-loop paths that are hard to hit from the public API:
  shutdown worker kills, restart-skip on normal exits, gen_server start
  failures, and restart requests for missing groups.
  """

  use ExUnit.Case, async: false

  alias SuperWorker.Supervisor, as: Sup

  setup do
    sup_id = :"sup_looper_#{System.unique_integer([:positive])}"
    {:ok, _} = Sup.start_with_config(link: false, id: sup_id, num_partitions: 1)

    on_exit(fn ->
      if Sup.running?(sup_id), do: Sup.stop(sup_id)
    end)

    %{sup_id: sup_id}
  end

  test "stop kills group, chain and standalone workers", %{sup_id: sup_id} do
    group_id = make_ref()
    chain_id = make_ref()

    {:ok, _} = Sup.add_group(sup_id, id: group_id, restart_strategy: :one_for_one)
    {:ok, _} = Sup.add_group_worker(sup_id, group_id, {MyTest, :loop, [1]}, id: 1)
    {:ok, _} = Sup.add_chain(sup_id, id: chain_id)
    {:ok, _} = Sup.add_chain_worker(sup_id, chain_id, fn data -> {:next, data} end, id: 1)
    {:ok, _} = Sup.add_standalone_worker(sup_id, {MyTest, :loop, [2]}, id: :standalone_1)

    {:ok, group_pid} = Sup.get_pid_group_worker(sup_id, group_id, 1)
    {:ok, chain_pid} = Sup.get_pid_chain_worker(sup_id, chain_id, 1)
    {:ok, standalone_pid} = Sup.get_pid_standalone_worker(sup_id, :standalone_1)

    assert Sup.stop(sup_id) in [:ok, {:ok, sup_id}]
    wait_until(fn -> not Sup.running?(sup_id) end)

    wait_until(fn -> not Process.alive?(group_pid) end)
    wait_until(fn -> not Process.alive?(chain_pid) end)
    wait_until(fn -> not Process.alive?(standalone_pid) end)
  end

  test "a one_for_one chain worker exiting normally is not restarted", %{sup_id: sup_id} do
    chain_id = make_ref()
    {:ok, _} = Sup.add_chain(sup_id, id: chain_id)
    {:ok, _} = Sup.add_chain_worker(sup_id, chain_id, {MyTest, :loop, [1]}, id: 1)

    {:ok, pid} = Sup.get_pid_chain_worker(sup_id, chain_id, 1)

    # {:stop, chain_id} makes the worker exit with :normal.
    send(pid, {:stop, chain_id})
    Process.sleep(200)

    # The worker is gone and stays gone (no restart).
    assert match?({:error, _}, Sup.get_pid_chain_worker(sup_id, chain_id, 1))
  end

  test "a one_for_all chain worker exiting normally is not restarted", %{sup_id: sup_id} do
    chain_id = make_ref()
    {:ok, _} = Sup.add_chain(sup_id, id: chain_id, restart_strategy: :one_for_all)
    {:ok, _} = Sup.add_chain_worker(sup_id, chain_id, {MyTest, :loop, [1]}, id: 1)
    {:ok, _} = Sup.add_chain_worker(sup_id, chain_id, {MyTest, :loop, [2]}, id: 2)

    {:ok, pid} = Sup.get_pid_chain_worker(sup_id, chain_id, 1)

    send(pid, {:stop, chain_id})
    Process.sleep(200)

    assert match?({:error, _}, Sup.get_pid_chain_worker(sup_id, chain_id, 1))
    assert match?({:ok, _}, Sup.get_pid_chain_worker(sup_id, chain_id, 2))
  end

  test "a one_for_all group worker exiting normally is not restarted", %{sup_id: sup_id} do
    group_id = make_ref()
    {:ok, _} = Sup.add_group(sup_id, id: group_id, restart_strategy: :one_for_all)
    {:ok, _} = Sup.add_group_worker(sup_id, group_id, {MyGenServer, []}, id: 1)

    {:ok, pid} = Sup.get_pid_group_worker(sup_id, group_id, 1)

    # MyGenServer stops normally on :stop_normal.
    send(pid, :stop_normal)
    Process.sleep(200)

    assert match?({:error, _}, Sup.get_pid_group_worker(sup_id, group_id, 1))
  end

  test "standalone gen_server worker whose start returns an error is not registered", %{
    sup_id: sup_id
  } do
    name = :"looper_taken_name_#{System.unique_integer([:positive])}"
    {:ok, pid} = MyGenServer.start_link(name: name)
    on_exit(fn -> Process.exit(pid, :kill) end)

    # Starting another instance with the same name fails; the partition
    # swallows the failure and reports a logical :ok, but no worker is
    # registered (documented current behaviour).
    assert {:ok, _} = Sup.add_standalone_worker(sup_id, {MyGenServer, [name: name]})

    Process.sleep(100)
    assert match?({:error, _}, Sup.get_pid_standalone_worker(sup_id, MyGenServer))
  end

  test "restart_group_worker for a missing group returns an error", %{sup_id: sup_id} do
    assert match?({:error, _}, Sup.restart_group_worker(sup_id, :missing_group, 1))
  end

  test "restart_group for a missing group returns an error", %{sup_id: sup_id} do
    assert match?({:error, _}, Sup.restart_group(sup_id, :missing_group))
  end

  test "internal restart request for a missing group is logged and ignored", %{sup_id: sup_id} do
    alias SuperWorker.Supervisor.Message

    partition = sup_id |> :sys.get_state() |> Map.fetch!(:partitions) |> Map.values() |> hd()

    message = Message.new(:restart_group_worker, partition, {:ghost, :missing_group})
    send(partition, {:internal_api, message})

    # The partition survives and still serves API calls.
    Process.sleep(100)
    assert Sup.running?(sup_id)
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
