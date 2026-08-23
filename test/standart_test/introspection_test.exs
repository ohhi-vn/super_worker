defmodule SuperWorker.Supervisor.IntrospectionTest do
  use ExUnit.Case, async: false

  alias SuperWorker.Supervisor, as: Sup
  alias SuperWorker.TermStorage

  @moduletag :capture_log

  setup do
    sup_id = :"introspection_#{System.unique_integer([:positive])}"
    {:ok, _} = Sup.start_with_config(link: false, id: sup_id, num_partitions: 2)
    on_exit(fn -> if Sup.running?(sup_id), do: Sup.stop(sup_id) end)

    %{sup_id: sup_id}
  end

  describe "running_supervisors/0" do
    test "lists the running supervisor with its pid", %{sup_id: sup_id} do
      assert {^sup_id, pid} = Enum.find(Sup.running_supervisors(), fn {id, _} -> id == sup_id end)
      assert pid == Process.whereis(sup_id)
      assert Process.alive?(pid)
    end

    test "drops entries whose process died without deregistering", %{sup_id: sup_id} do
      ghost_id = :"ghost_sup_#{System.unique_integer([:positive])}"
      ghost_key = {:super_worker_supervisor_registry, ghost_id}

      # Simulate a crashed supervisor that never deregistered.
      TermStorage.put(ghost_key, %{pid: spawn(fn -> :ok end), started_at: 0})
      Process.sleep(10)

      running_ids = Sup.running_supervisors() |> Enum.map(&elem(&1, 0))

      # The stale entry is filtered out...
      refute ghost_id in running_ids
      # ...the live one is kept...
      assert sup_id in running_ids
      # ...and the stale entry was cleaned up.
      assert {:error, :not_found} = TermStorage.get(ghost_key)
    end
  end

  describe "supervisor_info/1" do
    test "reports partition health and counts", %{sup_id: sup_id} do
      group_id = :"g_info_#{System.unique_integer([:positive])}"
      chain_id = :"c_info_#{System.unique_integer([:positive])}"

      {:ok, ^group_id} =
        Sup.add_group(sup_id, id: group_id, restart_strategy: :one_for_one)

      {:ok, _} = Sup.add_group_worker(sup_id, group_id, {MyTest, :loop, [1]}, id: :gw1)
      {:ok, _} = Sup.add_group_worker(sup_id, group_id, {MyTest, :loop, [2]}, id: :gw2)

      {:ok, ^chain_id} = Sup.add_chain(sup_id, id: chain_id)
      {:ok, _} = Sup.add_chain_worker(sup_id, chain_id, {MyTest, :task, [1]}, id: :cw1)

      {:ok, _} = Sup.add_standalone_worker(sup_id, {MyTest, :loop, [:sa]}, id: :sa1)

      Process.sleep(100)

      assert {:ok, info} = Sup.supervisor_info(sup_id)

      assert info.id == sup_id
      assert info.num_partitions == 2
      assert info.num_groups == 1
      assert info.num_chains == 1
      assert info.num_standalone_workers == 1
      # 2 group + 1 chain + 1 standalone worker processes.
      assert info.total_worker_processes == 4

      assert length(info.partitions) == 2
      assert Enum.all?(info.partitions, & &1.alive?)
      assert Enum.all?(info.partitions, &is_integer(&1.message_queue_len))
      assert Enum.map(info.partitions, & &1.id) == Enum.sort(Enum.map(info.partitions, & &1.id))
    end

    test "returns not_running for unknown supervisors" do
      assert {:error, :not_running} =
               Sup.supervisor_info(:"never_started_#{System.unique_integer()}")
    end

    test "reflects partition crash recovery", %{sup_id: sup_id} do
      assert {:ok, info} = Sup.supervisor_info(sup_id)
      victim = hd(info.partitions).pid
      other = info.partitions |> Enum.map(& &1.pid) |> Enum.find(&(&1 != victim))

      Process.exit(victim, :kill)

      wait_until(fn ->
        case Sup.supervisor_info(sup_id) do
          {:ok, fresh} ->
            pids = Enum.map(fresh.partitions, & &1.pid)
            length(pids) == 2 and victim not in pids and Enum.all?(fresh.partitions, & &1.alive?)

          _ ->
            false
        end
      end)

      assert Process.alive?(other)
    end
  end

  describe "list_* functions" do
    test "list_groups returns strategies and worker ids", %{sup_id: sup_id} do
      g1 = :"g_l1_#{System.unique_integer([:positive])}"
      g2 = :"g_l2_#{System.unique_integer([:positive])}"

      {:ok, ^g1} = Sup.add_group(sup_id, id: g1, restart_strategy: :one_for_all)
      {:ok, ^g2} = Sup.add_group(sup_id, id: g2, restart_strategy: :one_for_one)

      {:ok, _} = Sup.add_group_worker(sup_id, g1, {MyTest, :loop, [1]}, id: :w1)
      {:ok, _} = Sup.add_group_worker(sup_id, g1, {MyTest, :loop, [2]}, id: :w2)

      assert {:ok, groups} = Sup.list_groups(sup_id)

      by_id = Map.new(groups, &{&1.id, &1})
      assert by_id[g1].restart_strategy == :one_for_all
      assert MapSet.new(by_id[g1].worker_ids) == MapSet.new([:w1, :w2])
      assert by_id[g2].worker_ids == []
    end

    test "list_chains returns configuration and worker ids", %{sup_id: sup_id} do
      c1 = :"c_l1_#{System.unique_integer([:positive])}"

      {:ok, ^c1} =
        Sup.add_chain(sup_id,
          id: c1,
          restart_strategy: :rest_for_one,
          send_type: :round_robin,
          queue_length: 7
        )

      {:ok, _} = Sup.add_chain_worker(sup_id, c1, {MyTest, :task, [1]}, id: :cw)
      {:ok, _} = Sup.add_chain_worker(sup_id, c1, {MyTest, :task, [2]}, id: :cw2)

      assert {:ok, chains} = Sup.list_chains(sup_id)

      chain = Enum.find(chains, &(&1.id == c1))
      assert chain.restart_strategy == :rest_for_one
      assert chain.send_type == :round_robin
      assert chain.queue_length == 7
      assert MapSet.new(chain.worker_ids) == MapSet.new([:cw, :cw2])
    end

    test "list_standalone_workers returns worker metadata", %{sup_id: sup_id} do
      {:ok, _} =
        Sup.add_standalone_worker(sup_id, {MyTest, :loop, [:x]},
          id: :listed_sa,
          restart_strategy: :permanent
        )

      Process.sleep(50)

      assert {:ok, workers} = Sup.list_standalone_workers(sup_id)
      sa = Enum.find(workers, &(&1.id == :listed_sa))

      assert %{name: nil, restart_strategy: :permanent} = sa
    end

    test "return not_running for dead supervisors" do
      assert {:error, :not_running} = Sup.list_groups(:"nope_#{System.unique_integer()}")
      assert {:error, :not_running} = Sup.list_chains(:"nope_#{System.unique_integer()}")

      assert {:error, :not_running} =
               Sup.list_standalone_workers(:"nope_#{System.unique_integer()}")
    end

    test "lists stay consistent when entities are removed", %{sup_id: sup_id} do
      group_id = :"g_rm_#{System.unique_integer([:positive])}"
      chain_id = :"c_rm_#{System.unique_integer([:positive])}"

      {:ok, ^group_id} =
        Sup.add_group(sup_id, id: group_id, restart_strategy: :one_for_one)

      {:ok, _} = Sup.add_group_worker(sup_id, group_id, {MyTest, :loop, [1]}, id: :gw)
      {:ok, ^chain_id} = Sup.add_chain(sup_id, id: chain_id)

      Process.sleep(50)

      assert {:ok, groups} = Sup.list_groups(sup_id)
      assert hd(groups).worker_ids == [:gw]

      # Removing the worker is reflected immediately.
      {:ok, _} = Sup.remove_group_worker(sup_id, group_id, :gw)
      assert {:ok, groups} = Sup.list_groups(sup_id)
      assert %{worker_ids: []} = Enum.find(groups, &(&1.id == group_id))

      # Removing the whole chain too.
      assert :ok = Sup.remove_chain(sup_id, chain_id)
      assert {:ok, chains} = Sup.list_chains(sup_id)
      refute Enum.any?(chains, &(&1.id == chain_id))
    end
  end

  test "registry is cleaned up when the supervisor stops normally", %{sup_id: sup_id} do
    assert Enum.any?(Sup.running_supervisors(), fn {id, _} -> id == sup_id end)

    :ok = Sup.stop(sup_id)
    wait_until(fn -> not Sup.running?(sup_id) end)

    refute Enum.any?(Sup.running_supervisors(), fn {id, _} -> id == sup_id end)
  end

  defp wait_until(fun, tries \\ 100) do
    if fun.() do
      :ok
    else
      if tries <= 0, do: flunk("wait_until timed out")
      Process.sleep(20)
      wait_until(fun, tries - 1)
    end
  end
end
