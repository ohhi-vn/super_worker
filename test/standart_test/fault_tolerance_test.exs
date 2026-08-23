defmodule SuperWorker.Supervisor.FaultToleranceTest do
  use ExUnit.Case, async: true

  alias SuperWorker.Supervisor, as: Sup
  alias SuperWorker.Supervisor.{ApiHelper, Db, Message}

  @moduletag :capture_log

  setup do
    sup_id = :"fault_tolerance_test_#{System.unique_integer([:positive])}"
    {:ok, _} = Sup.start_with_config(link: false, id: sup_id, num_partitions: 2)
    on_exit(fn -> Sup.stop(sup_id) end)

    %{sup_id: sup_id}
  end

  defp partition_pids(sup_id) do
    state = :sys.get_state(sup_id)
    Map.values(state.partitions)
  end

  describe "partition crash recovery" do
    test "supervisor survives a crashed partition and restarts it", %{sup_id: sup_id} do
      [victim | _] = partition_pids(sup_id)

      # :kill cannot be trapped, so this simulates a hard partition crash.
      Process.exit(victim, :kill)

      # Wait for the master to notice and restart it.
      wait_until(fn ->
        pids = partition_pids(sup_id)
        length(pids) == 2 and victim not in pids and Enum.all?(pids, &Process.alive?/1)
      end)

      # The supervisor still serves API requests after the crash.
      assert true == Sup.running?(sup_id)
      assert {:error, :group_not_found} = Sup.count_workers_in_group(sup_id, :no_group)
    end

    test "workers keep working on other partitions after one partition dies", %{sup_id: sup_id} do
      group_id = :"g_#{System.unique_integer([:positive])}"
      worker_id = :"w_#{System.unique_integer([:positive])}"

      assert {:ok, ^group_id} =
               Sup.add_group(sup_id, id: group_id, restart_strategy: :one_for_one)

      {:ok, ^worker_id} =
        Sup.add_group_worker(sup_id, group_id, {MyTest, :loop, [worker_id]}, id: worker_id)

      Process.sleep(50)

      state = :sys.get_state(sup_id)

      {:ok, {_ref, pid}} =
        Db.get_worker_by_id(state.supervisor.table, worker_id, {:group, group_id})

      # Workers are hashed to a partition together with their
      # {parent, id} key; kill a different partition.
      order = Sup.Utils.get_hash_order({group_id, worker_id}, 2)
      host = Map.fetch!(state.partitions, order)
      victim = Enum.find(Map.values(state.partitions), &(&1 != host))

      Process.exit(victim, :kill)
      Process.sleep(200)

      # The supervisor recovered...
      wait_until(fn ->
        pids = partition_pids(sup_id)
        length(pids) == 2 and victim not in pids
      end)

      # ...and the worker itself is unaffected and still answers messages.
      send(pid, {:ping, self()})
      assert_receive {:pong, _}, 1_000
    end
  end

  describe "unknown internal api messages" do
    test "do not crash the partition process", %{sup_id: sup_id} do
      [partition | _] = partition_pids(sup_id)

      msg = Message.new(:totally_unknown_type, partition, :garbage)
      send(partition, {:internal_api, %{msg | from: self()}})

      Process.sleep(50)

      # The partition is still alive and processes new public API calls.
      assert Process.alive?(partition)

      group_id = :"g_#{System.unique_integer([:positive])}"

      assert {:ok, ^group_id} =
               Sup.add_group(sup_id, id: group_id, restart_strategy: :one_for_one)

      assert true == Sup.group_exists?(sup_id, group_id)
    end
  end

  describe "api_response/2 with fire-and-forget messages" do
    test "does not crash when the message has no sender" do
      msg = %Message{Message.new(:start_worker, self(), :data) | from: nil}

      assert {:error, :no_receiver} = ApiHelper.api_response(msg, {:ok, :result})
    end
  end

  describe "chain workers survive user function crashes" do
    test "a raising chain fun returns an error result and keeps the worker alive", %{
      sup_id: sup_id
    } do
      chain_id = :"chain_#{System.unique_integer([:positive])}"
      worker_id = :"cw_#{System.unique_integer([:positive])}"

      assert {:ok, ^chain_id} = Sup.add_chain(sup_id, id: chain_id)

      {:ok, ^worker_id} =
        Sup.add_chain_worker(sup_id, chain_id, fn _data -> raise "boom" end, id: worker_id)

      {:ok, original_pid} = Sup.get_pid_chain_worker(sup_id, chain_id, worker_id)
      Process.sleep(50)

      assert {:ok, :sent_to_one} = Sup.send_to_chain(sup_id, chain_id, :crashing_data)
      Process.sleep(100)

      # Same process still handles messages after the raised exception.
      {:ok, same_pid} = Sup.get_pid_chain_worker(sup_id, chain_id, worker_id)
      assert same_pid == original_pid
      assert Process.alive?(original_pid)
    end
  end

  defp wait_until(fun, tries \\ 50) do
    if fun.() do
      :ok
    else
      if tries <= 0, do: flunk("wait_until timed out")
      Process.sleep(20)
      wait_until(fun, tries - 1)
    end
  end

  describe "partition lifecycle" do
    test "a partition without a resolvable master pid still runs its loop" do
      import SuperWorker.Supervisor.Partition

      table =
        SuperWorker.Supervisor.Db.init(:"partition_nil_#{System.unique_integer([:positive])}")

      # master is a live pid (so the start notification is deliverable) but
      # master_pid is unset, exercising the fallback warning branch.
      parent = self()

      pid =
        spawn(fn ->
          # :master is added at runtime by Partition.restart_partition/2.
          state =
            %SuperWorker.Supervisor{id: 98, master_pid: nil, table: table}
            |> Map.put(:master, parent)

          start_partition(state)
        end)

      assert_receive {:partition_started, 98}, 1_000
      Process.sleep(50)
      assert Process.alive?(pid)

      send(pid, :garbage_message)
      Process.sleep(50)
      assert Process.alive?(pid)

      Process.exit(pid, :kill)
    end
  end
end
