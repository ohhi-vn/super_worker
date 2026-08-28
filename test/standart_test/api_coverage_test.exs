defmodule SuperWorker.Supervisor.ApiCoverageTest do
  use ExUnit.Case, async: false

  alias SuperWorker.Supervisor, as: Sup
  alias SuperWorker.Supervisor.ApiHelper
  alias SuperWorker.Supervisor.Db

  @moduletag :capture_log

  setup do
    sup_id = :"api_cov_#{System.unique_integer([:positive])}"
    {:ok, _} = Sup.start_with_config(link: false, id: sup_id, num_partitions: 1)
    on_exit(fn -> if Sup.running?(sup_id), do: Sup.stop(sup_id) end)

    %{sup_id: sup_id}
  end

  test "stop/3 on a missing supervisor returns :not_running" do
    assert {:error, :not_running} = Sup.stop(:"never_started_#{System.unique_integer()}")
  end

  test "start_link/1 and start/1 reject duplicate ids", %{sup_id: sup_id} do
    # The struct-based low level APIs detect the running instance.
    assert {:error, {:already_started, ^sup_id}} =
             Sup.start_link(%SuperWorker.Supervisor{id: sup_id, num_partitions: 1})

    assert {:error, {:already_started, ^sup_id}} =
             Sup.start(%SuperWorker.Supervisor{id: sup_id, num_partitions: 1})
  end

  test "missing entities return errors across the API surface", %{sup_id: sup_id} do
    assert {:error, :group_not_found} = Sup.count_workers_in_group(sup_id, :nope)
    assert false == Sup.group_exists?(sup_id, :nope)

    assert match?({:error, _}, Sup.broadcast_to_group(sup_id, :nope, :data))
    assert match?({:error, _}, Sup.send_to_group_worker(sup_id, :nope, :w, :data))
    assert match?({:error, _}, Sup.send_to_group_random(sup_id, :nope, :data))
    assert match?({:error, _}, Sup.restart_group(sup_id, :nope))
    assert match?({:error, _}, Sup.remove_group(sup_id, :nope))
    assert match?({:error, _}, Sup.remove_group_worker(sup_id, :nope, :w))

    assert {:error, :chain_not_found} = Sup.count_workers_in_chain(sup_id, :nochain)
    assert match?({:error, _}, Sup.send_to_chain(sup_id, :nochain, :data))
    assert match?({:error, _}, Sup.remove_chain(sup_id, :nochain))
    assert match?({:error, _}, Sup.remove_chain_worker(sup_id, :nochain, :w))

    assert match?({:error, _}, Sup.send_to_standalone_worker(sup_id, :noworker, :data))
    assert match?({:error, _}, Sup.remove_standalone_worker(sup_id, :noworker))
    assert match?({:error, _}, Sup.get_pid_standalone_worker(sup_id, :noworker))
  end

  test "adding workers to missing parents reports the parent type", %{sup_id: sup_id} do
    assert {:error, :group_not_found} =
             Sup.add_group_worker(sup_id, :nope, {MyTest, :loop, [1]}, id: :w)

    assert {:error, :chain_not_found} =
             Sup.add_chain_worker(sup_id, :nochain, fn d -> d end, id: :w)
  end

  test "standalone GenServer worker without valid spec returns an error", %{sup_id: sup_id} do
    assert match?({:error, _}, Sup.add_standalone_worker(sup_id, {String, []}))
  end

  test "a real GenServer can be added as standalone worker", %{sup_id: sup_id} do
    {:ok, id} = Sup.add_standalone_worker(sup_id, {MyGenServer, []}, id: :gs_worker)
    assert is_atom(id) or is_binary(id)

    Process.sleep(100)

    assert {:ok, pid} = Sup.get_pid_standalone_worker(sup_id, :gs_worker)
    assert Process.alive?(pid)

    assert {:ok, :gs_worker} = Sup.remove_standalone_worker(sup_id, :gs_worker)
    Process.sleep(50)
    refute Process.alive?(pid)
  end

  test "unknown public api messages are answered with an error", %{sup_id: sup_id} do
    [partition | _] =
      sup_id
      |> :sys.get_state()
      |> Map.get(:partitions)
      |> Map.values()

    result = ApiHelper.call_api(partition, :bogus_api_name, :some_data, 1_000)

    assert {:error, {:unknown, :some_data}} = result
  end

  test "workers that finish normally are not restarted under :transient", %{sup_id: sup_id} do
    group_id = :"g_norm_#{System.unique_integer([:positive])}"
    worker_id = :shortlived

    {:ok, ^group_id} =
      Sup.add_group(sup_id, id: group_id, restart_strategy: :one_for_one)

    {:ok, ^worker_id} =
      Sup.add_group_worker(
        sup_id,
        group_id,
        fn ->
          receive do
          after
            50 -> :done
          end
        end,
        id: worker_id,
        restart_strategy: :transient
      )

    Process.sleep(150)

    table = :sys.get_state(sup_id).supervisor.table

    # The worker exited normally and was NOT restarted.
    assert match?({:error, :not_found}, Db.get_worker_by_id(table, worker_id, {:group, group_id}))
  end
end
