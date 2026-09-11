defmodule SuperWorker.Supervisor.StandaloneTest do
  use ExUnit.Case, async: false

  alias SuperWorker.Supervisor, as: Sup

  @moduletag :capture_log

  setup do
    # Use a unique supervisor ID per test to avoid conflicts
    sup_id = :"sup_standalone_#{System.unique_integer([:positive])}"
    {:ok, _} = Sup.start_with_config(link: false, id: sup_id, num_partitions: 1)

    on_exit(fn ->
      if Sup.running?(sup_id) do
        Sup.stop(sup_id)
      end
    end)

    %{sup_id: sup_id}
  end

  @tag :standalone_add_workers
  test "add standalone workers to supervisor", %{sup_id: sup_id} do
    ref = make_ref()
    num_workers = 5

    ids =
      for index <- 1..num_workers do
        id = {ref, index}

        {:ok, _} =
          Sup.add_standalone_worker(sup_id, {MyTest, :loop, [index]},
            id: id,
            restart_strategy: :permanent
          )

        id
      end

    Enum.each(ids, fn id ->
      assert {:ok, _pid} = Sup.get_pid_standalone_worker(sup_id, id)
    end)
  end

  @tag :standalone_send_data
  test "send data to worker in supervisor", %{sup_id: sup_id} do
    worker_id = make_ref()

    {:ok, _} =
      Sup.add_standalone_worker(sup_id, {MyTest, :loop, [1]},
        id: worker_id,
        restart_strategy: :permanent
      )

    Sup.send_to_standalone_worker(sup_id, worker_id, {:ping, self()})

    assert_receive {:pong, _sender}, 1_000
  end

  @tag :standalone_get_pid
  test "get pid from worker in supervisor", %{sup_id: sup_id} do
    worker_id = make_ref()

    {:ok, _} =
      Sup.add_standalone_worker(sup_id, {MyTest, :loop, [1]},
        id: worker_id,
        restart_strategy: :permanent
      )

    Sup.send_to_standalone_worker(sup_id, worker_id, {:store, :test, :hello})
    Sup.send_to_standalone_worker(sup_id, worker_id, {:get, :test, self()})

    assert_receive {:result, :hello}, 1_000

    Sup.send_to_standalone_worker(sup_id, worker_id, {:get_pid, self()})

    assert_receive {:pid, pid}, 1_000
    assert is_pid(pid)

    send(pid, {:get, :test, self()})
    assert_receive {:result, :hello}, 1_000
  end

  @tag :standalone_send_data_gen_server
  test "send data to genserver worker in supervisor", %{sup_id: sup_id} do
    worker_id = make_ref()

    {:ok, _} =
      Sup.add_standalone_worker(sup_id, MyGenServer, id: worker_id)

    Sup.send_to_standalone_worker(sup_id, worker_id, {:ping, self()})
    assert_receive {:pong, _sender}, 1_000
  end

  @tag :standalone_restart_gen_server_worker
  test "restart genserver worker in supervisor", %{sup_id: sup_id} do
    worker_id = make_ref()

    {:ok, _} =
      Sup.add_standalone_worker(sup_id, MyGenServer,
        id: worker_id,
        restart_strategy: :permanent
      )

    Sup.send_to_standalone_worker(sup_id, worker_id, {:ping, self()})
    assert_receive {:pong, pid1}, 1_000

    Sup.send_to_standalone_worker(sup_id, worker_id, :crash)
    Process.sleep(200)

    Sup.send_to_standalone_worker(sup_id, worker_id, {:ping, self()})
    assert_receive {:pong, pid2}, 1_000
    assert pid1 != pid2
  end

  @tag :standalone_restart_gen_server_worker_2
  test "restart transient genserver worker in supervisor", %{sup_id: sup_id} do
    worker_id = make_ref()

    {:ok, _} =
      Sup.add_standalone_worker(sup_id, MyGenServer,
        id: worker_id,
        restart_strategy: :transient
      )

    Sup.send_to_standalone_worker(sup_id, worker_id, {:ping, self()})
    assert_receive {:pong, pid1}, 1_000

    Sup.send_to_standalone_worker(sup_id, worker_id, :crash)
    Process.sleep(200)

    Sup.send_to_standalone_worker(sup_id, worker_id, {:ping, self()})
    assert_receive {:pong, pid2}, 1_000
    assert pid1 != pid2
  end

  @tag :standalone_doesnt_restart_gen_server_worker
  test "doesnt restart temporary genserver worker in supervisor", %{sup_id: sup_id} do
    worker_id = make_ref()

    {:ok, _} =
      Sup.add_standalone_worker(sup_id, MyGenServer,
        id: worker_id,
        restart_strategy: :temporary
      )

    Sup.send_to_standalone_worker(sup_id, worker_id, {:ping, self()})
    assert_receive {:pong, _pid1}, 1_000

    Sup.send_to_standalone_worker(sup_id, worker_id, :crash)
    Process.sleep(200)

    Sup.send_to_standalone_worker(sup_id, worker_id, {:ping, self()})

    refute_receive {:pong, _pid2}, 1_000
  end

  @tag :standalone_remove_worker
  test "remove standalone worker from supervisor", %{sup_id: sup_id} do
    worker_id = make_ref()

    {:ok, _} =
      Sup.add_standalone_worker(sup_id, {MyTest, :loop, [1]},
        id: worker_id,
        restart_strategy: :permanent
      )

    {:ok, _} = Sup.remove_standalone_worker(sup_id, worker_id)
    result = Sup.send_to_standalone_worker(sup_id, worker_id, {:ping, self()})

    assert result == {:error, :not_found}
  end

  @tag :standalone_reuse_id_worker
  test "reuse standalone worker id from supervisor", %{sup_id: sup_id} do
    worker_id = make_ref()

    {:ok, _} =
      Sup.add_standalone_worker(sup_id, {MyTest, :loop, [1]},
        id: worker_id,
        restart_strategy: :permanent
      )

    {:ok, _} = Sup.remove_standalone_worker(sup_id, worker_id)

    assert {:error, :not_found} =
             Sup.send_to_standalone_worker(sup_id, worker_id, {:ping, self()})

    {:ok, _} =
      Sup.add_standalone_worker(sup_id, {MyTest, :loop, [1]},
        id: worker_id,
        restart_strategy: :permanent
      )

    Sup.send_to_standalone_worker(sup_id, worker_id, {:ping, self()})
    assert_receive {:pong, _sender}, 1_000
  end

  @tag :standalone_restart_worker
  test "restart a worker does not affect others", %{sup_id: sup_id} do
    worker1_id = make_ref()
    worker2_id = make_ref()

    {:ok, _} =
      Sup.add_standalone_worker(sup_id, {MyTest, :loop, [1]},
        id: worker1_id,
        restart_strategy: :permanent
      )

    {:ok, _} =
      Sup.add_standalone_worker(sup_id, {MyTest, :loop, [1]},
        id: worker2_id,
        restart_strategy: :permanent
      )

    Sup.send_to_standalone_worker(sup_id, worker1_id, {:ping, self()})
    assert_receive {:pong, _sender}, 1_000

    Sup.send_to_standalone_worker(sup_id, worker2_id, {:store, :test, :hello})
    Sup.send_to_standalone_worker(sup_id, worker2_id, {:get, :test, self()})
    assert_receive {:result, :hello}, 1_000

    Sup.send_to_standalone_worker(sup_id, worker1_id, {:raise, "Restart worker"})
    Process.sleep(200)

    Sup.send_to_standalone_worker(sup_id, worker2_id, {:get, :test, self()})
    assert_receive {:result, :hello}, 1_000
  end

  @tag :standalone_restart_worker2
  test "restart a worker clears its state", %{sup_id: sup_id} do
    worker1_id = make_ref()

    {:ok, _} =
      Sup.add_standalone_worker(sup_id, {MyTest, :loop, [1]},
        id: worker1_id,
        restart_strategy: :permanent
      )

    Sup.send_to_standalone_worker(sup_id, worker1_id, {:ping, self()})
    assert_receive {:pong, _sender}, 1_000

    Sup.send_to_standalone_worker(sup_id, worker1_id, {:store, :test, :hello})
    Sup.send_to_standalone_worker(sup_id, worker1_id, {:get, :test, self()})
    assert_receive {:result, :hello}, 1_000

    Sup.send_to_standalone_worker(sup_id, worker1_id, {:raise, "Restart worker"})
    Process.sleep(200)

    Sup.send_to_standalone_worker(sup_id, worker1_id, {:get, :test, self()})
    assert_receive {:result, nil}, 1_000
  end
end
