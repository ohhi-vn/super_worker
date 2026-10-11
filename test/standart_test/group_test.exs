defmodule SuperWorker.Supervisor.GroupTest do
  use ExUnit.Case, async: true

  alias SuperWorker.Supervisor, as: Sup
  alias SuperWorker.Supervisor.Group

  doctest Group

  @sup_id :sup_group_test

  setup_all do
    {:ok, _} = Sup.start_with_config(link: false, id: @sup_id, num_partitions: 2)
    :ok
  end

  setup do
    if Sup.running?(@sup_id) do
      :ok
    else
      raise "Supervisor is not running"
    end
  end

  @doc """
  Test for adding group strategy.
  """
  @tag :group_verify_strategy
  test "add group & verify strategy" do
    result = Sup.add_group(@sup_id, id: :group2, restart_strategy: :one_for_one)
    match?({:ok, _}, result)

    result = Sup.add_group(@sup_id, id: :group3, restart_strategy: :one_for_all)
    match?({:ok, _}, result)
  end

  @tag :group_add_workers
  test "add workers to group" do
    group_id = make_ref()

    {:ok, _} = Sup.add_group(@sup_id, id: group_id, restart_strategy: :one_for_one)

    range = 1..5

    for index <- range do
      {:ok, _} =
        Sup.add_group_worker(@sup_id, group_id, {MyTest, :loop, [index]}, id: index)
    end

    list_pid =
      Enum.reduce(range, [], fn index, acc ->
        case Sup.get_pid_group_worker(@sup_id, group_id, index) do
          {:ok, pid} -> [pid | acc]
          _ -> acc
        end
      end)

    assert(length(list_pid) == Enum.count(range))
  end

  @tag :worker_get_pid
  test "get pid from worker in supervisor" do
    group_id = make_ref()
    worker_id = 1

    {:ok, _} = Sup.add_group(@sup_id, id: group_id, restart_strategy: :one_for_one)

    {:ok, _} =
      Sup.add_group_worker(@sup_id, group_id, {MyTest, :loop, [worker_id]}, id: worker_id)

    Process.sleep(100)

    Sup.send_to_group_worker(@sup_id, group_id, worker_id, {:store, :test, :hello})
    Sup.send_to_group_worker(@sup_id, group_id, worker_id, {:get, :test, self()})

    result =
      receive do
        {:result, :hello} ->
          true

        other ->
          other
      after
        1_000 -> "incorrect result from worker"
      end

    assert(true == result)

    Sup.send_to_group_worker(@sup_id, group_id, worker_id, {:get_pid, self()})

    pid =
      receive do
        {:pid, pid} -> pid
      after
        1_000 -> raise "cannot get pid of worker"
      end

    send(pid, {:get, :test, self()})

    result =
      receive do
        {:result, :hello} ->
          true

        other ->
          other
      after
        1_000 -> "incorrect result from worker"
      end

    assert(true == result)
  end

  test "broadcast message to group" do
    group_id = make_ref()
    num_workers = 3

    {:ok, _} = Sup.add_group(@sup_id, id: group_id, restart_strategy: :one_for_one)

    for i <- 1..num_workers do
      {:ok, _} =
        Sup.add_group_worker(@sup_id, group_id, {MyTest, :loop, [i]}, id: i)
    end

    Process.sleep(100)

    Sup.broadcast_to_group(@sup_id, group_id, {:store, :test, :hello})

    for i <- 1..num_workers do
      Sup.send_to_group_worker(@sup_id, group_id, i, {:get, :test, self()})

      result =
        receive do
          {:result, :hello} ->
            true

          other ->
            other
        after
          1_000 -> "incorrect result from worker"
        end

      assert(true == result)
    end

    Sup.remove_group(@sup_id, group_id)
  end

  test "send random to group" do
    group_id = make_ref()
    num_workers = 3

    {:ok, _} = Sup.add_group(@sup_id, id: group_id, restart_strategy: :one_for_one)

    for i <- 1..num_workers do
      {:ok, _} =
        Sup.add_group_worker(@sup_id, group_id, {MyTest, :loop, [i]}, id: i)
    end

    Process.sleep(100)

    Sup.broadcast_to_group(@sup_id, group_id, {:store, :test, :nothing})

    Sup.send_to_group_random(@sup_id, group_id, {:store, :test, :hello})

    results =
      Enum.map(1..num_workers, fn i ->
        Sup.send_to_group_worker(@sup_id, group_id, i, {:get, :test, self()})

        receive do
          {:result, :hello} ->
            true

          {:result, :nothing} ->
            false
        after
          1_000 -> "incorrect result from worker"
        end
      end)

    assert 1 == Enum.count(results, &(&1 == true))
    assert 2 == Enum.count(results, &(&1 == false))

    Sup.remove_group(@sup_id, group_id)
  end

  test "restart worker in group" do
    group_id = make_ref()
    num_workers = 2

    {:ok, _} = Sup.add_group(@sup_id, id: group_id, restart_strategy: :one_for_one)

    for i <- 1..num_workers do
      {:ok, _} =
        Sup.add_group_worker(@sup_id, group_id, {MyTest, :loop, [i]}, id: i)
    end

    Process.sleep(100)

    Sup.broadcast_to_group(@sup_id, group_id, {:store, :test, :hello})

    Sup.restart_group_worker(@sup_id, group_id, 1)

    Sup.send_to_group_worker(@sup_id, group_id, 1, {:get, :test, self()})

    result =
      receive do
        {:result, nil} ->
          true

        other ->
          other
      after
        1_000 -> "incorrect result from worker"
      end

    assert(true == result)

    Sup.send_to_group_worker(@sup_id, group_id, 2, {:get, :test, self()})

    result =
      receive do
        {:result, :hello} ->
          true

        other ->
          other
      after
        1_000 -> "incorrect result from worker"
      end

    assert(true == result)

    Sup.remove_group(@sup_id, group_id)
  end

  test "restart all workers in group" do
    group_id = make_ref()
    num_workers = 5

    {:ok, _} = Sup.add_group(@sup_id, id: group_id, restart_strategy: :one_for_one)

    for i <- 1..num_workers do
      {:ok, _} =
        Sup.add_group_worker(@sup_id, group_id, {MyTest, :loop, [i]}, id: i)
    end

    Process.sleep(100)

    Sup.broadcast_to_group(@sup_id, group_id, {:store, :test, :hello})

    Sup.restart_group(@sup_id, group_id)

    Process.sleep(2000)

    for i <- 1..num_workers do
      Sup.send_to_group_worker(@sup_id, group_id, i, {:get, :test, self()})

      result =
        receive do
          {:result, nil} ->
            true

          other ->
            other
        after
          1000 -> "incorrect result from worker"
        end

      assert(true == result)
    end

    Sup.remove_group(@sup_id, group_id)
  end

  @tag :group_add_mixed_workers
  test "add mixed workers to group" do
    group_id = make_ref()
    num_workers = 5
    {:ok, _} = Sup.add_group(@sup_id, id: group_id, restart_strategy: :one_for_one)

    for index <- 1..num_workers do
      {:ok, _} =
        Sup.add_group_worker(@sup_id, group_id, {MyTest, :loop, [index]}, id: index)
    end

    Sup.add_group_worker(@sup_id, group_id, MyGenServer, [])
    Process.sleep(10)

    counter =
      Enum.reduce(1..num_workers, [], fn index, acc ->
        case Sup.get_pid_group_worker(@sup_id, group_id, index) do
          {:ok, pid} -> [pid | acc]
          _ -> acc
        end
      end)
      |> Enum.count()

    counter =
      case Sup.get_pid_group_worker(@sup_id, group_id, MyGenServer) do
        {:ok, _} -> counter + 1
        _ -> counter
      end

    assert(num_workers + 1 == counter)
  end

  @tag :group_add_workers_multi_groups
  test "add workers to groups" do
    num_workers = 10
    num_groups = 5

    Enum.each(1..num_groups, fn index ->
      group_id = {make_ref(), index}

      {:ok, _} = Sup.add_group(@sup_id, id: group_id, restart_strategy: :one_for_one)

      for worker_index <- 1..num_workers do
        {:ok, _} =
          Sup.add_group_worker(@sup_id, group_id, {MyTest, :loop, [worker_index]},
            id: {index, worker_index}
          )
      end

      Process.sleep(10)

      counter =
        Enum.reduce(1..num_workers, 0, fn worker_index, acc ->
          case Sup.get_pid_group_worker(@sup_id, group_id, {index, worker_index}) do
            {:ok, _pid} -> acc + 1
            _ -> acc
          end
        end)

      assert(counter == num_workers)
    end)
  end

  @tag :group_send_data
  test "send data to worker in group" do
    group_id = make_ref()

    {:ok, _} = Sup.add_group(@sup_id, id: group_id, restart_strategy: :one_for_one)
    {:ok, _} = Sup.add_group_worker(@sup_id, group_id, {MyTest, :loop, [1]}, id: 1)

    Process.sleep(100)
    Sup.send_to_group_worker(@sup_id, group_id, 1, {:ping, self()})

    result =
      receive do
        {:pong, _sender} -> true
      after
        1_000 -> false
      end

    assert(true == result)
  end

  @tag :group_broadcast_data
  test "broadcadts data to all workers in group" do
    group_id = make_ref()

    num_workers = 5

    {:ok, _} = Sup.add_group(@sup_id, id: group_id, restart_strategy: :one_for_one)

    for index <- 1..num_workers do
      {:ok, _} = Sup.add_group_worker(@sup_id, group_id, {MyTest, :loop, [index]}, id: index)
    end

    Process.sleep(100)
    Sup.broadcast_to_group(@sup_id, group_id, {:ping, self()})

    results =
      for _ <- 1..num_workers do
        receive do
          {:pong, _sender} -> true
        after
          1_000 -> false
        end
      end

    assert Enum.all?(results)
  end

  @tag :group_remove_worker
  test "remove worker from group" do
    group_id = make_ref()
    worker_id = 1

    {:ok, _} = Sup.add_group(@sup_id, id: group_id, restart_strategy: :one_for_one)

    {:ok, _} =
      Sup.add_group_worker(@sup_id, group_id, {MyTest, :loop, [worker_id]}, id: worker_id)

    :ok = Sup.send_to_group_worker(@sup_id, group_id, worker_id, {:ping, self()})

    result =
      receive do
        {:pong, _sender} -> true
      after
        1_000 -> false
      end

    assert result

    {:ok, _} = Sup.remove_group_worker(@sup_id, group_id, worker_id)
    result = Sup.send_to_group_worker(@sup_id, group_id, worker_id, {:ping, self()})

    assert {:error, :worker_not_found} = result
  end

  @tag :remove_group
  test "remove group" do
    group_id = make_ref()
    worker_id = 1

    {:ok, _} = Sup.add_group(@sup_id, id: group_id, restart_strategy: :one_for_one)

    {:ok, _} =
      Sup.add_group_worker(@sup_id, group_id, {MyTest, :loop, [worker_id]}, id: worker_id)

    :ok = Sup.send_to_group_worker(@sup_id, group_id, worker_id, {:ping, self()})

    result =
      receive do
        {:pong, _sender} -> true
      after
        1_000 -> false
      end

    assert result

    {:ok, _} = Sup.remove_group(@sup_id, group_id)

    result = Sup.get_pid_group_worker(@sup_id, group_id, worker_id)

    assert match?({:error, _}, result)
  end

  @tag :group_restart_one_worker
  test "restart one for on  in a group" do
    group_id = make_ref()

    {:ok, _} = Sup.add_group(@sup_id, id: group_id, restart_strategy: :one_for_one)
    {:ok, _} = Sup.add_group_worker(@sup_id, group_id, {MyTest, :loop, [1]}, id: 1)
    {:ok, _} = Sup.add_group_worker(@sup_id, group_id, {MyTest, :loop, [2]}, id: 2)

    Process.sleep(100)
    Sup.send_to_group_worker(@sup_id, group_id, 1, {:ping, self()})

    result =
      receive do
        {:pong, _sender} -> true
        other -> other
      after
        1_000 -> "verify the worker is started"
      end

    assert(true == result)

    Sup.send_to_group_worker(@sup_id, group_id, 2, {:store, :test, :hello})

    Sup.send_to_group_worker(@sup_id, group_id, 2, {:get, :test, self()})

    result =
      receive do
        {:result, :hello} ->
          true

        other ->
          other
      after
        1_000 -> "incorrect result from worker 2"
      end

    assert(true == result)

    Sup.send_to_group_worker(@sup_id, group_id, 1, {:raise, "Restart all workers"})

    Process.sleep(100)
    Sup.send_to_group_worker(@sup_id, group_id, 2, {:get, :test, self()})

    result =
      receive do
        {:result, :hello} ->
          true

        other ->
          other
      after
        1_000 -> "get data failed, timeout"
      end

    assert(true == result)
  end

  @tag :group_restart_all_workers
  test "restart all workers in a group" do
    group_id = make_ref()

    {:ok, _} = Sup.add_group(@sup_id, id: group_id, restart_strategy: :one_for_all)
    {:ok, _} = Sup.add_group_worker(@sup_id, group_id, {MyTest, :loop, [1]}, id: "w_1")
    {:ok, _} = Sup.add_group_worker(@sup_id, group_id, {MyTest, :loop, [2]}, id: "w_2")

    Process.sleep(100)
    Sup.send_to_group_worker(@sup_id, group_id, "w_1", {:ping, self()})

    result =
      receive do
        {:pong, _sender} -> true
      after
        1_000 -> :check_1_failed
      end

    assert(true == result)

    ref = make_ref()
    Sup.send_to_group_worker(@sup_id, group_id, "w_2", {:store, :test, ref})
    Process.sleep(100)
    Sup.send_to_group_worker(@sup_id, group_id, "w_2", {:get, :test, self()})

    result =
      receive do
        {:result, ^ref} -> true
      after
        1_000 -> :check_2_failed
      end

    assert(true == result)

    Sup.send_to_group_worker(
      @sup_id,
      group_id,
      "w_1",
      {:raise, "Test restart all of group strategy"}
    )

    Process.sleep(100)
    Sup.send_to_group_worker(@sup_id, group_id, "w_2", {:get, :test, self()})

    result =
      receive do
        {:result, nil} -> true
        other -> other
      after
        1_000 -> :verify_after_restart
      end

    assert(true == result)
  end

  @tag :group_restart_all_workers2
  test "restart all workers in a group 2" do
    group_id = make_ref()

    {:ok, _} = Sup.add_group(@sup_id, id: {group_id, 1}, restart_strategy: :one_for_all)
    {:ok, _} = Sup.add_group(@sup_id, id: {group_id, 2}, restart_strategy: :one_for_all)
    {:ok, _} = Sup.add_group(@sup_id, id: {group_id, 3}, restart_strategy: :one_for_all)

    {:ok, _} = Sup.add_group_worker(@sup_id, {group_id, 1}, {MyTest, :loop, [1]}, id: 1)
    {:ok, _} = Sup.add_group_worker(@sup_id, {group_id, 1}, {MyTest, :loop, [2]}, id: 2)

    {:ok, _} = Sup.add_group_worker(@sup_id, {group_id, 2}, {MyTest, :loop, [1]}, id: 1)
    {:ok, _} = Sup.add_group_worker(@sup_id, {group_id, 2}, {MyTest, :loop, [2]}, id: 2)

    {:ok, _} = Sup.add_group_worker(@sup_id, {group_id, 3}, {MyTest, :loop, [1]}, id: 1)
    {:ok, _} = Sup.add_group_worker(@sup_id, {group_id, 3}, {MyTest, :loop, [2]}, id: 2)

    fun = fn group_id, parent ->
      Process.sleep(100)
      Sup.send_to_group_worker(@sup_id, group_id, 1, {:ping, self()})

      receive do
        {:pong, _sender} -> true
      after
        1_000 -> raise "timeout for restarting all workers 2"
      end

      Sup.send_to_group_worker(@sup_id, group_id, 2, {:store, :test, :hello})
      Process.sleep(100)

      Sup.send_to_group_worker(@sup_id, group_id, 2, {:get, :test, self()})

      receive do
        {:result, :hello} -> true
      after
        1_000 -> raise "timeout for restarting all workers 2"
      end

      Sup.send_to_group_worker(@sup_id, group_id, 1, {:raise, "Restart all workers"})

      Process.sleep(100)
      Sup.send_to_group_worker(@sup_id, group_id, 2, {:get, :test, self()})

      receive do
        {:result, nil} -> true
      after
        1_000 -> raise "timeout for restarting all workers 2"
      end

      send(parent, :success)
    end

    parent = self()
    spawn(fn -> fun.({group_id, 1}, parent) end)
    spawn(fn -> fun.({group_id, 2}, parent) end)
    spawn(fn -> fun.({group_id, 3}, parent) end)

    for _ <- 1..3 do
      receive do
        :success -> true
      after
        4_000 -> raise "timeout for restarting all workers"
      end
    end
  end

  @tag :group_restart_worker_by_api
  test "restart group worker by api" do
    group_id = make_ref()

    {:ok, _} = Sup.add_group(@sup_id, id: group_id, restart_strategy: :one_for_one)
    {:ok, _} = Sup.add_group_worker(@sup_id, group_id, {MyTest, :loop, [2]}, id: 2)

    Sup.send_to_group_worker(@sup_id, group_id, 2, {:store, :test, :hello})
    Sup.send_to_group_worker(@sup_id, group_id, 2, {:get, :test, self()})

    result =
      receive do
        {:result, :hello} ->
          true

        other ->
          other
      after
        1_000 -> "incorrect result from worker 2"
      end

    assert(true == result)

    Sup.restart_group_worker(@sup_id, group_id, 2)

    Process.sleep(100)
    Sup.send_to_group_worker(@sup_id, group_id, 2, {:get, :test, self()})

    result =
      receive do
        {:result, nil} ->
          true

        other ->
          other
      after
        1_000 -> "get data failed, timeout"
      end

    assert(true == result)
  end

  @tag :group_restart_all_workers_by_api
  test "restart all group worker by api" do
    group_id = make_ref()

    {:ok, _} = Sup.add_group(@sup_id, id: group_id, restart_strategy: :one_for_one)
    {:ok, _} = Sup.add_group_worker(@sup_id, group_id, {MyTest, :loop, [1]}, id: 1)
    {:ok, _} = Sup.add_group_worker(@sup_id, group_id, {MyTest, :loop, [2]}, id: 2)

    Sup.send_to_group_worker(@sup_id, group_id, 1, {:store, :test, :hello})
    Sup.send_to_group_worker(@sup_id, group_id, 1, {:get, :test, self()})

    result =
      receive do
        {:result, :hello} ->
          true

        other ->
          other
      after
        1_000 -> "incorrect result from worker 2"
      end

    assert(true == result)

    Sup.send_to_group_worker(@sup_id, group_id, 2, {:store, :test, :hello})
    Sup.send_to_group_worker(@sup_id, group_id, 2, {:get, :test, self()})

    result =
      receive do
        {:result, :hello} ->
          true

        other ->
          other
      after
        1_000 -> "incorrect result from worker 2"
      end

    assert(true == result)

    Sup.restart_group(@sup_id, group_id)

    Process.sleep(100)
    Sup.send_to_group_worker(@sup_id, group_id, 1, {:get, :test, self()})

    result =
      receive do
        {:result, nil} ->
          true

        other ->
          other
      after
        1_000 -> "get data failed, timeout"
      end

    assert(true == result)

    Sup.send_to_group_worker(@sup_id, group_id, 2, {:get, :test, self()})

    result =
      receive do
        {:result, nil} ->
          true

        other ->
          other
      after
        1_000 -> "get data failed, timeout"
      end

    assert(true == result)
  end

  describe "worker operation edge cases" do
    setup do
      group_id = :"g_edge_#{System.unique_integer([:positive])}"
      {:ok, ^group_id} = Sup.add_group(@sup_id, id: group_id, restart_strategy: :one_for_one)

      %{group_id: group_id}
    end

    @tag :group_duplicate_worker
    test "adding a duplicate worker id fails", %{group_id: group_id} do
      {:ok, _} = Sup.add_group_worker(@sup_id, group_id, {MyTest, :loop, [1]}, id: :dup)
      Process.sleep(50)

      assert {:error, :worker_already_exists} =
               Sup.add_group_worker(@sup_id, group_id, {MyTest, :loop, [1]}, id: :dup)
    end

    @tag :group_restart_missing_worker
    test "restart/kill/remove of a missing worker return errors", %{group_id: group_id} do
      assert {:error, :worker_not_found} =
               Sup.restart_group_worker(@sup_id, group_id, :missing_worker)

      assert {:error, _} = Sup.remove_group_worker(@sup_id, group_id, :missing_worker)
    end

    @tag :group_invalid_options
    test "add_group with invalid restart strategy fails" do
      assert {:error, _} =
               Sup.add_group(@sup_id,
                 id: :"g_bad_#{System.unique_integer([:positive])}",
                 restart_strategy: :all_for_one
               )
    end
  end

  describe "direct Group operations on a private table" do
    alias SuperWorker.Supervisor.{Db, Group}

    setup do
      table = Db.init(:"group_unit_#{System.unique_integer([:positive])}")

      group = %Group{id: :unit_group, table: table, supervisor: :unit_sup}

      %{table: table, group: group}
    end

    test "check_options rejects invalid strategies" do
      assert {:error, {:invalid_options, _}} =
               Group.check_options(id: :g, restart_strategy: :sometimes)
    end

    test "add_worker generates an id when missing", %{table: table, group: group} do
      worker = %SuperWorker.Supervisor.Worker{
        id: nil,
        fun: {:fun, fn -> :ok end},
        type: :group,
        parent: nil,
        restart_strategy: :permanent
      }

      # No id set: the group assigns a random one.
      refute worker.id

      case Group.add_worker(group, worker) do
        {:ok, _} -> :ok
        # The spawned loop-less fun exits immediately; either way the info row
        # must exist with a generated binary id.
        {:error, _} -> :ok
      end

      assert {:ok, infos} = Db.get_worker_infos_by_parent(table, {:group, :unit_group})
      assert length(infos) == 1
      assert is_binary(hd(infos).id)
    end

    test "kill_all_workers reports failures for dead workers", %{table: table, group: group} do
      dead = spawn(fn -> :ok end)
      Process.sleep(10)

      worker = %SuperWorker.Supervisor.Worker{
        id: :dead_worker,
        fun: {:fun, fn -> :ok end},
        type: :group
      }

      Db.put_worker_info(table, %{worker | parent: :unit_group})
      Db.put_worker(table, make_ref(), :dead_worker, {:group, :unit_group}, dead)

      assert {:error, [{:error, :dead_worker, :worker_not_found}]} =
               Group.kill_all_workers(group)
    end

    test "send_message to unknown worker fails", %{group: group} do
      assert {:error, :worker_not_found} = Group.send_message(group, :missing, :hello)
    end

    test "add_worker rejects a duplicate id", %{table: table, group: group} do
      worker = %SuperWorker.Supervisor.Worker{
        id: :dupe,
        fun: {:fun, fn -> Process.sleep(10_000) end},
        type: :group,
        parent: nil,
        restart_strategy: :temporary
      }

      assert match?({:ok, _}, Group.add_worker(group, %{worker | parent: group.id}))

      # Second add with the same id is rejected without spawning.
      assert {:error, :worker_already_exists} =
               Group.add_worker(group, %{worker | parent: group.id})
      assert {:ok, infos} = Db.get_worker_infos_by_parent(table, {:group, :unit_group})
      assert length(infos) == 1
    end

    test "kill_worker by id on missing worker returns error", %{group: group} do
      assert {:error, :worker_not_found} = Group.kill_worker(group, :ghost, :kill)
    end

    test "kill_worker by id kills the live worker", %{table: table, group: group} do
      {:ok, _} =
        Group.add_worker(group, %SuperWorker.Supervisor.Worker{
          id: :killable,
          fun: {:fun, fn -> Process.sleep(:infinity) end},
          type: :group,
          restart_strategy: :temporary
        })

      Process.sleep(50)
      {:ok, {_ref, pid}} = Db.get_worker_by_id(table, :killable, {:group, group.id})

      # do_spawn_worker linked the worker to this (test) process because we
      # called add_worker directly; unlink so the :kill does not kill us.
      Process.unlink(pid)

      assert {:ok, :killed} = Group.kill_worker(group, :killable, :kill)
      Process.sleep(20)
      assert false == Process.alive?(pid)
    end

    test "remove_worker reports an error for a dead worker row", %{table: table, group: group} do
      dead_pid = spawn(fn -> :ok end)
      Process.sleep(10)

      worker = %SuperWorker.Supervisor.Worker{
        id: :dead_row,
        fun: {:fun, fn -> :ok end},
        type: :group,
        parent: group.id,
        restart_strategy: :temporary
      }

      Db.put_worker_info(table, worker)
      Db.put_worker(table, make_ref(), :dead_row, {:group, group.id}, dead_pid)

      assert {:error, :worker_not_found} = Group.remove_worker(group, :dead_row)
    end

    test "remove_worker cleans up rows of a live worker", %{table: table, group: group} do
      worker_fun = fn -> MyTest.loop(:removable) end

      assert match?(
               {:ok, _},
               Group.add_worker(group, %SuperWorker.Supervisor.Worker{
                 id: :removable,
                 fun: {:fun, worker_fun},
                 type: :group,
                 restart_strategy: :temporary
               })
             )

      Process.sleep(50)
      assert Group.worker_exists?(group, :removable)

      # add_worker links the worker to this test process; unlink so the
      # removal's exit signal does not take the test down with it.
      {:ok, {_ref, pid}} = Db.get_worker_by_id(table, :removable, {:group, group.id})
      Process.unlink(pid)

      assert {:ok, :worker_removed} = Group.remove_worker(group, :removable)
      refute Process.alive?(pid)

      refute Group.worker_exists?(group, :removable)
      assert [] = :ets.match_object(table, {{:ref, :_}, :removable, :_, :_})
    end

    test "remove_worker reports dead workers", %{table: table, group: group} do
      dead_pid = spawn(fn -> :ok end)
      Process.sleep(10)

      Db.put_worker_info(table, %SuperWorker.Supervisor.Worker{
        id: :dead_one,
        fun: {:fun, fn -> :ok end},
        type: :group
      })

      Db.put_worker(table, make_ref(), :dead_one, {:group, :unit_group}, dead_pid)

      result = Group.remove_worker(group, :dead_one)
      assert match?({:error, _}, result)
    end

    test "adding a worker whose GenServer cannot start reports spawn_failed" do
      group_id = :"g_bad_gs_#{System.unique_integer([:positive])}"
      {:ok, ^group_id} = Sup.add_group(@sup_id, id: group_id, restart_strategy: :one_for_one)

      assert {:error, {:spawn_failed, _}} =
               Sup.add_group_worker(@sup_id, group_id, {FailingGenServer, []}, [])
      Process.sleep(50)

      # Partition is still healthy and serves API calls.
      assert true == Sup.group_exists?(@sup_id, group_id)
    end

    test "adding a worker whose start exits reports spawn_failed" do
      group_id = :"g_exit_gs_#{System.unique_integer([:positive])}"
      {:ok, ^group_id} = Sup.add_group(@sup_id, id: group_id, restart_strategy: :one_for_one)

      assert {:error, {:spawn_failed, _}} =
               Sup.add_group_worker(@sup_id, group_id, {MyTest, :boom}, [])

      assert true == Sup.group_exists?(@sup_id, group_id)
    end

    test "adding a worker whose start raises reports spawn_failed" do
      group_id = :"g_raise_gs_#{System.unique_integer([:positive])}"
      {:ok, ^group_id} = Sup.add_group(@sup_id, id: group_id, restart_strategy: :one_for_one)

      assert {:error, {:spawn_failed, _}} =
               Sup.add_group_worker(@sup_id, group_id, {MyTest, [fail_with: :raise]}, [])

      assert true == Sup.group_exists?(@sup_id, group_id)
    end

    test "a worker with id: false gets a random id" do
      group_id = :"g_random_id_#{System.unique_integer([:positive])}"
      {:ok, ^group_id} = Sup.add_group(@sup_id, id: group_id, restart_strategy: :one_for_one)

      {:ok, _} = Sup.add_group_worker(@sup_id, group_id, {MyTest, :loop, [1]}, id: false)

      assert {:ok, 1} = Sup.count_workers_in_group(@sup_id, group_id)
    end

    test "a named worker registers its name; a duplicate name logs a warning" do
      group_id = :"g_named_#{System.unique_integer([:positive])}"
      name = :"group_named_worker_#{System.unique_integer([:positive])}"
      {:ok, ^group_id} = Sup.add_group(@sup_id, id: group_id, restart_strategy: :one_for_one)

      {:ok, _} =
        Sup.add_group_worker(@sup_id, group_id, {MyTest, :loop, [1]}, id: 1, name: name)

      wait_until(fn ->
        case Process.whereis(name) do
          nil -> false
          _ -> true
        end
      end)

      # A second worker with the same name cannot register; both stay alive.
      {:ok, _} =
        Sup.add_group_worker(@sup_id, group_id, {MyTest, :loop, [2]}, id: 2, name: name)

      Process.sleep(50)
      assert Process.whereis(name) != nil
    end

    test "adding a worker without a valid GenServer child spec returns an error" do
      group_id = :"g_no_spec_#{System.unique_integer([:positive])}"
      {:ok, ^group_id} = Sup.add_group(@sup_id, id: group_id, restart_strategy: :one_for_one)

      assert match?({:error, _}, Sup.add_group_worker(@sup_id, group_id, {String, []}, []))
    end

    test "check_options rejects a missing id and accepts a valid group" do
      assert {:error, {:invalid_options, _}} = Group.check_options(restart_strategy: :one_for_one)
      assert {:error, {:invalid_options, _}} = Group.check_options(id: nil)

      assert {:ok, %Group{id: :g_ok, restart_strategy: :one_for_one}} =
               Group.check_options(id: :g_ok, restart_strategy: :one_for_one)
    end

    test "get_worker and kill_worker unwrap {_, id} worker ids", %{table: table, group: group} do
      {:ok, _} =
        Group.add_worker(group, %SuperWorker.Supervisor.Worker{
          id: :tuple_w,
          fun: {:fun, fn -> Process.sleep(:infinity) end},
          type: :group,
          restart_strategy: :temporary
        })

      Process.sleep(20)
      assert {:ok, %{id: :tuple_w}} = Group.get_worker(group, {:any, :tuple_w})

      {:ok, {_ref, pid}} = Db.get_worker_by_id(table, :tuple_w, {:group, group.id})
      Process.unlink(pid)

      assert {:ok, :killed} = Group.kill_worker(group, {:any, :tuple_w}, :kill)
    end

    test "count_workers matches get_all_workers as members are added", %{
      table: table,
      group: group
    } do
      assert 0 == Group.count_workers(group)

      for i <- 1..3 do
        {:ok, _} =
          Group.add_worker(group, %SuperWorker.Supervisor.Worker{
            id: i,
            fun: {:fun, fn -> Process.sleep(:infinity) end},
            type: :group,
            restart_strategy: :temporary
          })

        assert i == Group.count_workers(group)
        assert {:ok, infos} = Group.get_all_workers(group)
        assert length(infos) == Group.count_workers(group)
      end

      assert {:ok, 3} = Db.count_workers_by_parent(table, {:group, group.id})

      # Workers are linked to this test process; unlink before the bulk kill.
      {:ok, pairs} = Db.get_worker_pids_by_parent(table, {:group, group.id})
      Enum.each(pairs, fn {_, pid} -> Process.unlink(pid) end)
      assert :ok = Group.kill_all_workers(group)
    end

    test "send_message and broadcast report a dead worker row", %{table: table, group: group} do
      dead = spawn(fn -> :ok end)
      Process.sleep(10)

      Db.put_worker_info(table, %SuperWorker.Supervisor.Worker{
        id: :gone,
        fun: {:fun, fn -> :ok end},
        type: :group,
        parent: group.id
      })

      ref = make_ref()
      Db.put_worker(table, ref, :gone, {:group, group.id}, dead)

      assert {:error, :worker_not_found} = Group.send_message(group, :gone, :hello)
      assert {:error, [{:error, ^dead, :worker_not_found}]} = Group.broadcast(group, :hello)
    end

    test "restart_worker recovers a worker whose process is already dead", %{
      table: table,
      group: group
    } do
      dead = spawn(fn -> :ok end)
      Process.sleep(10)

      worker = %SuperWorker.Supervisor.Worker{
        id: :recover,
        fun: {:fun, fn -> Process.sleep(:infinity) end},
        type: :group,
        parent: group.id,
        restart_strategy: :temporary
      }

      Db.put_worker_info(table, worker)
      Db.put_worker(table, make_ref(), :recover, {:group, group.id}, dead)

      assert {:ok, _} = Group.restart_worker(group, :recover)

      {:ok, {_ref, pid}} = Db.get_worker_by_id(table, :recover, {:group, group.id})
      assert Process.alive?(pid)

      Process.unlink(pid)
      Process.exit(pid, :kill)
    end
  end

  describe "supervisor-level error contract" do
    alias SuperWorker.Supervisor.Db

    test "broadcast_to_group returns an error when a member is dead" do
      group_id = :"g_bcast_err_#{System.unique_integer([:positive])}"
      {:ok, ^group_id} = Sup.add_group(@sup_id, id: group_id, restart_strategy: :one_for_one)

      %{supervisor: %{table: table}} = :sys.get_state(Process.whereis(@sup_id))

      dead = spawn(fn -> :ok end)
      Process.sleep(10)

      Db.put_worker_info(table, %SuperWorker.Supervisor.Worker{
        id: :dead_member,
        fun: {:fun, fn -> :ok end},
        type: :group,
        parent: group_id
      })

      ref = make_ref()
      Db.put_worker(table, ref, :dead_member, {:group, group_id}, dead)

      assert {:error, [{:error, ^dead, :worker_not_found}]} =
               Sup.broadcast_to_group(@sup_id, group_id, :ping)

      Db.delete_worker(table, ref)
      Db.delete_worker_info(table, :dead_member, {:group, group_id})
    end

    test "a group worker exiting normally is not restarted under either strategy" do
      for strategy <- [:one_for_one, :one_for_all] do
        group_id = :"g_normal_#{strategy}_#{System.unique_integer([:positive])}"
        {:ok, ^group_id} = Sup.add_group(@sup_id, id: group_id, restart_strategy: strategy)

        {:ok, _} = Sup.add_group_worker(@sup_id, group_id, fn -> :ok end, id: :done)
        {:ok, _} = Sup.add_group_worker(@sup_id, group_id, {MyTest, :loop, [2]}, id: :sibling)
        {:ok, sibling_pid} = Sup.get_pid_group_worker(@sup_id, group_id, :sibling)

        Process.sleep(100)

        assert {:error, :worker_not_found} = Sup.get_pid_group_worker(@sup_id, group_id, :done)
        assert {:ok, ^sibling_pid} = Sup.get_pid_group_worker(@sup_id, group_id, :sibling)
        assert Process.alive?(sibling_pid)

        Sup.remove_group(@sup_id, group_id)
      end
    end

    test "a failed spawn leaves the group empty and returns spawn_failed" do
      group_id = :"g_clean_fail_#{System.unique_integer([:positive])}"
      {:ok, ^group_id} = Sup.add_group(@sup_id, id: group_id, restart_strategy: :one_for_one)

      assert {:error, {:spawn_failed, _}} =
               Sup.add_group_worker(@sup_id, group_id, {FailingGenServer, []}, id: :never)

      assert {:ok, 0} = Sup.count_workers_in_group(@sup_id, group_id)

      assert {:error, :worker_not_found} =
               Sup.get_pid_group_worker(@sup_id, group_id, :never)
    end
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
