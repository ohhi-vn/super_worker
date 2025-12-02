defmodule SuperWorker.Supervisor.StandaloneWorkloadTest do
  use ExUnit.Case, async: false

  alias SuperWorker.Supervisor, as: Sup
  alias Sup.Db

  @sup_id :sup_workload_test

  setup_all do
    {:ok, _} = Sup.start_with_config(link: false, id: @sup_id, num_partitions: 20)
    :ok
  end

  setup do
    if Sup.running?(@sup_id) do
      :ok
    else
      raise "Supervisor is not running"
    end
  end

  @tag timeout: 300_000
  test "spawn workers in supervisor" do
    num_workers = 10_000
    ref = make_ref()

    for i <- 1..num_workers do
      restart_strategy = get_strategy(i)

      {:ok, _} =
        Sup.add_standalone_worker(@sup_id, MyGenServer,
          id: {ref, i},
          restart_strategy: restart_strategy
        )
    end

    Process.sleep(1_000)

    result =
      Enum.reduce(1..num_workers, true, fn i, acc ->
        Sup.send_to_standalone_worker(@sup_id, {ref, i}, {:ping, self()})

        receive do
          {:pong, sender} ->
            sender
            acc && true
        after
          1_000 -> false
        end
      end)

    assert result

    Enum.reduce(1..num_workers, true, fn i, acc ->
      Sup.send_to_standalone_worker(@sup_id, {ref, i}, :crash)
    end)

    Process.sleep(1_000)

    result =
      Enum.reduce(1..num_workers, true, fn i, acc ->
        Sup.send_to_standalone_worker(@sup_id, {ref, i}, {:ping, self()})

        receive do
          {:pong, sender} ->
            sender
            acc && true
        after
          1_000 -> false
        end
      end)

    assert result

    assert(true == result)
  end

  @tag timeout: 300_000
  test "restart workers in supervisor" do
    num_workers = 10_000
    ref = make_ref()

    for i <- 1..num_workers do
      restart_strategy = get_strategy(i)

      {:ok, _} =
        Sup.add_standalone_worker(@sup_id, MyGenServer,
          id: {ref, i},
          restart_strategy: restart_strategy
        )
    end

    Process.sleep(1_000)

    temporary_workers =
      Enum.reduce(1..num_workers, [], fn i, acc ->
        if get_strategy(i) == :temporary do
          [i | acc]
        else
          acc
        end
      end)

    transient_workers =
      Enum.reduce(1..num_workers, [], fn i, acc ->
        if get_strategy(i) == :transient do
          [i | acc]
        else
          acc
        end
      end)

    permanent_workers =
      Enum.reduce(1..num_workers, [], fn i, acc ->
        if get_strategy(i) == :permanent do
          [i | acc]
        else
          acc
        end
      end)

    parent = self()

    spawn(fn ->
      Enum.reduce(temporary_workers, true, fn i, acc ->
        Sup.send_to_standalone_worker(@sup_id, {ref, i}, :crash)
      end)

      send(parent, :temporary_done)
    end)

    spawn(fn ->
      Enum.reduce(transient_workers, true, fn i, acc ->
        Sup.send_to_standalone_worker(@sup_id, {ref, i}, :crash)
      end)

      send(parent, :transient_done)
    end)

    spawn(fn ->
      Enum.reduce(permanent_workers, true, fn i, acc ->
        Sup.send_to_standalone_worker(@sup_id, {ref, i}, :crash)
      end)

      send(parent, :permanent_done)
    end)

    Process.sleep(1_000)

    result =
      Enum.reduce(transient_workers ++ permanent_workers, true, fn i, acc ->
        Sup.send_to_standalone_worker(@sup_id, {ref, i}, {:ping, self()})

        receive do
          {:pong, sender} ->
            sender
            acc && true
        after
          1_000 -> false
        end
      end)

    assert result

    result =
      Enum.reduce(temporary_workers, false, fn i, acc ->
        send_result = Sup.send_to_standalone_worker(@sup_id, {ref, i}, {:ping, self()})

        match?({:ok, _}, send_result) || acc
      end)

    assert(false == result)
  end

  @tag timeout: 300_000
  test "restart workers with real task in supervisor" do
    num_workers = 1_000
    ref = make_ref()

    for i <- 1..num_workers do
      restart_strategy = :permanent

      {:ok, _} =
        Sup.add_standalone_worker(@sup_id, MyGenServer,
          id: {ref, i},
          restart_strategy: restart_strategy
        )
    end

    Process.sleep(1_000)

    parent = self()

    spawn(fn ->
      Enum.reduce(1..num_workers, true, fn i, acc ->
        Sup.send_to_standalone_worker(@sup_id, {ref, i}, :crash)
      end)

      send(parent, :crash_done)
    end)

    Process.sleep(1_000)

    Enum.map(1..num_workers, fn i ->
      spawn(fn ->
        send(parent, send_with_retry({ref, i}, 10_000, false))
      end)
    end)

    receive do
      :crash_done ->
        :ok
    after
      10_000 ->
        raise "wait crash timeout"
    end

    results =
      Enum.map(1..num_workers, fn i ->
        receive do
          :error ->
            false

          :ok ->
            true
        after
          3_000 ->
            false
        end
      end)

    assert Enum.any?(results)
  end

  defp get_strategy(index) when is_integer(index) do
    cond do
      rem(index, 3) == 0 -> :permanent
      rem(index, 3) == 2 -> :transient
      true -> :temporary
    end
  end

  defp send_with_retry(index, 0, last_failed) do
    :ok
  end

  defp send_with_retry(worker_id, times, last_failed) do
    Sup.send_to_standalone_worker(@sup_id, worker_id, {:ping, self()})

    result =
      receive do
        {:pong, sender} ->
          true
      after
        1_000 -> false
      end

    if result do
      Process.sleep(100)
      send_with_retry(worker_id, times - 1, false)
    else
      if last_failed do
        :error
      else
        Process.sleep(1000)
        send_with_retry(worker_id, times, true)
      end
    end
  end
end
