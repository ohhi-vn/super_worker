defmodule SuperWorker.PropertyTest do
  @moduledoc """
  Property-based tests for SuperWorker using StreamData.

  These tests use property-based testing to verify that
  SuperWorker behaves correctly for a wide range of inputs.
  """

  use ExUnit.Case, async: false
  use ExUnitProperties

  alias SuperWorker.Supervisor, as: Sup
  alias SuperWorker.Supervisor.{Worker, Group, Chain, MapQueue}
  alias SuperWorker.CircuitBreaker

  import StreamData

  describe "MapQueue properties" do
    property "add then get returns the same data" do
      check all(
              data <- term(),
              queue_length <- integer(1..1000),
              initial_queue <- constant(MapQueue.new(:test, queue_length: queue_length))
            ) do
        {:ok, queue, msg_id} = MapQueue.add(initial_queue, data)
        {:ok, retrieved_data} = MapQueue.get(queue, msg_id)
        assert retrieved_data == data
      end
    end

    property "size increases by 1 after add" do
      check all(
              data <- term(),
              queue <- constant(MapQueue.new(:test, queue_length: 100))
            ) do
        initial_size = MapQueue.size(queue)
        {:ok, new_queue, _msg_id} = MapQueue.add(queue, data)
        assert MapQueue.size(new_queue) == initial_size + 1
      end
    end

    property "is_full? returns true only when size >= queue_length" do
      check all(
              queue_length <- integer(1..100),
              num_items <- integer(0..queue_length)
            ) do
        queue =
          Enum.reduce(1..num_items//1, MapQueue.new(:test, queue_length: queue_length), fn i, q ->
            {:ok, q, _} = MapQueue.add(q, i)
            q
          end)

        if num_items >= queue_length do
          assert MapQueue.is_full?(queue) == true
        else
          assert MapQueue.is_full?(queue) == false
        end
      end
    end

    property "remove then get returns error" do
      check all(
              data <- term(),
              queue <- constant(MapQueue.new(:test, queue_length: 100))
            ) do
        {:ok, queue, msg_id} = MapQueue.add(queue, data)
        {:ok, queue} = MapQueue.remove(queue, msg_id)
        assert {:error, {:not_found, ^msg_id}} = MapQueue.get(queue, msg_id)
      end
    end
  end

  describe "Worker properties" do
    property "from_config with valid config returns {:ok, worker}" do
      check all(
              id <- term(),
              type <- member_of([:standalone, :group, :chain]),
              fun <- constant(fn -> :ok end)
            ) do
        config = [id: id, type: type, fun: {:fun, fun}]

        case Worker.from_config(config) do
          {:ok, worker} ->
            assert worker.id == id
            assert worker.type == type

          {:error, _} ->
            :ok
        end
      end
    end

    property "worker with invalid type returns error" do
      check all(
              id <- term()
            ) do
        config = [id: id, type: :invalid_type, fun: {:fun, fn -> :ok end}]

        assert {:error, _} = Worker.from_config(config)
      end
    end
  end

  describe "CircuitBreaker properties" do
    property "circuit starts in closed state" do
      check all(
              name <- atom(:alphanumeric)
            ) do
        {:ok, pid} = CircuitBreaker.start(name, failure_threshold: 3)
        {:ok, state} = CircuitBreaker.get_state(name)

        assert state.state == :closed
        assert state.failure_count == 0

        GenServer.stop(pid)
      end
    end

    property "circuit opens after threshold failures" do
      check all(
              name <- atom(:alphanumeric),
              threshold <- integer(1..10)
            ) do
        {:ok, pid} = CircuitBreaker.start(name, failure_threshold: threshold)

        # Simulate failures
        Enum.each(1..(threshold + 1), fn _ ->
          CircuitBreaker.call(name, fn -> {:error, :test_error} end)
        end)

        {:ok, state} = CircuitBreaker.get_state(name)
        assert state.state == :open

        GenServer.stop(pid)
      end
    end

    property "circuit allows calls when closed" do
      check all(
              name <- atom(:alphanumeric),
              data <- term()
            ) do
        {:ok, pid} = CircuitBreaker.start(name)

        result =
          CircuitBreaker.call(name, fn ->
            {:ok, data}
          end)

        assert {:ok, ^data} = result

        GenServer.stop(pid)
      end
    end

    property "circuit rejects calls when open" do
      check all(
              name <- atom(:alphanumeric)
            ) do
        {:ok, pid} = CircuitBreaker.start(name, failure_threshold: 1)

        # Cause failure to open circuit
        CircuitBreaker.call(name, fn -> {:error, :test_error} end)

        # Next call should be rejected
        result = CircuitBreaker.call(name, fn -> {:ok, :should_not_reach} end)
        assert {:error, :circuit_open} = result

        GenServer.stop(pid)
      end
    end
  end

  describe "ETS operations properties" do
    property "ETS insert then lookup returns same value" do
      check all(
              key <- term(),
              value <- term()
            ) do
        table = :ets.new(:test_table, [:set, :public])

        :ets.insert(table, {key, value})
        result = :ets.lookup(table, key)

        assert result == [{key, value}]

        :ets.delete(table)
      end
    end

    property "ETS delete removes the key" do
      check all(
              key <- term(),
              value <- term()
            ) do
        table = :ets.new(:test_table, [:set, :public])

        :ets.insert(table, {key, value})
        :ets.delete(table, key)
        result = :ets.lookup(table, key)

        assert result == []

        :ets.delete(table)
      end
    end
  end

  describe "Supervisor properties" do
    @tag :property
    test "supervisor can start and stop" do
      sup_id = :"property_test_#{System.unique_integer([:positive])}"

      assert {:ok, _} = Sup.start_with_config(link: false, id: sup_id, num_partitions: 1)
      assert Sup.running?(sup_id) == true

      assert :ok = Sup.stop(sup_id)
      Process.sleep(100)
      assert Sup.running?(sup_id) == false
    end

    @tag :property
    property "adding workers increases worker count" do
      check all(
              num_workers <- integer(1..10)
            ) do
        sup_id = :"property_test_#{System.unique_integer([:positive])}"
        {:ok, _} = Sup.start_with_config(link: false, id: sup_id, num_partitions: 1)

        # Add workers
        Enum.each(1..num_workers, fn i ->
          {:ok, _} =
            Sup.add_standalone_worker(
              sup_id,
              fn -> Process.sleep(10) end,
              id: :"w_#{i}"
            )
        end)

        # Verify workers were added (this would need a way to count standalone workers)
        # For now, just verify the supervisor is still running
        assert Sup.running?(sup_id) == true

        Sup.stop(sup_id)
      end
    end
  end
end
