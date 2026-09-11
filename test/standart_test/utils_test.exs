defmodule SuperWorker.Supervisor.UtilsTest do
  use ExUnit.Case, async: true

  alias SuperWorker.Supervisor.Utils

  # Doctests are evaluated in this module's context and need the functions
  # imported unqualified.
  import SuperWorker.Supervisor.Utils

  doctest Utils

  describe "safe_call/1" do
    test "returns ok tuples for successful calls" do
      assert {:ok, :result} = Utils.safe_call(fn -> :result end)
    end

    test "catches raises, throws and exits" do
      assert {:error, {:error, %RuntimeError{message: "x"}}} =
               Utils.safe_call(fn -> raise "x" end)

      assert {:error, {:throw, :thrown}} = Utils.safe_call(fn -> throw(:thrown) end)
      assert {:error, {:exit, :halt}} = Utils.safe_call(fn -> exit(:halt) end)
    end

    test "safe_call/3 applies mfas" do
      assert {:ok, 3} = Utils.safe_call(Enum, :sum, [[1, 2]])
      assert {:error, {:error, :undef}} = Utils.safe_call(:nope, :nope, [])
    end
  end

  describe "get_hash_order/2" do
    test "returns value within range" do
      order = Utils.get_hash_order("test_data", 10)
      assert order >= 0
      assert order < 10
    end

    test "is deterministic for same input" do
      data = {:some, "complex", [:data]}
      order1 = Utils.get_hash_order(data, 10)
      order2 = Utils.get_hash_order(data, 10)
      assert order1 == order2
    end

    test "distributes different inputs" do
      num_partitions = 10
      orders = Enum.map(1..100, fn i -> Utils.get_hash_order(i, num_partitions) end)

      # Check all values are in range
      assert Enum.all?(orders, fn o -> o > 0 and o <= num_partitions end)

      # Check we get some distribution (not all same value)
      assert length(Enum.uniq(orders)) > 1
    end

    test "works with various data types" do
      assert is_integer(Utils.get_hash_order(:atom, 5))
      assert is_integer(Utils.get_hash_order("string", 5))
      assert is_integer(Utils.get_hash_order(123, 5))
      assert is_integer(Utils.get_hash_order([1, 2, 3], 5))
      assert is_integer(Utils.get_hash_order(%{key: "value"}, 5))
    end

    test "handles single partition" do
      assert Utils.get_hash_order("anything", 1) == 1
    end
  end

  describe "get_default_schedulers/0" do
    test "returns positive integer" do
      schedulers = Utils.get_default_schedulers()
      assert is_integer(schedulers)
      assert schedulers > 0
    end

    test "returns consistent value" do
      s1 = Utils.get_default_schedulers()
      s2 = Utils.get_default_schedulers()
      assert s1 == s2
    end
  end

  describe "count_msgs/1" do
    test "returns 0 for process with empty mailbox" do
      pid = spawn(fn -> receive do: (:stop -> :ok) end)
      assert Utils.count_msgs(pid) == 0
      send(pid, :stop)
    end

    test "returns correct count for process with messages" do
      pid = spawn(fn -> receive do: (:stop -> :ok) end)

      send(pid, :msg1)
      send(pid, :msg2)
      send(pid, :msg3)

      # Give messages time to arrive
      Process.sleep(10)

      count = Utils.count_msgs(pid)
      assert count >= 3

      send(pid, :stop)
    end

    test "returns 0 for dead process" do
      pid = spawn(fn -> :ok end)
      Process.sleep(10)
      refute Process.alive?(pid)

      assert Utils.count_msgs(pid) == 0
    end
  end

  describe "random_id/0" do
    test "generates a string" do
      id = Utils.random_id()
      assert is_binary(id)
    end

    test "generates 32-character hex string" do
      id = Utils.random_id()
      assert String.length(id) == 32
      assert String.match?(id, ~r/^[0-9A-F]+$/)
    end

    test "generates unique IDs" do
      id1 = Utils.random_id()
      id2 = Utils.random_id()
      id3 = Utils.random_id()

      assert id1 != id2
      assert id2 != id3
      assert id1 != id3
    end

    test "generates many unique IDs" do
      ids = Enum.map(1..100, fn _ -> Utils.random_id() end)
      unique_ids = Enum.uniq(ids)

      assert length(unique_ids) == 100
    end
  end
end
