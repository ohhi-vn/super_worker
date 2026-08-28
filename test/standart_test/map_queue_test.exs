defmodule SuperWorker.Supervisor.MapQueueTest do
  use ExUnit.Case, async: true

  alias SuperWorker.Supervisor.MapQueue

  doctest MapQueue

  describe "new/1" do
    test "creates a new queue with default options" do
      queue = MapQueue.new(:test_queue)

      assert %MapQueue{} = queue
      assert queue.id == :test_queue
      assert queue.queue_length == 50
      assert queue.msgs == %{}
      assert queue.last_msg_id == 0
    end

    test "creates a new queue with custom queue_length" do
      queue = MapQueue.new(:custom_queue, queue_length: 100)

      assert queue.queue_length == 100
    end

    test "accepts various ID types" do
      assert %MapQueue{id: :atom} = MapQueue.new(:atom)
      assert %MapQueue{id: "string"} = MapQueue.new("string")
      assert %MapQueue{id: 123} = MapQueue.new(123)
      assert %MapQueue{id: {:tuple, :id}} = MapQueue.new({:tuple, :id})
    end
  end

  describe "add/2" do
    test "adds a message and returns updated queue with message ID" do
      queue = MapQueue.new(:test)

      assert {:ok, updated_queue, msg_id} = MapQueue.add(queue, "first message")
      assert msg_id == 1
      assert MapQueue.size(updated_queue) == 1
    end

    test "increments message ID for each addition" do
      queue = MapQueue.new(:test)

      {:ok, queue, id1} = MapQueue.add(queue, "msg1")
      {:ok, queue, id2} = MapQueue.add(queue, "msg2")
      {:ok, _queue, id3} = MapQueue.add(queue, "msg3")

      assert id1 == 1
      assert id2 == 2
      assert id3 == 3
    end

    test "accepts various message types" do
      queue = MapQueue.new(:test)

      assert {:ok, _q, _} = MapQueue.add(queue, "string")
      assert {:ok, _q, _} = MapQueue.add(queue, 123)
      assert {:ok, _q, _} = MapQueue.add(queue, [:list])
      assert {:ok, _q, _} = MapQueue.add(queue, %{key: "value"})
      assert {:ok, _q, _} = MapQueue.add(queue, {:tuple, "data"})
    end

    test "returns error when queue is full" do
      queue = MapQueue.new(:test, queue_length: 2)

      {:ok, queue, _} = MapQueue.add(queue, "msg1")
      {:ok, queue, _} = MapQueue.add(queue, "msg2")

      assert {:error, :queue_full} = MapQueue.add(queue, "msg3")
    end

    test "allows adding after removing messages" do
      queue = MapQueue.new(:test, queue_length: 2)

      {:ok, queue, id1} = MapQueue.add(queue, "msg1")
      {:ok, queue, _id2} = MapQueue.add(queue, "msg2")

      # Queue is full
      assert {:error, :queue_full} = MapQueue.add(queue, "msg3")

      # Remove one message
      {:ok, queue} = MapQueue.remove(queue, id1)

      # Now we can add again
      assert {:ok, _queue, _id} = MapQueue.add(queue, "msg3")
    end
  end

  describe "get/2" do
    test "retrieves existing message" do
      queue = MapQueue.new(:test)
      {:ok, queue, msg_id} = MapQueue.add(queue, "hello world")

      assert {:ok, "hello world"} = MapQueue.get(queue, msg_id)
    end

    test "returns error for non-existent message" do
      queue = MapQueue.new(:test)

      assert {:error, {:not_found, 999}} = MapQueue.get(queue, 999)
    end

    test "get does not remove the message" do
      queue = MapQueue.new(:test)
      {:ok, queue, msg_id} = MapQueue.add(queue, "persistent")

      {:ok, "persistent"} = MapQueue.get(queue, msg_id)
      {:ok, "persistent"} = MapQueue.get(queue, msg_id)
      {:ok, "persistent"} = MapQueue.get(queue, msg_id)

      assert MapQueue.size(queue) == 1
    end
  end

  describe "remove/2" do
    test "removes existing message" do
      queue = MapQueue.new(:test)
      {:ok, queue, msg_id} = MapQueue.add(queue, "to be removed")

      assert MapQueue.size(queue) == 1
      {:ok, queue} = MapQueue.remove(queue, msg_id)
      assert MapQueue.size(queue) == 0
    end

    test "returns ok even for non-existent message" do
      queue = MapQueue.new(:test)

      assert {:ok, _queue} = MapQueue.remove(queue, 999)
    end

    test "removed message cannot be retrieved" do
      queue = MapQueue.new(:test)
      {:ok, queue, msg_id} = MapQueue.add(queue, "data")

      {:ok, queue} = MapQueue.remove(queue, msg_id)

      assert {:error, {:not_found, ^msg_id}} = MapQueue.get(queue, msg_id)
    end

    test "removes only the specified message" do
      queue = MapQueue.new(:test)
      {:ok, queue, id1} = MapQueue.add(queue, "msg1")
      {:ok, queue, id2} = MapQueue.add(queue, "msg2")
      {:ok, queue, id3} = MapQueue.add(queue, "msg3")

      {:ok, queue} = MapQueue.remove(queue, id2)

      assert {:ok, "msg1"} = MapQueue.get(queue, id1)
      assert {:error, {:not_found, ^id2}} = MapQueue.get(queue, id2)
      assert {:ok, "msg3"} = MapQueue.get(queue, id3)
    end
  end

  describe "is_full?/1" do
    test "returns false for empty queue" do
      queue = MapQueue.new(:test, queue_length: 5)

      refute MapQueue.full?(queue)
    end

    test "returns false for partially filled queue" do
      queue = MapQueue.new(:test, queue_length: 5)
      {:ok, queue, _} = MapQueue.add(queue, "msg")

      refute MapQueue.full?(queue)
    end

    test "returns true when queue reaches capacity" do
      queue = MapQueue.new(:test, queue_length: 2)
      {:ok, queue, _} = MapQueue.add(queue, "msg1")
      {:ok, queue, _} = MapQueue.add(queue, "msg2")

      assert MapQueue.full?(queue)
    end

    test "returns false after removing from full queue" do
      queue = MapQueue.new(:test, queue_length: 1)
      {:ok, queue, msg_id} = MapQueue.add(queue, "msg")

      assert MapQueue.full?(queue)

      {:ok, queue} = MapQueue.remove(queue, msg_id)

      refute MapQueue.full?(queue)
    end
  end

  describe "is_empty?/1" do
    test "returns true for new queue" do
      queue = MapQueue.new(:test)

      assert MapQueue.empty?(queue)
    end

    test "returns false after adding message" do
      queue = MapQueue.new(:test)
      {:ok, queue, _} = MapQueue.add(queue, "msg")

      refute MapQueue.empty?(queue)
    end

    test "returns true after removing all messages" do
      queue = MapQueue.new(:test)
      {:ok, queue, id1} = MapQueue.add(queue, "msg1")
      {:ok, queue, id2} = MapQueue.add(queue, "msg2")

      {:ok, queue} = MapQueue.remove(queue, id1)
      {:ok, queue} = MapQueue.remove(queue, id2)

      assert MapQueue.empty?(queue)
    end
  end

  describe "size/1" do
    test "returns 0 for empty queue" do
      queue = MapQueue.new(:test)

      assert MapQueue.size(queue) == 0
    end

    test "returns correct size after additions" do
      queue = MapQueue.new(:test)

      {:ok, queue, _} = MapQueue.add(queue, "msg1")
      assert MapQueue.size(queue) == 1

      {:ok, queue, _} = MapQueue.add(queue, "msg2")
      assert MapQueue.size(queue) == 2

      {:ok, queue, _} = MapQueue.add(queue, "msg3")
      assert MapQueue.size(queue) == 3
    end

    test "decreases after removals" do
      queue = MapQueue.new(:test)
      {:ok, queue, id1} = MapQueue.add(queue, "msg1")
      {:ok, queue, _id2} = MapQueue.add(queue, "msg2")

      assert MapQueue.size(queue) == 2

      {:ok, queue} = MapQueue.remove(queue, id1)
      assert MapQueue.size(queue) == 1
    end
  end

  describe "remaining_capacity/1" do
    test "returns full capacity for empty queue" do
      queue = MapQueue.new(:test, queue_length: 10)

      assert MapQueue.remaining_capacity(queue) == 10
    end

    test "decreases as messages are added" do
      queue = MapQueue.new(:test, queue_length: 5)

      {:ok, queue, _} = MapQueue.add(queue, "msg1")
      assert MapQueue.remaining_capacity(queue) == 4

      {:ok, queue, _} = MapQueue.add(queue, "msg2")
      assert MapQueue.remaining_capacity(queue) == 3
    end

    test "returns 0 when queue is full" do
      queue = MapQueue.new(:test, queue_length: 2)
      {:ok, queue, _} = MapQueue.add(queue, "msg1")
      {:ok, queue, _} = MapQueue.add(queue, "msg2")

      assert MapQueue.remaining_capacity(queue) == 0
    end

    test "increases after removing messages" do
      queue = MapQueue.new(:test, queue_length: 5)
      {:ok, queue, id} = MapQueue.add(queue, "msg")

      assert MapQueue.remaining_capacity(queue) == 4

      {:ok, queue} = MapQueue.remove(queue, id)
      assert MapQueue.remaining_capacity(queue) == 5
    end
  end

  describe "clear/1" do
    test "empties the queue" do
      queue = MapQueue.new(:test)
      {:ok, queue, _} = MapQueue.add(queue, "msg1")
      {:ok, queue, _} = MapQueue.add(queue, "msg2")
      {:ok, queue, _} = MapQueue.add(queue, "msg3")

      queue = MapQueue.clear(queue)

      assert MapQueue.empty?(queue)
      assert MapQueue.size(queue) == 0
    end

    test "preserves queue metadata" do
      queue = MapQueue.new(:my_queue, queue_length: 100)
      {:ok, queue, _} = MapQueue.add(queue, "msg")

      cleared_queue = MapQueue.clear(queue)

      assert cleared_queue.id == :my_queue
      assert cleared_queue.queue_length == 100
    end

    test "clearing empty queue is idempotent" do
      queue = MapQueue.new(:test)
      cleared = MapQueue.clear(queue)

      assert cleared == queue
    end
  end

  describe "message_ids/1" do
    test "returns empty list for empty queue" do
      queue = MapQueue.new(:test)

      assert MapQueue.message_ids(queue) == []
    end

    test "returns all message IDs" do
      queue = MapQueue.new(:test)
      {:ok, queue, id1} = MapQueue.add(queue, "msg1")
      {:ok, queue, id2} = MapQueue.add(queue, "msg2")
      {:ok, queue, id3} = MapQueue.add(queue, "msg3")

      ids = MapQueue.message_ids(queue)

      assert length(ids) == 3
      assert id1 in ids
      assert id2 in ids
      assert id3 in ids
    end

    test "reflects removals" do
      queue = MapQueue.new(:test)
      {:ok, queue, id1} = MapQueue.add(queue, "msg1")
      {:ok, queue, id2} = MapQueue.add(queue, "msg2")
      {:ok, queue, _id3} = MapQueue.add(queue, "msg3")

      {:ok, queue} = MapQueue.remove(queue, id2)

      ids = MapQueue.message_ids(queue)

      assert length(ids) == 2
      assert id1 in ids
      refute id2 in ids
    end
  end

  describe "all_messages/1" do
    test "returns empty list for empty queue" do
      queue = MapQueue.new(:test)

      assert MapQueue.all_messages(queue) == []
    end

    test "returns all messages" do
      queue = MapQueue.new(:test)
      {:ok, queue, _} = MapQueue.add(queue, "msg1")
      {:ok, queue, _} = MapQueue.add(queue, "msg2")
      {:ok, queue, _} = MapQueue.add(queue, "msg3")

      messages = MapQueue.all_messages(queue)

      assert length(messages) == 3
      assert "msg1" in messages
      assert "msg2" in messages
      assert "msg3" in messages
    end

    test "reflects removals" do
      queue = MapQueue.new(:test)
      {:ok, queue, _} = MapQueue.add(queue, "msg1")
      {:ok, queue, id2} = MapQueue.add(queue, "msg2")
      {:ok, queue, _} = MapQueue.add(queue, "msg3")

      {:ok, queue} = MapQueue.remove(queue, id2)

      messages = MapQueue.all_messages(queue)

      assert length(messages) == 2
      assert "msg1" in messages
      refute "msg2" in messages
      assert "msg3" in messages
    end
  end

  describe "has_message?/2" do
    test "returns false for empty queue" do
      queue = MapQueue.new(:test)

      refute MapQueue.has_message?(queue, 1)
    end

    test "returns true for existing message" do
      queue = MapQueue.new(:test)
      {:ok, queue, msg_id} = MapQueue.add(queue, "msg")

      assert MapQueue.has_message?(queue, msg_id)
    end

    test "returns false for non-existent message" do
      queue = MapQueue.new(:test)
      {:ok, queue, _} = MapQueue.add(queue, "msg")

      refute MapQueue.has_message?(queue, 999)
    end

    test "returns false after message is removed" do
      queue = MapQueue.new(:test)
      {:ok, queue, msg_id} = MapQueue.add(queue, "msg")

      assert MapQueue.has_message?(queue, msg_id)

      {:ok, queue} = MapQueue.remove(queue, msg_id)

      refute MapQueue.has_message?(queue, msg_id)
    end
  end

  describe "update_queue_length/2" do
    test "updates the queue length" do
      queue = MapQueue.new(:test, queue_length: 10)

      updated_queue = MapQueue.update_queue_length(queue, 20)

      assert updated_queue.queue_length == 20
    end

    test "preserves existing messages" do
      queue = MapQueue.new(:test, queue_length: 10)
      {:ok, queue, id1} = MapQueue.add(queue, "msg1")
      {:ok, queue, id2} = MapQueue.add(queue, "msg2")

      updated_queue = MapQueue.update_queue_length(queue, 20)

      assert {:ok, "msg1"} = MapQueue.get(updated_queue, id1)
      assert {:ok, "msg2"} = MapQueue.get(updated_queue, id2)
    end

    test "allows adding beyond old limit with new limit" do
      queue = MapQueue.new(:test, queue_length: 2)
      {:ok, queue, _} = MapQueue.add(queue, "msg1")
      {:ok, queue, _} = MapQueue.add(queue, "msg2")

      assert MapQueue.full?(queue)

      queue = MapQueue.update_queue_length(queue, 5)

      refute MapQueue.full?(queue)
      assert {:ok, _queue, _} = MapQueue.add(queue, "msg3")
    end

    test "does not automatically remove messages if new limit is smaller" do
      queue = MapQueue.new(:test, queue_length: 10)
      {:ok, queue, _} = MapQueue.add(queue, "msg1")
      {:ok, queue, _} = MapQueue.add(queue, "msg2")
      {:ok, queue, _} = MapQueue.add(queue, "msg3")

      queue = MapQueue.update_queue_length(queue, 2)

      # Messages still exist
      assert MapQueue.size(queue) == 3
      # But queue is now considered "full"
      assert MapQueue.full?(queue)
    end
  end

  describe "integration scenarios" do
    test "can handle many operations" do
      queue = MapQueue.new(:test, queue_length: 100)

      # Add 50 messages
      {queue, ids} =
        Enum.reduce(1..50, {queue, []}, fn i, {q, ids} ->
          {:ok, q, id} = MapQueue.add(q, "msg_#{i}")
          {q, [id | ids]}
        end)

      ids = Enum.reverse(ids)

      assert MapQueue.size(queue) == 50
      refute MapQueue.full?(queue)

      # Remove every other message
      queue =
        ids
        |> Enum.take_every(2)
        |> Enum.reduce(queue, fn id, q ->
          {:ok, q} = MapQueue.remove(q, id)
          q
        end)

      assert MapQueue.size(queue) == 25

      # Add more messages
      {:ok, queue, _} = MapQueue.add(queue, "new_msg")
      assert MapQueue.size(queue) == 26
    end

    test "handles rapid add and remove" do
      queue = MapQueue.new(:test, queue_length: 5)

      queue =
        Enum.reduce(1..100, queue, fn i, q ->
          {:ok, q, id} = MapQueue.add(q, "msg_#{i}")
          {:ok, q} = MapQueue.remove(q, id)
          q
        end)

      assert MapQueue.empty?(queue)
      # Message IDs continue incrementing
      {:ok, _queue, id} = MapQueue.add(queue, "final")
      assert id == 101
    end
  end
end
