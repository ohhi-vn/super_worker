defmodule SuperWorker.Supervisor.MessageTest do
  use ExUnit.Case, async: true

  alias SuperWorker.Supervisor.Message

  describe "new/3" do
    test "creates a message with sender, receiver, type and data" do
      to = self()
      msg = Message.new(:public_api, to, {:data})

      assert %Message{} = msg
      assert is_reference(msg.id)
      assert msg.from == self()
      assert msg.to == to
      assert msg.type == :public_api
      assert msg.data == {:data}
    end

    test "each message gets a unique reference" do
      msg1 = Message.new(:internal_api, self(), :a)
      msg2 = Message.new(:internal_api, self(), :b)

      refute msg1.id == msg2.id
    end

    test "accepts atom targets and nil" do
      assert %Message{to: :some_name} = Message.new(:public_api, :some_name, :x)
      assert %Message{to: nil} = Message.new(:public_api, nil, :x)
    end

    test "raises on invalid target" do
      assert_raise FunctionClauseError, fn ->
        Message.new(:public_api, 123, :x)
      end
    end
  end
end
