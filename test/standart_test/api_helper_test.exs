defmodule SuperWorker.Supervisor.ApiHelperTest do
  use ExUnit.Case, async: false

  alias SuperWorker.Supervisor.{ApiHelper, Message}

  describe "api_receiver/2" do
    test "receives matching response" do
      message = Message.new(:response, self(), {:ok, :result})

      spawn(fn ->
        Process.sleep(10)
        ApiHelper.api_response(message, {:ok, :result})
      end)

      assert {:ok, :result} = ApiHelper.api_receiver(message.id, 1000)
    end

    test "times out when no response" do
      message = Message.new(:response, self(), {:ok, :result})

      assert {:error, :api_timeout} = ApiHelper.api_receiver(message.id, 50)
    end

    test "ignores non-matching messages" do
      message = Message.new(:response, self(), {:ok, :result})

      spawn(fn ->
        send(message.from, {:wrong, :message})
        Process.sleep(10)
        ApiHelper.api_response(message, :correct)
      end)

      assert :correct = ApiHelper.api_receiver(message.id, 1000)
    end

    test "handles infinity timeout" do
      message = Message.new(:response, self(), {:ok, :result})

      spawn(fn ->
        Process.sleep(100)
        ApiHelper.api_response(message, :delayed_response)
      end)

      assert :delayed_response = ApiHelper.api_receiver(message.id, :infinity)
    end
  end

  describe "api_response/2" do
    test "sends response to caller" do
      message = Message.new(:response, self(), :ok)

      spawn(fn ->
        ApiHelper.api_response(message, :test_result)
      end)

      ref = message.id
      assert_receive {^ref, :test_result}, 1000
    end

    test "sends various data types" do
      test_data = [
        :atom,
        "string",
        123,
        [1, 2, 3],
        %{key: "value"},
        {:ok, :result},
        {:error, :reason}
      ]

      for data <- test_data do
        message = Message.new(:response, self(), :ok)

        spawn(fn ->
          ApiHelper.api_response(message, data)
        end)

        ref = message.id
        assert_receive {^ref, ^data}, 1000
      end
    end
  end

  describe "call_api/4" do
    test "sends message and receives response" do
      server =
        spawn(fn ->
          receive do
            {:public_api, message = %Message{type: :test_api}} ->
              ApiHelper.api_response(message, {:ok, message.data})
          end
        end)

      result = ApiHelper.call_api(server, :test_api, :my_params, 1000)
      assert result == {:ok, :my_params}
    end

    test "times out when server doesn't respond" do
      server = spawn(fn -> receive do: (:never -> :ok) end)

      result = ApiHelper.call_api(server, :test_api, :params, 50)
      assert result == {:error, :api_timeout}
    end

    test "works with registered name" do
      server =
        spawn(fn ->
          receive do
            {:public_api, message = %Message{type: :get_value}} ->
              ApiHelper.api_response(message, 42)
          end
        end)

      Process.register(server, :test_server)

      result = ApiHelper.call_api(:test_server, :get_value, nil, 1_000)
      assert result == 42
    end

    test "handles errors from server" do
      server =
        spawn(fn ->
          receive do
            {:public_api, message} ->
              ApiHelper.api_response(message, {:error, :something_went_wrong})
          end
        end)

      result = ApiHelper.call_api(server, :failing_api, :params, 1000)
      assert result == {:error, :something_went_wrong}
    end
  end

  describe "call_api_no_reply/3" do
    test "sends message without waiting" do
      parent = self()

      server =
        spawn(fn ->
          receive do
            {:public_api, message = %Message{type: :notify}} ->
              send(parent, {:received, message.data})
          end
        end)

      ref = ApiHelper.call_api_no_reply(server, :notify, :test_data)

      assert is_reference(ref)
      assert_receive {:received, :test_data}, 1000
    end

    test "returns reference" do
      server = spawn(fn -> receive do: (_ -> :ok) end)
      ref = ApiHelper.call_api_no_reply(server, :test, :data)

      assert is_reference(ref)
    end

    test "works with registered name" do
      parent = self()

      server =
        spawn(fn ->
          receive do
            {:public_api, message = %Message{type: :event}} ->
              send(parent, message.data)
          end
        end)

      Process.register(server, :event_server)

      ApiHelper.call_api_no_reply(:event_server, :event, :my_event)

      assert_receive :my_event, 1000
    end
  end

  describe "integration tests" do
    test "full API call cycle" do
      # Simulate a simple GenServer-like process
      server =
        spawn(fn ->
          state = %{counter: 0}
          api_loop(state)
        end)

      Process.register(server, :counter_server)

      # Increment
      assert :ok = ApiHelper.call_api(:counter_server, :increment, nil, 1000)

      # Get value
      assert 1 = ApiHelper.call_api(:counter_server, :get, nil, 1000)

      # Increment again
      assert :ok = ApiHelper.call_api(:counter_server, :increment, nil, 1000)

      # Get value
      assert 2 = ApiHelper.call_api(:counter_server, :get, nil, 1000)
    end

    test "concurrent API calls" do
      server =
        spawn(fn ->
          api_echo_loop()
        end)

      tasks =
        for i <- 1..10 do
          Task.async(fn ->
            ApiHelper.call_api(server, :echo, i, 1000)
          end)
        end

      results = Task.await_many(tasks)

      assert Enum.sort(results) == Enum.to_list(1..10)
    end
  end

  # Helper functions for tests
  defp api_loop(state) do
    receive do
      {:public_api, message = %Message{type: :increment}} ->
        new_state = %{state | counter: state.counter + 1}
        ApiHelper.api_response(message, :ok)
        api_loop(new_state)

      {:public_api, message = %Message{type: :get}} ->
        ApiHelper.api_response(message, state.counter)
        api_loop(state)

      :stop ->
        :ok
    end
  end

  defp api_echo_loop do
    receive do
      {:public_api, message = %Message{type: :echo}} ->
        ApiHelper.api_response(message, message.data)
        api_echo_loop()

      :stop ->
        :ok
    end
  end
end
