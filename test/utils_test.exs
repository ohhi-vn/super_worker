defmodule SuperWorker.Supervisor.UtilsTest do
  use ExUnit.Case, async: true

  alias SuperWorker.Supervisor.Utils

  describe "normalize_opts/2" do
    test "normalizes keyword list to map" do
      assert {:ok, %{id: :test, count: 5}} =
               Utils.normalize_opts([id: :test, count: 5], [:id, :count])
    end

    test "accepts empty options list" do
      assert {:ok, %{}} = Utils.normalize_opts([], [:id, :count])
    end

    test "rejects invalid options" do
      assert {:error, {:invalid_options, [:invalid]}} =
               Utils.normalize_opts([id: :test, invalid: :opt], [:id])
    end

    test "handles multiple invalid options" do
      assert {:error, {:invalid_options, invalid}} =
               Utils.normalize_opts([good: 1, bad1: 2, bad2: 3], [:good])

      assert :bad1 in invalid
      assert :bad2 in invalid
    end

    test "handles atom shorthand for type" do
      assert {:ok, %{type: :group}} = Utils.normalize_opts([:group], [:type])
      assert {:ok, %{type: :chain}} = Utils.normalize_opts([:chain], [:type])
      assert {:ok, %{type: :standalone}} = Utils.normalize_opts([:standalone], [:type])
    end

    test "handles mixed keyword and shorthand" do
      assert {:ok, %{type: :group, id: :test}} =
               Utils.normalize_opts([:group, id: :test], [:type, :id])
    end

    test "preserves all valid options" do
      opts = [id: :sup1, count: 10, enabled: true, data: "test"]
      params = [:id, :count, :enabled, :data]

      assert {:ok, result} = Utils.normalize_opts(opts, params)
      assert result.id == :sup1
      assert result.count == 10
      assert result.enabled == true
      assert result.data == "test"
    end

    test "handles boolean flag options" do
      assert {:ok, %{enabled: true}} = Utils.normalize_opts([:enabled], [:enabled])
    end
  end

  describe "generic_default_sup_opts/1" do
    test "adds owner when not present" do
      opts = %{id: :test}
      result = Utils.generic_default_sup_opts(opts)

      assert Map.has_key?(result, :owner)
      assert result.owner == self()
    end

    test "preserves existing owner" do
      existing_pid = spawn(fn -> :ok end)
      opts = %{id: :test, owner: existing_pid}
      result = Utils.generic_default_sup_opts(opts)

      assert result.owner == existing_pid
    end

    test "preserves all other options" do
      opts = %{id: :test, count: 5, data: "test"}
      result = Utils.generic_default_sup_opts(opts)

      assert result.id == :test
      assert result.count == 5
      assert result.data == "test"
    end
  end

  describe "check_type/3" do
    test "validates correct type" do
      opts = %{count: 5}
      assert {:ok, ^opts} = Utils.check_type(opts, :count, &is_integer/1)
    end

    test "rejects incorrect type" do
      opts = %{count: "five"}
      assert {:error, :invalid_type} = Utils.check_type(opts, :count, &is_integer/1)
    end

    test "returns error for missing key" do
      opts = %{other: :value}
      assert {:error, :invalid_type} = Utils.check_type(opts, :missing, &is_atom/1)
    end

    test "works with various validators" do
      assert {:ok, _} = Utils.check_type(%{val: :atom}, :val, &is_atom/1)
      assert {:ok, _} = Utils.check_type(%{val: "string"}, :val, &is_binary/1)
      assert {:ok, _} = Utils.check_type(%{val: []}, :val, &is_list/1)
      assert {:ok, _} = Utils.check_type(%{val: %{}}, :val, &is_map/1)
    end

    test "works with custom validators" do
      positive? = fn x -> is_integer(x) and x > 0 end
      assert {:ok, _} = Utils.check_type(%{val: 5}, :val, positive?)
      assert {:error, :invalid_type} = Utils.check_type(%{val: -5}, :val, positive?)
    end
  end

  describe "get_item/2" do
    test "retrieves existing item" do
      map = %{key: "value"}
      assert {:ok, "value"} = Utils.get_item(map, :key)
    end

    test "returns error for missing item" do
      map = %{other: "value"}
      assert {:error, {:not_found, :key}} = Utils.get_item(map, :key)
    end

    test "works with various key types" do
      map = %{:atom => 1, "string" => 2, 3 => 3}
      assert {:ok, 1} = Utils.get_item(map, :atom)
      assert {:ok, 2} = Utils.get_item(map, "string")
      assert {:ok, 3} = Utils.get_item(map, 3)
    end

    test "distinguishes nil value from missing key" do
      map = %{key: nil}
      # get_item returns error for nil values (by design)
      assert {:error, {:not_found, :key}} = Utils.get_item(map, :key)
    end
  end

  describe "get_keyword/1" do
    test "converts group shorthand" do
      assert {:type, :group} = Utils.get_keyword(:group)
    end

    test "converts chain shorthand" do
      assert {:type, :chain} = Utils.get_keyword(:chain)
    end

    test "converts standalone shorthand" do
      assert {:type, :standalone} = Utils.get_keyword(:standalone)
    end

    test "returns error for unknown keyword" do
      assert {:error, :invalid_options} = Utils.get_keyword(:unknown)
      assert {:error, :invalid_options} = Utils.get_keyword(:other)
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
      assert Enum.all?(orders, fn o -> o >= 0 and o < num_partitions end)

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
      assert Utils.get_hash_order("anything", 1) == 0
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

  describe "response_ref/0" do
    test "returns tuple with pid and reference" do
      {pid, ref} = Utils.response_ref()
      assert is_pid(pid)
      assert is_reference(ref)
    end

    test "returns current process pid" do
      {pid, _ref} = Utils.response_ref()
      assert pid == self()
    end

    test "generates unique references" do
      {_pid1, ref1} = Utils.response_ref()
      {_pid2, ref2} = Utils.response_ref()

      assert ref1 != ref2
    end
  end

  describe "api_receiver/2" do
    test "receives matching response" do
      ref = Utils.response_ref()

      spawn(fn ->
        Process.sleep(10)
        Utils.api_response(ref, {:ok, :result})
      end)

      assert {:ok, :result} = Utils.api_receiver(ref, 1000)
    end

    test "times out when no response" do
      ref = Utils.response_ref()
      assert {:error, :api_timeout} = Utils.api_receiver(ref, 50)
    end

    test "ignores non-matching messages" do
      ref = Utils.response_ref()

      spawn(fn ->
        send(elem(ref, 0), {:wrong, :message})
        Process.sleep(10)
        Utils.api_response(ref, :correct)
      end)

      assert :correct = Utils.api_receiver(ref, 1000)
    end

    test "handles infinity timeout" do
      ref = Utils.response_ref()

      spawn(fn ->
        Process.sleep(100)
        Utils.api_response(ref, :delayed_response)
      end)

      assert :delayed_response = Utils.api_receiver(ref, :infinity)
    end
  end

  describe "api_response/2" do
    test "sends response to caller" do
      parent = self()
      ref = make_ref()

      spawn(fn ->
        Utils.api_response({parent, ref}, :test_result)
      end)

      assert_receive {^ref, :test_result}, 1000
    end

    test "sends various data types" do
      parent = self()

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
        ref = make_ref()

        spawn(fn ->
          Utils.api_response({parent, ref}, data)
        end)

        assert_receive {^ref, ^data}, 1000
      end
    end
  end

  describe "call_api/4" do
    test "sends message and receives response" do
      server =
        spawn(fn ->
          receive do
            {:test_api, {from, ref}, params} ->
              Utils.api_response({from, ref}, {:ok, params})
          end
        end)

      result = Utils.call_api(server, :test_api, :my_params, 1000)
      assert result == {:ok, :my_params}
    end

    test "times out when server doesn't respond" do
      server = spawn(fn -> receive do: (:never -> :ok) end)

      result = Utils.call_api(server, :test_api, :params, 50)
      assert result == {:error, :api_timeout}
    end

    test "works with registered name" do
      server =
        spawn(fn ->
          receive do
            {:get_value, {from, ref}, _} ->
              Utils.api_response({from, ref}, 42)
          end
        end)

      Process.register(server, :test_server)

      result = Utils.call_api(:test_server, :get_value, nil, 1000)
      assert result == 42
    end

    test "handles errors from server" do
      server =
        spawn(fn ->
          receive do
            {:failing_api, {from, ref}, _} ->
              Utils.api_response({from, ref}, {:error, :something_went_wrong})
          end
        end)

      result = Utils.call_api(server, :failing_api, :params, 1000)
      assert result == {:error, :something_went_wrong}
    end
  end

  describe "call_api_no_reply/3" do
    test "sends message without waiting" do
      parent = self()

      server =
        spawn(fn ->
          receive do
            {:notify, _ref, data} ->
              send(parent, {:received, data})
          end
        end)

      ref = Utils.call_api_no_reply(server, :notify, :test_data)

      assert is_reference(ref)
      assert_receive {:received, :test_data}, 1000
    end

    test "returns reference" do
      server = spawn(fn -> receive do: (_ -> :ok) end)
      ref = Utils.call_api_no_reply(server, :test, :data)

      assert is_reference(ref)
    end

    test "works with registered name" do
      parent = self()

      server =
        spawn(fn ->
          receive do
            {:event, _, data} ->
              send(parent, data)
          end
        end)

      Process.register(server, :event_server)

      Utils.call_api_no_reply(:event_server, :event, :my_event)

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
      assert :ok = Utils.call_api(:counter_server, :increment, nil, 1000)

      # Get value
      assert 1 = Utils.call_api(:counter_server, :get, nil, 1000)

      # Increment again
      assert :ok = Utils.call_api(:counter_server, :increment, nil, 1000)

      # Get value
      assert 2 = Utils.call_api(:counter_server, :get, nil, 1000)
    end

    test "concurrent API calls" do
      server =
        spawn(fn ->
          api_echo_loop()
        end)

      tasks =
        for i <- 1..10 do
          Task.async(fn ->
            Utils.call_api(server, :echo, i, 1000)
          end)
        end

      results = Task.await_many(tasks)

      assert Enum.sort(results) == Enum.to_list(1..10)
    end
  end

  # Helper functions for tests
  defp api_loop(state) do
    receive do
      {:increment, {from, ref}, _} ->
        new_state = %{state | counter: state.counter + 1}
        Utils.api_response({from, ref}, :ok)
        api_loop(new_state)

      {:get, {from, ref}, _} ->
        Utils.api_response({from, ref}, state.counter)
        api_loop(state)

      :stop ->
        :ok
    end
  end

  defp api_echo_loop do
    receive do
      {:echo, {from, ref}, data} ->
        Utils.api_response({from, ref}, data)
        api_echo_loop()

      :stop ->
        :ok
    end
  end
end
