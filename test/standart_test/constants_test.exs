defmodule SuperWorker.Supervisor.ConstantsTest do
  use ExUnit.Case, async: true

  alias SuperWorker.Supervisor.Constants.{Strategies, Timeouts, Types, Validation}

  describe "Types" do
    test "exposes non-empty type lists" do
      assert :standalone in Types.worker_types()
      assert :group in Types.worker_types()
      assert :chain in Types.worker_types()

      assert is_list(Types.chain_send_types())
      refute Enum.empty?(Types.chain_send_types())

      assert :kill in Types.shutdown_types()

      assert is_list(Types.api_messages())
      assert is_list(Types.exit_reasons())

      assert :id in Types.supervisor_params()
    end

    test "param groups contain the essential keys" do
      assert :id in Types.chain_params()
      assert :restart_strategy in Types.group_params()
      # standalone-specific params plus the shared worker params
      assert :max_restarts in Types.standalone_params()
      assert :fun in Types.worker_params()
      assert :fun in Types.standalone_worker_params()
    end
  end

  describe "Timeouts" do
    test "returns positive integer defaults" do
      assert is_integer(Timeouts.default_api_timeout()) and Timeouts.default_api_timeout() > 0
      assert is_integer(Timeouts.default_stop_timeout()) and Timeouts.default_stop_timeout() > 0

      assert is_integer(Timeouts.default_chain_timeout()) and
               Timeouts.default_chain_timeout() > 0
    end
  end

  describe "Validation" do
    test "queue length bounds" do
      assert Validation.default_queue_length() == 50
      assert Validation.min_queue_length() == 1
      assert Validation.max_queue_length() == 10_000
    end

    test "default_partitions matches schedulers" do
      assert Validation.default_partitions() == System.schedulers_online()
    end

    test "valid_restart_strategy?/2 checks per-type strategies" do
      Enum.each(Strategies.group_restart_strategies(), fn strategy ->
        assert Validation.valid_restart_strategy?(strategy, :group)
      end)

      Enum.each(Strategies.chain_restart_strategies(), fn strategy ->
        assert Validation.valid_restart_strategy?(strategy, :chain)
      end)

      Enum.each(Strategies.standalone_restart_strategies(), fn strategy ->
        assert Validation.valid_restart_strategy?(strategy, :standalone)
      end)

      refute Validation.valid_restart_strategy?(:bogus, :group)
      refute Validation.valid_restart_strategy?(:one_for_one, :unknown_type)
    end

    test "valid_send_type?/1" do
      Enum.each(Types.chain_send_types(), fn type ->
        assert Validation.valid_send_type?(type)
      end)

      refute Validation.valid_send_type?(:carrier_pigeon)
    end

    test "valid_worker_type?/1" do
      assert Validation.valid_worker_type?(:group)
      assert Validation.valid_worker_type?(:chain)
      assert Validation.valid_worker_type?(:standalone)
      refute Validation.valid_worker_type?(:swarm)
    end

    test "valid_queue_length?/1" do
      assert Validation.valid_queue_length?(1)
      assert Validation.valid_queue_length?(50)
      assert Validation.valid_queue_length?(10_000)
      refute Validation.valid_queue_length?(0)
      refute Validation.valid_queue_length?(10_001)
      refute Validation.valid_queue_length?("50")
    end

    test "valid_shutdown_type?/1" do
      Enum.each(Types.shutdown_types(), fn type ->
        assert Validation.valid_shutdown_type?(type)
      end)

      refute Validation.valid_shutdown_type?(:gently)
    end
  end
end
