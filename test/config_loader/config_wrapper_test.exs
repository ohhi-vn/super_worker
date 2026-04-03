defmodule SuperWorker.ConfigLoader.ConfigParserTest do
  use ExUnit.Case, async: false

  alias SuperWorker.ConfigLoader.ConfigParser
  alias SuperWorker.Supervisor

  @app :super_worker

  # Simple worker module for testing
  defmodule TestWorker do
    def start_link do
      pid = spawn_link(fn -> simple_worker() end)
      {:ok, pid}
    end

    def simple_worker do
      receive do
        :stop -> :ok
        _ -> simple_worker()
      end

      IO.puts("exiting...")
    end
  end

  setup do
    # Save original config
    original_config = Application.get_all_env(@app)

    # Clear all configs except :options
    Application.get_all_env(@app)
    |> Enum.each(fn {key, _} ->
      Application.delete_env(@app, key)
    end)

    on_exit(fn ->
      # Clean up any running supervisors
      Application.get_all_env(@app)
      |> Enum.each(fn {key, _} ->
        if is_atom(key) and Supervisor.running?(key) do
          Process.sleep(100)

          if Supervisor.running?(key) do
            Supervisor.stop(key)
          end

          wait_until_stopped(key, 1000)
        end
      end)

      # Restore original config
      Application.get_all_env(@app)
      |> Enum.each(fn {key, _} ->
        Application.delete_env(@app, key)
      end)

      Enum.each(original_config, fn {key, value} ->
        Application.put_env(@app, key, value)
      end)
    end)

    :ok
  end

  describe "load/0 with no configurations" do
    test "handles empty configuration gracefully" do
      # Ensure no configs except maybe :options
      Application.delete_env(@app, :test_sup)

      assert :ok = ConfigParser.load()
    end

    test "ignores :options key" do
      Application.put_env(@app, :options, some: :global_option)

      assert :ok = ConfigParser.load()
      refute Supervisor.running?(:options)
    end
  end

  describe "load/0 with valid configurations" do
    test "loads a single supervisor configuration" do
      sup_id = :load_test_sup_1

      config = [
        options: [
          num_partitions: 1,
          link: false
        ],
        groups: [
          test_group: [
            restart_strategy: :one_for_one,
            workers: []
          ]
        ]
      ]

      Application.put_env(@app, sup_id, config)

      assert :ok = ConfigParser.load()
      assert Supervisor.running?(sup_id)
      assert Supervisor.group_exists?(sup_id, :test_group)

      # Cleanup
      Supervisor.stop(sup_id)
    end

    test "loads multiple supervisor configurations" do
      sup_id_1 = :load_test_sup_multi_1
      sup_id_2 = :load_test_sup_multi_2

      config_1 = [
        options: [num_partitions: 1, link: false],
        groups: [group1: [restart_strategy: :one_for_one, workers: []]]
      ]

      config_2 = [
        options: [num_partitions: 1, link: false],
        groups: [group2: [restart_strategy: :one_for_all, workers: []]]
      ]

      Application.put_env(@app, sup_id_1, config_1)
      Application.put_env(@app, sup_id_2, config_2)

      assert :ok = ConfigParser.load()
      assert Supervisor.running?(sup_id_1)
      assert Supervisor.running?(sup_id_2)

      # Cleanup
      Supervisor.stop(sup_id_1)
      Supervisor.stop(sup_id_2)
    end

    test "loads configuration with workers" do
      sup_id = :load_test_with_workers
      group_id = :worker_group
      worker_id = :worker_1

      config = [
        options: [num_partitions: 1, link: false],
        groups: [
          {group_id,
           [
             restart_strategy: :one_for_one,
             workers: [
               [
                 mfa: {TestWorker, :simple_worker, []},
                 options: [id: worker_id]
               ]
             ]
           ]}
        ]
      ]

      Application.put_env(@app, sup_id, config)

      assert :ok = ConfigParser.load()
      Process.sleep(10)
      assert Supervisor.running?(sup_id)
      assert Supervisor.group_exists?(sup_id, group_id)
      result = Supervisor.get_pid_group_worker(sup_id, group_id, worker_id)
      assert match?({:ok, _pid}, result)
      # Cleanup
      Supervisor.stop(sup_id)
    end

    test "loads configuration with multiple child types" do
      sup_id = :load_test_multi_children

      config = [
        options: [num_partitions: 2, link: false],
        groups: [
          test_group: [restart_strategy: :one_for_one, workers: []]
        ],
        chains: [
          test_chain: [restart_strategy: :rest_for_one, workers: []]
        ],
        workers: [
          [mfa: {TestWorker, :start_link, []}, options: [id: :standalone1]]
        ]
      ]

      Application.put_env(@app, sup_id, config)

      assert :ok = ConfigParser.load()
      Process.sleep(10)
      assert Supervisor.running?(sup_id)

      # Cleanup
      Supervisor.stop(sup_id)
    end
  end

  describe "load/0 with invalid configurations" do
    test "continues loading other supervisors when one fails" do
      sup_id_valid = :load_test_valid
      sup_id_invalid = :load_test_invalid

      valid_config = [
        options: [num_partitions: 1, link: false],
        groups: [valid_group: [restart_strategy: :one_for_one, workers: []]]
      ]

      # Invalid config - group missing id
      invalid_config = [
        options: [num_partitions: 1, link: false],
        groups: [[restart_strategy: :one_for_one, workers: []]]
      ]

      Application.put_env(@app, sup_id_valid, valid_config)
      Application.put_env(@app, sup_id_invalid, invalid_config)

      # load/0 now returns {:error, [...]} when any supervisor fails,
      # but it still continues loading other valid supervisors.
      assert {:error, _errors} = ConfigParser.load()

      # Valid supervisor should have started
      assert Supervisor.running?(sup_id_valid)
      # Invalid supervisor should not be running
      refute Supervisor.running?(sup_id_invalid)

      # Cleanup
      Supervisor.stop(sup_id_valid)
    end
  end

  describe "load_one/1 with valid configuration" do
    test "loads a specific supervisor configuration" do
      sup_id = :load_one_test_sup

      config = [
        options: [num_partitions: 1, link: false],
        groups: [one_group: [restart_strategy: :one_for_one, workers: []]]
      ]

      Application.put_env(@app, sup_id, config)

      assert {:ok, pid} = ConfigParser.load_one(sup_id)
      assert is_pid(pid)
      assert Supervisor.running?(sup_id)
      assert Supervisor.group_exists?(sup_id, :one_group)

      # Cleanup
      Supervisor.stop(sup_id)
    end

    test "loads supervisor with workers" do
      sup_id = :load_one_with_workers

      config = [
        options: [num_partitions: 1, link: false],
        groups: [
          group_with_workers: [
            restart_strategy: :one_for_one,
            workers: [
              [mfa: {TestWorker, :start_link, []}, options: [id: :w1]]
            ]
          ]
        ]
      ]

      Application.put_env(@app, sup_id, config)

      assert {:ok, _pid} = ConfigParser.load_one(sup_id)
      assert Supervisor.running?(sup_id)

      # Cleanup
      Supervisor.stop(sup_id)
    end

    test "loads supervisor with multiple children types" do
      sup_id = :load_one_multi_type

      config = [
        options: [num_partitions: 2, link: false],
        groups: [g1: [restart_strategy: :one_for_one, workers: []]],
        chains: [c1: [restart_strategy: :one_for_all, workers: []]],
        workers: [[mfa: {TestWorker, :start_link, []}, options: [id: :s1]]]
      ]

      Application.put_env(@app, sup_id, config)

      assert {:ok, _pid} = ConfigParser.load_one(sup_id)
      assert Supervisor.running?(sup_id)

      # Cleanup
      Supervisor.stop(sup_id)
    end
  end

  describe "load_one/1 error handling" do
    test "returns error when configuration not found" do
      non_existent = :non_existent_supervisor

      assert {:error, :config_not_found} = ConfigParser.load_one(non_existent)
      refute Supervisor.running?(non_existent)
    end

    test "returns error when configuration is invalid" do
      sup_id = :invalid_config_sup

      # Invalid config - group missing required id
      invalid_config = [
        options: [num_partitions: 1, link: false],
        groups: [[restart_strategy: :one_for_one, workers: []]]
      ]

      Application.put_env(@app, sup_id, invalid_config)

      result = ConfigParser.load_one(sup_id)
      assert match?({:error, _}, result)
      refute Supervisor.running?(sup_id)
    end

    test "returns error when supervisor with same ID already exists" do
      sup_id = :duplicate_sup

      config = [
        options: [num_partitions: 1, link: false],
        groups: []
      ]

      Application.put_env(@app, sup_id, config)

      assert {:ok, _pid} = ConfigParser.load_one(sup_id)
      assert {:error, :already_running} = ConfigParser.load_one(sup_id)

      # Cleanup - safely stop supervisor, handling race conditions
      try do
        Supervisor.stop(sup_id)
      catch
        :exit, _ -> :ok
      end
    end
  end

  describe "load_one/1 edge cases" do
    test "handles empty configuration" do
      sup_id = :empty_config_sup

      config = [
        options: [num_partitions: 1, link: false]
      ]

      Application.put_env(@app, sup_id, config)

      assert {:ok, _pid} = ConfigParser.load_one(sup_id)
      assert Supervisor.running?(sup_id)

      # Cleanup
      Supervisor.stop(sup_id)
    end

    test "handles minimal options" do
      sup_id = :minimal_options_sup

      config = [
        options: []
      ]

      Application.put_env(@app, sup_id, config)

      assert {:ok, _pid} = ConfigParser.load_one(sup_id)
      assert Supervisor.running?(sup_id)

      # Cleanup
      Supervisor.stop(sup_id)
    end
  end

  describe "integration scenarios" do
    test "load/0 followed by load_one/1 for different supervisor" do
      sup_id_1 = :integration_sup_1
      sup_id_2 = :integration_sup_2

      config_1 = [
        options: [num_partitions: 1, link: false],
        groups: [int_group_1: [restart_strategy: :one_for_one, workers: []]]
      ]

      config_2 = [
        options: [num_partitions: 1, link: false],
        groups: [int_group_2: [restart_strategy: :one_for_all, workers: []]]
      ]

      Application.put_env(@app, sup_id_1, config_1)

      # Load first supervisor
      assert :ok = ConfigParser.load()
      assert Supervisor.running?(sup_id_1)

      # Add and load second supervisor
      Application.put_env(@app, sup_id_2, config_2)
      assert {:ok, _pid} = ConfigParser.load_one(sup_id_2)
      assert Supervisor.running?(sup_id_2)

      # Both should be running
      assert Supervisor.running?(sup_id_1)
      assert Supervisor.running?(sup_id_2)

      # Cleanup
      Supervisor.stop(sup_id_1)
      Supervisor.stop(sup_id_2)
    end

    test "supervisor ID is properly set from config key" do
      sup_id = :id_test_supervisor

      config = [
        options: [num_partitions: 1, link: false],
        groups: []
      ]

      Application.put_env(@app, sup_id, config)

      assert {:ok, _pid} = ConfigParser.load_one(sup_id)
      assert Supervisor.running?(sup_id)

      # Cleanup
      Supervisor.stop(sup_id)
      Process.sleep(10)
    end
  end

  # Helper function to wait until supervisor is stopped
  defp wait_until_stopped(_sup_id, timeout) when timeout <= 0 do
    :timeout
  end

  defp wait_until_stopped(sup_id, timeout) do
    if Supervisor.running?(sup_id) do
      Process.sleep(50)
      wait_until_stopped(sup_id, timeout - 50)
    else
      :ok
    end
  end
end
