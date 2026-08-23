defmodule SuperWorker.ConfigLoader.BootstrapTest do
  use ExUnit.Case, async: false

  alias SuperWorker.ConfigLoader.Bootstrap
  alias SuperWorker.Supervisor

  # Simple worker module for testing
  defmodule TestWorker do
    def start_link do
      pid = spawn_link(fn -> simple_worker() end)
      {:ok, pid}
    end

    def start_link(name) when is_atom(name) do
      pid = spawn(fn -> start_loop(name) end)
      {:ok, pid}
    end

    def start_loop(name) do
      Process.register(self(), name)
      loop(name)
    end

    def loop(name) do
      receive do
        {:ping, from} ->
          send(from, {:pong, name})
          loop(name)

        :stop ->
          :ok

        _ ->
          loop(name)
      end
    end

    def simple_worker do
      receive do
        :stop -> :ok
        _ -> simple_worker()
      end
    end
  end

  setup do
    # Generate unique supervisor ID for each test
    sup_id = :"test_sup_#{:erlang.unique_integer([:positive])}"

    on_exit(fn ->
      Process.sleep(10)

      if Supervisor.running?(sup_id) do
        Supervisor.stop(sup_id)
        wait_until_stopped(sup_id, 1000)
      end
    end)

    {:ok, sup_id: sup_id}
  end

  describe "start_supervisor/1 with valid configurations" do
    test "starts supervisor with minimal configuration", %{sup_id: sup_id} do
      config = %{
        options: [id: sup_id, num_partitions: 1, link: false],
        children: []
      }

      assert {:ok, pid} = Bootstrap.start_supervisor(config)
      assert is_pid(pid)
      assert Supervisor.running?(sup_id)
    end

    test "starts supervisor with custom partitions", %{sup_id: sup_id} do
      config = %{
        options: [id: sup_id, num_partitions: 4, link: false],
        children: []
      }

      assert {:ok, _pid} = Bootstrap.start_supervisor(config)
      assert Supervisor.running?(sup_id)
    end

    test "starts supervisor with a group", %{sup_id: sup_id} do
      config = %{
        options: [id: sup_id, num_partitions: 1, link: false],
        children: [
          %{
            type: :group,
            id: :test_group,
            options: [restart_strategy: :one_for_one],
            workers: []
          }
        ]
      }

      assert {:ok, _pid} = Bootstrap.start_supervisor(config)
      assert Supervisor.running?(sup_id)
      assert Supervisor.group_exists?(sup_id, :test_group)
    end

    test "starts supervisor with a group and workers", %{sup_id: sup_id} do
      config = %{
        options: [id: sup_id, num_partitions: 1, link: false],
        children: [
          %{
            type: :group,
            id: :worker_group,
            options: [restart_strategy: :one_for_one],
            workers: [
              %{
                mfa: {TestWorker, :start_link, []},
                options: [id: :group_worker_1]
              }
            ]
          }
        ]
      }

      assert {:ok, _pid} = Bootstrap.start_supervisor(config)
      assert Supervisor.running?(sup_id)
      assert Supervisor.group_exists?(sup_id, :worker_group)
    end

    test "starts supervisor with a chain", %{sup_id: sup_id} do
      config = %{
        options: [id: sup_id, num_partitions: 1, link: false],
        children: [
          %{
            type: :chain,
            id: :test_chain,
            options: [restart_strategy: :rest_for_one, send_type: :round_robin],
            workers: []
          }
        ]
      }

      assert {:ok, _pid} = Bootstrap.start_supervisor(config)
      assert Supervisor.running?(sup_id)

      # Note: Chain might not have a get_chain function exposed, so we just verify supervisor started
    end

    test "starts supervisor with a chain and workers", %{sup_id: sup_id} do
      config = %{
        options: [id: sup_id, num_partitions: 1, link: false],
        children: [
          %{
            type: :chain,
            id: :worker_chain,
            options: [restart_strategy: :one_for_one, send_type: :broadcast],
            workers: [
              %{
                mfa: {TestWorker, :start_link, []},
                options: [id: :chain_worker_1]
              }
            ]
          }
        ]
      }

      assert {:ok, _pid} = Bootstrap.start_supervisor(config)
      assert Supervisor.running?(sup_id)
    end

    test "starts supervisor with standalone worker using MFA", %{sup_id: sup_id} do
      config = %{
        options: [id: sup_id, num_partitions: 1, link: false],
        children: [
          %{
            type: :standalone,
            mfa: {TestWorker, :start_link, []},
            options: [id: :standalone_1]
          }
        ]
      }

      assert {:ok, _pid} = Bootstrap.start_supervisor(config)
      assert Supervisor.running?(sup_id)
    end

    test "starts supervisor with standalone worker using function", %{sup_id: sup_id} do
      fun = fn ->
        pid = spawn(fn -> TestWorker.simple_worker() end)
        {:ok, pid}
      end

      config = %{
        options: [id: sup_id, num_partitions: 1, link: false],
        children: [
          %{
            type: :standalone,
            mfa: {:fun, fun},
            options: [id: :standalone_fun]
          }
        ]
      }

      assert {:ok, _pid} = Bootstrap.start_supervisor(config)
      assert Supervisor.running?(sup_id)
    end

    test "starts supervisor with multiple children types", %{sup_id: sup_id} do
      config = %{
        options: [id: sup_id, num_partitions: 2, link: false],
        children: [
          %{
            type: :group,
            id: :multi_group,
            options: [restart_strategy: :one_for_one],
            workers: [
              %{
                mfa: {TestWorker, :start_link, []},
                options: [id: :mg_worker1]
              }
            ]
          },
          %{
            type: :chain,
            id: :multi_chain,
            options: [restart_strategy: :rest_for_one],
            workers: []
          },
          %{
            type: :standalone,
            mfa: {TestWorker, :start_link, []},
            options: [id: :multi_standalone]
          }
        ]
      }

      assert {:ok, _pid} = Bootstrap.start_supervisor(config)
      assert Supervisor.running?(sup_id)
      assert Supervisor.group_exists?(sup_id, :multi_group)
    end

    test "starts supervisor with multiple groups", %{sup_id: sup_id} do
      config = %{
        options: [id: sup_id, num_partitions: 1, link: false],
        children: [
          %{
            type: :group,
            id: :group1,
            options: [restart_strategy: :one_for_one],
            workers: []
          },
          %{
            type: :group,
            id: :group2,
            options: [restart_strategy: :one_for_all],
            workers: []
          }
        ]
      }

      assert {:ok, _pid} = Bootstrap.start_supervisor(config)
      assert Supervisor.group_exists?(sup_id, :group1)
      assert Supervisor.group_exists?(sup_id, :group2)
    end
  end

  describe "start_supervisor/1 error handling" do
    test "returns error when supervisor ID is missing" do
      config = %{
        options: [num_partitions: 1],
        children: []
      }

      assert {:error, :missing_supervisor_id} = Bootstrap.start_supervisor(config)
    end

    test "returns error for invalid configuration format" do
      assert {:error, :invalid_config_format} = Bootstrap.start_supervisor("invalid")
      assert {:error, :invalid_config_format} = Bootstrap.start_supervisor([])
      assert {:error, :invalid_config_format} = Bootstrap.start_supervisor(%{invalid: :config})
    end

    test "returns error when supervisor with same ID already exists", %{sup_id: sup_id} do
      config = %{
        options: [id: sup_id, num_partitions: 1, link: false],
        children: []
      }

      assert {:ok, _pid} = Bootstrap.start_supervisor(config)
      assert {:error, :already_running} = Bootstrap.start_supervisor(config)
    end

    test "cleans up supervisor when children fail to start", %{sup_id: sup_id} do
      # Using an invalid child type to force failure
      config = %{
        options: [id: sup_id, num_partitions: 1, link: false],
        children: [
          %{
            type: :invalid_type,
            id: :bad_child,
            options: [],
            workers: []
          }
        ]
      }

      result = Bootstrap.start_supervisor(config)
      assert match?({:error, _}, result)

      # Verify supervisor was cleaned up
      Process.sleep(100)
      refute Supervisor.running?(sup_id)
    end
  end

  describe "start_supervisor/1 edge cases" do
    test "handles empty children list gracefully", %{sup_id: sup_id} do
      config = %{
        options: [id: sup_id, num_partitions: 1, link: false],
        children: []
      }

      assert {:ok, _pid} = Bootstrap.start_supervisor(config)
      assert Supervisor.running?(sup_id)
    end

    test "handles group with empty workers list", %{sup_id: sup_id} do
      config = %{
        options: [id: sup_id, num_partitions: 1, link: false],
        children: [
          %{
            type: :group,
            id: :empty_group,
            options: [restart_strategy: :one_for_one],
            workers: []
          }
        ]
      }

      assert {:ok, _pid} = Bootstrap.start_supervisor(config)
      assert Supervisor.group_exists?(sup_id, :empty_group)
    end

    test "handles chain with empty workers list", %{sup_id: sup_id} do
      config = %{
        options: [id: sup_id, num_partitions: 1, link: false],
        children: [
          %{
            type: :chain,
            id: :empty_chain,
            options: [restart_strategy: :one_for_one],
            workers: []
          }
        ]
      }

      assert {:ok, _pid} = Bootstrap.start_supervisor(config)
      assert Supervisor.running?(sup_id)
    end

    test "handles minimal options", %{sup_id: sup_id} do
      config = %{
        options: [id: sup_id, link: false],
        children: []
      }

      # Should use defaults for missing options
      assert {:ok, _pid} = Bootstrap.start_supervisor(config)
      assert Supervisor.running?(sup_id)
    end
  end

  describe "integration with Parser" do
    test "works with Parser output for complete workflow", %{sup_id: sup_id} do
      alias SuperWorker.ConfigLoader.Parser

      raw_config = [
        options: [
          num_partitions: 2,
          link: false
        ],
        groups: [
          integration_group: [
            restart_strategy: :one_for_one,
            workers: [
              [
                mfa: {TestWorker, :start_link, []},
                options: [id: :int_worker1]
              ]
            ]
          ]
        ]
      ]

      assert {:ok, parsed_config} = Parser.parse(raw_config)

      # Add supervisor ID to parsed config
      config_with_id = put_in(parsed_config, [:options, :id], sup_id)

      assert {:ok, _pid} = Bootstrap.start_supervisor(config_with_id)
      assert Supervisor.running?(sup_id)
      assert Supervisor.group_exists?(sup_id, :integration_group)
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

  describe "start_supervisor/1 child failure branches" do
    test "reports failure when a group has invalid options", %{sup_id: sup_id} do
      _ = sup_id

      sup = :"bs_grp_fail_#{System.unique_integer([:positive])}"

      config = %{
        options: [id: sup, num_partitions: 1, link: false],
        children: [
          %{
            type: :group,
            id: :bad_group,
            options: [restart_strategy: :all_for_one],
            workers: []
          }
        ]
      }

      assert {:error, _reason} = Bootstrap.start_supervisor(config)
    end

    test "reports failure when a chain has invalid options", %{sup_id: sup_id} do
      _ = sup_id
      sup = :"bs_chain_fail_#{System.unique_integer([:positive])}"

      config = %{
        options: [id: sup, num_partitions: 1, link: false],
        children: [
          %{type: :chain, id: :bad_chain, options: [queue_length: 0], workers: []}
        ]
      }

      assert {:error, _reason} = Bootstrap.start_supervisor(config)
    end

    test "reports failure when a group worker has invalid options", %{sup_id: sup_id} do
      _ = sup_id
      sup = :"bs_gw_fail_#{System.unique_integer([:positive])}"

      config = %{
        options: [id: sup, num_partitions: 1, link: false],
        children: [
          %{
            type: :group,
            id: :g1,
            options: [restart_strategy: :one_for_one],
            workers: [
              %{
                mfa: {MyTest, :loop, [1]},
                options: [id: :w1, restart_strategy: :sometimes]
              }
            ]
          }
        ]
      }

      assert match?({:error, _}, Bootstrap.start_supervisor(config))
    after
      # nothing to clean up: the failed start tears the supervisor down
      :ok
    end

    test "rejects invalid child entries", %{sup_id: sup_id} do
      _ = sup_id
      sup = :"bs_invalid_#{System.unique_integer([:positive])}"

      config = %{
        options: [id: sup, num_partitions: 1, link: false],
        children: [%{type: :swarm, id: :weird}]
      }

      assert {:error, {:children_start_errors, [error: :invalid_child]}} =
               Bootstrap.start_supervisor(config)
    end
  end
end
