defmodule SuperWorker.ConfigLoader.ParserTest do
  use ExUnit.Case, async: true

  defmodule GenServerTest do
    use GenServer

    def start_link(opts) do
      GenServer.start_link(__MODULE__, opts)
    end

    def init(opts) do
      {:ok, opts}
    end
  end

  alias SuperWorker.ConfigLoader.Parser

  describe "parse/1 with valid configurations" do
    test "parses minimal valid configuration with empty children" do
      config = [
        options: [
          number_of_partitions: 2,
          link: false
        ]
      ]

      assert {:ok, parsed} = Parser.parse(config)
      assert parsed.options[:number_of_partitions] == 2
      assert parsed.options[:link] == false
      assert parsed.children == []
    end

    test "parses configuration with groups" do
      config = [
        options: [number_of_partitions: 1],
        groups: [
          [
            id: :my_group,
            restart_strategy: :one_for_one,
            workers: []
          ]
        ]
      ]

      assert {:ok, parsed} = Parser.parse(config)
      assert length(parsed.children) == 1
      assert [group] = parsed.children
      assert group.type == :group
      assert group.id == :my_group
      assert group.options[:restart_strategy] == :one_for_one
    end

    test "parses configuration with chains" do
      config = [
        options: [number_of_partitions: 1],
        chains: [
          [
            id: :my_chain,
            restart_strategy: :rest_for_one,
            send_type: :round_robin,
            workers: []
          ]
        ]
      ]

      assert {:ok, parsed} = Parser.parse(config)
      assert length(parsed.children) == 1
      assert [chain] = parsed.children
      assert chain.type == :chain
      assert chain.id == :my_chain
      assert chain.options[:restart_strategy] == :rest_for_one
      assert chain.options[:send_type] == :round_robin
    end

    test "parses configuration with standalone workers" do
      config = [
        options: [number_of_partitions: 1],
        workers: [
          [
            mfa: {MyModule, :my_function, [:arg1]},
            options: [id: :worker1]
          ],
          GenServerTest,
          {GenServerTest, []}
        ]
      ]

      assert {:ok, parsed} = Parser.parse(config)
      assert length(parsed.children) == 3
      assert [worker | _] = parsed.children
      assert worker.type == :standalone
      assert worker.mfa == {MyModule, :my_function, [:arg1]}
      assert worker.options[:id] == :worker1
    end

    test "parses configuration with function-based standalone worker" do
      fun = fn -> :ok end

      config = [
        options: [number_of_partitions: 1],
        workers: [
          [
            fun: fun,
            options: [id: :worker1]
          ]
        ]
      ]

      assert {:ok, parsed} = Parser.parse(config)
      assert length(parsed.children) == 1
      assert [worker] = parsed.children
      assert worker.type == :standalone
      assert worker.mfa == {:fun, fun}
    end

    test "parses group with workers" do
      config = [
        options: [],
        groups: [
          [
            id: :group_with_workers,
            restart_strategy: :one_for_all,
            workers: [
              [
                mfa: {MyModule, :worker1, []},
                options: [id: :w1]
              ],
              [
                mfa: {MyModule, :worker2, []},
                options: [id: :w2]
              ],
              GenServerTest,
              {GenServerTest, []}
            ]
          ]
        ]
      ]

      assert {:ok, parsed} = Parser.parse(config)
      assert [group] = parsed.children
      assert length(group.workers) == 4
      assert Enum.at(group.workers, 0).mfa == {MyModule, :worker1, []}
      assert Enum.at(group.workers, 1).mfa == {MyModule, :worker2, []}
    end

    test "parses chain with workers" do
      config = [
        options: [],
        chains: [
          [
            id: :chain_with_workers,
            restart_strategy: :before_for_one,
            send_type: :broadcast,
            workers: [
              [
                mfa: {MyModule, :step1, []},
                options: [id: :step1]
              ],
              [
                mfa: {MyModule, :step2, []},
                options: [id: :step2]
              ],
              GenServerTest,
              {GenServerTest, []}
            ]
          ]
        ]
      ]

      assert {:ok, parsed} = Parser.parse(config)
      assert [chain] = parsed.children
      assert length(chain.workers) == 4
      assert chain.options[:send_type] == :broadcast
      assert chain.options[:restart_strategy] == :before_for_one
    end

    test "parses complex configuration with multiple children types" do
      config = [
        options: [
          number_of_partitions: 4,
          link: true,
          report_to: []
        ],
        groups: [
          [
            id: :group1,
            restart_strategy: :one_for_one,
            workers: [
              [mfa: {MyModule, :g1_worker, []}, options: [id: :g1w1]]
            ]
          ]
        ],
        chains: [
          [
            id: :chain1,
            restart_strategy: :rest_for_one,
            send_type: :partition,
            workers: [
              [mfa: {MyModule, :c1_step1, []}, options: [id: :c1s1]]
            ]
          ]
        ],
        workers: [
          [mfa: {MyModule, :standalone, []}, options: [id: :sa1]]
        ]
      ]

      assert {:ok, parsed} = Parser.parse(config)
      assert parsed.options[:number_of_partitions] == 4
      assert parsed.options[:link] == true
      assert length(parsed.children) == 3

      types = Enum.map(parsed.children, & &1.type)
      assert :group in types
      assert :chain in types
      assert :standalone in types
    end
  end

  describe "parse/1 with invalid configurations" do
    test "returns error for non-list configuration" do
      assert {:error, :invalid_config_format} = Parser.parse("not a list")
      assert {:error, :invalid_config_format} = Parser.parse(%{})
      assert {:error, :invalid_config_format} = Parser.parse(123)
    end

    test "returns error for group without id" do
      config = [
        groups: [
          [
            restart_strategy: :one_for_one,
            workers: []
          ]
        ]
      ]

      assert {:error, {:group_parsing_errors, _}} = Parser.parse(config)
    end

    test "returns error for chain without id" do
      config = [
        chains: [
          [
            restart_strategy: :rest_for_one,
            workers: []
          ]
        ]
      ]

      assert {:error, {:chain_parsing_errors, _}} = Parser.parse(config)
    end

    test "returns error for worker without mfa or fun" do
      config = [
        workers: [
          [
            options: [id: :worker1]
          ]
        ]
      ]

      assert {:error, {:worker_parsing_errors, _}} = Parser.parse(config)
    end

    test "returns error for invalid mfa format" do
      config = [
        workers: [
          [
            mfa: "not a tuple",
            options: [id: :worker1]
          ]
        ]
      ]

      assert {:error, {:worker_parsing_errors, _}} = Parser.parse(config)
    end

    test "returns error for invalid mfa tuple" do
      config = [
        workers: [
          [
            mfa: {"NotAnAtom", :func, []},
            options: [id: :worker1]
          ]
        ]
      ]

      assert {:error, {:worker_parsing_errors, _}} = Parser.parse(config)
    end

    test "returns error for invalid function arity" do
      config = [
        workers: [
          [
            fun: fn x -> x end,
            options: [id: :worker1]
          ]
        ]
      ]

      assert {:error, {:worker_parsing_errors, _}} = Parser.parse(config)
    end

    test "returns error for group with invalid id type" do
      config = [
        groups: [
          [
            id: "not_an_atom",
            restart_strategy: :one_for_one
          ]
        ]
      ]

      assert {:error, {:group_parsing_errors, _}} = Parser.parse(config)
    end

    test "returns error for chain with invalid id type" do
      config = [
        chains: [
          [
            id: 123,
            restart_strategy: :rest_for_one
          ]
        ]
      ]

      assert {:error, {:chain_parsing_errors, _}} = Parser.parse(config)
    end
  end

  describe "parse/1 with validation and defaults" do
    test "uses default values for invalid restart_strategy in groups" do
      config = [
        groups: [
          [
            id: :my_group,
            restart_strategy: :invalid_strategy,
            workers: []
          ]
        ]
      ]

      assert {:ok, parsed} = Parser.parse(config)
      assert [group] = parsed.children
      assert group.options[:restart_strategy] == :one_for_one
    end

    test "uses default values for invalid restart_strategy in chains" do
      config = [
        chains: [
          [
            id: :my_chain,
            restart_strategy: :invalid_strategy,
            workers: []
          ]
        ]
      ]

      assert {:ok, parsed} = Parser.parse(config)
      assert [chain] = parsed.children
      assert chain.options[:restart_strategy] == :one_for_one
    end

    test "uses default values for invalid send_type in chains" do
      config = [
        chains: [
          [
            id: :my_chain,
            send_type: :invalid_send_type,
            workers: []
          ]
        ]
      ]

      assert {:ok, parsed} = Parser.parse(config)
      assert [chain] = parsed.children
      assert chain.options[:send_type] == :round_robin
    end

    test "filters out unknown supervisor options" do
      config = [
        options: [
          number_of_partitions: 2,
          unknown_option: :value,
          another_unknown: 123
        ]
      ]

      assert {:ok, parsed} = Parser.parse(config)
      refute Keyword.has_key?(parsed.options, :unknown_option)
      refute Keyword.has_key?(parsed.options, :another_unknown)
      assert parsed.options[:number_of_partitions] == 2
    end

    test "handles valid group restart strategies" do
      for strategy <- [:one_for_one, :one_for_all] do
        config = [
          groups: [
            [id: :group, restart_strategy: strategy, workers: []]
          ]
        ]

        assert {:ok, parsed} = Parser.parse(config)
        assert [group] = parsed.children
        assert group.options[:restart_strategy] == strategy
      end
    end

    test "handles valid chain restart strategies" do
      for strategy <- [:one_for_one, :one_for_all, :rest_for_one, :before_for_one] do
        config = [
          chains: [
            [id: :chain, restart_strategy: strategy, workers: []]
          ]
        ]

        assert {:ok, parsed} = Parser.parse(config)
        assert [chain] = parsed.children
        assert chain.options[:restart_strategy] == strategy
      end
    end

    test "handles valid chain send types" do
      for send_type <- [:broadcast, :random, :partition, :round_robin] do
        config = [
          chains: [
            [id: :chain, send_type: send_type, workers: []]
          ]
        ]

        assert {:ok, parsed} = Parser.parse(config)
        assert [chain] = parsed.children
        assert chain.options[:send_type] == send_type
      end
    end
  end

  describe "parse/1 edge cases" do
    test "handles empty groups list" do
      config = [groups: []]
      assert {:ok, parsed} = Parser.parse(config)
      assert parsed.children == []
    end

    test "handles empty chains list" do
      config = [chains: []]
      assert {:ok, parsed} = Parser.parse(config)
      assert parsed.children == []
    end

    test "handles empty workers list" do
      config = [workers: []]
      assert {:ok, parsed} = Parser.parse(config)
      assert parsed.children == []
    end

    test "handles missing options key" do
      config = [
        groups: [
          [id: :group, workers: []]
        ]
      ]

      assert {:ok, parsed} = Parser.parse(config)
      assert is_list(parsed.options)
    end

    test "handles group with empty workers list" do
      config = [
        groups: [
          [id: :group, restart_strategy: :one_for_one, workers: []]
        ]
      ]

      assert {:ok, parsed} = Parser.parse(config)
      assert [group] = parsed.children
      assert group.workers == []
    end

    test "handles chain with empty workers list" do
      config = [
        chains: [
          [id: :chain, restart_strategy: :rest_for_one, workers: []]
        ]
      ]

      assert {:ok, parsed} = Parser.parse(config)
      assert [chain] = parsed.children
      assert chain.workers == []
    end

    test "handles worker with empty options" do
      config = [
        workers: [
          [mfa: {MyModule, :func, []}]
        ]
      ]

      assert {:ok, parsed} = Parser.parse(config)
      assert [worker] = parsed.children
      assert worker.options == []
    end

    test "handles multiple groups with same configuration" do
      config = [
        groups: [
          [id: :group1, restart_strategy: :one_for_one, workers: []],
          [id: :group2, restart_strategy: :one_for_one, workers: []]
        ]
      ]

      assert {:ok, parsed} = Parser.parse(config)
      assert length(parsed.children) == 2
      assert Enum.at(parsed.children, 0).id == :group1
      assert Enum.at(parsed.children, 1).id == :group2
    end

    test "handles invalid number_of_partitions gracefully" do
      config = [
        options: [
          number_of_partitions: -1
        ]
      ]

      assert {:ok, parsed} = Parser.parse(config)
      refute Keyword.has_key?(parsed.options, :number_of_partitions)
    end

    test "handles invalid link value gracefully" do
      config = [
        options: [
          link: "not_a_boolean"
        ]
      ]

      assert {:ok, parsed} = Parser.parse(config)
      assert parsed.options[:link] == true
    end
  end
end
