# Example configuration for SuperWorker ConfigLoader
#
# This file demonstrates how to configure supervisors using the ConfigLoader system.
# The ConfigLoader automatically loads supervisor configurations from the application
# environment when the application starts.
#
# To use this configuration, copy it to your config/config.exs or config/runtime.exs
# and adjust it to your needs.

import Config

# =============================================================================
# EXAMPLE 1: Simple Supervisor with a Group
# =============================================================================
#
# This example shows the most basic configuration with a single group of workers.
# All workers in a group share the same restart strategy.

config :super_worker, :example_simple_supervisor,
  # Supervisor-level options
  options: [
    # Number of partitions for distributing work (default: number of schedulers)
    number_of_partitions: 2,
    # Whether to link the supervisor to the calling process
    link: false,
    # List of PIDs to report worker crashes and events to
    report_to: []
  ],
  # Groups of workers
  groups: [
    [
      # Unique identifier for the group
      id: :worker_pool,
      # Restart strategy: :one_for_one or :one_for_all
      restart_strategy: :one_for_one,
      # Workers in this group
      workers: [
        [
          # Worker function as {Module, :function, [args]}
          mfa: {MyApp.Workers.EmailWorker, :start_link, []},
          options: [id: :email_worker_1]
        ],
        [
          mfa: {MyApp.Workers.EmailWorker, :start_link, []},
          options: [id: :email_worker_2]
        ]
      ]
    ]
  ]

# =============================================================================
# EXAMPLE 2: Supervisor with Chains
# =============================================================================
#
# Chains are useful for sequential processing where data flows from one
# worker to the next. Each worker processes the data and passes it to the
# next worker in the chain.

config :super_worker, :example_chain_supervisor,
  options: [
    number_of_partitions: 4,
    link: false
  ],
  chains: [
    [
      id: :data_processing_chain,
      # Chain restart strategies: :one_for_one, :one_for_all, :rest_for_one, :before_for_one
      restart_strategy: :rest_for_one,
      # How to send data to workers: :broadcast, :random, :partition, :round_robin
      send_type: :round_robin,
      # Maximum queue length for buffering messages
      queue_length: 100,
      # Optional callback when data completes the chain
      # finished_callback: {MyApp.Callbacks, :chain_finished, []},
      workers: [
        [
          mfa: {MyApp.Pipeline.Validator, :start_link, []},
          options: [id: :validator]
        ],
        [
          mfa: {MyApp.Pipeline.Transformer, :start_link, []},
          options: [id: :transformer]
        ],
        [
          mfa: {MyApp.Pipeline.Persister, :start_link, []},
          options: [id: :persister]
        ]
      ]
    ]
  ]

# =============================================================================
# EXAMPLE 3: Supervisor with Standalone Workers
# =============================================================================
#
# Standalone workers are independent processes that don't belong to a group
# or chain. Each has its own restart strategy.

config :super_worker, :example_standalone_supervisor,
  options: [
    number_of_partitions: 1,
    link: false
  ],
  workers: [
    [
      mfa: {MyApp.Workers.MetricsCollector, :start_link, []},
      options: [
        id: :metrics_collector,
        # Standalone restart strategies: :permanent, :transient, :temporary
        restart_strategy: :permanent
      ]
    ],
    [
      # You can also use anonymous functions (0-arity)
      fun: fn ->
        receive do
          {:work, data} -> IO.puts("Processing: #{inspect(data)}")
        end
      end,
      options: [
        id: :simple_worker,
        restart_strategy: :transient
      ]
    ]
  ]

# =============================================================================
# EXAMPLE 4: Complex Supervisor with Multiple Children Types
# =============================================================================
#
# This example shows a real-world scenario with groups, chains, and
# standalone workers all in one supervisor.

config :super_worker, :example_complex_supervisor,
  options: [
    number_of_partitions: 8,
    link: false,
    report_to: []
  ],
  # Multiple groups can coexist
  groups: [
    # API request handlers
    [
      id: :api_handlers,
      restart_strategy: :one_for_one,
      workers: [
        [
          mfa: {MyApp.API.RequestHandler, :start_link, [:handler_1]},
          options: [id: :api_handler_1]
        ],
        [
          mfa: {MyApp.API.RequestHandler, :start_link, [:handler_2]},
          options: [id: :api_handler_2]
        ],
        [
          mfa: {MyApp.API.RequestHandler, :start_link, [:handler_3]},
          options: [id: :api_handler_3]
        ]
      ]
    ],
    # Background job processors
    [
      id: :job_processors,
      restart_strategy: :one_for_all,
      workers: [
        [
          mfa: {MyApp.Jobs.ImageProcessor, :start_link, []},
          options: [id: :image_processor]
        ],
        [
          mfa: {MyApp.Jobs.VideoProcessor, :start_link, []},
          options: [id: :video_processor]
        ]
      ]
    ]
  ],
  # Processing chains
  chains: [
    # Order processing pipeline
    [
      id: :order_processing,
      restart_strategy: :rest_for_one,
      send_type: :partition,
      queue_length: 200,
      workers: [
        [
          mfa: {MyApp.Orders.Validator, :start_link, []},
          options: [id: :order_validator]
        ],
        [
          mfa: {MyApp.Orders.PaymentProcessor, :start_link, []},
          options: [id: :payment_processor]
        ],
        [
          mfa: {MyApp.Orders.FulfillmentService, :start_link, []},
          options: [id: :fulfillment]
        ],
        [
          mfa: {MyApp.Orders.NotificationService, :start_link, []},
          options: [id: :notification]
        ]
      ]
    ],
    # Analytics pipeline
    [
      id: :analytics_pipeline,
      restart_strategy: :one_for_all,
      send_type: :broadcast,
      workers: [
        [
          mfa: {MyApp.Analytics.EventCollector, :start_link, []},
          options: [id: :event_collector]
        ],
        [
          mfa: {MyApp.Analytics.DataAggregator, :start_link, []},
          options: [id: :data_aggregator]
        ]
      ]
    ]
  ],
  # Independent workers
  workers: [
    [
      mfa: {MyApp.Monitors.HealthChecker, :start_link, []},
      options: [
        id: :health_checker,
        restart_strategy: :permanent
      ]
    ],
    [
      mfa: {MyApp.Cache.Manager, :start_link, []},
      options: [
        id: :cache_manager,
        restart_strategy: :permanent
      ]
    ]
  ]

# =============================================================================
# EXAMPLE 5: Development/Testing Supervisor
# =============================================================================
#
# A minimal supervisor useful for development and testing

config :super_worker, :example_dev_supervisor,
  options: [
    number_of_partitions: 1,
    link: false
  ],
  groups: [
    [
      id: :dev_workers,
      restart_strategy: :one_for_one,
      workers: [
        [
          # Simple echo worker for testing
          fun: fn ->
            receive do
              {:echo, from, msg} ->
                send(from, {:echoed, msg})
            end
          end,
          options: [id: :echo_worker]
        ]
      ]
    ]
  ]

# =============================================================================
# NOTES:
# =============================================================================
#
# 1. The ConfigLoader automatically loads all supervisor configurations
#    except the :options key when the application starts.
#
# 2. Each configuration key (e.g., :example_simple_supervisor) becomes the
#    supervisor ID automatically.
#
# 3. You can also load supervisors individually at runtime:
#    ```elixir
#    SuperWorker.ConfigLoader.ConfigParser.load_one(:example_simple_supervisor)
#    ```
#
# 4. To reload all supervisors from config:
#    ```elixir
#    SuperWorker.ConfigLoader.ConfigParser.load()
#    ```
#
# 5. Worker modules should implement a start_link function that returns
#    {:ok, pid} or {:error, reason}
#
# 6. For function-based workers, use 0-arity anonymous functions
#
# 7. Restart strategies explained:
#    - Groups:
#      * :one_for_one - restart only the failed worker
#      * :one_for_all - restart all workers in the group
#    - Chains:
#      * :one_for_one - restart only the failed worker
#      * :one_for_all - restart all workers in the chain
#      * :rest_for_one - restart failed worker and all after it
#      * :before_for_one - restart failed worker and all before it
#    - Standalone:
#      * :permanent - always restart
#      * :transient - restart only on abnormal exit
#      * :temporary - never restart
#
# 8. Chain send types:
#    - :broadcast - send to all workers at the level
#    - :random - send to a random worker
#    - :partition - use consistent hashing based on data
#    - :round_robin - distribute evenly across workers
#
# =============================================================================
