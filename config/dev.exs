import Config

config :super_worker,
    options: :hello,
    sup_fun_1: [ # Generate config by user functions.
      option: {SupConfig, :sup_options, []},
      chains: {SupConfig, :chains, []},
      workers: {SupConfig, :workers, []},
      groups: {SupConfig, :groups, []}
    ],
    sup_cfg_1: [  # Declare config by config file.
      options: [
        number_of_partitions: 2,
        link: true,
        report_to: {Dev, :report, []}
      ],
      groups: [
        group1: [
          options: [
            restart_strategy: :one_for_all
          ],
          workers: [
            worker1: [
              task: {Dev, :task, [15]}
            ],
            worker2: [
              options: [
                num_workers: 5,
                restart_strategy: :transient
              ],
              task: {Dev, :task_crash, [15, 5]}
            ]
          ]
        ],
      ],
      chains: [
        chain1: [
          options: [
            restart_strategy: :one_for_one,
            finished_callback: {Dev, :print, [:chain1]},
            send_type: :round_robin
          ],
          default_worker_options: [
            # restart_strategy: :transient,
            num_workers: 1
          ],
          workers: [
            worker1: [
              task: {Dev, :task, [15]}
            ],
            worker2: [
              options: [
                num_workers: 5,
                # restart_strategy: :transient
              ],
              task: {Dev, :task_crash, [15, 5]}
            ]
          ]
        ]
      ]
    ]
