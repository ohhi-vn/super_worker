defmodule SuperWorker.Supervisor.Constants.Types do
  @moduledoc """
  This module defines various types and constant lists used throughout the SuperWorker system.
  """

  # Worker, Chain, and Shutdown Types
  @worker_types [:standalone, :group, :chain]
  @chain_send_types [:broadcast, :random, :partition, :round_robin]
  @shutdown_types [:normal, :kill, :brutal_kill]

  # API Messages and Exit Reasons
  @api_messages [
    :start_worker,
    :get_group,
    :get_chain,
    :add_group,
    :add_chain,
    :remove_group_worker,
    :send_to_group,
    :send_to_group_random,
    :send_to_worker,
    :broadcast_to_group,
    :add_data_to_chain,
    :stop
  ]
  @exit_reasons [:normal, :killed, :restart, :removed]

  # Parameter Keys for configuration validation
  @supervisor_params [:id, :num_partitions, :link, :report_to, :children]
  @worker_params [:id, :type, :name, :fun, :parent, :restart_strategy]
  @standalone_params [:restart_strategy, :max_restarts, :max_seconds, :auto_restart_time]
  @group_params [:id, :restart_strategy, :type, :max_restarts, :max_seconds, :auto_restart_time]
  @chain_params [:id, :restart_strategy, :finished_callback, :queue_length, :send_type]

  @doc "Returns the list of valid worker types."
  @spec worker_types() :: list(atom())
  def worker_types, do: @worker_types

  @doc "Returns the list of valid send types for chains."
  @spec chain_send_types() :: list(atom())
  def chain_send_types, do: @chain_send_types

  @doc "Returns the list of valid shutdown types."
  @spec shutdown_types() :: list(atom())
  def shutdown_types, do: @shutdown_types

  @doc "Returns the list of valid API message types."
  @spec api_messages() :: list(atom())
  def api_messages, do: @api_messages

  @doc "Returns the list of valid process exit reasons."
  @spec exit_reasons() :: list(atom())
  def exit_reasons, do: @exit_reasons

  @doc "Returns the list of valid supervisor parameters."
  @spec supervisor_params() :: list(atom())
  def supervisor_params, do: @supervisor_params

  @doc "Returns the list of valid worker parameters."
  @spec worker_params() :: list(atom())
  def worker_params, do: @worker_params

  @doc "Returns the list of valid standalone worker parameters."
  @spec standalone_params() :: list(atom())
  def standalone_params, do: @standalone_params

  @doc "Returns the list of valid group parameters."
  @spec group_params() :: list(atom())
  def group_params, do: @group_params

  @doc "Returns the list of valid chain parameters."
  @spec chain_params() :: list(atom())
  def chain_params, do: @chain_params

  def chain_worker_params, do: @worker_params
  def group_worker_params, do: @worker_params
  def standalone_worker_params, do: @standalone_params ++ @worker_params
end
