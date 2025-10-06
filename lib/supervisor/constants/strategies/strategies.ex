defmodule SuperWorker.Supervisor.Constants.Strategies do
  @moduledoc """
  This module defines the valid restart strategies for different types of workers.
  """

  @group_restart_strategies [:one_for_one, :one_for_all]
  @chain_restart_strategies [:one_for_one, :one_for_all, :rest_for_one, :before_for_one]
  @standalone_restart_strategies [:permanent, :transient, :temporary]

  @doc "Returns the list of valid restart strategies for groups."
  @spec group_restart_strategies() :: list(atom())
  def group_restart_strategies, do: @group_restart_strategies

  @doc "Returns the list of valid restart strategies for chains."
  @spec chain_restart_strategies() :: list(atom())
  def chain_restart_strategies, do: @chain_restart_strategies

  @doc "Returns the list of valid restart strategies for standalone workers."
  @spec standalone_restart_strategies() :: list(atom())
  def standalone_restart_strategies, do: @standalone_restart_strategies
end
