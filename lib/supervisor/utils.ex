defmodule SuperWorker.Supervisor.Utils do
  @moduledoc false

  require Logger

  # Define the parameters for worker options
  @child_params [:id, :group_id, :chain_id, :restart_strategy, :type]

  @group_params [:id, :restart_strategy, :type]

  @group_restart_strategies [:one_for_one, :one_for_all]

  @chain_restart_strategies [:one_for_one, :one_for_all, :rest_for_one, :before_for_one]

  @child_restart_strategies [:permanent, :transient, :temporary]

  def validate_child_opts(opts) do
    # Validate the options
    # TO-DO: Implement the validation
    result = Enum.reduce(opts, %{}, fn
      {key, value}, acc -> Map.put(acc, key, value)
      value, acc ->
        {k, v} = get_keyword(value)
        Map.put(acc, k, v)
    end)

    Enum.all?(result, fn {key, _} -> key in @child_params end)

    {:ok, result}
  end

  # convert specified keyword to a tuple.
  def get_keyword(:group) do
    {:type, :group}
  end
  def get_keyword(:chain) do
    {:type, :chain}
  end
  def get_keyword(:standalone) do
    {:type, :standalone}
  end

  def validate_group_opts(opts) do

    if Enum.all?(opts, fn
      {key, _} -> key in @group_params
      key -> key in @group_params
    end) do
      # Validate the group options
      result = Enum.reduce(opts, %{}, fn
        {key, value}, acc -> Map.put(acc, key, value)
        value, acc ->
          {k, v} = get_keyword(value)
          Map.put(acc, k, v)
      end)

      {:ok, result}
    else
      {:error, "Invalid group options"}
    end
  end

  def validate_strategy(opts, :group) do
    strategy = opts[:restart_strategy]
    if strategy in @group_restart_strategies do
      {:ok, opts}
    else
      {:error, "Invalid group restart strategy, #{inspect strategy}"}
    end
  end
  def validate_strategy(opts, :chain) do
    strategy = opts[:restart_strategy]
    if strategy in @chain_restart_strategies do
      {:ok, opts}
    else
      {:error, "Invalid chain restart strategy, #{inspect strategy}"}
    end
  end
  def validate_strategy(opts, :child) do
    strategy = opts[:restart_strategy]
    if strategy in @child_restart_strategies do
      {:ok, opts}
    else
      if Map.has_key?(opts, :group_id) do
        {:ok, opts}
      else
        {:error, "Invalid child restart strategy, #{inspect strategy}"}
      end
    end
  end

  # Ignore unsupported options.
  def ignore_opt(%{type: type} = opts, :restart_strategy) when type in [:group, :chain] do
    if Map.has_key?(opts, :restart_strategy) do
      Logger.warning("Ignoring restart_strategy option for #{type}")
      {:ok, Map.delete(opts, :restart_strategy)}
    else
      {:ok, opts}
    end
  end
  def ignore_opt(%{type: :standalone} = opts, opt) when opt in [:group_id, :chain_id] do
    if Map.has_key?(opts, opt) do
      Logger.warning("Ignoring #{inspect opt} option for standalone worker")
      {:ok, Map.delete(opts, opt)}
    else
      {:ok, opts}
    end
  end
  def ignore_opt(opts, _opt) do
    {:ok, opts}
  end
end
