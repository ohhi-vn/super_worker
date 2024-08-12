defmodule SuperWorker.Supervisor.Group do
  @moduledoc """
  Documentation for `SuperWorker.Supervisor.Group`.
  """

  @group_params [:id, :restart_strategy, :type, :max_restarts, :max_seconds, :auto_restart_time]

  @group_restart_strategies [:one_for_one, :one_for_all]

  @child_params [:id, :group_id, :type]

  @enforce_keys [:id]
  defstruct id: nil, restart_strategy: :one_for_all, workers: %{}

  import SuperWorker.Supervisor.Utils

  ## Public functions

  def check_options(opts) do
    with {:ok, opts} <- normalize_opts(opts, @group_params),
         {:ok, opts} <- validate_restart_strategy(opts),
         {:ok, opts} <- validate_opts(opts) do
      {:ok, opts}
    end
  end

  def check_worker_opts(opts) do

  end

  ## Private functions

  defp validate_restart_strategy(opts) do
    if opts.restart_strategy in @group_restart_strategies do
      {:ok, opts}
    else
      {:error, "Invalid group restart strategy, #{inspect opts.restart_strategy}"}
    end
  end

  defp validate_opts(opts) do
    # TO-DO: Implement the validation
    {:ok, opts}
  end
end
