defmodule SuperWorker.Supervisor.Chain do
  @moduledoc """
  Documentation for `SuperWorker.Supervisor.Chain`.
  """

  @chain_params [:id, :restart_strategy]

  @chain_restart_strategies [:one_for_one, :one_for_all, :rest_for_one, :before_for_one]

  @child_params [:id, :chain_id]

  defstruct id: nil, restart_strategy: :one_for_one, workers: %{}

  import SuperWorker.Supervisor.Utils

  def check_options(opts) do
    with {:ok, opts} <- normalize_opts(opts, @chain_params),
         {:ok, opts} <- validate_restart_strategy(opts),
         {:ok, opts} <- validate_opts(opts) do
      {:ok, opts}
    end
  end

  def check_worker_options(opts) do
    with {:ok, opts} <- normalize_opts(opts, @child_params),
    {:ok, opts} <- validate_opts(opts) do
     {:ok, opts}
    end
  end

  defp validate_restart_strategy(opts) do
    if opts.restart_strategy in @chain_restart_strategies do
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
