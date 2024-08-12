defmodule SuperWorker.Supervisor.Standalone do
  @moduledoc """
  Documentation for `SuperWorker.Supervisor.Standalone`.
  """

  @standalone_params [:id, :restart_strategy]

  @standalone_restart_strategies [:permanent, :transient, :temporary]

  @enforce_keys [:id]
  defstruct id: nil, restart_strategy: :permanent

  import SuperWorker.Supervisor.Utils
  def check_options(opts) do
    with {:ok, opts} <- normalize_opts(opts, @standalone_params),
         {:ok, opts} <- validate_restart_strategy(opts),
         {:ok, opts} <- validate_opts(opts) do
      {:ok, opts}
    end
  end

  defp validate_restart_strategy(opts) do
    if opts.restart_strategy in @standalone_restart_strategies do
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
