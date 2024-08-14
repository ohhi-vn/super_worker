defmodule SuperWorker.Supervisor.Worker do
  @moduledoc """
  Documentation for `SuperWorker.Supervisor.Worker`.
  """

  @worker_params [:id, :restart_strategy, :type, :max_restarts, :max_seconds, :auto_restart_time]

  @restart_strategies [:permanent, :transient, :temporary]

  @enforce_keys [:id]
  defstruct [
    :id, # worker id, unique in supervior.
    :pid, # current pid of worker.
    :ref, # reference created when spawning the worker.
    :restart_count, # restart counter.
    :start_time, # start time of worker. if worker is restarted, this value is updated.
    restart_strategy: :transient, # restart strategy of worker. Affected by the supervisor restart strategy.
    type: :standalone # type of worker. :standalone, :group, :chain
  ]

  import SuperWorker.Supervisor.Utils

  def check_options(opts) do
    with {:ok, opts} <- normalize_opts(opts, @worker_params),
         {:ok, opts} <- validate_restart_strategy(opts),
         {:ok, opts} <- validate_opts(opts) do
      {:ok, opts}
    end
  end

  defp validate_restart_strategy(opts) do
    if opts.restart_strategy in @restart_strategies do
      {:ok, opts}
    else
      {:error, "Invalid group restart strategy, #{inspect opts.restart_strategy}"}
    end
  end
  defp validate_opts(opts) do
    # TO-DO: Implement the validation.
    {:ok, opts}
  end
end
