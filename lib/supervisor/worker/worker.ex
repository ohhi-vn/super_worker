defmodule SuperWorker.Supervisor.Worker do
  @moduledoc """
  Documentation for `SuperWorker.Supervisor.Worker`.
  """

  alias SuperWorker.Supervisor.{Validator, Constants}

  @enforce_keys [:id, :fun]
  defstruct [
    # worker id, unique in supervisor.
    :id,
    # name of worker.
    :name,
    # restart strategy of worker. Affected by the supervisor & parent restart strategy.
    restart_strategy: :transient,
    # type of worker. :standalone, :group, :chain
    type: :standalone,
    # anonymous function {:fun, fun} or  {function, module, arguments} of worker.
    fun: nil,
    # supervisor id.
    supervisor: nil,
    # partition id.
    partition: nil,
    table: nil,
    # number of workers in chain.
    num_workers: 1,
    # parent(group/chain) id.
    parent: nil,
    # order in chain.
    order: nil
  ]

  @type t :: %__MODULE__{
          id: any,
          restart_strategy: atom,
          type: :standalone | :group | :chain,
          fun: nil | {:fun, fun} | {module, atom, [any]} | {:gen_server, {module, atom, [any]}},
          supervisor: atom,
          partition: atom,
          num_workers: non_neg_integer,
          parent: :standalone | {atom, any},
          order: non_neg_integer | nil
        }

  def check_group_options(opts) do
    with {:ok, opts} <- Validator.normalize_options(opts, Constants.Types.group_worker_params()),
         {:ok, opts} <- validate_opts(opts),
         {:ok, opts} <- default_opts(opts),
         {:ok, opts} <- map_to_struct(opts) do
      {:ok, opts}
    end
  end

  def check_chain_options(opts) do
    with {:ok, opts} <- Validator.normalize_options(opts, Constants.Types.chain_worker_params()),
         {:ok, opts} <- validate_opts(opts),
         {:ok, opts} <- default_opts(opts),
         {:ok, opts} <- map_to_struct(opts) do
      {:ok, opts}
    end
  end

  def check_standalone_options(opts) do
    with {:ok, opts} <-
           Validator.normalize_options(opts, Constants.Types.standalone_worker_params()),
         {:ok, opts} <- validate_restart_strategy(opts),
         {:ok, opts} <- validate_opts(opts),
         {:ok, opts} <- default_opts(opts),
         {:ok, opts} <- map_to_struct(opts) do
      {:ok, opts}
    end
  end

  defp validate_restart_strategy(opts) do
    if opts.restart_strategy in Constants.Strategies.standalone_restart_strategies() do
      {:ok, opts}
    else
      {:error, "Invalid group restart strategy, #{inspect(opts.restart_strategy)}"}
    end
  end

  defp validate_opts(opts) do
    # TO-DO: Implement the validation.
    {:ok, opts}
  end

  defp default_opts(opts) do
    opts =
      opts
      |> Map.put(:start_time, DateTime.utc_now())

    opts =
      if Map.has_key?(opts, :id) do
        opts
      else
        Map.put(opts, :id, SuperWorker.Supervisor.Utils.random_id())
      end

    {:ok, opts}
  end

  defp map_to_struct(opts) when is_map(opts) do
    {:ok, struct(__MODULE__, opts)}
  end
end
