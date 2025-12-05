defmodule SuperWorker.Supervisor.Worker do
  @moduledoc """
  Documentation for `SuperWorker.Supervisor.Worker`.
  """

  @default_restart_strategy :transient

  alias SuperWorker.Supervisor.{Validator, Constants}

  @enforce_keys [:id, :fun]
  defstruct [
    # worker id, unique in supervisor.
    :id,
    # name of worker, is atom for register process to communicate directly without lookup pid.
    :name,
    # restart strategy of worker. Affected by the supervisor & parent restart strategy.
    restart_strategy: @default_restart_strategy,
    # type of worker. :standalone, :group, :chain
    type: :standalone,
    # anonymous function {:fun, fun} or  {function, module, arguments} of worker.
    fun: nil,
    # number of workers in chain.
    num_workers: 1,
    # parent(group/chain) id.
    parent: nil,
    # order in chain.
    order: nil
  ]

  @type t :: %__MODULE__{
          id: any,
          name: atom,
          restart_strategy: atom,
          type: :standalone | :group | :chain,
          fun: nil | {:fun, fun} | {module, atom, [any]} | {:gen_server, {module, atom, [any]}},
          num_workers: non_neg_integer,
          parent: :standalone | {atom, any},
          order: non_neg_integer | nil
        }

  def from_config(options) do
    params =
      case Keyword.get(options, :type) do
        :standalone ->
          Constants.Types.standalone_worker_params()

        :group ->
          Constants.Types.group_worker_params()

        :chain ->
          Constants.Types.chain_worker_params()
      end

    with {:ok, options} <-
           Validator.normalize_options(options, params),
         {:ok, options} <- default_options(options),
         {:ok, options} <- validate_restart_strategy(options),
         {:ok, options} <- validate_options(options),
         {:ok, options} <- map_to_struct(options) do
      {:ok, options}
    end
  end

  def default_restart_strategy() do
    @default_restart_strategy
  end

  defp validate_restart_strategy(opts) do
    if opts.restart_strategy in Constants.Strategies.standalone_restart_strategies() do
      {:ok, opts}
    else
      {:error, "Invalid group restart strategy, #{inspect(opts.restart_strategy)}"}
    end
  end

  defp validate_options(opts) do
    # TO-DO: Implement the validation.
    {:ok, opts}
  end

  defp default_options(options) do
    options =
      options
      |> Map.put(:start_time, DateTime.utc_now())

    options =
      if Map.has_key?(options, :id) do
        options
      else
        Map.put(options, :id, SuperWorker.Supervisor.Utils.random_id())
      end

    options =
      if Map.has_key?(options, :restart_strategy) do
        options
      else
        Map.put(options, :restart_strategy, default_restart_strategy())
      end

    {:ok, options}
  end

  defp map_to_struct(options) when is_map(options) do
    {:ok, struct(__MODULE__, options)}
  end
end
