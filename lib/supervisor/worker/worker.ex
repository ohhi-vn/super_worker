defmodule SuperWorker.Supervisor.Worker do
  @moduledoc """
  Documentation for `SuperWorker.Supervisor.Worker`.
  """

  @default_restart_strategy :transient

  alias SuperWorker.Supervisor.{Constants, Utils, Validator}

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
    order: nil,
    # supervisor id, populated when the worker is started.
    supervisor: nil
  ]

  @type t :: %__MODULE__{
          id: any(),
          name: atom() | nil,
          restart_strategy: atom(),
          type: :standalone | :group | :chain,
          fun:
            nil
            | {:fun, fun()}
            | {module(), atom(), [any()]}
            | {:gen_server, {module(), atom(), [any()]}},
          num_workers: non_neg_integer(),
          parent: :standalone | {atom(), any()},
          order: non_neg_integer() | nil,
          supervisor: atom() | nil
        }

  @type from_config_result :: {:ok, t()} | {:error, term()}

  @spec from_config(keyword()) :: from_config_result()
  def from_config(options) do
    case Keyword.get(options, :type) do
      :standalone ->
        params = Constants.Types.standalone_worker_params()
        validate_with_params(options, params)

      :group ->
        params = Constants.Types.group_worker_params()
        validate_with_params(options, params)

      :chain ->
        params = Constants.Types.chain_worker_params()
        validate_with_params(options, params)

      invalid_type ->
        {:error, "invalid type: #{inspect(invalid_type)}"}
    end
  end

  defp validate_with_params(options, params) do
    with {:ok, options} <-
           Validator.normalize_options(options, params),
         {:ok, options} <- default_options(options),
         {:ok, options} <- validate_restart_strategy(options),
         {:ok, options} <- validate_options(options) do
      map_to_struct(options)
    end
  end

  @spec default_restart_strategy() :: atom()
  def default_restart_strategy do
    @default_restart_strategy
  end

  defp validate_restart_strategy(opts) do
    if opts.restart_strategy in Constants.Strategies.standalone_restart_strategies() do
      {:ok, opts}
    else
      {:error, "Invalid standalone restart strategy, #{inspect(opts.restart_strategy)}"}
    end
  end

  defp validate_options(options) do
    errors = Enum.reduce(options, [], &validate_option/2)

    if errors != [] do
      {:error, Enum.reverse(errors)}
    else
      {:ok, options}
    end
  end

  defp validate_option({:name, value}, errors) when is_atom(value), do: errors
  defp validate_option({:name, value}, errors), do: invalid_option(:name, value, errors)

  defp validate_option({:fun, {module, function, args}}, errors)
       when is_atom(module) and is_atom(function) and is_list(args), do: errors

  defp validate_option({:fun, {:fun, fun}}, errors) when is_function(fun), do: errors

  defp validate_option({:fun, {:gen_server, {module, function, args}}}, errors)
       when is_atom(module) and is_atom(function) and is_list(args), do: errors

  defp validate_option({:fun, value}, errors), do: invalid_option(:fun, value, errors)
  defp validate_option(_option, errors), do: errors

  defp invalid_option(key, value, errors), do: [{:error, {:invalid, {key, value}}} | errors]

  defp default_options(options) do
    options = Map.put_new(options, :id, Utils.random_id())
    options = Map.put_new(options, :restart_strategy, default_restart_strategy())

    {:ok, options}
  end

  defp map_to_struct(options) when is_map(options) do
    {:ok, struct(__MODULE__, options)}
  end
end
