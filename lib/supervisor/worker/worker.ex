defmodule SuperWorker.Supervisor.Worker do
  @moduledoc """
  Documentation for `SuperWorker.Supervisor.Worker`.
  """

  @default_restart_strategy :transient
  @default_max_restarts 10
  @default_max_seconds 10

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
    supervisor: nil,
    # restart intensity ceiling within :max_seconds for standalone workers.
    max_restarts: @default_max_restarts,
    # window (seconds) in which :max_restarts is counted.
    max_seconds: @default_max_seconds,
    # cooldown (ms) before restart resumes after the ceiling is exceeded.
    auto_restart_time: nil
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
          supervisor: atom() | nil,
          max_restarts: pos_integer(),
          max_seconds: pos_integer(),
          auto_restart_time: pos_integer() | nil
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
         {:ok, options} <- validate_intensity(options),
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
      {:error,
       {:invalid_options,
        "Invalid standalone restart strategy, #{inspect(opts.restart_strategy)}"}}
    end
  end

  # Restart intensity options must be positive integers; `:auto_restart_time`
  # may also be nil (no automatic restart resumption).
  defp validate_intensity(options) do
    with :ok <- check_positive(options, :max_restarts),
         :ok <- check_positive(options, :max_seconds),
         :ok <- check_auto_restart_time(options) do
      {:ok, options}
    else
      {:error, detail} -> {:error, {:invalid_options, detail}}
    end
  end

  defp check_positive(options, key) do
    case Map.get(options, key) do
      value when is_integer(value) and value > 0 -> :ok
      value -> {:error, {key, value}}
    end
  end

  defp check_auto_restart_time(options) do
    case Map.get(options, :auto_restart_time) do
      nil -> :ok
      value when is_integer(value) and value > 0 -> :ok
      value -> {:error, {:auto_restart_time, value}}
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
    options = Map.put_new(options, :max_restarts, @default_max_restarts)
    options = Map.put_new(options, :max_seconds, @default_max_seconds)
    options = Map.put_new(options, :auto_restart_time, nil)

    {:ok, options}
  end

  defp map_to_struct(options) when is_map(options) do
    {:ok, struct(__MODULE__, options)}
  end
end
