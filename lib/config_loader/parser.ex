defmodule SuperWorker.ConfigLoader.Parser do
  @moduledoc """
  Parses and validates supervisor configurations from the application environment.

  This module takes raw configuration keyword lists and transforms them into
  validated structures that can be used by the Bootstrap module to start supervisors.
  """

  require Logger
  require SuperWorker.Log

  @type config :: keyword()
  @type parsed_config :: %{
          options: keyword(),
          children: list(child_spec())
        }
  @type child_spec :: %{
          type: :group | :chain | :standalone,
          options: keyword(),
          workers: list(worker_spec())
        }
  @type worker_spec :: %{
          mfa: {module(), atom(), list()} | {:fun, function()},
          options: keyword()
        }

  @supervisor_option_keys [:num_partitions, :link, :report_to, :strategy]
  @group_option_keys [
    :id,
    :restart_strategy,
    :type,
    :max_restarts,
    :max_seconds,
    :auto_restart_time
  ]
  @chain_option_keys [:id, :restart_strategy, :finished_callback, :queue_length, :send_type]
  #  @worker_option_keys [:id, :restart_strategy, :max_restarts, :max_seconds, :auto_restart_time]

  @valid_group_strategies [:one_for_one, :one_for_all]
  @valid_chain_strategies [:one_for_one, :one_for_all, :rest_for_one]
  @valid_chain_send_types [:broadcast, :random, :partition, :round_robin]

  @doc """
  Parses and validates a supervisor configuration.

  ## Parameters
    * `config` - A keyword list containing supervisor configuration

  ## Returns
    * `{:ok, parsed_config}` - Successfully parsed configuration
    * `{:error, reason}` - Validation or parsing error

  ## Expected Configuration Format

  ```elixir
  [
    options: [
      num_partitions: 2,
      link: false,
      strategy: :one_for_one
    ],
    groups: [
      [
        id: :my_group,
        restart_strategy: :one_for_one,
        workers: [
          [mfa: {MyModule, :my_function, [:arg1]}, options: [id: :worker1]],
          [fun: fn -> :ok end, options: [id: :worker2]]
        ]
      ]
    ],
    chains: [
      [
        id: :my_chain,
        restart_strategy: :one_for_one,
        send_type: :round_robin,
        workers: [
          [mfa: {MyModule, :step1, []}, options: [id: :step1]],
          [mfa: {MyModule, :step2, []}, options: [id: :step2]]
        ]
      ]
    ],
    workers: [
      [mfa: {MyModule, :standalone_worker, []}, options: [id: :standalone1]]
    ]
  ]
  ```
  """

  alias SuperWorker.Supervisor.Worker

  @spec parse(config()) :: {:ok, parsed_config()} | {:error, any()}
  def parse(config) when is_list(config) do
    SuperWorker.Log.debug(fn -> "SuperWorker, Parser, parsing config: #{inspect(config)}" end)

    with {:ok, options} <- parse_supervisor_options(config),
         {:ok, children} <- parse_children(config) do
      parsed = %{
        options: options,
        children: children
      }

      SuperWorker.Log.debug(fn ->
        "SuperWorker, Parser, successfully parsed config: #{inspect(parsed)}"
      end)

      {:ok, parsed}
    else
      {:error, reason} = error ->
        Logger.error("SuperWorker, Parser, failed to parse config: #{inspect(reason)}")
        error
    end
  end

  def parse(_config) do
    {:error, :invalid_config_format}
  end

  # Parses supervisor-level options
  defp parse_supervisor_options(config) do
    options = Keyword.get(config, :options, [])

    if is_list(options) do
      validated_options =
        options
        |> Enum.filter(fn {key, _value} -> key in @supervisor_option_keys end)
        |> validate_supervisor_options()

      {:ok, validated_options}
    else
      {:error, {:invalid_options, "Options must be a keyword list"}}
    end
  end

  defp validate_supervisor_options(options) do
    options
    |> Enum.map(fn
      {:num_partitions, value} when is_integer(value) and value > 0 ->
        {:num_partitions, value}

      {:num_partitions, value} ->
        Logger.warning(
          "SuperWorker, Parser, invalid num_partitions: #{inspect(value)}, using default"
        )

        nil

      {:link, value} when is_boolean(value) ->
        {:link, value}

      {:link, value} when is_pid(value) ->
        {:link, value}

      {:link, value} ->
        Logger.warning("SuperWorker, Parser, invalid link value: #{inspect(value)}, using true")
        {:link, true}

      {:report_to, value} when is_list(value) ->
        {:report_to, value}

      {:strategy, value} when value in [:one_for_one, :one_for_all, :rest_for_one] ->
        {:strategy, value}

      other ->
        other
    end)
    |> Enum.reject(&is_nil/1)
  end

  # Parses all children (groups, chains, standalone workers)
  defp parse_children(config) do
    with {:ok, groups} <- parse_groups(config),
         {:ok, chains} <- parse_chains(config),
         {:ok, workers} <- parse_standalone_workers(config) do
      all_children = groups ++ chains ++ workers
      {:ok, all_children}
    end
  end

  # Parses group configurations
  defp parse_groups(config) do
    parse_collection(config, :groups, :group, &parse_group/2)
  end

  defp parse_group({id, group_config}, index) when is_list(group_config) do
    SuperWorker.Log.debug(fn ->
      "SuperWorker, Parser, parsing group with id: #{inspect(id)}, config: #{inspect(group_config)}"
    end)

    with {:ok, options} <- extract_group_options(group_config),
         {:ok, workers} <- extract_workers(group_config) do
      {:ok,
       %{
         type: :group,
         id: id,
         options: options,
         workers: workers
       }}
    else
      other ->
        Logger.error(
          "SuperWorker, Parser, invalid group config at index #{index}, config: #{inspect(group_config)}, error: #{inspect(other)}"
        )

        {:error, {:invalid_group_config, other}}
    end
  end

  defp parse_group(group_config, index) do
    Logger.error(
      "SuperWorker, Parser, invalid group config at index #{index}, config: #{inspect(group_config)}"
    )

    {:error, {:invalid_group_config, "Group at index #{index} must be a keyword list"}}
  end

  defp extract_group_options(config) do
    config
    |> Enum.filter(fn {key, _value} -> key in @group_option_keys end)
    |> validate_group_options()
  end

  defp validate_group_options(options) do
    result =
      Enum.map(options, fn
        {:restart_strategy, value} when value in @valid_group_strategies ->
          {:restart_strategy, value}

        {:restart_strategy, value} ->
          Logger.error("SuperWorker, Parser, invalid group restart_strategy: #{inspect(value)}")
          {:error, :invalid_restart_strategy}

        other ->
          other
      end)

    if Enum.any?(result, fn
         {:error, _} -> true
         _ -> false
       end) do
      {:error, "invalid options for group"}
    else
      {:ok, result}
    end
  end

  # Parses chain configurations
  defp parse_chains(config) do
    parse_collection(config, :chains, :chain, &parse_chain/2)
  end

  defp parse_collection(config, key, type, parser) do
    entries = Keyword.get(config, key, [])

    if is_list(entries) do
      {success, errors} =
        entries
        |> Enum.with_index()
        |> Enum.map(fn {entry, index} -> parser.(entry, index) end)
        |> Enum.reduce({[], []}, fn
          {:ok, data}, {success, errors} -> {[data | success], errors}
          {:error, _} = error, {success, errors} -> {success, [error | errors]}
        end)

      case {Enum.reverse(success), Enum.reverse(errors)} do
        {success, []} -> {:ok, success}
        {_success, errors} -> {:error, {parsing_error(type), errors}}
      end
    else
      {:error, {invalid_collection(key), "#{key} must be a list"}}
    end
  end

  defp parsing_error(:group), do: :group_parsing_errors
  defp parsing_error(:chain), do: :chain_parsing_errors
  defp parsing_error(:worker), do: :worker_parsing_errors

  defp invalid_collection(:groups), do: :invalid_groups
  defp invalid_collection(:chains), do: :invalid_chains
  defp invalid_collection(:workers), do: :invalid_workers

  defp parse_chain({id, chain_config}, _index) when is_list(chain_config) do
    with {:ok, options} <- extract_chain_options(chain_config),
         {:ok, workers} <- extract_workers(chain_config) do
      {:ok,
       %{
         type: :chain,
         id: id,
         options: options,
         workers: workers
       }}
    end
  end

  defp parse_chain(chain_config, index) do
    Logger.error(
      "SuperWorker, Parser, invalid chain config at index #{index}, #{inspect(chain_config)}"
    )

    {:error, {:invalid_chain_config, "Chain at index #{index} must be a keyword list"}}
  end

  defp extract_chain_options(config) do
    options =
      config
      |> Enum.filter(fn {key, _value} -> key in @chain_option_keys end)
      |> validate_chain_options()

    {:ok, options}
  end

  defp validate_chain_options(options) do
    Enum.map(options, fn
      {:restart_strategy, value} when value in @valid_chain_strategies ->
        {:restart_strategy, value}

      {:restart_strategy, value} ->
        Logger.warning(
          "SuperWorker, Parser, invalid chain restart_strategy: #{inspect(value)}, using :one_for_one"
        )

        {:restart_strategy, :one_for_one}

      {:send_type, value} when value in @valid_chain_send_types ->
        {:send_type, value}

      {:send_type, value} ->
        Logger.warning(
          "SuperWorker, Parser, invalid chain send_type: #{inspect(value)}, using :round_robin"
        )

        {:send_type, :round_robin}

      other ->
        other
    end)
  end

  # Parses standalone worker configurations
  defp parse_standalone_workers(config) do
    parse_collection(config, :workers, :worker, &parse_standalone_worker/2)
  end

  defp parse_standalone_worker(worker_config, index) when is_list(worker_config) do
    with {:ok, mfa} <- extract_mfa(worker_config, index),
         {:ok, options} <- extract_worker_options(worker_config) do
      {:ok,
       %{
         type: :standalone,
         mfa: mfa,
         options: options
       }}
    end
  end

  defp parse_standalone_worker({module, _} = worker_config, index) when is_atom(module) do
    with {:ok, specs} <- parse_worker_spec(worker_config, index) do
      {:ok, Map.put_new(specs, :type, :standalone)}
    end
  end

  defp parse_standalone_worker(worker_config, index) when is_atom(worker_config) do
    with {:ok, specs} <- parse_worker_spec(worker_config, index) do
      {:ok, Map.put_new(specs, :type, :standalone)}
    end
  end

  defp parse_standalone_worker(_worker_config, index) do
    {:error, {:invalid_worker_config, "Worker at index #{index} must be a keyword list"}}
  end

  # Extracts workers from group or chain config
  defp extract_workers(config) do
    workers = Keyword.get(config, :workers, [])

    SuperWorker.Log.debug(fn ->
      "SuperWorker, Parser, parses workers config, workers: #{inspect(workers)}"
    end)

    if is_list(workers) do
      parsed_workers =
        workers
        |> Enum.with_index()
        |> Enum.map(fn {worker, index} ->
          SuperWorker.Log.debug(fn ->
            "SuperWorker, Parser, parses worker config at index #{index}, config: #{inspect(worker)}"
          end)

          case worker do
            {gen_server_worker, options}
            when is_atom(gen_server_worker) and is_list(options) ->
              parse_worker_spec(worker, index)

            {worker_id, worker_config} when is_list(worker_config) ->
              worker_config_with_id = Keyword.put(worker_config, :id, worker_id)
              parse_worker_spec(worker_config_with_id, index)

            worker_config ->
              parse_worker_spec(worker_config, index)
          end
        end)

      {successful, errors} =
        Enum.reduce(parsed_workers, {[], []}, fn
          {:ok, worker}, {ok, errs} -> {[worker | ok], errs}
          {:error, _} = err, {ok, errs} -> {ok, [err | errs]}
        end)

      successful = Enum.reverse(successful)
      errors = Enum.reverse(errors)

      if Enum.empty?(errors) do
        {:ok, successful}
      else
        {:error, {:workers_parsing_errors, errors}}
      end
    else
      {:ok, []}
    end
  end

  defp parse_worker_spec(worker_config, index) when is_list(worker_config) do
    with {:ok, mfa} <- extract_mfa(worker_config, index),
         {:ok, options} <- extract_worker_options(worker_config) do
      {:ok,
       %{
         mfa: mfa,
         options: options
       }}
    end
  end

  defp parse_worker_spec(worker, _index) when is_atom(worker) do
    convert_regular_child_spec(worker)
  end

  defp parse_worker_spec(worker = {module, _opts}, _index) when is_atom(module) do
    convert_regular_child_spec(worker)
  end

  defp parse_worker_spec(_worker_config, index) do
    {:error, {:invalid_worker_spec, "Worker spec at index #{index} must be a keyword list"}}
  end

  # Extracts MFA or function from worker config
  defp extract_mfa(config, index) do
    case Keyword.fetch(config, :mfa) do
      {:ok, value} -> validate_mfa(value, index)
      :error -> extract_fun_or_task(config, index)
    end
  end

  defp extract_fun_or_task(config, index) do
    case Keyword.fetch(config, :fun) do
      {:ok, value} -> validate_fun(value, index)
      :error -> extract_task(config, index)
    end
  end

  defp extract_task(config, index) do
    case Keyword.fetch(config, :task) do
      {:ok, value} ->
        validate_task(value, index)

      :error ->
        Logger.error(
          "Worker at index #{index} must have :mfa or :fun, config: #{inspect(config)}"
        )

        {:error, {:missing_worker_function, "Worker at index #{index} must have :mfa or :fun"}}
    end
  end

  defp validate_mfa({module, function, args}, _index)
       when is_atom(module) and is_atom(function) and is_list(args),
       do: {:ok, {module, function, args}}

  defp validate_mfa(value, index) do
    {:error,
     {:invalid_mfa,
      "Worker at index #{index} has invalid MFA: #{inspect(value)}. Expected {Module, :function, [args]}"}}
  end

  defp validate_fun(fun, _index) when is_function(fun, 0), do: {:ok, {:fun, fun}}

  defp validate_fun(value, index) do
    {:error,
     {:invalid_fun,
      "Worker at index #{index} has invalid function: #{inspect(value)}. Expected 0-arity function"}}
  end

  defp validate_task({module, function, args}, _index)
       when is_atom(module) and is_atom(function) and is_list(args),
       do: {:ok, {module, function, args}}

  defp validate_task(value, index) do
    {:error,
     {:invalid_task,
      "Worker at index #{index} has invalid task: #{inspect(value)}. Expected {Module, :function, [args]}"}}
  end

  # Extracts worker options
  defp extract_worker_options(config) do
    options =
      config
      |> Enum.filter(fn {key, _value} -> key in [:options] end)
      |> case do
        [{:options, opts}] when is_list(opts) -> opts
        [] -> []
        _ -> []
      end

    {:ok, options}
  end

  @doc """
  Converts a regular child spec (module or {module, opts}) to the internal worker spec format.

  This function is public because it's used by `SuperWorker.Supervisor` to convert
  GenServer module references into worker specifications.

  ## Parameters

    * `{module, keywords}` - A module with keyword options
    * `module` - A module atom (uses empty options)

  ## Returns

    * `{:ok, spec}` - Successfully converted spec
    * `{:error, reason}` - If the module doesn't implement `child_spec/1`
  """
  def convert_regular_child_spec({module, keywords}) do
    result =
      module.child_spec(keywords)
      |> add_default_options()
      |> regular_child_spec_to_spec()

    {:ok, result}
  rescue
    UndefinedFunctionError ->
      {:error, {:invalid_child_spec, "Module #{inspect(module)} does not implement child_spec/1"}}
  end

  @doc false
  def convert_regular_child_spec(module) do
    convert_regular_child_spec({module, []})
  end

  defp add_default_options(specs = %{}) do
    if Map.has_key?(specs, :restart) do
      specs
    else
      Map.put(specs, :restart, Worker.default_restart_strategy())
    end
  end

  defp regular_child_spec_to_spec(specs = %{}) do
    %{
      mfa: {:gen_server, specs.start},
      options: [],
      id: specs.id,
      restart_strategy: specs.restart
    }
  end
end
