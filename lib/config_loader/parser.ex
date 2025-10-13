defmodule SuperWorker.ConfigLoader.Parser do
  @moduledoc """
  Parses and validates supervisor configurations from the application environment.

  This module takes raw configuration keyword lists and transforms them into
  validated structures that can be used by the Bootstrap module to start supervisors.
  """

  require Logger

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

  @supervisor_option_keys [:number_of_partitions, :link, :report_to, :strategy]
  @group_option_keys [
    :id,
    :restart_strategy,
    :type,
    :max_restarts,
    :max_seconds,
    :auto_restart_time
  ]
  @chain_option_keys [:id, :restart_strategy, :finished_callback, :queue_length, :send_type]
  @worker_option_keys [:id, :restart_strategy, :max_restarts, :max_seconds, :auto_restart_time]

  @valid_group_strategies [:one_for_one, :one_for_all]
  @valid_chain_strategies [:one_for_one, :one_for_all, :rest_for_one, :before_for_one]
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
      number_of_partitions: 2,
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
  @spec parse(config()) :: {:ok, parsed_config()} | {:error, any()}
  def parse(config) when is_list(config) do
    Logger.debug("SuperWorker, Parser, parsing config: #{inspect(config)}")

    with {:ok, options} <- parse_supervisor_options(config),
         {:ok, children} <- parse_children(config) do
      parsed = %{
        options: options,
        children: children
      }

      Logger.debug("SuperWorker, Parser, successfully parsed config: #{inspect(parsed)}")
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
      {:number_of_partitions, value} when is_integer(value) and value > 0 ->
        {:number_of_partitions, value}

      {:number_of_partitions, value} ->
        Logger.warning(
          "SuperWorker, Parser, invalid number_of_partitions: #{inspect(value)}, using default"
        )

        nil

      {:link, value} when is_boolean(value) ->
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
    groups = Keyword.get(config, :groups, [])

    if is_list(groups) do
      parsed_groups =
        groups
        |> Enum.with_index()
        |> Enum.map(fn {group_config, index} ->
          parse_group(group_config, index)
        end)

      errors = Enum.filter(parsed_groups, fn result -> match?({:error, _}, result) end)

      if Enum.empty?(errors) do
        {:ok, Enum.map(parsed_groups, fn {:ok, group} -> group end)}
      else
        {:error, {:group_parsing_errors, errors}}
      end
    else
      {:error, {:invalid_groups, "Groups must be a list"}}
    end
  end

  defp parse_group(group_config, index) when is_list(group_config) do
    with {:ok, id} <- extract_id(group_config, :group, index),
         {:ok, options} <- extract_group_options(group_config),
         {:ok, workers} <- extract_workers(group_config) do
      {:ok,
       %{
         type: :group,
         id: id,
         options: options,
         workers: workers
       }}
    end
  end

  defp parse_group(_group_config, index) do
    {:error, {:invalid_group_config, "Group at index #{index} must be a keyword list"}}
  end

  defp extract_group_options(config) do
    options =
      config
      |> Enum.filter(fn {key, _value} -> key in @group_option_keys end)
      |> validate_group_options()

    {:ok, options}
  end

  defp validate_group_options(options) do
    Enum.map(options, fn
      {:restart_strategy, value} when value in @valid_group_strategies ->
        {:restart_strategy, value}

      {:restart_strategy, value} ->
        Logger.warning(
          "SuperWorker, Parser, invalid group restart_strategy: #{inspect(value)}, using :one_for_one"
        )

        {:restart_strategy, :one_for_one}

      other ->
        other
    end)
  end

  # Parses chain configurations
  defp parse_chains(config) do
    chains = Keyword.get(config, :chains, [])

    if is_list(chains) do
      parsed_chains =
        chains
        |> Enum.with_index()
        |> Enum.map(fn {chain_config, index} ->
          parse_chain(chain_config, index)
        end)

      errors = Enum.filter(parsed_chains, fn result -> match?({:error, _}, result) end)

      if Enum.empty?(errors) do
        {:ok, Enum.map(parsed_chains, fn {:ok, chain} -> chain end)}
      else
        {:error, {:chain_parsing_errors, errors}}
      end
    else
      {:error, {:invalid_chains, "Chains must be a list"}}
    end
  end

  defp parse_chain(chain_config, index) when is_list(chain_config) do
    with {:ok, id} <- extract_id(chain_config, :chain, index),
         {:ok, options} <- extract_chain_options(chain_config),
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

  defp parse_chain(_chain_config, index) do
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
    workers = Keyword.get(config, :workers, [])

    if is_list(workers) do
      parsed_workers =
        workers
        |> Enum.with_index()
        |> Enum.map(fn {worker_config, index} ->
          parse_standalone_worker(worker_config, index)
        end)

      errors = Enum.filter(parsed_workers, fn result -> match?({:error, _}, result) end)

      if Enum.empty?(errors) do
        {:ok, Enum.map(parsed_workers, fn {:ok, worker} -> worker end)}
      else
        {:error, {:worker_parsing_errors, errors}}
      end
    else
      {:error, {:invalid_workers, "Workers must be a list"}}
    end
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

  defp parse_standalone_worker(_worker_config, index) do
    {:error, {:invalid_worker_config, "Worker at index #{index} must be a keyword list"}}
  end

  # Extracts workers from group or chain config
  defp extract_workers(config) do
    workers = Keyword.get(config, :workers, [])

    if is_list(workers) do
      parsed_workers =
        workers
        |> Enum.with_index()
        |> Enum.map(fn {worker_config, index} ->
          parse_worker_spec(worker_config, index)
        end)

      errors = Enum.filter(parsed_workers, fn result -> match?({:error, _}, result) end)

      if Enum.empty?(errors) do
        {:ok, Enum.map(parsed_workers, fn {:ok, worker} -> worker end)}
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

  defp parse_worker_spec(_worker_config, index) do
    {:error, {:invalid_worker_spec, "Worker spec at index #{index} must be a keyword list"}}
  end

  # Extracts MFA or function from worker config
  defp extract_mfa(config, index) do
    cond do
      Keyword.has_key?(config, :mfa) ->
        case Keyword.get(config, :mfa) do
          {m, f, a} when is_atom(m) and is_atom(f) and is_list(a) ->
            {:ok, {m, f, a}}

          invalid ->
            {:error,
             {:invalid_mfa,
              "Worker at index #{index} has invalid MFA: #{inspect(invalid)}. Expected {Module, :function, [args]}"}}
        end

      Keyword.has_key?(config, :fun) ->
        case Keyword.get(config, :fun) do
          fun when is_function(fun, 0) ->
            {:ok, {:fun, fun}}

          invalid ->
            {:error,
             {:invalid_fun,
              "Worker at index #{index} has invalid function: #{inspect(invalid)}. Expected 0-arity function"}}
        end

      true ->
        {:error, {:missing_worker_function, "Worker at index #{index} must have :mfa or :fun"}}
    end
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

  # Extracts ID from configuration
  defp extract_id(config, type, index) do
    case Keyword.get(config, :id) do
      nil ->
        {:error, {:missing_id, "#{type} at index #{index} must have an :id"}}

      id when is_atom(id) ->
        {:ok, id}

      invalid ->
        {:error,
         {:invalid_id,
          "#{type} at index #{index} has invalid ID: #{inspect(invalid)}. Must be an atom"}}
    end
  end
end
