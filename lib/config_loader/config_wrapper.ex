defmodule SuperWorker.Supervisor.ConfigWrapper do
  @moduledoc """
  This module is a wrapper for the configuration of the supervisor.
  """

  @app :super_worker

  alias SuperWorker.Supervisor, as: Sup

  # @params [:options, :chains, :groups, :standalones]

  require Logger

  @doc """
  Load the configuration of the supervisor.
  """
  def load(opts) do
    opts
  end

  @doc """
  Load the configuration of the supervisor with the given configuration.

  Config has path 'config :super_worker,...'

  ## Examples
  ```Elixir
  config :super_worker,
    options: :hello,
    sup_cfg_1: [  # Declare config by config file.
      options: [
        number_of_partitions: 2,
        link: true,
        report_to: {Dev, :report, []}
      ],
      groups: [
        group1: [
          options: [
            restart_strategy: :one_for_all
          ],
          workers: [
            worker1: [
              task: {Dev, :task, [15]}
            ],
            worker2: [
              options: [

              ],
              task: {Dev, :task_crash, [15, 5]}
            ]
          ]
        ],
      ]
    ]
  ```
  """
  @spec load() :: :ok
  def load() do
    config =
      Application.get_all_env(@app)
      |> Enum.filter(fn
        {:options, _} ->
          false
        _ ->
          true
      end)

    Enum.each(config, fn {sup_id, sup_config} ->
      Logger.debug("SuperWorker, Config, loading supervisor #{inspect sup_id}, config: #{inspect sup_config}")
      {:ok, sup_config} =
        sup_config
        |> expand_config()
        |> verify_config()

      # add supervisor id to options
      sup_config = put_in(sup_config, [:options, :id], sup_id)

      Logger.debug("SuperWorker, Config, starting supervisor #{inspect sup_id} with config: #{inspect sup_config}")

      start_supervisor(sup_config)
    end)
  end

  @spec load_one(atom()) :: {:error, any()} | {:ok, pid()}
  @doc """
  Load one supervisor with the given configuration.
  """
  def load_one(sup_config) do
    {:ok, config} =
      Application.get_env(:super_worker, sup_config)
      |> expand_config()
      |> verify_config()

    # add supervisor id to options
    config = put_in(config, [:options, :id], sup_config)

    Logger.debug("SuperWorker, Config, load supervisor #{inspect sup_config} with config: #{inspect config}")

    start_supervisor(config)
  end

  ## Private functions ##

  defp expand_config(config) when is_list(config) do
    Enum.map(config, fn {key, value} -> {key, get_config(value)} end)
  end

  defp get_config({module, fun, args}) when is_atom(module) and is_atom(fun) and is_list(args) do
    try do
      apply(module, fun, args)
    catch
      error ->
        Logger.error("SuperWorker, Config, failed to get config by #{inspect module}.#{inspect fun}(#{inspect args}), reason: #{inspect error}")
        reraise error, __STACKTRACE__
    end
  end

  defp get_config(value) do
    value
  end

  defp verify_config(config) do
    if !Keyword.keyword?(config) do
     {:error, :invalid_config}
    else
      result =
      Enum.all?(config, fn {key, value} ->
        case key do
          :options ->
            is_list(value)
          :chains ->
            is_list(value)
          :groups ->
            is_list(value)
          :standalones ->
            is_list(value)
          _ ->
            false
        end
      end)

      if result do
        {:ok, config}
      else
        {:error, :invalid_config}
      end
    end
  end

  defp start_supervisor(config) do
    Logger.debug("SuperWorker, Config, starting supervisor with config: #{inspect config}")

    # Start the supervisor with the given config.
    [options] = Keyword.get_values(config, :options)

    case Sup.start(options) do
      {:ok, _} = result ->
        sup_id = get_in(options, [:id])
        Logger.info("SuperWorker, Config, supervisor #{inspect sup_id} started successfully")

        # add groups & workers of groups
        if Keyword.has_key?(config, :groups) do
          [groups] = Keyword.get_values(config, :groups)

          Enum.each(groups, fn {group_id, group} ->
            group = put_in(group, [:options, :id], group_id)
            group_id = get_in(group, [:options, :id])

            Sup.add_group(sup_id, Keyword.get(group, :options))
            Logger.debug("SuperWorker, Config, group #{inspect group_id} added to #{inspect sup_id}")

            default_worker_options = Keyword.get(group, :default_worker_options, [])

            Enum.each( get_in(group, [:workers]), fn {worker_id, worker} ->
              opts = Keyword.merge(default_worker_options, Keyword.get(worker, :options, []))
              Sup.add_group_worker(sup_id, group_id, Keyword.get(worker, :task), opts)
              Logger.debug("SuperWorker, Config, worker #{inspect worker_id} added to group #{inspect group_id}")
            end)
          end)
        else
          Logger.debug("SuperWorker, Config, no groups to add to #{inspect sup_id}")
        end

        # add chains & workers of chains
        if Keyword.has_key?(config, :chains) do
          [chains] = Keyword.get_values(config, :chains)

          Enum.each(chains, fn {chain_id, chain} ->
            chain = put_in(chain, [:options, :id], chain_id)
            chain_id = get_in(chain, [:options, :id])

            Sup.add_chain(sup_id, get_in(chain, [:options]))
            Logger.debug("SuperWorker, Config, chain #{inspect chain_id} added to #{inspect sup_id}")

            default_worker_options = Keyword.get(chain, :default_worker_options, [])

            Enum.each(get_in(chain, [:workers]), fn {worker_id, worker} ->
              opts = Keyword.merge(default_worker_options, Keyword.get(worker, :options, []))
              Sup.add_chain_worker(sup_id, chain_id, get_in(worker, [:task]), opts)
              Logger.debug("SuperWorker, Config, worker #{inspect worker_id} added to chain #{inspect chain_id}")
            end)
          end)
        else
          Logger.debug("SuperWorker, Config, no chains to add to #{inspect sup_id}")
        end

        result
      {:error, reason} = error ->
        Logger.error("SuperWorker, Config, failed to start supervisor: #{inspect reason}")
        error
    end
  end

end
