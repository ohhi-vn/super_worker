defmodule SuperWorker.ConfigLoader.ConfigParser do
  @moduledoc """
  This module is the main entry point for loading supervisor configurations.
  It reads configurations from the application environment, uses the Parser
  to validate and expand them, and the Bootstrap to start the supervisors.
  """

  @app :super_worker

  alias SuperWorker.ConfigLoader.Bootstrap
  alias SuperWorker.ConfigLoader.Parser

  require Logger
  require SuperWorker.Log

  @doc """
  Loads all supervisor configurations defined in the application environment,
  except for the general `:options` key.

  For each configuration found, it parses and starts a supervisor.

  ## Example Configuration in `config/config.exs`:

  ```elixir
  config :super_worker,
    my_awesome_supervisor: [
      options: [
        strategy: :one_for_one
      ],
      groups: [
        # ... group definitions
      ]
    ]
  ```
  """
  @spec load() :: :ok | {:error, list()}
  def load do
    SuperWorker.Log.debug(fn ->
      "SuperWorker, ConfigParser, loading all supervisor configurations."
    end)

    configs =
      Application.get_all_env(@app)
      |> Enum.reject(fn {key, _value} -> key == :options end)

    if Enum.empty?(configs) do
      Logger.info("SuperWorker, ConfigParser, no supervisor configurations found to load.")
      :ok
    else
      load_configs(configs)
    end
  end

  defp load_configs(configs) do
    errors =
      configs
      |> Enum.map(fn {sup_id, sup_config} -> load_config(sup_id, sup_config) end)
      |> Enum.filter(&match?({:error, _}, &1))

    case errors do
      [] -> :ok
      errors -> {:error, errors}
    end
  end

  defp load_config(sup_id, sup_config) do
    case load_and_start(sup_id, sup_config) do
      {:ok, _pid} ->
        SuperWorker.Log.debug(fn ->
          "SuperWorker, ConfigParser, started supervisor #{inspect(sup_id)}"
        end)

        :ok

      {:error, reason} = error ->
        Logger.error(
          "SuperWorker, ConfigParser, failed to start supervisor #{inspect(sup_id)}: #{inspect(reason)}"
        )

        error
    end
  end

  @doc """
  Loads and starts a single supervisor configuration by its ID from the
  application environment.
  """
  @spec load_one(sup_id :: atom()) :: {:ok, pid} | {:error, any}
  def load_one(sup_id) do
    SuperWorker.Log.debug(fn ->
      "SuperWorker, ConfigParser, loading supervisor: #{inspect(sup_id)}"
    end)

    case Application.get_env(@app, sup_id) do
      nil ->
        Logger.error(
          "SuperWorker, ConfigParser, configuration for supervisor #{inspect(sup_id)} not found."
        )

        {:error, :config_not_found}

      sup_config ->
        load_and_start(sup_id, sup_config)
    end
  end

  defp load_and_start(sup_id, sup_config) do
    SuperWorker.Log.debug(fn ->
      "SuperWorker, ConfigParser, processing supervisor #{inspect(sup_id)} with config: #{inspect(sup_config)}"
    end)

    case Parser.parse(sup_config) do
      {:ok, parsed_config} ->
        config_with_id = put_in(parsed_config, [:options, :id], sup_id)

        SuperWorker.Log.debug(fn ->
          "SuperWorker, ConfigParser, starting supervisor #{inspect(sup_id)} with processed config: #{inspect(config_with_id)}"
        end)

        Bootstrap.start_supervisor(config_with_id)

      {:error, reason} = error ->
        Logger.error(
          "SuperWorker, ConfigParser, failed to process configuration for supervisor #{inspect(sup_id)}: #{inspect(reason)}"
        )

        error
    end
  end
end
