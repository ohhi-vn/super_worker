defmodule SuperWorker.ConfigLoader.ConfigWrapper do
  @moduledoc """
  This module is the main entry point for loading supervisor configurations.
  It reads configurations from the application environment, uses the Parser
  to validate and expand them, and the Bootstrap to start the supervisors.
  """

  @app :super_worker

  alias SuperWorker.ConfigLoader.Parser
  alias SuperWorker.ConfigLoader.Bootstrap

  require Logger

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
  @spec load() :: :ok
  def load() do
    Logger.debug("SuperWorker, ConfigWrapper, loading all supervisor configurations.")

    configs =
      Application.get_all_env(@app)
      |> Enum.reject(fn {key, _value} -> key == :options end)

    if Enum.empty?(configs) do
      Logger.info("SuperWorker, ConfigWrapper, no supervisor configurations found to load.")
    else
      Enum.each(configs, fn {sup_id, sup_config} ->
        load_and_start(sup_id, sup_config)
      end)
    end

    :ok
  end

  @doc """
  Loads and starts a single supervisor configuration by its ID from the
  application environment.
  """
  @spec load_one(sup_id :: atom()) :: {:ok, pid} | {:error, any}
  def load_one(sup_id) do
    Logger.debug("SuperWorker, ConfigWrapper, loading supervisor: #{inspect(sup_id)}")

    case Application.get_env(@app, sup_id) do
      nil ->
        Logger.error(
          "SuperWorker, ConfigWrapper, configuration for supervisor #{inspect(sup_id)} not found."
        )

        {:error, :config_not_found}

      sup_config ->
        load_and_start(sup_id, sup_config)
    end
  end

  defp load_and_start(sup_id, sup_config) do
    Logger.debug(
      "SuperWorker, ConfigWrapper, processing supervisor #{inspect(sup_id)} with config: #{inspect(sup_config)}"
    )

    with {:ok, parsed_config} <- Parser.parse(sup_config) do
      config_with_id = put_in(parsed_config, [:options, :id], sup_id)

      Logger.debug(
        "SuperWorker, ConfigWrapper, starting supervisor #{inspect(sup_id)} with processed config: #{inspect(config_with_id)}"
      )

      Bootstrap.start_supervisor(config_with_id)
    else
      {:error, reason} ->
        Logger.error(
          "SuperWorker, ConfigWrapper, failed to process configuration for supervisor #{inspect(sup_id)}: #{inspect(reason)}"
        )

        {:error, reason}
    end
  end
end
