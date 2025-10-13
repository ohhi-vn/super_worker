defmodule SuperWorker.ConfigLoader.Bootstrap do
  @moduledoc """
  Bootstraps supervisors from parsed configurations.

  This module takes validated and parsed configurations from the Parser module
  and uses the SuperWorker.Supervisor API to start supervisors and their children.
  """

  alias SuperWorker.Supervisor

  require Logger

  @doc """
  Starts a supervisor with the given parsed configuration.

  ## Parameters
    * `config` - A parsed configuration map containing:
      * `:options` - Supervisor options (must include `:id`)
      * `:children` - List of child specifications

  ## Returns
    * `{:ok, pid}` - Successfully started supervisor
    * `{:error, reason}` - Failed to start supervisor

  ## Example

  ```elixir
  config = %{
    options: [id: :my_sup, number_of_partitions: 2, link: false],
    children: [
      %{
        type: :group,
        id: :my_group,
        options: [restart_strategy: :one_for_one],
        workers: [
          %{mfa: {MyModule, :worker_fun, []}, options: [id: :worker1]}
        ]
      }
    ]
  }

  Bootstrap.start_supervisor(config)
  ```
  """
  @spec start_supervisor(map()) :: {:ok, pid()} | {:error, any()}
  def start_supervisor(%{options: options, children: children}) do
    Logger.debug("SuperWorker, Bootstrap, starting supervisor with options: #{inspect(options)}")

    sup_id = Keyword.get(options, :id)

    if sup_id == nil do
      Logger.error("SuperWorker, Bootstrap, supervisor options must include :id")
      {:error, :missing_supervisor_id}
    else
      with {:ok, pid} <- start_supervisor_process(options),
           :ok <- add_children(sup_id, children) do
        Logger.info("SuperWorker, Bootstrap, successfully started supervisor: #{inspect(sup_id)}")
        {:ok, pid}
      else
        {:error, reason} = error ->
          Logger.error(
            "SuperWorker, Bootstrap, failed to start supervisor #{inspect(sup_id)}: #{inspect(reason)}"
          )

          # Attempt cleanup if supervisor was started but children failed
          if Supervisor.is_running?(sup_id) do
            Logger.debug(
              "SuperWorker, Bootstrap, cleaning up failed supervisor: #{inspect(sup_id)}"
            )

            Supervisor.stop(sup_id)
          end

          error
      end
    end
  end

  def start_supervisor(invalid) do
    Logger.error("SuperWorker, Bootstrap, invalid configuration format: #{inspect(invalid)}")
    {:error, :invalid_config_format}
  end

  # Starts the supervisor process with the given options
  defp start_supervisor_process(options) do
    Logger.debug("SuperWorker, Bootstrap, starting supervisor process with: #{inspect(options)}")

    case Supervisor.start(options) do
      {:ok, pid} ->
        Logger.debug("SuperWorker, Bootstrap, supervisor process started: #{inspect(pid)}")
        {:ok, pid}

      {:error, reason} = error ->
        Logger.error(
          "SuperWorker, Bootstrap, failed to start supervisor process: #{inspect(reason)}"
        )

        error
    end
  end

  # Adds all children to the supervisor
  defp add_children(_sup_id, []) do
    Logger.debug("SuperWorker, Bootstrap, no children to add")
    :ok
  end

  defp add_children(sup_id, children) do
    Logger.debug(
      "SuperWorker, Bootstrap, adding #{length(children)} children to supervisor #{inspect(sup_id)}"
    )

    results =
      children
      |> Enum.with_index()
      |> Enum.map(fn {child, index} ->
        add_child(sup_id, child, index)
      end)

    errors = Enum.filter(results, fn result -> match?({:error, _}, result) end)

    if Enum.empty?(errors) do
      :ok
    else
      Logger.error("SuperWorker, Bootstrap, errors adding children: #{inspect(errors)}")
      {:error, {:children_start_errors, errors}}
    end
  end

  # Adds a single child (group, chain, or standalone worker)
  defp add_child(sup_id, %{type: :group} = group, index) do
    Logger.debug("SuperWorker, Bootstrap, adding group #{inspect(group.id)} at index #{index}")

    group_options = Keyword.put(group.options, :id, group.id)

    with {:ok, _} <- Supervisor.add_group(sup_id, group_options),
         :ok <- add_group_workers(sup_id, group.id, group.workers) do
      Logger.debug("SuperWorker, Bootstrap, successfully added group: #{inspect(group.id)}")
      :ok
    else
      {:error, reason} = error ->
        Logger.error(
          "SuperWorker, Bootstrap, failed to add group #{inspect(group.id)}: #{inspect(reason)}"
        )

        error
    end
  end

  defp add_child(sup_id, %{type: :chain} = chain, index) do
    Logger.debug("SuperWorker, Bootstrap, adding chain #{inspect(chain.id)} at index #{index}")

    chain_options = Keyword.put(chain.options, :id, chain.id)

    with {:ok, _} <- Supervisor.add_chain(sup_id, chain_options),
         :ok <- add_chain_workers(sup_id, chain.id, chain.workers) do
      Logger.debug("SuperWorker, Bootstrap, successfully added chain: #{inspect(chain.id)}")
      :ok
    else
      {:error, reason} = error ->
        Logger.error(
          "SuperWorker, Bootstrap, failed to add chain #{inspect(chain.id)}: #{inspect(reason)}"
        )

        error
    end
  end

  defp add_child(sup_id, %{type: :standalone, mfa: mfa, options: options}, index) do
    Logger.debug("SuperWorker, Bootstrap, adding standalone worker at index #{index}")

    # Ensure restart_strategy is set for standalone workers (default: :permanent)
    options_with_defaults =
      if Keyword.has_key?(options, :restart_strategy) do
        options
      else
        Keyword.put(options, :restart_strategy, :permanent)
      end

    case Supervisor.add_standalone_worker(sup_id, mfa, options_with_defaults) do
      {:ok, worker_id} ->
        Logger.debug(
          "SuperWorker, Bootstrap, successfully added standalone worker: #{inspect(worker_id)}"
        )

        :ok

      {:error, reason} = error ->
        Logger.error(
          "SuperWorker, Bootstrap, failed to add standalone worker at index #{index}: #{inspect(reason)}"
        )

        error
    end
  end

  defp add_child(_sup_id, invalid, index) do
    Logger.error("SuperWorker, Bootstrap, invalid child at index #{index}: #{inspect(invalid)}")
    {:error, {:invalid_child, index}}
  end

  # Adds workers to a group
  defp add_group_workers(_sup_id, _group_id, []) do
    Logger.debug("SuperWorker, Bootstrap, no workers to add to group")
    :ok
  end

  defp add_group_workers(sup_id, group_id, workers) do
    Logger.debug(
      "SuperWorker, Bootstrap, adding #{length(workers)} workers to group #{inspect(group_id)}"
    )

    results =
      workers
      |> Enum.with_index()
      |> Enum.map(fn {worker, index} ->
        add_group_worker(sup_id, group_id, worker, index)
      end)

    errors = Enum.filter(results, fn result -> match?({:error, _}, result) end)

    if Enum.empty?(errors) do
      :ok
    else
      Logger.error(
        "SuperWorker, Bootstrap, errors adding workers to group #{inspect(group_id)}: #{inspect(errors)}"
      )

      {:error, {:group_workers_start_errors, errors}}
    end
  end

  defp add_group_worker(sup_id, group_id, %{mfa: mfa, options: options}, index) do
    Logger.debug(
      "SuperWorker, Bootstrap, adding worker at index #{index} to group #{inspect(group_id)}"
    )

    case Supervisor.add_group_worker(sup_id, group_id, mfa, options) do
      {:ok, worker_id} ->
        Logger.debug(
          "SuperWorker, Bootstrap, successfully added worker #{inspect(worker_id)} to group #{inspect(group_id)}"
        )

        :ok

      {:error, reason} = error ->
        Logger.error(
          "SuperWorker, Bootstrap, failed to add worker to group #{inspect(group_id)}: #{inspect(reason)}"
        )

        error
    end
  end

  # Adds workers to a chain
  defp add_chain_workers(_sup_id, _chain_id, []) do
    Logger.debug("SuperWorker, Bootstrap, no workers to add to chain")
    :ok
  end

  defp add_chain_workers(sup_id, chain_id, workers) do
    Logger.debug(
      "SuperWorker, Bootstrap, adding #{length(workers)} workers to chain #{inspect(chain_id)}"
    )

    results =
      workers
      |> Enum.with_index()
      |> Enum.map(fn {worker, index} ->
        add_chain_worker(sup_id, chain_id, worker, index)
      end)

    errors = Enum.filter(results, fn result -> match?({:error, _}, result) end)

    if Enum.empty?(errors) do
      :ok
    else
      Logger.error(
        "SuperWorker, Bootstrap, errors adding workers to chain #{inspect(chain_id)}: #{inspect(errors)}"
      )

      {:error, {:chain_workers_start_errors, errors}}
    end
  end

  defp add_chain_worker(sup_id, chain_id, %{mfa: mfa, options: options}, index) do
    Logger.debug(
      "SuperWorker, Bootstrap, adding worker at index #{index} to chain #{inspect(chain_id)}"
    )

    case Supervisor.add_chain_worker(sup_id, chain_id, mfa, options) do
      {:ok, worker_id} ->
        Logger.debug(
          "SuperWorker, Bootstrap, successfully added worker #{inspect(worker_id)} to chain #{inspect(chain_id)}"
        )

        :ok

      {:error, reason} = error ->
        Logger.error(
          "SuperWorker, Bootstrap, failed to add worker to chain #{inspect(chain_id)}: #{inspect(reason)}"
        )

        error
    end
  end
end
