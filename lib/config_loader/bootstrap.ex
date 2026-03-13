defmodule SuperWorker.ConfigLoader.Bootstrap do
  @moduledoc """
  Bootstraps supervisors from parsed configurations.

  This module takes validated and parsed configurations from the Parser module
  and uses the SuperWorker.Supervisor API to start supervisors and their children.
  """

  alias SuperWorker.Supervisor

  require Logger
  require SuperWorker.Log

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
    options: [id: :my_sup, num_partitions: 2, link: false],
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
    SuperWorker.Log.debug(fn ->
      "SuperWorker, Bootstrap, starting supervisor with options: #{inspect(options)}"
    end)

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

          Supervisor.stop(sup_id)

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
    SuperWorker.Log.debug(fn ->
      "SuperWorker, Bootstrap, starting supervisor process with: #{inspect(options)}"
    end)

    case Supervisor.start_with_config(options) do
      {:ok, pid} ->
        SuperWorker.Log.debug(fn ->
          "SuperWorker, Bootstrap, supervisor process started: #{inspect(pid)}"
        end)

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
    SuperWorker.Log.debug(fn -> "SuperWorker, Bootstrap, no children to add" end)
    :ok
  end

  defp add_children(sup_id, children) do
    SuperWorker.Log.debug(fn ->
      "SuperWorker, Bootstrap, adding #{length(children)} children to supervisor #{inspect(sup_id)}"
    end)

    results =
      children
      |> Enum.map(fn child ->
        add_child(sup_id, child)
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
  defp add_child(sup_id, %{type: :group} = group) do
    SuperWorker.Log.debug(fn ->
      "SuperWorker, Bootstrap, adding group #{inspect(group.id)}, options: #{inspect(group.options)}"
    end)

    group_options = Keyword.put(group.options, :id, group.id)

    with {:ok, _} <- Supervisor.add_group(sup_id, group_options),
         :ok <- add_group_workers(sup_id, group.id, group.workers) do
      SuperWorker.Log.debug(fn ->
        "SuperWorker, Bootstrap, successfully added group: #{inspect(group.id)}"
      end)

      :ok
    else
      {:error, reason} = error ->
        Logger.error(
          "SuperWorker, Bootstrap, failed to add group #{inspect(group.id)}: #{inspect(reason)}"
        )

        error
    end
  end

  defp add_child(sup_id, %{type: :chain} = chain) do
    SuperWorker.Log.debug(fn -> "SuperWorker, Bootstrap, adding chain #{inspect(chain.id)}" end)

    chain_options = Keyword.put(chain.options, :id, chain.id)

    with {:ok, _} <- Supervisor.add_chain(sup_id, chain_options),
         :ok <- add_chain_workers(sup_id, chain.id, chain.workers) do
      SuperWorker.Log.debug(fn ->
        "SuperWorker, Bootstrap, successfully added chain: #{inspect(chain.id)}"
      end)

      :ok
    else
      {:error, reason} = error ->
        Logger.error(
          "SuperWorker, Bootstrap, failed to add chain #{inspect(chain.id)}: #{inspect(reason)}"
        )

        error
    end
  end

  defp add_child(sup_id, %{type: :standalone, mfa: mfa, options: options}) do
    SuperWorker.Log.debug(fn -> "SuperWorker, Bootstrap, adding standalone worker" end)

    # Ensure restart_strategy is set for standalone workers (default: :permanent)
    options_with_defaults =
      if Keyword.has_key?(options, :restart_strategy) do
        options
      else
        Keyword.put(options, :restart_strategy, :permanent)
      end

    case Supervisor.add_standalone_worker(sup_id, mfa, options_with_defaults) do
      {:ok, worker_id} ->
        SuperWorker.Log.debug(fn ->
          "SuperWorker, Bootstrap, successfully added standalone worker: #{inspect(worker_id)}"
        end)

        :ok

      {:error, reason} = error ->
        Logger.error(
          "SuperWorker, Bootstrap, failed to add standalone worker: #{inspect(reason)}"
        )

        error
    end
  end

  defp add_child(_sup_id, invalid) do
    Logger.error("SuperWorker, Bootstrap, invalid child: #{inspect(invalid)}")
    {:error, :invalid_child}
  end

  # Adds workers to a group
  defp add_group_workers(_sup_id, _group_id, []) do
    SuperWorker.Log.debug(fn -> "SuperWorker, Bootstrap, no workers to add to group" end)
    :ok
  end

  defp add_group_workers(sup_id, group_id, workers) do
    SuperWorker.Log.debug(fn ->
      "SuperWorker, Bootstrap, adding #{length(workers)} workers to group #{inspect(group_id)}"
    end)

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
    SuperWorker.Log.debug(fn ->
      "SuperWorker, Bootstrap, adding worker at index #{index} to group #{inspect(group_id)}, mfa: #{inspect(mfa)}, options: #{inspect(options)}"
    end)

    case Supervisor.add_group_worker(sup_id, group_id, mfa, options) do
      {:ok, worker_id} ->
        SuperWorker.Log.debug(fn ->
          "SuperWorker, Bootstrap, successfully added worker #{inspect(worker_id)} to group #{inspect(group_id)}"
        end)

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
    SuperWorker.Log.debug(fn -> "SuperWorker, Bootstrap, no workers to add to chain" end)
    :ok
  end

  defp add_chain_workers(sup_id, chain_id, workers) do
    SuperWorker.Log.debug(fn ->
      "SuperWorker, Bootstrap, adding #{length(workers)} workers to chain #{inspect(chain_id)}"
    end)

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
    SuperWorker.Log.debug(fn ->
      "SuperWorker, Bootstrap, adding worker at index #{index} to chain #{inspect(chain_id)}"
    end)

    case Supervisor.add_chain_worker(sup_id, chain_id, mfa, options) do
      {:ok, worker_id} ->
        SuperWorker.Log.debug(fn ->
          "SuperWorker, Bootstrap, successfully added worker #{inspect(worker_id)} to chain #{inspect(chain_id)}"
        end)

        :ok

      {:error, reason} = error ->
        Logger.error(
          "SuperWorker, Bootstrap, failed to add worker to chain #{inspect(chain_id)}: #{inspect(reason)}"
        )

        error
    end
  end
end
