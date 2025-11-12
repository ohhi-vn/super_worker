defmodule SuperWorker.Supervisor do
  @moduledoc """
  Documentation for `SuperWorker.Supervisor`.
  This module is new model supervisor.
  That fix some issues in the old model.
  That is an all-in-one supervisor for Elixir application.

  New supervisor supports the following features:
  - Group processes
  - Chain processes
  - Freedom processes

  ## Group processes
  Group processes are a set of processes that are started together.
  If one of the processes dies, all the processes in the group will be stopped.
  Each group has a seperated restart strategy that determines how to restart the group when a process dies.

  ## Chain processes
  Chain processes are a set of processes that support for chain prcessing.
  Each process in a chain has order to process data.
  The output of the previous process is passed to the next process.

  ## Freedom processes
  Freedom processes are independent processes that are started separately.
  Each process has its own restart strategy.

  All type of processes can be started in parallel & can be stopped individually or in a group.

  ## Examples
  ```elixir
  # Start a supervisor with 2 partitions & 2 groups:
  alias SuperWorker.Supervisor, as: Sup

  # Config for supervisor
  opts = [id: :sup1, number_of_partitions: 2, link: false]

  # Start supervisor
  Sup.start(opts)

  # Add group in runtime, you also can add group in config.
  Sup.add_group(:sup1, [id: :group1, restart_strategy: :one_for_all])
  Sup.add_group_worker(:sup1, :group1, {Dev, :task, [15]}, [id: :g1_1])

  Sup.add_group(:sup1, [id: :group2, restart_strategy: :one_for_one])
  Sup.add_group_worker(:sup1, :group2, fn ->
    receice do
    msg ->
      :ok
    end
  end, [id: :g2_2])
  ```
  """

  defstruct [
    # partition id, if :id == :master that mean is master process
    :id,
    # owner of the supervisor
    :owner,
    # name of supervisor
    :master,

    # number of partitions, default is number of online schedulers
    :number_of_partitions,
    # link the supervisor to the caller

    link: true,
    children: [],
    # list of pid or callback function, for reporting worker crashed or worker finished.
    report_to: [],
    # list of linked external pids
    linked_pids: [],
    partitions: []
  ]

  alias __MODULE__

  alias Supervisor.{Group, Db, ApiHelper, Chain, Worker, Message, Partition, Looper, Validator}

  require Logger

  # Default timeout (miliseconds) for API calls.
  @default_time 3_000

  ## Public APIs

  @doc """
  Start supervisor for run standalone please set option :link to false.
  result format: {:ok, pid} or {:error, reason}
  """
  @spec start(
          id: atom(),
          link: boolean() | pid(),
          number_of_partitions: integer(),
          report_to: list()
        ) :: {:ok, pid} | {:error, any()}
  def start(opts, timeout \\ @default_time) when is_list(opts) do
    with {:ok, sup} <- Validator.validate_and_convert(opts),
         false <- is_running?(sup.id) do
      start_supervisor(sup, timeout)
    else
      true ->
        Logger.error(
          "SuperWorker, Supervisor, supervisor has id in #{inspect(opts)} is already running."
        )

        {:error, :already_running}

      {:error, _} = error ->
        Logger.error("SuperWorker, Supervisor, Error when starting supervisor: #{inspect(error)}")
        error
    end
  end

  @doc """
  Stop supervisor.
  Type of shutdown:
  - :normal supervisor will send a message to worker for graceful shutdown. Not support for spawn process by function.
  - :kill supervisor will kill worker.
  """
  @spec stop(atom(), shutdown_type :: atom(), timeout :: integer()) ::
          {:ok, atom()} | {:error, any()}
  def stop(sup_id, shutdown_type \\ :kill, timeout \\ @default_time) do
    case get_pid(sup_id) do
      {:error, _} = err ->
        Logger.error("SuperWorker, Supervisor, supervisor is not running.")
        err

      {:ok, pid} ->
        Logger.debug(
          "SuperWorker, Supervisor, stopping supervisor: #{inspect(pid)}, shutdown type: #{inspect(shutdown_type)}"
        )

        ApiHelper.call_api(pid, :stop, shutdown_type, timeout)
    end
  end

  @doc """
  Check if supervisor is running.
  return true if supervisor is running, otherwise return false.
  """
  @spec is_running?(atom()) :: boolean()
  def is_running?(sup_id) do
    case get_pid(sup_id) do
      {:ok, _} -> true
      {:error, _} -> false
    end
  end

  @doc """
  Get supervisor id in current process (except GenServer worker).
  """
  def get_my_supervisor() do
    Process.get({:supervisor, :sup_id})
  end

  ## Chan APIs ##

  @doc """
  Add a worker to the chain in supervisor.
  """
  @spec add_chain_worker(atom(), atom(), {module(), atom(), list()} | fun(), list(), integer()) ::
          {:ok, atom()} | {:error, any()}
  def add_chain_worker(sup_id, chain_id, mfa_or_fun, opts, timeout \\ @default_time)

  def add_chain_worker(sup_id, chain_id, {m, f, a} = mfa, opts, timeout)
      when is_list(opts) and is_atom(m) and is_atom(f) and is_list(a) do
    do_add_chain_worker(sup_id, chain_id, [{:fun, mfa} | opts], timeout)
  end

  def add_chain_worker(sup_id, chain_id, fun, opts, timeout)
      when is_list(opts) and is_function(fun, 0) do
    do_add_chain_worker(sup_id, chain_id, [{:fun, {:fun, fun}} | opts], timeout)
  end

  @doc """
  Add a chain to the supervisor.
  Chain's options follow docs in `Chain` module.
  """
  def add_chain(sup_id, opts, timeout \\ 5_000) do
    with true <- is_running?(sup_id),
         {:ok, chain} <- Chain.check_options(opts),
         {:error, _} <- get_chain(sup_id, chain.id),
         {:ok, parition_id, pid} <- Partition.get_host_partition(sup_id, chain.id) do
      %Chain{} = chain
      chain = %{chain | supervisor: sup_id, partition: parition_id}
      ApiHelper.call_api(pid, :add_chain, chain, timeout)
    else
      wrong ->
        Logger.error("SuperWorker, Supervisor, error when adding chain: #{inspect(wrong)}")
        {:error, :supervisor_not_found_or_chain_exists}
    end
  end

  @doc """
  Send data to the entry worker in the chain.
  If chain doesn't has any worker, it will be dropped.
  """
  def send_to_chain(sup_id, chain_id, data, timeout \\ @default_time) do
    with true <- is_running?(sup_id),
         {:ok, pid} <- verify_and_get_pid(sup_id, chain_id) do
      ApiHelper.call_api(pid, :add_data_to_chain, {chain_id, data}, timeout)
    end
  end

  @doc """
  get chain structure from supervisor.
  """
  @spec get_chain(atom(), any()) :: {:ok, Group.t()} | {:error, any()}
  def get_chain(sup_id, chain_id, timeout \\ @default_time) do
    with true <- is_running?(sup_id),
         {:ok, _partition_id, pid} <- Partition.get_host_partition(sup_id, chain_id) do
      ApiHelper.call_api(pid, :get_chain, chain_id, timeout)
    else
      wrong ->
        Logger.error("SuperWorker, Supervisor, error when get chain: #{inspect(wrong)}")
        {:error, :supervisor_not_found_or_chain_exists}
    end
  end

  ## Group APIs ##

  @doc """
  Add a  worker to a group in the supervisor.
  Function's options follow `Worker` module.
  """
  @spec add_group_worker(atom(), atom(), {module(), atom(), list()} | fun(), list(), integer()) ::
          {:ok, atom()} | {:error, any()}
  def add_group_worker(sup_id, group_id, mfa_or_fun, opts, timeout \\ @default_time)

  def add_group_worker(sup_id, group_id, {m, f, a} = mfa, opts, timeout)
      when is_list(opts) and is_atom(m) and is_atom(f) and is_list(a) and group_id != nil do
    do_add_group_worker(sup_id, group_id, [{:fun, mfa} | opts], timeout)
  end

  def add_group_worker(sup_id, group_id, fun, opts, timeout)
      when is_list(opts) and is_function(fun, 0) do
    do_add_group_worker(sup_id, group_id, [{:fun, {:fun, fun}} | opts], timeout)
  end

  def add_group_worker(sup_id, group_id, module, options, timeout)
      when is_atom(module) and group_id != nil do
    add_group_worker(sup_id, group_id, {module, []}, options, timeout)
  end

  def add_group_worker(sup_id, group_id, {module, init_options} = worker, options, timeout)
      when is_atom(module) and is_list(init_options) and group_id != nil do
    options = convert_gen_server_specs(worker, options)

    do_add_group_worker(sup_id, group_id, options, timeout)
  end

  @doc """
  Add a group to the supervisor.
  Group's options follow docs in `Group` module.
  """
  @spec add_group(atom(), list(), integer()) :: {:ok, atom()} | {:error, any()}
  def add_group(sup_id, opts, timeout \\ @default_time) do
    with true <- is_running?(sup_id),
         {:ok, group = %Group{}} <- Group.check_options(opts),
         {:error, _} <- get_group(sup_id, group.id),
         {:ok, parititon_id, pid} <- Partition.get_host_partition(sup_id, group.id) do
      group = %Group{group | supervisor: sup_id, partition: parititon_id}
      ApiHelper.call_api(pid, :add_group, group, timeout)
    else
      wrong ->
        Logger.error("SuperWorker, Supervisor, error when adding group: #{inspect(wrong)}")
        {:error, :supervisor_not_found_or_group_exists}
    end
  end

  @doc """
  get group structure from supervisor.
  """
  @spec get_group(atom(), atom()) :: {:ok, Group.t()} | {:error, any()}
  def get_group(sup_id, group_id, timeout \\ @default_time) do
    Db.get_group(sup_id, group_id)

    with true <- is_running?(sup_id),
         {:ok, _partition_id, pid} <- Partition.get_host_partition(sup_id, group_id) do
      ApiHelper.call_api(pid, :get_group, group_id, timeout)
    else
      wrong ->
        Logger.error("SuperWorker, Supervisor, error when get group: #{inspect(wrong)}")
        {:error, :supervisor_not_found_or_chain_exists}
    end
  end

  @doc """
  Send data to all workers in a group.
  """
  def broadcast_to_group(sup_id, group_id, data, timeout \\ @default_time) do
    with true <- is_running?(sup_id),
         {:ok, pid} <- verify_and_get_pid(sup_id, group_id) do
      ApiHelper.call_api(pid, :broadcast_to_group, {group_id, data}, timeout)
    end
  end

  @doc """
  Send data to all workers in current group of worker.
  Using for communite between workers in the same group.
  """
  def broadcast_to_my_group(data) do
    group_id = get_my_group()
    sup_id = get_my_supervisor()

    cond do
      group_id == nil ->
        Logger.error("SuperWorker, Supervisor, group not found.")
        {:error, :group_not_found}

      sup_id == nil ->
        Logger.error("SuperWorker, Supervisor, supervisor not found.")
        {:error, :supervisor_not_found}

      true ->
        broadcast_to_group(sup_id, group_id, data)
    end
  end

  @doc """
  Send data to a worker in the group.
  """
  def send_to_group(sup_id, group_id, worker_id, data, timeout \\ @default_time) do
    with true <- is_running?(sup_id),
         {:ok, pid} <- verify_and_get_pid(sup_id, group_id) do
      ApiHelper.call_api(pid, :send_to_group, {group_id, worker_id, data}, timeout)
    end
  end

  @doc """
  Send data to a random worker in the group.
  """
  def send_to_group_random(sup_id, group_id, data, timeout \\ @default_time) do
    with true <- is_running?(sup_id),
         {:ok, pid} <- verify_and_get_pid(sup_id, group_id) do
      ApiHelper.call_api(pid, :send_to_group_random, {group_id, data}, timeout)
    end
  end

  @doc """
  Send data to other worker in the same group.
  """
  def send_to_my_group(worker_id, data) do
    group_id = get_my_group()
    sup_id = get_my_supervisor()

    cond do
      group_id == nil ->
        Logger.error("SuperWorker, Supervisor, group not found.")
        {:error, :group_not_found}

      sup_id == nil ->
        Logger.error("SuperWorker, Supervisor, supervisor not found.")
        {:error, :supervisor_not_found}

      true ->
        send_to_group(sup_id, group_id, worker_id, data)
    end
  end

  def send_to_my_group_random(data) do
    group_id = get_my_group()
    sup_id = get_my_supervisor()

    cond do
      group_id == nil ->
        Logger.error("SuperWorker, Supervisor, group not found.")
        {:error, :grou_not_found}

      sup_id == nil ->
        Logger.error("SuperWorker, Supervisor, supervisor not found.")
        {:error, :supervisor_not_found}

      true ->
        send_to_group_random(sup_id, group_id, data)
    end
  end

  def remove_group_worker(sup_id, group_id, worker_id, timeout \\ @default_time) do
    with {:ok, pid} <- verify_and_get_pid(sup_id, group_id) do
      ApiHelper.call_api(pid, :remove_group_worker, {worker_id, group_id}, timeout)
    end
  end

  def get_my_group() do
    Process.get({:supervisor, :group_id})
  end

  ## Standalone worker api ##

  @doc """
  Add a standalone worker process to the supervisor.
  function for start worker can be a function or a {module, function, arguments}.
  Standalone worker is run independently from other workers follow :one_to_one strategy.
  If worker crashes, it will check the restart strategy of worker then act accordingly.
  """
  @spec add_standalone_worker(atom(), {module(), atom(), list()} | fun(), list(), integer()) ::
          {:ok, atom()} | {:error, any()}
  def add_standalone_worker(sup_id, mfa_or_fun, opts \\ [], timeout \\ @default_time)

  def add_standalone_worker(sup_id, {m, f, a} = mfa, opts, timeout)
      when is_list(opts) and is_atom(m) and is_atom(f) and is_list(a) do
    do_add_standalone_worker(sup_id, [{:fun, mfa} | opts], timeout)
  end

  def add_standalone_worker(sup_id, fun, opts, timeout)
      when is_list(opts) and is_function(fun, 0) do
    do_add_standalone_worker(sup_id, [{:fun, {:fun, fun}} | opts], timeout)
  end

  def add_standalone_worker(sup_id, {:fun, fun} = f, opts, timeout)
      when is_list(opts) and is_function(fun, 0) do
    do_add_standalone_worker(sup_id, [{:fun, f} | opts], timeout)
  end

  def add_standalone_worker(sup_id, {genserver_module, _} = f, options, timeout)
      when is_list(options) and is_atom(genserver_module) do
    options = convert_gen_server_specs(f, options)

    do_add_standalone_worker(sup_id, options, timeout)
  end

  def add_standalone_worker(sup_id, genserver_module, opts, timeout)
      when is_list(opts) and is_atom(genserver_module) do
    add_standalone_worker(sup_id, {genserver_module, []}, opts, timeout)
  end

  @doc """
  Send data directly to the worker standalone in the supervisor.
  """
  def send_to_standalone_worker(sup_id, worker_id, data, timeout \\ @default_time) do
    with true <- is_running?(sup_id),
         {:ok, pid} <- verify_and_get_pid(sup_id, :standalone) do
      ApiHelper.call_api(pid, :send_to_worker, {worker_id, data}, timeout)
    end
  end

  def remove_standalone_worker(sup_id, worker_id, timeout \\ @default_time) do
    with {:ok, pid} <- verify_and_get_pid(sup_id, :standalone) do
      ApiHelper.call_api(pid, :remove_standalone_worker, worker_id, timeout)
    end
  end

  def get_all_standalone_workers(sup_id, timeout \\ @default_time) do
    with {:ok, pid} <- verify_and_get_pid(sup_id, :standalone) do
      ApiHelper.call_api(pid, :get_all_standalone_workers, :standalone, timeout)
    end
  end

  ## Internal public functions

  def init(sup = %Supervisor{}, message = %Message{}) do
    # Register the supervisor process.
    Process.register(self(), sup.id)

    # Link to remote pid if link is a pids
    case sup.link do
      pid when is_pid(pid) ->
        Process.link(pid)

      list_pid when is_list(list_pid) ->
        Enum.each(list_pid, fn pid ->
          Process.link(pid)
        end)

      bool when bool in [true, false] ->
        :ok
    end

    Db.init(sup.id)

    Logger.debug("SuperWorker, Supervisor, create table for #{inspect(sup.id)} done.")

    Db.put_sup_info(sup.id, :master, sup)

    # Turn main process to system process.
    Process.flag(:trap_exit, true)

    list_partitions = init_additional_partitions(sup)

    started_partitions =
      Enum.reduce(1..length(list_partitions), [], fn _, acc ->
        receive do
          {:partition_started, id} ->
            Logger.debug(
              "SuperWorker, Supervisor, supervisor #{inspect(sup.id)} received started partition msg from #{inspect(id)}"
            )

            [id | acc]
        after
          @default_time ->
            Logger.debug(
              "SuperWorker, Supervisor, supervisor #{inspect(sup.id)} timeout when starting partition. Current list: #{inspect(acc)}"
            )

            acc
        end
      end)

    if length(started_partitions) != length(list_partitions) do
      Logger.error(
        "SuperWorker, Supervisor, supervisor #{inspect(sup.id)} failed to start partitions."
      )

      ApiHelper.api_response(message, {:error, :failed_to_start_partitions})
    else
      state =
        sup
        |> Map.put(:partitions, list_partitions)

      Logger.debug(
        "SuperWorker, Supervisor, supervisor #{inspect(state.id)} initialized: #{inspect(state)}"
      )

      # TO-DO: Add group, chain, worker from opts.
      if sup.children != nil do
        Enum.each(sup.children, fn child ->
          case child do
            {:group, group} ->
              add_group(state.id, group)

            {:chain, chain} ->
              add_chain(state.id, chain)

            {:standalone, worker} ->
              if Keyword.get(worker, :options) == nil do
                add_standalone_worker(state.id, worker.task)
              else
                add_standalone_worker(state.id, worker.task, worker.opts)
              end
          end
        end)
      end

      ApiHelper.api_response(message, {:ok, self()})

      # Start the main loop
      Looper.main_loop(state)
    end
  end

  def child_spec(opts) do
    %{
      # default id is module name
      id: Keyword.get(opts, :id, Supervisor),
      start: {Supervisor, :start, [opts]}
    }
  end

  ## Private functions

  defp init_partition(partition = %Supervisor{}) do
    # Start the main loop
    pid = spawn_link(Supervisor, :start_partition, [partition])

    Logger.debug(
      "SuperWorker, Supervisor, #{inspect(partition.id)} initialized, pid: #{inspect(pid)}"
    )

    {:ok, partition.id, pid}
  end

  def start_partition(state) do
    Db.put_sup_pid(state.master, state.id, self())

    send(state.master, {:partition_started, state.id})

    # Turn partition process to system process.
    Process.flag(:trap_exit, true)

    Looper.main_loop(state)
  end

  defp init_additional_partitions(sup) do
    partitions = sup.number_of_partitions

    Enum.map(0..(partitions - 1), fn i ->
      Logger.debug("SuperWorker, Supervisor, [#{inspect(sup.id)}] add partition: #{inspect(i)}")

      sup =
        sup
        |> Map.put(:master, sup.id)
        |> Map.put(:id, String.to_atom("#{Atom.to_string(sup.id)}_#{i}"))

      {:ok, partition, pid} = init_partition(sup)
      {partition, pid}
    end)
  end

  defp shutdown(state, :kill) do
    Logger.debug("SuperWorker, Supervisor, shutting down supervisor: #{inspect(state.id)}")

    # TO-DO: Implement graceful shutdown for worker processes.
    #
    {:ok, groups} = Db.get_all_groups(state.master)

    Enum.each(groups, fn group ->
      Group.kill_all_workers(group)
    end)

    {:ok, chains} = Db.get_all_chains(state.master)

    Enum.each(chains, fn chain ->
      Chain.kill_all_workers(chain)
    end)

    {:ok, workers} = Db.get_all_standalone_worker_infos(state.master)

    Enum.each(workers, fn worker ->
      Process.exit(worker.pid, :kill)
    end)

    {:ok, :brutal_kill}
  end

  defp get_pid(id) when is_atom(id) do
    case Process.whereis(id) do
      nil ->
        {:error, :not_running}

      pid ->
        {:ok, pid}
    end
  end

  # Start the supervisor main processes.
  defp start_supervisor(opts = %Supervisor{}, timeout) do
    Logger.debug("SuperWorker, Supervisor, starting supervisor with options: #{inspect(opts)}")

    message = Message.new(:init_sup, nil, nil)

    # Start main process of the supervisor
    case opts.link do
      true ->
        Logger.debug("SuperWorker, Supervisor, starting supervisor with link.")
        opts = Map.put(opts, :linked_pids, [self()])
        spawn_link(Supervisor, :init, [opts, message])

      false ->
        Logger.debug("SuperWorker, Supervisor, starting supervisor without link.")
        spawn(Supervisor, :init, [opts, message])

      pid when is_pid(pid) ->
        Logger.debug("SuperWorker, Supervisor, starting supervisor and link with remote pid.")
        opts = Map.put(opts, :linked_pids, [pid])
        spawn(Supervisor, :init, [opts, message])

      list_pid when is_list(list_pid) ->
        Logger.debug("SuperWorker, Supervisor, starting supervisor and link with remote pids.")
        opts = Map.put(opts, :linked_pids, list_pid)
        spawn(Supervisor, :init, [opts, message])
    end

    ApiHelper.api_receiver(message.id, timeout)
  end

  # Check the supervisor is running or not.
  # if running get pid of partition.
  @spec verify_and_get_pid(atom(), any()) :: {:error, atom()} | {:ok, pid()}
  defp verify_and_get_pid(sup_id, id) do
    with true <- is_running?(sup_id),
         {:ok, _partition_id, pid} <- Partition.get_host_partition(sup_id, id) do
      {:ok, pid}
    else
      false ->
        Logger.error("SuperWorker, Supervisor, supervisor #{inspect(sup_id)} not running.")
        {:error, :not_running}

      {:error, reason} = error ->
        Logger.error("SuperWorker, Supervisor, get target partition failed: #{inspect(reason)}")
        error
    end
  end

  @spec do_add_standalone_worker(atom(), list(), integer()) ::
          {:ok, any()} | {:error, any()}
  defp do_add_standalone_worker(sup_id, opts, timeout) do
    Logger.debug(
      "SuperWorker, Supervisor, starting standalone child process with options: #{inspect(opts)}"
    )

    with {:ok, opts} <- Worker.check_standalone_options(opts ++ [type: :standalone, parent: nil]),
         {:ok, pid} <- verify_and_get_pid(sup_id, opts.id) do
      ApiHelper.call_api(pid, :start_worker, opts, timeout)
    else
      error ->
        Logger.error(
          "SuperWorker, Supervisor, cannot add standalone worker, something happened: #{inspect(error)}, options: #{inspect(opts)}"
        )

        error
    end
  end

  defp do_add_group_worker(sup_id, group_id, opts, timeout)
       when group_id != nil do
    Logger.debug(
      "SuperWorker, Supervisor, starting worker group(#{inspect(group_id)}) process with options: #{inspect(opts)}"
    )

    with {:ok, opts} <- Worker.check_group_options(opts ++ [type: :group, parent: group_id]),
         {:ok, pid} <- verify_and_get_pid(sup_id, opts.id) do
      Logger.debug(
        "SuperWorker, Supervisor, start call :start_worker api with opts: #{inspect(opts)}"
      )

      ApiHelper.call_api(pid, :start_worker, opts, timeout)
    else
      error ->
        Logger.error(
          "SuperWorker, Supervisor, cannot add group worker: #{inspect(error)}, options: #{inspect(opts)}"
        )

        error
    end
  end

  defp do_add_chain_worker(sup_id, chain_id, opts, timeout) do
    Logger.debug("SuperWorker, Supervisor, starting child process with options: #{inspect(opts)}")

    with {:ok, opts} <- Worker.check_chain_options(opts ++ [type: :chain, parent: chain_id]),
         {:ok, pid} <- verify_and_get_pid(sup_id, opts.id) do
      ApiHelper.call_api(pid, :start_worker, opts, timeout)
    else
      error ->
        Logger.error(
          "SuperWorker, Supervisor, cannot add chain worker: #{inspect(error)}, options: #{inspect(opts)}"
        )

        error
    end
  end

  defp convert_gen_server_specs(f, opts) do
    {:ok, gen_sever_options = %{mfa: mfa}} =
      SuperWorker.ConfigLoader.Parser.convert_regular_child_spec(f)

    default_options =
      gen_sever_options
      |> Map.delete(:mfa)
      |> Map.delete(:options)
      |> Map.to_list()

    Keyword.merge([{:fun, mfa} | default_options], opts)
  end
end
