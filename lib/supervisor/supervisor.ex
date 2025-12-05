defmodule SuperWorker.Supervisor do
  @moduledoc """
  Documentation for `SuperWorker.Supervisor`.
  This module is new model supervisor.
  Better support for modern applications.
  That is an all-in-one supervisor for Elixir application.
  Easy to identify the worker for directly communicating with it.

  New supervisor supports the following features:
  - Group processes
  - Chain processes
  - Standalone processes

  ## Group processes
  Group processes are a set of processes that are started together.
  If one of the processes is crashed, depending on the restart strategy of group,
  only that process or all the processes will be restarted.
  Each group has a separated restart strategy.

  ## Chain processes
  Chain processes are a set of processes that support for chain processing.
  Each process in a chain has order to process data.
  The output of the previous process is passed to the next process.

  ## Standalone processes
  Standalone processes are independent processes that are started separately.
  Each process has its own restart strategy.

  All type of processes can be started in parallel & can be stopped individually or in a group.

  The restart strategy only works when the process is crashed, for normal exit, shutdown, or terminated it will be ignored.

  ## Examples
  ```elixir
  # Start a supervisor with 2 partitions & 2 groups:
  alias SuperWorker.Supervisor, as: Sup

  # Config for supervisor
  opts = [id: :sup1, num_partitions: 2, link: false]

  # Start supervisor
  Sup.start_with_config(opts)

  # Add group in runtime, you also can add group in config.
  Sup.add_group(:sup1, [id: :group1, restart_strategy: :one_for_all])
  Sup.add_group_worker(:sup1, :group1, {Dev, :task, [15]}, [id: :g1_1])

  Sup.add_group(:sup1, [id: :group2, restart_strategy: :one_for_one])
  Sup.add_group_worker(:sup1, :group2, fn ->
    receive do
      {:ping, ref, from} ->
        send(from, {:pong, ref})
      msg ->
        :ok
    end
  end, [id: :g2_2])

  ref = make_ref()
  Sup.send_to_group_worker(:sup1, :group2, :g2_2, {:ping, ref, self()})

  receive do
    {:pong, ^ref} ->
      :ok
  end
  ```

  Supervisor can add directly to other supervisor or
  by add config to config file or self start with start/start_link/startwith_config function.
  """

  use GenServer, restart: :permanent, shutdown: 5_000

  defstruct [
    # id of supervisor.
    :id,
    # number of partitions, default is number of online schedulers
    :num_partitions,
    # link the supervisor to the caller
    link: true,
    # list of pid or callback function, for reporting worker crashed or worker finished.
    # reserve for the future, not implemented.
    report_to: [],
    # for internal use only.
    partitions: %{},
    # storage data for supervisor, workers/groups/chains, for internal use only.
    table: nil
  ]

  @type t :: %__MODULE__{
          id: atom(),
          num_partitions: pos_integer(),
          link: boolean(),
          report_to: [pid() | {atom(), pid()}],
          partitions: %{pos_integer() => pid()},
          table: nil
        }

  @default_time 3_000

  alias __MODULE__

  alias SuperWorker.Supervisor.{Worker, Group, Chain}

  alias SuperWorker.Supervisor.{Utils, ApiHelper, Message, Validator, Db, Partition}

  require Logger

  @doc """
  start_link for using supervisor as child in other supervisor or link to current process.
  """
  @spec start_link(t()) :: {:ok, pid} | {:error, any()}
  def start_link(%Supervisor{} = options) do
    if running?(options.id) do
      {:error, {:already_started, options.id}}
    else
      name = options.id || __MODULE__
      GenServer.start_link(__MODULE__, options, name: name)
    end
  end

  @doc """
  Work like `start_link/1` with default options.
  """
  @spec start_link() :: {:ok, pid} | {:error, any()}
  def start_link() do
    supervisor = %Supervisor{
      id: __MODULE__,
      num_partitions: Utils.get_default_schedulers()
    }

    start_link(supervisor)
  end

  @doc """
  Start supervisor as independent process (no link process).
  """
  @spec start(t()) :: {:ok, pid} | {:error, any()}
  def start(%Supervisor{} = options) do
    if running?(options.id) do
      {:error, {:already_started, options.id}}
    else
      name = options.id || __MODULE__
      GenServer.start(__MODULE__, options, name: name)
    end
  end

  @doc """
  Start supervisor run as independent process with default options.
  """
  @spec start() :: {:ok, pid} | {:error, any()}
  def start() do
    supervisor = %Supervisor{
      id: __MODULE__,
      num_partitions: Utils.get_default_schedulers()
    }

    start(supervisor)
  end

  @doc """
  Start supervisor with configurations (Keyword).
  For run standalone, please set option :link to false.
  For link to other process (not current process), please set link to pid of that process.
  result format: {:ok, pid} or {:error, reason}
  """
  @spec start_with_config(
          id: atom(),
          link: boolean() | pid(),
          num_partitions: integer(),
          report_to: list()
        ) :: {:ok, pid} | {:error, any()}
  def start_with_config(config) when is_list(config) do
    with {:ok, supervisor} <- Validator.validate_and_convert(config),
         false <- running?(supervisor.id) do
      do_start_supervisor(supervisor)
    else
      true ->
        Logger.error(
          "SuperWorker, Supervisor, supervisor has id in #{inspect(config)} is already running."
        )

        {:error, :already_running}

      {:error, _} = error ->
        Logger.error("SuperWorker, Supervisor, Error when starting supervisor: #{inspect(error)}")

        error
    end
  end

  @doc """
  Check if supervisor is running.
  """
  @spec running?(atom()) :: boolean()
  def running?(id) when is_atom(id) do
    match?({:ok, _}, get_pid(id))
  end

  @doc """
  Stop supervisor. Type of shutdown is using for reason of exit in Process.exit function.
  Type of shutdown:
  - `:normal` supervisor will send a message to worker for graceful shutdown. Not support for spawn process by function.
  - `:kill` supervisor will kill worker.
  """
  @spec stop(atom(), shutdown_type :: atom(), timeout :: non_neg_integer()) ::
          {:ok, atom()} | {:error, any()}
  def stop(sup_id, shutdown_type \\ :kill, timeout \\ @default_time) do
    Logger.debug(
      "SuperWorker, Supervisor, stop supervisor: #{inspect(sup_id)},  shutdown type: #{inspect(shutdown_type)}"
    )

    with true <- running?(sup_id) do
      GenServer.call(sup_id, {:stop_supervisor, shutdown_type}, timeout)
    else
      false ->
        {:error, :not_running}
    end
  end

  @doc """
  Get supervisor id in current process (not support for GenServer worker).
  """
  @spec get_my_supervisor() :: atom() | nil
  def get_my_supervisor() do
    Process.get({:supervisor, :sup_id})
  end

  ## Standalone worker api ##

  @doc """
  Add a standalone worker process to the supervisor.
  function for start worker can be a function or a {module, function, arguments} or a GenServer.
  Standalone worker is run independently from other workers follow :one_to_one strategy.
  If worker crashes, it will check the restart strategy of worker then act accordingly.
  """
  @spec add_standalone_worker(
          atom(),
          {module(), atom(), list()} | fun() | module() | {module(), list()},
          list(),
          non_neg_integer()
        ) ::
          {:ok, atom()} | {:error, any()}
  def add_standalone_worker(sup_id, mfa_or_fun, options \\ [], timeout \\ @default_time)

  def add_standalone_worker(sup_id, {m, f, a} = mfa, options, timeout)
      when is_list(options) and is_atom(m) and is_atom(f) and is_list(a) do
    do_add_worker(
      sup_id,
      [fun: mfa, type: :standalone, parent: nil] ++ options,
      timeout
    )
  end

  def add_standalone_worker(sup_id, fun, options, timeout)
      when is_list(options) and is_function(fun, 0) do
    do_add_worker(sup_id, [fun: {:fun, fun}, type: :standalone, parent: nil] ++ options, timeout)
  end

  def add_standalone_worker(sup_id, {:fun, fun} = f, options, timeout)
      when is_list(options) and is_function(fun, 0) do
    do_add_worker(
      sup_id,
      [fun: f, type: :standalone, parent: nil] ++ options,
      timeout
    )
  end

  def add_standalone_worker(sup_id, {genserver_module, _} = f, options, timeout)
      when is_list(options) and is_atom(genserver_module) do
    options = convert_gen_server_specs(f, options)

    do_add_worker(
      sup_id,
      [type: :standalone, parent: nil] ++ options,
      timeout
    )
  end

  def add_standalone_worker(sup_id, genserver_module, opts, timeout)
      when is_list(opts) and is_atom(genserver_module) do
    add_standalone_worker(sup_id, {genserver_module, []}, opts, timeout)
  end

  @doc """
  Send data directly to the worker standalone in the supervisor.
  """
  @spec send_to_standalone_worker(atom(), any(), any(), non_neg_integer()) ::
          :ok | {:error, term()}
  def send_to_standalone_worker(sup_id, worker_id, data, timeout \\ @default_time) do
    Logger.debug(
      "SuperWorker, Supervisor, send standalone worker, supervisor: #{inspect(sup_id)},  worker id: #{inspect(worker_id)}"
    )

    get_partition_and_send(sup_id, :send_to_standalone_worker, {worker_id, data}, timeout)
  end

  @doc """
  Remove standalone worker from supervisor.
  """
  @spec remove_standalone_worker(atom(), any(), non_neg_integer()) ::
          :ok | {:error, term()}
  def remove_standalone_worker(sup_id, worker_id, timeout \\ @default_time) do
    Logger.debug(
      "SuperWorker, Supervisor, remove standalone worker, supervisor: #{inspect(sup_id)},  worker id: #{inspect(worker_id)}"
    )

    get_partition_and_send(sup_id, :remove_standalone_worker, worker_id, timeout)
  end

  @doc """
  get pid of standalone worker
  """
  @spec get_pid_standalone_worker(atom(), any(), non_neg_integer()) ::
          {:ok, pid()} | {:error, term()}
  def get_pid_standalone_worker(sup_id, worker_id, timeout \\ @default_time) do
    Logger.debug(
      "SuperWorker, Supervisor, get pid of standalone worker, supervisor: #{inspect(sup_id)},  worker id: #{inspect(worker_id)}"
    )

    get_partition_and_send(sup_id, :get_worker_pid, {worker_id, {:standalone, nil}}, timeout)
  end

  ## Group APIs ##

  @doc """
  Add a  worker to a group in the supervisor.
  Function's options follow `Worker` module.
  Support worker is function (mfa, anonymous function) or GenServer
  """
  @spec add_group_worker(
          atom(),
          atom(),
          {module(), atom(), list()} | fun() | module() | {module(), list()},
          list(),
          non_neg_integer()
        ) ::
          {:ok, atom()} | {:error, any()}
  def add_group_worker(sup_id, group_id, mfa_or_fun, opts, timeout \\ @default_time)

  def add_group_worker(sup_id, group_id, {m, f, a} = mfa, options, timeout)
      when is_list(options) and is_atom(m) and is_atom(f) and is_list(a) and group_id != nil do
    do_add_worker(
      sup_id,
      [fun: mfa, type: :group, parent: group_id] ++ options,
      timeout
    )
  end

  def add_group_worker(sup_id, group_id, fun, options, timeout)
      when is_list(options) and is_function(fun, 0) do
    do_add_worker(
      sup_id,
      [fun: {:fun, fun}, type: :group, parent: group_id] ++ options,
      timeout
    )
  end

  def add_group_worker(sup_id, group_id, module, options, timeout)
      when is_atom(module) and group_id != nil do
    add_group_worker(sup_id, group_id, {module, []}, options, timeout)
  end

  def add_group_worker(sup_id, group_id, {module, _init_arg} = worker, options, timeout)
      when is_atom(module) and group_id != nil do
    options = convert_gen_server_specs(worker, options)

    do_add_worker(
      sup_id,
      [type: :group, parent: group_id] ++ options,
      timeout
    )
  end

  @doc """
  Add a group to the supervisor.
  Group's options follow docs in `Group` module.
  """
  @spec add_group(atom(), list(), non_neg_integer()) :: {:ok, atom()} | {:error, any()}
  def add_group(sup_id, options, timeout \\ @default_time) do
    Logger.debug(
      "SuperWorker, Supervisor, add group, supervisor: #{inspect(sup_id)},  group options: #{inspect(options)}"
    )

    with {:ok, group = %Group{}} <- Group.check_options(options) do
      group = %Group{group | supervisor: sup_id}

      get_partition_and_send(sup_id, :add_group, group, timeout)
    end
  end

  @doc """
  Send data to all workers in a group.
  """
  @spec broadcast_to_group(atom(), atom(), any(), non_neg_integer()) :: :ok | {:error, any()}
  def broadcast_to_group(sup_id, group_id, data, timeout \\ @default_time) do
    Logger.debug(
      "SuperWorker, Supervisor, send data to all workers in group, supervisor: #{inspect(sup_id)},  group id: #{inspect(group_id)}"
    )

    get_partition_and_send(sup_id, :broadcast_to_group, {group_id, data}, timeout)
  end

  @doc """
  Send data to all workers in current group of worker.
  Using for communite between workers in the same group.
  """
  @spec broadcast_to_my_group(any()) :: :ok | {:error, any()}
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
  get current group id of worker.
  """
  @spec get_my_group() :: atom() | nil
  def get_my_group() do
    Process.get({:supervisor, :group_id})
  end

  @doc """
  Send data to a worker in the group.
  """
  @spec send_to_group_worker(atom(), any(), any(), any(), non_neg_integer()) ::
          :ok | {:error, any()}
  def send_to_group_worker(sup_id, group_id, worker_id, data, timeout \\ @default_time) do
    Logger.debug(
      "SuperWorker, Supervisor, send data to group worker, supervisor: #{inspect(sup_id)},  group id: #{inspect(group_id)}"
    )

    get_partition_and_send(sup_id, :send_to_group, {group_id, worker_id, data}, timeout)
  end

  @doc """
  Send data to a random worker in the group.
  """
  @spec send_to_group_random(atom(), any(), any(), non_neg_integer()) ::
          :ok | {:error, any()}
  def send_to_group_random(sup_id, group_id, data, timeout \\ @default_time) do
    with true <- running?(sup_id),
         {:ok, pid} <- query_target_partition(sup_id, group_id) do
      ApiHelper.call_api(pid, :send_to_group_random, {group_id, data}, timeout)
    end
  end

  @doc """
  Send data to other worker in the same group.
  """
  @spec send_to_my_group(any(), any()) ::
          :ok | {:error, any()}
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
        send_to_group_worker(sup_id, group_id, worker_id, data)
    end
  end

  @doc """
  Send data to a random worker in the same group.
  """
  @spec send_to_my_group_random(any()) ::
          :ok | {:error, any()}
  def send_to_my_group_random(data) do
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
        send_to_group_random(sup_id, group_id, data)
    end
  end

  @doc """
  remove a worker out of group
  """
  @spec remove_group_worker(atom(), any(), any(), non_neg_integer()) ::
          :ok | {:error, any()}
  def remove_group_worker(sup_id, group_id, worker_id, timeout \\ @default_time) do
    Logger.debug(
      "SuperWorker, Supervisor, remove worker from group, supervisor: #{inspect(sup_id)},  group id: #{inspect(group_id)}, worker id: #{inspect(worker_id)}"
    )

    get_partition_and_send(sup_id, :remove_group_worker, {worker_id, group_id}, timeout)
  end

  @doc """
  remove group
  """
  @spec remove_group(atom(), any(), non_neg_integer()) ::
          :ok | {:error, any()}
  def remove_group(sup_id, group_id, timeout \\ @default_time) do
    Logger.debug(
      "SuperWorker, Supervisor, remove group, supervisor: #{inspect(sup_id)},  group id: #{inspect(group_id)}"
    )

    get_partition_and_send(sup_id, :remove_group, group_id, timeout)
  end

  @doc """
  group is existed
  """
  @spec group_exists?(atom(), any(), non_neg_integer()) ::
          boolean() | {:error, any()}
  def group_exists?(sup_id, group_id, timeout \\ @default_time) do
    Logger.debug(
      "SuperWorker, Supervisor, check group is existed, supervisor: #{inspect(sup_id)},  group id: #{inspect(group_id)}"
    )

    get_partition_and_send(sup_id, :group_exists, group_id, timeout)
  end

  @doc """
  get pid of group worker.
  """
  @spec get_pid_group_worker(atom(), any(), any(), non_neg_integer()) ::
          pid() | {:error, any()}
  def get_pid_group_worker(sup_id, group_id, worker_id, timeout \\ @default_time) do
    Logger.debug(
      "SuperWorker, Supervisor, get pid of group worker, supervisor: #{inspect(sup_id)},  group id: #{inspect(group_id)}, worker id: #{inspect(worker_id)}"
    )

    get_partition_and_send(sup_id, :get_worker_pid, {worker_id, {:group, group_id}}, timeout)
  end

  @doc """
  Restart a worker in group
  """
  @spec restart_group_worker(atom(), any, any, non_neg_integer()) ::
          {:ok, atom()} | {:error, any()}
  def restart_group_worker(sup_id, group_id, worker_id, timeout \\ @default_time) do
    Logger.debug(
      "SuperWorker, Supervisor, restart group worker, supervisor: #{inspect(sup_id)},  group id: #{inspect(group_id)}, worker id: #{inspect(worker_id)}"
    )

    get_partition_and_send(sup_id, :restart_group_worker, {group_id, worker_id}, timeout)
  end

  @doc """
  Restart all workers in group
  """
  @spec restart_group(atom(), any, non_neg_integer()) :: {:ok, atom()} | {:error, any()}
  def restart_group(sup_id, group_id, timeout \\ @default_time) do
    Logger.debug(
      "SuperWorker, Supervisor, restart group , supervisor: #{inspect(sup_id)},  group id: #{inspect(group_id)}"
    )

    get_partition_and_send(sup_id, :restart_group, group_id, timeout)
  end

  ## Chan APIs ##

  @doc """
  Add a worker to the chain in supervisor.
  """
  @spec add_chain_worker(
          atom(),
          atom(),
          {module(), atom(), list()} | fun() | module() | {module(), list()},
          list(),
          non_neg_integer()
        ) ::
          {:ok, atom()} | {:error, any()}
  def add_chain_worker(sup_id, chain_id, mfa_or_fun, opts, timeout \\ @default_time)

  def add_chain_worker(sup_id, chain_id, {m, f, a} = mfa, options, timeout)
      when is_list(options) and is_atom(m) and is_atom(f) and is_list(a) do
    do_add_worker(
      sup_id,
      [fun: mfa, type: :chain, parent: chain_id] ++ options,
      timeout
    )
  end

  def add_chain_worker(sup_id, chain_id, fun, options, timeout)
      when is_list(options) and is_function(fun, 0) do
    do_add_worker(
      sup_id,
      [fun: {:fun, fun}, type: :chain, parent: chain_id] ++ options,
      timeout
    )
  end

  @doc """
  Add a chain to the supervisor.
  Chain's options follow docs in `Chain` module.
  """
  @spec add_chain(atom(), list(), non_neg_integer()) ::
          {:ok, atom()} | {:error, any()}
  def add_chain(sup_id, options, timeout \\ 5_000) do
    Logger.debug(
      "SuperWorker, Supervisor, add chain to supervisor , supervisor: #{inspect(sup_id)},  chain options: #{inspect(options)}"
    )

    with {:ok, chain} <- Chain.check_options(options) do
      %Chain{} = chain
      chain = %{chain | supervisor: sup_id}

      get_partition_and_send(sup_id, :add_chain, chain, timeout)
    end
  end

  @doc """
  Send data to the entry worker in the chain.
  If chain doesn't has any worker, it will be dropped.
  """
  @spec send_to_chain(atom(), any(), any(), non_neg_integer()) ::
          {:ok, any()} | {:error, any()}
  def send_to_chain(sup_id, chain_id, data, timeout \\ @default_time) do
    Logger.debug(
      "SuperWorker, Supervisor, send data to chain, supervisor: #{inspect(sup_id)},  chain: #{inspect(chain_id)}"
    )

    get_partition_and_send(sup_id, :add_data_to_chain, {chain_id, data}, timeout)
  end

  @doc """
  get chain structure from supervisor.
  """
  @spec get_chain(atom(), any(), non_neg_integer()) :: {:ok, Chain.t()} | {:error, any()}
  def get_chain(sup_id, chain_id, timeout \\ @default_time) do
    Logger.debug(
      "SuperWorker, Supervisor, get chain, supervisor: #{inspect(sup_id)},  chain: #{inspect(chain_id)}"
    )

    get_partition_and_send(sup_id, :get_chain, chain_id, timeout)
  end

  @doc """
  remove a worker from chain.
  """
  @spec remove_chain_worker(atom(), any(), any(), non_neg_integer()) ::
          {:ok, any()} | {:error, any()}
  def remove_chain_worker(sup_id, chain_id, worker_id, timeout \\ @default_time) do
    Logger.debug(
      "SuperWorker, Supervisor, remove worker in chain, supervisor: #{inspect(sup_id)},  chain: #{inspect(chain_id)}, worker: #{inspect(worker_id)}"
    )

    get_partition_and_send(sup_id, :remove_chain_worker, {worker_id, chain_id}, timeout)
  end

  @doc """
  remove chain.
  """
  @spec remove_chain(atom(), any(), non_neg_integer()) ::
          {:ok, any()} | {:error, any()}
  def remove_chain(sup_id, chain_id, timeout \\ @default_time) do
    Logger.debug(
      "SuperWorker, Supervisor, remove chain, #{inspect(sup_id)}, #{inspect(chain_id)}"
    )

    get_partition_and_send(sup_id, :remove_chain, chain_id, timeout)
  end

  @doc """
  get pid of chain worker
  """
  @spec get_pid_chain_worker(atom(), any(), any(), non_neg_integer()) ::
          {:ok, any()} | {:error, any()}
  def get_pid_chain_worker(sup_id, chain_id, worker_id, timeout \\ @default_time) do
    Logger.debug(
      "SuperWorker, Supervisor, get pid of chain worker, #{inspect(sup_id)}, #{inspect(chain_id)}, #{inspect(worker_id)}"
    )

    get_partition_and_send(sup_id, :get_worker_pid, {worker_id, {:chain, chain_id}}, timeout)
  end

  ## Call Backs

  @impl true
  def init(supervisor = %Supervisor{}) do
    with {:ok, _} <- link_process(supervisor) do
      # Initialize the table for the supervisor
      table = Db.init(supervisor.id)

      supervisor = %{supervisor | table: table}

      Logger.debug("SuperWorker, Supervisor, create table for #{inspect(supervisor.id)} done}")

      Db.put_sup_info(supervisor.table, :master, supervisor)

      partitions = Partition.init_additional_partitions(supervisor)
      list_partition_ids = Map.keys(partitions)

      Enum.each(partitions, fn {_, pid} ->
        msg = Message.new(:partition_list, pid, partitions)
        send(pid, {:internal_api, msg})
      end)

      {:ok, %{supervisor: supervisor, partitions: partitions, partition_ids: list_partition_ids}}
    else
      failed ->
        Logger.error(
          "SuperWorker, Supervisor, failed to init partitions/link process, reason: #{inspect(failed)}"
        )

        {:error, :failed_to_init}
    end
  end

  @impl true
  def handle_call({:query_target_partition, data}, _from, state) do
    order = Utils.get_hash_order(data, state.supervisor.num_partitions)
    pid = Map.get(state.partitions, order, {:error, :not_found_partition})

    {:reply, pid, state}
  end

  def handle_call({:stop_supervisor, shutdown_type}, _from, state) do
    Enum.each(state.partitions, fn {id, pid} ->
      ApiHelper.internal_call_api_no_reply(pid, :stop_supervisor, shutdown_type)
      Logger.info("SuperWorker, Supervisor, sent stop signal to partition: #{id}")
    end)

    {:reply, :ok, state}
  end

  @impl true
  def handle_info({:partition_started, partition_id}, state) do
    Logger.debug("SuperWorker, Supervisor, Partition started: #{partition_id}")
    {:noreply, state}
  end

  @impl true
  def handle_info({:partition_stopped, partition_id}, state) do
    Logger.debug("SuperWorker, Supervisor, Partition stopped: #{partition_id}")

    stopped_partitions =
      [partition_id | Map.get(state, :stopped_partitions, [])]

    if length(stopped_partitions) == state.supervisor.num_partitions do
      {:stop, :normal, state}
    else
      {:noreply, Map.put(state, :stopped_partitions, stopped_partitions)}
    end
  end

  ## Private Functions

  defp query_target_partition(supervisor_id, data) do
    # Implement logic to get target partition

    try do
      result = GenServer.call(supervisor_id, {:query_target_partition, data})
      {:ok, result}
    catch
      :exit, reason ->
        Logger.error(
          "SuperWorker, Supervisor, failed to query target partition, reason: #{inspect(reason)}"
        )

        {:error, :failed_to_query}

      :error, reason ->
        Logger.error(
          "SuperWorker, Supervisor, failed to query target partition, reason: #{inspect(reason)}"
        )

        {:error, :failed_to_query}
    end
  end

  defp link_process(supervisor = %Supervisor{}) do
    # Support for link to remote process
    cond do
      is_pid(supervisor.link) ->
        Process.link(supervisor.link)
        {:ok, :linked}

      supervisor.link == true || supervisor.link == false ->
        {:ok, :linked}

      true ->
        {:error, {:invalid_link, supervisor.link}}
    end
  end

  # Start the supervisor main processes.
  defp do_start_supervisor(opts = %Supervisor{}) do
    Logger.debug("SuperWorker, Supervisor, starting supervisor with options: #{inspect(opts)}")

    # Start main process of the supervisor
    case opts.link do
      true ->
        Logger.debug("SuperWorker, Supervisor, starting supervisor with link.")
        opts = Map.put(opts, :linked_pids, self())
        start_link(opts)

      _ ->
        Logger.debug("SuperWorker, Supervisor, starting supervisor without link.")
        start(opts)
    end
  end

  defp get_pid(id) when is_atom(id) do
    case Process.whereis(id) do
      nil ->
        {:error, :not_running}

      pid ->
        {:ok, pid}
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

  defp get_partition_and_send(sup_id, api, params, timeout) do
    get_partition_and_send(sup_id, api, params, params, timeout)
  end

  defp get_partition_and_send(sup_id, api, params, partition_info, timeout) do
    with true <- running?(sup_id),
         {:ok, pid} <- query_target_partition(sup_id, partition_info) do
      Logger.debug(
        "SuperWorker, Supervisor, sending api #{inspect(api)} to partition #{inspect(pid)}"
      )

      ApiHelper.call_api(pid, api, params, timeout)
    else
      false ->
        Logger.error("SuperWorker, Supervisor, not found supervisor for send api")
        {:error, :supervisor_not_found}

      {:error, reason} = error ->
        Logger.error(
          "SuperWorker, Supervisor, error when processing api #{inspect(api)}, reason: #{inspect(reason)}"
        )

        error
    end
  end

  @spec do_add_worker(atom(), list(), integer()) ::
          {:ok, any()} | {:error, any()}
  defp do_add_worker(sup_id, options, timeout) do
    Logger.debug("SuperWorker, Supervisor, starting worker with options: #{inspect(options)}")

    with {:ok, worker} <- Worker.from_config(options) do
      get_partition_and_send(sup_id, :start_worker, worker, {worker.parent, worker.id}, timeout)
    end
  end
end
