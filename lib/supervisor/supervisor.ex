defmodule SuperWorker.Supervisor do
  @moduledoc """
  Documentation for `SuperWorker.Supervisor`.
  This module is a new model for the Supervisor module.
  Supervisor supports the following features:
  - Group processes
  - Chain processes
  - Freedom processes

  ## Group processes
  Group processes are a set of processes that are started together. If one of the processes dies, all the processes in the group will be stopped.

  ## Chain processes
  Chain processes are a set of processes that are started one after another. The output of the previous process is passed to the next process.

  ## Freedom processes
  Freedom processes are independent processes that are started separately.

  All type of processes can be started in parallel & can be stopped individually or in a group.

  ## Examples
    # Start a supervisor with 2 partitions & 2 groups:
    alias SuperWorker.Supervisor, as: Sup
    opts = [id: :sup1, number_of_partitions: 2, link: false]
    Sup.start(opts)

    Sup.add_group(:sup1, [id: :group1, restart_strategy: :one_for_all])
    Sup.add_group_worker(:sup1, :group1, {Dev, :task, [15]}, [id: :g1_1])

    Sup.add_group(:sup1, [id: :group2, restart_strategy: :one_for_all])
    Sup.add_group_worker(:sup1, :group2, fn ->
      receice do
      msg ->
        :ok
      end
    end, [id: :g2_2])
  """

  require Logger

  alias SuperWorker.Supervisor.{Group, Chain, Worker, Message}
  alias :ets, as: Ets

  import SuperWorker.Supervisor.Utils

  defstruct [
    :id, # supervisor id
    :owner, # owner of the supervisor
    :number_of_partitions, # number of partitions, default is number of online schedulers
    link: true, # link the supervisor to the caller
    report_to: [] # list of pid or callback function, for reporting worker crashed or worker finished.
  ]

  @sup_params [:id, :number_of_partitions, :link]

  @me __MODULE__

  # Default timeout (miliseconds) for API calls.
  @default_time 5_000

  # List message from api.
  @api_messages [:start_worker, :get_group, :remove_group_worker, :restart_group_worker,
    :get_chain, :send_to_group, :send_to_group_random, :add_data_to_chain, :send_to_worker,
     :remove_group_worker, :add_group, :add_chain, :stop]

  # List internal message.
  @internal_messages []

  ## Public APIs

  @doc """
  Start supervisor for run standalone please set option :link to false.
  result format: {:ok, pid} or {:error, reason}
  """
  @spec start([id: atom(), link: boolean() | pid(), number_of_partitions: integer(),
    report_to: list()]) :: {:ok, pid} | {:error, any()}
  def start(opts, timeout \\ 5_000) when is_list(opts) do
    with {:ok, opts} <- check_opts(opts),
      false <- is_running?(opts.id) do
        start_supervisor(opts, timeout)
    else
      true ->
        Logger.error("Supervisor is already running.")
        {:error, :already_running}
      {:error, _} = error ->
        Logger.error("Error when starting supervisor: #{inspect error}")
        error
    end
  end

  @doc """
  Stop supervisor.
  Type of shutdown:
  - :normal supervisor will send a message to worker for graceful shutdown. Not support for spawn process by function.
  - :kill supervisor will kill worker.
  """
  @spec stop(atom(), shutdown_type :: atom(), timeout :: integer()) :: {:ok, atom()} | {:error, any()}
  def stop(sup_id, shutdown_type \\ :kill, timeout \\ @default_time) do
    case get_pid(sup_id) do
      {:error, _} = err ->
        Logger.error("Supervisor is not running.")
        err
      {:ok, pid} ->
        Logger.debug("Stopping supervisor: #{inspect pid}, shutdown type: #{inspect shutdown_type}")
        call_api(pid, :stop, shutdown_type, timeout)
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
  Add a standalone worker process to the supervisor.
  function for start worker can be a function or a {module, function, arguments}.
  Standalone worker is run independently from other workers follow :one_to_one strategy.
  If worker crashes, it will check the restart strategy of worker then act accordingly.
  """
  @spec add_standalone_worker(atom(), {module(), atom(), list()} | fun(), list(), integer()) :: {:ok, atom()} | {:error, any()}
  def add_standalone_worker(sup_id, mfa_or_fun, opts, timeout \\ @default_time)
  def add_standalone_worker(sup_id, {m, f, a} = mfa, opts, timeout)
   when is_list(opts) and is_atom(m) and is_atom(f) and is_list(a) do
    do_add_worker(sup_id, :standalone, [{:fun, mfa} | opts], timeout)
  end
  def add_standalone_worker(sup_id, fun, opts, timeout) when is_list(opts) and is_function(fun, 0) do
    do_add_worker(sup_id, :standalone, [{:fun, {:fun, fun}} | opts], timeout)
  end

  @doc """
  Add a  worker to a group in the supervisor.
  Function's options follow `Worker` module.
  """
  @spec add_group_worker(atom(), atom(), {module(), atom(), list()} | fun(), list(), integer()) :: {:ok, atom()} | {:error, any()}
  def add_group_worker(sup_id, group_id, mfa_or_fun, opts, timeout \\ @default_time)
  def add_group_worker(sup_id, group_id, {m, f, a} = mfa, opts, timeout)
   when is_list(opts) and is_atom(m) and is_atom(f) and is_list(a) do
    do_add_worker(sup_id, {:group_id, group_id}, [{:fun, mfa} | opts], timeout)
  end
  def add_group_worker(sup_id, group_id, fun, opts, timeout) when is_list(opts) and is_function(fun, 0) do
    do_add_worker(sup_id, {:group_id, group_id}, [{:fun, {:fun, fun}} | opts], timeout)
  end

  @doc """
  Add a worker to the chain in supervisor.
  """
  @spec add_chain_worker(atom(), atom(), {module(), atom(), list()} | fun(), list(), integer()) :: {:ok, atom()} | {:error, any()}
  def add_chain_worker(sup_id, chain_id, mfa_or_fun, opts, timeout \\ @default_time)
  def add_chain_worker(sup_id, chain_id, {m, f, a} = mfa, opts, timeout)
   when is_list(opts) and is_atom(m) and is_atom(f) and is_list(a) do
    do_add_worker(sup_id, {:chain_id, chain_id}, [ {:fun, mfa} | opts], timeout)
  end
  def add_chain_worker(sup_id, chain_id, fun, opts, timeout) when is_list(opts) and is_function(fun, 0) do
    do_add_worker(sup_id, {:chain_id, chain_id}, [{:fun, {:fun, fun}} | opts], timeout)
  end

  @doc """
  Add a group to the supervisor.
  Group's options follow docs in `Group` module.
  """
  @spec add_group(atom(), list(), integer()) :: {:ok, atom()} | {:error, any()}
  def add_group(sup_id, opts, timeout \\ @default_time) do
    with {:ok, group} <- Group.check_options(opts),
      true <- is_running?(sup_id) do
      case Ets.lookup(get_table_name(sup_id), {:group, group.id}) do
        [] ->
          with {:ok, pid} <- get_host_partition(sup_id, group.id) do
            call_api(pid, :add_group, group, timeout)
          end
        _ ->
          {:error, :already_exists}
      end
    else
      false ->
        Logger.error("Supervisor is not running.")
        {:error, :not_running}
      {:error, _} = error ->
        Logger.error("Error when adding group: #{inspect error}")
        error
    end
  end

  @doc """
  get group structure from supervisor.
  """
  @spec get_group(atom(), atom(), integer()) :: {:ok, map()} | {:error, any()}
  def get_group(sup_id, group_id, timeout \\ @default_time) do
    with {:ok, pid} <- verify_and_get_pid(sup_id, group_id) do
        call_api(pid, :get_group, group_id, timeout)
    end
  end

  @doc """
  Add a chain to the supervisor.
  Chain's options follow docs in `Chain` module.
  """
  def add_chain(sup_id, opts, timeout \\ 5_000) do
    with {:ok, chain} <- Chain.check_options(opts),
      true <- is_running?(sup_id) do
        case Registry.lookup(sup_id, {:chain, chain.id}) do
          [] ->
            with {:ok, pid} <- get_host_partition(sup_id, chain.id) do
              call_api(pid, :add_chain, chain, timeout)
            end
          _ ->
            {:error, :already_exists}
        end
    end
  end

  @doc """
  Send data to the entry worker in the chain.
  If chain doesn't has any worker, it will be dropped.
  """
  def send_to_chain(sup_id, chain_id, data, timeout \\ @default_time) do
    with {:ok, pid} <- verify_and_get_pid(sup_id, chain_id) do
      call_api(pid, :add_data_to_chain, {chain_id, data}, timeout)
    end
  end

  @doc """
  Send data directly to the worker (standalone, group, chain) in the supervisor.
  """
  def send_to_worker(sup_id, worker_id, data, timeout \\ @default_time) do
    with true <- is_running?(sup_id),
     [{pid, _}] <- Registry.lookup(sup_id, {:worker, worker_id}) do
        call_api(pid, :send_to_worker, {worker_id, data}, timeout)
    end
  end

  @doc """
  Send data to all workers in a group.
  """
  def broadcast_to_group(sup_id, group_id, data, timeout \\ @default_time) do
    with {:ok, pid} <- verify_and_get_pid(sup_id, group_id) do
      call_api(pid, :broadcast_to_group, {group_id, data}, timeout)
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
        Logger.error("Group not found.")
        {:error, :not_found}
      sup_id == nil ->
        Logger.error("Supervisor not found.")
        {:error, :not_found}
      true ->
        broadcast_to_group(sup_id, group_id, data)
    end
  end

  @doc """
  Send data to a worker in the group.
  """
  def send_to_group(sup_id, group_id, worker_id, data, timeout \\ @default_time) do
    with {:ok, pid} <- verify_and_get_pid(sup_id, group_id) do
      call_api(pid, :send_to_group, {group_id, worker_id, data}, timeout)
    end
  end

  @doc """
  Send data to a random worker in the group.
  """
  def send_to_group_random(sup_id, group_id, data, timeout \\ @default_time) do
    with {:ok, pid} <- verify_and_get_pid(sup_id, group_id) do
      call_api(pid, :send_to_group_random, {group_id, data}, timeout)
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
        Logger.error("Group not found.")
        {:error, :not_found}
      sup_id == nil ->
        Logger.error("Supervisor not found.")
        {:error, :not_found}
      true ->
        send_to_group(sup_id, group_id, worker_id, data)
    end
  end

  def send_to_my_group_random(data) do
    group_id = get_my_group()
    sup_id = get_my_supervisor()

    cond do
      group_id == nil ->
        Logger.error("Group not found.")
        {:error, :not_found}
      sup_id == nil ->
        Logger.error("Supervisor not found.")
        {:error, :not_found}
      true ->
        send_to_group_random(sup_id, group_id, data)
    end
  end

  def get_my_group() do
    Process.get({:supervisor, :group_id})
  end

  def get_my_supervisor() do
    Process.get({:supervisor, :sup_id})
  end

  def get_chain(sup_id, chain_id, timeout \\ @default_time) do
    with {:ok, pid} <- verify_and_get_pid(sup_id, chain_id) do
        call_api(pid, :get_chain, chain_id, timeout)
    end
  end

  def remove_group_worker(sup_id, group_id, worker_id, timeout \\ @default_time) do
    with {:ok, pid} <- verify_and_get_pid(sup_id, group_id) do
        call_api(pid, :remove_group_worker, {worker_id, group_id}, timeout)
    end
  end

  ## Internal public functions

  def init(opts, ref) do
    state = %{
      groups: MapSet.new(), # storage for group processes
      chains: MapSet.new(), # storage for chain processes
      standalone: MapSet.new(), # storage for standalone processes
      id: opts.id, # supervisor id
      owner: opts.owner,
      data_table: get_table_name(opts.id),
      master: opts.id,
      prefix: "[#{inspect opts.id}, master]"
    }

    # Register the supervisor process.
    Process.register(self(), get_master_id(opts.id))

    # Link to remote pid if link is a pid.
    # Other cases, process at start with/without link at start function.
    if is_pid(opts.link) do
      Process.link(opts.link)
    end

    # Create Registry for map worker/group/chain id to pid.
    {:ok, _} = Registry.start_link(keys: :duplicate, name: opts.id, partitions: opts.number_of_partitions)

    Registry.register(state.master, :master, opts.number_of_partitions)

    # Store group, chain, workers in Ets
    create_table(state.data_table)

    Ets.insert(state.data_table, {:number_of_partitions, opts.number_of_partitions})
    Ets.insert(state.data_table, {:owner, opts.owner})
    Ets.insert(state.data_table, {:master, opts.id})

    list_partitions = init_additional_partitions(opts)

    state =
      state
      |> Map.put(:partitions , list_partitions)
      |> Map.put(:role, :master)

    Logger.debug("Supervisor #{inspect state.id} initialized: #{inspect state}")

    api_response(ref, {:ok, self()})

    # Start the main loop
    main_loop(state, opts)
  end

  ## Private functions

  defp init_partition(opts) do
    # TO-DO: Implement partition supervisor.

    state = %{
      groups: %{}, # storage for group processes
      chains: %{}, # storage for chain processes
      standalone: %{}, # storage for standalone processes
      ref_to_id: %{}, # storage for refs to id & type
      id: opts.id, # supervisor id
      owner: opts[:owner],
      role: :partition,
      master: opts.master,
      number_of_partitions: opts.number_of_partitions,
      data_table: get_table_name(opts.master),
      prefix: "[#{inspect opts.master}, #{inspect opts.id}]"
    }

    # Start the main loop
    pid = spawn_link(@me, :main_loop, [state, opts])
    Process.register(pid, state.id)

    Logger.debug("#{inspect state.prefix} initialized, pid: #{inspect pid}")

    Registry.register(state.master, {:partition , state.id}, [])

    Ets.insert(state.data_table, {{:partition, state.id},  pid})

    {:ok, opts.id, pid}
  end

  defp init_additional_partitions(opts) do
    partitions = opts.number_of_partitions

    Enum.map(0..partitions - 1, fn i ->
      Logger.debug("[#{inspect opts.id}] add partition: #{inspect i}")
      opts
      |> Map.put(:master, opts.id)
      |> Map.put(:id, String.to_atom("#{Atom.to_string(opts.id)}_#{i}"))
      |> Map.put(:role, :partition)
      |> init_partition()
    end)
  end

  # Main loop for the supervisor & partition.
  def main_loop(state, sup_opts) do
    receive do
      {msg_type, _, _} = msg when msg_type in @api_messages ->
        Logger.debug("#{inspect state.prefix} received a api message: #{inspect(msg)}")
        process_api_message(state, sup_opts, msg)

      {:DOWN, _ref, :process, pid, reason} = msg ->
        Logger.debug("#{inspect state.prefix} Worker died: #{inspect(pid)}, reason: #{inspect(reason)}")
        process_worker_down(state, sup_opts, msg)

      {:stop_partition, type} ->
        Logger.info("#{inspect state.prefix} Stopping supervisor partition, for #{inspect self()}")
        # Stop the supervisor.
        shutdown(state, type)
      unknown ->
        Logger.warning("#{inspect state.prefix} main_loop, unknown message: #{inspect(unknown)}")
        main_loop(state, sup_opts)
    end

    Logger.debug("#{inspect state.prefix} #{inspect self()} main loop exited.")
  end


  defp shutdown(state, :kill) do
    Logger.debug("Shutting down supervisor: #{inspect state.id}")


    # TO-DO: Implement graceful shutdown for worker processes.
    Enum.each(state.groups, fn {_, group} ->
      Group.kill_all_workers(group)
    end)
    Enum.each(state.chains, fn {_, chain} ->
      Chain.kill_all_workers(chain)
    end)
    Enum.each(state.standalone, fn {_, worker} ->
      Process.exit(worker.pid, :kill)
    end)

    {:ok, :brutal_kill}
  end


  # Add new worker to group/chain/standalone.
  defp process_api_message(state, sup_opts, {:start_worker, ref, opts}) do
    runable =
      case opts.type do
        :group ->
          if has_group?(state, opts.group_id) or has_group_worker?(state, opts.group_id, opts.id) do
            true
          else
            :group_not_found_or_worker_already_exists
          end
        :chain ->
          if has_chain?(state, opts.chain_id) or  has_chain_worker?(state, opts.chain_id, opts.id) do
              true
            else
              :chain_not_found_or_worker_already_exists
            end
        :standalone ->
          if has_worker?(state, opts.id) do
            :worker_already_exists
          else
            true
          end
      end
    state =
      if runable == true do # start child process.
        Logger.debug("#{inspect state.prefix} Everything is fine, starting child process with options: #{inspect(opts)}")

        api_response(ref, {:ok, opts.id})
        sup_start_child(state, opts)
      else # not found group or chain, return error to the caller.
        api_response(ref, {:error, runable})
        state
      end

    main_loop(state, sup_opts)
  end

  # Get chain in supervisor and return to the caller.
  defp process_api_message(state, sup_opts, {:get_chain, ref, chain_id}) do
    result =
    case get_group_or_chain(state, chain_id, :chain) do
      {:error, _} = error ->
        Logger.error("#{inspect state.prefix} Not found chain with id #{inspect chain_id}")
        error
      {:ok, _} = res ->
        res
    end
    api_response(ref, result)

    main_loop(state, sup_opts)
  end

  # broadcast a data to all worker in group.
  defp process_api_message(state, sup_opts, {:broadcast_to_group, ref, {group_id, data}}) do
    result =
    case get_group_or_chain(state, group_id, :group) do
      {:ok, group} ->
        Group.broadcast(group, data)
        # TO-DO: Improve response
        :ok
      {:error, _} = error ->
        Logger.error("#{inspect state.prefix} Not found group with id #{inspect group_id}")
        error
    end
    api_response(ref, result)

    main_loop(state, sup_opts)
  end

  # send data directly to worker from api.
  defp process_api_message(state, sup_opts, {:send_to_group, ref, {group_id, worker_id, data}}) do
    with {:ok, group} <- get_group_or_chain(state, group_id, :group),
    {:ok, worker} <- Group.get_worker(group, worker_id) do
      send(worker.pid, data)
      api_response(ref, :ok)
    else
      failed ->
        Logger.error("#{inspect state.id}, send to worker #{inspect worker_id} in group #{inspect group_id}, error: #{inspect failed}")
        api_response(ref, failed)
    end

    main_loop(state, sup_opts)
  end

  # send data to random worker from api.
  defp process_api_message(state, sup_opts, {:send_to_group_random, ref, {group_id, data}}) do
    case get_group_or_chain(state, group_id, :group) do
      {:error, _} = error  ->
        Logger.error("#{inspect state.prefix} Group not found: #{inspect group_id}, error: #{inspect error}")
        api_response(ref, error)
      {:ok, group} ->
          worker_id = Enum.random(group.workers)
          case Group.get_worker(group, worker_id) do
            {:ok, worker} ->
              send(worker.pid, data)
              api_response(ref, :ok)
            {:error, _} = error ->
              Logger.error("#{inspect state.prefix} Not found worker #{inspect worker_id} in group #{inspect group_id}")
              api_response(ref, error)
          end
    end
    main_loop(state, sup_opts)
  end

  # add data to chain from api.
  defp process_api_message(state, sup_opts, {:add_data_to_chain, {from, _} = ref, {chain_id, data}}) do
    case get_group_or_chain(state, chain_id, :chain) do
      {:error, _} = error ->
        Logger.error("#{inspect state.prefix} Chain not found: #{inspect chain_id}")
        api_response(ref, error)
      {:ok, chain} ->
        msg = Message.new(from, nil, data)
        Chain.new_data(chain, msg)
        api_response(ref, :ok)
    end
    main_loop(state, sup_opts)
  end

  defp process_api_message(state, sup_opts,  {:send_to_worker, ref, {worker_id, data}}) do
    case get_group_or_chain(state, worker_id, :not_implement) do
      {:error, _} = error ->
        Logger.error("#{inspect state.prefix} Worker not found: #{inspect worker_id}")
        api_response(ref, error)
      worker ->
        send(worker.pid, data)
        api_response(ref, :ok)
    end
    main_loop(state, sup_opts)
  end

  # remove worker from group.
  defp process_api_message(state, sup_opts, {:remove_group_worker, ref, {worker_id, group_id}}) do
    result =
      case get_group_or_chain(state, group_id, :group) do
        {:ok, group} ->
          Group.remove_worker(group, worker_id)
        {:error, _} = error ->
          Logger.error("#{inspect state.prefix} Group not found: #{inspect group_id}")
          error
      end
    api_response(ref, result)
    main_loop(state, sup_opts)
  end

  defp process_api_message(state, sup_opts, {:restart_group_worker, worker_id , group_id}) do
    Logger.debug("#{inspect state.prefix} Starting worker process, worker id: #{inspect(worker_id)}, group id: #{inspect(group_id)}")

    [{_, group}] = Ets.lookup(state.data_table, {:group, group_id})
    # Restart the worker process.
    Group.restart_worker(group, worker_id)

    main_loop(state, sup_opts)
  end

  # add group from api.
  defp process_api_message(state, sup_opts, {:add_group, ref, group}) do
    case Ets.lookup(state.data_table, {:gorup, group.id}) do
      [_] ->
        Logger.error("#{inspect state.prefix} Group already exists: #{inspect(group.id)}")
        api_response(ref, {:error, :already_exists})
        main_loop(state, sup_opts)
      [] ->
        Logger.debug("#{inspect state.prefix} Adding group: #{inspect(group.id)}")

        # Send the response to the caller.
        api_response(ref, {:ok, group.id})

        state
        |> add_new_group(group)
        |> main_loop(sup_opts)
    end
  end

   # get group info from api.
  defp process_api_message(state, sup_opts, {:get_group, ref, group_id}) do
    result =
      case Ets.lookup(state.data_table, {:group, group_id}) do
        [] ->
          Logger.error("#{inspect state.prefix} Group not found: #{inspect(group_id)}")
          {:error, :not_found}
        [{_, group}] ->
          {:ok, group}
      end
    api_response(ref, result)
    main_loop(state, sup_opts)
  end

  # add chain from api.
  defp process_api_message(state, sup_opts, {:add_chain, ref, chain}) do
    case Ets.lookup(state.data_table, {:chain, chain.id}) do
      [_] ->
        Logger.error("#{inspect state.prefix} Chain already exists: #{inspect(chain.id)}")
        api_response(ref, {:error, :already_exists})
        main_loop(state, sup_opts)
      [] ->
        Logger.debug("#{inspect state.prefix} Adding chain: #{inspect(chain.id)}")
        # Send the response to the caller.
        api_response(ref, {:ok, chain.id})

        state
        |> add_new_chain(chain)
        |> main_loop(sup_opts)
    end
  end

  # Stop supervisor from api.
  defp process_api_message(state, sup_opts, {:stop, ref, type}) do
    Logger.info("#{inspect state.prefix} Stopping supervisor, request from #{inspect ref}")

    # Send shutdown signal to all partitions.
    Enum.each(0..sup_opts.number_of_partitions - 1, fn i ->
      partition_id = get_partition_id(state.id, i)

      case Ets.lookup(state.data_table, {:partition, partition_id}) do
        [{_, pid}] ->
          Logger.debug("#{inspect state.prefix} Sending shutdown signal to partition: #{inspect partition_id}")
          send(pid, {:stop_partition, type})
        _ ->
          Logger.error("#{inspect state.prefix} Supervisor not found: #{inspect partition_id}")
      end
    end)

    # stop worker on master.
    shutdown(state, type)

    # TO-DO: Clean KV store for supervisor.

    api_response(ref, {:ok, :stopped})

    exit(:normal)
  end

  defp process_api_message(state, sup_opts, unknown_msg) do
    Logger.warning("#{inspect state.prefix} Unknown api message: #{inspect(unknown_msg)}")
    main_loop(state, sup_opts)
  end

  defp process_worker_down(state, sup_opts, {:DOWN, _ref, :process, pid, :restart}) do
    Logger.debug("#{inspect state.prefix} Ignore died process (process by other msg): #{inspect(pid)}")
    main_loop(state, sup_opts)
  end
  defp  process_worker_down(state, sup_opts, {:DOWN, ref, :process, pid, reason})  do
    Logger.debug("Child process died: #{inspect(pid)}, ref: #{inspect ref}, reason: #{inspect(reason)}")

    state =
    case Ets.lookup(state.data_table, {:worker, :ref, ref}) do
     [] ->
      Logger.debug("Child is not found in table: #{inspect ref}, maybe already stopped.")
      state
    [{_, id, pid, type} = ref_data] ->
      Logger.debug("Child found: #{inspect pid}, restarting. meta: #{inspect ref_data}")
      Ets.delete(state.data_table, {:worker, :ref, ref})

      case type do
        :standalone ->
          [{_, child}] = Ets.lookup(state.data_table, {:worker, id})
          restart_standalone(state, child, {pid, reason})
        {:group, group_id} ->
          [{_, group}] = Ets.lookup(state.data_table, {:group, group_id})
          restart_group(state, group, id, {pid, reason})
        {:chain, chain_id} ->
          [{_, chain}] = Ets.lookup(state.data_table, {:chain, chain_id})
          restart_chain(state, chain, id, {pid, reason})
      end
    end
    main_loop(state, sup_opts)
  end

  defp restart_standalone(state, %Worker{} = child, {pid, reason}) do
    case child.restart_strategy do
      :permanent ->
        Logger.debug("#{inspect state.id}, :permanent, restarting #{inspect pid}")

        # Restart the child process
        sup_start_child(state, child)
      :transient when reason != :normal ->
        Logger.debug("#{inspect state.id}, :transient, reason down: #{inspect reason} restarting #{inspect pid}")

        # Restart the child process
        sup_start_child(state, child)
      :transient ->
        Logger.debug("#{inspect state.id}, :transient, ignore restarting #{inspect pid}")
        state
      :temporary ->
        Logger.debug("#{inspect state.id}, :temporary, ignore restarting #{inspect pid}")
        state
    end
  end

  defp restart_group(state, %{restart_strategy: :one_for_one} = group, child_id, {pid, reason}) do
    case reason do
      :normal ->
        Logger.debug("#{inspect state.id}, Child process(#{inspect(pid)}) is normal, ignore restarting.")
        state
      _ ->
        Logger.debug("#{inspect state.id}, Child process(#{inspect(pid)}) is down, restarting.")
        Group.restart_worker(group, child_id)

        state
    end
  end

  defp restart_group(state, %{restart_strategy: :one_for_all} = group, _child_id, {pid, reason}) do
    case reason do
      :normal ->
        Logger.debug("#{inspect state.id}, Child process(#{inspect(pid)}) is normal, ignore restarting.")
        state
      _ ->
        Logger.debug("#{inspect state.id}, Child process(#{inspect(pid)}) is down, restarting...")

        old_ref_keys = Enum.reduce(Group.get_all_workers(group), [], fn  worker, acc ->
          [worker.ref | acc]
        end)

        Logger.debug("#{inspect state.id}, Old ref: #{inspect old_ref_keys}")

        # Clean up old process.
        # TO-DO: make sure pid, ref in worker struct is cleaned & correct after restart.
        Group.kill_all_workers(group, :restart)

        Enum.each(Group.get_all_workers(group), fn %Worker{id: worker_id} ->
          {:ok, pid} = get_host_partition(state.master, worker_id)
          send(pid, {:restart_group_worker, worker_id, group.id})
        end)

        state
    end
  end

  # TO-DO: Follow restart strategy for child group & chain.

  defp restart_chain(state, %{restart_strategy: :one_for_one} = chain, child_id, {pid, reason}) do
    case reason do
      :normal ->
        Logger.debug("Worker(#{inspect child_id}) process(#{inspect(pid)}) is normal, ignore restarting.")
        state
      _ ->
        Logger.debug("Worker(#{inspect child_id}) process(#{inspect(pid)}) is down, restarting.")
        {:ok, chain} = Chain.restart_worker(chain, child_id)
        {:ok, worker} = Chain.get_worker(chain, child_id)
        chains = Map.put(state.chains, chain.id, chain)

        ref_to_id = Map.put(state.ref_to_id, worker.ref, {child_id, {:chain, chain.id}})

        state
        |> Map.put(:chains, chains)
        |> Map.put(:ref_to_id, ref_to_id)
    end
  end


  defp restart_chain(state, %{restart_strategy: :one_for_all} = chain, _child_id, {pid, reason}) do
    case reason do
      :normal ->
        Logger.debug("Child process(#{inspect(pid)}) is normal, ignore restarting.")
        state
      _ ->
        Logger.debug("Child process(#{inspect(pid)}) is down, restarting...")

        old_ref_keys = Enum.reduce(chain.workers, [], fn {_, worker}, acc ->  [worker.ref | acc] end)

        Logger.debug("Chai, Old ref: #{inspect old_ref_keys}")

        {:ok, chain} = Chain.restart_all_workers(chain)

        new_refs =
          state.ref_to_id
          |> Map.drop(old_ref_keys)

        new_refs = Enum.reduce(chain.workers, new_refs, fn {child_id, worker}, acc ->
          Map.put(acc, worker.ref, {child_id, {:chain, chain.id}})
        end)

        Logger.debug("New ref: #{inspect new_refs}")

        state
        |> Map.put(:chains, Map.put(state.chains, chain.id, chain))
        |> Map.put(:ref_to_id, new_refs)
    end
  end

  defp sup_start_child(state, %Worker{id: id, type: :standalone} = opts) do
    # Start a child process
    Logger.debug("Starting standalone worker process(#{inspect(id)})")

    Ets.insert(state.data_table, {{:worker, id}, opts})

    {pid, ref} =
      spawn_monitor(fn ->
        # Register the worker process.
        Registry.register(state.master, {:worker, id}, [])

        # Store for user can directly access to the worker.
        Process.put({:supervisor, :sup_id}, state.id)
        Process.put({:supervisor, :worker_id}, id)

        case opts.fun do
          {:fun, fun} ->
            fun.()
          {m, f, a} ->
            apply(m, f, a)
        end
      end)

    Ets.insert(state.data_table, {{:worker, :ref, ref}, id, pid, :standalone})

    state
  end

  defp sup_start_child(state, %Worker{} = %{id: id, group_id: group_id, type: :group} = opts) do
    # Start a child process
    Logger.debug("Starting child process(#{inspect(id)}) for group #{inspect(group_id)}")
    [{_, group}] = Ets.lookup(state.data_table, {:group, group_id})

    {:ok, _} = Group.add_worker(group, opts)

    state
  end

  defp sup_start_child(state, %Worker{} = %{id: id, chain_id: chain_id, type: :chain} = opts) do
    Logger.debug("Starting child process(#{inspect(id)}) for chain #{inspect(chain_id)}")

    [{_, chain}] = Ets.lookup(state.data_table, {:chain, chain_id})

    with {:ok, chain} <- Chain.add_worker(chain, opts) do
      # Move to Chain module.
      Ets.insert(state.data_table, {{:chain, chain_id}, chain})
      Logger.debug("part: #{inspect state.id}, added worker  to chain: #{inspect chain}")
    end

    state
  end

  defp get_group_or_chain(state, chain_id, type) do
    case Ets.lookup(state.data_table, {type, chain_id}) do
      [] ->
        Logger.info("#{inspect state.id}, Chain not found: #{inspect chain_id}")
        {:error, :not_found}
      [{_, data}] ->
        {:ok, data}
    end
  end

  defp add_new_group(state, group) do
    group = %Group{group | supervisor: state.master, partition: state.id, data_table: state.data_table}
    Registry.register(state.master, {:group, group.id}, [])
    Ets.insert(state.data_table, {{:group, group.id}, group})

    state
  end

  defp add_new_chain(state, chain) do
    chain = %Chain{chain | supervisor: state.master, partition: state.id, data_table: state.data_table}
    Registry.register(state.master, {:chain, chain.id}, [])
    Ets.insert(state.data_table, {{:chain, chain.id}, chain})

    state
  end

  @spec do_add_worker(atom(), atom() | tuple(), list(), integer()) :: {:ok, any()} | {:error, any()}
  defp do_add_worker(sup_id, :standalone, opts, timeout) do
    Logger.debug("Starting child process with options: #{inspect(opts)}")
    with {:ok, opts} <- Worker.check_standalone_options(opts),
      {:ok, pid} <- verify_and_get_pid(sup_id, opts.id) do
        opts =
          opts
          |> Map.put(:type, :standalone)

        call_api(pid, :start_worker, opts, timeout)
    else
      other ->
        Logger.error("cannot add standalone worker, something happened: #{inspect other}, options: #{inspect opts}")
        other
    end
  end
  defp do_add_worker(sup_id, {:group_id, group_id} = group, opts, timeout) do
    Logger.debug("Starting child process with options: #{inspect(opts)}")
    with {:ok, opts} <- Worker.check_group_options([group | opts]),
      {:ok, pid} <-verify_and_get_pid(sup_id, opts.id) do
        opts =
          opts
          |> Map.put(:group_id, group_id)
          |> Map.put(:type, :group)

        call_api(pid, :start_worker, opts, timeout)
      else
        error ->
          Logger.error("cannot add group worker: #{inspect error}, options: #{inspect opts}")
          error
    end
  end
  defp do_add_worker(sup_id, {:chain_id, chain_id} = chain, opts, timeout) do
    Logger.debug("Starting child process with options: #{inspect(opts)}")
    with {:ok, opts} <- Worker.check_chain_options([chain | opts]),
      {:ok, pid} <- verify_and_get_pid(sup_id, opts.id) do
        opts =
          opts
          |> Map.put(:chain_id, chain_id)
          |> Map.put(:type, :chain)

        call_api(pid, :start_worker, opts, timeout)
    else
      error ->
        Logger.error("cannot add chain worker: #{inspect error}, options: #{inspect opts}")
        error
    end
  end

  defp get_pid(id) when is_atom(id) do
    master = get_master_id(id)
    case Process.whereis(master) do
      nil ->
        {:error, :not_running}
      pid ->
        {:ok, pid}
    end
  end

  defp get_master_id(id) do
    String.to_atom("#{Atom.to_string(id)}_master")
  end

  defp check_opts(opts) do
    with {:ok, opts} <- normalize_opts(opts, @sup_params),
      {:ok, opts} <- validate_opts(opts) do
        {:ok, opts}
      end
  end

  # Validate the type & value of options.
  defp validate_opts(opts) do
    with {:ok, opts} <- check_type(opts, :id, &is_atom/1),
      opts <- default_sup_opts(opts),
      opts <- generic_default_sup_opts(opts),
      {:ok, opts} <- check_type(opts, :number_of_partitions, &is_integer/1),
      {:ok, opts} <- check_type(opts, :number_of_partitions, &(&1 > 0)),
      {:ok, opts} <- check_type(opts, :owner, &is_pid/1),
      {:ok, opts} <- check_type(opts, :link, &is_boolean/1) do
        {:ok, opts}
      else
        {:error, reason} = error ->
          Logger.error("Error in validating options: #{inspect reason}")
          error
      end
  end

  # Set the default options if not provided.
  # TO-DO: Merge with generic_default_sup_opts/1.
  defp default_sup_opts(opts) do
    if Map.has_key?(opts, :number_of_partitions) do
      opts
    else
      Map.put(opts, :number_of_partitions, :erlang.system_info(:schedulers_online))
    end
  end

  # Start the supervisor main processes.
  defp start_supervisor(opts, timeout) do
    Logger.debug("Starting supervisor with options: #{inspect opts}")

    ref = response_ref()

    # Start main process of the supervisor
    case opts.link do
      true ->
        Logger.debug("Starting supervisor with link.")
        spawn_link(__MODULE__, :init, [opts, ref])
      false ->
        Logger.debug("Starting supervisor without link.")
        spawn(__MODULE__, :init, [opts, ref])
      pid when is_pid(pid) ->
        Logger.debug("Starting supervisor and link with remote pid.")
        spawn(__MODULE__, :init, [opts, ref])
    end

    api_receiver(ref, timeout)
  end

  @spec get_partition_id(atom(), integer()) :: atom()
  defp get_partition_id(sup_id, partition_id) do
    if partition_id < 0 do
      sup_id
    else
      String.to_atom("#{Atom.to_string(sup_id)}_#{inspect partition_id}")
    end
  end

  @spec get_target_partition(atom(), any(), integer()) :: atom()
  defp get_target_partition(prefix, data, num_partitions) when is_integer(num_partitions) do
    partition_id = get_hash_order(data, num_partitions)
    get_partition_id(prefix, partition_id)
  end

  @spec get_partition_pid(atom(), atom()) :: {:error, atom()} | {:ok, pid()}
  defp get_partition_pid(sup_id, partition_id) do
    get_table_name(sup_id)
    |> Ets.lookup({:partition, partition_id})
    |> case do
      [{_, pid}] ->
        {:ok, pid}
      _ ->
        {:error, :not_found}
    end
  end

  @spec get_host_partition(atom(), any()) :: {:error, atom()} | {:ok, pid()}
  def get_host_partition(sup_id, data) do
    with [{_, num}] <- Ets.lookup(get_table_name(sup_id), :number_of_partitions),
      partition_id <- get_target_partition(sup_id, data, num),
      {:ok, pid} <- get_partition_pid(sup_id, partition_id) do
        {:ok, pid}
    else
      [] ->
        Logger.error("not found partition, data: #{inspect data}, sup_id: #{inspect sup_id}")
        {:error, :not_found}
      {:error, _} = error ->
        Logger.error("Get partition pid failed: #{inspect error}, data: #{inspect data}, sup_id: #{inspect sup_id}")
        error
    end
  end

  @spec create_table(atom()) :: atom()
  defp create_table(table_name) when is_atom(table_name) do
    ^table_name = Ets.new(table_name, [
      :set,
      :public,
      :named_table,
      {:keypos, 1},
      {:read_concurrency, true},
      {:decentralized_counters, true}
    ])
  end

  @spec has_group?(map(), any()) :: boolean()
  defp has_group?(%{} = state, group_id) do
    case Ets.lookup(state.data_table, {:group, group_id}) do
      [] ->
        false
      _ ->
        true
    end
  end

  @spec has_chain?(map(), any()) :: boolean()
  defp has_chain?(%{} = state, chain_id) do
    case Ets.lookup(state.data_table, {:chain, chain_id}) do
      [] ->
        false
      _ ->
        true
    end
  end

  @spec has_group_worker?(map(), any(), any()) :: boolean()
  defp has_group_worker?(%{} = state, group_id, worker_id) do
    case Ets.lookup(state.data_table, {:worker, {:group, group_id}, worker_id}) do
      [] ->
        false
      _ ->
        true
    end
  end

  @spec has_chain_worker?(map(), any(), any()) :: boolean()
  defp has_chain_worker?(%{} = state, chain_id, worker_id) do
    case Ets.lookup(state.data_table, {:worker, {:chain, chain_id}, worker_id}) do
      [] ->
        false
      _ ->
        true
    end
  end

  @spec has_worker?(map(), any()) :: boolean()
  defp has_worker?(%{} = state, worker_id) do
    case Ets.lookup(state.data_table, {:worker, worker_id}) do
      [] ->
        false
      _ ->
        true
    end
  end

  # Check the supervisor is running or not.
  # if running get pid of partition.
  @spec verify_and_get_pid(atom(), any()) :: {:error, atom()} | {:ok, pid()}
  defp verify_and_get_pid(sup_id, id) do
    with true <- is_running?(sup_id),
      {:ok, pid} <- get_host_partition(sup_id, id) do
        {:ok, pid}
      else
        false ->
          Logger.error("Supervisor not running.")
          {:error, :not_running}
        {:error, reason} = error ->
          Logger.error("Get target partition failed: #{inspect reason}")
          error
    end
  end
end
