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

  alias SuperWorker.Supervisor.{Group, Chain, Worker}
  alias :ets, as: Ets

  import SuperWorker.Supervisor.Utils

  defstruct [
    :id, # supervisor id
    :owner, # owner of the supervisor
    :number_of_partitions, # number of partitions, default is number of online schedulers
    link: true # link the supervisor to the caller
  ]

  @sup_params [:id, :number_of_partitions, :link]

  @me __MODULE__

  # Default timeout (miliseconds) for API calls.
  @default_time 5_000

  ## Public APIs

  @doc """
  Start supervisor for run standalone please set option :link to false.
  """
  def start(opts \\ []) when is_list(opts) do
    with {:ok, opts} <- check_opts(opts),
      {:error, :not_running} <- verify_running(opts.id) do
        start_supervisor(opts)
      end
  end

  @doc """
  Stop supervisor.
  Type of shutdown:
  - :normal supervisor will send a message to worker for graceful shutdown. Not support for spawn process by function.
  - :kill supervisor will kill worker.
  """
  def stop(sup_id, shutdown_type \\ :kill, timeout \\ @default_time) do
    case verify_running(sup_id) do
      {:error, _} = err ->
        Logger.error("Supervisor is not running.")
        err
      {:ok, pid} ->
        Logger.debug("Stopping supervisor: #{inspect pid}, shutdown type: #{inspect shutdown_type}")
        ref = make_ref()
        send(pid, {:stop, self(), shutdown_type, ref})
        case api_receiver(ref, timeout) do
          {:ok, _} ->
            Logger.debug("Supervisor stopped.")
            {:ok, :stopped}
          {:error, :timeout} ->
            Logger.error("Supervisor stopped with timeout.")
            Process.exit(pid, :kill)
            {:ok, :killed}
          {:error, reason} ->
            Logger.error("Supervisor stopped with error: #{inspect reason}")
            {:error, reason}
        end
    end
  end

  @doc """
  Add a standalone worker process to the supervisor.
  """
  def add_standalone_worker(sup_id, mfa_or_fun, opts, timeout \\ @default_time)

  def add_standalone_worker(sup_id, {m, f, a} = mfa, opts, timeout)
   when is_list(opts) and is_atom(m) and is_atom(f) and is_list(a) do
    do_start_child(sup_id, :standalone, [{:fun, mfa} | opts], timeout)
  end
  def add_standalone_worker(sup_id, fun, opts, timeout) when is_list(opts) and is_function(fun, 0) do
    do_start_child(sup_id, :standalone, [{:fun, {:fun, fun}} | opts], timeout)
  end

  @doc """
  Add a group worker to the group in supervisor.
  """
  def add_group_worker(sup_id, group_id, mfa_or_fun, opts, timeout \\ @default_time)
  def add_group_worker(sup_id, group_id, {m, f, a} = mfa, opts, timeout)
   when is_list(opts) and is_atom(m) and is_atom(f) and is_list(a) do
    do_start_child(sup_id, {:group_id, group_id}, [{:fun, mfa} | opts], timeout)
  end
  def add_group_worker(sup_id, group_id, fun, opts, timeout) when is_list(opts) and is_function(fun, 0) do
    do_start_child(sup_id, {:group_id, group_id}, [{:fun, {:fun, fun}} | opts], timeout)
  end

  @doc """
  Add a chain worker to the chain in supervisor.
  """
  def add_chain_worker(sup_id, chain_id, mfa_or_fun, opts, timeout \\ @default_time)
  def add_chain_worker(sup_id, chain_id, {m, f, a} = mfa, opts, timeout)
   when is_list(opts) and is_atom(m) and is_atom(f) and is_list(a) do
    do_start_child(sup_id, {:chain_id, chain_id}, [ {:fun, mfa} | opts], timeout)
  end
  def add_chain_worker(sup_id, chain_id, fun, opts, timeout) when is_list(opts) and is_function(fun, 0) do
    do_start_child(sup_id, {:chain_id, chain_id}, [{:fun, {:fun, fun}} | opts], timeout)
  end

  @doc """
  Add a group of processes to the supervisor.
  """
  def add_group(sup_id, opts, timeout \\ @default_time) do
    with {:ok, group} <- Group.check_options(opts),
      {:ok, _} <- verify_running(sup_id) do
      case Registry.lookup(sup_id, {:group, group.id}) do
        [] ->
          [{_, num}] = Ets.lookup(get_table_name(sup_id), :number_of_partitions)
          part_id = get_target_partition(sup_id, group.id, num)
          [{pid, _}] = Registry.lookup(sup_id, {:partition, part_id})

          ref = make_ref()
          send(pid, {:add_group, self(), ref, group})
          api_receiver(ref, timeout)
        _ ->
          {:error, :already_exists}
      end
    end
  end

  @doc """
  get group info
  """
  def get_group_info(sup_id, group_id, timeout \\ @default_time) do
    with verify_running(sup_id),
     {:ok, pid} <- get_host_partition(sup_id, group_id) do
        ref = make_ref()
        send(pid, {:get_group_info, self(), ref, group_id})
        api_receiver(ref, timeout)
    end
  end

  @doc """
  Start a chain of processes.
  """
  def add_chain(sup_id, opts, timeout \\ 5_000) do
    with {:ok, chain} <- Chain.check_options(opts),
      {:ok, _} <- verify_running(sup_id) do
        case Registry.lookup(sup_id, {:chain, chain.id}) do
          [] ->
            [{_, num}] = Ets.lookup(get_table_name(sup_id), :number_of_partitions)
            part_id = get_target_partition(sup_id, chain.id, num)
            [{pid, _}] = Registry.lookup(sup_id, {:partition, part_id})

            ref = make_ref()
            send(pid, {:add_chain, self(), ref, chain})
            api_receiver(ref, timeout)
          _ ->
            {:error, :already_exists}
        end
    end
  end

  def send_to_chain(sup_id, chain_id, data, timeout \\ @default_time) do
    with {:ok, _} <- verify_running(sup_id),
      [{_, num}] <- Ets.lookup(get_table_name(sup_id), :number_of_partitions),
      part_id = get_target_partition(sup_id, chain_id, num),
      [{pid, _}] = Registry.lookup(sup_id, {:partition, part_id}) do
        ref = make_ref()
        send(pid, {:add_data_to_chain, self(), ref, chain_id, data})
        api_receiver(ref, timeout)
    end
  end

  def send_to_worker(sup_id, worker_id, data, timeout \\ @default_time) do
    with {:ok, _} <- verify_running(sup_id),
     [{pid, _}] <- Registry.lookup(sup_id, {:worker, worker_id}) do
        ref = make_ref()
        send(pid, {:send_to_worker, self(), ref, worker_id, data})
        api_receiver(ref, timeout)
    end
  end

  def boardcast_to_group(sup_id, group_id, data, timeout \\ @default_time) do
    with {:ok, _} <- verify_running(sup_id),
      {:ok, pid} <- get_host_partition(sup_id, group_id) do
        ref = make_ref()
        # TO-DO dispatch to all worker in group.
        send(pid, {:broadcast_to_group, self(), ref, group_id, data})
        api_receiver(ref, timeout)
    end
  end

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
        boardcast_to_group(sup_id, group_id, data)
    end
  end

  def send_to_group(sup_id, group_id, worker_id, data, timeout \\ @default_time) do
    with {:ok, _} <- verify_running(sup_id),
      {:ok, pid} <- get_host_partition(sup_id, group_id) do
        ref = make_ref()
        send(pid, {:send_to_group, self(), ref, group_id, worker_id, data})
        api_receiver(ref, timeout)
    end
  end

  def send_to_group_random(sup_id, group_id, data, timeout \\ @default_time) do
    with {:ok, _} <- verify_running(sup_id),
      {:ok, pid} <- get_host_partition(sup_id, group_id) do
        ref = make_ref()
        send(pid, {:send_to_group_random, self(), ref, group_id, data})
        api_receiver(ref, timeout)
    end
  end

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
    with {:ok, _} <- verify_running(sup_id),
      {:ok, pid} <- get_host_partition(sup_id, chain_id) do
        ref = make_ref()
        send(pid, {:get_chain, self(), ref, chain_id})
        api_receiver(ref, timeout)
    end
  end

  ## Private functions

  def init(opts) do
    table = get_table_name(opts.id)

    state = %{
      groups: MapSet.new(), # storage for group processes
      chains: MapSet.new(), # storage for chain processes
      standalone: MapSet.new(), # storage for standalone processes
      id: opts.id, # supervisor id
      owner: opts.owner,
      table: table
    }

    # Register the supervisor process.
    Process.register(self(), get_master_id(opts.id))

    # Create Registry for map worker/group/chain id to pid.
    {:ok, _} = Registry.start_link(keys: :unique, name: opts.id, partitions: opts.number_of_partitions)

    Registry.register(opts.id, :master, 0)

    # Store group, chain, worker in Ets
    create_table(table)
    Ets.insert(table, {:number_of_partitions, opts.number_of_partitions})
    Ets.insert(table, {:owner, opts.owner})
    Ets.insert(table, {:master, opts.id})

    list_partitions = init_additional_partitions(opts)

    send(state.owner, {{self(), state.owner}, :ok})

    Logger.debug("Supervisor initialized: #{inspect state}")

    # Start the main loop
    state
    |> Map.put(:partitions , list_partitions)
    |> Map.put(:role, :master)
    |> Map.put(:master, opts.id)
    |> main_loop(opts)
  end

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
      table: get_table_name(opts.master)
    }

    # Start the main loop
    pid = spawn_link(@me, :main_loop, [state, opts])
    Process.register(pid, state.id)

    Logger.debug("Supervisor partition #{inspect state.id} initialized, pid: #{inspect pid}")

    Registry.register(state.master, {:partition , state.id}, [])

    Ets.insert(state.table, {{:partition, state.id},  pid})

    {:ok, opts.id, pid}
  end

  defp init_additional_partitions(opts) do
    partitions = opts.number_of_partitions

    Enum.map(1..partitions, fn i ->
      Logger.debug("Starting additional partition: #{inspect i}")
      opts
      |> Map.put(:master, opts.id)
      |> Map.put(:id, String.to_atom("#{Atom.to_string(opts.id)}_#{i}"))
      |> Map.put(:role, :partition)
      |> init_partition()
    end)
  end

  # Main loop for the supervisor.
  # TO-DO: Improve for easy to read & clean code.
  def main_loop(state, sup_opts) do
    receive do
      {:start_worker, from, ref, opts} = msg ->
        Logger.debug("#{inspect state.id}, Starting child process with options: #{inspect(msg)}")
        runable =
          case opts.type do
            :group ->
              if has_group?(state, opts.group_id) or  has_worker?(state.worker, opts.id) do
                true
              else
                :group_not_found_or_worker_already_exists
              end
            :chain ->
              if has_chain?(state, opts.chain_id) or  has_worker?(state, opts.id) do
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
            Logger.debug("#{inspect state.id}, Everything is fine, starting child process with options: #{inspect(opts)}")

            send(from, {ref, {:ok, opts.id}})
            sup_start_child(state, opts)
          else # not found group or chain, return error to the caller.
            send(from, {ref, {:error, runable}})
            state
          end

        main_loop(state, sup_opts)

      {:get_chain, from, ref, chain_id} ->
        case get_group_or_chain(state, chain_id, :chain) do
          {:error, _} = error ->
            Logger.error("#{inspect state.id}, Chain not found: #{inspect chain_id}")
            send(from, {ref, error})
          {:ok, chain} ->
            send(from, {ref, chain})
        end
        main_loop(state, sup_opts)

      {:broadcast_to_group, from, ref, group_id, data} ->
        case get_group_or_chain(state, group_id, :group) do
          {:ok, group} ->
            Group.broadcast(group, data)
            send(from, {ref, :ok})
          {:error, _} = error ->
            Logger.error("#{inspect state.id}, Group not found: #{inspect group_id}")
            send(from, {ref, error})
        end
        main_loop(state, sup_opts)

      # send directly to worker from api.
      {:send_to_group, from, ref, group_id, worker_id, data} ->
        with {:ok, group} <- get_group_or_chain(state, group_id, :group),
        {:ok, worker} <- Group.get_worker(group, worker_id) do
          send(worker.pid, data)
          send(from, {ref, :ok})
        else
          failed ->
            Logger.error("#{inspect state.id}, Not found worker #{inspect worker_id} in group #{inspect group_id}")
            send(from, {ref, failed})
        end

        main_loop(state, sup_opts)

      # send data to random worker from api.
      {:send_to_group_random, from, ref, group_id, data} ->
        case get_group_or_chain(state, group_id, :group) do
          {:error, _} = error  ->
            Logger.error("#{inspect state.id}, Group not found: #{inspect group_id}, error: #{inspect error}")
            send(from, {ref, error})
          {:ok, group} ->
             worker_id = Enum.random(group.workers)
              case Group.get_worker(group, worker_id) do
                {:ok, worker} ->
                  send(worker.pid, data)
                  send(from, {ref, :ok})
                {:error, _} = error ->
                  Logger.error("#{inspect state.id}, Not found worker #{inspect worker_id} in group #{inspect group_id}")
                  send(from, {ref, error})
              end
        end
        main_loop(state, sup_opts)

      {:add_data_to_chain, from, ref, chain_id, data} ->
        case get_group_or_chain(state, chain_id, :chain) do
          {:error, _} = error ->
            Logger.error("#{inspect state.id}, Chain not found: #{inspect chain_id}")
            send(from, {ref, error})
          {:ok, chain} ->
            Chain.new_data(chain, data)
            send(from, {ref, :ok})
        end
        main_loop(state, sup_opts)

      {:send_to_worker, from, ref, worker_id, data} ->
        case get_group_or_chain(state, worker_id, :not_implement) do
          {:error, _} = error ->
            Logger.error("#{inspect state.id}, Worker not found: #{inspect worker_id}")
            send(from, {ref, error})
          worker ->
            send(worker.pid, data)
            send(from, {ref, :ok})
        end
        main_loop(state, sup_opts)

      {:restart_group_worker, worker_id , group_id} ->
        Logger.debug("#{inspect state.id}, Starting worker process, worker id: #{inspect(worker_id)}, group id: #{inspect(group_id)}")

        [{_, group}] = Ets.lookup(state.table, {:group, group_id})
        # Restart the worker process.
        Group.restart_worker(group, worker_id)

        main_loop(state, sup_opts)

      {:DOWN, ref, :process, pid, :restart} ->
        Logger.debug("#{inspect state.id}, Ignore died process (process by other msg): #{inspect(pid)}, ref: #{inspect(ref)}")

        main_loop(state, sup_opts)

      {:DOWN, ref, :process, pid, reason} ->
        Logger.debug("#{inspect state.id}, Worker died: #{inspect(pid)}, reason: #{inspect(reason)}")

        state
        |> process_child_down(pid, ref, reason)
        |> main_loop(sup_opts)

      # add group from api.
      {:add_group, from, ref, group} ->
        case Ets.lookup(state.table, {:gorup, group.id}) do
          [_] ->
            Logger.error("#{inspect state.id}, Group already exists: #{inspect(group.id)}")
            send(from, {ref, {:error, :already_exists}})
            main_loop(state, sup_opts)
          [] ->
            Logger.debug("#{inspect state.id}, Adding group: #{inspect(group.id)}")

            # Send the response to the caller.
            send(from, {ref, {:ok, group.id}})

            state
            |> add_new_group(group)
            |> main_loop(sup_opts)
        end

    # get group info from api.
    {:get_group_info, from, ref, group_id} ->
      group =
        case Ets.lookup(state.table, {:group, group_id}) do
          [] ->
            Logger.error("#{inspect state.id}, Group not found: #{inspect(group_id)}")
            {:error, :not_found}
          [{_, group}] ->
            %{id: group.id, count: map_size(group.children), stats: nil}
        end
      send(from, {ref, group})
      main_loop(state, sup_opts)

    # add chain from api.
    {:add_chain, from, ref, chain} ->
      case Ets.lookup(state.table, {:chain, chain.id}) do
        [_] ->
          Logger.error("#{inspect state.id}, Chain already exists: #{inspect(chain.id)}")
          send(from, {ref, {:error, :already_exists}})
          main_loop(state, sup_opts)
        [] ->
          Logger.debug("#{inspect state.id}, Adding chain: #{inspect(chain.id)}")
          # Send the response to the caller.
          send(from, {ref, {:ok, chain.id}})

          state
          |> add_new_chain(chain)
          |> main_loop(sup_opts)
      end

    # Stop supervisor from api.
    {:stop, from, type, ref} ->
        Logger.info("#{inspect state.id}, Stopping supervisor, request from #{inspect from}")

        # Send shutdown signal to all partitions.
        Enum.each(1..sup_opts.number_of_partitions, fn i ->
          partition_id = get_partition_id(state.id, i)
          # TO-DO: change to Ets
          case Ets.lookup(state.table, {:partition, partition_id}) do
            [{_, pid}] ->
              Logger.debug("#{inspect state.id}, Sending shutdown signal to partition: #{inspect partition_id}")
              send(pid, {:stop_partition, type})
            _ ->
              Logger.error("#{inspect state.id}, Supervisor not found: #{inspect partition_id}")
          end
        end)

        # stop worker on master.
        shutdown(state, type)

        # TO-DO: Clean KV store for supervisor.

        send(from, {ref, {:ok, :stopped}})

        exit(:normal)

    {:stop_partition, type} ->
        Logger.info("#{inspect state.id}, Stopping supervisor partition, for #{inspect self()}")
        # Stop the supervisor.
        shutdown(state, type)
      unknown ->
        Logger.warning("#{inspect state.id}, unknown message: #{inspect(unknown)}")
        main_loop(state, sup_opts)
    end

    Logger.debug("#{inspect state.id}, #{inspect state.role}, #{inspect self()} Supervisor main loop exited.")
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

  defp process_child_down(state, pid, ref, reason) do
    Logger.debug("Child process died: #{inspect(pid)}, ref: #{inspect ref}, reason: #{inspect(reason)}")

    case Ets.lookup(state.table, {:worker, :ref, ref}) do
     [] ->
      Logger.debug("Child is not found in table: #{inspect ref}, maybe already stopped.")
      state
    [{_, id, pid, type} = ref_data] ->
      Logger.debug("Child found: #{inspect pid}, restarting. meta: #{inspect ref_data}")
      Ets.delete(state.table, {:worker, :ref, ref})

      case type do
        :standalone ->
          child = Ets.lookup(state.table, {:worker, id})
          restart_standalone(state, child, {pid, reason})
        {:group, group_id} ->
          [{_, group}] = Ets.lookup(state.table, {:group, group_id})
          restart_group(state, group, id, {pid, reason})
        {:chain, chain_id} ->
          [{_, chain}] = Ets.lookup(state.table, {:chain, chain_id})
          restart_chain(state, chain, id, {pid, reason})
      end
    end
  end

  defp restart_standalone(state, child, {pid, reason}) do
    opts = child.opts

    case opts.restart_strategy do
      :permanent ->
        Logger.debug("#{inspect state.id}, :permanent, restarting #{inspect pid}")

        # Restart the child process
        sup_start_child(state, opts)
      :transient when reason != :normal ->
        Logger.debug("#{inspect state.id}, :transient, reason down: #{inspect reason} restarting #{inspect pid}")

        # Restart the child process
        sup_start_child(state, opts)
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
        {:ok, group} = Group.restart_worker(group, child_id)
        {:ok, worker} = Group.get_worker(group, child_id)
        Ets.insert(state.table, {{:worker, :ref, worker.ref}, worker.id, worker.pid, {:group, group.id}})
        Ets.insert(state.table, {{:group, group.id}, group})

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

        old_ref_keys = Enum.reduce(group.workers, [], fn  worker_id, acc ->
          [{_, worker}] = Ets.lookup(state.table, {:worker, {:group, group.id}, worker_id})
          [worker.ref | acc]
        end)

        Logger.debug("#{inspect state.id}, Old ref: #{inspect old_ref_keys}")

        # Clean up old process.
        # TO-DO: make sure pid, ref in worker struct is cleaned & correct after restart.
        Group.kill_all_workers(group, :restart)
        Enum.each(old_ref_keys, fn ref ->
          Ets.delete(state.table, {:worker, :ref, ref})
        end)

        Enum.each(group.workers, fn child_id ->
          {:ok, pid} = get_host_partition(state.master, child_id)
          send(pid, {:restart_group_worker, child_id, group.id})
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

        Logger.debug("Old ref: #{inspect old_ref_keys}")

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

    Ets.insert(state.table, {{:worker, id}, opts})

    {pid, ref} =
      spawn_monitor(fn ->
        # Register the worker process.
        Registry.register(state.id, {:worker, id}, [])

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

    Ets.insert(state.table, {{:worker, :ref, ref}, id, pid, :standalone})

    state
  end

  defp sup_start_child(state, %Worker{} = %{id: id, group_id: group_id, type: :group} = opts) do
    # Start a child process
    Logger.debug("Starting child process(#{inspect(id)}) for group #{inspect(group_id)}")
    [{_, group}] = Ets.lookup(state.table, {:group, group_id})

    {:ok, _} = Group.add_worker(group, opts)

    state
  end

  defp sup_start_child(state, %Worker{} = %{id: id, chain_id: chain_id, type: :chain} = opts) do
    Logger.debug("Starting child process(#{inspect(id)}) for chain #{inspect(chain_id)}")

    [{_, chain}] = Ets.lookup(state.table, {:chain, chain_id})

    with {:ok, chain} <- Chain.add_worker(chain, %{id: id, opts: opts}) do
      Ets.insert(state.table, {{:chain, chain_id}, chain})
    end

    state
  end

  defp get_group_or_chain(state, chain_id, type) do
    case Ets.lookup(state.table, {type, chain_id}) do
      [] ->
        Logger.info("#{inspect state.id}, Chain not found: #{inspect chain_id}")
        {:error, :not_found}
      [{_, data}] ->
        {:ok, data}
    end
  end

  defp api_receiver(ref, timeout \\ 5_000) do
    receive do
      {^ref, result} ->
        result
      after timeout ->
        {:error, :timeout}
    end
  end

  defp add_new_group(state, group) do
    group = %Group{group | supervisor: state.master, partition: state.id}
    Registry.register(state.id, {:group, group.id}, [])
    Ets.insert(state.table, {{:group, group.id}, group})

    state
  end

  defp add_new_chain(state, chain) do
    chain = %Chain{chain | supervisor: state.master, partition: state.id}
    Registry.register(state.id, {:chain, chain.id}, [])
    Ets.insert(state.table, {{:chain, chain.id}, chain})

    state
  end

  defp do_start_child(sup_id, :standalone, opts, timeout) do
    Logger.debug("Starting child process with options: #{inspect(opts)}")
    with {:ok, opts} <- Worker.check_standalone_options(opts),
      {:ok, _} <- verify_running(sup_id),
      {:ok, pid} <- get_host_partition(sup_id, opts.id) do
        opts =
          opts
          |> Map.put(:type, :standalone)

        ref = make_ref()
        send(pid, {:start_worker, self(), ref, opts})
        api_receiver(ref, timeout)
    else
      other ->
        Logger.error("Something happened: #{inspect other}")
        other
    end
  end
  defp do_start_child(sup_id, {:group_id, group_id} = group, opts, timeout) do
    Logger.debug("Starting child process with options: #{inspect(opts)}")
    with {:ok, opts} <- Worker.check_group_options([group | opts]),
      {:ok, _} <- verify_running(sup_id),
      {:ok, pid} <-get_host_partition(sup_id, opts.id) do
        opts =
          opts
          |> Map.put(:group_id, group_id)
          |> Map.put(:type, :group)
        ref = make_ref()
        send(pid, {:start_worker, self(), ref, opts})
        api_receiver(ref, timeout)
        else
          error ->
            Logger.error("Error in starting group worker: #{inspect error}")
            error
    end
  end
  defp do_start_child(sup_id, {:chain_id, chain_id} = chain, opts, timeout) do
    Logger.debug("Starting child process with options: #{inspect(opts)}")
    with {:ok, opts} <- Worker.check_chain_options([chain | opts]),
      {:ok, _} <- verify_running(sup_id),
      {:ok, pid} <- get_host_partition(sup_id, opts.id) do
        opts =
          opts
          |> Map.put(:chain_id, chain_id)
          |> Map.put(:type, :chain)

        ref = make_ref()
        send(pid, {:start_worker, self(), ref, opts})
        api_receiver(ref, timeout)
    end
  end

  defp verify_running(id) when is_atom(id) do
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
  defp start_supervisor(opts) do
    Logger.debug("Starting supervisor with options: #{inspect opts}")

    # Start main process of the supervisor
    pid = if opts.link do # link the supervisor to the caller
      spawn_link(__MODULE__, :init, [opts])
    else
      spawn(__MODULE__, :init, [opts])
    end

    api_receiver({pid, self()})
  end

  defp get_partition_id(sup_id, partition_id) do
    if partition_id == 0 do
      sup_id
    else
      String.to_atom("#{Atom.to_string(sup_id)}_#{inspect partition_id}")
    end
  end

  defp get_target_partition(prefix, data, num_partitions) when is_integer(num_partitions) do
    partition_id = get_hash_order(data, num_partitions)
    get_partition_id(prefix, partition_id)
  end

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

  def get_host_partition(sup_id, data) do
    with [{_, num}] <- Ets.lookup(get_table_name(sup_id), :number_of_partitions),
      order <- get_hash_order(data, num),
      partition_id <- get_partition_id(sup_id, order),
      [{_, pid}] <- Ets.lookup(get_table_name(sup_id), {:partition, partition_id}) do
        {:ok, pid}
    else
      error ->
        Logger.error("Get partition pid failed: #{inspect error}, data: #{inspect data}, sup_id: #{inspect sup_id}")
        error
    end
  end

  defp create_table(table_name) when is_atom(table_name) do
    ^table_name = Ets.new(table_name, [
      :set,
      :public,
      :named_table,
      {:keypos, 1},
      {:write_concurrency, true},
      {:read_concurrency, true},
      {:decentralized_counters, true}
    ])
  end

  defp has_group?(%{} = state, group_id) do
    case Ets.lookup(state.table, {:group, group_id}) do
      [] ->
        false
      _ ->
        true
    end
  end

  defp has_chain?(%{} = state, chain_id) do
    case Ets.lookup(state.table, {:chain, chain_id}) do
      [] ->
        false
      _ ->
        true
    end
  end

  defp has_worker?(%{} = state, worker_id) do
    case Ets.lookup(state.table, {:worker, worker_id}) do
      [] ->
        false
      _ ->
        true
    end
  end
end
