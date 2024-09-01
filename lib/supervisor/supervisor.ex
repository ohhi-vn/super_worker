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
  alias SuperWorker.TermStorage, as: KV

  import SuperWorker.Supervisor.Utils

  defstruct [
    :groups, # storage for group processes
    :chains, # storage for chain processes
    :standalone, # storage for standalone processes
    :ref_to_id, # storage for refs to id & type
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
    do_start_child(sup_id, :standalone, mfa, opts, timeout)
  end
  def add_standalone_worker(sup_id, fun, opts, timeout) when is_list(opts) and is_function(fun, 0) do
    do_start_child(sup_id, :standalone, {:fun, fun}, opts, timeout)
  end

  @doc """
  Add a group worker to the group in supervisor.
  """
  def add_group_worker(sup_id, group_id, mfa_or_fun, opts, timeout \\ @default_time)
  def add_group_worker(sup_id, group_id, {m, f, a} = mfa, opts, timeout)
   when is_list(opts) and is_atom(m) and is_atom(f) and is_list(a) do
    do_start_child(sup_id, {:group, group_id}, mfa, opts, timeout)
  end
  def add_group_worker(sup_id, group_id, fun, opts, timeout) when is_list(opts) and is_function(fun, 0) do
    do_start_child(sup_id, {:group, group_id}, {:fun, fun}, opts, timeout)
  end

  @doc """
  Add a chain worker to the chain in supervisor.
  """
  def add_chain_worker(sup_id, chain_id, mfa_or_fun, opts, timeout \\ @default_time)
  def add_chain_worker(sup_id, chain_id, {m, f, a} = mfa, opts, timeout)
   when is_list(opts) and is_atom(m) and is_atom(f) and is_list(a) do
    do_start_child(sup_id, {:chain, chain_id}, mfa, opts, timeout)
  end
  def add_chain_worker(sup_id, chain_id, fun, opts, timeout) when is_list(opts) and is_function(fun, 0) do
    do_start_child(sup_id, {:chain, chain_id}, {:fun, fun}, opts, timeout)
  end

  @doc """
  Add a group of processes to the supervisor.
  """
  def add_group(sup_id, opts, timeout \\ @default_time) do
    with {:ok, group} <- Group.check_options(opts),
      {:ok, pid} <- verify_running(sup_id) do
        ref = make_ref()
        send(pid, {:add_group, self(), ref, group})
        api_receiver(ref, timeout)
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
      {:ok, _} <- verify_running(sup_id),
      {:ok, pid} <- get_host_partition(sup_id, opts.id) do
        ref = make_ref()
        send(pid, {:add_chain, self(), ref, chain})
        api_receiver(ref, timeout)
    end
  end

  def send_to_chain(sup_id, chain_id, data, timeout \\ @default_time) do
    with {:ok, _} <- verify_running(sup_id),
      {:ok, pid} <- get_host_partition(sup_id, chain_id) do
        ref = make_ref()
        send(pid, {:add_data_to_chain, self(), ref, chain_id, data})
        api_receiver(ref, timeout)
    end
  end

  def send_to_worker(sup_id, worker_id, data, timeout \\ @default_time) do
    with {:ok, _} <- verify_running(sup_id),
     {:ok, pid} <- get_host_partition(sup_id, worker_id) do
        ref = make_ref()
        send(pid, {:send_to_worker, self(), ref, worker_id, data})
        api_receiver(ref, timeout)
    end
  end

  def boardcast_to_group(sup_id, group_id, data, timeout \\ @default_time) do
    with {:ok, _} <- verify_running(sup_id),
      {:ok, pid} <- get_host_partition(sup_id, group_id) do
        ref = make_ref()
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

    state = %{
      groups: %{}, # storage for group processes
      chains: %{}, # storage for chain processes
      standalone: %{}, # storage for standalone processes
      ref_to_id: %{}, # storage for refs to id & type
      id: opts.id, # supervisor id
      owner: opts.owner
    }

    Process.register(self(), opts.id)

    KV.put({opts.id, :number_of_partitions}, opts.number_of_partitions)

    list_partitions = init_additional_partitions(opts)

    send(state.owner, {{self(), state.owner}, :ok})

    Logger.debug("Supervisor initialized: #{inspect state}")

    # Start the main loop
    state
    |> Map.put(:partitions , list_partitions)
    |> Map.put(:role, :master)
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
      role: :partition
    }

    # Start the main loop
    pid = spawn_link(@me, :main_loop, [state, opts])

    Logger.debug("Supervisor partition #{inspect opts.id} initialized, pid: #{inspect pid}")

    # Store the pid in the KV store.
    KV.put({opts.master, :pid, opts.id}, pid)

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
      {:start_worker, from, ref, mfa_or_fun, opts} = msg ->
        Logger.debug("Starting child process with options: #{inspect(msg)}")
        runable =
          case opts.type do
            :group ->
              if Map.has_key?(state.groups, opts.group_id) do
                group = Map.get(state.groups, opts.group_id)
                if Group.worker_exists?(group, opts.id) do
                  :worker_already_exists
                else
                  true
                end
              else
                :group_not_found
              end
            :chain ->
              with {:ok, chain} <- get_item(state.chains, opts.chain_id),
                {:error, :not_found} <- Chain.get_worker(chain, opts.id) do
                  true
                else
                  {:ok, _} ->
                    :worker_already_exists
                  {:error, :not_found} ->
                    :chain_not_found
                end
            :standalone ->
              if Map.has_key?(state.standalone, opts.id) do
                :worker_already_exists
              else
                true
              end
          end
        if runable == true do # start child process.
          Logger.debug("Everything is fine, starting child process with options: #{inspect(opts)}")
          state = sup_start_child(state, mfa_or_fun, opts)
          send(from, {ref, {:ok, opts.id}})

          main_loop(state, sup_opts)
        else # not found group or chain, return error to the caller.
          send(from, {ref, {:error, runable}})

          main_loop(state, sup_opts)
        end

      {:get_chain, from, ref, chain_id} ->
        case Map.get(state.chains, chain_id) do
          nil ->
            Logger.error("Chain not found: #{inspect chain_id}")
            send(from, {ref, {:error, :not_found}})
          chain ->
            send(from, {ref, chain})
        end
        main_loop(state, sup_opts)

      {:broadcast_to_group, from, ref, group_id, data} ->
        case Map.get(state.groups, group_id) do
          nil ->
            Logger.error("Group not found: #{inspect group_id}")
            send(from, {ref, {:error, :not_found}})
          group ->
            Group.broadcast(group, data)
            send(from, {ref, :ok})
        end
        main_loop(state, sup_opts)

      # send directly to worker from api.
      {:send_to_group, from, ref, group_id, worker_id, data} ->
        case Map.get(state.groups, group_id) do
          nil ->
            Logger.error("Group not found: #{inspect group_id}")
            send(from, {ref, {:error, :not_found}})
          group ->
            case Group.get_worker(group, worker_id) do
              {:ok, worker} ->
                send(worker.pid, data)
                send(from, {ref, :ok})
              {:error, _} ->
                Logger.error("Worker not found: #{inspect worker_id}")
                send(from, {ref, {:error, :not_found}})
            end
        end
        main_loop(state, sup_opts)

      # send data to random worker from api.
      {:send_to_group_random, from, ref, group_id, data} ->
        case Map.get(state.groups, group_id) do
          nil ->
            Logger.error("Group not found: #{inspect group_id}")
            send(from, {ref, {:error, :not_found}})
          group ->
            {_, worker} = Enum.random(group.workers)
            send(worker.pid, data)
            send(from, {ref, :ok})
        end
        main_loop(state, sup_opts)


      {:add_data_to_chain, from, ref, chain_id, data} ->
        case Map.get(state.chains, chain_id) do
          nil ->
            Logger.error("Chain not found: #{inspect chain_id}")
            send(from, {ref, {:error, :not_found}})
            main_loop(state, sup_opts)
          chain ->
            Chain.new_data(chain, data)
            send(from, {ref, :ok})
            main_loop(state, sup_opts)
        end

      {:send_to_worker, from, ref, worker_id, data} ->
        case Map.get(state.standalone, worker_id) do
          nil ->
            Logger.error("Worker not found: #{inspect worker_id}")
            send(from, {ref, {:error, :not_found}})
          worker ->
            send(worker.pid, data)
            send(from, {ref, :ok})
        end
        main_loop(state, sup_opts)

      {:DOWN, ref, :process, pid, reason} ->
        Logger.debug("receiced signal worker died: #{inspect(pid)}, reason: #{inspect(reason)}")

        state
        |> process_child_down(pid, ref, reason)
        |> main_loop(sup_opts)

      # add group from api.
      {:add_group, from, ref, group} ->
        case Map.has_key?(state.groups, group.id) do
          true ->
            Logger.error("Group already exists: #{inspect(group.id)}")
            send(from, {ref, {:error, :already_exists}})
            main_loop(state, sup_opts)
          false ->
            Logger.debug("Adding group: #{inspect(group.id)}")

            # Send the response to the caller.
            send(from, {ref, {:ok, group.id}})

            state
            |> add_new_group(group)
            |> main_loop(sup_opts)
        end

    # get group info from api.
    {:get_group_info, from, ref, group_id} ->
      group =
        case Map.get(state.groups, group_id, nil) do
          nil ->
            Logger.error("Group not found: #{inspect(group_id)}")
            {:error, :not_found}
          group ->
            %{id: group.id, count: map_size(group.children), stats: nil}
        end
      send(from, {ref, group})
      main_loop(state, sup_opts)

    # add chain from api.
    {:add_chain, from, ref, chain} ->
      case Map.has_key?(state.chains, chain.id) do
        true ->
          Logger.error("Chain already exists: #{inspect(chain.id)}")
          send(from, {ref, {:error, :already_exists}})
          main_loop(state, sup_opts)
        false ->
          Logger.debug("Adding chain: #{inspect(chain.id)}")
          # Send the response to the caller.
          send(from, {ref, {:ok, chain.id}})

          state
          |> add_new_chain(chain)
          |> main_loop(sup_opts)
      end

    # Stop supervisor from api.
    {:stop, from, type, ref} ->
        Logger.info("Stopping supervisor, request from #{inspect from}")

        # Send shutdown signal to all partitions.
        Enum.each(1..sup_opts.number_of_partitions, fn i ->
          partition_id = get_partition_id(state.id, i)
          case KV.get({state.id, :pid, partition_id}) do
            {:ok, pid} ->
              Logger.debug("Sending shutdown signal to partition: #{inspect partition_id}")
              send(pid, {:stop_partition, type})
            _ ->
              Logger.error("Supervisor not found: #{inspect partition_id}")
          end
        end)

        # stop worker on master.
        shutdown(state, type)

        # TO-DO: Clean KV store for supervisor.

        send(from, {ref, {:ok, :stopped}})

        exit(:normal)

    {:stop_partition, type} ->
        Logger.info("Stopping supervisor partition, for #{inspect self()}")
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
    {id, type} = Map.get(state.ref_to_id, ref, {:error, :not_found})
    if id == :error do
      Logger.debug("Child not found: #{inspect ref}, maybe already stopped.")
      state
    else
      ref_to_id = Map.delete(state.ref_to_id, ref)
      state = Map.put(state, :ref_to_id, ref_to_id)

      case type do
        :standalone ->
          child = Map.get(state.standalone, id)
          restart_standalone(state, child, {pid, reason})
        {:group, group_id} ->
          group = Map.get(state.groups, group_id)
          restart_group(state, group, id, {pid, reason})
        {:chain, chain_id} ->
          chain = Map.get(state.chains, chain_id)
          restart_chain(state, chain, id, {pid, reason})
        :not_found ->
          Logger.debug("Child not found: #{inspect id}, ref: #{inspect ref}, maybe already stopped.")
          state
      end
    end
  end

  defp restart_standalone(state, child, {pid, reason}) do
    opts = child.opts

    case opts.restart_strategy do
      :permanent ->
        Logger.debug(":permanent, restarting #{inspect pid}")

        # Restart the child process
        sup_start_child(state, child.mfa, opts)
      :transient when reason != :normal ->
        Logger.debug(":transient, reason down: #{inspect reason} restarting #{inspect pid}")

        # Restart the child process
        sup_start_child(state, child.mfa, opts)
      :transient ->
        Logger.debug(":transient, ignore restarting #{inspect pid}")
        state
      :temporary ->
        Logger.debug(":temporary, ignore restarting #{inspect pid}")
        state
    end
  end

  defp restart_group(state, %{restart_strategy: :one_for_one} = group, child_id, {pid, reason}) do
    case reason do
      :normal ->
        Logger.debug("Child process(#{inspect(pid)}) is normal, ignore restarting.")
        state
      _ ->
        Logger.debug("Child process(#{inspect(pid)}) is down, restarting.")
        {:ok, group} = Group.restart_worker(group, child_id)
        {:ok, worker} = Group.get_worker(group, child_id)
        groups = Map.put(state.groups, group.id, group)

        ref_to_id = Map.put(state.ref_to_id, worker.ref, {child_id, {:group, group.id}})

        state
        |> Map.put(:groups, groups)
        |> Map.put(:ref_to_id, ref_to_id)
    end
  end

  defp restart_group(state, %{restart_strategy: :one_for_all} = group, _child_id, {pid, reason}) do
    case reason do
      :normal ->
        Logger.debug("Child process(#{inspect(pid)}) is normal, ignore restarting.")
        state
      _ ->
        Logger.debug("Child process(#{inspect(pid)}) is down, restarting...")

        old_ref_keys = Enum.reduce(group.workers, [], fn {_, worker}, acc ->  [worker.ref | acc] end)

        Logger.debug("Old ref: #{inspect old_ref_keys}")

        {:ok, group} = Group.restart_all_workers(group)

        new_refs =
          state.ref_to_id
          |> Map.drop(old_ref_keys)

        new_refs = Enum.reduce(group.workers, new_refs, fn {child_id, worker}, acc ->
          Map.put(acc, worker.ref, {child_id, {:group, group.id}})
        end)

        Logger.debug("New ref: #{inspect new_refs}")

        state
        |> Map.put(:groups, Map.put(state.groups, group.id, group))
        |> Map.put(:ref_to_id, new_refs)
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

  defp sup_start_child(state, mfa, %{id: id, type: :standalone} = opts) do
    # Start a child process
    Logger.debug("Starting standalone worker process(#{inspect(id)})")

    {pid, ref} =
      spawn_monitor(fn ->
        # Store for user can directly access to the worker.
        Process.put({:supervisor, :sup_id}, state.id)
        Process.put({:supervisor, :worker_id}, id)

        case mfa do
          {:fun, fun} ->
            fun.()
          {m, f, a} ->
            apply(m, f, a)
        end
      end)

    # add/update ref to id.
    ref_to_id = Map.put(state.ref_to_id, ref, {id, :standalone})
    standalone = Map.put(state.standalone, id, %{id: id, pid: pid, ref: ref, mfa: mfa, opts: opts})

    # return new state, including the new child
    state
    |> Map.put(:standalone, standalone)
    |> Map.put(:ref_to_id, ref_to_id)
  end

  defp sup_start_child(state, fun_mfa, %{id: id, group_id: group_id, type: :group} = opts) do
    # Start a child process
    Logger.debug("Starting child process(#{inspect(id)}) for group #{inspect(group_id)}")

    with {:ok, group} <- get_item(state.groups, group_id),
      {:ok, group} = Group.add_worker(group, %{id: id, mfa: fun_mfa, opts: opts}),
      {:ok, worker} = Group.get_worker(group, id) do
        ref_to_id = Map.put(state.ref_to_id, worker.ref, {id, {:group, group_id}})
        groups = Map.put(state.groups, group_id, group)
        state
        |> Map.put(:groups, groups)
        |> Map.put(:ref_to_id, ref_to_id)
      else
        reason ->
          Logger.error("Error in starting group(#{inspect group_id}) process(#{inspect(id)}): #{inspect(reason)}")
          state
      end
  end

  defp sup_start_child(state, mfa, %{id: id, chain_id: chain_id, type: :chain} = opts) do
    Logger.debug("Starting child process(#{inspect(id)}) for chain #{inspect(chain_id)}")

    with {:ok, chain} <- get_item(state.chains, chain_id),
      {:ok, chain} <- Chain.add_worker(chain, %{id: id, mfa: mfa, opts: opts}),
      {:ok, worker} <- Chain.get_worker(chain, id) do
         # add/update ref to id.
        refs = Map.put(state.ref_to_id, worker.ref, {id, {:chain, chain_id}})
        chains = Map.put(state.chains, chain_id, chain)

        state
        |> Map.put(:chains,chains)
        |> Map.put(:ref_to_id, refs)
      else
        reason ->
          Logger.error("Error in starting chain(#{inspect chain_id}) process(#{inspect(id)}): #{inspect(reason)}")
          state
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
    group = %Group{group | supervisor: state.id}
    groups = Map.put(state.groups, group.id, group)

    # Update the state
    state
    |> Map.put(:groups, groups)
  end

  defp add_new_chain(state, chain) do
    chain = %Chain{chain | supervisor: state.id}
    chains = Map.put(state.chains, chain.id, chain)

    # Update the state
    state
    |> Map.put(:chains, chains)
  end

  defp do_start_child(sup_id, :standalone, mfa_or_fun, opts, timeout) do
    Logger.debug("Starting child process with options: #{inspect(opts)}")
    with {:ok, opts} <- Worker.check_options(opts),
      {:ok, _} <- verify_running(sup_id),
      {:ok, pid} <- get_host_partition(sup_id, opts.id) do
        opts =
          opts
          |> Map.put(:type, :standalone)

        ref = make_ref()
        send(pid, {:start_worker, self(), ref, mfa_or_fun, opts})
        api_receiver(ref, timeout)
    else
      other ->
        Logger.error("Something happened: #{inspect other}")
        other
    end
  end
  defp do_start_child(sup_id, {:group, group_id}, mfa_or_fun, opts, timeout) do
    Logger.debug("Starting child process with options: #{inspect(opts)}")
    with {:ok, opts} <- Group.check_worker_opts([{:group_id, group_id} | opts]),
      {:ok, _} <- verify_running(sup_id),
      {:ok, pid} <- get_host_partition(sup_id, group_id) do
        opts =
          opts
          |> Map.put(:group_id, group_id)
          |> Map.put(:type, :group)
        ref = make_ref()
        send(pid, {:start_worker, self(), ref, mfa_or_fun, opts})
        api_receiver(ref, timeout)
    end
  end
  defp do_start_child(sup_id, {:chain, chain_id}, mfa_or_fun, opts, timeout) do
    Logger.debug("Starting child process with options: #{inspect(opts)}")
    with {:ok, opts} <- Chain.check_worker_options([{:chain_id, chain_id} | opts] ),
      {:ok, _} <- verify_running(sup_id),
      {:ok, pid} <- get_host_partition(sup_id, chain_id) do
        opts =
          opts
          |> Map.put(:chain_id, chain_id)
          |> Map.put(:type, :chain)

        ref = make_ref()
        send(pid, {:start_worker, self(), ref, mfa_or_fun, opts})
        api_receiver(ref, timeout)
    end
  end

  defp verify_running(id) when is_atom(id) do
    case Process.whereis(id) do
      nil ->
        {:error, :not_running}
      pid ->
        {:ok, pid}
    end
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

  def get_partition_id(sup_id, partition_id) do
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
    case KV.get({sup_id, :pid, partition_id}) do
      {:ok, pid} ->
        {:ok, pid}
      _ ->
        {:error, :not_found}
    end
  end

  def get_host_partition(sup_id, data) do
    with {:ok, num} <- KV.get({sup_id, :number_of_partitions}),
      order <- get_hash_order(data, num),
      partition_id <- get_partition_id(sup_id, order),
      {:ok, pid} <- get_partition_pid(sup_id, partition_id) do
        {:ok, pid}
    else
      error ->
        Logger.error("Get partition pid failed: #{inspect error}, data: #{inspect data}, sup_id: #{inspect sup_id}")
        error
    end
  end

end
