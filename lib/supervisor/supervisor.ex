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

      iex> SuperWorker.Supervisor.start_link()
      {:ok, pid}
  """


  require Logger

  alias SuperWorker.Supervisor.{Group, Chain, Standalone}

  import SuperWorker.Supervisor.Utils

  # TO-DO: Move to option for flexibility.
  @name __MODULE__

  ## Public APIs

  def start_link(opts \\ []) when is_list(opts) do
    case Process.whereis(@name) do
      nil ->
        Logger.error("Starting supervisor...")
        # Start the supervisor
        pid = spawn_link(__MODULE__, :init, [opts])
        api_receiver({pid, self()})
      pid ->
        Logger.error("Supervisor is already running (pid: #{inspect(pid)}).")
        {:error, :already_running}
    end
  end

  @doc """
  Start supervisor for run standalone.
  """
  def start(opts \\ []) when is_list(opts) do
    case Process.whereis(@name) do
      nil ->
        Logger.debug("Starting supervisor...")

        opts = default_sup_opts(opts)

        # Start the supervisor
        pid = spawn(__MODULE__, :init, [opts])
        api_receiver({pid, self()})
      pid ->
        Logger.error("Supervisor is already running (pid: #{inspect(pid)}).")
        {:error, :already_running}
    end

  end

  @doc """
  Start a child process.
  Can run standalone or in a group/chain.
  """
  def add_standalone_worker(mfa_or_fun, opts, timeout \\ 5_000)
  def add_standalone_worker({m, f, a} = mfa, opts, timeout)
   when is_list(opts) and is_atom(m) and is_atom(f) and is_list(a) do
    do_start_child(:standalone, mfa, opts, timeout)
  end
  def add_standalone_worker(fun, opts, timeout) when is_list(opts) and is_function(fun, 0) do
    do_start_child(:standalone, {:fun, fun}, opts, timeout)
  end

  def add_group_worker(group_id, mfa_or_fun, opts, timeout \\ 5_000)
  def add_group_worker(group_id, {m, f, a} = mfa, opts, timeout)
   when is_list(opts) and is_atom(m) and is_atom(f) and is_list(a) do
    do_start_child({:group, group_id}, mfa, opts, timeout)
  end
  def add_group_worker(group_id, fun, opts, timeout) when is_list(opts) and is_function(fun, 0) do
    do_start_child({:group, group_id}, {:fun, fun}, opts, timeout)
  end

  def add_chain_worker(chain_id, mfa_or_fun, opts, timeout \\ 5_000)
  def add_chain_worker(chain_id, {m, f, a} = mfa, opts, timeout)
   when is_list(opts) and is_atom(m) and is_atom(f) and is_list(a) do
    do_start_child({:chain, chain_id}, mfa, opts, timeout)
  end
  def add_chain_worker(chain_id, fun, opts, timeout) when is_list(opts) and is_function(fun, 0) do
    do_start_child({:chain, chain_id}, {:fun, fun}, opts, timeout)
  end

  @doc """
  Add a group of processes to the supervisor.
  """
  def add_group(opts, timeout \\ 5_000) do
    with {:ok, opts} <- Group.check_options(opts),
      {:ok, pid} <- verify_running() do
        ref = make_ref()
        send(pid, {:add_group, self(), ref, opts})
        api_receiver(ref, timeout)
    end
  end

  @doc """
  get group info
  """
  def get_group_info(group_id) do
    case Process.whereis(@name) do
      nil ->
        Logger.error("Supervisor is not running.")
        {:error, :not_running}
      pid ->
        ref = make_ref()
        send(pid, {:get_group_info, self(), ref, group_id})
        api_receiver(ref, 5000)
    end
  end

  @doc """
  Start a chain of processes.
  """
  def add_chain(opts, timeout \\ 5_000) do
    with {:ok, opts} <- Chain.check_options(opts),
      {:ok, pid} <- verify_running() do
        ref = make_ref()
        send(pid, {:add_chain, self(), ref, opts})
        api_receiver(ref, timeout)
    end
  end

  ## GenServer callbacks

  def init(opts) do

    state = %{
      groups: %{}, # storage for group processes
      chains: %{}, # storage for chain processes
      standalone: %{}, # storage for standalone processes
      ref_to_id: %{}, # storage for refs to id & type
      id: nil,
      owner: opts[:owner]
    }

    Process.register(self(), __MODULE__)

    send(state.owner, {{self(), state.owner}, :ok})

    # Start the main loop
    main_loop(state, opts)
  end


  ## Private functions

  # Main loop for the supervisor.
  defp main_loop(state, sup_opts) do
    receive do
      {:start_worker, from, ref, mfa_or_fun, opts} ->
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
               # TO-DO: Implement the chain process.
               :not_implemented
            :standalone ->
              if Map.has_key?(state.standalone, opts.id) do
                true
              else
                :worker_already_exists
              end
          end
        if runable == true do # start child process.
            state = sup_start_child(state, mfa_or_fun, opts)
            send(from, {ref, {:ok, opts.id}})

            main_loop(state, sup_opts)
        else # not found group or chain, return error to the caller.
          send(from, {ref, {:error, runable}})

          main_loop(state, sup_opts)
        end

      {:DOWN, ref, :process, pid, reason} ->
        Logger.debug("Child process died: #{inspect(pid)}, reason: #{inspect(reason)}")

        state
        |> process_child_down(pid, ref, reason)
        |> main_loop(sup_opts)

      # add group from api.
      {:add_group, from, ref, opts} ->
        case Map.has_key?(state.groups, opts.id) do
          true ->
            Logger.error("Group already exists: #{inspect(opts.id)}")
            send(from, {ref, {:error, :already_exists}})
            main_loop(state, sup_opts)
          false ->
            Logger.debug("Adding group: #{inspect(opts.id)}")

            # Send the response to the caller.
            send(from, {ref, {:ok, opts.id}})

            state
            |> add_new_group(opts)
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
    {:add_chain, from, ref, opts} ->
      case Map.has_key?(state.chains, opts.id) do
        true ->
          Logger.error("Chain already exists: #{inspect(opts.id)}")
          send(from, {ref, {:error, :already_exists}})
          main_loop(state, sup_opts)
        false ->
          Logger.debug("Adding chain: #{inspect(opts.id)}")

          # Send the response to the caller.
          send(from, {ref, {:ok, opts.id}})

          state
          |> add_new_chain(opts)
          |> main_loop(sup_opts)
      end


      unknown ->
        Logger.warning("#{inspect state.id}, unknown message: #{inspect(unknown)}")
        main_loop(state, sup_opts)
    end

  end

  defp process_child_down(state, pid, ref, reason) do
    {id, type} = state.ref_to_id[ref]
    case type do
      :standalone ->
        child = Map.get(state, id)
        restart_standalone(state, child, {pid, reason})
      {:group, group_id} ->
        group = Map.get(state.groups, group_id)
        restart_group(state, group, id, {pid, reason})
      {:chain, chain_id} ->
        Map.get(state.chains, chain_id)
        |> Map.get(id)
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

  defp restart_group(state, %{restart_strategy: :one_for_all} = group, child_id, {pid, reason}) do
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

  defp restart_chain(state, chain, {pid, reason}) do
    # TO-DO: Implement the chain process.
    state
  end

  defp sup_start_child(state, mfa, %{id: id, type: :standalone} = opts) do
    # Start a child process
    Logger.debug("Starting standalone worker process(#{inspect(id)})")

    {pid, ref} =
      case mfa do
        {:fun, fun} ->
          spawn_monitor(fun)
        {m, f, a} ->
          spawn_monitor(fn ->
            apply(m, f, a)
          end)
      end

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

    group = Map.get(state.groups, group_id, :not_found_group)

    # update group.
    {:ok, group} = Group.add_worker(group, %{id: id, mfa: fun_mfa, opts: opts})
    groups = Map.put(state.groups, group_id, group)
    {:ok, worker} = Group.get_worker(group, id)

    ref_to_id = Map.put(state.ref_to_id, worker.ref, {id, {:group, group_id}})

    state
    |> Map.put(:groups, groups)
    |> Map.put(:ref_to_id, ref_to_id)
  end

  defp sup_start_child(state, mfa, %{id: id, chain_id: chain_id, type: :chain} = opts) do
    Logger.debug("Starting child process(#{inspect(id)}) for chain #{inspect(chain_id)}")

    chain = Map.get(state.chains, chain_id, %{children: %{}, child_pids: []})

    {pid, ref} = spawn_chain(mfa, chain.child_pids, {id, chain_id})

    # update chain.
    chain = Map.put(chain, id, %{id: id, pid: pid, ref: ref, mfa: mfa, opts: opts})

    # add/update ref to id.
    refs = Map.put(state.refs, ref, {id, {:chain, chain_id}})

    state
    |> Map.put(chain_id, chain)
    |> Map.put(:refs, refs)
  end

  defp spawn_chain(mfa, [], opts) do
    # Start a new process in the chain
    spawn_monitor(fn ->
      loop_chain(mfa, opts)
    end)
  end
  defp spawn_chain(mfa, [child_pid | _], opts) do
    # Start a new process in the chain
    spawn_monitor(fn ->
      # Link to the child process, just link to first child process for save resources.
      # So, if the child process dies, the new process will also die.
      # Avoid turning on trap_exit.
      Process.link(child_pid)

      loop_chain(mfa, opts)
    end)
  end

  # Support receive data from the previous process in the chain and pass it to the next process.
  defp loop_chain(mfa, {id, chain_id} = opts) do
    receive do
      {:new_data, data} ->
        result =
          case mfa do
            {:fun, f} ->
              f.(data)
            {m, f, a} ->
              apply(m, f, [data | a])
          end
        case result do
          {:next, new_data} ->
            Logger.debug("Passing data to the next process(#{inspect(id)}), chain: #{inspect(chain_id)}")
            send(self(), {:new_data, new_data})
            loop_chain(mfa, opts)
          {:error, reason} ->
            Logger.error("Error in chain process(#{inspect(id)}), chain: #{inspect(chain_id)}: #{inspect(reason)}")
            # TO-DO: decide to ignore or stop the chain.
          {:drop, reason} ->
            Logger.debug("Dropping chain process(#{inspect(id)}), chain: #{inspect(chain_id)}: #{inspect(reason)}")
            loop_chain(mfa, opts)
          {:stop} ->
            Logger.debug("Stopping chain process(#{inspect(id)}), chain: #{inspect(chain_id)}")
        end

      {:stop, ^chain_id} ->
        Logger.debug("Stopping chain process(#{inspect(id)}), chain: #{inspect(chain_id)}")
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

  defp add_new_group(state, opts) do
    group = %Group{id: opts.id, workers: %{}, restart_strategy: opts.restart_strategy}

    groups = Map.put(state.groups, group.id, group)

    # Update the state
    state
    |> Map.put(:groups, groups)
  end

  defp add_new_chain(state, opts) do
    chain = %Chain{id: opts.id, restart_strategy: opts.restart_strategy}

    chains = Map.put(state.chains, chain.id, chain)

    # Update the state
    state
    |> Map.put(:chains, chains)
  end

  defp do_start_child(:standalone, mfa_or_fun, opts, timeout) do
    Logger.debug("Starting child process with options: #{inspect(opts)}")
    with {:ok, opts} <- Standalone.check_options(opts),
      {:ok, pid} <- verify_running() do
        opts =
          opts
          |> Map.put(:type, :standalone)

        ref = make_ref()
        send(pid, {:start_worker, self(), ref, mfa_or_fun, opts})
        api_receiver(ref, timeout)
    end
  end
  defp do_start_child({:group, group_id}, mfa_or_fun, opts, timeout) do
    Logger.debug("Starting child process with options: #{inspect(opts)}")
    with {:ok, opts} <- Group.check_worker_opts([{:group_id, group_id} | opts]),
      {:ok, pid} <- verify_running() do
        opts =
          opts
          |> Map.put(:group_id, group_id)
          |> Map.put(:type, :group)

        ref = make_ref()
        send(pid, {:start_worker, self(), ref, mfa_or_fun, opts})
        api_receiver(ref, timeout)
    end
  end
  defp do_start_child({:chain, chain_id}, mfa_or_fun, opts, timeout) do
    Logger.debug("Starting child process with options: #{inspect(opts)}")
    with {:ok, opts} <- Chain.check_worker_options([{:chain_id, chain_id} | opts] ),
      {:ok, pid} <- verify_running() do
        opts =
          opts
          |> Map.put(:chain_id, chain_id)
          |> Map.put(:type, :chain)

        ref = make_ref()
        send(pid, {:start_worker, self(), ref, mfa_or_fun, opts})
        api_receiver(ref, timeout)
    end
  end

  defp verify_running() do
    case Process.whereis(@name) do
      nil ->
        {:error, :not_running}
      pid ->
        {:ok, pid}
    end
  end
end
