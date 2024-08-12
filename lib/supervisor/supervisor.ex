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
  def add_worker(mfa_or_fun, opts, timeout \\ 5_000)
  def add_worker({m, f, a} = mfa, opts, timeout)
   when is_list(opts) and is_atom(m) and is_atom(f) and is_list(a) do
    do_start_child(mfa, opts, timeout)
  end
  def add_worker(fun, opts, timeout) when is_list(opts) and is_function(fun, 0) do
    do_start_child({:fun, fun}, opts, timeout)
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
      children: %{}, # storage for standalone processes
      refs: %{}, # storage for refs to id & type
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
              Map.has_key?(state.groups, opts.group_id)
            :chain ->
              Map.has_key?(state.chains, opts.chain_id)
            :standalone ->
              true
          end
        if runable do # start child process.
          if Map.has_key?(state.children, opts.id) do
            Logger.error("Child already exists: #{inspect(opts.id)}")
            send(from, {ref, {:error, :already_exists}})
            main_loop(state, sup_opts)
          else
            state = sup_start_child(state, mfa_or_fun, opts)

            send(from, {ref, {:ok, opts.id}})
            main_loop(state, sup_opts)
          end

        else # not found group or chain, return error to the caller.
          reason = case opts.type do
            :group ->
              Logger.error("Group not found: #{inspect(opts.group_id)}")
              :group_not_found
            :chain ->
              Logger.error("Chain not found: #{inspect(opts.chain_id)}")
              :chain_not_found
          end
          send(from, {ref, {:error, reason}})

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
    {id, type} = state.refs[ref]
    child =  case type do
      :standalone -> Map.get(state.children, id)
      :group -> Map.get(state.groups, id)
      :chain -> Map.get(state.chains, id)
    end
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
    refs = Map.put(state.refs, ref, {id, :standalone})
    children = Map.put(state.children, id, %{id: id, pid: pid, ref: ref, mfa: mfa, opts: opts})

    # return new state, including the new child
    state
    |> Map.put(:children, children)
    |> Map.put(:refs, refs)
  end

  defp sup_start_child(state, fun_mfa, %{id: id, group_id: group_id, type: :group} = opts) do
    # Start a child process
    Logger.debug("Starting child process(#{inspect(id)}) for group #{inspect(group_id)}")

    group = Map.get(state.groups, group_id, :not_found_group)

    child_pid =
      case Enum.take(group.children, 1) do
        [] -> nil
        [child] -> child.pid
      end

    {pid, ref} = spawn_group(fun_mfa, group.restart_strategy, child_pid)

    # update group.
    group = Map.put(group, id, %{id: id, pid: pid, ref: ref, mfa: fun_mfa, opts: opts})
    groups = Map.put(state.groups, group_id, group)

    # add/update ref to id.
    refs = Map.put(state.refs, ref, {id, :group})

    state
    |> Map.put(:groups, groups)
    |> Map.put(:refs, refs)
  end

  defp sup_start_child(state, mfa, %{id: id, chain_id: chain_id, type: :chain} = opts) do
    Logger.debug("Starting child process(#{inspect(id)}) for chain #{inspect(chain_id)}")

    chain = Map.get(state.chains, chain_id, %{children: %{}, child_pids: []})

    {pid, ref} = spawn_chain(mfa, chain.child_pids, {id, chain_id})

    # update chain.
    chain = Map.put(chain, id, %{id: id, pid: pid, ref: ref, mfa: mfa, opts: opts})

    # add/update ref to id.
    refs = Map.put(state.refs, ref, {id, :chain})

    state
    |> Map.put(chain_id, chain)
    |> Map.put(:refs, refs)
  end

  defp spawn_group({m, f, a}, restart_strategy, child_pid) do
    # Start a new process in the group
    spawn_monitor(fn ->
      # Link to other child process.
      # So, if the child process dies, the new process will also die.
      # Avoid turning on trap_exit.
      case restart_strategy do
        :one_for_all ->
          if child_pid do
            Process.link(child_pid)
          end
        :one_for_one ->
          :ok
      end

      apply(m, f, a)
    end)
  end
  defp spawn_group({:fun, f}, restart_strategy, child_pid) do
    # Start a new process in the group
    spawn_monitor(fn ->
      # Link to a child process.
      # So, if the child process dies, the new process will also die.
      # Avoid turning on trap_exit.
      case restart_strategy do
        :one_for_all ->
          if child_pid do
            Process.link(child_pid)
          end
        :one_for_one ->
          :ok
      end

      f.()
    end)
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

  defp do_start_child(mfa_or_fun, opts, timeout) do
    Logger.debug("Starting child process with options: #{inspect(opts)}")
    with {:ok, opts} <- Standalone.check_options(opts),
      {:ok, pid} <- verify_running() do
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
