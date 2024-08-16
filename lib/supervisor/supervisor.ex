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

  alias SuperWorker.Supervisor.{Group, Chain, Worker}

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

  def send_data_to_chain(chain_id, data) do
    case Process.whereis(@name) do
      nil ->
        Logger.error("Supervisor is not running.")
        {:error, :not_running}
      pid ->
        ref = make_ref()
        send(pid, {:add_data_to_chain, self(), ref, chain_id, data})
        api_receiver(ref, 5000)
    end
  end

  def boardcast_to_group(group_id, data) do
    case Process.whereis(@name) do
      nil ->
        Logger.error("Supervisor is not running.")
        {:error, :not_running}
      pid ->
        ref = make_ref()
        send(pid, {:broadcast_to_group, self(), ref, group_id, data})
        api_receiver(ref, 5000)
    end
  end

  def broadcast_to_my_group(data) do
    id = get_my_group()
    sup = get_my_supervisor()

    cond do
      id == nil ->
        Logger.error("Group not found.")
        {:error, :not_found}
      sup == nil ->
        Logger.error("Supervisor not found.")
        {:error, :not_found}
      true ->
        boardcast_to_group(id, data)
    end
  end

  def get_my_group() do
    Process.get(:group)
  end

  def get_my_supervisor() do
    Process.get(:supervisor)
  end

  def get_chain(chain_id) do
    case Process.whereis(@name) do
      nil ->
        Logger.error("Supervisor is not running.")
        {:error, :not_running}
      pid ->
        ref = make_ref()
        send(pid, {:get_chain, self(), ref, chain_id})
        api_receiver(ref, 5000)
    end
  end

  ## GenServer callbacks

  def init(opts) do

    state = %{
      groups: %{}, # storage for group processes
      chains: %{}, # storage for chain processes
      standalone: %{}, # storage for standalone processes
      ref_to_id: %{}, # storage for refs to id & type
      id: @name,
      owner: opts[:owner]
    }

    Process.register(self(), @name)

    send(state.owner, {{self(), state.owner}, :ok})

    # Start the main loop
    main_loop(state, opts)
  end


  ## Private functions

  # Main loop for the supervisor.
  defp main_loop(state, sup_opts) do
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

      {:DOWN, ref, :process, pid, reason} ->
        Logger.debug("receiced signal worker died: #{inspect(pid)}, reason: #{inspect(reason)}")

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
          child = Map.get(state, id)
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
        Process.put(:supervisor, state.id)
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

  defp add_new_group(state, opts) do
    group = %Group{id: opts.id, restart_strategy: opts.restart_strategy, supervisor: state.id}

    groups = Map.put(state.groups, group.id, group)

    # Update the state
    state
    |> Map.put(:groups, groups)
  end

  defp add_new_chain(state, opts) do
    # Improve the chain process.
    chain = %Chain{id: opts.id, restart_strategy: opts.restart_strategy, supervisor: state.id,
    finished_callback: opts.finished_callback}

    chains = Map.put(state.chains, chain.id, chain)

    # Update the state
    state
    |> Map.put(:chains, chains)
  end

  defp do_start_child(:standalone, mfa_or_fun, opts, timeout) do
    Logger.debug("Starting child process with options: #{inspect(opts)}")
    with {:ok, opts} <- Worker.check_options(opts),
      {:ok, pid} <- verify_running() do
        opts =
          opts
          |> Map.put(:type, :standalone)

        ref = make_ref()
        send(pid, {:start_worker, self(), ref, mfa_or_fun, opts})
        api_receiver(ref, timeout)
    else
      other ->
        Logger.info("Something happened: #{inspect other}")
        other
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
