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

  import SuperWorker.Supervisor.Utils

  ## Public APIs

  def start_link(opts) do
    # Start the supervisor
    spawn_link(__MODULE__, :init, [opts])
  end

  @doc """
  Start supervisor for run standalone.
  """
  def start(opts) do
    # Start the supervisor
    spawn(__MODULE__, :init, [opts])
  end

  def start_child({_, _, _} = mfa, opts) when is_list(opts) do

    case validate_opts(opts) do
      {:ok, opts} ->
        spawn_link(__MODULE__, :start_child, [mfa, opts])
      {:error, reason} = result ->
        Logger.error("Invalid options: #{inspect(reason)}")
        result
    end
  end
  def start_child(fun, opts) when is_list(opts) and is_function(fun, 0) do
    case validate_opts(opts) do
      {:ok, opts} ->
        spawn_link(__MODULE__, :start_child, [{:func, fun}, opts])
      {:error, reason} = result ->
        Logger.error("Invalid options: #{inspect(reason)}")
        result
    end
  end


  ## GenServer callbacks

  def init(opts) do

    state = %{
      groups: %{},
      chains: %{},
      childs: %{},
      id: nil
    }
    # Start the main loop
    main_loop(state, opts)
  end


  ## Private functions

  defp main_loop(state, opts) do
    # Loop through the processes

    receive do
      {:call, from, {:start, group_id, mfa = {_, _, _}}} ->
        # Start the process
        # Send the response to the caller
        state = start_child(mfa, group_id, state)

        main_loop(state, opts)
      {:cast, :stop, child_id} ->
        # TO-DO: Stop the child process.
        main_loop(state, opts)
      {:cast, :stop, :group_id} ->
        :ok
      unknown ->
        Logger.warning("#{inspect state.id}, unknown message: #{inspect(unknown)}")
        main_loop(state, opts)
    end

  end

  defp start_child({m, f, a}, %{id: id, group_id: nil} = opts, state) do
    # Start a child process
    Logger.debug("Starting freedom child process(#{inspect(id)}): #{inspect({m, f, a})}")

    {pid, ref} = spawn_monitor(m, f, a)

    # return new state, including the new child
    state
    |> Map.put(id, %{id: id, pid: pid, ref: ref, mfa: {m, f, a}, opts: opts})
  end

  defp start_child({m, f, a} = mfa, %{id: id, group_id: group_id, type: :group} = _opts, state) do
    # Start a child process
    Logger.debug("Starting child process(#{inspect(id)}) for group #{inspect(group_id)}: #{inspect({m, f, a})}")

    group = Map.get(state.groups, group_id, %{children: %{}, child_pids: []})

    {pid, ref} = spawn_group(mfa, group.child_pids)

    # update group.
    group = Map.put(group, id, %{id: id, pid: pid, ref: ref, mfa: mfa})

    # return new state, including updated group.
    state
    |> Map.put(group_id, group)
  end

  defp start_child({m, f, a} = mfa, %{id: id, chain_id: chain_id, type: :chain} = _opts, state) do
    Logger.debug("Starting child process(#{inspect(id)}) for chain #{inspect(chain_id)}: #{inspect({m, f, a})}")

    chain = Map.get(state.chains, chain_id, %{children: %{}, child_pids: []})

    {pid, ref} = spawn_chain(mfa, chain.child_pids, {id, chain_id})

    # update chain.
    chain = Map.put(chain, id, %{id: id, pid: pid, ref: ref, mfa: mfa})

    # return new state, including updated chain.
    state
    |> Map.put(chain_id, chain)
  end

  # Start a the first process in the group, ignore link process in this case.
  defp spawn_group({m, f, a}, []) do
    # Start a new process in the group
    spawn_monitor(fn ->
      apply(m, f, a)
    end)
  end
  defp spawn_group({m, f, a}, [child_pid | _]) do
    # Start a new process in the group
    spawn_monitor(fn ->
      # Link to the child process, just link to first child process for save resources.
      # So, if the child process dies, the new process will also die.
      # Avoid turning on trap_exit.
      Process.link(child_pid)

      apply(m, f, a)
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
  defp loop_chain(mfa = {m, f, a}, {id, chain_id} = opts) do
    receive do
      {:new_data, data} ->
        result = apply(m, f, [data | a])
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


end
