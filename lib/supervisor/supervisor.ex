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

  alias __MODULE__

  alias Supervisor.{Group, Chain, Worker, Message}

  import Supervisor.Utils

  require Logger

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

  @sup_params [:id, :number_of_partitions, :link, :report_to, :children]

  alias __MODULE__
  alias Supervisor.Db

  # Default timeout (miliseconds) for API calls.
  @default_time 3_000

  # List message from api.
  @api_messages [
    :start_worker,
    :get_group,
    :remove_group_worker,
    :restart_group_worker,
    :get_chain,
    :send_to_group,
    :send_to_group_random,
    :add_data_to_chain,
    :send_to_worker,
    :remove_group_worker,
    :add_group,
    :add_chain,
    :stop
  ]

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
    with {:ok, sup} <- check_opts(opts),
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

  def add_standalone_worker(sup_id, {genserver_module, _} = f, opts, timeout)
      when is_list(opts) and is_atom(genserver_module) do
    {:ok, %{mfa: mfa}} = SuperWorker.ConfigLoader.Parser.convert_regular_child_spec(f)

    do_add_standalone_worker(sup_id, [{:fun, mfa} | opts], timeout)
  end

  def add_standalone_worker(sup_id, genserver_module, opts, timeout)
      when is_list(opts) and is_atom(genserver_module) do
    add_standalone_worker(sup_id, {genserver_module, []}, opts, timeout)
  end

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

  def add_group_worker(sup_id, group_id, {:gen_server, {m, f, a}} = mfa, opts, timeout)
      when is_list(opts) and is_atom(m) and is_atom(f) and is_list(a) and group_id != nil do
    do_add_group_worker(sup_id, group_id, [{:fun, mfa} | opts], timeout)
  end

  def add_group_worker(sup_id, group_id, fun, opts, timeout)
      when is_list(opts) and is_function(fun, 0) do
    do_add_group_worker(sup_id, group_id, [{:fun, {:fun, fun}} | opts], timeout)
  end

  def add_group_worker(sup_id, group_id, module, opts, timeout)
      when is_atom(module) and group_id != nil do
    specs = SuperWorker.ConfigLoader.Parser.convert_regular_child_spec(module)
    do_add_group_worker(sup_id, {:group_id, group_id}, [{:fun, specs.mfa} | opts], timeout)
  end

  def add_group_worker(sup_id, group_id, {module, _} = worker, opts, timeout)
      when is_atom(module) and group_id != nil do
    specs = SuperWorker.ConfigLoader.Parser.convert_regular_child_spec(worker)
    do_add_group_worker(sup_id, group_id, [{:fun, specs.mfa} | opts], timeout)
  end

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
  Add a group to the supervisor.
  Group's options follow docs in `Group` module.
  """
  @spec add_group(atom(), list(), integer()) :: {:ok, atom()} | {:error, any()}
  def add_group(sup_id, opts, timeout \\ @default_time) do
    with {:ok, group} <- Group.check_options(opts),
         true <- is_running?(sup_id),
         {:error, _} <- get_group(sup_id, group.id),
         {:ok, parititon_id, pid} <- get_host_partition(sup_id, group.id) do
      %Group{} = group
      group = %{group | supervisor: sup_id, partition: parititon_id}
      call_api(pid, :add_group, group, timeout)
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
  def get_group(sup_id, group_id) do
    Db.get_group(sup_id, group_id)
  end

  @doc """
  Add a chain to the supervisor.
  Chain's options follow docs in `Chain` module.
  """
  def add_chain(sup_id, opts, timeout \\ 5_000) do
    with {:ok, chain} <- Chain.check_options(opts),
         true <- is_running?(sup_id),
         {:error, _} <- get_chain(sup_id, chain.id),
         {:ok, parition_id, pid} <- get_host_partition(sup_id, chain.id) do
      %Chain{} = chain
      chain = %{chain | supervisor: sup_id, partition: parition_id}
      call_api(pid, :add_chain, chain, timeout)
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
    with {:ok, pid} <- verify_and_get_pid(sup_id, chain_id) do
      call_api(pid, :add_data_to_chain, {chain_id, data}, timeout)
    end
  end

  @doc """
  Send data directly to the worker standalone in the supervisor.
  """
  def send_to_worker(sup_id, worker_id, data, timeout \\ @default_time) do
    with true <- is_running?(sup_id),
         [{pid, _}] <- Db.get_worker_by_id(sup_id, worker_id, {:standalone, nil}) do
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

  def get_my_group() do
    Process.get({:supervisor, :group_id})
  end

  def get_my_supervisor() do
    Process.get({:supervisor, :sup_id})
  end

  def get_chain(sup_id, chain_id) do
    Db.get_chain(sup_id, chain_id)
  end

  def remove_group_worker(sup_id, group_id, worker_id, timeout \\ @default_time) do
    with {:ok, pid} <- verify_and_get_pid(sup_id, group_id) do
      call_api(pid, :remove_group_worker, {worker_id, group_id}, timeout)
    end
  end

  ## Internal public functions

  def init(sup = %Supervisor{}, ref) do
    # Register the supervisor process.
    Process.register(self(), get_master_id(sup.id))

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

      api_response(ref, {:error, :failed_to_start_partitions})
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

      api_response(ref, {:ok, self()})

      # Start the main loop
      main_loop(state)
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

    send(get_master_id(state.master), {:partition_started, state.id})

    # Turn partition process to system process.
    Process.flag(:trap_exit, true)

    main_loop(state)
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

  # Main loop for the supervisor & partition.
  def main_loop(state) do
    receive do
      {msg_type, _, _} = msg when msg_type in @api_messages ->
        Logger.debug(
          "SuperWorker, Supervisor, #{state.id} received a api message: #{inspect(msg)}"
        )

        process_api_message(state, msg)

      {:DOWN, _ref, :process, pid, reason} = msg ->
        Logger.debug(
          "SuperWorker, Supervisor, #{state.id} Worker died: #{inspect(pid)}, reason: #{inspect(reason)}"
        )

        process_worker_down(state, msg)

      {:EXIT, from, reason} ->
        process_exit_message(state, from, reason)

      {:stop_partition, type} ->
        Logger.info(
          "SuperWorker, Supervisor, #{state.id} Stopping supervisor partition, for #{inspect(self())}"
        )

        # Stop the supervisor.
        shutdown(state, type)

        send(get_master_id(state.master), {:partition_stopped, state.id})

      unknown ->
        Logger.warning(
          "SuperWorker, Supervisor, #{state.id} main_loop, unknown message: #{inspect(unknown)}"
        )

        main_loop(state)
    end

    Logger.debug("SuperWorker, Supervisor, #{state.id} #{inspect(self())} main loop exited.")
  end

  defp shutdown(state, :kill) do
    Logger.debug("SuperWorker, Supervisor, shutting down supervisor: #{inspect(state.id)}")

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

  # process exit message for outside processes.
  defp process_exit_message(state, from, reason) do
    Logger.debug(
      "SuperWorker, Supervisor, #{state.id} Exit message from: #{inspect(from)}, reason: #{inspect(reason)}"
    )

    if from in state.linked_pids do
      Logger.warning(
        "SuperWorker, Supervisor, #{state.id} exited follow external process (crashed): #{inspect(from)}"
      )

      raise "#{inspect(state.master)} crashed follow external process: #{inspect(from)}"
    else
      Logger.debug(
        "SuperWorker, Supervisor, #{state.id} skipped exit for internal process: #{inspect(from)}, reason: #{inspect(reason)}"
      )

      main_loop(state)
    end
  end

  # Add new worker to group/chain/standalone.
  defp process_api_message(state, {:start_worker, ref, worker = %Worker{}}) do
    runable =
      case worker.type do
        :group ->
          cond do
            !has_group?(state, worker.parent) ->
              :group_not_found

            has_group_worker?(state, worker.parent, worker.id) ->
              :worker_already_exists

            true ->
              true
          end

        :chain ->
          cond do
            !has_chain?(state, worker.parent) ->
              :chain_not_found

            has_chain_worker?(state, worker.parent, worker.id) ->
              :worker_already_exists

            true ->
              true
          end

        :standalone ->
          if has_standalone_worker?(state, worker.id) do
            :worker_already_exists
          else
            true
          end
      end

    # start child process.
    # not found group or chain, return error to the caller.
    state =
      if runable == true do
        Logger.debug(
          "SuperWorker, Supervisor, #{state.id} Everything is fine, starting worker: #{inspect(worker)}"
        )

        result =
          case worker.type do
            :group ->
              with {:ok, group} <- get_group(state.master, worker.parent) do
                Group.add_worker(group, worker)
                {:ok, worker.id}
              else
                _ ->
                  {:error, :not_found}
              end

            :chain ->
              with {:ok, chain} <- get_chain(state.master, worker.parent) do
                Chain.add_worker(chain, worker)
                {:ok, worker.id}
              else
                _ ->
                  {:error, :not_found}
              end

            :standalone ->
              sup_start_child(state, worker)
              {:ok, worker.id}
          end

        api_response(ref, result)

        state
      else
        Logger.error(
          "SuperWorker, Supervisor, #{state.id} Error when starting worker: #{inspect(runable)}"
        )

        api_response(ref, {:error, runable})
        state
      end

    main_loop(state)
  end

  # Get chain in supervisor and return to the caller.
  defp process_api_message(state, {:get_chain, ref, chain_id}) do
    result = Db.get_chain(state.master, chain_id)

    api_response(ref, result)

    main_loop(state)
  end

  # broadcast a data to all worker in group.
  defp process_api_message(state, {:broadcast_to_group, ref, {group_id, data}}) do
    result =
      with {:ok, group} <- Db.get_group(state.master, group_id) do
        Group.broadcast(group, data)

        :ok
      else
        {:error, _} = error ->
          Logger.error(
            "SuperWorker, Supervisor, #{state.id} Not found group with id #{inspect(group_id)}"
          )

          error
      end

    api_response(ref, result)

    main_loop(state)
  end

  # send data directly to worker from api.
  defp process_api_message(state, {:send_to_group, ref, {group_id, worker_id, data}}) do
    with {:ok, group} <- Db.get_group(state.master, group_id),
         {:ok, {_ref, pid}} <- Db.get_worker_by_id(state.master, worker_id, {:group, group_id}) do
      send(pid, data)
      api_response(ref, :ok)
    else
      failed ->
        Logger.error(
          "SuperWorker, Supervisor, supervisor #{state.id}, send to worker #{inspect(worker_id)} in group #{inspect(group_id)}, error: #{inspect(failed)}"
        )

        api_response(ref, failed)
    end

    main_loop(state)
  end

  # send data to random worker from api.
  defp process_api_message(state, {:send_to_group_random, ref, {group_id, data}}) do
    result =
      with {:ok, group} <- Db.get_group(state.master, group_id),
           workers <- Db.get_workers_by_parent(state.master, {:group, group_id}) do
        if length(workers) > 0 do
          {worker_id, _} = Enum.random(workers)

          Group.send_message(group, worker_id, data)
        else
          {:error, :no_worker}
        end
      else
        {:error, _} = error ->
          Logger.error(
            "SuperWorker, Supervisor, #{state.id} Group not found: #{inspect(group_id)}, error: #{inspect(error)}"
          )

          error
      end

    api_response(ref, result)

    main_loop(state)
  end

  # add data to chain from api.
  defp process_api_message(
         state,
         {:add_data_to_chain, {from, _} = ref, {chain_id, data}}
       ) do
    result =
      with {:ok, chain} <- Db.get_chain(state.master, chain_id) do
        msg = Message.new(from, nil, data)

        Logger.debug(
          "SuperWorker, Supervisor, #{state.id} Add data to chain: #{inspect(chain_id)}, msg: #{inspect(msg)}"
        )

        Chain.new_data(chain, msg)
      else
        error ->
          Logger.error(
            "SuperWorker, Supervisor, #{state.id} cannot send to chain, error: #{inspect(error)}"
          )

          {:error, :cannot_send}
      end

    Logger.debug(
      "SuperWorker, Supervisor, #{state.id} Added data to chain: #{inspect(chain_id)}, result: #{inspect(result)}"
    )

    api_response(ref, result)

    main_loop(state)
  end

  defp process_api_message(state, {:send_to_worker, ref, {worker_id, data}}) do
    result =
      with {:ok, {_ref, pid}} <- Db.get_worker_by_id(state.master, worker_id, {:standalone, nil}) do
        send(pid, data)
        :ok
      else
        error ->
          Logger.warning(
            "SuperWorker, Supervisor, not found standalone worker #{inspect(worker_id)}"
          )

          error
      end

    api_response(ref, result)

    main_loop(state)
  end

  # remove worker from group.
  defp process_api_message(state, {:remove_group_worker, ref, {worker_id, group_id}}) do
    result =
      with {:ok, group} <- Db.get_group(state.master, group_id) do
        Group.remove_worker(group, worker_id)
      else
        error ->
          Logger.error(
            "SuperWorker, Supervisor, #{state.id} Group not found: #{inspect(group_id)}"
          )

          error
      end

    api_response(ref, result)

    main_loop(state)
  end

  defp process_api_message(state, {:restart_group_worker, worker_id, group_id}) do
    Logger.debug(
      "SuperWorker, Supervisor, #{state.id} Starting worker process, worker id: #{inspect(worker_id)}, group id: #{inspect(group_id)}"
    )

    with {:ok, group} <- Db.get_group(state.master, group_id) do
      # Restart the worker process.
      Group.restart_worker(group, worker_id)
    end

    main_loop(state)
  end

  # add group from api.
  defp process_api_message(state, {:add_group, ref, group}) do
    case Db.get_group(state.master, group.id) do
      {:error, :not_found} ->
        Logger.debug("SuperWorker,Supervisor,  #{state.id} Adding group: #{inspect(group.id)}")

        state = add_new_group(state, group)
        # Send the response to the caller.
        api_response(ref, {:ok, group.id})

        state

      {:ok, _} ->
        Logger.error(
          "SuperWorker, Supervisor, #{state.id} Group already exists: #{inspect(group.id)}"
        )

        api_response(ref, {:error, :already_exists})

        state
    end
    |> main_loop()
  end

  # get group info from api.
  defp process_api_message(state, {:get_group, ref, group_id}) do
    result = Db.get_group(state.master, group_id)

    api_response(ref, result)
    main_loop(state)
  end

  # add chain from api.
  defp process_api_message(state, {:add_chain, ref, chain}) do
    case Db.get_chain(state.master, chain.id) do
      {:error, :not_found} ->
        Logger.debug("SuperWorker, Supervisor, #{state.id} Adding chain: #{inspect(chain.id)}")

        state = add_new_chain(state, chain)
        # Send the response to the caller.
        api_response(ref, {:ok, chain.id})

        state

      {:ok, _} ->
        Logger.error(
          "SuperWorker, Supervisor, #{state.id} Chain already exists: #{inspect(chain.id)}"
        )

        api_response(ref, {:error, :already_exists})

        state
    end
    |> main_loop()
  end

  # Stop supervisor from api.
  defp process_api_message(state, {:stop, ref, type}) do
    Logger.info(
      "SuperWorker, Supervisor, #{state.id} Stopping supervisor, request from #{inspect(ref)}"
    )

    {:ok, list_partitions} = Db.get_all_sup_pids(state.master)

    # Send shutdown signal to all partitions.
    Enum.each(list_partitions, fn {id, pid} ->
      Logger.debug(
        "SuperWorker, Supervisor, #{state.id} Sending shutdown signal to partition: #{inspect(id)} (#{inspect(pid)})"
      )

      send(pid, {:stop_partition, type})
    end)

    stopped_partitions =
      Enum.reduce(1..length(list_partitions), [], fn _, acc ->
        receive do
          {:partition_stopped, id} ->
            Logger.debug(
              "SuperWorker, Supervisor, #{state.id} received stopped partition msg from #{inspect(id)}"
            )

            [id | acc]
        after
          @default_time ->
            Logger.debug(
              "SuperWorker, Supervisor, #{state.id} timeout when stopping partition. Current list: #{inspect(acc)}"
            )

            acc
        end
      end)

    result =
      if length(list_partitions) != length(stopped_partitions) do
        Logger.error("SuperWorker, Supervisor, #{state.id} failed to stop partitions.")
        {:error, :failed_to_stop_partitions}
      else
        Logger.debug("SuperWorker, Supervisor, #{state.id} stopped all partitions.")
        {:ok, :stopped}
      end

    # stop worker on master.
    shutdown(state, type)

    # TO-DO: Clean KV store for supervisor.

    api_response(ref, result)

    exit(:normal)
  end

  defp process_api_message(state, unknown_msg) do
    Logger.warning(
      "SuperWorker, Supervisor, #{state.id} Unknown api message: #{inspect(unknown_msg)}"
    )

    main_loop(state)
  end

  defp process_worker_down(state, {:DOWN, _ref, :process, pid, :restart}) do
    Logger.debug(
      "SuperWorker, Supervisor, #{state.id} Ignore died process (restarting): #{inspect(pid)}"
    )

    main_loop(state)
  end

  defp process_worker_down(state, {:DOWN, _ref, :process, pid, :removed}) do
    Logger.debug(
      "SuperWorker, Supervisor, #{state.id} Ignore died process (removed by user): #{inspect(pid)}"
    )

    main_loop(state)
  end

  defp process_worker_down(state, {:DOWN, ref, :process, pid, reason}) do
    Logger.debug(
      "SuperWorker, Supervisor, child process died: #{inspect(pid)}, ref: #{inspect(ref)}, reason: #{inspect(reason)}"
    )

    with {:ok, {worker_id, parent, _} = info} <- Db.get_worker(state.master, ref),
         {:ok, worker} <- Db.get_worker_info(state.master, worker_id, parent) do
      Logger.debug(
        "SuperWorker, Supervisor, worker found: #{inspect(info)}, orig_pid: #{inspect(pid)}, restarting..."
      )

      # clean up old data
      Db.delete_worker(state.master, ref)

      case parent do
        {:standalone, nil} ->
          restart_standalone(state, worker, {pid, reason})

        {:group, group_id} ->
          Logger.debug("SuperWorker, Supervisor, restart worker for group #{inspect(group_id)}")

          with {:ok, group} <- Db.get_group(state.master, group_id) do
            restart_group(state, group, worker_id, {pid, reason})
          end

        {:chain, chain_id} ->
          Logger.debug("SuperWorker, Supervisor, restart worker for chain #{inspect(chain_id)}")

          with {:ok, chain} <- Db.get_chain(state.master, chain_id) do
            restart_chain(state, chain, worker, {pid, reason})
          end
      end
    end

    main_loop(state)
  end

  defp restart_standalone(state, %Worker{} = child, {pid, reason}) do
    case child.restart_strategy do
      :permanent ->
        Logger.debug(
          "SuperWorker, Supervisor, #{inspect(state.id)}, :permanent, restarting #{inspect(pid)}"
        )

        # Restart the child process
        sup_start_child(state, child)

      :transient when reason != :normal ->
        Logger.debug(
          "SuperWorker, Supervisor, #{inspect(state.id)}, :transient, reason down: #{inspect(reason)} restarting #{inspect(pid)}"
        )

        # Restart the child process
        sup_start_child(state, child)

      :transient ->
        Logger.debug(
          "SuperWorker, Supervisor, #{inspect(state.id)}, :transient, ignore restarting #{inspect(pid)}"
        )

        state

      :temporary ->
        Logger.debug(
          "SuperWorker, Supervisor, #{inspect(state.id)}, :temporary, ignore restarting #{inspect(pid)}"
        )

        state
    end
  end

  defp restart_group(
         state,
         %Group{restart_strategy: :one_for_one} = group,
         worker = %Worker{},
         {pid, reason}
       ) do
    case reason do
      :normal ->
        Logger.debug(
          "SuperWorker, Supervisor, #{inspect(state.id)}, Child process(#{inspect(pid)}) is shutdown with reason :normal, ignore restart phase."
        )

        state

      reason ->
        Logger.debug(
          "SuperWorker, Supervisor, #{inspect(state.id)}, Child process(#{inspect(pid)}) is down with reason: #{inspect(reason)}, restarting..."
        )

        Group.restart_worker(group, worker)

        state
    end
  end

  defp restart_group(
         state,
         %Group{restart_strategy: :one_for_all} = group,
         _worker,
         {pid, reason}
       ) do
    case reason do
      :normal ->
        Logger.debug(
          "SuperWorker, Supervisor, #{inspect(state.id)}, Child process(#{inspect(pid)}) is :normal shutdown, ignore restart phase."
        )

        state

      reason ->
        Logger.debug(
          "SuperWorker, Supervisor, #{inspect(state.id)}, Child process(#{inspect(pid)}) is down with reason #{inspect(reason)}, restarting..."
        )

        {:ok, list_worker_id} = Group.get_all_workers(group)

        old_ref_keys =
          Enum.reduce(list_worker_id, [], fn {_pid, worker_id}, acc ->
            {:ok, worker} = Group.get_worker(group, worker_id)
            [worker.ref | acc]
          end)

        Logger.debug(
          "SuperWorker, Supervisor, #{inspect(state.id)}, Old ref: #{inspect(old_ref_keys)}, group workers: #{inspect(list_worker_id)}"
        )

        # Clean up old process.
        # TO-DO: make sure pid, ref in worker struct is cleaned & correct after restart.
        Group.kill_all_workers(group, :restart)

        Enum.each(list_worker_id, fn {_worker_pid, worker_id} ->
          {:ok, _, pid} = get_host_partition(state.master, worker_id)

          Logger.debug(
            "SuperWorker, Supervisor, send restart signal to #{inspect(pid)} for group worker #{inspect(worker_id)}"
          )

          send(pid, {:restart_group_worker, worker_id, group.id})
        end)

        state
    end
  end

  # TO-DO: Follow restart strategy for child group & chain.

  defp restart_chain(
         state,
         %Chain{restart_strategy: :one_for_one} = chain,
         worker = %Worker{},
         {pid, reason}
       ) do
    case reason do
      :normal ->
        Logger.debug(
          "SuperWorker, Supervisor, worker(#{inspect(worker.id)}) process(#{inspect(pid)}) is normal, ignore restarting."
        )

        state

      _ ->
        Logger.debug(
          "SuperWorker, Supervisor, worker(#{inspect(worker.id)}) process(#{inspect(pid)}) is down, restarting."
        )

        Chain.restart_worker(chain, worker.id)
    end
  end

  defp restart_chain(
         state,
         %Chain{restart_strategy: :one_for_all} = chain,
         %Worker{},
         {pid, reason}
       ) do
    case reason do
      :normal ->
        Logger.debug(
          "SuperWorker, Supervisor, child process(#{inspect(pid)}) is normal, ignore restarting."
        )

        state

      _ ->
        Logger.debug(
          "SuperWorker, Supervisor, child process(#{inspect(pid)}) is down, restarting..."
        )

        Chain.restart_all_workers(chain)
    end
  end

  defp sup_start_child(state, %Worker{id: id, type: :standalone} = worker) do
    # Start a child process
    Logger.debug("SuperWorker, Supervisor, starting standalone worker process(#{inspect(id)})")

    result =
      case worker.fun do
        {:gen_server, {m, f, a}} ->
          case apply(m, f, a) do
            {:ok, pid} ->
              ref = Process.monitor(pid)
              {:ok, {pid, ref}}

            {:error, reason} ->
              Logger.error("Failed to start gen_server: #{inspect(reason)}")

              {:error, reason}
          end

        _ ->
          spawn_monitor(fn ->
            # Store for user can directly access to the worker.
            Process.put({:supervisor, :sup_id}, state.id)
            Process.put({:supervisor, :worker_id}, id)

            case worker.fun do
              {:fun, fun} ->
                fun.()

              {m, f, a} ->
                apply(m, f, a)
            end
          end)
      end

    case result do
      {:ok, {pid, ref}} ->
        # Link to child for case supervisor is down.
        Process.link(pid)

        Db.put_worker(state.master, ref, worker.id, {:standalone, nil}, pid)

      # ignore failed worker
      _ ->
        :ok
    end

    state
  end

  defp add_new_group(state, %Group{} = group) do
    group = %Group{group | supervisor: state.master, partition: state.id}
    Db.put_group(state.master, group)

    state
  end

  defp add_new_chain(state, %Chain{} = chain) do
    chain = %{chain | supervisor: state.master, partition: state.id}
    Db.put_chain(state.master, chain)

    state
  end

  @spec do_add_standalone_worker(atom(), list(), integer()) ::
          {:ok, any()} | {:error, any()}
  defp do_add_standalone_worker(sup_id, opts, timeout) do
    Logger.debug(
      "SuperWorker, Supervisor, starting standalone child process with options: #{inspect(opts)}"
    )

    with {:ok, opts} <- Worker.check_standalone_options([type: :standalone] ++ opts),
         {:ok, pid} <- verify_and_get_pid(sup_id, opts.id) do
      opts =
        opts
        |> Map.put(:type, :standalone)

      call_api(pid, :start_worker, opts, timeout)
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

    with {:ok, opts} <- Worker.check_group_options([group_id: group_id] ++ opts),
         {:ok, pid} <- verify_and_get_pid(sup_id, opts.id) do
      opts =
        opts
        |> Map.put(:parent, group_id)
        |> Map.put(:type, :group)

      Logger.debug(
        "SuperWorker, Supervisor, start call :start_worker api with opts: #{inspect(opts)}"
      )

      call_api(pid, :start_worker, opts, timeout)
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

    with {:ok, opts} <- Worker.check_chain_options([parent: chain_id, type: :chain] ++ opts),
         {:ok, pid} <- verify_and_get_pid(sup_id, opts.id) do
      opts =
        opts
        |> Map.put(:parent, chain_id)
        |> Map.put(:type, :chain)

      call_api(pid, :start_worker, opts, timeout)
    else
      error ->
        Logger.error(
          "SuperWorker, Supervisor, cannot add chain worker: #{inspect(error)}, options: #{inspect(opts)}"
        )

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
    # String.to_atom("#{Atom.to_string(id)}_master")
    id
  end

  defp check_opts(opts) do
    with {:ok, opts} <- normalize_opts(opts, @sup_params),
         {:ok, opts} <- default_sup_opts(opts),
         {:ok, opts} <- generic_default_sup_opts(opts),
         {:ok, opts} <- validate_opts(opts),
         {:ok, sup} <- map_to_struct(opts) do
      {:ok, sup}
    end
  end

  # Validate the type & value of options.
  defp validate_opts(opts) do
    with {:ok, opts} <- check_type(opts, :id, &is_atom/1),
         {:ok, opts} <- check_type(opts, :number_of_partitions, &is_integer/1),
         {:ok, opts} <- check_type(opts, :number_of_partitions, &(&1 > 0)),
         {:ok, opts} <- check_type(opts, :owner, &is_pid/1),
         {:ok, opts} <- check_type(opts, :link, &is_boolean/1) do
      {:ok, opts}
    else
      {:error, reason} = error ->
        Logger.error("SuperWorker, Supervisor, error in validating options: #{inspect(reason)}")
        error
    end
  end

  # Set the default options if not provided.
  # TO-DO: Merge with generic_default_sup_opts/1.
  defp default_sup_opts(opts) do
    opts =
      if Map.has_key?(opts, :number_of_partitions) do
        opts
      else
        Map.put(opts, :number_of_partitions, :erlang.system_info(:schedulers_online))
      end

    opts =
      if Map.has_key?(opts, :link) do
        opts
      else
        Map.put(opts, :link, true)
      end

    {:ok, opts}
  end

  # Start the supervisor main processes.
  defp start_supervisor(opts = %Supervisor{}, timeout) do
    Logger.debug("SuperWorker, Supervisor, starting supervisor with options: #{inspect(opts)}")

    ref = response_ref()

    # Start main process of the supervisor
    case opts.link do
      true ->
        Logger.debug("SuperWorker, Supervisor, starting supervisor with link.")
        opts = Map.put(opts, :linked_pids, [self()])
        spawn_link(Supervisor, :init, [opts, ref])

      false ->
        Logger.debug("SuperWorker, Supervisor, starting supervisor without link.")
        spawn(Supervisor, :init, [opts, ref])

      pid when is_pid(pid) ->
        Logger.debug("SuperWorker, Supervisor, starting supervisor and link with remote pid.")
        opts = Map.put(opts, :linked_pids, [pid])
        spawn(Supervisor, :init, [opts, ref])

      list_pid when is_list(list_pid) ->
        Logger.debug("SuperWorker, Supervisor, starting supervisor and link with remote pids.")
        opts = Map.put(opts, :linked_pids, list_pid)
        spawn(Supervisor, :init, [opts, ref])
    end

    api_receiver(ref, timeout)
  end

  @spec get_partition_id(atom(), integer()) :: atom()
  defp get_partition_id(sup_id, partition_id) do
    if partition_id < 0 do
      sup_id
    else
      String.to_atom("#{Atom.to_string(sup_id)}_#{inspect(partition_id)}")
    end
  end

  @spec get_target_partition(atom(), any(), integer()) :: atom()
  defp get_target_partition(prefix, data, num_partitions) when is_integer(num_partitions) do
    partition_id = get_hash_order(data, num_partitions)
    get_partition_id(prefix, partition_id)
  end

  @spec get_partition_pid(atom(), atom()) :: {:error, atom()} | {:ok, pid()}
  defp get_partition_pid(sup_id, partition_id) do
    case Db.get_sup_pid(sup_id, partition_id) do
      {:ok, pid} ->
        {:ok, pid}

      _ ->
        Logger.error(
          "SuperWorker, Supervisor, supervisor #{inspect(sup_id)} partition not found: #{inspect(partition_id)}"
        )

        {:error, {:partition_not_found, partition_id}}
    end
  end

  @spec get_host_partition(atom, any) :: {:error, atom} | {:ok, atom, pid}
  def get_host_partition(sup_id, data) do
    with {:ok, sup} <- Db.get_sup_info(sup_id, :master),
         partition_id <- get_target_partition(sup_id, data, sup.number_of_partitions),
         {:ok, pid} <- get_partition_pid(sup_id, partition_id) do
      {:ok, partition_id, pid}
    else
      error ->
        Logger.error(
          "SuperWorker, Supervisor, get partition pid failed: #{inspect(error)}, data: #{inspect(data)}, sup_id: #{inspect(sup_id)}"
        )

        error
    end
  end

  @spec has_group?(map(), any()) :: boolean()
  defp has_group?(%{} = state, group_id) do
    match?({:ok, _}, Db.get_group(state.master, group_id))
  end

  @spec has_chain?(map(), any()) :: boolean()
  defp has_chain?(%{} = state, chain_id) do
    match?({:ok, _}, Db.get_chain(state.master, chain_id))
  end

  @spec has_group_worker?(map(), any(), any()) :: boolean()
  defp has_group_worker?(%{} = state, group_id, worker_id) do
    match?({:ok, _}, Db.get_worker_info(state.master, worker_id, {:group, group_id}))
  end

  @spec has_chain_worker?(map(), any(), any()) :: boolean()
  defp has_chain_worker?(%{} = state, chain_id, worker_id) do
    match?({:ok, _}, Db.get_worker_info(state.master, worker_id, {:chain, chain_id}))
  end

  @spec has_standalone_worker?(map(), any()) :: boolean()
  defp has_standalone_worker?(%{} = state, worker_id) do
    match?({:ok, _}, Db.get_worker_info(state.master, worker_id, {:standalone, nil}))
  end

  # Check the supervisor is running or not.
  # if running get pid of partition.
  @spec verify_and_get_pid(atom(), any()) :: {:error, atom()} | {:ok, pid()}
  defp verify_and_get_pid(sup_id, id) do
    with true <- is_running?(sup_id),
         {:ok, _partition_id, pid} <- get_host_partition(sup_id, id) do
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

  defp map_to_struct(opts) when is_map(opts) do
    {:ok, struct(Supervisor, opts)}
  end
end
