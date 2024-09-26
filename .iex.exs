alias SuperWorker.Supervisor, as: Sup
alias SuperWorker.Supervisor.{Group, Chain, Standalone}
alias SuperWorker.TermStorage, as: KV

IO.puts "Dev mode is running"
IO.puts "SuperWorker.Supervisor has alias is Sup"

defmodule Dev do
  @moduledoc """
  Module for development purpose.
  """

  @me __MODULE__

  # Start the supervisor, add a group and a chain.
  def start(sup_id \\ :sup1) do
    result = Sup.start([link: false, id: sup_id])
    IO.inspect result

    # Group & workers for group.
    # add_group_data()

    # Standalone
    #add_standalone_data()

    # Chain & its workers.
    # add_chain_data(sup_id)
  end

  def add_group_data(sup_id \\ :sup1, group \\ :group_1, restart_strategy \\ :one_for_all, num_workers \\ 3) do
    {:ok, _} = Sup.add_group(sup_id, [id: group, restart_strategy: restart_strategy])
    for i <- 1..num_workers do
      {:ok, _} = Sup.add_group_worker(sup_id, group, {__MODULE__, :task, [15]}, [id: :"w_#{i}"])
    end
  end

  def add_group_data_loop(sup_id \\ :sup1, group \\ :group_loop, restart_strategy \\ :one_for_all, num_workers \\ 3) do
    {:ok, _} = Sup.add_group(sup_id, [id: group, restart_strategy: restart_strategy])
    for i <- 1..num_workers do
      {:ok, _} = Sup.add_group_worker(sup_id, :group_loop, {__MODULE__, :loop, [i]}, [id: :"w_#{i}"])
    end
  end

  def add_chain_data(sup_id \\ :sup1, chain_id \\ :chain_1, restart_strategy \\ :one_for_one, num_workers \\ 3, process_of_worker \\ 3) do
    {:ok, _} = Sup.add_chain(sup_id, [id: chain_id, restart_strategy: restart_strategy, finished_callback: {__MODULE__, :print,[chain_id]}, send_type: :partition])
    for i <- 1..num_workers do
      {:ok, _} = Sup.add_chain_worker(sup_id, chain_id, {__MODULE__, :task, [15]}, [id: :"c_#{i}", num_workers: process_of_worker])
    end
  end

  def add_standalone_data(sup_id \\ :sup1) do
    {:ok, _} = Sup.add_standalone_worker(sup_id, {__MODULE__, :task, [15]}, [id: :w1, restart_strategy: :permanent])
    {:ok, _} = Sup.add_standalone_worker(sup_id, {__MODULE__, :task_crash, [15, 5]}, [id: :w2, restart_strategy: :transient])
    {:ok, _} = Sup.add_standalone_worker(sup_id, fn ->
      receive do
        msg -> IO.puts "Standalone worker received: #{inspect msg}"
      end
    end, [id: :w3, restart_strategy: :temporary])
  end

  # function to add a worker to the supervisor.
  def task(n, sleep \\ 1_000) do
    prefix = "[#{inspect Process.get({:supervisor, :worker_id})}, #{inspect self()}]"
    IO.puts prefix <> " Task is started, param: #{n}"

    sum = Enum.reduce(1..n, 0, fn i, acc ->
      :timer.sleep(sleep)
      acc + i
    end)
    IO.puts  IO.puts prefix <> " Task done, #{sum}"

    {:next, n + 1}
  end

  def task_crash(n, at, sleep \\ 1_000) do
    prefix = "[#{inspect Process.get({:supervisor, :worker_id})}, #{inspect self()}]"
    IO.puts prefix <> " Task is started, param: #{n}"

    sum = Enum.reduce(1..n, 0, fn i, acc ->
      if i == at, do: raise "Task #{inspect Process.get({:supervisor, :worker_id})} raised an error at #{i}"
      :timer.sleep(sleep)
      acc + i
    end)
    IO.puts prefix <> " Task done, #{sum}"

    {:next, n + 1}
  end

  def send_to_chain(sup_id, chain_id, data \\ 10) do
    Sup.send_to_chain(sup_id, chain_id, data)
  end

  # return a anonymous function.
  def anonymous do
    fn ->
      prefix = "[#{inspect Process.get({:supervisor, :worker_id})}, #{inspect self()}]"
      IO.puts prefix <> " Anonymous function"
      for i <- 1..5 do
        IO.puts prefix <> " Task #{i}"
        :timer.sleep(1500)
      end
    end
  end

  # receive the result and print it. Raise an error if the result is an error.
  def print({:raise, reason}, chain_id) do
    prefix = "[#{inspect Process.get({:supervisor, :worker_id})}, #{inspect self()}]"
    IO.puts prefix <> " Chain #{inspect chain_id} will raise an error #{inspect reason}"
    raise reason
  end
  def print(result, chain_id) do
    prefix = "[#{inspect Process.get({:supervisor, :worker_id})}, #{inspect self()}]"
    IO.puts prefix <> " Chain #{inspect chain_id} finished with result #{inspect result}"
  end

  # Basic loop, receive messages and print them.
  def loop(id) do
    prefix = "[#{inspect Process.get({:supervisor, :worker_id})}, #{inspect self()}]"
    receive do
      msg -> IO.puts prefix <> " task received: #{inspect msg}"
    end

    loop(id)
  end
end
