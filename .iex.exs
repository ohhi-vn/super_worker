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
  def start do
    result = Sup.start([link: false, id: :sup1])
    IO.inspect result

    # Group & workers for group.
    add_group_data()

    # Standalone
    add_standalone_data()

    # Chain & its workers.
    add_chain_data()
  end

  def add_group_data do
    {:ok, _} = Sup.add_group(:sup1, [id: :group1, restart_strategy: :one_for_all])
    {:ok, _} = Sup.add_group_worker(:sup1, :group1, {__MODULE__, :task, [15]}, [id: :g1_1])
    {:ok, _} = Sup.add_group_worker(:sup1, :group1, {__MODULE__, :task_crash, [15, 5]}, [id: :g1_2])
    {:ok, _} = Sup.add_group_worker(:sup1, :group1, {__MODULE__, :loop, [:a]}, [id: :g1_3])
    {:ok, _} = Sup.add_group_worker(:sup1, :group1, {__MODULE__, :loop, [:b]}, [id: :g1_4])

    {:ok, _} = Sup.add_group(:sup1, [id: :group2, restart_strategy: :one_for_one])
    {:ok, _} = Sup.add_group_worker(:sup1, :group2, {__MODULE__, :task, [1500]}, [id: :g2_1])
    {:ok, _} = Sup.add_group_worker(:sup1, :group2, {__MODULE__, :task_crash, [1500, 5]}, [id: :g2_2])
  end

  def add_chain_data do
    {:ok, _} = Sup.add_chain(:sup1, [id: :chain1, restart_strategy: :one_for_one])
    {:ok, _} = Sup.add_chain_worker(:sup1, :chain1, {__MODULE__, :task, []}, [id: :c1_1, restart_strategy: :permanent])
    {:ok, _} = Sup.add_chain_worker(:sup1, :chain1, {__MODULE__, :task, []}, [id: :c1_2, restart_strategy: :permanent])
    {:ok, _} = Sup.add_chain_worker(:sup1, :chain1, {__MODULE__, :task_crash, [10]}, [id: :c1_3, restart_strategy: :permanent])

    {:ok, _} = Sup.add_chain(:sup1, [id: :chain2, restart_strategy: :one_for_one, finished_callback: {__MODULE__, :print,[:chain1]}])
    {:ok, _} = Sup.add_chain_worker(:sup1, :chain2, {__MODULE__, :task, []}, [id: :c2_1, restart_strategy: :permanent])
    {:ok, _} = Sup.add_chain_worker(:sup1, :chain2, {__MODULE__, :task, []}, [id: :c2_2, restart_strategy: :permanent])
  end

  def add_standalone_data do
    {:ok, _} = Sup.add_standalone_worker(:sup1, {__MODULE__, :task, [15]}, [id: :w1, restart_strategy: :permanent])
    {:ok, _} = Sup.add_standalone_worker(:sup1, {__MODULE__, :task_crash, [15, 5]}, [id: :w2, restart_strategy: :transient])
  end

  # function to add a worker to the supervisor.
  def task(n) do
    prefix = "[#{inspect Process.get({:supervisor, :worker_id})}, #{inspect self()}]"
    IO.puts prefix <> " Task is started"

    sum = Enum.reduce(1..n, 0, fn i, acc ->
      :timer.sleep(1000)
      acc + i
    end)
    IO.puts  IO.puts prefix <> " Task done, #{sum}"

    {:next, n + 1}
  end

  def task_crash(n, at) do
    prefix = "[#{inspect Process.get({:supervisor, :worker_id})}, #{inspect self()}]"
    IO.puts prefix <> " Task is started"

    sum = Enum.reduce(1..n, 0, fn i, acc ->
      if i == at, do: raise "Task #{inspect Process.get({:supervisor, :worker_id})} raised an error at #{i}"
      IO.puts "#{inspect Process.get({:supervisor, :worker_id})}, #{inspect self()}, Task #{i}"
      :timer.sleep(500)
      acc + i
    end)
    IO.puts prefix <> " Task done, #{sum}"

    {:next, n + 1}
  end

  # return a anonymous function.
  def anonymous do
    fn ->
      prefix = "[#{inspect Process.get({:supervisor, :worker_id})}, #{inspect self()}]"
      IO.puts prefix <> " Anonymous function"
      for i <- 1..5 do
        IO.puts prefix <> " Task #{i}"
        :timer.sleep(500)
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
