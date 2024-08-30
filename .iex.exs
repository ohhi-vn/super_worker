alias SuperWorker.Supervisor, as: Sup
alias SuperWorker.Supervisor.{Group, Chain, Standalone}

IO.puts "Dev mode is running"
IO.puts "SuperWorker.Supervisor has alias is Sup"

defmodule Dev do
  # Start the supervisor, add a group and a chain.
  def init_sup do
    Sup.start([link: false, id: :sup1])
    {:ok, _} = Sup.add_group(:sup1, [id: :group1, restart_strategy: :one_for_all])
    # {:ok, _} = Sup.add_group([id: :group2, restart_strategy: :one_for_one])
    {:ok, _} = Sup.add_chain(:sup1, [id: :chain1, restart_strategy: :one_for_one])
    {:ok, _} = Sup.add_chain(:sup1, [id: :chain2, restart_strategy: :one_for_one, finished_callback: {__MODULE__, :print,[:chain1]}])

    # Add a worker to the group.
    {:ok, _} = Sup.add_group_worker(:sup1, :group1, {__MODULE__, :task, [15]}, [id: :f])
    #  Sup.add_group_worker(:group1, {__MODULE__, :task_crash, [15, 5]}, [id: :b])
    # {:ok, _} = Sup.add_group_worker(:group2, {__MODULE__, :task, [1500]}, [id: :c])
    # {:ok, _} = Sup.add_group_worker(:group2, {__MODULE__, :task_crash, [1500, 5]}, [id: :d])

    {:ok, _} = Sup.add_group_worker(:sup1, :group1, {__MODULE__, :loop, [:a]}, [id: :a])
    {:ok, _} = Sup.add_group_worker(:sup1, :group1, {__MODULE__, :loop, [:b]}, [id: :b])

    # r = Sup.add_standalone_worker({__MODULE__, :task, [1500]}, [id: 1, restart_strategy: :permanent])
    # IO.inspect r

    # {:ok, _ } = Sup.add_standalone_worker({__MODULE__, :task_crash, [15, 5]}, [id: 2, restart_strategy: :transient])

     {:ok, _} = Sup.add_chain_worker(:sup1, :chain1, {__MODULE__, :task, []}, [id: 31, restart_strategy: :permanent])
     {:ok, _} = Sup.add_chain_worker(:sup1, :chain1, {__MODULE__, :task, []}, [id: 32, restart_strategy: :permanent])
     {:ok, _} = Sup.add_chain_worker(:sup1, :chain1, {__MODULE__, :task_crash, [10]}, [id: 34, restart_strategy: :permanent])
     {:ok, _} = Sup.add_chain_worker(:sup1, :chain2, {__MODULE__, :task, []}, [id: 33, restart_strategy: :permanent])
     {:ok, _} = Sup.add_chain_worker(:sup1, :chain2, {__MODULE__, :task, []}, [id: 35, restart_strategy: :permanent])

    :ok
  end

  # function to add a worker to the supervisor.
  def task(n) do
    IO.puts "#{inspect self()}, Task is started"
    sum = Enum.reduce(1..n, 0, fn i, acc ->
      IO.puts "#{inspect self()}, Task #{i}"
      :timer.sleep(500)
      acc + i
    end)
    {:next, sum}
  end

  def task_crash(n, at) do
    IO.puts "#{inspect self()}, Task is started"
    Enum.reduce(1..n, 0, fn i, acc ->
      if i == at, do: raise "Task crashed"
      IO.puts "#{inspect self()}, Task #{i}"
      :timer.sleep(500)
      acc + i
    end)
  end

  # return a anonymous function.
  def anonymous do
    fn ->
      IO.puts "#{inspect self()}, Anonymous function"
      for i <- 1..5 do
        IO.puts "#{inspect self()}, Task #{i}"
        :timer.sleep(500)
      end
    end
  end

  # receive the result and print it. Raise an error if the result is an error.
  def print({:raise, reason}, chain_id) do
    IO.puts "#{inspect self()}, Chain #{inspect chain_id} will raise an error #{inspect reason}"
    raise reason
  end
  def print(result, chain_id) do
    IO.puts "#{inspect self()}, Chain #{inspect chain_id} finished with result #{inspect result}"
  end

  # Basic loop, receive messages and print them.
  def loop(id) do
    receive do
      msg -> IO.puts "worker #{inspect id}, received: #{inspect msg}"
    end

    loop(id)
  end
end
