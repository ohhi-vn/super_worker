alias SuperWorker.Supervisor, as: Sup

IO.puts "Dev mode is running"
IO.puts "SuperWorker.Supervisor has alias is Sup"

defmodule Dev do
  # Start the supervisor, add a group and a chain.
  def init_sup do
    Sup.start([])
    Sup.add_group([id: 1, restart_strategy: :one_for_all])
    # Sup.add_chain([id: 1, restart_strategy: :one_for_all])

    # Add a worker to the group.
    result = Sup.add_worker({__MODULE__, :task, [15]}, [id: 1, group_id: 1, type: :group])
    IO.inspect result
    result = Sup.add_worker({__MODULE__, :task_crash, [15, 5]}, [id: 1, group_id: 1, type: :group])
    IO.inspect result
    # result = Sup.add_worker({__MODULE__, :task, [15]}, [id: 1, restart_strategy: :permanent, type: :standalone])
    # IO.inspect result
    # result = Sup.add_worker({__MODULE__, :task_crash, [15, 3]}, [id: 2, restart_strategy: :transient, type: :standalone])
    # IO.inspect result
    # result = Sup.add_worker({__MODULE__, :task_crash, [15, 5]}, [id: 3, restart_strategy: :temporary, type: :standalone])
    # IO.inspect result
    :ok
  end

  # function to add a worker to the supervisor.
  def task(n) do
    IO.puts "Task is running"
    for i <- 1..n do
      IO.puts "#{inspect self()}, Task #{i}"
      :timer.sleep(1000)
    end
  end

  def task_crash(n, at) do
    IO.puts "Task is running"
    for i <- 1..n do
      if i == at, do: raise "Task crashed"
      IO.puts "#{inspect self()}, Task #{i}"
      :timer.sleep(1000)
    end

  end

  # return a anonymous function.
  def anonymous do
    fn ->
      IO.puts "Anonymous function"
      for i <- 1..5 do
        IO.puts "#{inspect self()}, Task #{i}"
        :timer.sleep(1000)
      end
    end
  end
end
