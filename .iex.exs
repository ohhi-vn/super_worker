alias SuperWorker.Supervisor, as: Sup

IO.puts "Dev mode is running"
IO.puts "SuperWorker.Supervisor has alias is Sup"

defmodule Dev do
  def init_sup do
    Sup.start(:ok)
    Sup.add_group([id: 1, restart_strategy: :one_for_all])
    Sup.add_chain([id: 1, restart_strategy: :one_for_all])
  end
end
