defmodule SuperWorker.Supervisor.MapQueue do
  defstruct [
    :id, # id, unique in system, for managing.
    last_send: 0, # last worker id in the chain. where the data is received.
    last_msg_id: 0,
    msgs: %{},
    queue_length: 50,
  ]

  alias SuperWorker.Supervisor.MapQueue

  def new(id) do
    %MapQueue{id: id}
  end

  def add(%MapQueue{} = queue, msg) do
    if map_size(queue.msgs) > queue.queue_length do
      {:error, :queue_full}
    else
      id = queue.last_msg_id + 1
      queue = %MapQueue{queue | last_msg_id: id}
      msgs = Map.put(queue.msgs, id, msg)
      queue = Map.put(queue, :msgs, msgs)
      {:ok, queue, id}
    end
  end

  def get(%MapQueue{} = queue, id) do
    case Map.get(queue.msgs, id) do
      nil -> {:error, :not_found}
      msg -> {:ok, msg}
    end
  end

  def remove(%MapQueue{} = queue, id) do
    msgs = Map.delete(queue.msgs, id)
    {:ok, %MapQueue{queue | msgs: msgs}}
  end

  def is_full?(%MapQueue{} = queue) do
    map_size(queue.msgs) >= queue.queue_length
  end
end
