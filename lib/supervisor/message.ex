defmodule SuperWorker.Supervisor.Message do
  @moduledoc false

  alias __MODULE__

  defstruct [
    :id, # Message id. Randomly generated.
    :from, # Pid/alias of sender.
    :to, # Pid/alias of receiver.
    :data, # Message to send.
  ]

  @type t :: %Message{
    id: binary,
    from: pid | atom | nil,
    to: pid | atom | nil,
    data: any
  }

  import SuperWorker.Supervisor.Utils

  @spec new(atom | pid, atom | pid, any) :: t
  def new(from, to, data, id \\ nil) do
    id = id || random_id()
    %Message{
      id: id,
      from: from,
      to: to,
      data: data
    }
  end
end
