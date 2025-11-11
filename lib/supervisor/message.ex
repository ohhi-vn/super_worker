defmodule SuperWorker.Supervisor.Message do
  @moduledoc false

  alias __MODULE__

  defstruct [
    # Message id.
    :id,
    # Pid/alias of sender.
    :from,
    # Pid/alias of receiver.
    :to,
    # internal or api message
    :type,
    # Message to send.
    :data
  ]

  @type t :: %Message{
          id: reference,
          from: pid | atom | nil,
          to: pid | atom | nil,
          type: :internal | :api,
          data: any
        }

  @spec new(atom, atom | pid, any) :: t
  def new(type, to, data) do
    %Message{
      id: make_ref(),
      from: self(),
      type: type,
      to: to,
      data: data
    }
  end
end
