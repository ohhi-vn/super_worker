defmodule SuperWorker.Supervisor.MapQueue do
  @moduledoc """
  A simple queue implementation using a map for O(1) lookups.

  This queue is designed for managing messages in chain workers where:
  - Fast lookups by message ID are required
  - Queue size limits need to be enforced
  - Messages can be removed in any order (not strictly FIFO)

  ## Examples

      iex> queue = MapQueue.new(:my_queue)
      iex> {:ok, queue, msg_id} = MapQueue.add(queue, "hello")
      iex> {:ok, "hello"} = MapQueue.get(queue, msg_id)
      iex> false = MapQueue.empty?(queue)
      iex> 1 = MapQueue.size(queue)

  """

  @enforce_keys []
  defstruct [
    :id,
    last_send: 0,
    last_msg_id: 0,
    msgs: %{},
    queue_length: 50
  ]

  @type msg_id :: non_neg_integer()
  @type message :: any()

  @type t :: %__MODULE__{
          id: any(),
          last_send: non_neg_integer(),
          last_msg_id: msg_id(),
          msgs: %{msg_id() => message()},
          queue_length: pos_integer()
        }

  @default_queue_length 50

  # ============================================================================
  # Public API
  # ============================================================================

  @doc """
  Creates a new queue with the given ID.

  ## Options

    * `:queue_length` - Maximum number of messages in the queue (default: #{@default_queue_length})

  ## Examples

      iex> MapQueue.new(:my_queue)
      %MapQueue{id: :my_queue, queue_length: 50, msgs: %{}}

      iex> MapQueue.new(:my_queue, queue_length: 100)
      %MapQueue{id: :my_queue, queue_length: 100, msgs: %{}}

  """
  @spec new(any(), keyword()) :: t()
  def new(id, opts \\ []) do
    queue_length = Keyword.get(opts, :queue_length, @default_queue_length)

    %__MODULE__{
      id: id,
      queue_length: queue_length
    }
  end

  @doc """
  Adds a message to the queue.

  Returns `{:ok, updated_queue, message_id}` if successful,
  or `{:error, :queue_full}` if the queue is at capacity.

  ## Examples

      iex> queue0 = MapQueue.new(:test)
      iex> {:ok, queue1, 1} = MapQueue.add(queue0, "message")
      iex> {:ok, _queue2, 2} = MapQueue.add(queue1, "another")

  """
  @spec add(t(), message()) :: {:ok, t(), msg_id()} | {:error, :queue_full}
  def add(%__MODULE__{} = queue, msg) do
    if full?(queue) do
      {:error, :queue_full}
    else
      msg_id = queue.last_msg_id + 1
      updated_msgs = Map.put(queue.msgs, msg_id, msg)

      updated_queue = %__MODULE__{
        queue
        | last_msg_id: msg_id,
          msgs: updated_msgs
      }

      {:ok, updated_queue, msg_id}
    end
  end

  @doc """
  Retrieves a message from the queue by ID without removing it.

  Returns `{:ok, message}` if found, or `{:error, {:not_found, id}}` otherwise.

  ## Examples

      iex> queue = MapQueue.new(:test)
      iex> {:ok, queue, msg_id} = MapQueue.add(queue, "hello")
      iex> {:ok, "hello"} = MapQueue.get(queue, msg_id)
      iex> {:error, {:not_found, 999}} = MapQueue.get(queue, 999)

  """
  @spec get(t(), msg_id()) :: {:ok, message()} | {:error, {:not_found, msg_id()}}
  def get(%__MODULE__{} = queue, id) when is_integer(id) do
    case Map.get(queue.msgs, id) do
      nil -> {:error, {:not_found, id}}
      msg -> {:ok, msg}
    end
  end

  @doc """
  Removes a message from the queue by ID.

  Returns `{:ok, updated_queue}` regardless of whether the message existed.

  ## Examples

      iex> queue = MapQueue.new(:test)
      iex> {:ok, queue, msg_id} = MapQueue.add(queue, "hello")
      iex> {:ok, queue} = MapQueue.remove(queue, msg_id)
      iex> {:error, {:not_found, _}} = MapQueue.get(queue, msg_id)

  """
  @spec remove(t(), msg_id()) :: {:ok, t()}
  def remove(%__MODULE__{} = queue, id) when is_integer(id) do
    updated_msgs = Map.delete(queue.msgs, id)
    {:ok, %__MODULE__{queue | msgs: updated_msgs}}
  end

  @doc """
  Checks if the queue is full.

  Returns `true` if the queue has reached its maximum capacity.

  ## Examples

      iex> queue = MapQueue.new(:test, queue_length: 2)
      iex> {:ok, queue, _} = MapQueue.add(queue, "msg1")
      iex> {:ok, queue, _} = MapQueue.add(queue, "msg2")
      iex> true = MapQueue.full?(queue)

  """
  @spec full?(t()) :: boolean()
  def full?(%__MODULE__{msgs: msgs, queue_length: max_length}) do
    map_size(msgs) >= max_length
  end

  @doc """
  Checks if the queue is empty.

  Returns `true` if the queue contains no messages.

  ## Examples

      iex> queue = MapQueue.new(:test)
      iex> true = MapQueue.empty?(queue)
      iex> {:ok, queue, _} = MapQueue.add(queue, "msg")
      iex> false = MapQueue.empty?(queue)

  """
  @spec empty?(t()) :: boolean()
  def empty?(%__MODULE__{msgs: msgs}) do
    map_size(msgs) == 0
  end

  @doc """
  Returns the current number of messages in the queue.

  ## Examples

      iex> queue = MapQueue.new(:test)
      iex> 0 = MapQueue.size(queue)
      iex> {:ok, queue, _} = MapQueue.add(queue, "msg")
      iex> 1 = MapQueue.size(queue)

  """
  @spec size(t()) :: non_neg_integer()
  def size(%__MODULE__{msgs: msgs}) do
    map_size(msgs)
  end

  @doc """
  Returns the remaining capacity of the queue.

  ## Examples

      iex> queue = MapQueue.new(:test, queue_length: 10)
      iex> 10 = MapQueue.remaining_capacity(queue)
      iex> {:ok, queue, _} = MapQueue.add(queue, "msg")
      iex> 9 = MapQueue.remaining_capacity(queue)

  """
  @spec remaining_capacity(t()) :: non_neg_integer()
  def remaining_capacity(%__MODULE__{msgs: msgs, queue_length: max_length}) do
    max(0, max_length - map_size(msgs))
  end

  @doc """
  Clears all messages from the queue.

  Returns the queue with an empty message map.

  ## Examples

      iex> queue = MapQueue.new(:test)
      iex> {:ok, queue, _} = MapQueue.add(queue, "msg1")
      iex> {:ok, queue, _} = MapQueue.add(queue, "msg2")
      iex> queue = MapQueue.clear(queue)
      iex> true = MapQueue.empty?(queue)

  """
  @spec clear(t()) :: t()
  def clear(%__MODULE__{} = queue) do
    %__MODULE__{queue | msgs: %{}}
  end

  @doc """
  Returns all message IDs currently in the queue.

  ## Examples

      iex> queue = MapQueue.new(:test)
      iex> {:ok, queue, id1} = MapQueue.add(queue, "msg1")
      iex> {:ok, queue, id2} = MapQueue.add(queue, "msg2")
      iex> ids = MapQueue.message_ids(queue)
      iex> id1 in ids and id2 in ids
      true

  """
  @spec message_ids(t()) :: [msg_id()]
  def message_ids(%__MODULE__{msgs: msgs}) do
    Map.keys(msgs)
  end

  @doc """
  Returns all messages currently in the queue as a list.

  ## Examples

      iex> queue = MapQueue.new(:test)
      iex> {:ok, queue, _} = MapQueue.add(queue, "msg1")
      iex> {:ok, queue, _} = MapQueue.add(queue, "msg2")
      iex> messages = MapQueue.all_messages(queue)
      iex> "msg1" in messages and "msg2" in messages
      true

  """
  @spec all_messages(t()) :: [message()]
  def all_messages(%__MODULE__{msgs: msgs}) do
    Map.values(msgs)
  end

  @doc """
  Updates the queue length limit.

  Note: This does not remove existing messages if the new limit is smaller
  than the current size.

  ## Examples

      iex> queue = MapQueue.new(:test, queue_length: 10)
      iex> queue = MapQueue.update_queue_length(queue, 20)
      iex> 20 = queue.queue_length

  """
  @spec update_queue_length(t(), pos_integer()) :: t()
  def update_queue_length(%__MODULE__{} = queue, new_length)
      when is_integer(new_length) and new_length > 0 do
    %__MODULE__{queue | queue_length: new_length}
  end

  @doc """
  Checks if a message with the given ID exists in the queue.

  ## Examples

      iex> queue = MapQueue.new(:test)
      iex> {:ok, queue, msg_id} = MapQueue.add(queue, "msg")
      iex> true = MapQueue.has_message?(queue, msg_id)
      iex> false = MapQueue.has_message?(queue, 999)

  """
  @spec has_message?(t(), msg_id()) :: boolean()
  def has_message?(%__MODULE__{msgs: msgs}, id) when is_integer(id) do
    Map.has_key?(msgs, id)
  end
end
