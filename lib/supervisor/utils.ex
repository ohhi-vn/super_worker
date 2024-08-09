defmodule SuperWorker.Supervisor.Utils do
  @moduledoc false

  # Define the parameters for worker options
  @child_params [:id, :group_id, :chain_id, :restart_strategy, :type]

  def validate_opts(opts) do
    # Validate the options
    # TO-DO: Implement the validation
    result = Enum.reduce(opts, %{}, fn
      {key, value}, acc -> Map.put(acc, key, value)
      value, acc ->
        {k, v} = get_keyword(value)
        Map.put(acc, k, v)
    end)

    Enum.all?(result, fn {key, _} -> key in @child_params end)

    {:ok, result}
  end

  # convert specified keyword to a tuple.
  def get_keyword(:group) do
    {:type, :group}
  end
  def get_keyword(:chain) do
    {:type, :chain}
  end
  def get_keyword(:standalone) do
    {:type, :standalone}
  end
end
