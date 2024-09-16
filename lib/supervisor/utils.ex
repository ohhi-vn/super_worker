defmodule SuperWorker.Supervisor.Utils do
  @moduledoc false

  require Logger

  # Define the parameters for worker options
  @child_params [:id, :group_id, :chain_id,  :type]

  def validate_child_opts(opts) do
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
  def get_keyword(value) do
    {:error, "Invalid type, #{inspect value}"}
  end

  def generic_default_sup_opts(opts) do
    if Map.has_key?(opts, :owner) do
      opts
    else
      Map.put(opts, :owner, self())
    end
  end

  def normalize_opts(opts, params) do
    do_normalize_opts(opts, params, %{}, [])
  end

  def get_item(map, key) do
    case Map.get(map, key) do
      nil -> {:error, :not_found}
      value -> {:ok, value}
    end
  end

  def get_hash_order(term, num) do
    :erlang.phash2(term, num)
  end

  def get_default_schedulers() do
    System.schedulers_online()
  end

  def count_msgs(pid) do
    Process.info(pid, :message_queue_len)
  end

  def check_type(opts, key, fun) do
    case Map.get(opts, key) do
      nil ->
        Logger.warning("Option #{inspect key} is not found")
        {:error, :invalid_type}
      value ->
        if fun.(value) do
          {:ok, opts}
        else
          Logger.warning("Option #{inspect key} is invalid")
          {:error, :invalid_type}
        end
    end
  end

  def get_table_name(id) when is_atom(id) do
    String.to_atom("#{Atom.to_string(id)}_data_table")
  end

  def random_id() do
    :crypto.strong_rand_bytes(16) |> Base.encode16()
  end

  def response_ref() do
    {self(), make_ref()}
  end

  ## Private functions

  defp do_normalize_opts([], _params, valid, invalid) do
    if Enum.empty?(invalid) do
      {:ok, valid}
    else
      {:error, "Invalid options: #{inspect invalid}"}
    end
  end
  defp do_normalize_opts([{key, value}|rest], params, valid, invalid) do
    if key in params do
      do_normalize_opts(rest, params, Map.put(valid, key, value), invalid)
    else
      do_normalize_opts(rest, params, valid, [key | invalid])
    end
  end
  defp do_normalize_opts([short_opt|rest], params, valid, invalid) when is_atom(short_opt) do
    if short_opt in params do
      {key, value} = get_keyword(short_opt)
      do_normalize_opts(rest, params, Map.put(valid, key, value), invalid)
    else
      do_normalize_opts(rest, params, valid, [short_opt | invalid])
    end
  end
  defp do_normalize_opts([opt|rest], params, valid, invalid) do
    do_normalize_opts(rest, params, valid, [opt | invalid])
  end
end
