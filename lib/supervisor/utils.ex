defmodule SuperWorker.Supervisor.Utils do
  @moduledoc false

  require Logger

  # convert specified keyword to a tuple.
  @spec get_keyword(atom()) :: {:type, atom()} | {:error, atom()}
  def get_keyword(:group) do
    {:type, :group}
  end
  def get_keyword(:chain) do
    {:type, :chain}
  end
  def get_keyword(:standalone) do
    {:type, :standalone}
  end
  def get_keyword(_) do
    {:error, :invalid_options}
  end

  @spec generic_default_sup_opts(map()) :: map()
  def generic_default_sup_opts(opts) do
    if Map.has_key?(opts, :owner) do
      opts
    else
      Map.put(opts, :owner, self())
    end
  end

  @spec normalize_opts([atom() | keyword()], list(atom())) :: {:ok, map()} | {:error, atom()|{atom(), any()}}
  def normalize_opts(opts, params) do
    do_normalize_opts(opts, params, %{}, [])
  end

  @spec get_item(map(), any()) :: {:ok, any()} | {:error, atom()}
  def get_item(map, key) do
    case Map.get(map, key) do
      nil -> {:error, :not_found}
      value -> {:ok, value}
    end
  end

  @spec get_hash_order(term(), non_neg_integer()) :: non_neg_integer()
  def get_hash_order(term, num) do
    :erlang.phash2(term, num)
  end

  @spec get_default_schedulers() :: non_neg_integer()
  def get_default_schedulers() do
    System.schedulers_online()
  end

  @spec count_msgs(pid()) :: non_neg_integer()
  def count_msgs(pid) do
    case Process.info(pid, :message_queue_len) do
      {:message_queue_len, n} -> n
      nil -> 0
    end
  end

  @spec check_type(map(), atom(), (any() -> boolean())) :: {:ok, map()} | {:error, atom()}
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

  @spec get_table_name(atom()) :: atom()
  def get_table_name(id) when is_atom(id) do
    String.to_atom("#{Atom.to_string(id)}_data_table")
  end

  @spec random_id() :: binary()
  def random_id() do
    :crypto.strong_rand_bytes(16) |> Base.encode16()
  end

  @spec response_ref() :: {pid(), reference()}
  def response_ref() do
    {self(), make_ref()}
  end

  @spec api_receiver({pid(), reference()}, integer() | :infinity) :: any()
  def api_receiver({_from, ref}, timeout) do
    receive do
      {^ref, result} ->
        result
      after timeout ->
        {:error, :api_timeout}
    end
  end

  @spec api_response({pid(), reference()}, any()) :: any()
  def api_response({from, ref}, result) do
    send(from, {ref, result})
  end

  @spec call_api(atom() | pid(), atom(), any(), integer() | :infinity) :: any()
  def call_api(target, api, params, timeout) when is_atom(target) or is_pid(target) do
    Logger.debug("Call API: #{inspect api}, #{inspect params}, target: #{inspect target}")
    ref = response_ref()
    send(target, {api, ref, params})
    api_receiver(ref, timeout)
  end

  @spec call_api_no_reply(atom() | pid(), atom(), any()) :: any()
  def call_api_no_reply(target, api, params) when is_atom(target) or is_pid(target) do
    {_, ref} = response_ref() # ref is just for tracking, adds more in the future.
    send(target, {api, ref, params})
  end

  ## Private functions

  @spec do_normalize_opts([keyword() | atom()], list(atom()), map(), list(keyword() | atom()))
     :: {:ok, map()} | {:error, {atom()|{atom(), list(keyword() | atom())}}}
  defp do_normalize_opts([], _params, valid, invalid) do
    if Enum.empty?(invalid) do
      {:ok, valid}
    else
      {:error, {:invalid_options, invalid}}
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
