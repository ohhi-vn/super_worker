defmodule SuperWorker.CircuitBreaker do
  @moduledoc """
  A simple Circuit Breaker implementation for protecting external API calls.

  The circuit breaker has three states:
  - `:closed` - Normal operation, requests go through
  - `:open` - Requests are short-circuited (fail fast)
  - `:half_open` - Testing if the service has recovered

  ## Usage

      # Protect an API call
      result =
        SuperWorker.CircuitBreaker.call(:my_external_service, fn ->
          HTTPoison.get("https://api.example.com/data")
        end)

      case result do
        {:ok, response} -> # Handle success
        {:error, :circuit_open} -> # Handle circuit open
        {:error, reason} -> # Handle other errors
      end
  """

  use GenServer, restart: :temporary

  require Logger

  @default_failure_threshold 5
  @default_reset_timeout 10_000
  @default_half_open_max_calls 3

  defstruct [
    :name,
    :state,
    :failure_count,
    :success_count,
    :last_failure_time,
    :failure_threshold,
    :reset_timeout,
    :half_open_max_calls
  ]

  @type t :: %__MODULE__{
          name: atom(),
          state: :closed | :open | :half_open,
          failure_count: non_neg_integer(),
          success_count: non_neg_integer(),
          last_failure_time: integer() | nil,
          failure_threshold: pos_integer(),
          reset_timeout: pos_integer(),
          half_open_max_calls: pos_integer()
        }

  ## Public API

  @doc """
  Start a circuit breaker for a named service.
  """
  @spec start(atom(), keyword()) :: {:ok, pid()} | {:error, term()}
  def start(name, opts \\ []) when is_atom(name) do
    failure_threshold = Keyword.get(opts, :failure_threshold, @default_failure_threshold)
    reset_timeout = Keyword.get(opts, :reset_timeout, @default_reset_timeout)
    half_open_max_calls = Keyword.get(opts, :half_open_max_calls, @default_half_open_max_calls)

    state = %__MODULE__{
      name: name,
      state: :closed,
      failure_count: 0,
      success_count: 0,
      last_failure_time: nil,
      failure_threshold: failure_threshold,
      reset_timeout: reset_timeout,
      half_open_max_calls: half_open_max_calls
    }

    GenServer.start_link(__MODULE__, state, name: name)
  end

  @doc """
  Call a function protected by the circuit breaker.
  """
  @spec call(atom(), (-> {:ok, any()} | {:error, any()})) ::
          {:ok, any()} | {:error, term()}
  def call(name, fun) when is_atom(name) and is_function(fun, 0) do
    GenServer.call(name, {:execute, fun})
  catch
    :exit, {:noproc, _} ->
      {:error, :circuit_not_started}

    :exit, {:timeout, _} ->
      {:error, :circuit_timeout}
  end

  @doc """
  Get the current state of the circuit breaker.
  """
  @spec get_state(atom()) :: {:ok, t()} | {:error, :not_found}
  def get_state(name) when is_atom(name) do
    GenServer.call(name, :get_state)
  catch
    :exit, {:noproc, _} ->
      {:error, :not_found}
  end

  @doc """
  Reset the circuit breaker to closed state.
  """
  @spec reset(atom()) :: :ok | {:error, :not_found}
  def reset(name) when is_atom(name) do
    GenServer.call(name, :reset)
  catch
    :exit, {:noproc, _} ->
      {:error, :not_found}
  end

  ## GenServer Callbacks

  @impl true
  def init(state) do
    {:ok, state}
  end

  @impl true
  def handle_call({:execute, fun}, _from, state) do
    case state.state do
      :closed ->
        handle_closed(state, fun)

      :open ->
        handle_open(state, fun)

      :half_open ->
        handle_half_open(state, fun)
    end
  end

  def handle_call(:get_state, _from, state) do
    {:reply, {:ok, state}, state}
  end

  def handle_call(:reset, _from, state) do
    state =
      state
      |> Map.put(:state, :closed)
      |> Map.put(:failure_count, 0)
      |> Map.put(:success_count, 0)
      |> Map.put(:last_failure_time, nil)

    {:reply, :ok, state}
  end

  ## Private Functions

  defp handle_closed(state, fun) do
    case execute_function(fun) do
      {:ok, result} ->
        state = reset_failure_count(state)
        {:reply, {:ok, result}, state}

      {:error, reason} ->
        state = record_failure(state, reason)

        if state.failure_count >= state.failure_threshold do
          Logger.warning(
            "SuperWorker.CircuitBreaker: Circuit opened for #{inspect(state.name)} after #{state.failure_count} failures"
          )

          state = open_circuit(state)
          {:reply, {:error, :circuit_open}, state}
        else
          {:reply, {:error, reason}, state}
        end
    end
  end

  defp handle_open(state, fun) do
    if should_attempt_reset(state) do
      Logger.info("SuperWorker.CircuitBreaker: Attempting reset for #{inspect(state.name)}")

      state = %{state | state: :half_open, success_count: 0}
      handle_half_open(state, fun)
    else
      {:reply, {:error, :circuit_open}, state}
    end
  end

  defp handle_half_open(state, fun) do
    case execute_function(fun) do
      {:ok, result} ->
        state = record_success(state)

        if state.success_count >= state.half_open_max_calls do
          Logger.info(
            "SuperWorker.CircuitBreaker: Circuit closed for #{inspect(state.name)} after #{state.success_count} successes"
          )

          state =
            state
            |> Map.put(:state, :closed)
            |> Map.put(:failure_count, 0)
            |> Map.put(:success_count, 0)

          {:reply, {:ok, result}, state}
        else
          {:reply, {:ok, result}, state}
        end

      {:error, reason} ->
        Logger.warning("SuperWorker.CircuitBreaker: Circuit re-opened for #{inspect(state.name)}")

        state =
          state
          |> Map.put(:state, :open)
          |> Map.put(:last_failure_time, System.monotonic_time(:millisecond))

        {:reply, {:error, reason}, state}
    end
  end

  defp execute_function(fun) do
    try do
      fun.()
    catch
      type, reason ->
        {:error, {type, reason}}
    end
  end

  defp record_failure(state, _reason) do
    %{
      state
      | failure_count: state.failure_count + 1,
        last_failure_time: System.monotonic_time(:millisecond)
    }
  end

  defp record_success(state) do
    %{state | success_count: state.success_count + 1}
  end

  defp reset_failure_count(state) do
    %{state | failure_count: 0, last_failure_time: nil}
  end

  defp open_circuit(state) do
    %{
      state
      | state: :open,
        last_failure_time: System.monotonic_time(:millisecond)
    }
  end

  defp should_attempt_reset(state) do
    now = System.monotonic_time(:millisecond)
    state.last_failure_time + state.reset_timeout < now
  end
end
