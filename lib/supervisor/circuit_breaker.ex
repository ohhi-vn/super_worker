defmodule SuperWorker.CircuitBreaker do
  @moduledoc """
  A simple Circuit Breaker implementation for protecting external API calls.

  The circuit breaker has three states:
  - `:closed` - Normal operation, requests go through
  - `:open` - Requests are short-circuited (fail fast)
  - `:half_open` - Testing if the service has recovered

  The protected function is executed **in the caller process**, so slow calls
  never block the breaker process itself and concurrent calls are not
  serialized through a single process.

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

  When the call that trips the threshold fails, the real error is returned;
  all subsequent calls fail fast with `{:error, :circuit_open}` until the
  reset timeout elapses.
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
    :half_open_max_calls,
    # number of probes currently running in :half_open state
    in_flight: 0
  ]

  @type t :: %__MODULE__{
          name: atom(),
          state: :closed | :open | :half_open,
          failure_count: non_neg_integer(),
          success_count: non_neg_integer(),
          last_failure_time: integer() | nil,
          failure_threshold: pos_integer(),
          reset_timeout: pos_integer(),
          half_open_max_calls: pos_integer(),
          in_flight: non_neg_integer()
        }

  ## Public API

  @doc """
  Start a circuit breaker for a named service.

  The breaker process is linked to the caller.
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

  The function runs in the caller process; the breaker only tracks the
  outcome. Exceptions and exits inside `fun` are converted to error tuples.
  """
  @spec call(atom(), (-> {:ok, any()} | {:error, any()})) ::
          {:ok, any()} | {:error, term()}
  def call(name, fun) when is_atom(name) and is_function(fun, 0) do
    with {:ok, :execute} <- GenServer.call(name, :acquire) do
      result = execute_function(fun)
      GenServer.call(name, {:report, result})
      result
    end
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
  def handle_call(:acquire, _from, state) do
    case state.state do
      :closed ->
        {:reply, {:ok, :execute}, state}

      :open ->
        if should_attempt_reset(state) do
          Logger.info("SuperWorker.CircuitBreaker: Attempting reset for #{inspect(state.name)}")

          state = %{state | state: :half_open, success_count: 0, in_flight: 1}
          {:reply, {:ok, :execute}, state}
        else
          {:reply, {:error, :circuit_open}, state}
        end

      :half_open ->
        if state.in_flight < state.half_open_max_calls do
          {:reply, {:ok, :execute}, %{state | in_flight: state.in_flight + 1}}
        else
          # Too many probes already running, fail fast.
          {:reply, {:error, :circuit_open}, state}
        end
    end
  end

  def handle_call({:report, result}, _from, state) do
    case {state.state, result} do
      {:half_open, {:ok, _}} ->
        state = record_success(%{state | in_flight: max(state.in_flight - 1, 0)})

        if state.success_count >= state.half_open_max_calls do
          Logger.info(
            "SuperWorker.CircuitBreaker: Circuit closed for #{inspect(state.name)} after #{state.success_count} successes"
          )

          {:reply, result, close_circuit(state)}
        else
          {:reply, result, state}
        end

      {:half_open, {:error, reason}} ->
        Logger.warning(
          "SuperWorker.CircuitBreaker: Circuit re-opened for #{inspect(state.name)}, reason: #{inspect(reason)}"
        )

        {:reply, result, reopen_circuit(state)}

      {:closed, {:ok, _result}} ->
        {:reply, result, reset_failure_count(state)}

      {:closed, {:error, _reason}} ->
        state = record_failure(state)

        if state.failure_count >= state.failure_threshold do
          Logger.warning(
            "SuperWorker.CircuitBreaker: Circuit opened for #{inspect(state.name)} after #{state.failure_count} failures"
          )

          {:reply, result, open_circuit(state)}
        else
          {:reply, result, state}
        end

      # Late report after another probe already re-opened the circuit.
      _ ->
        {:reply, result, state}
    end
  end

  def handle_call(:get_state, _from, state) do
    {:reply, {:ok, state}, state}
  end

  def handle_call(:reset, _from, state) do
    {:reply, :ok, close_circuit(state)}
  end

  ## Private Functions

  defp execute_function(fun) do
    fun.()
  catch
    type, reason ->
      {:error, {type, reason}}
  end

  defp record_failure(state) do
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
        in_flight: 0,
        last_failure_time: System.monotonic_time(:millisecond)
    }
  end

  defp reopen_circuit(state) do
    %{
      state
      | state: :open,
        in_flight: 0,
        success_count: 0,
        last_failure_time: System.monotonic_time(:millisecond)
    }
  end

  defp close_circuit(state = %__MODULE__{}) do
    %__MODULE__{state | state: :closed, in_flight: 0}
    |> reset_failure_count()
    |> Map.put(:success_count, 0)
  end

  defp should_attempt_reset(state) do
    now = System.monotonic_time(:millisecond)
    state.last_failure_time + state.reset_timeout < now
  end
end
