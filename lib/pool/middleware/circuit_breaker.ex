defmodule SuperWorker.Pool.Middleware.CircuitBreaker do
  @default_failure_threshold 5
  @default_reset_timeout 10_000

  @moduledoc """
  Circuit breaker middleware for `SuperWorker.Pool`.

  One breaker process per pool partition. Two integration points:

  - `check_enqueue/2` — invoked by the partition on every submission. When
    the circuit is `:open` the submission is rejected *before* it enters the
    queue, so the caller immediately gets `{:error, :circuit_open}` and no
    worker capacity is wasted on a failing service.
  - `notify/3` — invoked by the partition on each final job outcome.
    Consecutive failures increment a counter; reaching `:failure_threshold`
    trips the circuit `:open`. After `:reset_timeout` elapsed, one probe job
    is let through (`:half_open`); a success closes the circuit, a failure
    opens it again.

  Unlike `SuperWorker.CircuitBreaker` (which protects a call *in the caller
  process*), this breaker is a supervised process owned by the pool and
  shared by all callers of the pool, so failures of any caller count towards
  the same circuit.

  Emits `[:super_worker, :pool, :circuit, :open]` and
  `[:super_worker, :pool, :circuit, :closed]` telemetry events with
  `%{pool: name, partition: id}` metadata.

  Options (via the pool's `circuit_breaker:` option):

  - `:failure_threshold` — consecutive failures that trip the circuit
    (default: #{@default_failure_threshold});
  - `:reset_timeout` — ms in `:open` state before probing again
    (default: #{@default_reset_timeout}).
  """

  use GenServer
  @behaviour SuperWorker.Pool.Middleware

  require Logger

  alias SuperWorker.Pool

  defstruct [
    :pool,
    :partition,
    :failure_threshold,
    :reset_timeout,
    # circuit state: :closed | :open | :half_open
    state: :closed,
    failure_count: 0,
    opened_at: nil,
    # probes currently allowed through while :half_open
    in_flight_probes: 0
  ]

  ## Middleware callbacks (run in the partition process)

  @impl true
  def call(job, meta, next), do: next.(job, meta)

  @impl true
  def check_enqueue(_job, meta) do
    case lookup(meta) do
      nil ->
        # Breaker is down (crashed/restarting); fail open so the pool keeps
        # working and the partition supervisor restores the breaker shortly.
        :ok

      pid ->
        try do
          GenServer.call(pid, :check)
        catch
          :exit, reason ->
            Logger.warning(
              "SuperWorker, Pool, CircuitBreaker check failed, failing open: #{inspect(reason)}"
            )

            :ok
        end
    end
  end

  @impl true
  def notify(outcome, _job, meta) do
    case lookup(meta) do
      nil -> :ok
      pid -> GenServer.cast(pid, {:notify, outcome})
    end

    :ok
  end

  defp lookup(meta) do
    key = {meta.pool, {:breaker, meta.partition}}

    case Registry.lookup(SuperWorker.Pool.Registry, key) do
      [{pid, _}] -> pid
      [] -> nil
    end
  end

  ## Breaker process (one per partition)

  @doc false
  def start_link(opts) do
    pool = Keyword.fetch!(opts, :pool)
    partition = Keyword.fetch!(opts, :partition)

    GenServer.start_link(__MODULE__, opts, name: Pool.via(pool, {:breaker, partition}))
  end

  @impl true
  def init(opts) do
    state = %__MODULE__{
      pool: Keyword.fetch!(opts, :pool),
      partition: Keyword.fetch!(opts, :partition),
      failure_threshold: Keyword.get(opts, :failure_threshold, @default_failure_threshold),
      reset_timeout: Keyword.get(opts, :reset_timeout, @default_reset_timeout)
    }

    {:ok, state}
  end

  @impl true
  def handle_call(:check, _from, state) do
    case state.state do
      :closed ->
        {:reply, :ok, state}

      :open ->
        if elapsed_since_open(state) >= state.reset_timeout do
          {:reply, :ok, %{state | state: :half_open, in_flight_probes: 1}}
        else
          {:reply, {:error, :circuit_open}, state}
        end

      :half_open ->
        {:reply, {:error, :circuit_open}, state}
    end
  end

  @impl true
  def handle_cast({:notify, :ok}, state) do
    {:noreply, maybe_close(state)}
  end

  def handle_cast({:notify, {:error, _reason}}, state) do
    {:noreply, record_failure(state)}
  end

  @impl true
  def handle_info(_message, state), do: {:noreply, state}

  ## Private helpers

  defp elapsed_since_open(%__MODULE__{opened_at: opened_at}) do
    System.monotonic_time(:millisecond) - opened_at
  end

  # A job completed successfully: close the circuit and reset the counter.
  defp maybe_close(state = %__MODULE__{state: :closed}), do: state

  defp maybe_close(state) do
    :telemetry.execute(
      [:super_worker, :pool, :circuit, :closed],
      %{},
      %{pool: state.pool, partition: state.partition}
    )

    %{state | state: :closed, failure_count: 0, opened_at: nil, in_flight_probes: 0}
  end

  defp record_failure(state = %__MODULE__{state: :closed}) do
    failure_count = state.failure_count + 1

    if failure_count >= state.failure_threshold do
      trip(state, failure_count)
    else
      %{state | failure_count: failure_count}
    end
  end

  defp record_failure(state = %__MODULE__{state: :half_open}) do
    # The probe failed: the service has not recovered, open again.
    trip(state, 0)
  end

  # Failures reported while already :open are late results of jobs submitted
  # before the trip; the reset timer keeps running from the original trip.
  defp record_failure(state), do: state

  defp trip(state, failure_count) do
    :telemetry.execute(
      [:super_worker, :pool, :circuit, :open],
      %{},
      %{pool: state.pool, partition: state.partition}
    )

    %{
      state
      | state: :open,
        failure_count: failure_count,
        opened_at: System.monotonic_time(:millisecond),
        in_flight_probes: 0
    }
  end
end
