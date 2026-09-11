defmodule SuperWorker.Pool.Worker do
  @moduledoc """
  Behaviour for stateful pool workers.

  Modules implementing this behaviour can be handed to `SuperWorker.Pool.start_link/1`
  via the `:worker` option. Each pool worker process runs `init/1` once and then
  processes jobs with `handle_job/2`, keeping its state between jobs — e.g. a
  database connection, an HTTP client or an accumulator buffer.

  Bare functions and MFAs do not need this behaviour; they are auto-wrapped
  into a stateless default implementation (see `SuperWorker.Pool.TaskWorker`).

  ## Return values of `handle_job/2`

  - `{:ok, result, new_state}` — the job succeeded; `result` is returned to
    the caller (`run`/`await`) or delivered to `:on_result` (cast).
  - `{:error, reason, new_state}` — expected, final failure. The job is not
    retried; `:on_failure` fires after this (the reason is not logged as a
    crash and the worker keeps its state).
  - `{:retry, reason, new_state}` — "please try again". The worker stays
    free; the partition reschedules the job after a backoff delay until
    `max_attempts` is exhausted. Use this for transient conditions
    (timeouts, throttling) as opposed to validation errors.
  """

  @callback init(opts :: keyword()) :: {:ok, state :: term()} | {:error, reason :: term()}

  @callback handle_job(job :: term(), state :: term()) ::
              {:ok, result :: term(), new_state :: term()}
              | {:error, reason :: term(), new_state :: term()}
              | {:retry, reason :: term(), new_state :: term()}
end
