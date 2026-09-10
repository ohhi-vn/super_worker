defmodule SuperWorker.Pool.Backoff do
  @moduledoc """
  Retry backoff computation for `SuperWorker.Pool`.

  Two forms are supported:

      {:fixed, 500}
      {:exponential, base: 200, max: 10_000, jitter: true}

  - `{:fixed, ms}` always waits `ms` milliseconds.
  - `{:exponential, ...}` waits `min(base * 2^(attempt - 1), max)`; when
    `jitter: true` the delay is randomized in `[delay / 2, delay]` so many
    simultaneous retries do not align on the same tick.

  Attempt numbers start at 1 (the first retry).
  """

  @type t ::
          {:fixed, non_neg_integer()}
          | {:exponential, base: non_neg_integer(), max: pos_integer(), jitter: boolean()}

  @default_base 200
  @default_max 10_000

  @doc """
  Checks whether the given term is a valid backoff specification.
  """
  @spec valid?(term()) :: boolean()
  def valid?({:fixed, ms}) when is_integer(ms) and ms >= 0, do: true

  def valid?({:exponential, opts}) when is_list(opts) do
    base = Keyword.get(opts, :base, @default_base)
    max = Keyword.get(opts, :max, @default_max)
    jitter = Keyword.get(opts, :jitter, false)

    is_integer(base) and base >= 0 and is_integer(max) and max > 0 and is_boolean(jitter)
  end

  def valid?(_spec), do: false

  @doc """
  Returns the delay in milliseconds before the given attempt (1-based).

  Raises `FunctionClauseError` for an invalid specification or attempt; pool
  configuration is validated at `start_link` time, so this should never fire
  in normal operation.
  """
  @spec delay(t(), pos_integer()) :: non_neg_integer()
  def delay({:fixed, ms}, _attempt) when is_integer(ms) and ms >= 0, do: ms

  def delay({:exponential, opts}, attempt) when is_integer(attempt) and attempt > 0 do
    base = Keyword.get(opts, :base, @default_base)
    max = Keyword.get(opts, :max, @default_max)
    raw = min(base * 2 ** (attempt - 1), max)

    if Keyword.get(opts, :jitter, false) do
      half = div(raw, 2)
      half + :rand.uniform(raw - half + 1) - 1
    else
      raw
    end
  end
end
