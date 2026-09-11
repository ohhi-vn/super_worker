defmodule SuperWorker.Pool.Middleware.Telemetry do
  @moduledoc """
  Telemetry middleware for `SuperWorker.Pool`.

  Emits the following `:telemetry` events around each job execution (runs in
  the worker process):

  | event                                   | measurements            | metadata |
  |-----------------------------------------|-------------------------|----------|
  | `[:super_worker, :pool, :job, :start]`  | `%{system_time}`        | pool, partition, job_id, attempts |
  | `[:super_worker, :pool, :job, :stop]`   | `%{duration}` (native)  | pool, partition, job_id, attempts, status (`:ok\|:error\|:retry`) |
  | `[:super_worker, :pool, :job, :exception]` | `%{duration}` (native) | pool, partition, job_id, attempts, kind, error |

  The partition process additionally emits pool-level events directly (no
  middleware needed):

  | event                                        | metadata |
  |----------------------------------------------|----------|
  | `[:super_worker, :pool, :job, :retry]`       | pool, partition, job_id, attempts, delay, reason |
  | `[:super_worker, :pool, :job, :crash]`       | pool, partition, job_id, attempts, reason |
  | `[:super_worker, :pool, :job, :dead_letter]` | pool, partition, job_id, attempts, reason |
  | `[:super_worker, :pool, :overloaded]`        | pool, partition, job_id (nil) |
  """

  @behaviour SuperWorker.Pool.Middleware

  @prefix [:super_worker, :pool, :job]

  @impl true
  def call(job, meta, next) do
    start_time = System.monotonic_time()

    :telemetry.execute(@prefix ++ [:start], %{system_time: System.system_time()}, meta)

    try do
      result = next.(job, meta)

      # executor output is {outcome, new_worker_state}; status is the
      # outcome's tag: :ok | :error | :retry
      status = result |> elem(0) |> elem(0)

      :telemetry.execute(
        @prefix ++ [:stop],
        %{duration: System.monotonic_time() - start_time},
        Map.put(meta, :status, status)
      )

      result
    rescue
      error ->
        :telemetry.execute(
          @prefix ++ [:exception],
          %{duration: System.monotonic_time() - start_time},
          Map.merge(meta, %{kind: :error, error: error})
        )

        reraise error, __STACKTRACE__
    end
  end
end
