defmodule SuperWorker.Pool.TaskWorker do
  @moduledoc """
  Default worker implementation for `SuperWorker.Pool` wrapping a bare
  function or MFA — stateless `SuperWorker.Pool.Worker` behaviour impl.

  Created automatically by `SuperWorker.Pool.start_link/1` when the pool is
  configured with `task: fun` or `task: {module, function, args}`:

  - fun/1 receives the job; fun/0 ignores it;
  - MFA is invoked as `apply(module, function, [job | args])`.

  Any value the fun returns is treated as the job result. Exceptions thrown,
  raised or exited inside the fun are converted to an expected error
  (`{:error, reason, state}`), so a raising task never crashes its worker
  process — it simply fails the job (and, if `:on_failure` is configured,
  fires the dead-letter callback).
  """

  @behaviour SuperWorker.Pool.Worker

  alias SuperWorker.Supervisor.Utils

  @impl true
  def init({:fun, fun}) when is_function(fun, 1), do: {:ok, {:fun, fun}}
  def init({:fun0, fun}) when is_function(fun, 0), do: {:ok, {:fun0, fun}}

  def init(mfa = {:mfa, {module, function, args}})
      when is_atom(module) and is_atom(function) and is_list(args),
      do: {:ok, mfa}

  def init(other), do: {:error, {:invalid_task, other}}

  @impl true
  def handle_job(job, state = {:fun, fun}) do
    case Utils.safe_call(fn -> fun.(job) end) do
      {:ok, result} -> {:ok, result, state}
      {:error, {kind, reason}} -> {:error, {kind, reason}, state}
    end
  end

  def handle_job(_job, state = {:fun0, fun}) do
    case Utils.safe_call(fun) do
      {:ok, result} -> {:ok, result, state}
      {:error, {kind, reason}} -> {:error, {kind, reason}, state}
    end
  end

  def handle_job(job, state = {:mfa, {module, function, args}}) do
    case Utils.safe_call(module, function, [job | args]) do
      {:ok, result} -> {:ok, result, state}
      {:error, {kind, reason}} -> {:error, {kind, reason}, state}
    end
  end

  def handle_job(_job, other), do: {:error, {:invalid_task_state, other}, other}
end
