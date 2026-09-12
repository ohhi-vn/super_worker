defmodule SuperWorker.Pool.FunctionChain do
  @moduledoc """
  `SuperWorker.Pool.Worker` implementation that runs a
  `SuperWorker.FunctionChain` for every submitted job — fanning chain runs
  out over the pool's partitions.

      {:ok, _} =
        SuperWorker.Pool.start_link(
          name: MyChainPool,
          worker: SuperWorker.Pool.FunctionChain,
          worker_opts: [
            chain: MyApp.MyChain,   # %SuperWorker.FunctionChain{} or fun/0
            run_opts: [],           # keyword, or fun/1 job -> keyword
            on_error: :error        # :error (default) | :retry
          ],
          size: 20
        )

      {:ok, result} = SuperWorker.Pool.run(MyChainPool, %{id: 123})

  Options (`worker_opts`):

  - `:chain` (required) — a `%SuperWorker.FunctionChain{}` struct, or a fun/0
    returning one (a fresh chain per job — e.g. a chain built per job from a
    prototype).
  - `:run_opts` — run options passed to `SuperWorker.FunctionChain.run/3`:
    a static keyword list, or a fun/1 receiving the job and returning a
    keyword list (per-job `:arg_overrides` / `:context` — safe for concurrent
    runs of a shared chain).
  - `:on_error` — what a failed chain run means for the pool:
    `:error` (default) fails the job (the pool's `:on_failure` fires);
    `:retry` treats it as transient and lets the pool's retry budget
    reschedule the whole chain run.

  Exceptions raised by the chain are normalized to
  `{:error, {:exception, exception}}` — a raising chain never crashes its
  worker process. Note the chain's own per-step `:on_error: :retry` strategy
  is independent of, and applied before, the pool-level retry budget.
  """

  @behaviour SuperWorker.Pool.Worker

  alias SuperWorker.FunctionChain.Executor

  @impl true
  def init(opts) when is_list(opts) do
    on_error = Keyword.get(opts, :on_error, :error)

    cond do
      not Keyword.has_key?(opts, :chain) ->
        {:error, {:missing, :chain}}

      not Executor.valid_chain_spec?(Keyword.get(opts, :chain)) ->
        {:error, {:invalid, {:chain, Keyword.get(opts, :chain)}}}

      not Executor.valid_run_opts?(Keyword.get(opts, :run_opts, [])) ->
        {:error, {:invalid, {:run_opts, Keyword.get(opts, :run_opts)}}}

      on_error not in [:error, :retry] ->
        {:error, {:invalid, {:on_error, on_error}}}

      true ->
        {:ok,
         %{
           chain: Keyword.fetch!(opts, :chain),
           run_opts: Keyword.get(opts, :run_opts, []),
           on_error: on_error
         }}
    end
  end

  def init(other), do: {:error, {:invalid_worker_opts, other}}

  @impl true
  def handle_job(job, state) do
    case Executor.run(state.chain, state.run_opts, job) do
      {:ok, result} ->
        {:ok, result, state}

      {:error, reason} ->
        classify_failure(state, reason)

      # return_context: true shape
      {:error, reason, _meta} ->
        classify_failure(state, reason)
    end
  end

  defp classify_failure(state = %{on_error: :retry}, reason), do: {:retry, reason, state}
  defp classify_failure(state, reason), do: {:error, reason, state}
end
