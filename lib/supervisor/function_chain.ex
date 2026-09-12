defmodule SuperWorker.Supervisor.FunctionChain do
  @moduledoc """
  Bridges `SuperWorker.FunctionChain` into `SuperWorker.Supervisor` worker
  types.

  Two supporters:

  - `chain_node_fun/1,2` — a fun/1 for `Sup.add_chain_worker/4`: every message
    the node receives is run through the chain, the result is forwarded to the
    next node.
  - `job_loop/1,2` — a fun/0 receive loop for standalone or group workers:
    every message the worker receives is run through the chain.
  """

  alias SuperWorker.FunctionChain
  alias SuperWorker.FunctionChain.Executor

  ## Chain node

  @doc """
  Returns a fun/1 to use as a `SuperWorker.Supervisor` chain worker function
  (`Sup.add_chain_worker/4`): each message flowing through the process chain
  is run through `chain`, and `{:ok, result}` is forwarded to the next node
  (`{:next, result}`); a failed run is reported as `{:error, reason}` — the
  data is dropped and the node keeps serving.

  Options:

  - `:run_opts` — run options passed to `SuperWorker.FunctionChain.run/3`:
    a static keyword list, or a fun/1 receiving the message and returning a
    keyword list (per-message `:arg_overrides` / `:context`).

      {:ok, _} = Sup.add_chain(:sup1, [id: :chain1, restart_strategy: :one_for_one])

      {:ok, _} =
        Sup.add_chain_worker(:sup1, :chain1,
          SuperWorker.Supervisor.FunctionChain.chain_node_fun(chain),
          [id: :c1]
        )
  """
  @spec chain_node_fun(FunctionChain.t() | (-> FunctionChain.t()), keyword()) ::
          (term() -> {:next, term()} | {:error, term()})
  def chain_node_fun(chain, opts \\ []) when is_list(opts) do
    state = bridge_state!(chain, opts)

    fn data ->
      chain_node_run(state, data)
    end
  end

  ## Job loop (standalone / group workers)

  @doc """
  Returns a fun/0 receive loop for a standalone or group worker
  (`Sup.add_standalone_worker/3`, `Sup.add_group_worker/4`) that runs every
  received message through `chain`.

  Message protocol:

  - `{:run, ref, job, from}` — request/response: the chain runs on `job` and
    the loop replies `{:super_worker_function_chain, ref, {:ok, result}}` or
    `{:super_worker_function_chain, ref, {:error, reason}}` to `from`.
  - any other message — fire-and-forget: it is run through the chain and the
    result is delivered to the `:on_result` callback, if configured (without
    one, it is dropped).

  Options:

  - `:run_opts` — as in `chain_node_fun/2`.
  - `:on_result` — fun/2 invoked as `(job, {:ok, result} | {:error, reason})`
    in the worker process.

      {:ok, _} =
        Sup.add_standalone_worker(:sup1,
          SuperWorker.Supervisor.FunctionChain.job_loop(chain),
          [id: :fc]
        )

      ref = make_ref()
      Sup.send_to_standalone_worker(:sup1, :fc, {:run, ref, job, self()})

      receive do
        {:super_worker_function_chain, ^ref, result} -> result
      end

  A message that happens to be a 4-element `{:run, _, _, _}` tuple is treated
  as the request/response envelope, not as a plain job. The loop itself never
  exits; crashes inside the chain are converted to `{:error, ...}` results.
  """
  @spec job_loop(FunctionChain.t() | (-> FunctionChain.t()), keyword()) ::
          (-> no_return())
  def job_loop(chain, opts \\ []) when is_list(opts) do
    state = bridge_state!(chain, opts)
    on_result = Keyword.get(opts, :on_result)

    fn ->
      do_job_loop(state, on_result)
    end
  end

  defp do_job_loop(state, on_result) do
    receive do
      {:run, ref, job, from} when is_reference(ref) and is_pid(from) ->
        result = Executor.run(state.chain, state.run_opts, job)

        case result do
          {:error, reason, _meta} ->
            send(from, {:super_worker_function_chain, ref, {:error, reason}})

          other ->
            send(from, {:super_worker_function_chain, ref, other})
        end

        maybe_on_result(on_result, job, normalize(result))

      job ->
        result = Executor.run(state.chain, state.run_opts, job)
        maybe_on_result(on_result, job, normalize(result))
    end

    do_job_loop(state, on_result)
  end

  defp maybe_on_result(nil, _job, _result), do: :ok

  defp maybe_on_result(on_result, job, result) when is_function(on_result, 2),
    do: on_result.(job, result)

  # normalize() strips the return_context meta so :on_result always sees a
  # 2-tuple, matching the Pool's :on_result shape.
  defp normalize({:error, reason, _meta}), do: {:error, reason}
  defp normalize(other), do: other

  ## Shared internals

  defp bridge_state!(chain, opts) do
    unless Executor.valid_chain_spec?(chain) do
      raise ArgumentError,
            "invalid :chain — expected %SuperWorker.FunctionChain{} or a fun/0 returning one, got: #{inspect(chain)}"
    end

    run_opts = Keyword.get(opts, :run_opts, [])

    unless Executor.valid_run_opts?(run_opts) do
      raise ArgumentError,
            "invalid :run_opts — expected a keyword list or a fun/1, got: #{inspect(run_opts)}"
    end

    %{chain: chain, run_opts: run_opts}
  end

  defp chain_node_run(state, data) do
    case Executor.run(state.chain, state.run_opts, data) do
      {:ok, result} -> {:next, result}
      {:error, reason} -> {:error, reason}
      {:error, reason, _meta} -> {:error, reason}
    end
  end
end
