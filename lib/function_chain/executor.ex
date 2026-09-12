defmodule SuperWorker.FunctionChain.Executor do
  @moduledoc """
  Shared plumbing for hosts that execute a `SuperWorker.FunctionChain` per
  job/message — currently `SuperWorker.Pool.FunctionChain` and
  `SuperWorker.Supervisor.FunctionChain`.

  A host holds two values resolved per execution:

  - a **chain spec** — a `%SuperWorker.FunctionChain{}` struct, or a fun/0
    returning one (a fresh chain per execution);
  - **run options** — a static keyword list passed to
    `SuperWorker.FunctionChain.run/3`, or a fun/1 receiving the input and
    returning a keyword list (per-input `:arg_overrides` / `:context`,
    safe for concurrent runs of a shared chain).

  `run/3` resolves both and normalizes anything thrown or raised to
  `{:error, {:exception, exception}}` / `{:error, {kind, reason}}` so a
  failing chain never crashes its host process.
  """

  alias SuperWorker.FunctionChain

  @doc "Returns true for a `%SuperWorker.FunctionChain{}` or a fun/0 chain builder."
  @spec valid_chain_spec?(term()) :: boolean()
  def valid_chain_spec?(%FunctionChain{}), do: true
  def valid_chain_spec?(fun) when is_function(fun, 0), do: true
  def valid_chain_spec?(_), do: false

  @doc "Returns true for a keyword list or a fun/1 of run options."
  @spec valid_run_opts?(term()) :: boolean()
  def valid_run_opts?(opts) when is_list(opts), do: true
  def valid_run_opts?(fun) when is_function(fun, 1), do: true
  def valid_run_opts?(_), do: false

  @doc """
  Runs the chain spec on `input` with `run_opts` (see the moduledoc).

  Returns the shapes of `SuperWorker.FunctionChain.run/3`, with exceptions
  and throws normalized to errors.
  """
  @spec run(term(), term(), term()) ::
          {:ok, term()} | {:error, term()} | {:error, term(), map()}
  def run(chain_spec, run_opts, input) do
    chain = resolve_chain(chain_spec)
    opts = resolve_run_opts(run_opts, input)

    try do
      FunctionChain.run(chain, input, opts)
    rescue
      e -> {:error, {:exception, e}}
    catch
      kind, reason -> {:error, {kind, reason}}
    end
  end

  defp resolve_chain(chain = %FunctionChain{}), do: chain
  defp resolve_chain(fun) when is_function(fun, 0), do: fun.()

  defp resolve_run_opts(run_opts, _input) when is_list(run_opts), do: run_opts
  defp resolve_run_opts(fun, input) when is_function(fun, 1), do: fun.(input)
end
