defmodule SuperWorker.Supervisor.Constants.Timeouts do
  @moduledoc """
  This module contains all timeout-related constants used in the SuperWorker system.
  """

  @default_api_timeout 3_000
  @default_stop_timeout 5_000
  @default_chain_timeout 5_000

  @doc "Default timeout for API calls in milliseconds."
  @spec default_api_timeout() :: non_neg_integer()
  def default_api_timeout, do: @default_api_timeout

  @doc "Default timeout for stop operations in milliseconds."
  @spec default_stop_timeout() :: non_neg_integer()
  def default_stop_timeout, do: @default_stop_timeout

  @doc "Default timeout for chain operations in milliseconds."
  @spec default_chain_timeout() :: non_neg_integer()
  def default_chain_timeout, do: @default_chain_timeout
end
