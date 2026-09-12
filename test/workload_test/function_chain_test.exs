defmodule SuperWorker.Supervisor.FunctionChainTest do
  @moduledoc false

  use ExUnit.Case, async: false

  alias SuperWorker.FunctionChain
  alias SuperWorker.Supervisor, as: Sup
  alias SuperWorker.Supervisor.FunctionChain, as: Bridge

  @moduletag :capture_log

  setup do
    sup_id = :"sup_fc_#{System.unique_integer([:positive])}"
    {:ok, _} = Sup.start_with_config(link: false, id: sup_id, num_partitions: 2)

    on_exit(fn ->
      if Sup.running?(sup_id) do
        try do
          Sup.stop(sup_id)
        catch
          :exit, _ -> :ok
        end
      end
    end)

    %{sup_id: sup_id}
  end

  defp echo_chain do
    FunctionChain.new()
    |> FunctionChain.add(:double, fn n -> {:ok, n * 2} end)
    |> FunctionChain.add(:stringify, fn n -> {:ok, "n=#{n}"} end)
  end

  # ---------------------------------------------------------------------------
  # chain_node_fun/2
  # ---------------------------------------------------------------------------

  test "chain_node_fun maps {:ok, _} to {:next, _}" do
    fun = Bridge.chain_node_fun(echo_chain())

    assert fun.(2) == {:next, "n=4"}
    assert fun.(5) == {:next, "n=10"}
  end

  test "chain_node_fun maps chain failure to {:error, reason}" do
    chain =
      FunctionChain.new()
      |> FunctionChain.add(:fail, fn _ -> {:error, :not_allowed} end)

    fun = Bridge.chain_node_fun(chain, run_opts: [return_context: true])

    # return_context meta is stripped; the node contract stays 2-tuple.
    assert fun.(:data) == {:error, :not_allowed}
  end

  test "chain_node_fun as a real supervisor chain worker forwards the result" do
    sup_id = :"sup_fc_node_#{System.unique_integer([:positive])}"
    {:ok, _} = Sup.start_with_config(link: false, id: sup_id, num_partitions: 1)

    test_pid = self()

    # The chain node runs the FunctionChain; a plain fun worker then sends
    # the value it receives to the test.
    {:ok, _} = Sup.add_chain(sup_id, id: :chain1, restart_strategy: :one_for_one)

    {:ok, _} =
      Sup.add_chain_worker(sup_id, :chain1, Bridge.chain_node_fun(echo_chain()), id: :fc_node)

    {:ok, _} =
      Sup.add_chain_worker(
        sup_id,
        :chain1,
        fn value ->
          send(test_pid, {:got, value})
          {:next, value}
        end,
        id: :tail
      )

    assert {:ok, _} = Sup.send_to_chain(sup_id, :chain1, 21)

    assert_receive {:got, "n=42"}, 5_000
    Sup.stop(sup_id)
  end

  test "chain_node_fun accepts a fun/0 chain builder" do
    fun = Bridge.chain_node_fun(fn -> echo_chain() end)
    assert fun.(3) == {:next, "n=6"}
  end

  test "chain_node_fun raises on an invalid chain" do
    assert_raise ArgumentError, ~r/invalid :chain/, fn ->
      Bridge.chain_node_fun(:not_a_chain)
    end
  end

  # ---------------------------------------------------------------------------
  # job_loop/2
  # ---------------------------------------------------------------------------

  test "job_loop as a standalone worker: request/response and fire-and-forget", %{
    sup_id: sup_id
  } do
    test_pid = self()

    on_result = fn job, result -> send(test_pid, {:on_result, job, result}) end

    {:ok, _} =
      Sup.add_standalone_worker(
        sup_id,
        Bridge.job_loop(echo_chain(), on_result: on_result),
        id: :fc
      )

    # Request/response envelope.
    ref = make_ref()
    assert :ok = Sup.send_to_standalone_worker(sup_id, :fc, {:run, ref, 2, self()})

    assert_receive {:super_worker_function_chain, ^ref, {:ok, "n=4"}}, 5_000

    # Fire-and-forget: any other message is a job; the result goes to :on_result.
    assert :ok = Sup.send_to_standalone_worker(sup_id, :fc, 5)
    assert_receive {:on_result, 5, {:ok, "n=10"}}, 5_000

    # A failing run is an error result, not a loop crash.
    assert :ok = Sup.send_to_standalone_worker(sup_id, :fc, :not_a_number)
    assert_receive {:on_result, :not_a_number, {:error, _}}, 5_000

    # The worker survived the failure.
    ref2 = make_ref()
    assert :ok = Sup.send_to_standalone_worker(sup_id, :fc, {:run, ref2, 3, self()})
    assert_receive {:super_worker_function_chain, ^ref2, {:ok, "n=6"}}, 5_000
  end

  test "job_loop in a group: every worker runs the chain on its messages", %{sup_id: sup_id} do
    test_pid = self()

    {:ok, _} = Sup.add_group(sup_id, id: :group1, restart_strategy: :one_for_one)

    for i <- 1..3 do
      {:ok, _} =
        Sup.add_group_worker(
          sup_id,
          :group1,
          Bridge.job_loop(echo_chain()),
          id: {:fc_g, i}
        )
    end

    ref = make_ref()

    assert :ok =
             Sup.send_to_group_worker(sup_id, :group1, {:fc_g, 2}, {:run, ref, 7, self()})

    assert_receive {:super_worker_function_chain, ^ref, {:ok, "n=14"}}, 5_000
  end

  test "job_loop raises on invalid options" do
    assert_raise ArgumentError, ~r/invalid :run_opts/, fn ->
      Bridge.job_loop(echo_chain(), run_opts: :bad)
    end
  end
end
