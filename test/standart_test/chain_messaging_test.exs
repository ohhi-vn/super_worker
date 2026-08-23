defmodule SuperWorker.Supervisor.ChainMessagingTest do
  use ExUnit.Case, async: true

  alias SuperWorker.Supervisor, as: Sup
  alias SuperWorker.Supervisor.{Chain, Db, Message}
  alias SuperWorker.Supervisor.Chain.Messaging

  @moduletag :capture_log

  describe "send_next/3 unit behaviour" do
    test "reports an invalid finished_callback" do
      table = Db.init(:"messaging_unit_#{System.unique_integer([:positive])}")
      chain = %Chain{id: :cb_chain, table: table, finished_callback: :garbage}

      msg = Message.new(:new_data, nil, :payload)

      assert {:error, :invalid_callback} = Messaging.send_next(chain, 9, msg)
    end

    test "returns :no_callback when there is no callback and no worker at order" do
      table = Db.init(:"messaging_unit_#{System.unique_integer([:positive])}")
      chain = %Chain{id: :nc_chain, table: table, finished_callback: nil}

      msg = Message.new(:new_data, nil, :payload)

      assert {:ok, :no_callback} = Messaging.send_next(chain, 1, msg)
    end
  end

  describe "finished callback failures via the running supervisor" do
    setup do
      sup_id = :"messaging_sup_#{System.unique_integer([:positive])}"
      {:ok, _} = Sup.start_with_config(link: false, id: sup_id, num_partitions: 1)
      on_exit(fn -> if Sup.running?(sup_id), do: Sup.stop(sup_id) end)

      %{sup_id: sup_id}
    end

    test "a raising fun callback does not crash anything", %{sup_id: sup_id} do
      chain_id = :"cb_#{System.unique_integer([:positive])}"

      {:ok, _} =
        Sup.add_chain(sup_id,
          id: chain_id,
          finished_callback: {:fun, fn _data -> raise("callback boom") end}
        )

      {:ok, _} =
        Sup.add_chain_worker(sup_id, chain_id, fn data -> {:next, data} end, id: 1)

      assert {:ok, _} = Sup.send_to_chain(sup_id, chain_id, :payload)
      Process.sleep(100)

      # Supervisor and worker are still healthy afterwards.
      assert true == Sup.running?(sup_id)
      {:ok, pid} = Sup.get_pid_chain_worker(sup_id, chain_id, 1)
      assert Process.alive?(pid)
    end

    test "a raising mfa callback does not crash anything", %{sup_id: sup_id} do
      chain_id = :"cb_mfa_#{System.unique_integer([:positive])}"

      {:ok, _} =
        Sup.add_chain(sup_id,
          id: chain_id,
          finished_callback: {__MODULE__, :boom_callback, []}
        )

      {:ok, _} =
        Sup.add_chain_worker(sup_id, chain_id, fn data -> {:next, data} end, id: 1)

      assert {:ok, _} = Sup.send_to_chain(sup_id, chain_id, :payload)
      Process.sleep(100)

      assert true == Sup.running?(sup_id)
    end

    @doc "MFA callback that always fails; used by tests."
    def boom_callback(_data) do
      raise("mfa callback boom")
    end
  end
end
