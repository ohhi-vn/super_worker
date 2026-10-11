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

  describe "routing contract (new_data/2 and send_next/3)" do
    setup do
      Process.flag(:trap_exit, true)
      table = Db.init(:"messaging_contract_#{System.unique_integer([:positive])}")
      %{table: table}
    end

    test "new_data/2 delivers to the node at order 1", %{table: table} do
      parent = self()
      chain = %Chain{id: make_ref(), table: table, send_type: :random}

      {:ok, chain} =
        Chain.add_worker(chain, %SuperWorker.Supervisor.Worker{
          id: :first,
          type: :chain,
          fun:
            {:fun,
             fn d ->
               send(parent, {:got, d})
               {:drop, :seen}
             end}
        })

      msg = Message.new(:new_data, nil, :hello)
      assert {:ok, :sent_to_one} = Messaging.new_data(chain, msg)
      assert_receive {:got, :hello}, 2_000
    end

    test "send_next/3 past the last node runs the finished callback", %{table: table} do
      parent = self()

      chain = %Chain{
        id: make_ref(),
        table: table,
        finished_callback: {:fun, fn d -> send(parent, {:done, d}) end}
      }

      msg = Message.new(:new_data, nil, :end_data)
      assert {:ok, :callback_executed} = Messaging.send_next(chain, 1, msg)
      assert_receive {:done, :end_data}
    end

    test "send_next/3 reports :no_live_replica when every replica is dead", %{table: table} do
      chain = %Chain{id: make_ref(), table: table}

      {:ok, chain} =
        Chain.add_worker(chain, %SuperWorker.Supervisor.Worker{
          id: :dead,
          type: :chain,
          fun: {:fun, fn d -> {:drop, d} end}
        })

      {:ok, _} = Chain.kill_worker(chain, :dead)

      msg = Message.new(:new_data, nil, :payload)
      assert {:error, :no_live_replica} = Messaging.send_next(chain, 1, msg)
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
