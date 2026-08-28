defmodule SuperWorker.Supervisor.ErrorHandlerTest do
  use ExUnit.Case, async: true

  alias SuperWorker.Error
  alias SuperWorker.Supervisor.ErrorHandler

  doctest ErrorHandler

  describe "not_found/1" do
    test "returns typed not found errors" do
      assert ErrorHandler.not_found(:worker) == {:error, :worker_not_found}
      assert ErrorHandler.not_found(:group) == {:error, :group_not_found}
      assert ErrorHandler.not_found(:chain) == {:error, :chain_not_found}
      assert ErrorHandler.not_found(:supervisor) == {:error, :supervisor_not_found}
      assert ErrorHandler.not_found() == {:error, :not_found}
      assert ErrorHandler.not_found(:anything_else) == {:error, :not_found}
    end
  end

  describe "already_exists/1" do
    test "returns typed already exists errors" do
      assert ErrorHandler.already_exists(:worker) == {:error, :worker_already_exists}
      assert ErrorHandler.already_exists(:group) == {:error, :group_already_exists}
      assert ErrorHandler.already_exists(:chain) == {:error, :chain_already_exists}
      assert ErrorHandler.already_exists(:supervisor) == {:error, :supervisor_already_exists}
      assert ErrorHandler.already_exists() == {:error, :already_exists}
    end
  end

  describe "error tuple helpers" do
    test "invalid_options/1 wraps details" do
      assert ErrorHandler.invalid_options([:bad]) == {:error, {:invalid_options, [:bad]}}
    end

    test "invalid_config/1 wraps details" do
      assert ErrorHandler.invalid_config(:nope) == {:error, {:invalid_config, :nope}}
    end

    test "api_timeout/0, already_running/0 and not_running/0" do
      assert ErrorHandler.api_timeout() == {:error, :api_timeout}
      assert ErrorHandler.already_running() == {:error, :already_running}
      assert ErrorHandler.not_running() == {:error, :not_running}
    end
  end

  describe "exception messages" do
    test "maps known reasons to readable text" do
      assert Exception.message(ErrorHandler.exception(reason: :worker_not_found)) ==
               "Worker not found"

      assert Exception.message(ErrorHandler.exception(reason: :api_timeout)) ==
               "API call timed out"
    end

    test "falls back to inspect for unknown reasons" do
      assert Exception.message(ErrorHandler.exception(reason: {:weird, 1})) =~
               inspect({:weird, 1})
    end
  end

  describe "log_error/3" do
    test "logs and returns :ok" do
      assert :ok =
               ErrorHandler.log_error(__MODULE__, "test failure", worker_id: :w1, reason: :x)
    end
  end

  describe "SuperWorker.Error" do
    test "formats known atom reasons" do
      assert Exception.message(Error.exception(:queue_full)) == "Queue is full"
      assert Exception.message(Error.exception("plain message")) == "plain message"

      # Unknown atoms are humanized.
      assert Exception.message(Error.exception(:some_new_error)) == "Some new error"
    end

    test "formats every documented atom reason" do
      expected = %{
        supervisor_not_running: "Supervisor is not running",
        already_running: "Supervisor is already running",
        worker_not_found: "Worker not found",
        worker_already_exists: "Worker already exists",
        group_not_found: "Group not found",
        chain_not_found: "Chain not found",
        invalid_options: "Invalid options provided",
        invalid_restart_strategy: "Invalid restart strategy",
        timeout: "Operation timed out",
        api_timeout: "API call timed out",
        queue_full: "Queue is full",
        partition_not_found: "Partition not found"
      }

      Enum.each(expected, fn {reason, message} ->
        assert Error.format_message(reason) == message
      end)
    end

    test "exception/1 accepts keyword options and arbitrary reasons" do
      error = Error.exception(reason: :timeout, message: "custom text")
      assert error.reason == :timeout
      assert Exception.message(error) == "custom text"

      assert Exception.message(Error.exception(%{weird: :term})) == "%{weird: :term}"
    end

    test "message/1 falls back to the reason when no message was set" do
      assert Exception.message(Error.exception(:worker_not_found)) == "Worker not found"

      # A struct built directly (message: nil) also falls back to the reason.
      assert Exception.message(%Error{reason: :worker_not_found}) == "Worker not found"
      assert Exception.message(%Error{reason: :some_new_error}) == "Some new error"
      assert Exception.message(%Error{reason: "plain text"}) == "plain text"
    end

    test "raise_error raises a SuperWorker.Error" do
      assert_raise Error, "Worker not found", fn ->
        ErrorHandler.raise_error(:worker_not_found)
      end
    end
  end

  describe "exception message coverage for all known reasons" do
    test "ErrorHandler.message/1 maps every documented reason" do
      reasons_to_messages = %{
        :not_found => "Resource not found",
        :already_exists => "Resource already exists",
        :already_running => "Process is already running",
        :not_running => "Process is not running",
        :api_timeout => "API call timed out",
        :invalid_type => "Invalid type provided",
        :worker_not_found => "Worker not found",
        :group_not_found => "Group not found",
        :chain_not_found => "Chain not found",
        :supervisor_not_found => "Supervisor not found",
        :worker_already_exists => "Worker already exists",
        :group_already_exists => "Group already exists",
        :chain_already_exists => "Chain already exists",
        :supervisor_already_exists => "Supervisor already exists"
      }

      Enum.each(reasons_to_messages, fn {reason, expected} ->
        exception = ErrorHandler.exception(reason: reason)

        assert Exception.message(exception) == expected
      end)

      assert Exception.message(ErrorHandler.exception(reason: {:invalid_options, [:x]})) =~
               "Invalid options"

      assert Exception.message(ErrorHandler.exception(reason: {:custom, %{a: 1}})) =~
               "custom error"
    end

    test "log_error/2 works without context" do
      assert :ok = ErrorHandler.log_error(__MODULE__, "no context given")
    end
  end
end
