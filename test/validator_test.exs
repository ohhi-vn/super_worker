defmodule SuperWorker.Supervisor.ValidatorTest do
  use ExUnit.Case, async: true

  alias SuperWorker.Supervisor.Validator

  describe "normalize_opts/2" do
    test "normalizes keyword list to map" do
      assert {:ok, %{id: :test, count: 5}} =
               Validator.normalize_options([id: :test, count: 5], [:id, :count])
    end

    test "accepts empty options list" do
      assert {:ok, %{}} = Validator.normalize_options([], [:id, :count])
    end

    test "rejects invalid options" do
      assert {:error, {:invalid_options, [:invalid]}} =
               Validator.normalize_options([id: :test, invalid: :opt], [:id])
    end

    test "handles multiple invalid options" do
      assert {:error, {:invalid_options, invalid}} =
               Validator.normalize_options([good: 1, bad1: 2, bad2: 3], [:good])

      assert :bad1 in invalid
      assert :bad2 in invalid
    end

    test "handles atom shorthand for type" do
      assert {:ok, %{type: :group}} = Validator.normalize_options([:group], [:type])
      assert {:ok, %{type: :chain}} = Validator.normalize_options([:chain], [:type])
      assert {:ok, %{type: :standalone}} = Validator.normalize_options([:standalone], [:type])
    end

    test "handles mixed keyword and shorthand" do
      assert {:ok, %{type: :group, id: :test}} =
               Validator.normalize_options([:group, id: :test], [:type, :id])
    end

    test "preserves all valid options" do
      opts = [id: :sup1, count: 10, enabled: true, data: "test"]
      params = [:id, :count, :enabled, :data]

      assert {:ok, result} = Validator.normalize_options(opts, params)
      assert result.id == :sup1
      assert result.count == 10
      assert result.enabled == true
      assert result.data == "test"
    end

    test "handles boolean flag options" do
      assert {:ok, %{enabled: true}} = Validator.normalize_options([:enabled], [:enabled])
    end
  end

  describe "check_type/3" do
    test "validates correct type" do
      opts = %{count: 5}
      assert {:ok, ^opts} = Validator.check_type(opts, :count, &is_integer/1)
    end

    test "rejects incorrect type" do
      opts = %{count: "five"}
      assert {:error, :invalid_type} = Validator.check_type(opts, :count, &is_integer/1)
    end

    test "returns error for missing key" do
      opts = %{other: :value}
      assert {:error, :invalid_type} = Validator.check_type(opts, :missing, &is_atom/1)
    end

    test "works with various validators" do
      assert {:ok, _} = Validator.check_type(%{val: :atom}, :val, &is_atom/1)
      assert {:ok, _} = Validator.check_type(%{val: "string"}, :val, &is_binary/1)
      assert {:ok, _} = Validator.check_type(%{val: []}, :val, &is_list/1)
      assert {:ok, _} = Validator.check_type(%{val: %{}}, :val, &is_map/1)
    end

    test "works with custom validators" do
      positive? = fn x -> is_integer(x) and x > 0 end
      assert {:ok, _} = Validator.check_type(%{val: 5}, :val, positive?)
      assert {:error, :invalid_type} = Validator.check_type(%{val: -5}, :val, positive?)
    end
  end

  describe "get_keyword/1" do
    test "converts group shorthand" do
      assert {:type, :group} = Validator.get_keyword(:group)
    end

    test "converts chain shorthand" do
      assert {:type, :chain} = Validator.get_keyword(:chain)
    end

    test "converts standalone shorthand" do
      assert {:type, :standalone} = Validator.get_keyword(:standalone)
    end

    test "returns error for unknown keyword" do
      assert {:error, :invalid_options} = Validator.get_keyword(:unknown)
      assert {:error, :invalid_options} = Validator.get_keyword(:other)
    end
  end
end
