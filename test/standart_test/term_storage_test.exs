defmodule SuperWorker.TermStorageTest do
  use ExUnit.Case, async: true

  alias SuperWorker.TermStorage

  doctest TermStorage

  setup do
    key = :"term_storage_test_#{System.unique_integer([:positive])}"
    on_exit(fn -> TermStorage.delete(key) end)
    %{key: key}
  end

  test "put then get returns the value", %{key: key} do
    assert {:error, :not_found} = TermStorage.get(key)

    TermStorage.put(key, %{some: :data})

    assert {:ok, %{some: :data}} = TermStorage.get(key)
  end

  test "put overwrites existing values", %{key: key} do
    TermStorage.put(key, 1)
    TermStorage.put(key, 2)

    assert {:ok, 2} = TermStorage.get(key)
  end

  test "delete removes the key", %{key: key} do
    TermStorage.put(key, :value)
    assert {:ok, :value} = TermStorage.get(key)

    TermStorage.delete(key)

    assert {:error, :not_found} = TermStorage.get(key)
  end

  test "get_all includes keys from this module only" do
    unique_key = :"term_storage_all_#{System.unique_integer([:positive])}"
    TermStorage.put(unique_key, :marker)
    on_exit(fn -> TermStorage.delete(unique_key) end)

    all = TermStorage.get_all()

    # Keys come back without the internal module prefix.
    assert {^unique_key, :marker} = Enum.find(all, fn {k, _v} -> k == unique_key end)
  end

  test "a stored nil is distinguishable from a missing key", %{key: key} do
    assert {:error, :not_found} = TermStorage.get(key)

    TermStorage.put(key, nil)

    assert {:ok, nil} = TermStorage.get(key)
  end
end
