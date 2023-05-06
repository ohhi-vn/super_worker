defmodule SuperworkerTest do
  use ExUnit.Case
  doctest Superworker

  test "greets the world" do
    assert Superworker.hello() == :world
  end
end
