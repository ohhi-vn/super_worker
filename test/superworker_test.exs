defmodule SuperWorkerTest do
  use ExUnit.Case
  doctest SuperWorker

  test "greets the world" do
    assert SuperWorker.hello() == :world
  end
end
