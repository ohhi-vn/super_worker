defmodule SuperWorker.Pool.BackoffTest do
  @moduledoc false

  use ExUnit.Case, async: true

  alias SuperWorker.Pool.Backoff

  describe "valid?/1" do
    test "accepts fixed and exponential specs" do
      assert Backoff.valid?({:fixed, 100})
      assert Backoff.valid?({:fixed, 0})
      assert Backoff.valid?({:exponential, []})
      assert Backoff.valid?({:exponential, base: 10, max: 100, jitter: true})
    end

    test "rejects invalid specs" do
      refute Backoff.valid?(:nope)
      refute Backoff.valid?({:fixed, -1})
      refute Backoff.valid?({:exponential, base: -1})
      refute Backoff.valid?({:exponential, max: 0})
      refute Backoff.valid?({:exponential, jitter: "yes"})
    end
  end

  describe "delay/2" do
    test "fixed spec always returns the same delay" do
      assert Backoff.delay({:fixed, 500}, 1) == 500
      assert Backoff.delay({:fixed, 500}, 7) == 500
    end

    test "exponential grows from base" do
      spec = {:exponential, base: 100, max: 10_000, jitter: false}
      assert Backoff.delay(spec, 1) == 100
      assert Backoff.delay(spec, 2) == 200
      assert Backoff.delay(spec, 3) == 400
      assert Backoff.delay(spec, 4) == 800
    end

    test "exponential is capped at max" do
      spec = {:exponential, base: 100, max: 250, jitter: false}
      assert Backoff.delay(spec, 10) == 250
    end

    test "jitter stays within [delay / 2, delay]" do
      spec = {:exponential, base: 400, max: 10_000, jitter: true}

      for attempt <- 1..4 do
        upper = 400 * 2 ** (attempt - 1)
        lower = div(upper, 2)

        for _ <- 1..20 do
          delay = Backoff.delay(spec, attempt)
          assert delay >= lower and delay <= upper
        end
      end
    end
  end
end
