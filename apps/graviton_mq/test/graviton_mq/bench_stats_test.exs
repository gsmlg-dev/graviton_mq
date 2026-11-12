Code.require_file("../../../../scripts/bench/stats.exs", __DIR__)

defmodule GravitonMQ.BenchStatsTest do
  use ExUnit.Case, async: true

  alias GravitonMQBench.Stats

  describe "latency_summary/1" do
    test "has no summary without samples" do
      assert Stats.latency_summary([]) == nil
    end

    test "a single sample is every percentile" do
      assert Stats.latency_summary([42]) == %{count: 1, p50: 42, p99: 42, p999: 42, max: 42}
    end

    test "uses exact nearest ranks on unsorted samples" do
      samples = Enum.shuffle(1..1000)

      assert Stats.latency_summary(samples) ==
               %{count: 1000, p50: 500, p99: 990, p999: 999, max: 1000}
    end

    test "rounds the rank up for small sample counts" do
      assert Stats.latency_summary(Enum.to_list(10..1//-1)) ==
               %{count: 10, p50: 5, p99: 10, p999: 10, max: 10}

      assert Stats.latency_summary([3, 1, 2, 2]) == %{count: 4, p50: 2, p99: 3, p999: 3, max: 3}
    end
  end

  describe "accounting/2" do
    test "every publication consumed once" do
      seen = %{{0, 1} => 1, {0, 2} => 1, {0, 3} => 1, {1, 1} => 1, {1, 2} => 1}
      accounting = Stats.accounting(%{0 => 3, 1 => 2}, seen)

      assert accounting == %{expected: 5, delivered: 5, missing: 0, duplicates: 0, unexpected: 0}
      assert Stats.verdict(accounting) == :ok
    end

    test "duplicates are counted but do not fail the gate" do
      accounting = Stats.accounting(%{0 => 2}, %{{0, 1} => 3, {0, 2} => 1})

      assert accounting == %{expected: 2, delivered: 2, missing: 0, duplicates: 2, unexpected: 0}
      assert Stats.verdict(accounting) == :ok
    end

    test "a missing sequence number fails the gate" do
      accounting = Stats.accounting(%{0 => 3}, %{{0, 1} => 1, {0, 3} => 2})

      assert accounting == %{expected: 3, delivered: 2, missing: 1, duplicates: 1, unexpected: 0}
      assert Stats.verdict(accounting) == {:error, "missing 1, unexpected 0"}
    end

    test "a sequence number nobody published fails the gate" do
      seen = %{{0, 1} => 1, {0, 2} => 1, {7, 1} => 1}
      accounting = Stats.accounting(%{0 => 1}, seen)

      assert accounting == %{expected: 1, delivered: 1, missing: 0, duplicates: 0, unexpected: 2}
      assert Stats.verdict(accounting) == {:error, "missing 0, unexpected 2"}
    end

    test "nothing published and nothing consumed passes" do
      accounting = Stats.accounting(%{0 => 0}, %{})

      assert accounting == %{expected: 0, delivered: 0, missing: 0, duplicates: 0, unexpected: 0}
      assert Stats.verdict(accounting) == :ok
    end
  end

  test "rate/2 is events per second over a millisecond window" do
    assert Stats.rate(500, 2_000) == 250.0
    assert Stats.rate(1, 3_000) == 0.3
    assert Stats.rate(0, 20_000) == 0.0
  end
end
