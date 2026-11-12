defmodule GravitonMQBench.Stats do
  @moduledoc false
  # Pure helpers for scripts/bench/run.exs: no processes, clocks, files or sockets.
  # Tested by apps/graviton_mq/test/graviton_mq/bench_stats_test.exs.

  @doc """
  Exact nearest-rank summary of integer latency samples in microseconds.
  Returns nil when there are no samples.
  """
  def latency_summary([]), do: nil

  def latency_summary(samples) when is_list(samples) do
    sorted = samples |> Enum.sort() |> List.to_tuple()
    count = tuple_size(sorted)

    %{
      count: count,
      p50: nearest_rank(sorted, count, 500),
      p99: nearest_rank(sorted, count, 990),
      p999: nearest_rank(sorted, count, 999),
      max: elem(sorted, count - 1)
    }
  end

  # The smallest sample with at least per_mille/1000 of all samples at or
  # below it: rank = ceil(count * per_mille / 1000), in integer arithmetic.
  defp nearest_rank(sorted, count, per_mille),
    do: elem(sorted, max(div(count * per_mille + 999, 1000), 1) - 1)

  @doc """
  Accounts for consumed sequence numbers.

  `published` maps each publisher index to its publication count; that
  publisher's sequence numbers are 1..count. `seen` maps each consumed
  `{publisher, sequence}` key to the number of times it was consumed.
  """
  def accounting(published, seen) when is_map(published) and is_map(seen) do
    expected = published |> Map.values() |> Enum.sum()
    delivered = Enum.count(seen, fn {key, _times} -> published?(published, key) end)

    %{
      expected: expected,
      delivered: delivered,
      missing: expected - delivered,
      duplicates: Enum.sum(Map.values(seen)) - map_size(seen),
      unexpected: map_size(seen) - delivered
    }
  end

  defp published?(published, {publisher, sequence}) when is_integer(sequence),
    do: sequence >= 1 and sequence <= Map.get(published, publisher, 0)

  defp published?(_published, _key), do: false

  @doc "Duplicates are only reported; a missing or unexpected sequence fails the gate."
  def verdict(%{missing: 0, unexpected: 0}), do: :ok

  def verdict(%{missing: missing, unexpected: unexpected}),
    do: {:error, "missing #{missing}, unexpected #{unexpected}"}

  @doc "Events per second over a window in milliseconds, rounded to 0.1."
  def rate(count, window_ms)
      when is_integer(count) and count >= 0 and is_integer(window_ms) and window_ms > 0,
      do: Float.round(count * 1000 / window_ms, 1)
end
