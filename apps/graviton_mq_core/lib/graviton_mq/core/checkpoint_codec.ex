defmodule GravitonMQ.Core.CheckpointCodec do
  @moduledoc """
  Bounded portable checkpoint terms. Role-owned adapters must validate their
  schemas separately. ETF is inspected before the VM decoder allocates terms.
  Only existing atoms, 64-bit integers, binaries, proper lists, tuples and maps
  are accepted; compressed ETF and runtime handles are never accepted.
  """
  @minimum -9_223_372_036_854_775_808
  @maximum 18_446_744_073_709_551_615

  def encode(term, options \\ []) do
    with {:ok, limits} <- limits(options),
         {:ok, _left} <- portable(term, 0, limits.max_items, limits),
         true <- :erlang.external_size(term) <= limits.max_bytes do
      {:ok, :erlang.term_to_binary(term)}
    else
      false -> {:error, :byte_limit}
      {:error, _} = error -> error
    end
  end

  def decode(binary, options \\ []) do
    with {:ok, limits} <- limits(options),
         true <- is_binary(binary) and byte_size(binary) <= limits.max_bytes,
         <<131, bytes::binary>> <- binary,
         {:ok, <<>>, _left} <- scan(bytes, 0, limits.max_items, limits),
         {term, used} <- :erlang.binary_to_term(binary, [:safe, :used]),
         true <- used == byte_size(binary),
         {:ok, _left} <- portable(term, 0, limits.max_items, limits) do
      {:ok, term}
    else
      {:error, _} = error -> error
      _ -> {:error, :invalid_checkpoint_term}
    end
  rescue
    ArgumentError -> {:error, :invalid_checkpoint_term}
  end

  defp limits(options) when is_list(options) do
    if Keyword.keyword?(options) and
         Enum.all?(Keyword.keys(options), &(&1 in [:max_bytes, :max_items, :max_depth])) do
      limits = %{
        max_bytes: Keyword.get(options, :max_bytes, 8_388_608),
        max_items: Keyword.get(options, :max_items, 100_000),
        max_depth: Keyword.get(options, :max_depth, 64)
      }

      if Enum.all?(Map.values(limits), &(is_integer(&1) and &1 > 0)),
        do: {:ok, limits},
        else: {:error, :invalid_limits}
    else
      {:error, :invalid_limits}
    end
  end

  defp limits(_), do: {:error, :invalid_limits}

  defp portable(_, depth, _, %{max_depth: maximum}) when depth > maximum,
    do: {:error, :depth_limit}

  defp portable(_, _, left, _) when left <= 0, do: {:error, :item_limit}
  defp portable(term, _, left, _) when is_atom(term) or is_binary(term), do: {:ok, left - 1}

  defp portable(term, _, left, _) when is_integer(term) and term >= @minimum and term <= @maximum,
    do: {:ok, left - 1}

  defp portable(term, _, left, _) when is_tuple(term) and tuple_size(term) > left - 1,
    do: {:error, :item_limit}

  defp portable(term, depth, left, limits) when is_tuple(term),
    do: portable_items(Tuple.to_list(term), depth + 1, left - 1, limits)

  defp portable(term, _, left, _) when is_map(term) and map_size(term) * 2 > left - 1,
    do: {:error, :item_limit}

  defp portable(term, depth, left, limits) when is_map(term) do
    Enum.reduce_while(:maps.to_list(term), {:ok, left - 1}, fn {key, value}, {:ok, remaining} ->
      with {:ok, remaining} <- portable(key, depth + 1, remaining, limits),
           {:ok, remaining} <- portable(value, depth + 1, remaining, limits) do
        {:cont, {:ok, remaining}}
      else
        error -> {:halt, error}
      end
    end)
  end

  defp portable(term, depth, left, limits) when is_list(term),
    do: portable_items(term, depth + 1, left - 1, limits)

  defp portable(_, _, _, _), do: {:error, :unsupported_term}

  defp portable_items([], _, left, _), do: {:ok, left}

  defp portable_items([item | rest], depth, left, limits) do
    with {:ok, left} <- portable(item, depth, left, limits),
         do: portable_items(rest, depth, left, limits)
  end

  defp portable_items(_, _, _, _), do: {:error, :improper_list}

  defp scan(_, depth, _, %{max_depth: maximum}) when depth > maximum,
    do: {:error, :depth_limit}

  defp scan(_, _, left, _) when left <= 0, do: {:error, :item_limit}
  defp scan(<<97, _integer, rest::binary>>, _, left, _), do: {:ok, rest, left - 1}
  defp scan(<<98, _integer::32, rest::binary>>, _, left, _), do: {:ok, rest, left - 1}
  defp scan(<<106, rest::binary>>, _, left, _), do: {:ok, rest, left - 1}

  defp scan(<<tag, size::16, rest::binary>>, _, left, _) when tag in [100, 118],
    do: skip(rest, size, left - 1)

  defp scan(<<tag, size, rest::binary>>, _, left, _) when tag in [115, 119],
    do: skip(rest, size, left - 1)

  defp scan(<<109, size::32, rest::binary>>, _, left, _), do: skip(rest, size, left - 1)

  defp scan(<<107, size::16, rest::binary>>, _, left, _) when size <= left - 1,
    do: skip(rest, size, left - 1 - size)

  defp scan(<<110, size, sign, rest::binary>>, _, left, _) when size <= 8 and sign in [0, 1],
    do: skip(rest, size, left - 1)

  defp scan(<<111, size::32, sign, rest::binary>>, _, left, _) when size <= 8 and sign in [0, 1],
    do: skip(rest, size, left - 1)

  defp scan(<<104, count, rest::binary>>, depth, left, limits),
    do: scan_items(rest, count, depth + 1, left - 1, limits)

  defp scan(<<105, count::32, rest::binary>>, depth, left, limits),
    do: scan_items(rest, count, depth + 1, left - 1, limits)

  defp scan(<<116, count::32, rest::binary>>, depth, left, limits),
    do: scan_items(rest, count * 2, depth + 1, left - 1, limits)

  defp scan(<<108, count::32, rest::binary>>, depth, left, limits) do
    with {:ok, <<106, rest::binary>>, left} <-
           scan_items(rest, count, depth + 1, left - 1, limits),
         do: {:ok, rest, left}
  end

  defp scan(_, _, _, _), do: {:error, :invalid_checkpoint_term}

  defp skip(bytes, size, left) when size <= byte_size(bytes) do
    <<_::binary-size(size), rest::binary>> = bytes
    {:ok, rest, left}
  end

  defp skip(_, _, _), do: {:error, :invalid_checkpoint_term}

  defp scan_items(_, count, _, left, _) when count > left, do: {:error, :item_limit}
  defp scan_items(bytes, 0, _, left, _), do: {:ok, bytes, left}

  defp scan_items(bytes, count, depth, left, limits) do
    with {:ok, rest, left} <- scan(bytes, depth, left, limits),
         do: scan_items(rest, count - 1, depth, left, limits)
  end
end
