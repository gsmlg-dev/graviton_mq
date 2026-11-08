defmodule GravitonMQ.AMQP10.AnnotationPolicy do
  @moduledoc """
  Bounded annotation-understanding policy for received opaque messages.

  Only annotation keys and the standard delivery annotation `rejected` are
  interpreted. Other values and body sections are skipped using their wire
  lengths, including scalar and array types outside the semantic value codec.
  No bytes are rewritten. This check does not claim to validate a complete
  message: unrelated raw protocol fixtures remain opaque.
  """

  alias GravitonMQ.AMQP10.Codec.{DeliveryState, Limits}
  alias GravitonMQ.AMQP10.Codec.Value, as: WireValue
  alias GravitonMQ.AMQP10.Value

  @annotation_sections [
    0x71,
    0x72,
    0x78,
    "amqp:delivery-annotations:map",
    "amqp:message-annotations:map",
    "amqp:footer:map"
  ]
  @delivery_annotations [0x71, "amqp:delivery-annotations:map"]

  @spec check(binary(), Limits.t()) ::
          :ok | {:error, :not_implemented | :malformed_annotations | :annotation_limit}
  def check(raw, %Limits{} = limits) when is_binary(raw) do
    with {:ok, limits} <- Limits.validate(limits, :value_decode),
         true <- byte_size(raw) <= limits.max_value_bytes do
      sections(raw, limits, 0, limits.max_zero_width_elements)
    else
      _ -> {:error, :annotation_limit}
    end
  end

  def check(_, _), do: {:error, :malformed_annotations}

  defp sections(<<>>, _limits, _count, _budget), do: :ok

  defp sections(_raw, %{max_compound_items: maximum}, count, _budget) when count >= maximum,
    do: {:error, :annotation_limit}

  # One zero-width element budget covers every value decoded from `raw`.
  defp sections(<<0, raw::binary>>, limits, count, budget) do
    case WireValue.decode_bounded(raw, limits, budget) do
      {:ok, %Value{type: type, value: descriptor}, body, budget} when type in [:ulong, :symbol] ->
        result =
          if descriptor in @annotation_sections,
            do: annotations(body, descriptor, limits, budget),
            else: with({:ok, rest} <- skip(body, limits, 0), do: {:ok, rest, budget})

        with {:ok, rest, budget} <- result, do: sections(rest, limits, count + 1, budget)

      _ ->
        :ok
    end
  end

  defp sections(_opaque, _limits, _count, _budget), do: :ok

  defp annotations(<<0xC1, size, count, raw::binary>>, section, limits, budget) when size >= 1,
    do: annotation_map(raw, size - 1, count, section, limits, budget)

  defp annotations(<<0xD1, size::32, count::32, raw::binary>>, section, limits, budget)
       when size >= 4,
       do: annotation_map(raw, size - 4, count, section, limits, budget)

  defp annotations(_raw, _section, _limits, _budget), do: {:error, :malformed_annotations}

  defp annotation_map(_raw, _size, count, _section, limits, _budget)
       when count > limits.max_compound_items,
       do: {:error, :annotation_limit}

  defp annotation_map(raw, size, count, section, limits, budget)
       when rem(count, 2) == 0 and byte_size(raw) >= size do
    <<entries::binary-size(size), rest::binary>> = raw

    with {:ok, budget} <- entries(entries, count, section, limits, budget),
         do: {:ok, rest, budget}
  end

  defp annotation_map(_raw, _size, _count, _section, _limits, _budget),
    do: {:error, :malformed_annotations}

  defp entries(<<>>, 0, _section, _limits, budget), do: {:ok, budget}
  defp entries(_raw, 0, _section, _limits, _budget), do: {:error, :malformed_annotations}

  defp entries(raw, count, section, limits, budget) do
    case WireValue.decode_bounded(raw, limits, budget) do
      {:ok, %Value{type: :symbol, value: key}, value, budget} ->
        cond do
          String.starts_with?(key, "x-opt") ->
            with {:ok, rest} <- skip(value, limits, 0),
                 do: entries(rest, count - 2, section, limits, budget)

          key == "rejected" and section in @delivery_annotations ->
            with {:ok, decoded, rest, budget} <- WireValue.decode_bounded(value, limits, budget),
                 {:ok, %GravitonMQ.AMQP10.Error{}} <-
                   DeliveryState.decode_error_value(decoded, :value_decode, :malformed) do
              entries(rest, count - 2, section, limits, budget)
            else
              {:error, %{reason: :zero_width_element_limit}} -> {:error, :annotation_limit}
              _ -> {:error, :malformed_annotations}
            end

          true ->
            {:error, :not_implemented}
        end

      {:ok, %Value{type: :ulong}, _value, _budget} ->
        {:error, :not_implemented}

      {:error, %{reason: :zero_width_element_limit}} ->
        {:error, :annotation_limit}

      _ ->
        {:error, :malformed_annotations}
    end
  end

  # Container sizes include their count and constructor fields. Their contents
  # stay opaque; only described constructors require bounded recursive work.
  defp skip(<<0, raw::binary>>, limits, depth) when depth < limits.max_nesting_depth do
    with {:ok, value} <- skip(raw, limits, depth + 1),
         {:ok, rest} <- skip(value, limits, depth + 1),
         do: {:ok, rest}
  end

  defp skip(<<0, _::binary>>, _limits, _depth), do: {:error, :annotation_limit}

  defp skip(<<code, rest::binary>>, _limits, _depth) when code in 0x40..0x45,
    do: {:ok, rest}

  defp skip(<<code, rest::binary>>, limits, _depth) when code in 0x50..0x56,
    do: cut(rest, 1, limits)

  defp skip(<<code, rest::binary>>, limits, _depth) when code in [0x60, 0x61],
    do: cut(rest, 2, limits)

  defp skip(<<code, rest::binary>>, limits, _depth) when code in 0x70..0x74,
    do: cut(rest, 4, limits)

  defp skip(<<code, rest::binary>>, limits, _depth) when code in 0x80..0x84,
    do: cut(rest, 8, limits)

  defp skip(<<code, rest::binary>>, limits, _depth) when code in [0x94, 0x98],
    do: cut(rest, 16, limits)

  defp skip(<<code, size, rest::binary>>, limits, _depth) when code in [0xA0, 0xA1, 0xA3],
    do: cut(rest, size, limits)

  defp skip(<<code, size::32, rest::binary>>, limits, _depth) when code in [0xB0, 0xB1, 0xB3],
    do: cut(rest, size, limits)

  defp skip(<<code, size, rest::binary>>, limits, _depth)
       when code in [0xC0, 0xC1, 0xE0] and size >= 1,
       do: cut(rest, size, limits)

  defp skip(<<code, size::32, rest::binary>>, limits, _depth)
       when code in [0xD0, 0xD1, 0xF0] and size >= 4,
       do: cut(rest, size, limits)

  defp skip(_raw, _limits, _depth), do: {:error, :malformed_annotations}

  defp cut(_raw, size, limits) when size > limits.max_value_bytes,
    do: {:error, :annotation_limit}

  defp cut(raw, size, _limits) when byte_size(raw) >= size do
    <<_value::binary-size(size), rest::binary>> = raw
    {:ok, rest}
  end

  defp cut(_raw, _size, _limits), do: {:error, :malformed_annotations}
end
