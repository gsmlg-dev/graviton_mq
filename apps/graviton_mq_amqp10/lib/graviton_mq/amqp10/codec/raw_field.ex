defmodule GravitonMQ.AMQP10.Codec.RawField do
  @moduledoc false

  alias GravitonMQ.AMQP10.Codec.Error
  alias GravitonMQ.AMQP10.Codec.Limits
  alias GravitonMQ.AMQP10.Codec.Value

  # The caller has already decoded this complete described list with the same
  # limits. Walk only to the requested field, retaining its actual constructor
  # bytes instead of reconstructing a canonical encoding from semantic values.
  @spec composite_field(binary(), non_neg_integer(), Limits.t()) ::
          {:ok, binary() | nil} | {:error, Error.t()}
  def composite_field(<<0, rest::binary>>, index, limits)
      when is_integer(index) and index >= 0 do
    with {:ok, _descriptor, list, budget} <-
           Value.decode_bounded(rest, limits, limits.max_zero_width_elements),
         {:ok, count, fields} <- list_body(list),
         {:ok, field} <- find_field(fields, count, index, limits, budget) do
      {:ok, field}
    else
      {:error, %Error{}} = error -> error
      _ -> invalid_composite()
    end
  end

  def composite_field(_wire, _index, _limits), do: invalid_composite()

  defp list_body(<<0x45, _::binary>>), do: {:ok, 0, <<>>}

  defp list_body(<<0xC0, size, count, rest::binary>>) when size >= 1 do
    case rest do
      <<fields::binary-size(size - 1), _::binary>> -> {:ok, count, fields}
      _ -> invalid_composite()
    end
  end

  defp list_body(<<0xD0, size::32, count::32, rest::binary>>) when size >= 4 do
    case rest do
      <<fields::binary-size(size - 4), _::binary>> -> {:ok, count, fields}
      _ -> invalid_composite()
    end
  end

  defp list_body(_wire), do: invalid_composite()

  # Every walked field shares the descriptor's zero-width element budget.
  defp find_field(_fields, count, index, _limits, _budget) when index >= count, do: {:ok, nil}

  defp find_field(fields, _count, 0, limits, budget) do
    case Value.decode_bounded(fields, limits, budget) do
      {:ok, _value, rest, _budget} ->
        {:ok, binary_part(fields, 0, byte_size(fields) - byte_size(rest))}

      {:error, %Error{}} = error ->
        error

      _ ->
        invalid_composite()
    end
  end

  defp find_field(fields, count, index, limits, budget) do
    case Value.decode_bounded(fields, limits, budget) do
      {:ok, _value, rest, budget} -> find_field(rest, count - 1, index - 1, limits, budget)
      {:error, %Error{}} = error -> error
      _ -> invalid_composite()
    end
  end

  defp invalid_composite do
    {:error, Error.new(:value_decode, :malformed, :invalid_composite)}
  end
end
