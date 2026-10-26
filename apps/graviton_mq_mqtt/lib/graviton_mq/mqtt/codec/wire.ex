defmodule GravitonMQ.MQTT.Codec.Wire do
  @moduledoc false

  import Bitwise

  alias GravitonMQ.MQTT.Codec.Error

  def decode_vbi(bytes, canonical? \\ true), do: decode_vbi(bytes, 0, 0, canonical?)

  defp decode_vbi(<<>>, _value, _width, _canonical?), do: {:more, 1}

  defp decode_vbi(<<octet, rest::binary>>, value, width, canonical?) do
    value = value + ((octet &&& 0x7F) <<< (width * 7))
    width = width + 1

    cond do
      (octet &&& 0x80) != 0 and width == 4 ->
        {:error, :invalid_variable_integer}

      (octet &&& 0x80) != 0 ->
        decode_vbi(rest, value, width, canonical?)

      canonical? and width != vbi_size(value) ->
        {:error, :nonminimal_variable_integer}

      true ->
        {:ok, value, rest, width}
    end
  end

  def vbi_size(value) when value < 128, do: 1
  def vbi_size(value) when value < 16_384, do: 2
  def vbi_size(value) when value < 2_097_152, do: 3
  def vbi_size(_value), do: 4

  def encode_vbi(value) when value in 0..127, do: <<value>>

  def encode_vbi(value) when value in 128..268_435_455,
    do: <<(value &&& 0x7F) ||| 0x80, encode_vbi(value >>> 7)::binary>>

  def decode(:u8, <<value, rest::binary>>, _context, _field), do: {:ok, value, rest}
  def decode(:u16, <<value::16, rest::binary>>, _context, _field), do: {:ok, value, rest}
  def decode(:u32, <<value::32, rest::binary>>, _context, _field), do: {:ok, value, rest}

  def decode(type, <<size::16, data::binary>>, context, field)
      when type in [:utf8, :binary] do
    cond do
      size > context.limits.max_field_bytes ->
        error(context, :limit_exceeded, :field_too_large, %{field: field, size: size})

      byte_size(data) < size ->
        error(context, :malformed, :truncated_field, %{field: field})

      true ->
        <<value::binary-size(size), rest::binary>> = data

        if type == :utf8 and not valid_utf8?(value),
          do: error(context, :malformed, :invalid_utf8, %{field: field}),
          else: {:ok, value, rest}
    end
  end

  def decode(_type, _bytes, context, field),
    do: error(context, :malformed, :truncated_field, %{field: field})

  def encode(:u8, value, _context, _field) when is_integer(value) and value in 0..255,
    do: {:ok, <<value>>}

  def encode(:u16, value, _context, _field) when is_integer(value) and value in 0..65_535,
    do: {:ok, <<value::16>>}

  def encode(:u32, value, _context, _field) when is_integer(value) and value in 0..0xFFFF_FFFF,
    do: {:ok, <<value::32>>}

  def encode(type, value, context, field) when type in [:utf8, :binary] and is_binary(value) do
    cond do
      byte_size(value) > 65_535 ->
        error(context, :invalid_value, :field_length_out_of_range, %{field: field})

      byte_size(value) > context.limits.max_field_bytes ->
        error(context, :limit_exceeded, :field_too_large, %{field: field, size: byte_size(value)})

      type == :utf8 and not valid_utf8?(value) ->
        error(context, :invalid_value, :invalid_utf8, %{field: field})

      true ->
        {:ok, <<byte_size(value)::16, value::binary>>}
    end
  end

  def encode(_type, _value, context, field),
    do: error(context, :invalid_value, :invalid_field, %{field: field})

  def valid_utf8?(value), do: String.valid?(value) and :binary.match(value, <<0>>) == :nomatch

  def topic_name?(value),
    do: value != <<>> and :binary.match(value, ["+", "#"]) == :nomatch

  def value_class(%{operation: :packet_decode}), do: :malformed
  def value_class(_context), do: :invalid_value

  def error(context, class, reason, details \\ %{}),
    do: {:error, Error.new(context.operation, class, reason, details)}
end
