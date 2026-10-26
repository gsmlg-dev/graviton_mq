defmodule GravitonMQ.MQTT.Codec.Frame do
  @moduledoc """
  Incremental MQTT packet-envelope codec with opaque bodies.

  This module never invokes packet-schema decoding. Remaining Length is
  limited to four bytes; canonical MQTT 5 lengths are checked by its packet
  facade after the protocol version is known.
  """

  import Bitwise

  alias GravitonMQ.MQTT.Codec.{Error, Limits, Wire}
  alias GravitonMQ.MQTT.Frame

  @spec decode(binary(), Limits.t()) ::
          {:ok, Frame.t(), binary()} | {:more, pos_integer()} | {:error, Error.t()}
  def decode(bytes, limits \\ Limits.default()) do
    with {:ok, limits} <- Limits.validate(limits, :frame_decode) do
      decode_header(bytes, limits)
    end
  end

  defp decode_header(<<>>, _limits), do: {:more, 1}

  defp decode_header(<<type::4, flags::4, bytes::binary>>, limits) do
    with :ok <- validate_header(type, flags, :frame_decode),
         {:ok, size, data, width} <- variable_length(bytes) do
      total_size = 1 + width + size

      cond do
        total_size > limits.max_packet_size ->
          {:error,
           Error.new(:frame_decode, :limit_exceeded, :packet_too_large, %{size: total_size})}

        byte_size(data) < size ->
          {:more, size - byte_size(data)}

        true ->
          <<body::binary-size(size), rest::binary>> = data
          {:ok, %Frame{type: type, flags: flags, body: body, header_size: 1 + width}, rest}
      end
    end
  end

  defp decode_header(_bytes, _limits),
    do: {:error, Error.new(:frame_decode, :invalid_value, :not_binary)}

  defp variable_length(bytes) do
    case Wire.decode_vbi(bytes, false) do
      {:error, reason} -> {:error, Error.new(:frame_decode, :malformed, reason)}
      result -> result
    end
  end

  @spec encode(Frame.t(), Limits.t()) :: {:ok, binary()} | {:error, Error.t()}
  def encode(frame, limits \\ Limits.default()) do
    with {:ok, limits} <- Limits.validate(limits, :frame_encode) do
      encode_frame(frame, limits)
    end
  end

  defp encode_frame(%Frame{type: type, flags: flags, body: body}, limits)
       when is_binary(body) do
    size = byte_size(body)

    with :ok <- validate_header(type, flags, :frame_encode) do
      if size > 268_435_455 or 1 + Wire.vbi_size(size) + size > limits.max_packet_size do
        {:error, Error.new(:frame_encode, :limit_exceeded, :packet_too_large)}
      else
        {:ok, <<type::4, flags::4, Wire.encode_vbi(size)::binary, body::binary>>}
      end
    end
  end

  defp encode_frame(_frame, _limits),
    do: {:error, Error.new(:frame_encode, :invalid_value, :invalid_frame)}

  defp validate_header(type, flags, operation) do
    valid? =
      is_integer(type) and type in 1..15 and is_integer(flags) and flags in 0..15 and
        cond do
          type == 3 -> (flags &&& 6) != 6 and ((flags &&& 6) != 0 or (flags &&& 8) == 0)
          type in [6, 8, 10] -> flags == 2
          true -> flags == 0
        end

    if valid? do
      :ok
    else
      class = if operation == :frame_decode, do: :malformed, else: :invalid_value
      {:error, Error.new(operation, class, :invalid_fixed_header)}
    end
  end
end
