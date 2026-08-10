defmodule GravitonMQ.AMQP10.Codec.Frame do
  @moduledoc """
  Decodes complete AMQP 1.0 frame envelopes without interpreting their bodies.

  The decoder is process-free and preserves the extended header and opaque body
  exactly for later protocol layers.
  """

  alias GravitonMQ.AMQP10.Codec
  alias GravitonMQ.AMQP10.Codec.Error
  alias GravitonMQ.AMQP10.Codec.Limits
  alias GravitonMQ.AMQP10.Frame, as: AMQPFrame

  @fixed_header_size 8

  @spec decode(binary()) :: Codec.decode_result(AMQPFrame.t())
  @spec decode(binary(), Limits.t()) :: Codec.decode_result(AMQPFrame.t())
  def decode(input, limits \\ Limits.default()) when is_binary(input) do
    with {:ok, limits} <- Limits.validate(limits, :frame_decode) do
      decode_frame(input, limits)
    end
  end

  defp decode_frame(input, _limits) when byte_size(input) < @fixed_header_size do
    {:more, @fixed_header_size - byte_size(input)}
  end

  defp decode_frame(
         <<size::32, data_offset_words, type, channel::16, _rest::binary>> = input,
         limits
       ) do
    with :ok <- validate_size(size),
         :ok <- validate_data_offset(data_offset_words),
         :ok <- validate_frame_boundary(size, data_offset_words),
         :ok <- validate_frame_limit(size, limits),
         :ok <- validate_type(type) do
      decode_complete_frame(input, size, data_offset_words, channel)
    else
      {:error, error} -> {:error, error}
    end
  end

  defp decode_complete_frame(input, size, _data_offset_words, _channel)
       when byte_size(input) < size do
    {:more, size - byte_size(input)}
  end

  defp decode_complete_frame(input, size, data_offset_words, channel) do
    data_offset = data_offset_words * 4
    extended_header_size = data_offset - @fixed_header_size
    body_size = size - data_offset

    <<_fixed_header::binary-size(@fixed_header_size),
      extended_header::binary-size(extended_header_size), body::binary-size(body_size),
      rest::binary>> = input

    {:ok,
     %AMQPFrame{
       declared_size: size,
       data_offset_words: data_offset_words,
       type: :amqp,
       channel: channel,
       extended_header: extended_header,
       body: body
     }, rest}
  end

  defp validate_size(size) when size >= @fixed_header_size, do: :ok

  defp validate_size(_size) do
    {:error, Error.new(:frame_decode, :malformed, :frame_too_small, offset: 0)}
  end

  defp validate_data_offset(data_offset_words) when data_offset_words >= 2, do: :ok

  defp validate_data_offset(_data_offset_words) do
    {:error, Error.new(:frame_decode, :malformed, :data_offset_too_small, offset: 4)}
  end

  defp validate_frame_boundary(size, data_offset_words) when data_offset_words * 4 <= size,
    do: :ok

  defp validate_frame_boundary(_size, _data_offset_words) do
    {:error, Error.new(:frame_decode, :malformed, :data_offset_beyond_frame, offset: 4)}
  end

  defp validate_frame_limit(size, %{max_frame_size: max_frame_size}) when size <= max_frame_size,
    do: :ok

  defp validate_frame_limit(_size, _limits) do
    {:error, Error.new(:frame_decode, :limit_exceeded, :frame_size_limit, offset: 0)}
  end

  defp validate_type(0), do: :ok

  defp validate_type(1) do
    {:error, Error.new(:frame_decode, :unsupported, :sasl_frame_not_supported, offset: 5)}
  end

  defp validate_type(_type) do
    {:error, Error.new(:frame_decode, :unsupported, :unsupported_frame_type, offset: 5)}
  end
end
