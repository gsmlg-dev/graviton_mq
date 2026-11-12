defmodule GravitonMQ.AMQP10.Codec.Frame do
  @moduledoc """
  Encodes and decodes AMQP 1.0 and SASL frame envelopes without interpreting their bodies.

  The codec is process-free and preserves the extended header and opaque body
  exactly for later protocol layers. Encoding validates the supplied size and
  data offset against the actual bytes before allocating the wire envelope.

  The `types:` option selects the expected layer: `[:amqp]` (default) or
  `[:sasl]`. A SASL frame (type `0x01`, Part 5 §5.3.1) is bounded by
  MIN-MAX-FRAME-SIZE (512 octets) in both directions regardless of
  `Limits.max_frame_size`. Its channel bytes and extended header are ignored
  data on decode; encoding requires channel 0 and data offset 2.
  """

  alias GravitonMQ.AMQP10.Codec
  alias GravitonMQ.AMQP10.Codec.Error
  alias GravitonMQ.AMQP10.Codec.Limits
  alias GravitonMQ.AMQP10.Frame, as: AMQPFrame

  @fixed_header_size 8
  @max_wire_frame_size 0xFFFF_FFFF
  @max_data_offset_words 255
  @sasl_max_frame_size 512

  @spec decode(binary()) :: Codec.decode_result(AMQPFrame.t())
  @spec decode(binary(), Limits.t()) :: Codec.decode_result(AMQPFrame.t())
  @spec decode(binary(), Limits.t(), keyword()) :: Codec.decode_result(AMQPFrame.t())
  def decode(input, limits \\ Limits.default(), options \\ []) when is_binary(input) do
    with {:ok, limits} <- Limits.validate(limits, :frame_decode),
         {:ok, type} <- frame_type(options, :frame_decode) do
      decode_frame(input, limits, type)
    end
  end

  @spec encode(AMQPFrame.t()) :: Codec.encode_result()
  @spec encode(AMQPFrame.t(), Limits.t()) :: Codec.encode_result()
  @spec encode(AMQPFrame.t(), Limits.t(), keyword()) :: Codec.encode_result()
  def encode(frame, limits \\ Limits.default(), options \\ []) do
    with {:ok, limits} <- Limits.validate(limits, :frame_encode),
         {:ok, type} <- frame_type(options, :frame_encode) do
      encode_frame(frame, limits, type)
    end
  end

  defp frame_type(options, operation) do
    case Keyword.keyword?(options) && Keyword.get(options, :types, [:amqp]) do
      [:amqp] -> {:ok, :amqp}
      [:sasl] -> {:ok, :sasl}
      _other -> {:error, Error.new(operation, :invalid_value, :invalid_frame_types)}
    end
  end

  defp encode_frame(
         %AMQPFrame{
           declared_size: declared_size,
           data_offset_words: data_offset_words,
           type: _,
           channel: channel,
           extended_header: extended_header,
           body: body
         } = frame,
         limits,
         type
       ) do
    cond do
      not integer_between?(declared_size, @fixed_header_size, @max_wire_frame_size) ->
        encode_error(:invalid_value, :invalid_declared_size)

      not integer_between?(data_offset_words, 2, @max_data_offset_words) ->
        encode_error(:invalid_value, :invalid_data_offset)

      not integer_between?(channel, 0, 65_535) ->
        encode_error(:invalid_value, :invalid_channel)

      not is_binary(extended_header) ->
        encode_error(:invalid_value, :invalid_extended_header)

      not is_binary(body) ->
        encode_error(:invalid_value, :invalid_body)

      true ->
        encode_envelope(frame, limits, type)
    end
  end

  defp encode_frame(_frame, _limits, _type), do: encode_error(:invalid_value, :invalid_frame)

  defp encode_envelope(frame, limits, type) do
    header_size = @fixed_header_size + byte_size(frame.extended_header)
    size = header_size + byte_size(frame.body)

    cond do
      rem(header_size, 4) != 0 ->
        encode_error(:invalid_value, :unaligned_extended_header)

      div(header_size, 4) > @max_data_offset_words ->
        encode_error(:limit_exceeded, :data_offset_limit)

      frame.data_offset_words * 4 != header_size ->
        encode_error(:invalid_value, :data_offset_mismatch)

      size > @max_wire_frame_size or (type == :amqp and size > limits.max_frame_size) ->
        encode_error(:limit_exceeded, :frame_size_limit)

      frame.declared_size != size ->
        encode_error(:invalid_value, :declared_size_mismatch)

      frame.type == :sasl and type == :amqp ->
        encode_error(:unsupported, :sasl_frame_not_supported)

      frame.type != type ->
        encode_error(:invalid_value, :invalid_frame_type)

      type == :sasl and (frame.channel != 0 or frame.data_offset_words != 2) ->
        encode_error(:invalid_value, :invalid_sasl_envelope)

      type == :sasl and size > @sasl_max_frame_size ->
        encode_error(:limit_exceeded, :frame_size_limit)

      true ->
        {:ok,
         <<size::32, frame.data_offset_words, type_code(type), frame.channel::16,
           frame.extended_header::binary, frame.body::binary>>}
    end
  end

  defp type_code(:amqp), do: 0
  defp type_code(:sasl), do: 1

  defp integer_between?(value, minimum, maximum),
    do: is_integer(value) and value >= minimum and value <= maximum

  defp encode_error(class, reason), do: {:error, Error.new(:frame_encode, class, reason)}

  defp decode_frame(input, _limits, _type) when byte_size(input) < @fixed_header_size do
    {:more, @fixed_header_size - byte_size(input)}
  end

  defp decode_frame(
         <<size::32, data_offset_words, type_code, channel::16, _rest::binary>> = input,
         limits,
         type
       ) do
    with :ok <- validate_size(size),
         :ok <- validate_data_offset(data_offset_words),
         :ok <- validate_frame_boundary(size, data_offset_words),
         :ok <- validate_frame_limit(size, maximum_frame_size(type, limits)),
         {:ok, type} <- validate_type(type_code, type) do
      decode_complete_frame(input, size, data_offset_words, channel, type)
    end
  end

  defp maximum_frame_size(:sasl, _limits), do: @sasl_max_frame_size
  defp maximum_frame_size(:amqp, limits), do: limits.max_frame_size

  defp decode_complete_frame(input, size, _data_offset_words, _channel, _type)
       when byte_size(input) < size do
    {:more, size - byte_size(input)}
  end

  defp decode_complete_frame(input, size, data_offset_words, channel, type) do
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
       type: type,
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

  defp validate_frame_limit(size, maximum) when size <= maximum, do: :ok

  defp validate_frame_limit(_size, _maximum) do
    {:error, Error.new(:frame_decode, :limit_exceeded, :frame_size_limit, offset: 0)}
  end

  defp validate_type(0, :amqp), do: {:ok, :amqp}
  defp validate_type(1, :sasl), do: {:ok, :sasl}

  defp validate_type(1, :amqp) do
    {:error, Error.new(:frame_decode, :unsupported, :sasl_frame_not_supported, offset: 5)}
  end

  defp validate_type(0, :sasl) do
    {:error, Error.new(:frame_decode, :malformed, :amqp_frame_in_sasl_layer, offset: 5)}
  end

  defp validate_type(_type_code, _type) do
    {:error, Error.new(:frame_decode, :unsupported, :unsupported_frame_type, offset: 5)}
  end
end
