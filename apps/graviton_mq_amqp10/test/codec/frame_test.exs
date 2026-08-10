defmodule GravitonMQ.AMQP10.Codec.FrameTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.AMQP10.Codec.Error
  alias GravitonMQ.AMQP10.Codec.Frame
  alias GravitonMQ.AMQP10.Codec.Limits
  alias GravitonMQ.AMQP10.Frame, as: AMQPFrame

  test "decodes a heartbeat and requests exactly the missing header bytes" do
    heartbeat = <<0, 0, 0, 8, 2, 0, 0, 0>>

    for length <- 0..7 do
      assert {:more, remaining} = Frame.decode(binary_part(heartbeat, 0, length))
      assert remaining == 8 - length
    end

    assert {:ok,
            %AMQPFrame{
              declared_size: 8,
              data_offset_words: 2,
              type: :amqp,
              channel: 0,
              extended_header: <<>>,
              body: <<>>
            }, <<>>} = Frame.decode(heartbeat)
  end

  test "preserves the frame envelope and suffix exactly" do
    extended_header = <<1, 2, 3, 4>>
    body = <<9, 8, 7, 6>>
    suffix = <<5, 4>>
    frame = <<16::32, 3, 0, 7::16, extended_header::binary, body::binary, suffix::binary>>

    assert {:ok,
            %AMQPFrame{
              declared_size: 16,
              data_offset_words: 3,
              type: :amqp,
              channel: 7,
              extended_header: ^extended_header,
              body: ^body
            }, ^suffix} = Frame.decode(frame)
  end

  test "rejects malformed fixed-header values in validation order" do
    assert_frame_error(<<0::32, 2, 0, 0::16>>, :malformed, :frame_too_small, 0)
    assert_frame_error(<<8::32, 1, 0, 0::16>>, :malformed, :data_offset_too_small, 4)
    assert_frame_error(<<8::32, 3, 0, 0::16>>, :malformed, :data_offset_beyond_frame, 4)
  end

  test "rejects unsupported frame types after size and data-offset validation" do
    assert_frame_error(<<8::32, 2, 1, 0::16>>, :unsupported, :sasl_frame_not_supported, 5)
    assert_frame_error(<<8::32, 2, 2, 0::16>>, :unsupported, :unsupported_frame_type, 5)
  end

  test "enforces the frame size limit before requesting a declared body" do
    limits = %{Limits.default() | max_frame_size: 8}

    assert_frame_error(
      <<16::32, 2, 0, 0::16>>,
      :limit_exceeded,
      :frame_size_limit,
      0,
      limits
    )
  end

  test "returns invalid limits as a structured frame decode error" do
    limits = %{Limits.default() | max_frame_size: 0}

    assert {:error,
            %Error{
              operation: :frame_decode,
              class: :invalid_value,
              reason: :invalid_limits,
              offset: nil,
              details: details
            }} = Frame.decode(<<>>, limits)

    assert details == Map.from_struct(limits)
  end

  defp assert_frame_error(input, class, reason, offset, limits \\ Limits.default()) do
    assert {:error,
            %Error{
              operation: :frame_decode,
              class: ^class,
              reason: ^reason,
              offset: ^offset,
              details: %{}
            }} = Frame.decode(input, limits)
  end
end
