defmodule GravitonMQ.AMQP10.Codec.ProtocolHeaderTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.AMQP10.Codec.Error
  alias GravitonMQ.AMQP10.Codec.ProtocolHeader

  test "recognizes the AMQP 1.0 protocol header and preserves its suffix" do
    suffix = <<1, 2, 3>>

    assert {:ok, %ProtocolHeader{protocol_id: 0, major: 1, minor: 0, revision: 0}, ^suffix} =
             ProtocolHeader.recognize(<<"AMQP", 0, 1, 0, 0, suffix::binary>>)
  end

  test "requests precisely the remaining bytes for every valid strict prefix" do
    header = <<"AMQP", 0, 1, 0, 0>>

    for length <- 0..7 do
      assert {:more, remaining} = ProtocolHeader.recognize(binary_part(header, 0, length))
      assert remaining == 8 - length
    end
  end

  test "rejects invalid protocol magic" do
    assert {:error,
            %Error{
              operation: :protocol_header,
              class: :malformed,
              reason: :invalid_protocol_magic,
              offset: 0,
              details: %{}
            }} = ProtocolHeader.recognize(<<"AMQX">>)
  end

  test "rejects SASL protocol headers without negotiating SASL" do
    assert {:error,
            %Error{
              operation: :protocol_header,
              class: :unsupported,
              reason: :sasl_not_supported,
              offset: 4,
              details: %{protocol_id: 3}
            }} = ProtocolHeader.recognize(<<"AMQP", 3, 1, 0, 0>>)
  end

  test "rejects unsupported protocol identifiers" do
    assert {:error,
            %Error{
              operation: :protocol_header,
              class: :unsupported,
              reason: :unsupported_protocol_id,
              offset: 4,
              details: %{protocol_id: 1}
            }} = ProtocolHeader.recognize(<<"AMQP", 1, 1, 0, 0>>)
  end

  test "rejects unsupported AMQP versions" do
    assert {:error,
            %Error{
              operation: :protocol_header,
              class: :unsupported,
              reason: :unsupported_version,
              offset: 5,
              details: %{protocol_id: 0, major: 1, minor: 0, revision: 1}
            }} = ProtocolHeader.recognize(<<"AMQP", 0, 1, 0, 1>>)
  end
end
