defmodule GravitonMQ.AMQP10.Codec.ValueNullKeySchemaTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.AMQP10.Codec.{Error, Performative}
  alias GravitonMQ.AMQP10.Error, as: ProtocolError
  alias GravitonMQ.AMQP10.Performative.{Begin, End, Open}
  alias GravitonMQ.AMQP10.Value

  test "Open, Begin, and nested Error still reject null keys in symbol-keyed schema maps" do
    null_key_map = Value.map([{Value.null(), Value.uint(0)}])

    packets = [
      %Open{container_id: Value.string("c"), properties: null_key_map},
      %Begin{
        next_outgoing_id: Value.uint(0),
        incoming_window: Value.uint(100),
        outgoing_window: Value.uint(100),
        properties: null_key_map
      },
      %End{error: %ProtocolError{condition: Value.symbol("x"), info: null_key_map}}
    ]

    for packet <- packets do
      assert {:error,
              %Error{
                operation: :performative_encode,
                class: :invalid_value,
                reason: :invalid_property_key
              }} = Performative.encode(packet)
    end

    fixtures = [
      <<0x00, 0x53, 0x10, 0xC0, 17, 10, 0xA1, 1, "c", 0x40, 0x40, 0x40, 0x40, 0x40, 0x40, 0x40,
        0x40, 0xC1, 3, 2, 0x40, 0x43>>,
      <<0x00, 0x53, 0x11, 0xC0, 15, 8, 0x40, 0x43, 0x52, 100, 0x52, 100, 0x40, 0x40, 0x40, 0xC1,
        3, 2, 0x40, 0x43>>,
      <<0x00, 0x53, 0x17, 0xC0, 16, 1, 0x00, 0x53, 0x1D, 0xC0, 10, 3, 0xA3, 1, "x", 0x40, 0xC1, 3,
        2, 0x40, 0x43>>
    ]

    for fixture <- fixtures do
      assert {:error,
              %Error{
                operation: :performative_decode,
                class: :malformed,
                reason: :invalid_property_key
              }} = Performative.decode(fixture)
    end
  end
end
