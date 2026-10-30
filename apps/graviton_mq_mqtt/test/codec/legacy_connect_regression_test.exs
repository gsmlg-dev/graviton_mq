defmodule GravitonMQ.MQTT.Codec.LegacyConnectRegressionTest do
  use ExUnit.Case, async: true
  alias GravitonMQ.MQTT.Codec.{Error, Packet}

  @connect <<0x10, 13, 0, 4, "MQTT", 4, 2, 0, 60, 0, 1, "c">>

  test "retains MQTT 3.1.1 CONNECT and CONNACK codec vectors" do
    assert {:ok, connect, <<>>} = Packet.decode(@connect)
    assert connect.version == :v3_1_1
    assert {:ok, @connect} = Packet.encode(connect)
    assert {:ok, ack, <<>>} = Packet.decode(<<0x20, 2, 0, 0>>, :v3_1_1)
    assert {:ok, <<0x20, 2, 0, 0>>} = Packet.encode(ack)
    assert {:error, %Error{reason: :version_mismatch}} = Packet.decode(@connect, :v5)
  end

  test "retains legacy flag, empty-id, property and reason-code restrictions" do
    for wire <- [
          <<0x10, 14, 0, 4, "MQTT", 4, 0x42, 0, 0, 0, 0, 0, 0>>,
          <<0x10, 12, 0, 4, "MQTT", 4, 0, 0, 0, 0, 0>>
        ] do
      assert {:error, %Error{class: :malformed}} = Packet.decode(wire)
    end

    assert {:ok, ack, <<>>} = Packet.decode(<<0x20, 2, 0, 5>>, :v3_1_1)
    assert {:error, %Error{reason: :invalid_reason_code}} = Packet.encode(%{ack | reason_code: 6})

    assert {:error, %Error{reason: :properties_not_supported}} =
             Packet.encode(%{ack | properties: [user_property: {"a", "b"}]})
  end

  test "legacy strict prefixes, suffixes, empty credentials and Will remain unchanged" do
    bodies = [
      <<0, 4, "MQTT", 4, 2, 0, 60, 0, 1, "c">>,
      <<0, 4, "MQTT", 4, 0xC2, 0, 0, 0, 1, "c", 0, 0, 0, 0>>,
      <<0, 4, "MQTT", 4, 0x2E, 0, 0, 0, 1, "c", 0, 1, "w", 0, 2, 0, 0xFF>>
    ]

    for body <- bodies do
      wire = <<0x10, byte_size(body), body::binary>>
      assert {:ok, packet, <<0xAA>>} = Packet.decode(wire <> <<0xAA>>)
      assert {:ok, ^wire} = Packet.encode(packet)

      for n <- 0..(byte_size(wire) - 1) do
        assert {:more, count} = Packet.decode(binary_part(wire, 0, n))
        assert count > 0
      end
    end
  end

  test "legacy CONNACK validates every reason code and Session Present" do
    for code <- 0..255 do
      result = Packet.decode(<<0x20, 2, 0, code>>, :v3_1_1)

      if code in 0..5 do
        assert {:ok, packet, <<>>} = result
        assert {:ok, <<0x20, 2, 0, ^code>>} = Packet.encode(packet)
      else
        assert {:error, %Error{reason: :invalid_reason_code}} = result
      end
    end

    assert {:ok, packet, <<>>} = Packet.decode(<<0x20, 2, 1, 0>>, :v3_1_1)

    assert {:error, %Error{reason: :invalid_session_present}} =
             Packet.encode(%{packet | reason_code: 1})

    assert {:error, %Error{reason: :invalid_session_present}} =
             Packet.decode(<<0x20, 2, 1, 1>>, :v3_1_1)
  end

  test "legacy CONNECT allows the historical nonminimal Remaining Length but MQTT5 rejects it" do
    <<header, size, body::binary>> = @connect
    assert {:ok, _, <<>>} = Packet.decode(<<header, size + 128, 0, body::binary>>)

    assert {:error, %Error{reason: :nonminimal_variable_integer}} =
             Packet.decode(<<0x10, 0x8E, 0, 0, 4, "MQTT", 5, 2, 0, 60, 0, 0, 1, "c">>)
  end
end
