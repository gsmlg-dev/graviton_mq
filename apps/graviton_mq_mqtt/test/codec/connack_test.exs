defmodule GravitonMQ.MQTT.Codec.ConnackTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.MQTT.Codec.Packet

  @v5_codes [
    0,
    0x80,
    0x81,
    0x82,
    0x83,
    0x84,
    0x85,
    0x86,
    0x87,
    0x88,
    0x89,
    0x8A,
    0x8C,
    0x90,
    0x95,
    0x97,
    0x99,
    0x9A,
    0x9B,
    0x9C,
    0x9D,
    0x9F
  ]

  test "independent MQTT 5 CONNACK fixture decodes and encodes exactly" do
    bytes = <<0x20, 3, 0, 0, 0>>
    assert {:ok, packet, <<0xAA, 0xBB>>} = Packet.decode(bytes <> <<0xAA, 0xBB>>, :v5)
    assert %{version: :v5, session_present: false, reason_code: 0, properties: []} = packet
    assert {:ok, ^bytes} = Packet.encode(packet)

    for size <- 0..(byte_size(bytes) - 1) do
      assert {:more, required} = Packet.decode(binary_part(bytes, 0, size), :v5)
      assert required > 0
    end
  end

  test "CONNACK cannot infer its protocol version" do
    assert {:error, %{class: :unsupported, reason: :version_required}} =
             Packet.decode(<<0x20, 2, 0, 0>>)
  end

  test "reason codes are validated against the MQTT 5 table" do
    for code <- 0..255 do
      result5 = Packet.decode(<<0x20, 3, 0, code, 0>>, :v5)

      if code in @v5_codes,
        do: assert({:ok, %{reason_code: ^code}, <<>>} = result5),
        else: assert({:error, %{class: :malformed, reason: :invalid_reason_code}} = result5)
    end
  end

  test "Session Present is only valid on successful CONNACK" do
    bytes = <<0x20, 3, 1, 0, 0>>
    assert {:ok, %{session_present: true} = packet, <<>>} = Packet.decode(bytes, :v5)
    assert {:ok, ^bytes} = Packet.encode(packet)

    assert {:error, %{class: :invalid_value, reason: :invalid_session_present}} =
             Packet.encode(%{packet | reason_code: 0x80})

    assert {:error, %{class: :malformed, reason: :invalid_session_present}} =
             Packet.decode(<<0x20, 3, 1, 0x80, 0>>, :v5)
  end

  test "rejects reserved flags, missing properties, and extra fields in complete packets" do
    for bytes <- [
          <<0x20, 3, 2, 0, 0>>,
          <<0x20, 2, 0, 0>>,
          <<0x20, 4, 0, 0, 0, 0>>
        ] do
      assert {:error, %{class: :malformed}} = Packet.decode(bytes, :v5)
    end
  end
end
