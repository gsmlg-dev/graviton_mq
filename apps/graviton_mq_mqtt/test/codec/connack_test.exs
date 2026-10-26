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

  test "independent CONNACK fixtures decode and encode exactly with an explicit version" do
    for {version, bytes} <- [{:v3_1_1, <<0x20, 2, 0, 0>>}, {:v5, <<0x20, 3, 0, 0, 0>>}] do
      assert {:ok, packet, <<0xAA, 0xBB>>} = Packet.decode(bytes <> <<0xAA, 0xBB>>, version)
      assert %{version: ^version, session_present: false, reason_code: 0, properties: []} = packet
      assert {:ok, ^bytes} = Packet.encode(packet)

      for size <- 0..(byte_size(bytes) - 1) do
        assert {:more, required} = Packet.decode(binary_part(bytes, 0, size), version)
        assert required > 0
      end
    end
  end

  test "CONNACK cannot infer its protocol version" do
    assert {:error, %{class: :unsupported, reason: :version_required}} =
             Packet.decode(<<0x20, 2, 0, 0>>)
  end

  test "reason codes are validated against their exact version's table" do
    for code <- 0..255 do
      result311 = Packet.decode(<<0x20, 2, 0, code>>, :v3_1_1)
      result5 = Packet.decode(<<0x20, 3, 0, code, 0>>, :v5)

      if code in 0..5,
        do: assert({:ok, %{reason_code: ^code}, <<>>} = result311),
        else: assert({:error, %{class: :malformed, reason: :invalid_reason_code}} = result311)

      if code in @v5_codes,
        do: assert({:ok, %{reason_code: ^code}, <<>>} = result5),
        else: assert({:error, %{class: :malformed, reason: :invalid_reason_code}} = result5)
    end
  end

  test "Session Present is only valid on successful CONNACK" do
    for {version, bytes} <- [{:v3_1_1, <<0x20, 2, 1, 0>>}, {:v5, <<0x20, 3, 1, 0, 0>>}] do
      assert {:ok, %{session_present: true} = packet, <<>>} = Packet.decode(bytes, version)
      assert {:ok, ^bytes} = Packet.encode(packet)
      failure_code = if version == :v5, do: 0x80, else: 1

      assert {:error, %{class: :invalid_value, reason: :invalid_session_present}} =
               Packet.encode(%{packet | reason_code: failure_code})
    end

    assert {:error, %{class: :malformed, reason: :invalid_session_present}} =
             Packet.decode(<<0x20, 3, 1, 0x80, 0>>, :v5)
  end

  test "rejects reserved flags, missing properties, and extra fields in complete packets" do
    for {version, bytes} <- [
          {:v3_1_1, <<0x20, 2, 2, 0>>},
          {:v3_1_1, <<0x20, 3, 0, 0, 0>>},
          {:v5, <<0x20, 2, 0, 0>>},
          {:v5, <<0x20, 4, 0, 0, 0, 0>>}
        ] do
      assert {:error, %{class: :malformed}} = Packet.decode(bytes, version)
    end
  end
end
