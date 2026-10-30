defmodule GravitonMQ.MQTT.Codec.VersionBoundaryTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.MQTT.Codec.{Error, Frame, Packet}

  @connect5 <<0x10, 14, 0, 4, "MQTT", 5, 2, 0, 60, 0, 0, 1, "c">>

  test "MQTT 3.1 CONNECT packets remain explicitly unsupported" do
    for bytes <- [
          <<0x10, 15, 0, 6, "MQIsdp", 3, 2, 0, 60, 0, 1, "c">>
        ],
        version <- [:auto, :v5] do
      assert {:error, %Error{operation: :packet_decode, class: :unsupported}} =
               Packet.decode(bytes, version)
    end
  end

  test "CONNECT rejects every protocol level except 4 and 5" do
    for level <- 0..255, level not in [4, 5] do
      bytes = <<0x10, 14, 0, 4, "MQTT", level, 2, 0, 60, 0, 0, 1, "c">>

      assert {:error, %Error{class: :unsupported, reason: :unsupported_version}} =
               Packet.decode(bytes)
    end
  end

  test "unsupported version selectors cannot enable decoding or encoding" do
    assert {:ok, connect, <<>>} = Packet.decode(@connect5)
    assert {:ok, connack, <<>>} = Packet.decode(<<0x20, 3, 0, 0, 0>>, :v5)

    for version <- [:v3_1, :mqtt31] do
      assert {:error, %Error{operation: :packet_decode, reason: :invalid_version}} =
               Packet.decode(@connect5, version)

      for packet <- [connect, connack] do
        assert {:error, %Error{operation: :packet_encode, reason: :invalid_version}} =
                 Packet.encode(%{packet | version: version})
      end
    end
  end

  test "MQTT 3.1.1 selectors enforce version agreement without enabling runtime" do
    assert {:error, %Error{class: :unsupported, reason: :version_mismatch}} =
             Packet.decode(@connect5, :v3_1_1)

    legacy = <<0x10, 13, 0, 4, "MQTT", 4, 2, 0, 60, 0, 1, "c">>
    assert {:ok, %{version: :v3_1_1}, <<>>} = Packet.decode(legacy, :v3_1_1)

    assert {:error, %Error{class: :unsupported, reason: :version_mismatch}} =
             Packet.decode(legacy, :v5)
  end

  test "opaque envelopes also enforce MQTT 5 minimum Remaining Length encoding" do
    for bytes <- [<<0xC0, 0x80, 0>>, <<0xC0, 0x80, 0x80, 0>>] do
      assert {:error, %Error{class: :malformed, reason: :nonminimal_variable_integer}} =
               Frame.decode(bytes)
    end
  end
end
