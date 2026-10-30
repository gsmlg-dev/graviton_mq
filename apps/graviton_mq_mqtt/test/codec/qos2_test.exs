defmodule GravitonMQ.MQTT.Codec.QoS2Test do
  use ExUnit.Case, async: true

  alias GravitonMQ.MQTT.Codec.{Error, Limits, Packet}
  alias GravitonMQ.MQTT.Packet.{Pubrec, Pubrel, Pubcomp}

  @packets [{0x50, Pubrec}, {0x62, Pubrel}, {0x70, Pubcomp}]

  test "independent compact QoS2 acknowledgement vectors preserve suffix and default success" do
    for {header, module} <- @packets do
      assert {:ok, packet, <<0xAA>>} = Packet.decode(<<header, 2, 0, 7, 0xAA>>, :v5)
      assert packet == struct!(module, packet_id: 7)
      assert {:ok, <<^header, 2, 0, 7>>} = Packet.encode(packet)

      for wire <- [<<header, 3, 0, 7, 0>>, <<header, 4, 0, 7, 0, 0>>] do
        assert {:ok, ^packet, <<>>} = Packet.decode(wire, :v5)
      end

      wire = <<header, 2, 0, 7>>
      for size <- 0..3, do: assert({:more, _} = Packet.decode(binary_part(wire, 0, size), :v5))
    end
  end

  test "every acknowledgement reason octet obeys its exact OASIS table" do
    for {header, module} <- @packets, code <- 0..255 do
      valid =
        if module == Pubrec,
          do: code in [0, 0x10, 0x80, 0x83, 0x87, 0x90, 0x91, 0x97, 0x99],
          else: code in [0, 0x92]

      result = Packet.decode(<<header, 4, 0, 1, code, 0>>, :v5)

      if valid do
        assert {:ok, packet, <<>>} = result
        assert {:ok, _} = Packet.encode(packet)
      else
        assert {:error, %Error{class: :malformed, reason: :invalid_reason_code}} = result
      end
    end
  end

  test "QoS2 properties preserve ordered repeated pairs and enforce location and bounds" do
    properties = [reason_string: "accepted", user_property: {"a", "1"}, user_property: {"a", "2"}]

    for {_header, module} <- @packets do
      packet = struct!(module, packet_id: 65_535, properties: properties)
      assert {:ok, bytes} = Packet.encode(packet)
      assert {:ok, ^packet, <<>>} = Packet.decode(bytes, :v5)

      assert {:error, %Error{class: :limit_exceeded}} =
               Packet.encode(packet, %Limits{max_properties: 2})

      assert {:error, %Error{class: :limit_exceeded}} =
               Packet.decode(bytes, :v5, %Limits{max_properties: 2})

      assert {:error, %Error{reason: :property_not_allowed}} =
               Packet.encode(%{packet | properties: [topic_alias: 1]})

      assert {:error, %Error{reason: :duplicate_property}} =
               Packet.encode(%{packet | properties: [reason_string: "a", reason_string: "b"]})

      assert {:error, %Error{class: :limit_exceeded}} =
               Packet.encode(packet, %Limits{max_packet_size: 4})
    end
  end

  test "malformed identifiers flags properties and legacy operational use stay explicit" do
    for {header, module} <- @packets do
      for wire <- [
            <<header, 1, 1>>,
            <<header, 2, 0, 0>>,
            <<header, 4, 0, 1, 0, 1>>,
            <<header, 5, 0, 1, 0, 0, 0>>
          ] do
        assert {:error, %Error{class: :malformed}} = Packet.decode(wire, :v5)
      end

      assert {:error, %Error{reason: :invalid_fixed_header}} =
               Packet.decode(<<Bitwise.bxor(header, 1), 2, 0, 1>>, :v5)

      assert {:error, %Error{class: :unsupported}} = Packet.decode(<<header, 2, 0, 1>>, :v3_1_1)
      assert {:error, %Error{reason: :version_required}} = Packet.decode(<<header, 2, 0, 1>>)

      assert {:error, %Error{reason: :invalid_version}} =
               Packet.encode(struct!(module, packet_id: 1, version: :v3_1_1))
    end
  end
end
