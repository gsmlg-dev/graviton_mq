defmodule GravitonMQ.MQTT.Codec.ConnectTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.MQTT.Codec.{Limits, Packet}

  @v5 <<0x10, 0x0E, 0, 4, "MQTT", 5, 2, 0, 60, 0, 0, 1, "c">>

  test "independent MQTT 5 CONNECT fixture decodes and encodes exactly" do
    suffix = <<0x20, 3, 0, 0, 0, 0xAB>>

    assert {:ok, packet, ^suffix} = Packet.decode(@v5 <> suffix)

    assert %{
             version: :v5,
             client_id: "c",
             clean_start: true,
             keep_alive: 60,
             will: nil,
             username: nil,
             password: nil,
             properties: []
           } = packet

    assert {:ok, @v5} = Packet.encode(packet)
    assert {:ok, ^packet, <<>>} = Packet.decode(@v5, :v5)
  end

  test "every strict prefix remains incomplete" do
    for size <- 0..(byte_size(@v5) - 1) do
      assert {:more, required} = Packet.decode(binary_part(@v5, 0, size))
      assert required > 0
    end
  end

  test "unsupported selectors, protocol levels, and names fail explicitly" do
    assert {:error, %{class: :invalid_value, reason: :invalid_version}} =
             Packet.decode(@v5, :mqtt31)

    assert {:error, %{class: :unsupported, reason: :unsupported_version}} =
             Packet.decode(<<0x10, 0x0D, 0, 4, "MQTT", 6, 2, 0, 60, 0, 1, "c">>)

    assert {:error, %{class: :unsupported, reason: :unsupported_protocol_name}} =
             Packet.decode(<<0x10, 0x0D, 0, 4, "AMQP", 4, 2, 0, 60, 0, 1, "c">>)
  end

  test "MQTT 5 permits a binary password without a username" do
    body5 = <<0, 4, "MQTT", 5, 0x42, 0, 0, 0, 0, 1, "c", 0, 2, 0, 0xFF>>
    bytes5 = <<0x10, byte_size(body5), body5::binary>>
    assert {:ok, packet, <<>>} = Packet.decode(bytes5)
    assert %{username: nil, password: <<0, 0xFF>>} = packet
    assert {:ok, ^bytes5} = Packet.encode(packet)
  end

  test "empty username and password are present values, distinct from omission" do
    body = <<0, 4, "MQTT", 5, 0xC2, 0, 0, 0, 0, 1, "c", 0, 0, 0, 0>>
    bytes = <<0x10, byte_size(body), body::binary>>
    assert {:ok, packet, <<>>} = Packet.decode(bytes)
    assert %{username: "", password: ""} = packet
    assert {:ok, ^bytes} = Packet.encode(packet)
  end

  test "Will topic, payload, QoS, and retain flag remain protocol data" do
    body = <<0, 4, "MQTT", 5, 0x2E, 0, 0, 0, 0, 1, "c", 0, 0, 1, "w", 0, 2, 0, 0xFF>>
    bytes = <<0x10, byte_size(body), body::binary>>
    assert {:ok, packet, <<>>} = Packet.decode(bytes)
    assert %{will: %{topic: "w", payload: <<0, 0xFF>>, qos: 1, retain: true}} = packet
    assert {:ok, ^bytes} = Packet.encode(packet)
  end

  test "reserved and inconsistent Will flags are malformed" do
    for flags <- [1, 8, 0x20, 0x1C] do
      assert {:error, %{class: :malformed, reason: :invalid_connect_flags}} =
               Packet.decode(<<0x10, 14, 0, 4, "MQTT", 5, flags, 0, 60, 0, 0, 1, "c">>)
    end
  end

  test "empty Client ID is retained with either MQTT 5 Clean Start value" do
    for flags <- [0, 2] do
      bytes = <<0x10, 13, 0, 4, "MQTT", 5, flags, 0, 0, 0, 0, 0>>
      assert {:ok, %{client_id: ""} = packet, <<>>} = Packet.decode(bytes)
      assert {:ok, ^bytes} = Packet.encode(packet)
    end
  end

  test "UTF-8 preserves BOM and normalization and rejects null, surrogates, and bad sequences" do
    for id <- [<<0xEF, 0xBB, 0xBF, "c">>, "e\u0301", "é", <<1>>, <<0xEF, 0xBF, 0xBF>>] do
      body = <<0, 4, "MQTT", 5, 2, 0, 0, 0, byte_size(id)::16, id::binary>>
      bytes = <<0x10, byte_size(body), body::binary>>
      assert {:ok, %{client_id: ^id} = packet, <<>>} = Packet.decode(bytes)
      assert {:ok, ^bytes} = Packet.encode(packet)
    end

    for id <- [<<0>>, <<0xFF>>, <<0xED, 0xA0, 0x80>>, <<0xC0, 0x80>>] do
      body = <<0, 4, "MQTT", 5, 2, 0, 0, 0, byte_size(id)::16, id::binary>>

      assert {:error, %{class: :malformed, reason: :invalid_utf8}} =
               Packet.decode(<<0x10, byte_size(body), body::binary>>)
    end
  end

  test "complete packets with truncated fields or unexpected trailing fields are malformed" do
    for body <- [
          <<0, 4, "MQTT", 5, 2, 0, 60, 0, 0>>,
          <<0, 4, "MQTT", 5, 2, 0, 60, 0, 0, 2, "c">>,
          <<0, 4, "MQTT", 5, 2, 0, 60, 0, 0, 1, "c", 0>>
        ] do
      assert {:error, %{class: :malformed}} =
               Packet.decode(<<0x10, byte_size(body), body::binary>>)
    end
  end

  test "MQTT 5 rejects nonminimal Remaining Length" do
    <<header, size, body::binary>> = @v5

    assert {:error, %{class: :malformed, reason: :nonminimal_variable_integer}} =
             Packet.decode(<<header, size + 128, 0, body::binary>>)
  end

  test "Will Topic Name must be nonempty and cannot contain wildcards" do
    for topic <- ["", "+", "a/#"] do
      body =
        <<0, 4, "MQTT", 5, 6, 0, 0, 0, 0, 1, "c", 0, byte_size(topic)::16, topic::binary, 0, 0>>

      assert {:error, %{class: :malformed, reason: :invalid_will_topic}} =
               Packet.decode(<<0x10, byte_size(body), body::binary>>)
    end
  end

  test "packet limits count the entire header and validate configuration" do
    limits = struct(Limits, max_packet_size: byte_size(@v5))
    assert {:ok, packet, <<>>} = Packet.decode(@v5, :auto, limits)
    assert {:ok, @v5} = Packet.encode(packet, limits)

    limits = %{limits | max_packet_size: byte_size(@v5) - 1}
    assert {:error, %{class: :limit_exceeded}} = Packet.decode(@v5, :auto, limits)
    assert {:error, %{class: :limit_exceeded}} = Packet.encode(packet, limits)

    for limits <- [nil, struct(Limits, max_field_bytes: -1), struct(Limits, max_properties: 0.5)] do
      assert {:error, %{class: :invalid_value, reason: :invalid_limits}} =
               Packet.decode(@v5, :auto, limits)
    end
  end

  test "unimplemented well-framed packets remain unsupported" do
    assert {:error, %{class: :unsupported, reason: :unsupported_packet_type}} =
             Packet.decode(<<0xF0, 0>>, :v5)
  end

  test "credentials never appear in invalid-value diagnostics" do
    packet =
      struct(GravitonMQ.MQTT.Packet.Connect,
        version: :v5,
        client_id: "c",
        clean_start: true,
        password: ["private-credential"]
      )

    assert {:error, error} = Packet.encode(packet)
    refute inspect(error) =~ "private-credential"
  end
end
