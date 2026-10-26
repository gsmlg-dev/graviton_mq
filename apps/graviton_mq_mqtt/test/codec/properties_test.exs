defmodule GravitonMQ.MQTT.Codec.PropertiesTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.MQTT.Codec.{Limits, Packet}

  @shared [
    {:session_expiry_interval, 10, <<0x11, 0, 0, 0, 10>>},
    {:receive_maximum, 2, <<0x21, 0, 2>>},
    {:maximum_packet_size, 1024, <<0x27, 0, 0, 4, 0>>},
    {:topic_alias_maximum, 0, <<0x22, 0, 0>>},
    {:authentication_method, "m", <<0x15, 0, 1, "m">>},
    {:user_property, {"k", "v"}, <<0x26, 0, 1, "k", 0, 1, "v">>}
  ]

  @connect @shared ++
             [
               {:request_response_information, 1, <<0x19, 1>>},
               {:request_problem_information, 0, <<0x17, 0>>}
             ]

  @connack @shared ++
             [
               {:maximum_qos, 1, <<0x24, 1>>},
               {:retain_available, 0, <<0x25, 0>>},
               {:assigned_client_identifier, "id", <<0x12, 0, 2, "id">>},
               {:reason_string, "why", <<0x1F, 0, 3, "why">>},
               {:wildcard_subscription_available, 1, <<0x28, 1>>},
               {:subscription_identifier_available, 0, <<0x29, 0>>},
               {:shared_subscription_available, 0, <<0x2A, 0>>},
               {:server_keep_alive, 65_535, <<0x13, 0xFF, 0xFF>>},
               {:response_information, "r", <<0x1A, 0, 1, "r">>},
               {:server_reference, "s", <<0x1C, 0, 1, "s">>},
               {:authentication_data, <<0, 0xFF>>, <<0x16, 0, 2, 0, 0xFF>>}
             ]

  @will [
    {:will_delay_interval, 5, <<0x18, 0, 0, 0, 5>>},
    {:payload_format_indicator, 1, <<0x01, 1>>},
    {:message_expiry_interval, 10, <<0x02, 0, 0, 0, 10>>},
    {:content_type, "t", <<0x03, 0, 1, "t">>},
    {:response_topic, "a/b", <<0x08, 0, 3, "a/b">>},
    {:correlation_data, <<0, 0xFF>>, <<0x09, 0, 2, 0, 0xFF>>},
    {:user_property, {"k", "v"}, <<0x26, 0, 1, "k", 0, 1, "v">>}
  ]

  for {scope, entries} <- [connect: @connect, connack: @connack, will: @will],
      {name, value, encoded} <- entries do
    @scope scope
    @property {name, value}
    @encoded encoded

    test "independent #{@scope} fixture retains #{name} and its exact type" do
      bytes = fixture(@scope, @encoded)
      version = if @scope == :connack, do: :v5, else: :auto
      assert {:ok, packet, <<>>} = Packet.decode(bytes, version)
      properties = if @scope == :will, do: packet.will.properties, else: packet.properties
      assert [@property] == properties
      assert {:ok, ^bytes} = Packet.encode(packet)

      for size <- 0..(byte_size(bytes) - 1) do
        assert {:more, required} = Packet.decode(binary_part(bytes, 0, size), version)
        assert required > 0
      end
    end
  end

  test "ordered repeated User Properties survive decode and canonical encoding" do
    bytes =
      <<0x10, 0x21, 0, 4, "MQTT", 5, 2, 0, 60, 0x13, 0x11, 0, 0, 0, 10, 0x26, 0, 1, "k", 0, 1,
        "a", 0x26, 0, 1, "k", 0, 1, "b", 0, 1, "c">>

    assert {:ok, packet, <<>>} = Packet.decode(bytes)

    assert [session_expiry_interval: 10, user_property: {"k", "a"}, user_property: {"k", "b"}] =
             packet.properties

    assert {:ok, ^bytes} = Packet.encode(packet)
  end

  test "CONNECT Authentication Data requires a Method, regardless of property ordering" do
    properties = <<0x16, 0, 2, 0, 0xFF, 0x15, 0, 1, "m">>
    bytes = fixture(:connect, properties)
    assert {:ok, packet, <<>>} = Packet.decode(bytes)
    assert [authentication_data: <<0, 0xFF>>, authentication_method: "m"] = packet.properties
    assert {:ok, ^bytes} = Packet.encode(packet)

    assert {:error, %{class: :malformed, reason: :authentication_method_required}} =
             Packet.decode(fixture(:connect, <<0x16, 0, 0>>))

    assert {:error, %{class: :invalid_value, reason: :authentication_method_required}} =
             Packet.encode(%{packet | properties: [authentication_data: ""]})
  end

  test "rejects unknown, misplaced, duplicate, truncated, and invalid properties" do
    for properties <- [
          <<0x7F>>,
          <<0x80, 1>>,
          <<0x18, 0, 0, 0, 1>>,
          <<0x11, 0, 0, 0, 0, 0x11, 0, 0, 0, 1>>,
          <<0x21, 0, 0>>,
          <<0x27, 0, 0, 0, 0>>,
          <<0x19, 2>>,
          <<0x15, 0, 1, 0>>,
          <<0x21, 1>>,
          <<0x91, 0, 0, 0, 0, 1>>
        ] do
      assert {:error, %{class: :malformed}} = Packet.decode(fixture(:connect, properties))
    end

    for topic <- ["", "+", "a/#"] do
      encoded = <<0x08, byte_size(topic)::16, topic::binary>>
      assert {:error, %{class: :malformed}} = Packet.decode(fixture(:will, encoded))
    end

    assert {:error, %{class: :malformed}} = Packet.decode(fixture(:connack, <<0x12, 0, 0>>), :v5)
    assert {:error, %{class: :malformed}} = Packet.decode(fixture(:connack, <<0x24, 2>>), :v5)
  end

  test "Property Length cannot consume client fields or use nonminimal encoding" do
    for body <- [
          <<0, 4, "MQTT", 5, 2, 0, 60, 1, 0x11, 0, 1, "c">>,
          <<0, 4, "MQTT", 5, 2, 0, 60, 0x80, 0, 0, 1, "c">>
        ] do
      assert {:error, %{class: :malformed}} = Packet.decode(packet(0x10, body))
    end
  end

  test "property count limits include repeated user properties" do
    properties = <<0x26, 0, 0, 0, 0, 0x26, 0, 0, 0, 0>>
    bytes = fixture(:connect, properties)
    limits = struct(Limits, max_properties: 2)
    assert {:ok, value, <<>>} = Packet.decode(bytes, :auto, limits)
    assert {:ok, ^bytes} = Packet.encode(value, limits)
    limits = %{limits | max_properties: 1}

    assert {:error, %{class: :limit_exceeded, reason: :too_many_properties}} =
             Packet.decode(bytes, :auto, limits)

    assert {:error, %{class: :limit_exceeded, reason: :too_many_properties}} =
             Packet.encode(value, limits)
  end

  test "3.1.1 encoding rejects MQTT 5 properties and invalid semantic property data" do
    assert {:ok, packet, <<>>} = Packet.decode(<<0x10, 13, 0, 4, "MQTT", 4, 2, 0, 60, 0, 1, "c">>)

    assert {:error, %{class: :invalid_value, reason: :properties_not_supported}} =
             Packet.encode(%{packet | properties: [receive_maximum: 1]})

    for properties <- [
          %{},
          [unknown: 1],
          [receive_maximum: 0],
          [user_property: {"k", 1}],
          [session_expiry_interval: -1],
          [:invalid_entry],
          [{:user_property, {"", ""}} | :bad]
        ] do
      assert {:error, %{class: :invalid_value}} =
               Packet.encode(%{packet | version: :v5, properties: properties})
    end
  end

  defp fixture(:connect, properties) do
    packet(
      0x10,
      <<0, 4, "MQTT", 5, 2, 0, 60, length_bytes(byte_size(properties))::binary,
        properties::binary, 0, 1, "c">>
    )
  end

  defp fixture(:connack, properties),
    do: packet(0x20, <<0, 0, length_bytes(byte_size(properties))::binary, properties::binary>>)

  defp fixture(:will, properties) do
    packet(
      0x10,
      <<0, 4, "MQTT", 5, 6, 0, 60, 0, 0, 1, "c", length_bytes(byte_size(properties))::binary,
        properties::binary, 0, 1, "w", 0, 2, 0, 0xFF>>
    )
  end

  defp packet(header, body), do: <<header, length_bytes(byte_size(body))::binary, body::binary>>
  defp length_bytes(n) when n < 128, do: <<n>>
  defp length_bytes(n), do: <<rem(n, 128) + 128, length_bytes(div(n, 128))::binary>>
end
