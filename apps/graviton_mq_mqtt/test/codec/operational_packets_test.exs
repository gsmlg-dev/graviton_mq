defmodule GravitonMQ.MQTT.Codec.OperationalPacketsTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.MQTT.Codec.{Error, Limits, Packet}
  alias GravitonMQ.MQTT.Packet, as: Data

  # Literal MQTT 5 wire vectors derived from OASIS MQTT 5 §§3.3–3.14.
  @fixtures [
    {<<0x30, 5, 0, 1, "t", 0, "x">>, Data.Publish,
     %{
       topic: "t",
       payload: "x",
       qos: 0,
       dup: false,
       retain: false,
       packet_id: nil,
       properties: []
     }},
    {<<0x3B, 7, 0, 1, "t", 0, 7, 0, "x">>, Data.Publish,
     %{topic: "t", payload: "x", qos: 1, dup: true, retain: true, packet_id: 7, properties: []}},
    {<<0x34, 7, 0, 1, "t", 0, 8, 0, "x">>, Data.Publish,
     %{topic: "t", payload: "x", qos: 2, packet_id: 8}},
    {<<0x40, 2, 0, 7>>, Data.Puback, %{packet_id: 7, reason_code: 0, properties: []}},
    {<<0x40, 3, 0, 7, 0x87>>, Data.Puback, %{packet_id: 7, reason_code: 0x87, properties: []}},
    {<<0x82, 7, 0, 9, 0, 0, 1, "t", 1>>, Data.Subscribe,
     %{
       packet_id: 9,
       subscriptions: [
         {"t", %{qos: 1, no_local: false, retain_as_published: false, retain_handling: 0}}
       ]
     }},
    {<<0x90, 5, 0, 9, 0, 1, 0x87>>, Data.Suback,
     %{packet_id: 9, reason_codes: [1, 0x87], properties: []}},
    {<<0xA2, 6, 0, 10, 0, 0, 1, "t">>, Data.Unsubscribe,
     %{packet_id: 10, filters: ["t"], properties: []}},
    {<<0xB0, 4, 0, 10, 0, 0>>, Data.Unsuback,
     %{packet_id: 10, reason_codes: [0], properties: []}},
    {<<0xC0, 0>>, Data.Pingreq, %{}},
    {<<0xD0, 0>>, Data.Pingresp, %{}},
    {<<0xE0, 0>>, Data.Disconnect, %{reason_code: 0, properties: []}},
    {<<0xE0, 1, 0x80>>, Data.Disconnect, %{reason_code: 0x80, properties: []}}
  ]

  test "decodes independent operational fixtures and preserves suffixes" do
    for {wire, module, fields} <- @fixtures do
      assert {:ok, packet, <<0xAA, 0xBB>>} = Packet.decode(wire <> <<0xAA, 0xBB>>, :v5)
      assert packet.__struct__ == module
      assert Map.take(packet, Map.keys(fields)) == fields
    end
  end

  test "every strict prefix remains incomplete" do
    for {wire, _, _} <- @fixtures, n <- 0..(byte_size(wire) - 1) do
      assert {:more, needed} = Packet.decode(binary_part(wire, 0, n), :v5)
      assert needed > 0
    end
  end

  test "encodes independent canonical vectors" do
    for {wire, module, fields} <- @fixtures do
      assert {:ok, packet, <<>>} = Packet.decode(wire, :v5)
      assert packet.__struct__ == module
      assert {:ok, encoded} = Packet.encode(packet)
      assert {:ok, ^packet, <<>>} = Packet.decode(encoded, :v5)
      # Optional reason/property tails are elided canonically.
      if module not in [Data.Puback, Data.Disconnect], do: assert(encoded == wire)
      assert Map.take(packet, Map.keys(fields)) == fields
    end
  end

  test "SUBSCRIBE shorthand encodes exact options and accepts all subscription options" do
    module = Data.Subscribe
    packet = struct!(module, packet_id: 9, subscriptions: [{"t", 1}])
    assert {:ok, <<0x82, 7, 0, 9, 0, 0, 1, "t", 1>>} = Packet.encode(packet)
    opts = %{qos: 2, no_local: true, retain_as_published: true, retain_handling: 2}
    packet = %{packet | subscriptions: [{"a/+", opts}]}
    assert {:ok, <<0x82, 9, 0, 9, 0, 0, 3, "a/+", 0x2E>>} = Packet.encode(packet)
    assert {:ok, ^packet, <<>>} = Packet.decode(<<0x82, 9, 0, 9, 0, 0, 3, "a/+", 0x2E>>, :v5)
  end

  test "malformed complete packets do not borrow a suffix" do
    for wire <- [
          <<0x30, 2, 0, 1>>,
          <<0x32, 5, 0, 1, "t", 0, 1>>,
          <<0x40, 1, 0>>,
          <<0x40, 2, 0, 0>>,
          <<0x82, 3, 0, 1, 0>>,
          <<0x82, 7, 0, 1, 0, 0, 1, "t", 3>>,
          <<0x82, 7, 0, 1, 0, 0, 1, "t", 0x40>>,
          <<0x90, 3, 0, 1, 0>>,
          <<0x90, 4, 0, 1, 0, 3>>,
          <<0xA2, 3, 0, 1, 0>>,
          <<0xB0, 3, 0, 1, 0>>,
          <<0xB0, 4, 0, 1, 0, 1>>,
          <<0xC0, 1, 0>>,
          <<0xD0, 1, 0>>,
          <<0xE0, 1, 1>>
        ] do
      assert {:error, %Error{operation: :packet_decode, class: :malformed}} =
               Packet.decode(wire <> <<0xC0, 0>>, :v5)
    end
  end

  test "topic filters enforce wildcard levels, shared names, and No Local" do
    for filter <- ["", "a#", "a/#/b", "a+", "$share//a", "$share/g/", "$share/g+/a"] do
      packet = struct!(Data.Subscribe, packet_id: 1, subscriptions: [{filter, 1}])
      assert {:error, %Error{class: :invalid_value}} = Packet.encode(packet)
    end

    for filter <- ["#", "+", "/", "a/+/#", "$SYS/#", "$share/group/a/+"] do
      packet = struct!(Data.Subscribe, packet_id: 1, subscriptions: [{filter, 1}])
      assert {:ok, _} = Packet.encode(packet)
    end

    packet =
      struct!(Data.Subscribe,
        packet_id: 1,
        subscriptions: [{"$share/g/t", %{qos: 1, no_local: true}}]
      )

    assert {:error, %Error{class: :invalid_value}} = Packet.encode(packet)
  end

  test "PUBLISH properties preserve order, repeated subscription identifiers, and aliases" do
    # PUBLISH Topic Alias 1, Subscription Identifier 3 twice, User Property k=v.
    wire =
      <<0x30, 20, 0, 0, 16, 0x23, 0, 1, 0x0B, 3, 0x0B, 3, 0x26, 0, 1, "k", 0, 1, "v", 0x01, 1,
        "x">>

    assert {:ok, packet, <<>>} = Packet.decode(wire, :v5)
    assert packet.topic == ""

    assert packet.properties == [
             topic_alias: 1,
             subscription_identifier: 3,
             subscription_identifier: 3,
             user_property: {"k", "v"},
             payload_format_indicator: 1
           ]

    assert {:ok, ^wire} = Packet.encode(packet)

    for props <- [
          [topic_alias: 0],
          [subscription_identifier: 0],
          [topic_alias: 1, topic_alias: 2]
        ] do
      invalid = %{packet | properties: props}
      assert {:error, %Error{class: :invalid_value}} = Packet.encode(invalid)
    end

    assert {:error, %Error{class: :invalid_value}} = Packet.encode(%{packet | properties: []})
  end

  test "packet properties enforce location and singleton cardinality" do
    for {module, fields, properties} <- [
          {Data.Puback, [packet_id: 1], [maximum_qos: 1]},
          {Data.Subscribe, [packet_id: 1, subscriptions: [{"t", 0}]],
           [subscription_identifier: 1, subscription_identifier: 2]},
          {Data.Unsubscribe, [packet_id: 1, filters: ["t"]], [subscription_identifier: 1]},
          {Data.Disconnect, [], [receive_maximum: 1]}
        ] do
      assert {:error, %Error{class: :invalid_value}} =
               Packet.encode(struct!(module, fields ++ [properties: properties]))
    end
  end

  test "limits apply to full packets and payloads without truncating" do
    packet = struct!(Data.Publish, topic: "t", payload: :binary.copy("x", 100))
    assert {:ok, wire} = Packet.encode(packet)
    assert {:ok, ^wire} = Packet.encode(packet, %Limits{max_packet_size: byte_size(wire)})

    assert {:error, %Error{class: :limit_exceeded}} =
             Packet.encode(packet, %Limits{max_packet_size: byte_size(wire) - 1})

    assert {:error, %Error{class: :limit_exceeded}} =
             Packet.decode(wire, :v5, %Limits{max_packet_size: byte_size(wire) - 1})
  end
end
