defmodule GravitonMQ.MQTT.Codec.OperationalValidationTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.MQTT.Codec.{Error, Limits, Packet}
  alias GravitonMQ.MQTT.Packet, as: Data

  test "acknowledgement and DISCONNECT codes match every octet of their tables" do
    tables = [
      {4, [0, 0x10, 0x80, 0x83, 0x87, 0x90, 0x91, 0x97, 0x99]},
      {9, [0, 1, 2, 0x80, 0x83, 0x87, 0x8F, 0x91, 0x97, 0x9E, 0xA1, 0xA2]},
      {11, [0, 0x11, 0x80, 0x83, 0x87, 0x8F, 0x91]},
      {14,
       [
         0,
         4,
         0x80,
         0x81,
         0x82,
         0x83,
         0x87,
         0x89,
         0x8B,
         0x8D,
         0x8E,
         0x8F,
         0x90,
         0x93,
         0x94,
         0x95,
         0x96,
         0x97,
         0x98,
         0x99,
         0x9A,
         0x9B,
         0x9C,
         0x9D,
         0x9E,
         0x9F,
         0xA0,
         0xA1,
         0xA2
       ]}
    ]

    for {type, valid} <- tables, code <- 0..255 do
      body =
        case type do
          4 -> <<0, 1, code, 0>>
          14 -> <<code, 0>>
          _ -> <<0, 1, 0, code>>
        end

      result = Packet.decode(<<type::4, 0::4, byte_size(body), body::binary>>, :v5)

      if code in valid do
        assert {:ok, packet, <<>>} = result
        assert {:ok, _} = Packet.encode(packet)
      else
        assert {:error, %Error{class: :malformed, reason: :invalid_reason_code}} = result
      end
    end
  end

  test "all subscription-option octets obey reserved bits and defined ranges" do
    for flags <- 0..255 do
      result = Packet.decode(<<0x82, 7, 0, 1, 0, 0, 1, "t", flags>>, :v5)
      valid? = flags < 64 and rem(flags, 4) < 3 and div(flags, 16) < 3

      if valid?,
        do: assert({:ok, _, <<>>} = result),
        else:
          assert(
            {:error, %Error{class: :malformed, reason: :invalid_subscription_options}} = result
          )
    end
  end

  test "operational semantic shape and scalar errors are explicit" do
    publish = struct!(Data.Publish, topic: "t", payload: "x")
    subscribe = struct!(Data.Subscribe, packet_id: 1, subscriptions: [{"t", 0}])

    for packet <- [
          %{publish | qos: 3},
          %{publish | qos: 1.0},
          %{publish | dup: 1},
          %{publish | retain: 1},
          %{publish | dup: true},
          %{publish | packet_id: 1},
          %{publish | qos: 1},
          %{publish | topic: "a/#"},
          %{publish | topic: nil},
          %{publish | payload: ["x"]},
          %{publish | properties: %{}},
          %{publish | version: :v3_1_1},
          %{subscribe | packet_id: 0},
          %{subscribe | packet_id: 65_536},
          %{subscribe | subscriptions: []},
          %{subscribe | subscriptions: ["t"]},
          %{subscribe | subscriptions: [{"t", %{qos: 1, unknown_option: true}}]},
          %{subscribe | subscriptions: [{"t", %{qos: 1, no_local: 1}}]},
          struct!(Data.Suback, packet_id: 1, reason_codes: []),
          struct!(Data.Suback, packet_id: 1, reason_codes: [256]),
          struct!(Data.Unsubscribe, packet_id: 1, filters: []),
          struct!(Data.Unsubscribe, packet_id: 1, filters: [nil]),
          struct!(Data.Unsuback, packet_id: 1, reason_codes: [1]),
          struct!(Data.Disconnect, reason_code: -1),
          struct!(Data.Puback, packet_id: 0)
        ] do
      assert {:error, %Error{operation: :packet_encode, class: :invalid_value}} =
               Packet.encode(packet)
    end
  end

  test "property-bearing acknowledgements, subscriptions, and DISCONNECT retain ordered pairs" do
    props = [reason_string: "why", user_property: {"a", "1"}, user_property: {"a", "2"}]

    for packet <- [
          struct!(Data.Puback, packet_id: 65535, reason_code: 0x87, properties: props),
          struct!(Data.Suback, packet_id: 65535, reason_codes: [1], properties: props),
          struct!(Data.Unsuback, packet_id: 65535, reason_codes: [0], properties: props),
          struct!(Data.Disconnect, reason_code: 0x80, properties: props)
        ] do
      assert {:ok, encoded} = Packet.encode(packet)
      assert {:ok, ^packet, <<>>} = Packet.decode(encoded, :v5)

      assert {:error, %Error{class: :limit_exceeded, reason: :too_many_properties}} =
               Packet.encode(packet, %Limits{max_properties: 2})

      assert {:error, %Error{class: :limit_exceeded, reason: :too_many_properties}} =
               Packet.decode(encoded, :v5, %Limits{max_properties: 2})
    end

    packet =
      struct!(Data.Subscribe,
        packet_id: 1,
        subscriptions: [{"t", 0}],
        properties: [subscription_identifier: 268_435_455]
      )

    assert {:ok, encoded} = Packet.encode(packet)
    assert {:ok, decoded, <<>>} = Packet.decode(encoded, :v5)
    assert decoded.properties == packet.properties
  end

  test "large wildcard-filter level counts remain valid and exact" do
    filter = :binary.copy("/", 32_767) <> "+/#"
    packet = struct!(Data.Subscribe, packet_id: 1, subscriptions: [{filter, 1}])
    assert {:ok, encoded} = Packet.encode(packet)
    assert {:ok, decoded, <<>>} = Packet.decode(encoded, :v5)
    assert [{^filter, %{qos: 1}}] = decoded.subscriptions
  end

  test "complete adversarial operational envelopes always return explicit results" do
    for type <- [3, 4, 8, 9, 10, 11, 12, 13, 14],
        size <- [0, 1, 2, 3, 4, 7, 12, 32],
        byte <- 0..255 do
      body = :binary.copy(<<byte>>, size)
      flags = if type in [8, 10], do: 2, else: 0

      case Packet.decode(<<type::4, flags::4, size, body::binary>>, :v5) do
        {:ok, packet, <<>>} ->
          assert {:ok, encoded} = Packet.encode(packet)
          assert {:ok, ^packet, <<>>} = Packet.decode(encoded, :v5)

        {:error, %Error{operation: :packet_decode, class: class}} ->
          assert class in [:malformed, :unsupported, :limit_exceeded, :invalid_value]
      end
    end
  end
end
