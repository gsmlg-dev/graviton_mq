defmodule GravitonMQ.MQTT.BrokerAliasTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.MQTT.{Broker, Broker.Checkpoint}
  alias GravitonMQ.MQTT.Codec.Packet
  alias GravitonMQ.MQTT.Packet.{Connack, Connect, Disconnect, Publish, Puback, Pubrec, Subscribe}

  defp new(options \\ []) do
    {:ok, state} = Broker.new(options)
    state
  end

  defp allowed(state, effects, now \\ 0) do
    Enum.reduce(effects, {state, []}, fn
      {:authorize, op, request, _deadline}, {state, effects} ->
        result = if is_list(request), do: Enum.map(request, fn _ -> :ok end), else: {:ok, :user}

        {state, more} =
          Broker.transition(
            state,
            {:auth_result, op, Map.fetch!(state.operations, op).generation, result, now}
          )

        {state, effects ++ more}

      effect, {state, effects} ->
        {state, effects ++ [effect]}
    end)
    |> then(fn {state, effects} ->
      if Enum.any?(effects, &match?({:authorize, _, _, _}, &1)),
        do: allowed(state, effects, now),
        else: {state, effects}
    end)
  end

  defp attach_as(state, id, client_id, options) do
    {state, []} = Broker.transition(state, {:connection_open, id, 1, 0})

    connect = %Connect{
      version: :v5,
      client_id: client_id,
      clean_start: Keyword.get(options, :clean_start, true),
      properties: Keyword.get(options, :properties, [])
    }

    {state, effects} = Broker.transition(state, {:packet, id, 1, connect, <<16, 0>>, 0})
    allowed(state, effects)
  end

  defp attach(state, id, options \\ []), do: attach_as(state, id, to_string(id), options)

  defp packet(state, id, packet, now \\ 1) do
    {state, effects} = Broker.transition(state, {:packet, id, 1, packet, <<1, 2>>, now})
    allowed(state, effects, now)
  end

  defp subscribe(state, id, filters) do
    packet(state, id, %Subscribe{
      packet_id: rem(state.next_id, 65_535) + 1,
      subscriptions: filters
    })
  end

  defp publishes(effects, id), do: for({:send, ^id, _, _, %Publish{} = p} <- effects, do: p)

  defp disconnected?(effects, id, code),
    do: Enum.any?(effects, &match?({:send, ^id, _, _, %Disconnect{reason_code: ^code}}, &1))

  test "CONNACK advertises the configured inbound Topic Alias Maximum" do
    for {options, expected} <- [
          {[], 10},
          {[topic_alias_maximum: 0], 0},
          {[topic_alias_maximum: 65_535, max_alias_bytes: 9_000_000_000_000], 65_535}
        ] do
      {_state, effects} = attach(new(options), :one)

      assert [{:send, :one, 1, _, %Connack{properties: properties}}] =
               Enum.filter(effects, &match?({:send, _, _, _, %Connack{}}, &1))

      assert properties[:topic_alias_maximum] == expected
    end
  end

  test "alias limits are validated and statically bounded by max_alias_bytes" do
    assert {:error, :invalid_options} = Broker.new(topic_alias_maximum: 65_536)
    assert {:error, :invalid_options} = Broker.new(max_outbound_topic_aliases: 65_536)
    assert {:error, :alias_bytes_exceeded} = Broker.new(max_alias_bytes: 1)
    assert {:ok, _} = Broker.new(topic_alias_maximum: 0, max_alias_bytes: 0)

    bounded = [max_connections: 1, topic_alias_maximum: 1, max_packet_size: 100]
    assert {:ok, _} = Broker.new(bounded ++ [max_alias_bytes: 100])
    assert {:error, :alias_bytes_exceeded} = Broker.new(bounded ++ [max_alias_bytes: 99])

    outbound = [max_connections: 2, topic_alias_maximum: 1, max_outbound_topic_aliases: 2]
    assert {:ok, _} = Broker.new(outbound ++ [max_packet_size: 10, max_alias_bytes: 60])

    assert {:error, :alias_bytes_exceeded} =
             Broker.new(outbound ++ [max_packet_size: 10, max_alias_bytes: 59])
  end

  # Upgrade compatibility: configurations that started before Topic Alias
  # support (any max_connections) must still start; an omitted bound is the
  # derived static worst case, never a rejection.
  test "an omitted max_alias_bytes is derived as the static worst case" do
    assert {:ok, state} = Broker.new()
    assert state.limits.max_alias_bytes == 128 * 10 * 65_535

    assert {:ok, state} = Broker.new(max_connections: 4_096)
    assert state.limits.max_alias_bytes == 4_096 * 10 * 65_535

    assert {:ok, state} =
             Broker.new(
               max_connections: 3,
               topic_alias_maximum: 2,
               max_outbound_topic_aliases: 5,
               max_packet_size: 100
             )

    assert state.limits.max_alias_bytes == 3 * 7 * 100
  end

  test "inbound aliases resolve before every PUBLISH check and never reach subscribers" do
    {state, _} = attach(new(), :pub)
    {state, _} = attach(state, :sub)
    {state, _} = subscribe(state, :sub, [{"a/#", 2}])

    {state, first} =
      packet(state, :pub, %Publish{topic: "a/b", payload: "0", properties: [topic_alias: 1]})

    {state, second} =
      packet(state, :pub, %Publish{
        topic: "",
        payload: "1",
        qos: 1,
        packet_id: 1,
        properties: [topic_alias: 1]
      })

    {state, third} =
      packet(state, :pub, %Publish{
        topic: "",
        payload: "2",
        qos: 2,
        packet_id: 2,
        properties: [topic_alias: 1]
      })

    received = publishes(first ++ second ++ third, :sub)

    assert Enum.map(received, &{&1.topic, &1.payload}) == [
             {"a/b", "0"},
             {"a/b", "1"},
             {"a/b", "2"}
           ]

    refute Enum.any?(received, &Keyword.has_key?(&1.properties, :topic_alias))

    assert Enum.any?(
             second,
             &match?({:send, :pub, 1, _, %Puback{packet_id: 1, reason_code: 0}}, &1)
           )

    assert state.connections[:pub].aliases.inbound == %{1 => "a/b"}
  end

  test "stored messages keep the resolved topic and re-encoded bytes without the alias" do
    {state, _} = attach(new(), :pub)

    {state, _} =
      packet(state, :pub, %Publish{
        topic: "r/t",
        payload: "x",
        retain: true,
        properties: [topic_alias: 2]
      })

    {state, _} =
      packet(state, :pub, %Publish{
        topic: "",
        payload: "y",
        retain: true,
        properties: [topic_alias: 2]
      })

    message = state.retained["r/t"]
    assert message.payload == "y"
    refute Keyword.has_key?(message.properties, :topic_alias)

    assert {:ok, %Publish{topic: "r/t", properties: []}, <<>>} =
             Packet.decode(message.encoded, :v5)

    assert {:ok, _snapshot} = Checkpoint.capture(state, 0, 100_000)
  end

  test "out-of-range aliases disconnect with 0x94 and unknown aliases with 0x82" do
    for {options, publication, code} <- [
          {[], %Publish{topic: "a", payload: "x", properties: [topic_alias: 11]}, 0x94},
          {[topic_alias_maximum: 0],
           %Publish{topic: "a", payload: "x", properties: [topic_alias: 1]}, 0x94},
          {[], %Publish{topic: "", payload: "x", properties: [topic_alias: 3]}, 0x82}
        ] do
      {state, _} = attach(new(options), :pub)
      {state, effects} = packet(state, :pub, publication)
      assert disconnected?(effects, :pub, code), inspect({options, publication})
      refute Map.has_key?(state.connections, :pub)
    end
  end

  test "alias mappings belong to one Network Connection (MQTT-3.3.2-7)" do
    {state, _} = attach(new(), :pub, properties: [session_expiry_interval: 60])

    {state, _} =
      packet(state, :pub, %Publish{topic: "a", payload: "x", properties: [topic_alias: 1]})

    {state, _} = Broker.transition(state, {:connection_down, :pub, 1, :abnormal, 2})

    {state, _} =
      attach_as(state, :pub_again, "pub",
        clean_start: false,
        properties: [session_expiry_interval: 60]
      )

    {_state, effects} =
      packet(state, :pub_again, %Publish{topic: "", payload: "y", properties: [topic_alias: 1]})

    assert disconnected?(effects, :pub_again, 0x82)
  end

  test "a DUP retransmission in alias form is compared after resolution" do
    {state, _} = attach(new(), :pub)
    {state, _} = attach(state, :sub)
    {state, _} = subscribe(state, :sub, [{"d", 1}])
    first = %Publish{topic: "d", payload: "x", qos: 1, packet_id: 5, properties: [topic_alias: 1]}
    {state, pending} = Broker.transition(state, {:packet, :pub, 1, first, <<1, 2>>, 1})
    duplicate = %Publish{first | topic: "", dup: true}
    assert {state, []} = Broker.transition(state, {:packet, :pub, 1, duplicate, <<1, 2>>, 2})
    {_state, effects} = allowed(state, pending, 3)
    assert [%Publish{topic: "d", payload: "x"}] = publishes(effects, :sub)
  end

  test "packets held during CONNECT authentication resolve aliases in arrival order" do
    {state, _} = attach(new(), :sub)
    {state, _} = subscribe(state, :sub, [{"h", 0}])
    {state, []} = Broker.transition(state, {:connection_open, :pub, 1, 0})
    connect = %Connect{version: :v5, client_id: "pub", clean_start: true}

    {state, [{:authorize, op, _, _}]} =
      Broker.transition(state, {:packet, :pub, 1, connect, <<16, 0>>, 0})

    first = %Publish{topic: "h", payload: "1", properties: [topic_alias: 1]}
    second = %Publish{topic: "", payload: "2", properties: [topic_alias: 1]}
    {state, []} = Broker.transition(state, {:packet, :pub, 1, first, <<1, 2>>, 0})
    {state, []} = Broker.transition(state, {:packet, :pub, 1, second, <<1, 2>>, 0})

    {state, effects} = Broker.transition(state, {:auth_result, op, 1, {:ok, :user}, 1})
    {_state, effects} = allowed(state, effects, 1)
    assert Enum.map(publishes(effects, :sub), &{&1.topic, &1.payload}) == [{"h", "1"}, {"h", "2"}]
  end

  test "a resolved publication larger than Maximum Packet Size disconnects with 0x95" do
    {state, _} = attach(new(max_packet_size: 64), :pub)
    topic = String.duplicate("t", 40)

    {state, _} =
      packet(state, :pub, %Publish{topic: topic, payload: "", properties: [topic_alias: 1]})

    {_state, effects} =
      packet(state, :pub, %Publish{
        topic: "",
        payload: String.duplicate("p", 40),
        properties: [topic_alias: 1]
      })

    assert disconnected?(effects, :pub, 0x95)
    assert {:close, :pub, 1, :resolved_packet_too_large} in effects
  end

  defp publish_all(state, id, topics) do
    Enum.reduce(topics, {state, []}, fn topic, {state, acc} ->
      {state, effects} = packet(state, id, %Publish{topic: topic, payload: topic})
      {state, acc ++ effects}
    end)
  end

  test "outbound aliases are bounded by the client maximum and the broker cap (MQTT-3.1.2-26/27)" do
    {state, _} = attach(new(max_outbound_topic_aliases: 2), :pub)
    {state, _} = attach(state, :sub, properties: [topic_alias_maximum: 5])
    {state, _} = subscribe(state, :sub, [{"o/#", 0}])
    {_state, effects} = publish_all(state, :pub, ["o/a", "o/a", "o/b", "o/c", "o/c", "o/b"])

    assert Enum.map(publishes(effects, :sub), &{&1.topic, &1.properties[:topic_alias]}) ==
             [{"o/a", 1}, {"", 1}, {"o/b", 2}, {"o/c", nil}, {"o/c", nil}, {"", 2}]
  end

  test "the default broker cap and a client maximum of zero never send aliases" do
    for {options, properties} <- [
          {[], [topic_alias_maximum: 5]},
          {[max_outbound_topic_aliases: 5], []}
        ] do
      {state, _} = attach(new(options), :pub)
      {state, _} = attach(state, :sub, properties: properties)
      {state, _} = subscribe(state, :sub, [{"o/#", 0}])
      {_state, effects} = publish_all(state, :pub, ["o/a", "o/a"])

      assert Enum.map(publishes(effects, :sub), &{&1.topic, &1.properties[:topic_alias]}) ==
               [{"o/a", nil}, {"o/a", nil}]
    end
  end

  test "a reconnect starts a new outbound table and retransmits the full topic" do
    {state, _} = attach(new(max_outbound_topic_aliases: 2), :pub)

    {state, _} =
      attach(state, :sub, properties: [topic_alias_maximum: 2, session_expiry_interval: 60])

    {state, _} = subscribe(state, :sub, [{"r", 1}])
    {state, _} = packet(state, :pub, %Publish{topic: "r", payload: "1", qos: 1, packet_id: 1})
    {state, _} = packet(state, :pub, %Publish{topic: "r", payload: "2", qos: 1, packet_id: 2})
    {state, _} = Broker.transition(state, {:connection_down, :sub, 1, :abnormal, 2})

    {_state, effects} =
      attach_as(state, :sub_again, "sub",
        clean_start: false,
        properties: [topic_alias_maximum: 2, session_expiry_interval: 60]
      )

    assert [
             %Publish{topic: "r", dup: true, properties: first},
             %Publish{topic: "", dup: true, properties: second}
           ] = publishes(effects, :sub_again)

    assert first[:topic_alias] == 1 and second[:topic_alias] == 1
  end

  test "a new alias that would exceed the peer Maximum Packet Size is not assigned" do
    # The limit admits CONNACK/SUBACK and the full-topic PUBLISH, but not the
    # 3 extra bytes of a first alias assignment.
    payload = String.duplicate("a", 100)
    {:ok, bytes} = Packet.encode(%Publish{topic: "s", payload: payload})
    {state, _} = attach(new(max_outbound_topic_aliases: 2), :pub)

    {state, _} =
      attach(state, :sub,
        properties: [topic_alias_maximum: 2, maximum_packet_size: byte_size(bytes)]
      )

    {state, _} = subscribe(state, :sub, [{"s", 0}])
    {state, effects} = packet(state, :pub, %Publish{topic: "s", payload: payload})
    assert [%Publish{topic: "s", properties: []}] = publishes(effects, :sub)
    assert state.connections[:sub].aliases.outbound == %{}
  end

  test "an undeliverable oversized publication records no outbound alias" do
    {state, _} = attach(new(max_outbound_topic_aliases: 2), :pub)

    {state, _} =
      attach(state, :sub, properties: [topic_alias_maximum: 2, maximum_packet_size: 64])

    {state, _} = subscribe(state, :sub, [{"s/#", 0}])

    {state, effects} =
      packet(state, :pub, %Publish{topic: "s/big", payload: String.duplicate("b", 100)})

    assert publishes(effects, :sub) == []
    assert state.connections[:sub].aliases.outbound == %{}

    {state, effects} = publish_all(state, :pub, ["s/small", "s/big"])

    assert Enum.map(publishes(effects, :sub), &{&1.topic, &1.properties[:topic_alias]}) ==
             [{"s/small", 1}, {"s/big", 2}]

    assert state.connections[:sub].aliases.outbound == %{"s/small" => 1, "s/big" => 2}
  end

  test "a quota-suppressed publication records no outbound alias until it is sent" do
    {state, _} = attach(new(max_outbound_topic_aliases: 2), :pub)

    {state, _} =
      attach(state, :sub, properties: [topic_alias_maximum: 2, receive_maximum: 1])

    {state, _} = subscribe(state, :sub, [{"q/#", 1}])

    {state, first} =
      packet(state, :pub, %Publish{topic: "q/a", payload: "1", qos: 1, packet_id: 1})

    {state, held} =
      packet(state, :pub, %Publish{topic: "q/b", payload: "2", qos: 1, packet_id: 2})

    assert [%Publish{topic: "q/a", packet_id: sent_id, properties: [topic_alias: 1]}] =
             publishes(first, :sub)

    assert publishes(held, :sub) == []
    assert state.connections[:sub].aliases.outbound == %{"q/a" => 1}

    {state, released} = packet(state, :sub, %Puback{packet_id: sent_id}, 2)
    assert [%Publish{topic: "q/b", properties: [topic_alias: 2]}] = publishes(released, :sub)
    assert state.connections[:sub].aliases.outbound == %{"q/a" => 1, "q/b" => 2}
  end

  test "a QoS 2 duplicate in alias form after PUBREC is acknowledged, not redelivered" do
    {state, _} = attach(new(), :pub)
    {state, _} = attach(state, :sub)
    {state, _} = subscribe(state, :sub, [{"x/#", 2}])

    first = %Publish{
      topic: "x/q2",
      payload: "once",
      qos: 2,
      packet_id: 7,
      properties: [topic_alias: 3]
    }

    {state, effects} = packet(state, :pub, first)
    assert [%Publish{topic: "x/q2"}] = publishes(effects, :sub)
    assert Enum.any?(effects, &match?({:send, :pub, 1, _, %Pubrec{packet_id: 7}}, &1))
    assert Map.has_key?(state.sessions["pub"].incoming_qos2, 7)

    {state, effects} = packet(state, :pub, %Publish{first | topic: "", dup: true}, 2)
    assert publishes(effects, :sub) == []
    assert Enum.any?(effects, &match?({:send, :pub, 1, _, %Pubrec{packet_id: 7}}, &1))
    assert Map.has_key?(state.connections, :pub)
  end

  test "a client Topic Alias Maximum below the broker cap bounds outbound aliases" do
    {state, _} = attach(new(max_outbound_topic_aliases: 5), :pub)
    {state, _} = attach(state, :sub, properties: [topic_alias_maximum: 1])
    {state, _} = subscribe(state, :sub, [{"o/#", 0}])
    {state, effects} = publish_all(state, :pub, ["o/a", "o/b", "o/a", "o/b"])

    assert Enum.map(publishes(effects, :sub), &{&1.topic, &1.properties[:topic_alias]}) ==
             [{"o/a", 1}, {"o/b", nil}, {"", 1}, {"o/b", nil}]

    assert state.connections[:sub].aliases.outbound == %{"o/a" => 1}
  end

  test "checkpointed QoS 1 messages carry no topic_alias property" do
    {state, _} = attach(new(max_outbound_topic_aliases: 5), :pub)

    {state, _} =
      attach(state, :sub, properties: [topic_alias_maximum: 5, session_expiry_interval: 60])

    {state, _} = subscribe(state, :sub, [{"k/#", 1}])

    {state, _} =
      packet(state, :pub, %Publish{
        topic: "k/a",
        payload: "1",
        qos: 1,
        packet_id: 1,
        properties: [topic_alias: 1]
      })

    {state, _} =
      packet(state, :pub, %Publish{
        topic: "",
        payload: "2",
        qos: 1,
        packet_id: 2,
        properties: [topic_alias: 1]
      })

    {state, _} = Broker.transition(state, {:connection_down, :sub, 1, :abnormal, 2})

    {state, _} =
      packet(state, :pub, %Publish{
        topic: "",
        payload: "3",
        qos: 1,
        packet_id: 3,
        properties: [topic_alias: 1]
      })

    assert {:ok, snapshot} = Checkpoint.capture(state, 3, 100_000)
    deliveries = snapshot.state.sessions["sub"].deliveries
    assert Enum.map(deliveries, & &1.message.payload) == ["1", "2", "3"]

    for %{message: message} <- deliveries do
      assert message.topic == "k/a"
      refute Keyword.has_key?(message.properties, :topic_alias)

      assert {:ok, %Publish{topic: "k/a", properties: []}, <<>>} =
               Packet.decode(message.encoded, :v5)
    end
  end

  # Unreachable from the wire (the stored topic was validated when it was
  # received); a corrupted table must still close one connection, not crash.
  test "an unexpected re-encode failure disconnects with 0x83 instead of crashing" do
    {state, _} = attach(new(), :pub)
    conn = state.connections[:pub]
    aliases = %{conn.aliases | inbound: %{1 => "bad/#"}}
    state = %{state | connections: %{state.connections | pub: %{conn | aliases: aliases}}}

    {state, effects} =
      packet(state, :pub, %Publish{topic: "", payload: "x", properties: [topic_alias: 1]})

    assert disconnected?(effects, :pub, 0x83)
    assert {:close, :pub, 1, :resolved_packet_invalid} in effects
    refute Map.has_key?(state.connections, :pub)
  end
end
