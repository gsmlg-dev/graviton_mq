defmodule GravitonMQ.MQTT.BrokerAliasTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.MQTT.{Broker, Broker.Checkpoint}
  alias GravitonMQ.MQTT.Codec.Packet
  alias GravitonMQ.MQTT.Packet.{Connack, Connect, Disconnect, Publish, Puback, Subscribe}

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

  defp attach_as(state, id, client_id, options \\ []) do
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
    assert {:error, :invalid_options} = Broker.new(max_alias_bytes: 1)
    assert {:ok, _} = Broker.new(topic_alias_maximum: 0, max_alias_bytes: 0)

    bounded = [max_connections: 1, topic_alias_maximum: 1, max_packet_size: 100]
    assert {:ok, _} = Broker.new(bounded ++ [max_alias_bytes: 100])
    assert {:error, :invalid_options} = Broker.new(bounded ++ [max_alias_bytes: 99])
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
  end
end
