defmodule GravitonMQ.MQTT.ClientAliasTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.MQTT.Client
  alias GravitonMQ.MQTT.Packet.{Connack, Connect, Disconnect, Publish}

  defp connect(properties \\ []),
    do: %Connect{
      version: :v5,
      client_id: "client",
      clean_start: true,
      keep_alive: 2,
      properties: properties
    }

  defp online(connack_properties, options \\ []) do
    {:ok, state} = Client.new(connect(Keyword.get(options, :connect_properties, [])), options)
    {state, [{:open, 1}]} = Client.transition(state, :start)

    {state, [{:send, %Connect{} = sent, :connect}]} =
      Client.transition(state, {:transport_up, 1, 0})

    {state, _effects} =
      Client.transition(
        state,
        {:packet, 1,
         %Connack{
           version: :v5,
           session_present: false,
           reason_code: 0,
           properties: connack_properties
         }, 1}
      )

    {state, sent}
  end

  defp deliver(state, packet, now) do
    {state, effects} = Client.transition(state, {:packet, state.generation, packet, now})
    {state, for({:notify, {:delivery, _, %Publish{} = p}} <- effects, do: p), effects}
  end

  defp sends(effects), do: for({:send, %Publish{} = p, _} <- effects, do: p)

  test "the inbound maximum is an option advertised in CONNECT; the default is zero" do
    {_state, sent} = online([])
    assert sent.properties[:topic_alias_maximum] == 0
    {_state, sent} = online([], topic_alias_maximum: 3)
    assert sent.properties[:topic_alias_maximum] == 3
    assert {:ok, _} = Client.new(connect(topic_alias_maximum: 1))
    assert {:error, :invalid_options} = Client.new(connect(), topic_alias_maximum: 65_536)
    assert {:error, :invalid_options} = Client.new(connect(), max_outbound_topic_aliases: -1)
  end

  test "inbound aliases resolve before delivery and the property is stripped (Q15)" do
    {state, _} = online([], topic_alias_maximum: 2)
    {state, _} = Client.transition(state, {:demand, 5, 2})

    {state, [first], _} =
      deliver(state, %Publish{topic: "a", payload: "1", properties: [topic_alias: 1]}, 3)

    {_state, [second], _} =
      deliver(state, %Publish{topic: "", payload: "2", properties: [topic_alias: 1]}, 4)

    assert {first.topic, first.properties} == {"a", []}
    assert {second.topic, second.properties} == {"a", []}
  end

  test "out-of-range aliases terminate with 0x94 and unknown aliases with 0x82" do
    for {packet, code} <- [
          {%Publish{topic: "a", payload: "x", properties: [topic_alias: 3]}, 0x94},
          {%Publish{topic: "", payload: "x", properties: [topic_alias: 2]}, 0x82}
        ] do
      {state, _} = online([], topic_alias_maximum: 2)
      {state, _, effects} = deliver(state, packet, 3)
      assert state.phase == :stopped
      assert {:send, %Disconnect{reason_code: code}, :disconnect} in effects
    end
  end

  test "inbound mappings are reset with each Network Connection (MQTT-3.3.2-7)" do
    {state, _} =
      online([],
        topic_alias_maximum: 2,
        reconnect_max: 1,
        connect_properties: [session_expiry_interval: 60]
      )

    {state, _, _} =
      deliver(state, %Publish{topic: "a", payload: "1", properties: [topic_alias: 1]}, 3)

    {state, _} = Client.transition(state, {:transport_down, 1, :closed, 4})
    {state, _} = Client.transition(state, {:retry, 5})
    {state, _} = Client.transition(state, {:transport_up, 2, 6})

    {state, _} =
      Client.transition(
        state,
        {:packet, 2, %Connack{version: :v5, session_present: true, reason_code: 0}, 7}
      )

    {state, _, effects} =
      deliver(state, %Publish{topic: "", payload: "2", properties: [topic_alias: 1]}, 8)

    assert state.phase == :stopped
    assert {:send, %Disconnect{reason_code: 0x82}, :disconnect} in effects
  end

  test "outbound aliases are bounded by min(CONNACK maximum, client cap) (MQTT-3.2.2-17/18)" do
    {state, _} = online([topic_alias_maximum: 2], max_outbound_topic_aliases: 5)

    {_state, effects} =
      Enum.reduce(Enum.with_index(["a", "a", "b", "c", "c"]), {state, []}, fn {topic, index},
                                                                              {state, acc} ->
        {state, effects} =
          Client.transition(
            state,
            {:publish, {:op, index}, %Publish{topic: topic, payload: "x"}, 10 + index}
          )

        {state, acc ++ effects}
      end)

    assert Enum.map(sends(effects), &{&1.topic, &1.properties[:topic_alias]}) ==
             [{"a", 1}, {"", 1}, {"b", 2}, {"c", nil}, {"c", nil}]
  end

  test "a peer maximum of zero, the default cap and caller aliases never alias" do
    for {connack, options} <- [
          {[], [max_outbound_topic_aliases: 5]},
          {[topic_alias_maximum: 5], []}
        ] do
      {state, _} = online(connack, options)

      {state, first} =
        Client.transition(state, {:publish, :one, %Publish{topic: "a", payload: "x"}, 10})

      {_state, second} =
        Client.transition(state, {:publish, :two, %Publish{topic: "a", payload: "x"}, 11})

      assert Enum.map(sends(first ++ second), & &1.topic) == ["a", "a"]
    end

    {state, _} = online([topic_alias_maximum: 5], max_outbound_topic_aliases: 5)

    assert {_state, [{:notify, {:operation, :op, {:error, :topic_aliases_unsupported}}}]} =
             Client.transition(
               state,
               {:publish, :op, %Publish{topic: "a", payload: "x", properties: [topic_alias: 1]},
                10}
             )
  end

  test "stored outbound publications keep the full topic and resend it after reconnect" do
    {state, _} =
      online([topic_alias_maximum: 2],
        max_outbound_topic_aliases: 2,
        reconnect_max: 1,
        connect_properties: [session_expiry_interval: 60]
      )

    {state, _} =
      Client.transition(state, {:publish, :one, %Publish{topic: "a", payload: "1", qos: 1}, 10})

    {state, _} = Client.transition(state, {:sent, 1, {:operation, :one}, :ok, 10})

    {state, effects} =
      Client.transition(state, {:publish, :two, %Publish{topic: "a", payload: "2", qos: 1}, 11})

    assert [%Publish{topic: ""}] = sends(effects)
    assert state.outbound[:two].packet.topic == "a"
    refute Keyword.has_key?(state.outbound[:two].packet.properties, :topic_alias)
    {state, _} = Client.transition(state, {:sent, 1, {:operation, :two}, :ok, 11})
    {state, _} = Client.transition(state, {:transport_down, 1, :closed, 12})
    {state, _} = Client.transition(state, {:retry, 13})
    {state, _} = Client.transition(state, {:transport_up, 2, 14})

    {_state, effects} =
      Client.transition(
        state,
        {:packet, 2,
         %Connack{
           version: :v5,
           session_present: true,
           reason_code: 0,
           properties: [topic_alias_maximum: 2]
         }, 15}
      )

    assert [%Publish{topic: "a", dup: true} | _] = sends(effects)
  end
end
