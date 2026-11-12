defmodule GravitonMQ.MQTT.TopicAliasTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.MQTT.Packet.Publish
  alias GravitonMQ.MQTT.TopicAlias

  defp publish(topic, properties \\ []),
    do: %Publish{topic: topic, payload: "x", properties: properties}

  test "an absent alias leaves the packet and table unchanged" do
    table = TopicAlias.new(10, 0)
    packet = publish("a", user_property: {"k", "v"})
    assert {:ok, ^table, ^packet} = TopicAlias.resolve(table, packet)
  end

  test "alias zero or above the advertised maximum is invalid (U-055)" do
    for {maximum, value} <- [{10, 0}, {10, 11}, {0, 1}, {1, 2}] do
      assert {:error, :topic_alias_invalid} =
               TopicAlias.resolve(TopicAlias.new(maximum, 0), publish("a", topic_alias: value))
    end
  end

  test "an empty topic with an unmapped alias is unknown (U-056)" do
    assert {:error, :topic_alias_unknown} =
             TopicAlias.resolve(TopicAlias.new(10, 0), publish("", topic_alias: 3))
  end

  test "a topic with an alias sets then replaces the mapping; the property is removed" do
    table = TopicAlias.new(10, 0)

    assert {:ok, table, %Publish{topic: "a", properties: [user_property: {"k", "v"}]}} =
             TopicAlias.resolve(table, publish("a", topic_alias: 1, user_property: {"k", "v"}))

    assert {:ok, table, %Publish{topic: "a", properties: []}} =
             TopicAlias.resolve(table, publish("", topic_alias: 1))

    assert {:ok, table, %Publish{topic: "b"}} =
             TopicAlias.resolve(table, publish("b", topic_alias: 1))

    assert {:ok, _table, %Publish{topic: "b", properties: []}} =
             TopicAlias.resolve(table, publish("", topic_alias: 1))
  end

  test "every alias from one to the advertised maximum is accepted (MQTT-3.3.2-10/12)" do
    table =
      Enum.reduce(1..5, TopicAlias.new(5, 0), fn value, table ->
        assert {:ok, table, _} =
                 TopicAlias.resolve(table, publish("t/#{value}", topic_alias: value))

        table
      end)

    assert map_size(table.inbound) == 5

    assert {:ok, _, %Publish{topic: "z"}} =
             TopicAlias.resolve(TopicAlias.new(65_535, 0), publish("z", topic_alias: 65_535))
  end

  test "outbound assignment is stable first-N and never exceeds its maximum (MQTT-3.3.2-9/11)" do
    table = TopicAlias.new(0, 2)
    {table, first} = TopicAlias.assign(table, publish("a"))
    assert {first.topic, first.properties} == {"a", [topic_alias: 1]}
    {table, again} = TopicAlias.assign(table, publish("a"))
    assert {again.topic, again.properties} == {"", [topic_alias: 1]}
    {table, second} = TopicAlias.assign(table, publish("b"))
    assert {second.topic, second.properties} == {"b", [topic_alias: 2]}
    {table, full} = TopicAlias.assign(table, publish("c"))
    assert {full.topic, full.properties} == {"c", []}
    {_table, reused} = TopicAlias.assign(table, publish("b"))
    assert {reused.topic, reused.properties} == {"", [topic_alias: 2]}
  end

  test "a zero outbound maximum or an existing alias property is left untouched" do
    table = TopicAlias.new(10, 0)
    packet = publish("a")
    assert {^table, ^packet} = TopicAlias.assign(table, packet)

    table = TopicAlias.new(0, 5)
    aliased = publish("a", topic_alias: 4)
    assert {^table, ^aliased} = TopicAlias.assign(table, aliased)
  end
end
