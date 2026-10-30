defmodule GravitonMQ.MQTT.TopicTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.MQTT.Topic

  test "topic names retain empty levels, Unicode and system names" do
    for name <- ["a", "/", "a//b/", "$SYS/status", "$share/group/topic", "温度/室内", "é"] do
      assert Topic.valid_name?(name), inspect(name)
    end
  end

  test "topic names reject empty, nonbinary, malformed UTF8, NUL and wildcard inputs" do
    for name <- ["", nil, :topic, 42, [97], <<255>>, <<0>>, "a\0b", "a/+", "a/#", "a+b", "a#b"] do
      refute Topic.valid_name?(name), inspect(name)
    end
  end

  test "ordinary filters reuse the wire grammar and reject shared subscriptions" do
    for filter <- ["#", "+", "/", "/+", "a/#", "a/+/c", "a//b/", "$SYS/#", "温度/+"] do
      assert Topic.valid_filter?(filter), inspect(filter)
    end

    for filter <- [
          "",
          nil,
          :filter,
          42,
          [97],
          <<255>>,
          <<0>>,
          "a\0b",
          "a+",
          "a/+b",
          "a#",
          "a/#/b",
          "a/##",
          "$share/g/a",
          "$share/g/#",
          "$share//a"
        ] do
      refute Topic.valid_filter?(filter), inspect(filter)
    end
  end

  test "name and filter limits count encoded bytes, including multibyte Unicode" do
    for value <- [String.duplicate("a", 65_535), String.duplicate("é", 32_767) <> "a"] do
      assert Topic.valid_name?(value)
      assert Topic.valid_filter?(value)
      assert Topic.matches?(value, value)
    end

    for value <- [String.duplicate("a", 65_536), String.duplicate("é", 32_768)] do
      refute Topic.valid_name?(value)
      refute Topic.valid_filter?(value)
      refute Topic.matches?("#", value)
      refute Topic.matches?(value, "a")
    end
  end

  # MQTT 5.0 section 4.7 examples: + matches exactly one level; / creates
  # distinct levels, and a trailing # includes the parent topic itself.
  test "single-level wildcard vectors include empty levels but never omit a level" do
    for {filter, topic} <- [
          {"sport/tennis/+", "sport/tennis/player1"},
          {"sport/+", "sport/"},
          {"+/+", "/"},
          {"a/+/b", "a//b"},
          {"+", "a"},
          {"+/a", "/a"}
        ] do
      assert Topic.matches?(filter, topic), inspect({filter, topic})
    end

    for {filter, topic} <- [
          {"sport/+", "sport"},
          {"sport/tennis/+", "sport/tennis/player1/ranking"},
          {"+", "/"},
          {"a/+/b", "a/b"},
          {"+/a", "a"}
        ] do
      refute Topic.matches?(filter, topic), inspect({filter, topic})
    end
  end

  test "multi-level wildcard vectors include the parent and arbitrarily empty levels" do
    for topic <- ["sport", "sport/", "sport/tennis", "sport///"] do
      assert Topic.matches?("sport/#", topic)
    end

    for topic <- ["a", "/", "a//", "温度/室内"] do
      assert Topic.matches?("#", topic)
    end

    assert Topic.matches?("a/+/#", "a/b")
    assert Topic.matches?("a/+/#", "a/")
    refute Topic.matches?("a/+/#", "a")
    refute Topic.matches?("sport/#", "sports")
    refute Topic.matches?("sport/#", "x/sport")
  end

  test "root wildcards exclude system topics while an explicit system level can match" do
    for filter <- ["#", "+", "+/#", "+/status"] do
      refute Topic.matches?(filter, "$SYS/status")
      refute Topic.matches?(filter, "$SYS")
    end

    assert Topic.matches?("$SYS/#", "$SYS")
    assert Topic.matches?("$SYS/#", "$SYS/status")
    assert Topic.matches?("$SYS/+", "$SYS/status")
    assert Topic.matches?("a/+", "a/$SYS")
    assert Topic.matches?("a/#", "a/$SYS/status")
  end

  test "matching is exact and does not normalize Unicode or case" do
    assert Topic.matches?("温度/+", "温度/室内")
    assert Topic.matches?("é", "é")
    refute Topic.matches?("é", "é")
    refute Topic.matches?("sport", "Sport")
    refute Topic.matches?("sport", "sport/")
    refute Topic.matches?("/sport", "sport")
    refute Topic.matches?("sport//tennis", "sport/tennis")
  end

  test "matching rejects invalid names and filters before wildcard evaluation" do
    for {filter, topic} <- [
          {"#", ""},
          {"#", "a/+"},
          {"#", <<255>>},
          {"#", nil},
          {nil, "a"},
          {"a/#/b", "a/b"},
          {"$share/g/#", "a"},
          {"", "a"},
          {"#", "a\0b"}
        ] do
      refute Topic.matches?(filter, topic), inspect({filter, topic})
    end
  end
end
