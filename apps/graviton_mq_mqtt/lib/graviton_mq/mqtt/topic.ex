defmodule GravitonMQ.MQTT.Topic do
  @moduledoc """
  Pure MQTT 5.0 topic validation and ordinary subscription matching.

  Names and filters are nonempty UTF-8 strings of at most 65,535 encoded
  bytes, without NUL. Topic levels, including empty levels, are compared
  exactly without Unicode normalization. Shared subscriptions are outside
  this matching profile and are explicitly rejected.
  """

  alias GravitonMQ.MQTT.Codec.{Operational, Wire}

  @spec valid_name?(term()) :: boolean()
  def valid_name?(name) when is_binary(name) and byte_size(name) in 1..65_535,
    do: Wire.valid_utf8?(name) and Wire.topic_name?(name)

  def valid_name?(_name), do: false

  @spec valid_filter?(term()) :: boolean()
  def valid_filter?(<<"$share/", _::binary>>), do: false

  def valid_filter?(filter) when is_binary(filter) and byte_size(filter) in 1..65_535,
    do: Operational.topic_filter?(filter)

  def valid_filter?(_filter), do: false

  @spec matches?(term(), term()) :: boolean()
  def matches?(filter, topic) do
    valid_filter?(filter) and valid_name?(topic) and
      not system_wildcard?(filter, topic) and
      match_levels(String.split(filter, "/"), String.split(topic, "/"))
  end

  # MQTT-4.7.2-1 excludes $ topics only when the first filter level is a wildcard.
  defp system_wildcard?(<<wildcard, _::binary>>, <<"$", _::binary>>)
       when wildcard in [?+, ?#],
       do: true

  defp system_wildcard?(_filter, _topic), do: false

  defp match_levels(["#"], _topics), do: true
  defp match_levels([], []), do: true
  defp match_levels(["+" | filters], [_topic | topics]), do: match_levels(filters, topics)
  defp match_levels([level | filters], [level | topics]), do: match_levels(filters, topics)
  defp match_levels(_filters, _topics), do: false
end
