defmodule GravitonMQ.MQTT.Codec.Operational do
  @moduledoc false

  import Bitwise

  alias GravitonMQ.MQTT.Codec.{Properties, Wire}
  alias GravitonMQ.MQTT.Packet, as: Data

  @puback_codes [0, 0x10, 0x80, 0x83, 0x87, 0x90, 0x91, 0x97, 0x99]
  @suback_codes [0, 1, 2, 0x80, 0x83, 0x87, 0x8F, 0x91, 0x97, 0x9E, 0xA1, 0xA2]
  @unsuback_codes [0, 0x11, 0x80, 0x83, 0x87, 0x8F, 0x91]
  @disconnect_codes [
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
  ]
  @modules [
    Data.Publish,
    Data.Puback,
    Data.Pubrec,
    Data.Pubrel,
    Data.Pubcomp,
    Data.Subscribe,
    Data.Suback,
    Data.Unsubscribe,
    Data.Unsuback,
    Data.Pingreq,
    Data.Pingresp,
    Data.Disconnect
  ]

  def packet?(%{__struct__: module}), do: module in @modules
  def packet?(_), do: false

  def decode(_frame, :auto, context), do: Wire.error(context, :unsupported, :version_required)

  def decode(_frame, :v3_1_1, context),
    do: Wire.error(context, :unsupported, :unsupported_packet_type)

  def decode(%{type: 3, flags: flags, body: bytes}, :v5, context) do
    qos = flags >>> 1 &&& 3

    with {:ok, topic, rest} <- Wire.decode(:utf8, bytes, context, :topic),
         {:ok, packet_id, rest} <- decode_publish_id(rest, qos, context),
         {:ok, properties, payload} <- Properties.decode(rest, :v5, :publish, context),
         :ok <- publish_topic(topic, properties, context) do
      {:ok,
       %Data.Publish{
         topic: topic,
         payload: payload,
         packet_id: packet_id,
         qos: qos,
         dup: (flags &&& 8) != 0,
         retain: (flags &&& 1) != 0,
         properties: properties
       }}
    end
  end

  def decode(%{type: type, body: bytes}, :v5, context) when type in [4, 5, 6, 7] do
    {scope, module} = acknowledgement(type)

    with {:ok, packet_id, rest} <- decode_id(bytes, context),
         {:ok, reason_code, properties} <- decode_reason_tail(rest, scope, context) do
      {:ok,
       struct!(module, packet_id: packet_id, reason_code: reason_code, properties: properties)}
    end
  end

  def decode(%{type: 8, body: bytes}, :v5, context) do
    with {:ok, packet_id, rest} <- decode_id(bytes, context),
         {:ok, properties, rest} <- Properties.decode(rest, :v5, :subscribe, context),
         {:ok, subscriptions} <- decode_subscriptions(rest, context, []) do
      {:ok,
       %Data.Subscribe{packet_id: packet_id, properties: properties, subscriptions: subscriptions}}
    end
  end

  def decode(%{type: type, body: bytes}, :v5, context) when type in [9, 11] do
    scope = if type == 9, do: :suback, else: :unsuback
    module = if type == 9, do: Data.Suback, else: Data.Unsuback

    with {:ok, packet_id, rest} <- decode_id(bytes, context),
         {:ok, properties, rest} <- Properties.decode(rest, :v5, scope, context),
         {:ok, reason_codes} <- decode_reason_codes(rest, scope, context) do
      {:ok,
       struct!(module, packet_id: packet_id, properties: properties, reason_codes: reason_codes)}
    end
  end

  def decode(%{type: 10, body: bytes}, :v5, context) do
    with {:ok, packet_id, rest} <- decode_id(bytes, context),
         {:ok, properties, rest} <- Properties.decode(rest, :v5, :unsubscribe, context),
         {:ok, filters} <- decode_filters(rest, context, []) do
      {:ok, %Data.Unsubscribe{packet_id: packet_id, properties: properties, filters: filters}}
    end
  end

  def decode(%{type: 12, body: <<>>}, :v5, _context), do: {:ok, %Data.Pingreq{}}
  def decode(%{type: 13, body: <<>>}, :v5, _context), do: {:ok, %Data.Pingresp{}}

  def decode(%{type: 14, body: bytes}, :v5, context) do
    with {:ok, reason_code, properties} <- decode_reason_tail(bytes, :disconnect, context),
         do: {:ok, %Data.Disconnect{reason_code: reason_code, properties: properties}}
  end

  def decode(%{type: type}, :v5, context) when type in [12, 13],
    do: Wire.error(context, :malformed, :trailing_packet_data)

  def decode(_frame, _version, context),
    do: Wire.error(context, :unsupported, :unsupported_packet_type)

  def encode(%{version: version}, context) when version != :v5,
    do: Wire.error(context, :invalid_value, :invalid_version)

  def encode(%Data.Publish{} = packet, context) do
    with :ok <- publish_flags(packet, context),
         :ok <- publish_topic(packet.topic, packet.properties, context),
         :ok <- binary_payload(packet.payload, context),
         {:ok, topic} <- Wire.encode(:utf8, packet.topic, context, :topic),
         {:ok, packet_id} <- encode_publish_id(packet.packet_id, packet.qos, context),
         {:ok, properties} <- Properties.encode(packet.properties, :v5, :publish, context) do
      flags = flag(packet.dup, 8) ||| packet.qos <<< 1 ||| flag(packet.retain, 1)
      {:ok, 3, flags, [topic, packet_id, properties, packet.payload]}
    end
  end

  def encode(%{__struct__: module} = packet, context)
      when module in [Data.Puback, Data.Pubrec, Data.Pubrel, Data.Pubcomp] do
    {type, scope, flags} =
      case module do
        Data.Puback -> {4, :puback, 0}
        Data.Pubrec -> {5, :pubrec, 0}
        Data.Pubrel -> {6, :pubrel, 2}
        Data.Pubcomp -> {7, :pubcomp, 0}
      end

    with {:ok, packet_id} <- encode_id(packet.packet_id, context),
         {:ok, tail} <- encode_reason_tail(packet.reason_code, packet.properties, scope, context),
         do: {:ok, type, flags, [packet_id, tail]}
  end

  def encode(%Data.Subscribe{} = packet, context) do
    with {:ok, packet_id} <- encode_id(packet.packet_id, context),
         {:ok, properties} <- Properties.encode(packet.properties, :v5, :subscribe, context),
         {:ok, subscriptions} <- encode_subscriptions(packet.subscriptions, context, [], 0) do
      {:ok, 8, 2, [packet_id, properties, subscriptions]}
    end
  end

  def encode(%Data.Suback{} = packet, context), do: encode_ack(packet, :suback, 9, context)
  def encode(%Data.Unsuback{} = packet, context), do: encode_ack(packet, :unsuback, 11, context)

  def encode(%Data.Unsubscribe{} = packet, context) do
    with {:ok, packet_id} <- encode_id(packet.packet_id, context),
         {:ok, properties} <- Properties.encode(packet.properties, :v5, :unsubscribe, context),
         {:ok, filters} <- encode_filters(packet.filters, context, [], 0) do
      {:ok, 10, 2, [packet_id, properties, filters]}
    end
  end

  def encode(%Data.Pingreq{}, _context), do: {:ok, 12, 0, <<>>}
  def encode(%Data.Pingresp{}, _context), do: {:ok, 13, 0, <<>>}

  def encode(%Data.Disconnect{} = packet, context) do
    with {:ok, tail} <-
           encode_reason_tail(packet.reason_code, packet.properties, :disconnect, context),
         do: {:ok, 14, 0, tail}
  end

  defp acknowledgement(4), do: {:puback, Data.Puback}
  defp acknowledgement(5), do: {:pubrec, Data.Pubrec}
  defp acknowledgement(6), do: {:pubrel, Data.Pubrel}
  defp acknowledgement(7), do: {:pubcomp, Data.Pubcomp}

  defp encode_ack(packet, scope, type, context) do
    with {:ok, packet_id} <- encode_id(packet.packet_id, context),
         {:ok, properties} <- Properties.encode(packet.properties, :v5, scope, context),
         {:ok, codes} <- encode_reason_codes(packet.reason_codes, scope, context) do
      {:ok, type, 0, [packet_id, properties, codes]}
    end
  end

  defp decode_id(bytes, context) do
    with {:ok, packet_id, rest} <- Wire.decode(:u16, bytes, context, :packet_id),
         :ok <- valid_id(packet_id, context),
         do: {:ok, packet_id, rest}
  end

  defp encode_id(packet_id, context) do
    with :ok <- valid_id(packet_id, context),
         do: Wire.encode(:u16, packet_id, context, :packet_id)
  end

  defp valid_id(id, _context) when is_integer(id) and id in 1..65_535, do: :ok

  defp valid_id(_id, context),
    do: Wire.error(context, Wire.value_class(context), :invalid_packet_identifier)

  defp decode_publish_id(bytes, 0, _context), do: {:ok, nil, bytes}
  defp decode_publish_id(bytes, _qos, context), do: decode_id(bytes, context)
  defp encode_publish_id(nil, 0, _context), do: {:ok, <<>>}

  defp encode_publish_id(_id, 0, context),
    do: Wire.error(context, :invalid_value, :unexpected_packet_identifier)

  defp encode_publish_id(id, _qos, context), do: encode_id(id, context)

  defp decode_reason_tail(<<>>, _scope, _context), do: {:ok, 0, []}

  defp decode_reason_tail(<<code>>, scope, context) do
    with :ok <- reason_code(code, scope, context), do: {:ok, code, []}
  end

  defp decode_reason_tail(<<code, bytes::binary>>, scope, context) do
    with :ok <- reason_code(code, scope, context),
         {:ok, properties, <<>>} <- Properties.decode(bytes, :v5, scope, context) do
      {:ok, code, properties}
    else
      {:ok, _properties, _rest} -> Wire.error(context, :malformed, :trailing_packet_data)
      {:error, _} = error -> error
    end
  end

  defp encode_reason_tail(code, properties, scope, context) do
    with :ok <- reason_code(code, scope, context),
         {:ok, encoded} <- Properties.encode(properties, :v5, scope, context) do
      cond do
        code == 0 and properties == [] -> {:ok, <<>>}
        properties == [] -> {:ok, <<code>>}
        true -> {:ok, [<<code>>, encoded]}
      end
    end
  end

  defp decode_reason_codes(<<>>, _scope, context),
    do: Wire.error(context, :malformed, :empty_reason_codes)

  defp decode_reason_codes(bytes, scope, context) do
    codes = :binary.bin_to_list(bytes)
    with :ok <- validate_codes(codes, scope, context), do: {:ok, codes}
  end

  defp encode_reason_codes(codes, scope, context) when is_list(codes) and codes != [] do
    with :ok <- validate_codes(codes, scope, context),
         :ok <- size_limit(length(codes), context),
         do: {:ok, :erlang.list_to_binary(codes)}
  end

  defp encode_reason_codes(_codes, _scope, context),
    do: Wire.error(context, :invalid_value, :empty_reason_codes)

  defp validate_codes([], _scope, _context), do: :ok

  defp validate_codes([code | rest], scope, context) do
    with :ok <- reason_code(code, scope, context), do: validate_codes(rest, scope, context)
  end

  defp validate_codes(_codes, _scope, context),
    do: Wire.error(context, Wire.value_class(context), :invalid_reason_code)

  defp reason_code(code, scope, context) do
    valid? =
      is_integer(code) and
        case scope do
          scope when scope in [:puback, :pubrec] -> code in @puback_codes
          scope when scope in [:pubrel, :pubcomp] -> code in [0, 0x92]
          :suback -> code in @suback_codes
          :unsuback -> code in @unsuback_codes
          :disconnect -> code in @disconnect_codes
        end

    if valid?, do: :ok, else: Wire.error(context, Wire.value_class(context), :invalid_reason_code)
  end

  defp decode_subscriptions(<<>>, context, []),
    do: Wire.error(context, :malformed, :empty_subscriptions)

  defp decode_subscriptions(<<>>, _context, items), do: {:ok, Enum.reverse(items)}

  defp decode_subscriptions(bytes, context, items) do
    with {:ok, filter, rest} <- Wire.decode(:utf8, bytes, context, :topic_filter),
         :ok <- valid_filter(filter, context),
         {:ok, flags, rest} <- Wire.decode(:u8, rest, context, :subscription_options),
         {:ok, options} <- decode_options(flags, filter, context) do
      decode_subscriptions(rest, context, [{filter, options} | items])
    end
  end

  defp encode_subscriptions([], context, [], _size),
    do: Wire.error(context, :invalid_value, :empty_subscriptions)

  defp encode_subscriptions([], _context, parts, _size), do: {:ok, Enum.reverse(parts)}

  defp encode_subscriptions([{filter, options} | rest], context, parts, size) do
    with :ok <- valid_filter(filter, context),
         {:ok, encoded} <- Wire.encode(:utf8, filter, context, :topic_filter),
         {:ok, flags} <- encode_options(options, filter, context),
         new_size = size + byte_size(encoded) + 1,
         :ok <- size_limit(new_size, context) do
      encode_subscriptions(rest, context, [[encoded, <<flags>>] | parts], new_size)
    end
  end

  defp encode_subscriptions(_items, context, _parts, _size),
    do: Wire.error(context, :invalid_value, :invalid_subscriptions)

  defp decode_options(flags, filter, context) do
    qos = flags &&& 3
    retain_handling = flags >>> 4 &&& 3
    no_local = (flags &&& 4) != 0

    if flags >>> 6 != 0 or qos == 3 or retain_handling == 3 or (no_local and shared?(filter)) do
      Wire.error(context, Wire.value_class(context), :invalid_subscription_options)
    else
      {:ok,
       %{
         qos: qos,
         no_local: no_local,
         retain_as_published: (flags &&& 8) != 0,
         retain_handling: retain_handling
       }}
    end
  end

  defp encode_options(qos, filter, context) when is_integer(qos),
    do: encode_options(%{qos: qos}, filter, context)

  defp encode_options(options, filter, context) when is_map(options) do
    qos = Map.get(options, :qos, 0)
    no_local = Map.get(options, :no_local, false)
    rap = Map.get(options, :retain_as_published, false)
    rh = Map.get(options, :retain_handling, 0)

    if is_integer(qos) and qos in 0..2 and is_boolean(no_local) and is_boolean(rap) and
         is_integer(rh) and rh in 0..2 and not (no_local and shared?(filter)) and
         Enum.all?(
           Map.keys(options),
           &(&1 in [:qos, :no_local, :retain_as_published, :retain_handling])
         ) do
      {:ok, qos ||| flag(no_local, 4) ||| flag(rap, 8) ||| rh <<< 4}
    else
      Wire.error(context, :invalid_value, :invalid_subscription_options)
    end
  end

  defp encode_options(_options, _filter, context),
    do: Wire.error(context, :invalid_value, :invalid_subscription_options)

  defp decode_filters(<<>>, context, []), do: Wire.error(context, :malformed, :empty_filters)
  defp decode_filters(<<>>, _context, items), do: {:ok, Enum.reverse(items)}

  defp decode_filters(bytes, context, items) do
    with {:ok, filter, rest} <- Wire.decode(:utf8, bytes, context, :topic_filter),
         :ok <- valid_filter(filter, context),
         do: decode_filters(rest, context, [filter | items])
  end

  defp encode_filters([], context, [], _size),
    do: Wire.error(context, :invalid_value, :empty_filters)

  defp encode_filters([], _context, parts, _size), do: {:ok, Enum.reverse(parts)}

  defp encode_filters([filter | rest], context, parts, size) do
    with :ok <- valid_filter(filter, context),
         {:ok, encoded} <- Wire.encode(:utf8, filter, context, :topic_filter),
         new_size = size + byte_size(encoded),
         :ok <- size_limit(new_size, context),
         do: encode_filters(rest, context, [encoded | parts], new_size)
  end

  defp encode_filters(_filters, context, _parts, _size),
    do: Wire.error(context, :invalid_value, :invalid_filters)

  defp publish_flags(%{qos: qos, dup: dup, retain: retain}, _context)
       when is_integer(qos) and qos in 0..2 and is_boolean(dup) and is_boolean(retain) and
              (qos != 0 or dup == false),
       do: :ok

  defp publish_flags(_packet, context),
    do: Wire.error(context, :invalid_value, :invalid_publish_flags)

  defp publish_topic(topic, properties, context) when is_binary(topic) and is_list(properties) do
    alias? =
      Enum.any?(properties, fn
        {:topic_alias, value} when is_integer(value) and value > 0 -> true
        _ -> false
      end)

    if Wire.valid_utf8?(topic) and (Wire.topic_name?(topic) or (topic == "" and alias?)),
      do: :ok,
      else: Wire.error(context, Wire.value_class(context), :invalid_topic_name)
  end

  defp publish_topic(_topic, _properties, context),
    do: Wire.error(context, Wire.value_class(context), :invalid_topic_name)

  defp binary_payload(payload, _context) when is_binary(payload), do: :ok

  defp binary_payload(_payload, context),
    do: Wire.error(context, :invalid_value, :invalid_payload)

  defp valid_filter(filter, context) do
    if topic_filter?(filter),
      do: :ok,
      else: Wire.error(context, Wire.value_class(context), :invalid_topic_filter)
  end

  @doc false
  def topic_filter?(filter) when is_binary(filter) and filter != "" do
    Wire.valid_utf8?(filter) and
      case filter do
        <<"$share/", shared::binary>> ->
          case String.split(shared, "/", parts: 2) do
            [group, actual] ->
              group != "" and :binary.match(group, ["+", "#"]) == :nomatch and
                ordinary_filter?(actual)

            _ ->
              false
          end

        _ ->
          ordinary_filter?(filter)
      end
  end

  def topic_filter?(_filter), do: false

  defp ordinary_filter?(""), do: false

  defp ordinary_filter?(filter) do
    levels = String.split(filter, "/")
    last_index = length(levels) - 1

    levels
    |> Enum.with_index()
    |> Enum.all?(fn {level, index} ->
      case level do
        "#" -> index == last_index
        "+" -> true
        _ -> :binary.match(level, ["+", "#"]) == :nomatch
      end
    end)
  end

  defp shared?(<<"$share/", _::binary>>), do: true
  defp shared?(_filter), do: false

  defp size_limit(size, context) do
    if size > context.limits.max_packet_size,
      do: Wire.error(context, :limit_exceeded, :packet_too_large),
      else: :ok
  end

  defp flag(true, bits), do: bits
  defp flag(false, _bits), do: 0
end
