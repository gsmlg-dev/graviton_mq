defmodule GravitonMQ.MQTT.Codec.Connect do
  @moduledoc false

  import Bitwise

  alias GravitonMQ.MQTT.Codec.{Properties, Wire}
  alias GravitonMQ.MQTT.Packet.{Connect, Will}

  def decode(bytes, expected_version, context) do
    with {:ok, name, rest} <- Wire.decode(:utf8, bytes, context, :protocol_name),
         :ok <- protocol_name(name, context),
         {:ok, level, rest} <- Wire.decode(:u8, rest, context, :protocol_level),
         {:ok, version} <- version(level, context),
         :ok <- expected_version(version, expected_version, context),
         {:ok, flags, rest} <- Wire.decode(:u8, rest, context, :connect_flags),
         :ok <- flags(flags, version, context),
         {:ok, keep_alive, rest} <- Wire.decode(:u16, rest, context, :keep_alive),
         {:ok, properties, rest} <- Properties.decode(rest, version, :connect, context),
         {:ok, client_id, rest} <- Wire.decode(:utf8, rest, context, :client_id),
         clean_start = (flags &&& 2) != 0,
         :ok <- client_id(client_id, version, clean_start, context),
         {:ok, will, rest} <- decode_will(rest, flags, version, context),
         {:ok, username, rest} <-
           optional_decode(rest, (flags &&& 0x80) != 0, :utf8, context, :username),
         {:ok, password, rest} <-
           optional_decode(rest, (flags &&& 0x40) != 0, :binary, context, :password),
         :ok <- exhausted(rest, context) do
      {:ok,
       %Connect{
         version: version,
         client_id: client_id,
         clean_start: clean_start,
         keep_alive: keep_alive,
         properties: properties,
         will: will,
         username: username,
         password: password
       }}
    end
  end

  defp protocol_name("MQTT", _context), do: :ok

  defp protocol_name(_name, context),
    do: Wire.error(context, :unsupported, :unsupported_protocol_name)

  defp version(4, _context), do: {:ok, :v3_1_1}
  defp version(5, _context), do: {:ok, :v5}
  defp version(_level, context), do: Wire.error(context, :unsupported, :unsupported_version)

  defp expected_version(version, expected, _context) when expected in [:auto, version], do: :ok

  defp expected_version(_version, _expected, context),
    do: Wire.error(context, :unsupported, :version_mismatch)

  defp flags(flags, version, context) do
    will? = (flags &&& 4) != 0

    cond do
      (flags &&& 1) != 0 or (flags &&& 0x18) == 0x18 or
          (not will? and (flags &&& 0x38) != 0) ->
        Wire.error(context, :malformed, :invalid_connect_flags)

      version == :v3_1_1 and (flags &&& 0x40) != 0 and (flags &&& 0x80) == 0 ->
        Wire.error(context, :malformed, :password_requires_username)

      true ->
        :ok
    end
  end

  defp client_id(<<>>, :v3_1_1, false, context),
    do: Wire.error(context, Wire.value_class(context), :empty_client_identifier)

  defp client_id(_client_id, _version, _clean_start, _context), do: :ok

  defp decode_will(bytes, flags, version, context) do
    if (flags &&& 4) == 0 do
      {:ok, nil, bytes}
    else
      with {:ok, properties, rest} <- Properties.decode(bytes, version, :will, context),
           {:ok, topic, rest} <- Wire.decode(:utf8, rest, context, :will_topic),
           :ok <- will_topic(topic, context),
           {:ok, payload, rest} <- Wire.decode(:binary, rest, context, :will_payload) do
        {:ok,
         %Will{
           topic: topic,
           payload: payload,
           properties: properties,
           qos: flags >>> 3 &&& 3,
           retain: (flags &&& 0x20) != 0
         }, rest}
      end
    end
  end

  defp will_topic(topic, context) do
    if Wire.topic_name?(topic),
      do: :ok,
      else: Wire.error(context, Wire.value_class(context), :invalid_will_topic)
  end

  defp optional_decode(bytes, false, _type, _context, _field), do: {:ok, nil, bytes}

  defp optional_decode(bytes, true, type, context, field),
    do: Wire.decode(type, bytes, context, field)

  defp exhausted(<<>>, _context), do: :ok
  defp exhausted(_rest, context), do: Wire.error(context, :malformed, :trailing_packet_data)

  def encode(%Connect{} = packet, context) do
    with :ok <- boolean(packet.clean_start, context, :clean_start),
         :ok <- credentials(packet, context),
         {:ok, protocol_name} <- Wire.encode(:utf8, "MQTT", context, :protocol_name),
         {:ok, keep_alive} <- Wire.encode(:u16, packet.keep_alive, context, :keep_alive),
         {:ok, properties} <-
           Properties.encode(packet.properties, packet.version, :connect, context),
         {:ok, client} <- Wire.encode(:utf8, packet.client_id, context, :client_id),
         :ok <- client_id(packet.client_id, packet.version, packet.clean_start, context),
         {:ok, will_flags, will} <- encode_will(packet.will, packet.version, context),
         {:ok, username} <- optional_encode(packet.username, :utf8, context, :username),
         {:ok, password} <- optional_encode(packet.password, :binary, context, :password) do
      level = if packet.version == :v5, do: 5, else: 4

      flags =
        will_flags ||| flag(packet.clean_start, 2) |||
          flag(not is_nil(packet.username), 0x80) ||| flag(not is_nil(packet.password), 0x40)

      {:ok,
       [protocol_name, <<level, flags>>, keep_alive, properties, client, will, username, password]}
    end
  end

  defp credentials(%Connect{version: :v3_1_1, username: nil, password: password}, context)
       when not is_nil(password),
       do: Wire.error(context, :invalid_value, :password_requires_username)

  defp credentials(_packet, _context), do: :ok

  defp boolean(value, _context, _field) when is_boolean(value), do: :ok

  defp boolean(_value, context, field),
    do: Wire.error(context, :invalid_value, :invalid_field, %{field: field})

  defp encode_will(nil, _version, _context), do: {:ok, 0, <<>>}

  defp encode_will(%Will{qos: qos, retain: retain} = will, version, context)
       when is_integer(qos) and qos in 0..2 and is_boolean(retain) do
    with {:ok, properties} <- Properties.encode(will.properties, version, :will, context),
         {:ok, topic} <- Wire.encode(:utf8, will.topic, context, :will_topic),
         :ok <- will_topic(will.topic, context),
         {:ok, payload} <- Wire.encode(:binary, will.payload, context, :will_payload) do
      flags = 4 ||| qos <<< 3 ||| flag(retain, 0x20)
      {:ok, flags, [properties, topic, payload]}
    end
  end

  defp encode_will(_will, _version, context),
    do: Wire.error(context, :invalid_value, :invalid_will)

  defp optional_encode(nil, _type, _context, _field), do: {:ok, <<>>}
  defp optional_encode(value, type, context, field), do: Wire.encode(type, value, context, field)

  defp flag(true, bits), do: bits
  defp flag(false, _bits), do: 0
end
