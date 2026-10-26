defmodule GravitonMQ.MQTT.Codec.Properties do
  @moduledoc """
  Typed, ordered MQTT 5.0 properties for CONNECT, CONNACK, and Will.

  Known names select their exact wire type. User Property pairs may repeat;
  all other properties are singletons within their own property section.
  """

  alias GravitonMQ.MQTT.Codec.Wire

  @type value :: non_neg_integer() | binary() | {binary(), binary()}
  @type t :: [{atom(), value()}]

  @definitions %{
    0x01 => {:payload_format_indicator, :u8, [:will]},
    0x02 => {:message_expiry_interval, :u32, [:will]},
    0x03 => {:content_type, :utf8, [:will]},
    0x08 => {:response_topic, :utf8, [:will]},
    0x09 => {:correlation_data, :binary, [:will]},
    0x11 => {:session_expiry_interval, :u32, [:connect, :connack]},
    0x12 => {:assigned_client_identifier, :utf8, [:connack]},
    0x13 => {:server_keep_alive, :u16, [:connack]},
    0x15 => {:authentication_method, :utf8, [:connect, :connack]},
    0x16 => {:authentication_data, :binary, [:connect, :connack]},
    0x17 => {:request_problem_information, :u8, [:connect]},
    0x18 => {:will_delay_interval, :u32, [:will]},
    0x19 => {:request_response_information, :u8, [:connect]},
    0x1A => {:response_information, :utf8, [:connack]},
    0x1C => {:server_reference, :utf8, [:connack]},
    0x1F => {:reason_string, :utf8, [:connack]},
    0x21 => {:receive_maximum, :u16, [:connect, :connack]},
    0x22 => {:topic_alias_maximum, :u16, [:connect, :connack]},
    0x24 => {:maximum_qos, :u8, [:connack]},
    0x25 => {:retain_available, :u8, [:connack]},
    0x26 => {:user_property, :utf8_pair, [:connect, :connack, :will]},
    0x27 => {:maximum_packet_size, :u32, [:connect, :connack]},
    0x28 => {:wildcard_subscription_available, :u8, [:connack]},
    0x29 => {:subscription_identifier_available, :u8, [:connack]},
    0x2A => {:shared_subscription_available, :u8, [:connack]}
  }

  @by_name Map.new(@definitions, fn {id, {name, type, scopes}} -> {name, {id, type, scopes}} end)
  @binary_flags [
    :payload_format_indicator,
    :request_problem_information,
    :request_response_information,
    :maximum_qos,
    :retain_available,
    :wildcard_subscription_available,
    :subscription_identifier_available,
    :shared_subscription_available
  ]

  @doc false
  def decode(bytes, :v3_1_1, _scope, _context), do: {:ok, [], bytes}

  def decode(bytes, :v5, scope, context) do
    with {:ok, size, data, _width} <- variable_integer(bytes, context),
         {:ok, section, rest} <- section(data, size, context),
         {:ok, properties} <- decode_entries(section, scope, context, MapSet.new(), [], 0),
         :ok <- validate_relation(properties, scope, context) do
      {:ok, properties, rest}
    end
  end

  defp section(bytes, size, context) do
    if byte_size(bytes) >= size do
      <<section::binary-size(size), rest::binary>> = bytes
      {:ok, section, rest}
    else
      Wire.error(context, :malformed, :truncated_properties)
    end
  end

  defp variable_integer(bytes, context) do
    case Wire.decode_vbi(bytes) do
      {:ok, _, _, _} = result -> result
      {:more, _} -> Wire.error(context, :malformed, :truncated_properties)
      {:error, reason} -> Wire.error(context, :malformed, reason)
    end
  end

  defp decode_entries(<<>>, _scope, _context, _seen, properties, _count),
    do: {:ok, Enum.reverse(properties)}

  defp decode_entries(bytes, scope, context, seen, properties, count) do
    with :ok <- count_limit(count + 1, context),
         {:ok, id, data, _width} <- variable_integer(bytes, context),
         {:ok, name, type} <- definition(id, scope, seen, context),
         {:ok, value, rest} <- decode_value(type, data, context, name),
         :ok <- validate_value(name, value, context) do
      decode_entries(
        rest,
        scope,
        context,
        MapSet.put(seen, name),
        [{name, value} | properties],
        count + 1
      )
    end
  end

  defp definition(id, scope, seen, context) do
    case Map.fetch(@definitions, id) do
      {:ok, {name, type, scopes}} ->
        with :ok <- validate_location(name, scope, scopes, seen, context),
             do: {:ok, name, type}

      :error ->
        Wire.error(context, :malformed, :unknown_property, %{identifier: id})
    end
  end

  defp decode_value(:utf8_pair, bytes, context, name) do
    with {:ok, key, rest} <- Wire.decode(:utf8, bytes, context, name),
         {:ok, value, rest} <- Wire.decode(:utf8, rest, context, name) do
      {:ok, {key, value}, rest}
    end
  end

  defp decode_value(type, bytes, context, name), do: Wire.decode(type, bytes, context, name)

  @doc false
  def encode([], :v3_1_1, _scope, _context), do: {:ok, <<>>}

  def encode(_properties, :v3_1_1, _scope, context),
    do: Wire.error(context, :invalid_value, :properties_not_supported)

  def encode(properties, :v5, scope, context) when is_list(properties) do
    with {:ok, parts, size} <- encode_entries(properties, scope, context, MapSet.new(), [], 0, 0),
         :ok <- validate_relation(properties, scope, context) do
      data = :erlang.iolist_to_binary(Enum.reverse(parts))
      {:ok, <<Wire.encode_vbi(size)::binary, data::binary>>}
    end
  end

  def encode(_properties, :v5, _scope, context),
    do: Wire.error(context, :invalid_value, :invalid_properties)

  defp encode_entries([], _scope, _context, _seen, parts, _count, size),
    do: {:ok, parts, size}

  defp encode_entries([{name, value} | rest], scope, context, seen, parts, count, size) do
    with :ok <- count_limit(count + 1, context),
         {:ok, id, type} <- named_definition(name, scope, seen, context),
         {:ok, data} <- encode_value(type, value, context, name),
         :ok <- validate_value(name, value, context),
         new_size = size + 1 + byte_size(data),
         :ok <- size_limit(new_size, context) do
      encode_entries(
        rest,
        scope,
        context,
        MapSet.put(seen, name),
        [[<<id>>, data] | parts],
        count + 1,
        new_size
      )
    end
  end

  defp encode_entries(_invalid, _scope, context, _seen, _parts, _count, _size),
    do: Wire.error(context, :invalid_value, :invalid_properties)

  defp named_definition(name, scope, seen, context) do
    case Map.fetch(@by_name, name) do
      {:ok, {id, type, scopes}} ->
        with :ok <- validate_location(name, scope, scopes, seen, context),
             do: {:ok, id, type}

      :error ->
        Wire.error(context, :invalid_value, :unknown_property)
    end
  end

  defp validate_location(name, scope, scopes, seen, context) do
    cond do
      scope not in scopes ->
        Wire.error(context, Wire.value_class(context), :property_not_allowed, %{property: name})

      name != :user_property and MapSet.member?(seen, name) ->
        Wire.error(context, Wire.value_class(context), :duplicate_property, %{property: name})

      true ->
        :ok
    end
  end

  defp encode_value(:utf8_pair, {key, value}, context, name) do
    with {:ok, key} <- Wire.encode(:utf8, key, context, name),
         {:ok, value} <- Wire.encode(:utf8, value, context, name),
         do: {:ok, <<key::binary, value::binary>>}
  end

  defp encode_value(:utf8_pair, _value, context, name),
    do: Wire.error(context, :invalid_value, :invalid_field, %{field: name})

  defp encode_value(type, value, context, name), do: Wire.encode(type, value, context, name)

  defp validate_value(name, value, context) do
    invalid? =
      cond do
        name in @binary_flags -> value not in [0, 1]
        name in [:receive_maximum, :maximum_packet_size] -> value == 0
        name == :assigned_client_identifier -> value == <<>>
        name == :response_topic -> not Wire.topic_name?(value)
        true -> false
      end

    if invalid?,
      do:
        Wire.error(context, Wire.value_class(context), :invalid_property_value, %{property: name}),
      else: :ok
  end

  defp validate_relation(properties, :connect, context) do
    if Keyword.has_key?(properties, :authentication_data) and
         not Keyword.has_key?(properties, :authentication_method),
       do: Wire.error(context, Wire.value_class(context), :authentication_method_required),
       else: :ok
  end

  defp validate_relation(_properties, _scope, _context), do: :ok

  defp count_limit(count, context) do
    if count > context.limits.max_properties,
      do: Wire.error(context, :limit_exceeded, :too_many_properties),
      else: :ok
  end

  defp size_limit(size, context) do
    if size > 268_435_455 or size > context.limits.max_packet_size,
      do: Wire.error(context, :limit_exceeded, :packet_too_large),
      else: :ok
  end
end
