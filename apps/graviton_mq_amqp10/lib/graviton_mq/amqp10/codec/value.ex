defmodule GravitonMQ.AMQP10.Codec.Value do
  @moduledoc """
  Encodes and decodes the bounded AMQP 1.0 values needed by the initial Open
  and Begin codec foundation.

  Wire constructor widths are normalized into semantic values when decoding.
  Encoding makes the canonical width choice for each supported semantic type.
  """

  alias GravitonMQ.AMQP10.Codec
  alias GravitonMQ.AMQP10.Codec.Error
  alias GravitonMQ.AMQP10.Codec.Limits
  alias GravitonMQ.AMQP10.Value, as: AMQPValue

  @supported_semantic_types [
    :null,
    :ushort,
    :uint,
    :ulong,
    :string,
    :symbol,
    :list,
    :map,
    :array,
    :described
  ]

  @spec decode(binary()) :: Codec.decode_result(AMQPValue.t())
  @spec decode(binary(), Limits.t()) :: Codec.decode_result(AMQPValue.t())
  def decode(input, limits \\ Limits.default()) when is_binary(input) do
    with {:ok, limits} <- Limits.validate(limits, :value_decode) do
      decode_value(input, limits, 0)
    end
  end

  @spec encode(AMQPValue.t()) :: Codec.encode_result()
  @spec encode(AMQPValue.t(), Limits.t()) :: Codec.encode_result()
  def encode(%AMQPValue{} = value, limits \\ Limits.default()) do
    with {:ok, limits} <- Limits.validate(limits, :value_encode) do
      encode_value(value, limits, 0)
    end
  end

  defp decode_value(<<>>, _limits, _depth), do: {:more, 1}

  defp decode_value(<<0x40, rest::binary>>, _limits, _depth),
    do: {:ok, AMQPValue.null(), rest}

  defp decode_value(<<0x43, rest::binary>>, _limits, _depth),
    do: {:ok, AMQPValue.uint(0), rest}

  defp decode_value(<<0x44, rest::binary>>, _limits, _depth),
    do: {:ok, AMQPValue.ulong(0), rest}

  defp decode_value(<<0x60, rest::binary>>, limits, _depth),
    do: decode_fixed(rest, 2, &AMQPValue.ushort/1, limits)

  defp decode_value(<<0x52, rest::binary>>, limits, _depth),
    do: decode_fixed(rest, 1, &AMQPValue.uint/1, limits)

  defp decode_value(<<0x70, rest::binary>>, limits, _depth),
    do: decode_fixed(rest, 4, &AMQPValue.uint/1, limits)

  defp decode_value(<<0x53, rest::binary>>, limits, _depth),
    do: decode_fixed(rest, 1, &AMQPValue.ulong/1, limits)

  defp decode_value(<<0x80, rest::binary>>, limits, _depth),
    do: decode_fixed(rest, 8, &AMQPValue.ulong/1, limits)

  defp decode_value(<<0xA1, rest::binary>>, limits, _depth),
    do: decode_variable(rest, 1, :string, limits)

  defp decode_value(<<0xB1, rest::binary>>, limits, _depth),
    do: decode_variable(rest, 4, :string, limits)

  defp decode_value(<<0xA3, rest::binary>>, limits, _depth),
    do: decode_variable(rest, 1, :symbol, limits)

  defp decode_value(<<0xB3, rest::binary>>, limits, _depth),
    do: decode_variable(rest, 4, :symbol, limits)

  defp decode_value(<<0x00, rest::binary>>, limits, depth),
    do: decode_described(rest, limits, depth)

  defp decode_value(<<0x45, rest::binary>>, limits, depth) do
    with :ok <- validate_nesting_depth(:value_decode, limits, depth) do
      {:ok, AMQPValue.list([]), rest}
    end
  end

  defp decode_value(<<0xC0, rest::binary>>, limits, depth),
    do: decode_compound(rest, 1, :list, limits, depth)

  defp decode_value(<<0xD0, rest::binary>>, limits, depth),
    do: decode_compound(rest, 4, :list, limits, depth)

  defp decode_value(<<0xC1, rest::binary>>, limits, depth),
    do: decode_compound(rest, 1, :map, limits, depth)

  defp decode_value(<<0xD1, rest::binary>>, limits, depth),
    do: decode_compound(rest, 4, :map, limits, depth)

  defp decode_value(<<0xE0, rest::binary>>, limits, depth),
    do: decode_array(rest, 1, limits, depth)

  defp decode_value(<<0xF0, rest::binary>>, limits, depth),
    do: decode_array(rest, 4, limits, depth)

  defp decode_value(<<format_code, _rest::binary>>, _limits, _depth) do
    {:error,
     Error.new(:value_decode, :unsupported, :format_code,
       offset: 0,
       details: %{format_code: format_code}
     )}
  end

  defp decode_compound(input, width, _type, _limits, _depth) when byte_size(input) < width,
    do: {:more, width - byte_size(input)}

  defp decode_compound(input, width, type, limits, depth) do
    <<size::unsigned-big-integer-size(width)-unit(8), payload_and_rest::binary>> = input

    with :ok <- validate_nesting_depth(:value_decode, limits, depth),
         :ok <- validate_compound_size(size, width, :value_decode),
         :ok <- validate_compound_value_size(size, limits, :value_decode) do
      if byte_size(payload_and_rest) < size do
        {:more, size - byte_size(payload_and_rest)}
      else
        <<payload::binary-size(size), rest::binary>> = payload_and_rest
        decode_compound_payload(payload, width, type, limits, depth, rest)
      end
    end
  end

  defp decode_compound_payload(payload, width, type, limits, depth, rest) do
    <<count::unsigned-big-integer-size(width)-unit(8), items::binary>> = payload

    with :ok <- validate_compound_count(count, type, limits, :value_decode),
         {:ok, values, <<>>} <- decode_compound_items(items, count, limits, depth + 1, []),
         :ok <- validate_decoded_map_values(type, values) do
      {:ok, compound_value(type, values), rest}
    else
      {:ok, _values, _leftover} -> compound_error(:compound_size_mismatch)
      {:more, _needed} -> compound_error(:compound_item_truncated)
      {:error, _error} = error -> error
    end
  end

  defp decode_compound_items(input, 0, _limits, _depth, values),
    do: {:ok, Enum.reverse(values), input}

  defp decode_compound_items(input, count, limits, depth, values) do
    case decode_value(input, limits, depth) do
      {:ok, value, rest} ->
        decode_compound_items(rest, count - 1, limits, depth, [value | values])

      {:more, needed} ->
        {:more, needed}

      {:error, _error} = error ->
        error
    end
  end

  defp decode_array(input, width, _limits, _depth) when byte_size(input) < width,
    do: {:more, width - byte_size(input)}

  defp decode_array(input, width, limits, depth) do
    <<size::unsigned-big-integer-size(width)-unit(8), payload_and_rest::binary>> = input

    with :ok <- validate_nesting_depth(:value_decode, limits, depth),
         :ok <- validate_array_size(size, width),
         :ok <- validate_compound_value_size(size, limits, :value_decode) do
      if byte_size(payload_and_rest) < size do
        {:more, size - byte_size(payload_and_rest)}
      else
        <<payload::binary-size(size), rest::binary>> = payload_and_rest
        decode_array_payload(payload, width, limits, rest)
      end
    end
  end

  defp decode_array_payload(payload, width, limits, rest) do
    <<count::unsigned-big-integer-size(width)-unit(8), constructor, elements::binary>> = payload

    with :ok <- validate_compound_count(count, :array, limits, :value_decode),
         {:ok, element_width} <- symbol_array_element_width(constructor, :value_decode),
         {:ok, values, <<>>} <-
           decode_symbol_array_elements(elements, count, element_width, limits, []),
         {:ok, array} <- build_symbol_array(values) do
      {:ok, array, rest}
    else
      {:ok, _values, _leftover} -> array_error(:array_size_mismatch)
      {:more, _needed} -> array_error(:array_element_truncated)
      {:error, _error} = error -> error
    end
  end

  defp decode_symbol_array_elements(input, 0, _width, _limits, values),
    do: {:ok, Enum.reverse(values), input}

  defp decode_symbol_array_elements(input, _count, width, _limits, _values)
       when byte_size(input) < width,
       do: {:more, width - byte_size(input)}

  defp decode_symbol_array_elements(input, count, width, limits, values) do
    <<size::unsigned-big-integer-size(width)-unit(8), payload_and_rest::binary>> = input

    cond do
      size > limits.max_value_bytes ->
        value_size_limit(:value_decode, nil)

      byte_size(payload_and_rest) < size ->
        {:more, size - byte_size(payload_and_rest)}

      true ->
        <<payload::binary-size(size), rest::binary>> = payload_and_rest

        with :ok <- validate_payload(:symbol, payload, :value_decode, nil) do
          decode_symbol_array_elements(
            rest,
            count - 1,
            width,
            limits,
            [AMQPValue.symbol(payload) | values]
          )
        end
    end
  end

  defp build_symbol_array(values), do: {:ok, AMQPValue.array(:symbol, values)}

  defp decode_described(input, limits, depth) do
    with :ok <- validate_nesting_depth(:value_decode, limits, depth),
         {:ok, descriptor, after_descriptor} <- decode_value(input, limits, depth + 1),
         :ok <- validate_descriptor(descriptor, :value_decode),
         {:ok, value, rest} <- decode_value(after_descriptor, limits, depth + 1),
         :ok <-
           validate_total_value_size(
             1 + byte_size(input) - byte_size(rest),
             limits,
             :value_decode
           ) do
      {:ok, AMQPValue.described(descriptor, value), rest}
    end
  end

  defp compound_value(:list, values), do: AMQPValue.list(values)

  defp compound_value(:map, values) do
    values
    |> Enum.chunk_every(2)
    |> Enum.map(fn [key, value] -> {key, value} end)
    |> AMQPValue.map()
  end

  defp validate_decoded_map_values(:list, _values), do: :ok

  defp validate_decoded_map_values(:map, values) do
    values
    |> Enum.chunk_every(2)
    |> Enum.reduce_while(MapSet.new(), fn [key, _value], keys ->
      cond do
        key.type == :null -> {:halt, :null_map_key}
        MapSet.member?(keys, key) -> {:halt, :duplicate_map_key}
        true -> {:cont, MapSet.put(keys, key)}
      end
    end)
    |> case do
      :null_map_key -> compound_error(:null_map_key)
      :duplicate_map_key -> compound_error(:duplicate_map_key)
      _keys -> :ok
    end
  end

  defp decode_fixed(input, width, _constructor, _limits) when byte_size(input) < width,
    do: {:more, width - byte_size(input)}

  defp decode_fixed(input, width, constructor, _limits) do
    <<value::unsigned-big-integer-size(width)-unit(8), rest::binary>> = input
    {:ok, constructor.(value), rest}
  end

  defp decode_variable(input, width, _type, _limits) when byte_size(input) < width,
    do: {:more, width - byte_size(input)}

  defp decode_variable(input, width, type, limits) do
    <<size::unsigned-big-integer-size(width)-unit(8), rest::binary>> = input

    if size > limits.max_value_bytes do
      value_size_limit(:value_decode, 1)
    else
      decode_variable_payload(rest, size, width, type)
    end
  end

  defp decode_variable_payload(input, size, _width, _type) when byte_size(input) < size,
    do: {:more, size - byte_size(input)}

  defp decode_variable_payload(input, size, width, type) do
    <<payload::binary-size(size), rest::binary>> = input

    with :ok <- validate_payload(type, payload, :value_decode, 1 + width) do
      {:ok, semantic_value(type, payload), rest}
    end
  end

  defp encode_value(%AMQPValue{type: :null, value: nil}, _limits, _depth), do: {:ok, <<0x40>>}

  defp encode_value(%AMQPValue{type: :ushort, value: value}, _limits, _depth)
       when is_integer(value) and value in 0..65_535,
       do: {:ok, <<0x60, value::16>>}

  defp encode_value(%AMQPValue{type: :uint, value: 0}, _limits, _depth), do: {:ok, <<0x43>>}

  defp encode_value(%AMQPValue{type: :uint, value: value}, _limits, _depth)
       when is_integer(value) and value in 1..255,
       do: {:ok, <<0x52, value>>}

  defp encode_value(%AMQPValue{type: :uint, value: value}, _limits, _depth)
       when is_integer(value) and value in 256..4_294_967_295,
       do: {:ok, <<0x70, value::32>>}

  defp encode_value(%AMQPValue{type: :ulong, value: 0}, _limits, _depth), do: {:ok, <<0x44>>}

  defp encode_value(%AMQPValue{type: :ulong, value: value}, _limits, _depth)
       when is_integer(value) and value in 1..255,
       do: {:ok, <<0x53, value>>}

  defp encode_value(%AMQPValue{type: :ulong, value: value}, _limits, _depth)
       when is_integer(value) and value in 256..18_446_744_073_709_551_615,
       do: {:ok, <<0x80, value::64>>}

  defp encode_value(%AMQPValue{type: type, value: value}, limits, _depth)
       when type in [:string, :symbol] and is_binary(value) do
    with :ok <- validate_value_size(value, limits),
         :ok <- validate_payload(type, value, :value_encode, nil) do
      encode_variable(type, value)
    end
  end

  defp encode_value(%AMQPValue{type: :list, value: values}, limits, depth) when is_list(values),
    do: encode_list(values, limits, depth)

  defp encode_value(%AMQPValue{type: :map, value: entries}, limits, depth) when is_list(entries),
    do: encode_map(entries, limits, depth)

  defp encode_value(
         %AMQPValue{
           type: :array,
           value: %AMQPValue.Array{element_type: :symbol, values: values}
         },
         limits,
         depth
       )
       when is_list(values),
       do: encode_symbol_array(values, limits, depth)

  defp encode_value(
         %AMQPValue{type: :array, value: %AMQPValue.Array{element_type: element_type}},
         _limits,
         _depth
       ) do
    {:error,
     Error.new(:value_encode, :unsupported, :array_element_type,
       details: %{element_type: element_type}
     )}
  end

  defp encode_value(
         %AMQPValue{
           type: :described,
           value: %AMQPValue.Described{
             descriptor: %AMQPValue{} = descriptor,
             value: %AMQPValue{} = value
           }
         },
         limits,
         depth
       ),
       do: encode_described(descriptor, value, limits, depth)

  defp encode_value(%AMQPValue{type: type}, _limits, _depth)
       when type in @supported_semantic_types do
    {:error,
     Error.new(:value_encode, :invalid_value, :invalid_semantic_value, details: %{type: type})}
  end

  defp encode_value(%AMQPValue{type: type}, _limits, _depth) do
    {:error, Error.new(:value_encode, :unsupported, :semantic_type, details: %{type: type})}
  end

  defp encode_list([], limits, depth) do
    with :ok <- validate_nesting_depth(:value_encode, limits, depth) do
      {:ok, <<0x45>>}
    end
  end

  defp encode_list(values, limits, depth) do
    with :ok <- validate_nesting_depth(:value_encode, limits, depth),
         :ok <- validate_compound_count(length(values), :list, limits, :value_encode),
         {:ok, encoded_items} <- encode_compound_items(values, limits, depth + 1) do
      encode_compound(encoded_items, length(values), :list, :value_encode, limits)
    end
  end

  defp encode_map(entries, limits, depth) do
    with :ok <- validate_nesting_depth(:value_encode, limits, depth),
         {:ok, values} <- map_values(entries),
         :ok <- validate_compound_count(length(values), :map, limits, :value_encode),
         {:ok, encoded_items} <- encode_compound_items(values, limits, depth + 1) do
      encode_compound(encoded_items, length(values), :map, :value_encode, limits)
    end
  end

  defp encode_symbol_array(values, limits, depth) do
    with :ok <- validate_nesting_depth(:value_encode, limits, depth),
         :ok <- validate_compound_count(length(values), :array, limits, :value_encode),
         {:ok, symbol_values, element_width} <- encode_symbol_array_values(values, limits) do
      encode_array(symbol_values, element_width, length(values), limits)
    end
  end

  defp encode_symbol_array_values(values, limits) do
    Enum.reduce_while(values, {:ok, [], 1}, fn
      %AMQPValue{type: :symbol, value: value}, {:ok, encoded_values, element_width}
      when is_binary(value) ->
        with :ok <- validate_value_size(value, limits),
             :ok <- validate_payload(:symbol, value, :value_encode, nil) do
          width = if byte_size(value) <= 255, do: element_width, else: 4
          {:cont, {:ok, [value | encoded_values], width}}
        else
          {:error, _error} = error -> {:halt, error}
        end

      _value, _acc ->
        {:halt, invalid_compound_value(:invalid_semantic_value)}
    end)
    |> case do
      {:ok, encoded_values, element_width} ->
        {:ok, Enum.reverse(encoded_values), element_width}

      error ->
        error
    end
  end

  defp encode_array(values, element_width, count, limits) do
    constructor = if element_width == 1, do: 0xA3, else: 0xB3

    encoded_values =
      Enum.map(values, fn value ->
        if element_width == 1 do
          [<<byte_size(value)>>, value]
        else
          [<<byte_size(value)::32>>, value]
        end
      end)

    item_size = IO.iodata_length(encoded_values)

    if count <= 255 and 2 + item_size <= 255 do
      size = 2 + item_size

      with :ok <- validate_compound_value_size(size, limits, :value_encode) do
        {:ok, IO.iodata_to_binary([<<0xE0, size, count, constructor>>, encoded_values])}
      end
    else
      size = 5 + item_size

      with :ok <- validate_compound_value_size(size, limits, :value_encode) do
        {:ok,
         IO.iodata_to_binary([
           <<0xF0, size::32, count::32, constructor>>,
           encoded_values
         ])}
      end
    end
  end

  defp encode_described(descriptor, value, limits, depth) do
    with :ok <- validate_nesting_depth(:value_encode, limits, depth),
         :ok <- validate_descriptor(descriptor, :value_encode),
         {:ok, encoded_descriptor} <- encode_value(descriptor, limits, depth + 1),
         {:ok, encoded_value} <- encode_value(value, limits, depth + 1),
         :ok <-
           validate_total_value_size(
             1 + byte_size(encoded_descriptor) + byte_size(encoded_value),
             limits,
             :value_encode
           ) do
      {:ok, <<0x00, encoded_descriptor::binary, encoded_value::binary>>}
    end
  end

  defp map_values(entries) do
    Enum.reduce_while(entries, {:ok, [], MapSet.new()}, fn
      {%AMQPValue{type: :null}, %AMQPValue{}}, _acc ->
        {:halt, invalid_compound_value(:null_map_key)}

      {%AMQPValue{} = key, %AMQPValue{} = value}, {:ok, values, keys} ->
        if MapSet.member?(keys, key) do
          {:halt, invalid_compound_value(:duplicate_map_key)}
        else
          {:cont, {:ok, [value, key | values], MapSet.put(keys, key)}}
        end

      _entry, _acc ->
        {:halt, invalid_compound_value(:invalid_semantic_value)}
    end)
    |> case do
      {:ok, values, _keys} -> {:ok, Enum.reverse(values)}
      error -> error
    end
  end

  defp encode_compound_items(values, limits, depth) do
    Enum.reduce_while(values, {:ok, []}, fn
      %AMQPValue{} = value, {:ok, encoded_values} ->
        case encode_value(value, limits, depth) do
          {:ok, encoded} -> {:cont, {:ok, [encoded | encoded_values]}}
          {:error, _error} = error -> {:halt, error}
        end

      _value, _acc ->
        {:halt, invalid_compound_value(:invalid_semantic_value)}
    end)
    |> case do
      {:ok, encoded_values} -> {:ok, Enum.reverse(encoded_values)}
      error -> error
    end
  end

  defp encode_compound(encoded_items, count, type, operation, limits) do
    item_size = IO.iodata_length(encoded_items)

    if count <= 255 and 1 + item_size <= 255 do
      size = 1 + item_size

      with :ok <- validate_compound_value_size(size, limits, operation) do
        {:ok,
         IO.iodata_to_binary([compound_format_code(type, :short), <<size, count>>, encoded_items])}
      end
    else
      size = 4 + item_size

      with :ok <- validate_compound_value_size(size, limits, operation) do
        {:ok,
         IO.iodata_to_binary([
           compound_format_code(type, :long),
           <<size::32, count::32>>,
           encoded_items
         ])}
      end
    end
  end

  defp compound_format_code(:list, :short), do: 0xC0
  defp compound_format_code(:list, :long), do: 0xD0
  defp compound_format_code(:map, :short), do: 0xC1
  defp compound_format_code(:map, :long), do: 0xD1

  defp encode_variable(type, value) when byte_size(value) <= 255 do
    {:ok, <<variable_format_code(type, 1), byte_size(value), value::binary>>}
  end

  defp encode_variable(type, value) do
    {:ok, <<variable_format_code(type, 4), byte_size(value)::32, value::binary>>}
  end

  defp variable_format_code(:string, 1), do: 0xA1
  defp variable_format_code(:string, 4), do: 0xB1
  defp variable_format_code(:symbol, 1), do: 0xA3
  defp variable_format_code(:symbol, 4), do: 0xB3

  defp semantic_value(:string, value), do: AMQPValue.string(value)
  defp semantic_value(:symbol, value), do: AMQPValue.symbol(value)

  defp validate_value_size(value, limits) when byte_size(value) <= limits.max_value_bytes,
    do: :ok

  defp validate_value_size(_value, _limits), do: value_size_limit(:value_encode, nil)

  defp validate_nesting_depth(_operation, limits, depth) when depth < limits.max_nesting_depth,
    do: :ok

  defp validate_nesting_depth(operation, _limits, _depth) do
    {:error, Error.new(operation, :limit_exceeded, :nesting_depth_limit)}
  end

  defp validate_compound_size(size, width, _operation) when size >= width, do: :ok

  defp validate_compound_size(_size, _width, operation),
    do: compound_error(operation, :compound_size_too_small)

  defp validate_array_size(size, width) when size >= width + 1, do: :ok
  defp validate_array_size(_size, _width), do: array_error(:array_size_too_small)

  defp validate_compound_value_size(size, limits, _operation)
       when size <= limits.max_value_bytes and size <= 0xFFFF_FFFF,
       do: :ok

  defp validate_compound_value_size(_size, _limits, operation),
    do: value_size_limit(operation, if(operation == :value_decode, do: 1, else: nil))

  defp validate_compound_count(count, :map, _limits, operation) when rem(count, 2) != 0,
    do: compound_error(operation, :odd_map_count)

  defp validate_compound_count(count, _type, limits, _operation)
       when count <= limits.max_compound_items,
       do: :ok

  defp validate_compound_count(_count, _type, _limits, operation) do
    {:error, Error.new(operation, :limit_exceeded, :compound_item_limit)}
  end

  defp symbol_array_element_width(0xA3, _operation), do: {:ok, 1}
  defp symbol_array_element_width(0xB3, _operation), do: {:ok, 4}

  defp symbol_array_element_width(format_code, operation) do
    {:error,
     Error.new(operation, :unsupported, :array_element_type, details: %{format_code: format_code})}
  end

  defp validate_descriptor(%AMQPValue{type: type}, _operation) when type in [:ulong, :symbol],
    do: :ok

  defp validate_descriptor(%AMQPValue{type: type}, operation) do
    {:error, Error.new(operation, :unsupported, :descriptor_type, details: %{type: type})}
  end

  defp validate_total_value_size(size, limits, operation),
    do: validate_compound_value_size(size, limits, operation)

  defp compound_error(reason), do: compound_error(:value_decode, reason)

  defp compound_error(operation, reason) do
    {:error, Error.new(operation, :malformed, reason)}
  end

  defp invalid_compound_value(reason) do
    {:error, Error.new(:value_encode, :invalid_value, reason)}
  end

  defp array_error(reason) do
    {:error, Error.new(:value_decode, :malformed, reason)}
  end

  defp validate_payload(:string, value, operation, offset) when is_binary(value) do
    if String.valid?(value), do: :ok, else: invalid_payload(:invalid_utf8, operation, offset)
  end

  defp validate_payload(:symbol, value, operation, offset) do
    if ascii?(value), do: :ok, else: invalid_payload(:invalid_symbol, operation, offset)
  end

  defp ascii?(<<>>), do: true
  defp ascii?(<<octet, rest::binary>>) when octet <= 0x7F, do: ascii?(rest)
  defp ascii?(<<_octet, _rest::binary>>), do: false

  defp invalid_payload(reason, :value_decode, offset) do
    {:error, Error.new(:value_decode, :malformed, reason, offset: offset)}
  end

  defp invalid_payload(reason, :value_encode, offset) do
    {:error, Error.new(:value_encode, :invalid_value, reason, offset: offset)}
  end

  defp value_size_limit(operation, offset) do
    {:error, Error.new(operation, :limit_exceeded, :value_size_limit, offset: offset)}
  end
end
