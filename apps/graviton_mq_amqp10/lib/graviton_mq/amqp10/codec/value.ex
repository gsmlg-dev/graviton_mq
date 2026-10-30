defmodule GravitonMQ.AMQP10.Codec.Value do
  @moduledoc """
  Encodes and decodes the shared bounded AMQP 1.0 value subset, including
  all primitive scalars and recursively homogeneous arrays. Floating-point
  and decimal payloads retain exact wire bits, including NaN payloads.

  Wire constructor widths are normalized into semantic values when decoding.
  Encoding makes the canonical width choice for each supported semantic type.
  """

  alias GravitonMQ.AMQP10.Codec
  alias GravitonMQ.AMQP10.Codec.Error
  alias GravitonMQ.AMQP10.Codec.Limits
  alias GravitonMQ.AMQP10.Value, as: AMQPValue

  @supported_semantic_types [
    :null,
    :boolean,
    :ubyte,
    :ushort,
    :uint,
    :ulong,
    :byte,
    :short,
    :int,
    :long,
    :float,
    :double,
    :decimal32,
    :decimal64,
    :decimal128,
    :char,
    :timestamp,
    :uuid,
    :binary,
    :string,
    :symbol,
    :list,
    :map,
    :array,
    :described
  ]

  @array_constructors %{
    0x41 => {:boolean, :constant, 1},
    0x42 => {:boolean, :constant, 0},
    0x43 => {:uint, :constant, 0},
    0x44 => {:ulong, :constant, 0},
    0x45 => {:list, :constant, 0},
    0x56 => {:boolean, :boolean, 1},
    0x50 => {:ubyte, :unsigned, 1},
    0x60 => {:ushort, :unsigned, 2},
    0x52 => {:uint, :unsigned, 1},
    0x70 => {:uint, :unsigned, 4},
    0x53 => {:ulong, :unsigned, 1},
    0x80 => {:ulong, :unsigned, 8},
    0x51 => {:byte, :signed, 1},
    0x61 => {:short, :signed, 2},
    0x54 => {:int, :signed, 1},
    0x71 => {:int, :signed, 4},
    0x55 => {:long, :signed, 1},
    0x81 => {:long, :signed, 8},
    0x83 => {:timestamp, :signed, 8},
    0x98 => {:uuid, :bits, 16},
    0x72 => {:float, :bits, 4},
    0x82 => {:double, :bits, 8},
    0x74 => {:decimal32, :bits, 4},
    0x84 => {:decimal64, :bits, 8},
    0x94 => {:decimal128, :bits, 16},
    0x73 => {:char, :char, 4},
    0xA0 => {:binary, :variable, 1},
    0xB0 => {:binary, :variable, 4},
    0xA1 => {:string, :variable, 1},
    0xB1 => {:string, :variable, 4},
    0xA3 => {:symbol, :variable, 1},
    0xB3 => {:symbol, :variable, 4},
    0xC0 => {:list, :compound, 1},
    0xD0 => {:list, :compound, 4},
    0xC1 => {:map, :compound, 1},
    0xD1 => {:map, :compound, 4},
    0xE0 => {:array, :array, 1},
    0xF0 => {:array, :array, 4}
  }

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
    with {:ok, limits} <- Limits.validate(limits, :value_encode),
         {:ok, _size} <- encoding_size(value, limits, 0) do
      encode_value(value, limits, 0)
    end
  end

  @doc "Validates and measures the canonical encoding before allocating its bytes."
  @spec encoded_size(AMQPValue.t(), Limits.t()) ::
          {:ok, non_neg_integer()} | {:error, Error.t()}
  def encoded_size(value, limits \\ Limits.default()) do
    with {:ok, limits} <- Limits.validate(limits, :value_encode) do
      encoding_size(value, limits, 0)
    end
  end

  defp decode_value(<<>>, _limits, _depth), do: {:more, 1}

  defp decode_value(<<0x40, rest::binary>>, _limits, _depth),
    do: {:ok, AMQPValue.null(), rest}

  defp decode_value(<<0x41, rest::binary>>, _limits, _depth),
    do: {:ok, AMQPValue.boolean(true), rest}

  defp decode_value(<<0x42, rest::binary>>, _limits, _depth),
    do: {:ok, AMQPValue.boolean(false), rest}

  defp decode_value(<<0x56>>, _limits, _depth), do: {:more, 1}

  defp decode_value(<<0x56, value, rest::binary>>, _limits, _depth) when value in [0, 1],
    do: {:ok, AMQPValue.boolean(value == 1), rest}

  defp decode_value(<<0x56, value, _rest::binary>>, _limits, _depth) do
    {:error,
     Error.new(:value_decode, :malformed, :invalid_boolean,
       offset: 1,
       details: %{value: value}
     )}
  end

  defp decode_value(<<0x43, rest::binary>>, _limits, _depth),
    do: {:ok, AMQPValue.uint(0), rest}

  defp decode_value(<<0x44, rest::binary>>, _limits, _depth),
    do: {:ok, AMQPValue.ulong(0), rest}

  defp decode_value(<<0x50, rest::binary>>, limits, _depth),
    do: decode_fixed(rest, 1, &AMQPValue.ubyte/1, limits)

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

  defp decode_value(<<0x51, rest::binary>>, limits, _depth),
    do: decode_signed(rest, 1, &AMQPValue.byte/1, limits)

  defp decode_value(<<0x61, rest::binary>>, limits, _depth),
    do: decode_signed(rest, 2, &AMQPValue.short/1, limits)

  defp decode_value(<<0x54, rest::binary>>, limits, _depth),
    do: decode_signed(rest, 1, &AMQPValue.int/1, limits)

  defp decode_value(<<0x71, rest::binary>>, limits, _depth),
    do: decode_signed(rest, 4, &AMQPValue.int/1, limits)

  defp decode_value(<<0x55, rest::binary>>, limits, _depth),
    do: decode_signed(rest, 1, &AMQPValue.long/1, limits)

  defp decode_value(<<0x81, rest::binary>>, limits, _depth),
    do: decode_signed(rest, 8, &AMQPValue.long/1, limits)

  defp decode_value(<<0x72, rest::binary>>, limits, _depth),
    do: decode_bits(rest, 4, :float, limits)

  defp decode_value(<<0x82, rest::binary>>, limits, _depth),
    do: decode_bits(rest, 8, :double, limits)

  defp decode_value(<<0x74, rest::binary>>, limits, _depth),
    do: decode_bits(rest, 4, :decimal32, limits)

  defp decode_value(<<0x84, rest::binary>>, limits, _depth),
    do: decode_bits(rest, 8, :decimal64, limits)

  defp decode_value(<<0x94, rest::binary>>, limits, _depth),
    do: decode_bits(rest, 16, :decimal128, limits)

  defp decode_value(<<0x73, rest::binary>>, limits, _depth), do: decode_char(rest, limits)

  defp decode_value(<<0x83, rest::binary>>, limits, _depth),
    do: decode_signed(rest, 8, &AMQPValue.timestamp/1, limits)

  defp decode_value(<<0x98, rest::binary>>, limits, _depth),
    do: decode_uuid(rest, limits)

  defp decode_value(<<0xA0, rest::binary>>, limits, _depth),
    do: decode_variable(rest, 1, :binary, limits)

  defp decode_value(<<0xB0, rest::binary>>, limits, _depth),
    do: decode_variable(rest, 4, :binary, limits)

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
        decode_array_payload(payload, width, limits, depth, rest)
      end
    end
  end

  defp decode_array_payload(payload, width, limits, depth, rest) do
    <<count::unsigned-big-integer-size(width)-unit(8), constructor_and_elements::binary>> =
      payload

    with :ok <- validate_compound_count(count, :array, limits, :value_decode),
         {:ok, constructor, elements} <-
           decode_array_constructor(constructor_and_elements, limits, depth + 1),
         {:ok, values, <<>>} <-
           decode_array_elements(elements, count, constructor, limits, depth + 1, []) do
      {:ok, AMQPValue.array(array_element_type(constructor), values), rest}
    else
      {:ok, _values, _leftover} -> array_error(:array_size_mismatch)
      {:more, _needed} -> array_error(:array_element_truncated)
      {:error, _} = error -> error
    end
  end

  defp decode_array_constructor(<<0, rest::binary>>, limits, depth) do
    with :ok <- validate_nesting_depth(:value_decode, limits, depth),
         {:ok, descriptor, rest} <- decode_value(rest, limits, depth + 1),
         :ok <- validate_descriptor(descriptor, :value_decode),
         {:ok, inner, rest} <- decode_array_constructor(rest, limits, depth + 1) do
      {:ok, {:described, descriptor, inner}, rest}
    end
  end

  defp decode_array_constructor(<<0x40, _::binary>>, _limits, _depth),
    do: array_error(:null_array_element_type)

  defp decode_array_constructor(<<code, rest::binary>>, _limits, _depth) do
    case Map.fetch(@array_constructors, code) do
      {:ok, {type, kind, width}} ->
        {:ok, {type, kind, width}, rest}

      :error ->
        {:error,
         Error.new(:value_decode, :unsupported, :array_element_type,
           details: %{format_code: code}
         )}
    end
  end

  defp decode_array_constructor(<<>>, _limits, _depth), do: {:more, 1}

  defp array_element_type({:described, descriptor, inner}),
    do: {:described, descriptor, array_element_type(inner)}

  defp array_element_type({type, _kind, _width}), do: type

  defp decode_array_elements(input, 0, _constructor, _limits, _depth, values),
    do: {:ok, Enum.reverse(values), input}

  defp decode_array_elements(input, count, constructor, limits, depth, values) do
    with {:ok, value, rest} <- decode_array_element(input, constructor, limits, depth) do
      decode_array_elements(rest, count - 1, constructor, limits, depth, [value | values])
    end
  end

  defp decode_array_element(input, {:described, descriptor, inner}, limits, depth) do
    with :ok <- validate_nesting_depth(:value_decode, limits, depth),
         {:ok, value, rest} <- decode_array_element(input, inner, limits, depth + 1) do
      {:ok, AMQPValue.described(descriptor, value), rest}
    end
  end

  defp decode_array_element(input, {:list, :constant, 0}, limits, depth) do
    with :ok <- validate_nesting_depth(:value_decode, limits, depth),
         do: {:ok, AMQPValue.list([]), input}
  end

  defp decode_array_element(input, {:boolean, :constant, value}, _limits, _depth),
    do: {:ok, AMQPValue.boolean(value == 1), input}

  defp decode_array_element(input, {type, :constant, 0}, _limits, _depth),
    do: {:ok, %AMQPValue{type: type, value: 0}, input}

  defp decode_array_element(input, {:boolean, :boolean, 1}, _limits, _depth) do
    case input do
      <<value, rest::binary>> when value in [0, 1] -> {:ok, AMQPValue.boolean(value == 1), rest}
      <<_value, _::binary>> -> array_error(:invalid_boolean)
      <<>> -> {:more, 1}
    end
  end

  defp decode_array_element(input, {type, :unsigned, width}, limits, _depth),
    do: decode_fixed(input, width, &%AMQPValue{type: type, value: &1}, limits)

  defp decode_array_element(input, {type, :signed, width}, limits, _depth),
    do: decode_signed(input, width, &%AMQPValue{type: type, value: &1}, limits)

  defp decode_array_element(input, {type, :bits, width}, limits, _depth),
    do: decode_bits(input, width, type, limits)

  defp decode_array_element(input, {:char, :char, 4}, limits, _depth),
    do: decode_char(input, limits)

  defp decode_array_element(input, {type, :variable, width}, limits, _depth),
    do: decode_variable(input, width, type, limits)

  defp decode_array_element(input, {type, :compound, width}, limits, depth),
    do: decode_compound(input, width, type, limits, depth)

  defp decode_array_element(input, {:array, :array, width}, limits, depth),
    do: decode_array(input, width, limits, depth)

  defp decode_bits(_input, width, _type, limits) when width > limits.max_value_bytes,
    do: value_size_limit(:value_decode, 1)

  defp decode_bits(input, width, _type, _limits) when byte_size(input) < width,
    do: {:more, width - byte_size(input)}

  defp decode_bits(input, width, type, _limits) do
    <<bits::binary-size(width), rest::binary>> = input
    {:ok, %AMQPValue{type: type, value: bits}, rest}
  end

  defp decode_char(_input, limits) when limits.max_value_bytes < 4,
    do: value_size_limit(:value_decode, 1)

  defp decode_char(input, _limits) when byte_size(input) < 4,
    do: {:more, 4 - byte_size(input)}

  defp decode_char(<<value::32, rest::binary>>, _limits) do
    if value <= 0x10FFFF and value not in 0xD800..0xDFFF,
      do: {:ok, AMQPValue.char(value), rest},
      else: {:error, Error.new(:value_decode, :malformed, :invalid_char, offset: 1)}
  end

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
        MapSet.member?(keys, key) -> {:halt, :duplicate_map_key}
        true -> {:cont, MapSet.put(keys, key)}
      end
    end)
    |> case do
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

  defp decode_signed(_input, width, _constructor, limits) when width > limits.max_value_bytes,
    do: value_size_limit(:value_decode, 1)

  defp decode_signed(input, width, _constructor, _limits) when byte_size(input) < width,
    do: {:more, width - byte_size(input)}

  defp decode_signed(input, width, constructor, _limits) do
    <<value::signed-big-integer-size(width)-unit(8), rest::binary>> = input
    {:ok, constructor.(value), rest}
  end

  defp decode_uuid(_input, limits) when limits.max_value_bytes < 16,
    do: value_size_limit(:value_decode, 1)

  defp decode_uuid(input, _limits) when byte_size(input) < 16,
    do: {:more, 16 - byte_size(input)}

  defp decode_uuid(<<value::binary-size(16), rest::binary>>, _limits),
    do: {:ok, AMQPValue.uuid(value), rest}

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

  # Measure the complete encoding before allocating any variable or compound
  # wire payload. Each container stops when its running declared size exceeds
  # its budget, including count-width and shared-array-constructor changes.
  defp encoding_size(%AMQPValue{type: type, value: value}, limits, _depth)
       when type in [:binary, :string, :symbol] and is_binary(value) do
    with :ok <- validate_value_size(value, limits),
         :ok <- validate_payload(type, value, :value_encode, nil) do
      {:ok, 1 + if(byte_size(value) <= 255, do: 1, else: 4) + byte_size(value)}
    end
  end

  defp encoding_size(%AMQPValue{type: :list, value: []}, limits, depth) do
    with :ok <- validate_nesting_depth(:value_encode, limits, depth), do: {:ok, 1}
  end

  defp encoding_size(%AMQPValue{type: :list, value: values}, limits, depth)
       when is_list(values),
       do: compound_encoding_size(values, :list, limits, depth)

  defp encoding_size(%AMQPValue{type: :map, value: entries}, limits, depth)
       when is_list(entries) do
    count = length(entries) * 2

    with :ok <- validate_nesting_depth(:value_encode, limits, depth),
         :ok <- validate_compound_count(count, :map, limits, :value_encode),
         {:ok, _size} <- compound_wire_size(count, 0, 0, limits) do
      entries
      |> Enum.reduce_while({:ok, 0, MapSet.new()}, fn
        {%AMQPValue{} = key, %AMQPValue{} = value}, {:ok, bytes, keys} ->
          if MapSet.member?(keys, key) do
            {:halt, invalid_compound_value(:duplicate_map_key)}
          else
            with {:ok, key_size} <- encoding_size(key, limits, depth + 1),
                 {:ok, _size} <- compound_wire_size(count, bytes + key_size, 0, limits),
                 {:ok, value_size} <- encoding_size(value, limits, depth + 1),
                 {:ok, _size} <-
                   compound_wire_size(count, bytes + key_size + value_size, 0, limits) do
              {:cont, {:ok, bytes + key_size + value_size, MapSet.put(keys, key)}}
            else
              error -> {:halt, error}
            end
          end

        _invalid, _acc ->
          {:halt, invalid_compound_value(:invalid_semantic_value)}
      end)
      |> case do
        {:ok, bytes, _keys} -> compound_wire_size(count, bytes, 0, limits)
        error -> error
      end
    end
  end

  defp encoding_size(%AMQPValue{type: :array, value: %AMQPValue.Array{} = array}, limits, depth) do
    with {:ok, info} <- array_measure(array, limits, depth), do: {:ok, info.wire_size}
  end

  defp encoding_size(
         %AMQPValue{
           type: :described,
           value: %AMQPValue.Described{
             descriptor: %AMQPValue{} = descriptor,
             value: %AMQPValue{} = value
           }
         },
         limits,
         depth
       ) do
    with :ok <- validate_nesting_depth(:value_encode, limits, depth),
         :ok <- validate_descriptor(descriptor, :value_encode),
         {:ok, descriptor_size} <- encoding_size(descriptor, limits, depth + 1),
         :ok <- validate_total_value_size(1 + descriptor_size, limits, :value_encode),
         {:ok, value_size} <- encoding_size(value, limits, depth + 1),
         :ok <-
           validate_total_value_size(1 + descriptor_size + value_size, limits, :value_encode) do
      {:ok, 1 + descriptor_size + value_size}
    end
  end

  defp encoding_size(%AMQPValue{} = value, limits, depth) do
    # The remaining supported encodings are fixed scalars of at most 17 bytes.
    # Reuse their exact range/type validation rather than duplicating it here.
    with {:ok, encoded} <- encode_value(value, limits, depth), do: {:ok, byte_size(encoded)}
  end

  defp encoding_size(_value, _limits, _depth),
    do: invalid_compound_value(:invalid_semantic_value)

  defp compound_encoding_size(values, type, limits, depth) do
    count = length(values)

    with :ok <- validate_nesting_depth(:value_encode, limits, depth),
         :ok <- validate_compound_count(count, type, limits, :value_encode),
         {:ok, _size} <- compound_wire_size(count, 0, 0, limits) do
      values
      |> Enum.reduce_while({:ok, 0}, fn value, {:ok, bytes} ->
        with {:ok, size} <- encoding_size(value, limits, depth + 1),
             {:ok, _size} <- compound_wire_size(count, bytes + size, 0, limits) do
          {:cont, {:ok, bytes + size}}
        else
          error -> {:halt, error}
        end
      end)
      |> case do
        {:ok, bytes} -> compound_wire_size(count, bytes, 0, limits)
        error -> error
      end
    end
  end

  defp compound_wire_size(count, item_bytes, constructor_bytes, limits) do
    width = if count <= 255 and 1 + constructor_bytes + item_bytes <= 255, do: 1, else: 4
    size = width + constructor_bytes + item_bytes

    with :ok <- validate_compound_value_size(size, limits, :value_encode) do
      {:ok, 1 + width + size}
    end
  end

  defp encode_value(%AMQPValue{type: :null, value: nil}, _limits, _depth), do: {:ok, <<0x40>>}

  defp encode_value(%AMQPValue{type: :boolean, value: true}, _limits, _depth),
    do: {:ok, <<0x41>>}

  defp encode_value(%AMQPValue{type: :boolean, value: false}, _limits, _depth),
    do: {:ok, <<0x42>>}

  defp encode_value(%AMQPValue{type: :ubyte, value: value}, _limits, _depth)
       when is_integer(value) and value in 0..255,
       do: {:ok, <<0x50, value>>}

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

  defp encode_value(%AMQPValue{type: :byte, value: value}, limits, _depth)
       when is_integer(value) and value in -128..127,
       do: encode_signed(0x51, value, 1, limits)

  defp encode_value(%AMQPValue{type: :short, value: value}, limits, _depth)
       when is_integer(value) and value in -32_768..32_767,
       do: encode_signed(0x61, value, 2, limits)

  defp encode_value(%AMQPValue{type: :int, value: value}, limits, _depth)
       when is_integer(value) and value in -128..127,
       do: encode_signed(0x54, value, 1, limits)

  defp encode_value(%AMQPValue{type: :int, value: value}, limits, _depth)
       when is_integer(value) and value in -2_147_483_648..2_147_483_647,
       do: encode_signed(0x71, value, 4, limits)

  defp encode_value(%AMQPValue{type: :long, value: value}, limits, _depth)
       when is_integer(value) and value in -128..127,
       do: encode_signed(0x55, value, 1, limits)

  defp encode_value(%AMQPValue{type: :long, value: value}, limits, _depth)
       when is_integer(value) and value in -9_223_372_036_854_775_808..9_223_372_036_854_775_807,
       do: encode_signed(0x81, value, 8, limits)

  defp encode_value(%AMQPValue{type: type, value: value}, limits, _depth)
       when type in [:float, :double, :decimal32, :decimal64, :decimal128] and is_binary(value) do
    {code, width} = scalar_bits_type(type)

    if byte_size(value) == width do
      with :ok <- validate_value_size(value, limits), do: {:ok, <<code, value::binary>>}
    else
      invalid_compound_value(:invalid_semantic_value)
    end
  end

  defp encode_value(%AMQPValue{type: :char, value: value}, limits, _depth)
       when is_integer(value) and value in 0..0x10FFFF and value not in 0xD800..0xDFFF,
       do: encode_signed(0x73, value, 4, limits)

  defp encode_value(%AMQPValue{type: :timestamp, value: value}, limits, _depth)
       when is_integer(value) and value in -9_223_372_036_854_775_808..9_223_372_036_854_775_807,
       do: encode_signed(0x83, value, 8, limits)

  defp encode_value(%AMQPValue{type: :uuid, value: <<_::128>> = value}, limits, _depth) do
    with :ok <- validate_value_size(value, limits), do: {:ok, <<0x98, value::binary>>}
  end

  defp encode_value(%AMQPValue{type: type, value: value}, limits, _depth)
       when type in [:binary, :string, :symbol] and is_binary(value) do
    with :ok <- validate_value_size(value, limits),
         :ok <- validate_payload(type, value, :value_encode, nil) do
      encode_variable(type, value)
    end
  end

  defp encode_value(%AMQPValue{type: :list, value: values}, limits, depth) when is_list(values),
    do: encode_list(values, limits, depth)

  defp encode_value(%AMQPValue{type: :map, value: entries}, limits, depth) when is_list(entries),
    do: encode_map(entries, limits, depth)

  defp encode_value(%AMQPValue{type: :array, value: %AMQPValue.Array{} = array}, limits, depth),
    do: encode_general_array(array, limits, depth)

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

  defp encode_signed(format_code, value, width, limits) do
    if width <= limits.max_value_bytes,
      do: {:ok, <<format_code, value::signed-big-integer-size(width)-unit(8)>>},
      else: value_size_limit(:value_encode, nil)
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

  defp scalar_bits_type(:float), do: {0x72, 4}
  defp scalar_bits_type(:double), do: {0x82, 8}
  defp scalar_bits_type(:decimal32), do: {0x74, 4}
  defp scalar_bits_type(:decimal64), do: {0x84, 8}
  defp scalar_bits_type(:decimal128), do: {0x94, 16}

  # Array planning measures constructor promotion and every body before the
  # encoder builds any aggregate bytes. Compound elements use common 32-bit
  # constructors; variable scalar elements retain the shortest common width.
  defp array_measure(array, limits, depth, outer_width \\ :canonical) do
    with :ok <- validate_nesting_depth(:value_encode, limits, depth),
         {:ok, count} <- array_count(array.values, limits.max_compound_items, 0),
         {:ok, plan, constructor_size} <-
           array_constructor(array.element_type, limits, depth + 1),
         {:ok, _} <- array_wire_info(count, 0, constructor_size, limits, outer_width) do
      Enum.reduce_while(array.values, {:ok, plan, 0, 0}, fn value, {:ok, plan, bytes, seen} ->
        with {:ok, plan, promotion} <- promote_array_constructor(plan, value),
             {:ok, size} <- array_body_size(value, plan, limits, depth + 1),
             bytes = bytes + size + promotion * seen,
             {:ok, _} <- array_wire_info(count, bytes, constructor_size, limits, outer_width) do
          {:cont, {:ok, plan, bytes, seen + 1}}
        else
          error -> {:halt, error}
        end
      end)
      |> case do
        {:ok, plan, bytes, _} ->
          with {:ok, info} <-
                 array_wire_info(count, bytes, constructor_size, limits, outer_width),
               do: {:ok, Map.merge(info, %{plan: plan, count: count})}

        error ->
          error
      end
    end
  end

  defp array_count([], _maximum, count), do: {:ok, count}

  defp array_count([_ | _], maximum, count) when count >= maximum,
    do: {:error, Error.new(:value_encode, :limit_exceeded, :compound_item_limit)}

  defp array_count([_ | rest], maximum, count), do: array_count(rest, maximum, count + 1)
  defp array_count(_, _, _), do: invalid_compound_value(:invalid_semantic_value)

  defp array_wire_info(count, bytes, constructor_size, limits, requested_width) do
    width =
      if requested_width == 4 or count > 255 or 1 + constructor_size + bytes > 255, do: 4, else: 1

    size = width + constructor_size + bytes

    with :ok <- validate_compound_value_size(size, limits, :value_encode),
         do: {:ok, %{width: width, size: size, wire_size: 1 + width + size}}
  end

  defp array_constructor({:described, %AMQPValue{} = descriptor, inner}, limits, depth) do
    with :ok <- validate_nesting_depth(:value_encode, limits, depth),
         :ok <- validate_descriptor(descriptor, :value_encode),
         {:ok, descriptor_size} <- encoding_size(descriptor, limits, depth + 1),
         :ok <- validate_total_value_size(1 + descriptor_size, limits, :value_encode),
         {:ok, plan, size} <- array_constructor(inner, limits, depth + 1) do
      {:ok, {:described, descriptor, plan}, 1 + descriptor_size + size}
    end
  end

  defp array_constructor(type, _limits, _depth) do
    code =
      case type do
        :boolean -> 0x56
        :ubyte -> 0x50
        :ushort -> 0x60
        :uint -> 0x70
        :ulong -> 0x80
        :byte -> 0x51
        :short -> 0x61
        :int -> 0x71
        :long -> 0x81
        :float -> 0x72
        :double -> 0x82
        :decimal32 -> 0x74
        :decimal64 -> 0x84
        :decimal128 -> 0x94
        :char -> 0x73
        :timestamp -> 0x83
        :uuid -> 0x98
        :binary -> 0xA0
        :string -> 0xA1
        :symbol -> 0xA3
        :list -> 0xD0
        :map -> 0xD1
        :array -> 0xF0
        _ -> nil
      end

    if code,
      do: {:ok, {code, Map.fetch!(@array_constructors, code)}, 1},
      else: invalid_compound_value(:invalid_semantic_value)
  end

  defp promote_array_constructor(
         {:described, descriptor, plan},
         %AMQPValue{
           type: :described,
           value: %AMQPValue.Described{descriptor: descriptor, value: value}
         }
       ) do
    with {:ok, plan, promotion} <- promote_array_constructor(plan, value),
         do: {:ok, {:described, descriptor, plan}, promotion}
  end

  defp promote_array_constructor({code, {type, :variable, 1}}, %AMQPValue{
         type: type,
         value: value
       })
       when is_binary(value) and byte_size(value) > 255,
       do: {:ok, {code + 0x10, {type, :variable, 4}}, 3}

  defp promote_array_constructor({_code, {type, _, _}} = plan, %AMQPValue{type: type}),
    do: {:ok, plan, 0}

  defp promote_array_constructor(_, _), do: invalid_compound_value(:invalid_semantic_value)

  defp array_body_size(
         %AMQPValue{
           type: :described,
           value: %AMQPValue.Described{descriptor: descriptor, value: value}
         },
         {:described, descriptor, plan},
         limits,
         depth
       ) do
    with :ok <- validate_nesting_depth(:value_encode, limits, depth),
         do: array_body_size(value, plan, limits, depth + 1)
  end

  defp array_body_size(
         %AMQPValue{type: type, value: bytes} = value,
         {_code, {type, :variable, width}},
         limits,
         depth
       )
       when is_binary(bytes) do
    with {:ok, _} <- encoding_size(value, limits, depth), do: {:ok, width + byte_size(bytes)}
  end

  defp array_body_size(
         %AMQPValue{type: :array, value: %AMQPValue.Array{} = array},
         {_code, {:array, :array, 4}},
         limits,
         depth
       ) do
    with {:ok, info} <- array_measure(array, limits, depth, 4), do: {:ok, info.wire_size - 1}
  end

  defp array_body_size(
         %AMQPValue{type: type} = value,
         {_code, {type, :compound, 4}},
         limits,
         depth
       ),
       do: compound32_body_size(value, limits, depth)

  defp array_body_size(%AMQPValue{type: type} = value, {_code, {type, _, width}}, limits, depth) do
    with {:ok, _} <- encoding_size(value, limits, depth), do: {:ok, width}
  end

  defp array_body_size(_, _, _, _), do: invalid_compound_value(:invalid_semantic_value)

  defp compound32_body_size(%AMQPValue{type: :list, value: values}, limits, depth)
       when is_list(values) do
    with :ok <- validate_nesting_depth(:value_encode, limits, depth),
         :ok <- validate_compound_count(length(values), :list, limits, :value_encode),
         :ok <- validate_compound_value_size(4, limits, :value_encode) do
      Enum.reduce_while(values, {:ok, 8}, fn value, {:ok, size} ->
        with {:ok, child_size} <- encoding_size(value, limits, depth + 1),
             :ok <- validate_compound_value_size(size + child_size - 4, limits, :value_encode),
             do: {:cont, {:ok, size + child_size}},
             else: (error -> {:halt, error})
      end)
    end
  end

  defp compound32_body_size(%AMQPValue{type: :map, value: entries}, limits, depth)
       when is_list(entries) do
    with :ok <- validate_nesting_depth(:value_encode, limits, depth),
         :ok <- validate_compound_count(length(entries) * 2, :map, limits, :value_encode),
         :ok <- validate_compound_value_size(4, limits, :value_encode) do
      Enum.reduce_while(entries, {:ok, 8, MapSet.new()}, fn
        {%AMQPValue{} = key, %AMQPValue{} = value}, {:ok, size, keys} ->
          if MapSet.member?(keys, key) do
            {:halt, invalid_compound_value(:duplicate_map_key)}
          else
            with {:ok, key_size} <- encoding_size(key, limits, depth + 1),
                 :ok <- validate_compound_value_size(size + key_size - 4, limits, :value_encode),
                 {:ok, value_size} <- encoding_size(value, limits, depth + 1),
                 :ok <-
                   validate_compound_value_size(
                     size + key_size + value_size - 4,
                     limits,
                     :value_encode
                   ),
                 do: {:cont, {:ok, size + key_size + value_size, MapSet.put(keys, key)}},
                 else: (error -> {:halt, error})
          end

        _, _ ->
          {:halt, invalid_compound_value(:invalid_semantic_value)}
      end)
      |> case do
        {:ok, size, _} -> {:ok, size}
        error -> error
      end
    end
  end

  defp compound32_body_size(_, _, _), do: invalid_compound_value(:invalid_semantic_value)

  defp encode_general_array(array, limits, depth) do
    with {:ok, info} <- array_measure(array, limits, depth),
         {:ok, body} <- encode_array_body(array, info, limits, depth) do
      code = if info.width == 1, do: 0xE0, else: 0xF0
      {:ok, IO.iodata_to_binary([<<code>>, body])}
    end
  end

  defp encode_array_body(array, info, limits, depth) do
    with {:ok, constructor} <- encode_array_constructor(info.plan, limits, depth + 1) do
      values = Enum.map(array.values, &encode_array_element(&1, info.plan, limits, depth + 1))

      {:ok,
       [
         <<info.size::unsigned-big-integer-size(info.width)-unit(8),
           info.count::unsigned-big-integer-size(info.width)-unit(8)>>,
         constructor,
         values
       ]}
    end
  end

  defp encode_array_constructor({:described, descriptor, inner}, limits, depth) do
    with {:ok, descriptor} <- encode_value(descriptor, limits, depth + 1),
         {:ok, inner} <- encode_array_constructor(inner, limits, depth + 1),
         do: {:ok, [<<0>>, descriptor, inner]}
  end

  defp encode_array_constructor({code, _}, _limits, _depth), do: {:ok, <<code>>}

  defp encode_array_element(
         %AMQPValue{value: %AMQPValue.Described{value: value}},
         {:described, _descriptor, inner},
         limits,
         depth
       ),
       do: encode_array_element(value, inner, limits, depth + 1)

  defp encode_array_element(
         %AMQPValue{value: value},
         {_code, {_type, :unsigned, width}},
         _limits,
         _depth
       ),
       do: <<value::unsigned-big-integer-size(width)-unit(8)>>

  defp encode_array_element(
         %AMQPValue{value: value},
         {_code, {_type, :signed, width}},
         _limits,
         _depth
       ),
       do: <<value::signed-big-integer-size(width)-unit(8)>>

  defp encode_array_element(
         %AMQPValue{value: value},
         {_code, {:char, :char, 4}},
         _limits,
         _depth
       ),
       do: <<value::32>>

  defp encode_array_element(
         %AMQPValue{value: value},
         {_code, {:boolean, :boolean, 1}},
         _limits,
         _depth
       ),
       do: if(value, do: <<1>>, else: <<0>>)

  defp encode_array_element(
         %AMQPValue{value: value},
         {_code, {_type, :bits, _width}},
         _limits,
         _depth
       ),
       do: value

  defp encode_array_element(
         %AMQPValue{value: value},
         {_code, {_type, :variable, width}},
         _limits,
         _depth
       ),
       do: [<<byte_size(value)::unsigned-big-integer-size(width)-unit(8)>>, value]

  defp encode_array_element(
         %AMQPValue{type: type, value: values},
         {_code, {type, :compound, 4}},
         limits,
         depth
       ) do
    values = if type == :map, do: elem(map_values(values), 1), else: values
    {:ok, encoded} = encode_compound_items(values, limits, depth + 1)
    [<<4 + IO.iodata_length(encoded)::32, length(values)::32>>, encoded]
  end

  defp encode_array_element(
         %AMQPValue{value: %AMQPValue.Array{} = array},
         {_code, {:array, :array, 4}},
         limits,
         depth
       ) do
    {:ok, info} = array_measure(array, limits, depth, 4)
    {:ok, body} = encode_array_body(array, info, limits, depth)
    body
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

  defp variable_format_code(:binary, 1), do: 0xA0
  defp variable_format_code(:binary, 4), do: 0xB0
  defp variable_format_code(:string, 1), do: 0xA1
  defp variable_format_code(:string, 4), do: 0xB1
  defp variable_format_code(:symbol, 1), do: 0xA3
  defp variable_format_code(:symbol, 4), do: 0xB3

  defp semantic_value(:binary, value), do: AMQPValue.binary(value)
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

  defp validate_payload(:binary, _value, _operation, _offset), do: :ok

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
