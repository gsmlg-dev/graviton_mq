defmodule GravitonMQ.AMQP10.Codec.Terminus do
  @moduledoc """
  Pure schema codec for AMQP 1.0 Source and Target composites.

  Standard fields and known delivery states are validated; described extension
  data is retained without interpreting or accepting its endpoint semantics.
  A later endpoint must explicitly negotiate or reject unsupported extensions.
  Coordinator and transaction states are explicitly unsupported.
  """

  alias GravitonMQ.AMQP10.Codec.{DeliveryState, Error, Limits, Value}
  alias GravitonMQ.AMQP10.Terminus.{Source, Target}
  alias GravitonMQ.AMQP10.Value, as: V
  alias GravitonMQ.AMQP10.Value.{Array, Described}

  @standard_descriptors %{
    0x10 => "amqp:open:list",
    0x11 => "amqp:begin:list",
    0x12 => "amqp:attach:list",
    0x13 => "amqp:flow:list",
    0x14 => "amqp:transfer:list",
    0x15 => "amqp:disposition:list",
    0x16 => "amqp:detach:list",
    0x17 => "amqp:end:list",
    0x18 => "amqp:close:list",
    0x1D => "amqp:error:list",
    0x23 => "amqp:received:list",
    0x24 => "amqp:accepted:list",
    0x25 => "amqp:rejected:list",
    0x26 => "amqp:released:list",
    0x27 => "amqp:modified:list",
    0x28 => "amqp:source:list",
    0x29 => "amqp:target:list",
    0x2B => "amqp:delete-on-close:list",
    0x2C => "amqp:delete-on-no-links:list",
    0x2D => "amqp:delete-on-no-messages:list",
    0x2E => "amqp:delete-on-no-links-or-messages:list",
    0x30 => "amqp:coordinator:list",
    0x31 => "amqp:declare:list",
    0x32 => "amqp:discharge:list",
    0x33 => "amqp:declared:list",
    0x34 => "amqp:transactional-state:list",
    0x40 => "amqp:sasl-mechanisms:list",
    0x41 => "amqp:sasl-init:list",
    0x42 => "amqp:sasl-challenge:list",
    0x43 => "amqp:sasl-response:list",
    0x44 => "amqp:sasl-outcome:list",
    0x70 => "amqp:header:list",
    0x71 => "amqp:delivery-annotations:map",
    0x72 => "amqp:message-annotations:map",
    0x73 => "amqp:properties:list",
    0x74 => "amqp:application-properties:map",
    0x75 => "amqp:data:binary",
    0x76 => "amqp:amqp-sequence:list",
    0x77 => "amqp:amqp-value:*",
    0x78 => "amqp:footer:map"
  }
  @standard_codes Map.keys(@standard_descriptors)
  @symbol_codes Map.new(@standard_descriptors, fn {code, symbol} -> {symbol, code} end)

  defguardp absent(value)
            when is_nil(value) or
                   (is_struct(value, V) and value.type == :null and is_nil(value.value))

  @spec decode(binary(), Limits.t()) ::
          {:ok, Source.t() | Target.t(), binary()}
          | {:more, pos_integer()}
          | {:error, Error.t()}
  def decode(input, limits \\ Limits.default()) do
    with {:ok, value, rest} <- Value.decode(input, limits),
         {:ok, terminus} <- decode_value(value, :performative_decode, :malformed) do
      {:ok, terminus, rest}
    end
  end

  @spec encode(Source.t() | Target.t(), Limits.t()) :: {:ok, binary()} | {:error, Error.t()}
  def encode(terminus, limits \\ Limits.default()) do
    with {:ok, value} <- encode_value(terminus, :performative_encode, :invalid_value) do
      Value.encode(value, limits)
    end
  end

  @spec decode_value(V.t(), Error.operation(), Error.class()) ::
          {:ok, Source.t() | Target.t()} | {:error, Error.t()}
  def decode_value(
        %V{type: :described, value: %Described{descriptor: descriptor, value: body}},
        operation,
        class
      ) do
    case descriptor_code(descriptor) do
      code when code in [0x28, 0x29] -> decode_body(code, body, operation, class)
      code when code in 0x30..0x34 -> error(operation, :unsupported, :transactions_unsupported)
      _ -> error(operation, :unsupported, :unknown_terminus)
    end
  end

  def decode_value(_, operation, class), do: error(operation, class, :invalid_terminus)

  @spec encode_value(Source.t() | Target.t(), Error.operation(), Error.class()) ::
          {:ok, V.described_value()} | {:error, Error.t()}
  def encode_value(%module{} = terminus, operation, class) when module in [Source, Target] do
    code = if module == Source, do: 0x28, else: 0x29
    specs = schema(code)
    fields = Enum.map(specs, fn {name, _, _} -> Map.get(terminus, name) end)

    with {:ok, normalized} <- validate_fields(code, fields, operation, class),
         {:ok, normalized} <- canonical_default_outcome(normalized, operation, class) do
      canonical =
        Enum.map(specs, fn {name, _, default} ->
          value = Map.fetch!(normalized, name)
          if value == default, do: V.null(), else: value || V.null()
        end)

      fields =
        canonical
        |> Enum.reverse()
        |> Enum.drop_while(fn value -> absent(value) end)
        |> Enum.reverse()

      {:ok, V.described(V.ulong(code), V.list(fields))}
    end
  end

  def encode_value(value, operation, class) do
    case value do
      %V{type: :described, value: %Described{descriptor: descriptor}} ->
        if descriptor_code(descriptor) in 0x30..0x34,
          do: error(operation, :unsupported, :transactions_unsupported),
          else: error(operation, class, :invalid_terminus)

      _ ->
        error(operation, class, :invalid_terminus)
    end
  end

  # Attach and Source share precisely the same tagged delivery-state validation.
  # Retaining extensions here is data preservation, not protocol negotiation.
  @doc false
  def validate_delivery_state(value, outcome_only, operation, class) do
    reason = if outcome_only, do: :invalid_outcome, else: :invalid_delivery_state

    case value do
      %V{type: :described, value: %Described{descriptor: descriptor, value: %V{}}} ->
        case descriptor_code(descriptor) do
          code when code in 0x30..0x34 ->
            error(operation, :unsupported, :transactions_unsupported)

          0x23 when outcome_only ->
            error(operation, class, :invalid_outcome)

          code when code in 0x23..0x27 ->
            with {:ok, _state} <- DeliveryState.decode_value(value, operation, class),
                 do: {:ok, value}

          :extension ->
            {:ok, value}

          _ ->
            error(operation, class, reason)
        end

      _ ->
        error(operation, class, reason)
    end
  end

  @doc false
  def extension_value?(%V{
        type: :described,
        value: %Described{descriptor: descriptor, value: %V{}}
      }),
      do: descriptor_code(descriptor) == :extension

  def extension_value?(_), do: false

  @doc false
  def canonical_delivery_state(value, operation, class) do
    with {:ok, _} <- validate_delivery_state(value, false, operation, class) do
      if extension_value?(value) do
        {:ok, value}
      else
        with {:ok, state} <- DeliveryState.decode_value(value, operation, class),
             do: DeliveryState.encode_value(state, operation, class)
      end
    end
  end

  defp canonical_default_outcome(%Source{default_outcome: value} = source, operation, class)
       when not is_nil(value) do
    with {:ok, value} <- canonical_delivery_state(value, operation, class),
         do: {:ok, %{source | default_outcome: value}}
  end

  defp canonical_default_outcome(terminus, _, _), do: {:ok, terminus}

  defp decode_body(code, %V{type: :list, value: fields}, operation, class) when is_list(fields),
    do: validate_fields(code, fields, operation, class)

  defp decode_body(_, _, operation, class), do: error(operation, class, :invalid_terminus_body)

  defp schema(code) do
    common = [
      {:address, :address, nil},
      {:durable, {:choice, :uint, [0, 1, 2]}, V.uint(0)},
      {:expiry_policy,
       {:choice, :symbol, ["link-detach", "session-end", "connection-close", "never"]},
       V.symbol("session-end")},
      {:timeout, :uint, V.uint(0)},
      {:dynamic, :boolean, V.boolean(false)},
      {:dynamic_node_properties, :node_properties, nil}
    ]

    if code == 0x28 do
      common ++
        [
          {:distribution_mode, :symbol, nil},
          {:filter, :filter, nil},
          {:default_outcome, :outcome, nil},
          {:outcomes, :outcomes, nil},
          {:capabilities, :symbols, nil}
        ]
    else
      common ++ [{:capabilities, :symbols, nil}]
    end
  end

  defp validate_fields(code, fields, operation, class) do
    specs = schema(code)

    if length(fields) > length(specs) do
      error(operation, class, :too_many_fields, %{actual: length(fields), maximum: length(specs)})
    else
      result =
        specs
        |> Enum.with_index()
        |> Enum.reduce_while(
          {:ok, %{}},
          fn {{name, type, default}, index}, {:ok, acc} ->
            value = Enum.at(fields, index)

            validated =
              if absent(value),
                do: {:ok, default},
                else: validate_field(value, type, name, index, operation, class)

            case validated do
              {:ok, value} -> {:cont, {:ok, Map.put(acc, name, value)}}
              {:error, _} = failure -> {:halt, failure}
            end
          end
        )

      with {:ok, values} <- result,
           :ok <- validate_dynamic_properties(values, operation, class) do
        {:ok, struct(if(code == 0x28, do: Source, else: Target), values)}
      end
    end
  end

  defp validate_field(value, {:choice, type, choices}, name, index, operation, class) do
    with {:ok, value} <- validate_field(value, type, name, index, operation, class) do
      if value.value in choices,
        do: {:ok, value},
        else: error(operation, class, :field_value_out_of_range, %{field: name, index: index})
    end
  end

  defp validate_field(%V{type: :string, value: text} = value, :address, _, _, _, _)
       when is_binary(text),
       do: {:ok, value}

  defp validate_field(
         %V{type: :described, value: %Described{descriptor: descriptor, value: %V{}}} = value,
         :address,
         name,
         index,
         operation,
         class
       ) do
    if descriptor_code(descriptor) == :extension,
      do: {:ok, value},
      else: mismatch(value, :address, name, index, operation, class)
  end

  defp validate_field(%V{type: :uint, value: n} = value, :uint, _, _, _, _)
       when n in 0..4_294_967_295,
       do: {:ok, value}

  defp validate_field(%V{type: :boolean, value: b} = value, :boolean, _, _, _, _)
       when is_boolean(b),
       do: {:ok, value}

  defp validate_field(%V{type: :symbol, value: s} = value, :symbol, _, _, _, _)
       when is_binary(s),
       do: {:ok, value}

  defp validate_field(value, :symbols, name, index, operation, class) do
    if symbol_values(value) != :invalid,
      do: {:ok, value},
      else: mismatch(value, :symbol_or_symbol_array, name, index, operation, class)
  end

  defp validate_field(value, :outcomes, name, index, operation, class) do
    case symbol_values(value) do
      :invalid ->
        mismatch(value, :symbol_or_symbol_array, name, index, operation, class)

      symbols ->
        if Enum.all?(symbols, fn s ->
             Map.get(@symbol_codes, s, :extension) in [0x24, 0x25, 0x26, 0x27, :extension]
           end),
           do: {:ok, value},
           else: error(operation, class, :invalid_outcome, %{field: name, index: index})
    end
  end

  defp validate_field(%V{type: :described} = value, :outcome, _, _, operation, class),
    do: validate_delivery_state(value, true, operation, class)

  defp validate_field(%V{type: :map, value: entries} = value, kind, name, index, operation, class)
       when kind in [:node_properties, :filter] and is_list(entries) do
    if Enum.all?(entries, fn
         {%V{type: :symbol, value: key}, %V{}} when is_binary(key) -> true
         _ -> false
       end) do
      result =
        Enum.reduce_while(entries, :ok, fn {key, entry}, :ok ->
          case validate_entry(kind, key.value, entry, operation, class) do
            :ok -> {:cont, :ok}
            {:error, _} = failure -> {:halt, failure}
          end
        end)

      with :ok <- result, do: {:ok, value}
    else
      error(operation, class, :invalid_property_key, %{field: name, index: index})
    end
  end

  defp validate_field(value, type, name, index, operation, class),
    do: mismatch(value, type, name, index, operation, class)

  defp validate_entry(:filter, _, value, _, _) when absent(value), do: :ok

  defp validate_entry(
         :filter,
         _,
         %V{type: :described, value: %Described{descriptor: descriptor, value: %V{}}},
         operation,
         class
       ) do
    if descriptor_code(descriptor) == :extension,
      do: :ok,
      else: error(operation, class, :invalid_filter_value)
  end

  defp validate_entry(:filter, _, _, operation, class),
    do: error(operation, class, :invalid_filter_value)

  defp validate_entry(
         :node_properties,
         "lifetime-policy",
         %V{type: :described, value: %Described{descriptor: descriptor, value: body}},
         operation,
         class
       ) do
    case {descriptor_code(descriptor), body} do
      {code, %V{type: :list, value: []}} when code in 0x2B..0x2E -> :ok
      {:extension, %V{}} -> :ok
      _ -> error(operation, class, :invalid_node_property)
    end
  end

  defp validate_entry(:node_properties, "lifetime-policy", _, operation, class),
    do: error(operation, class, :invalid_node_property)

  defp validate_entry(:node_properties, "supported-dist-modes", value, operation, class) do
    case symbol_values(value) do
      :invalid ->
        error(operation, class, :invalid_node_property)

      [] ->
        error(operation, class, :invalid_node_property)

      _values ->
        :ok
    end
  end

  defp validate_entry(:node_properties, _, _, _, _), do: :ok

  defp validate_dynamic_properties(%{dynamic: %V{value: true}}, _, _), do: :ok
  defp validate_dynamic_properties(%{dynamic_node_properties: nil}, _, _), do: :ok

  defp validate_dynamic_properties(_, operation, class),
    do:
      error(operation, class, :dynamic_node_properties_without_dynamic, %{
        field: :dynamic_node_properties,
        index: 5
      })

  defp symbol_values(%V{type: :symbol, value: s}) when is_binary(s), do: [s]

  defp symbol_values(%V{type: :array, value: %Array{element_type: :symbol, values: values}})
       when is_list(values) do
    if Enum.all?(values, &match?(%V{type: :symbol, value: s} when is_binary(s), &1)),
      do: Enum.map(values, & &1.value),
      else: :invalid
  end

  defp symbol_values(_), do: :invalid

  defp descriptor_code(%V{type: :ulong, value: code}) when code in @standard_codes, do: code

  defp descriptor_code(%V{type: :ulong, value: code})
       when code in 0..18_446_744_073_709_551_615,
       do: :extension

  defp descriptor_code(%V{type: :symbol, value: s}) when is_binary(s),
    do: Map.get(@symbol_codes, s, :extension)

  defp descriptor_code(_), do: :invalid

  defp mismatch(value, expected, name, index, operation, class) do
    actual = if is_struct(value, V), do: value.type, else: :invalid

    error(operation, class, :field_type_mismatch, %{
      field: name,
      index: index,
      expected: expected,
      actual: actual
    })
  end

  defp error(operation, class, reason, details \\ %{}),
    do: {:error, Error.new(operation, class, reason, details: details)}
end
