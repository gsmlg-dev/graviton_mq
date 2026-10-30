defmodule GravitonMQ.AMQP10.Codec.Attach do
  @moduledoc """
  Pure AMQP 1.0 Attach schema codec, without Link or Session transitions.

  Exact tagged extension values and unsettled entries remain protocol data.
  Retaining an extension does not accept its endpoint semantics; unsupported
  negotiated behavior must be rejected by a later endpoint. Transactions are
  explicitly unsupported.
  """

  alias GravitonMQ.AMQP10.Codec.{Error, Limits, Terminus, Value}
  alias GravitonMQ.AMQP10.Performative.Attach, as: AttachData
  alias GravitonMQ.AMQP10.Terminus.{Source, Target}
  alias GravitonMQ.AMQP10.Value, as: V
  alias GravitonMQ.AMQP10.Value.{Array, Described}

  @fields [
    :name,
    :handle,
    :role,
    :snd_settle_mode,
    :rcv_settle_mode,
    :source,
    :target,
    :unsettled,
    :incomplete_unsettled,
    :initial_delivery_count,
    :max_message_size,
    :offered_capabilities,
    :desired_capabilities,
    :properties
  ]

  defguardp absent(value)
            when is_nil(value) or
                   (is_struct(value, V) and value.type == :null and is_nil(value.value))

  @spec decode(binary(), Limits.t()) ::
          {:ok, AttachData.t(), binary()}
          | {:more, pos_integer()}
          | {:error, Error.t()}
  def decode(input, limits \\ Limits.default()) do
    with {:ok, value, rest} <- Value.decode(input, limits),
         {:ok, attach} <- decode_value(value, :performative_decode, :malformed) do
      {:ok, attach, rest}
    end
  end

  @spec encode(AttachData.t(), Limits.t()) :: {:ok, binary()} | {:error, Error.t()}
  def encode(attach, limits \\ Limits.default()) do
    with {:ok, value} <- encode_value(attach, :performative_encode, :invalid_value) do
      Value.encode(value, limits)
    end
  end

  @spec decode_value(V.t(), Error.operation(), Error.class()) ::
          {:ok, AttachData.t()} | {:error, Error.t()}
  def decode_value(
        %V{type: :described, value: %Described{descriptor: descriptor, value: body}},
        operation,
        class
      )
      when descriptor in [
             %V{type: :ulong, value: 0x12},
             %V{type: :symbol, value: "amqp:attach:list"}
           ] do
    case body do
      %V{type: :list, value: fields} when is_list(fields) -> validate(fields, operation, class)
      _ -> error(operation, class, :invalid_attach_body)
    end
  end

  def decode_value(_, operation, class), do: error(operation, class, :invalid_attach)

  @spec encode_value(AttachData.t(), Error.operation(), Error.class()) ::
          {:ok, V.described_value()} | {:error, Error.t()}
  def encode_value(%AttachData{} = attach, operation, class) do
    fields = Enum.map(@fields, &Map.get(attach, &1))

    with {:ok, normalized} <- validate(fields, operation, class),
         {:ok, source} <- encoded_terminus(normalized.source, operation, class),
         {:ok, target} <- encoded_terminus(normalized.target, operation, class),
         {:ok, unsettled} <- canonical_unsettled(normalized.unsettled, operation, class) do
      fields =
        Enum.map(@fields, fn field ->
          value =
            case field do
              :source -> source
              :target -> target
              :unsettled -> unsettled
              _ -> Map.fetch!(normalized, field)
            end

          if value == default(field), do: nil, else: value
        end)

      fields =
        fields
        |> Enum.reverse()
        |> Enum.drop_while(&is_nil/1)
        |> Enum.reverse()
        |> Enum.map(fn value -> value || V.null() end)

      {:ok, V.described(V.ulong(0x12), V.list(fields))}
    end
  end

  def encode_value(_, operation, class), do: error(operation, class, :invalid_attach)

  defp validate(fields, operation, class) when length(fields) > 14,
    do: error(operation, class, :too_many_fields, %{maximum: 14, actual: length(fields)})

  defp validate(fields, operation, class) do
    result =
      @fields
      |> Enum.with_index()
      |> Enum.reduce_while(
        {:ok, %{}},
        fn {name, index}, {:ok, acc} ->
          value = Enum.at(fields, index)

          result =
            cond do
              absent(value) and index < 3 ->
                error(operation, class, :mandatory_field_missing, %{field: name, index: index})

              absent(value) ->
                {:ok, default(name)}

              true ->
                field(value, name, index, operation, class)
            end

          case result do
            {:ok, value} -> {:cont, {:ok, Map.put(acc, name, value)}}
            {:error, _} = failure -> {:halt, failure}
          end
        end
      )

    with {:ok, values} <- result,
         :ok <- initial_delivery_count(values, operation, class),
         :ok <- dynamic_addresses(values, operation, class) do
      {:ok, struct(AttachData, values)}
    end
  end

  defp default(:snd_settle_mode), do: V.ubyte(2)
  defp default(:rcv_settle_mode), do: V.ubyte(0)
  defp default(:incomplete_unsettled), do: V.boolean(false)
  defp default(_), do: nil

  defp field(%V{type: :string, value: s} = value, :name, _, _, _) when is_binary(s),
    do: {:ok, value}

  defp field(%V{type: :uint, value: n} = value, name, _, _, _)
       when name in [:handle, :initial_delivery_count] and n in 0..4_294_967_295,
       do: {:ok, value}

  defp field(%V{type: :boolean, value: b} = value, name, _, _, _)
       when name in [:role, :incomplete_unsettled] and is_boolean(b),
       do: {:ok, value}

  defp field(%V{type: :ubyte, value: n} = value, name, index, operation, class)
       when name in [:snd_settle_mode, :rcv_settle_mode] and n in 0..255 do
    maximum = if name == :snd_settle_mode, do: 2, else: 1

    if n <= maximum,
      do: {:ok, value},
      else: error(operation, class, :field_value_out_of_range, %{field: name, index: index})
  end

  defp field(%V{type: :ulong, value: n} = value, :max_message_size, _, _, _)
       when n in 0..18_446_744_073_709_551_615,
       do: {:ok, value}

  defp field(value, name, index, operation, class) when name in [:source, :target],
    do: terminus(value, name, index, operation, class)

  defp field(%V{type: :map, value: entries} = value, :unsettled, _, operation, class)
       when is_list(entries) do
    with :ok <- unsettled_entries(entries, operation, class), do: {:ok, value}
  end

  defp field(%V{type: :map, value: entries} = value, :properties, index, operation, class)
       when is_list(entries) do
    if Enum.all?(entries, fn
         {%V{type: :symbol, value: s}, %V{}} when is_binary(s) -> true
         _ -> false
       end),
       do: {:ok, value},
       else: error(operation, class, :invalid_property_key, %{field: :properties, index: index})
  end

  defp field(%V{type: :symbol, value: s} = value, name, _, _, _)
       when name in [:offered_capabilities, :desired_capabilities] and is_binary(s),
       do: {:ok, value}

  defp field(
         %V{type: :array, value: %Array{element_type: :symbol, values: values}} = value,
         name,
         index,
         operation,
         class
       )
       when name in [:offered_capabilities, :desired_capabilities] and is_list(values) do
    if Enum.all?(values, &match?(%V{type: :symbol, value: s} when is_binary(s), &1)),
      do: {:ok, value},
      else: mismatch(value, name, index, operation, class)
  end

  defp field(value, name, index, operation, class),
    do: mismatch(value, name, index, operation, class)

  defp terminus(%module{} = value, name, _, operation, class) when module in [Source, Target] do
    expected = if name == :source, do: Source, else: Target

    if module == expected do
      with {:ok, tagged} <- Terminus.encode_value(value, operation, class),
           do: Terminus.decode_value(tagged, operation, class)
    else
      error(operation, class, :wrong_terminus, %{field: name})
    end
  end

  defp terminus(%V{type: :described} = value, name, _, operation, class) do
    case Terminus.decode_value(value, operation, class) do
      {:ok, %module{} = decoded} ->
        expected = if name == :source, do: Source, else: Target

        if module == expected,
          do: {:ok, decoded},
          else: error(operation, class, :wrong_terminus, %{field: name})

      {:error, %Error{reason: :unknown_terminus}} ->
        if Terminus.extension_value?(value),
          do: {:ok, value},
          else: error(operation, class, :wrong_terminus, %{field: name})

      {:error, _} = failure ->
        failure
    end
  end

  defp terminus(value, name, index, operation, class),
    do: mismatch(value, name, index, operation, class)

  defp encoded_terminus(nil, _, _), do: {:ok, nil}
  defp encoded_terminus(%V{} = value, _, _), do: {:ok, value}

  defp encoded_terminus(value, operation, class),
    do: Terminus.encode_value(value, operation, class)

  defp canonical_unsettled(nil, _, _), do: {:ok, nil}

  defp canonical_unsettled(%V{type: :map, value: entries}, operation, class) do
    entries
    |> Enum.reduce_while({:ok, []}, fn {key, state}, {:ok, acc} ->
      result =
        if absent(state),
          do: {:ok, state},
          else: Terminus.canonical_delivery_state(state, operation, class)

      case result do
        {:ok, value} -> {:cont, {:ok, [{key, value} | acc]}}
        {:error, _} = failure -> {:halt, failure}
      end
    end)
    |> case do
      {:ok, values} -> {:ok, V.map(Enum.reverse(values))}
      {:error, _} = failure -> failure
    end
  end

  defp unsettled_entries(entries, operation, class) do
    Enum.reduce_while(entries, :ok, fn
      {%V{type: :binary, value: tag}, %V{} = state}, :ok
      when is_binary(tag) and byte_size(tag) <= 32 ->
        if absent(state) do
          {:cont, :ok}
        else
          case Terminus.validate_delivery_state(state, false, operation, class) do
            {:ok, _} -> {:cont, :ok}
            {:error, _} = failure -> {:halt, failure}
          end
        end

      _, :ok ->
        {:halt, error(operation, class, :invalid_unsettled_key, %{field: :unsettled, index: 7})}
    end)
  end

  defp initial_delivery_count(
         %{role: %V{value: false}, initial_delivery_count: nil},
         operation,
         class
       ),
       do:
         error(operation, class, :mandatory_field_missing, %{
           field: :initial_delivery_count,
           index: 9
         })

  defp initial_delivery_count(_, _, _), do: :ok

  defp dynamic_addresses(values, operation, class) do
    with :ok <-
           dynamic_address(values.source, values.role.value, true, :source, operation, class),
         :ok <-
           dynamic_address(values.target, values.role.value, false, :target, operation, class),
         do: :ok
  end

  defp dynamic_address(
         %{dynamic: %V{type: :boolean, value: true}, address: address},
         role,
         requester_role,
         name,
         operation,
         class
       ) do
    cond do
      role == requester_role and not is_nil(address) ->
        error(operation, class, :dynamic_address_forbidden, %{field: name})

      role != requester_role and is_nil(address) ->
        error(operation, class, :dynamic_address_required, %{field: name})

      true ->
        :ok
    end
  end

  defp dynamic_address(_, _, _, _, _, _), do: :ok

  defp mismatch(value, name, index, operation, class) do
    actual = if is_struct(value, V), do: value.type, else: :invalid

    error(operation, class, :field_type_mismatch, %{
      field: name,
      index: index,
      actual: actual,
      expected: expected_type(name)
    })
  end

  defp expected_type(:name), do: :string
  defp expected_type(name) when name in [:handle, :initial_delivery_count], do: :uint
  defp expected_type(name) when name in [:role, :incomplete_unsettled], do: :boolean
  defp expected_type(name) when name in [:snd_settle_mode, :rcv_settle_mode], do: :ubyte
  defp expected_type(:max_message_size), do: :ulong
  defp expected_type(name) when name in [:unsettled, :properties], do: :map
  defp expected_type(name) when name in [:source, :target], do: name
  defp expected_type(_), do: :symbol_or_symbol_array

  defp error(operation, class, reason, details \\ %{}),
    do: {:error, Error.new(operation, class, reason, details: details)}
end
