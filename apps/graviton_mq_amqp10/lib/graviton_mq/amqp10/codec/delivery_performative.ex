defmodule GravitonMQ.AMQP10.Codec.DeliveryPerformative do
  @moduledoc false

  alias GravitonMQ.AMQP10.Codec.DeliveryState
  alias GravitonMQ.AMQP10.Codec.Error
  alias GravitonMQ.AMQP10.Codec.Limits
  alias GravitonMQ.AMQP10.Performative.Disposition
  alias GravitonMQ.AMQP10.Performative.Transfer
  alias GravitonMQ.AMQP10.Value
  alias GravitonMQ.AMQP10.Value.Described

  @transfer_fields [
    {:handle, :uint, :required},
    {:delivery_id, :uint, nil},
    {:delivery_tag, :binary, nil},
    {:message_format, :uint, nil},
    {:settled, :boolean, nil},
    {:more, :boolean, false},
    {:rcv_settle_mode, :receiver_settle_mode, nil},
    {:state, :delivery_state, nil},
    {:resume, :boolean, false},
    {:aborted, :boolean, false},
    {:batchable, :boolean, false}
  ]
  @disposition_fields [
    {:role, :boolean, :required},
    {:first, :uint, :required},
    {:last, :uint, nil},
    {:settled, :boolean, false},
    {:state, :delivery_state, nil},
    {:batchable, :boolean, false}
  ]

  def decode_value(
        %Value{type: :described, value: %Described{descriptor: descriptor, value: body}},
        operation,
        class,
        raw_state \\ nil,
        limits \\ Limits.default()
      ) do
    raw_state = {raw_state, limits}

    case descriptor do
      %Value{type: :ulong, value: 0x14} ->
        decode_body(Transfer, @transfer_fields, body, operation, class, raw_state)

      %Value{type: :symbol, value: "amqp:transfer:list"} ->
        decode_body(Transfer, @transfer_fields, body, operation, class, raw_state)

      %Value{type: :ulong, value: 0x15} ->
        decode_body(Disposition, @disposition_fields, body, operation, class, raw_state)

      %Value{type: :symbol, value: "amqp:disposition:list"} ->
        decode_body(Disposition, @disposition_fields, body, operation, class, raw_state)

      _ ->
        error(operation, :unsupported, :unknown_descriptor)
    end
  end

  def encode_value(%Transfer{} = value, operation, class),
    do: encode_fields(value, @transfer_fields, 0x14, operation, class)

  def encode_value(%Disposition{} = value, operation, class),
    do: encode_fields(value, @disposition_fields, 0x15, operation, class)

  defp decode_body(
         module,
         schema,
         %Value{type: :list, value: fields},
         operation,
         class,
         raw_state
       )
       when is_list(fields) do
    with {:ok, normalized} <- validate_fields(fields, schema, operation, class, raw_state) do
      {:ok, struct!(module, normalized)}
    end
  end

  defp decode_body(_module, _schema, _body, operation, class, _raw_state),
    do: error(operation, class, :invalid_performative_body)

  defp encode_fields(value, schema, descriptor, operation, class) do
    fields = Enum.map(schema, fn {name, _, _} -> Map.fetch!(value, name) end)

    with {:ok, normalized} <- validate_fields(fields, schema, operation, class, nil),
         {:ok, state_value} <- encode_state(Map.get(normalized, :state), operation, class) do
      fields =
        Enum.map(schema, fn
          {:state, _, _} -> state_value
          {name, :boolean, false} -> omit_false(Map.fetch!(normalized, name))
          {name, _, _} -> Map.fetch!(normalized, name)
        end)

      fields = fields |> Enum.reverse() |> Enum.drop_while(&is_nil/1) |> Enum.reverse()

      fields =
        Enum.map(fields, fn
          nil -> Value.null()
          value -> value
        end)

      {:ok, Value.described(Value.ulong(descriptor), Value.list(fields))}
    end
  end

  defp validate_fields(fields, schema, operation, class, raw_state) do
    if length(fields) > length(schema) do
      error(operation, class, :too_many_fields, actual: length(fields), maximum: length(schema))
    else
      schema
      |> Enum.with_index()
      |> Enum.reduce_while({:ok, %{}}, fn {{name, type, default}, index}, {:ok, acc} ->
        case field(
               Enum.at(fields, index),
               name,
               index,
               type,
               default,
               operation,
               class,
               raw_state
             ) do
          {:ok, value} -> {:cont, {:ok, Map.put(acc, name, value)}}
          {:error, %Error{}} = error -> {:halt, error}
        end
      end)
    end
  end

  defp field(
         %Value{type: :null, value: nil},
         name,
         index,
         type,
         default,
         operation,
         class,
         raw_state
       ),
       do: field(nil, name, index, type, default, operation, class, raw_state)

  defp field(nil, name, index, _type, :required, operation, class, _raw_state),
    do: error(operation, class, :mandatory_field_missing, field: name, index: index)

  defp field(nil, _name, _index, :boolean, false, _operation, _class, _raw_state),
    do: {:ok, Value.boolean(false)}

  defp field(nil, _name, _index, _type, nil, _operation, _class, _raw_state), do: {:ok, nil}

  defp field(value, _name, _index, :delivery_state, _, operation, class, raw_state) do
    if operation == :performative_encode do
      with {:ok, _value} <- DeliveryState.encode_value(value, operation, class), do: {:ok, value}
    else
      {original, limits} = raw_state
      DeliveryState.decode_value(value, operation, class, original, limits)
    end
  end

  defp field(%Value{type: :uint, value: value} = tagged, _name, _index, :uint, _, _, _, _)
       when is_integer(value) and value >= 0 and value <= 0xFFFF_FFFF,
       do: {:ok, tagged}

  defp field(%Value{type: :boolean, value: value} = tagged, _name, _index, :boolean, _, _, _, _)
       when is_boolean(value),
       do: {:ok, tagged}

  defp field(
         %Value{type: :binary, value: value} = tagged,
         name,
         index,
         :binary,
         _,
         operation,
         class,
         _
       )
       when is_binary(value) do
    if byte_size(value) <= 32,
      do: {:ok, tagged},
      else:
        error(operation, class, :field_value_out_of_range, field: name, index: index, maximum: 32)
  end

  defp field(
         %Value{type: :ubyte, value: value} = tagged,
         name,
         index,
         :receiver_settle_mode,
         _,
         operation,
         class,
         _
       )
       when is_integer(value) do
    if value in [0, 1],
      do: {:ok, tagged},
      else:
        error(operation, class, :field_value_out_of_range,
          field: name,
          index: index,
          allowed: [0, 1]
        )
  end

  defp field(value, name, index, type, _, operation, class, _raw_state) do
    actual =
      case value do
        %Value{type: actual} -> actual
        _ -> :invalid
      end

    error(operation, class, :field_type_mismatch,
      field: name,
      index: index,
      expected: type,
      actual: actual
    )
  end

  defp encode_state(nil, _operation, _class), do: {:ok, nil}

  defp encode_state(value, operation, class),
    do: DeliveryState.encode_value(value, operation, class)

  defp omit_false(%Value{type: :boolean, value: false}), do: nil
  defp omit_false(value), do: value

  defp error(operation, class, reason, details \\ []),
    do: {:error, Error.new(operation, class, reason, details: Map.new(details))}
end
