defmodule GravitonMQ.AMQP10.Codec.DeliveryState do
  @moduledoc """
  Pure bounded schema codec for Received, Accepted, Rejected, Released and Modified.

  Standalone decoding retains original nested Error and message-annotations
  bytes. `decode_value/3` has no original wire provenance and leaves `encoded_*`
  fields nil. `decode_value/4` accepts the complete original delivery-state
  encoding (or nil) to retain those bytes during performative decoding.
  Encoding uses canonical numeric descriptors; cached original bytes are data
  for lossless outcome mapping, not an alternate source of schema fields.
  Transactions and unknown delivery-state extensions are explicitly unsupported.
  """

  alias GravitonMQ.AMQP10.Codec.Error
  alias GravitonMQ.AMQP10.Codec.RawField
  alias GravitonMQ.AMQP10.Codec.Limits
  alias GravitonMQ.AMQP10.Codec.Value, as: WireValue
  alias GravitonMQ.AMQP10.DeliveryState
  alias GravitonMQ.AMQP10.DeliveryState.Received
  alias GravitonMQ.AMQP10.Error, as: ProtocolError
  alias GravitonMQ.AMQP10.Outcome
  alias GravitonMQ.AMQP10.Value, as: V
  alias GravitonMQ.AMQP10.Value.Described

  @states [
    {0x23, "amqp:received:list", :received, 2},
    {0x24, "amqp:accepted:list", :accepted, 0},
    {0x25, "amqp:rejected:list", :rejected, 1},
    {0x26, "amqp:released:list", :released, 0},
    {0x27, "amqp:modified:list", :modified, 3}
  ]

  @spec decode(binary(), Limits.t()) ::
          {:ok, DeliveryState.t(), binary()} | {:more, pos_integer()} | {:error, Error.t()}
  def decode(input, limits \\ Limits.default())

  def decode(input, limits) when is_binary(input) do
    with {:ok, value, rest} <- WireValue.decode(input, limits),
         original = binary_part(input, 0, byte_size(input) - byte_size(rest)),
         {:ok, state} <- decode_value(value, :value_decode, :malformed, original, limits) do
      {:ok, state, rest}
    end
  end

  def decode(_input, _limits), do: failure(:value_decode, :invalid_value, :invalid_delivery_state)

  @spec encode(DeliveryState.t(), Limits.t()) :: {:ok, binary()} | {:error, Error.t()}
  def encode(state, limits \\ Limits.default()) do
    with {:ok, value} <- encode_value(state, :value_encode, :invalid_value) do
      WireValue.encode(value, limits)
    end
  end

  @doc "Converts a delivery-state struct to a validated tagged composite."
  @spec encode_value(DeliveryState.t(), Error.operation(), Error.class()) ::
          {:ok, V.t()} | {:error, Error.t()}
  def encode_value(state, operation, class) do
    with {:ok, code, fields} <- state_fields(state, operation, class),
         value = tagged_composite(code, fields),
         {:ok, _state} <- decode_value(value, operation, class) do
      {:ok, value}
    end
  end

  @doc "Decodes schema data without original bytes; encoded provenance fields stay nil."
  @spec decode_value(V.t(), Error.operation(), Error.class()) ::
          {:ok, DeliveryState.t()} | {:error, Error.t()}
  def decode_value(value, operation, class), do: decode_value(value, operation, class, nil)

  @doc "Decodes schema data and optionally retains nested bytes from the original encoding."
  @spec decode_value(V.t(), Error.operation(), Error.class(), binary() | nil) ::
          {:ok, DeliveryState.t()} | {:error, Error.t()}
  def decode_value(value, operation, class, original),
    do: decode_value(value, operation, class, original, Limits.default())

  @doc "Decodes schema data and preserves original bytes under the caller's wire limits."
  @spec decode_value(V.t(), Error.operation(), Error.class(), binary() | nil, Limits.t()) ::
          {:ok, DeliveryState.t()} | {:error, Error.t()}
  def decode_value(value, operation, class, original, limits) do
    with {:ok, name, fields} <- composite_fields(value, operation, class),
         {:ok, state} <- decode_fields(name, fields, operation, class) do
      retain_original(state, value, original, limits, operation, class)
    end
  end

  @doc "Validates a nested Error value shared by delivery states and annotations."
  @spec decode_error_value(V.t(), Error.operation(), Error.class()) ::
          {:ok, ProtocolError.t() | nil} | {:error, Error.t()}
  def decode_error_value(value, operation, class), do: decode_error(value, operation, class)

  defp composite_fields(
         %V{type: :described, value: %Described{descriptor: descriptor, value: body}},
         operation,
         class
       ) do
    case Enum.find(@states, fn {code, symbol, _, _} ->
           descriptor == V.ulong(code) or descriptor == V.symbol(symbol)
         end) do
      {_, _, name, count} ->
        fields(body, count, operation, class)
        |> case do
          {:ok, values} -> {:ok, name, values}
          error -> error
        end

      nil ->
        failure(operation, :unsupported, :unsupported_delivery_state)
    end
  end

  defp composite_fields(_value, operation, class),
    do: failure(operation, class, :invalid_delivery_state)

  defp fields(%V{type: :list, value: values}, maximum, operation, class) when is_list(values) do
    if length(values) <= maximum,
      do: {:ok, values},
      else: failure(operation, class, :too_many_delivery_state_fields)
  end

  defp fields(_value, _maximum, operation, class),
    do: failure(operation, class, :invalid_delivery_state_fields)

  defp decode_fields(:accepted, [], _operation, _class), do: {:ok, %Outcome.Accepted{}}
  defp decode_fields(:released, [], _operation, _class), do: {:ok, %Outcome.Released{}}

  defp decode_fields(:received, fields, operation, class) do
    with {:ok, number} <- scalar(Enum.at(fields, 0), :uint, true, operation, class),
         {:ok, offset} <- scalar(Enum.at(fields, 1), :ulong, true, operation, class) do
      {:ok, %Received{section_number: number, section_offset: offset}}
    end
  end

  defp decode_fields(:rejected, fields, operation, class) do
    with {:ok, error} <- decode_error(Enum.at(fields, 0), operation, class) do
      {:ok, %Outcome.Rejected{error: error}}
    end
  end

  defp decode_fields(:modified, fields, operation, class) do
    with {:ok, failed} <- scalar(Enum.at(fields, 0), :boolean, false, operation, class),
         {:ok, undeliverable} <- scalar(Enum.at(fields, 1), :boolean, false, operation, class),
         {:ok, annotations} <- fields_map(Enum.at(fields, 2), operation, class) do
      {:ok,
       %Outcome.Modified{
         delivery_failed?: not is_nil(failed) and failed.value,
         undeliverable_here?: not is_nil(undeliverable) and undeliverable.value,
         message_annotations: annotations
       }}
    end
  end

  defp scalar(value, type, mandatory, operation, class) do
    cond do
      absent?(value) and not mandatory -> {:ok, nil}
      match?(%V{type: ^type}, value) and valid_payload?(type, value.value) -> {:ok, value}
      true -> failure(operation, class, :invalid_delivery_state_field)
    end
  end

  defp valid_payload?(:boolean, value), do: is_boolean(value)
  defp valid_payload?(:uint, value), do: is_integer(value) and value >= 0 and value <= 0xFFFF_FFFF

  defp valid_payload?(:ulong, value),
    do: is_integer(value) and value >= 0 and value <= 0xFFFF_FFFF_FFFF_FFFF

  defp valid_payload?(:symbol, value), do: is_binary(value) and ascii?(value)
  defp valid_payload?(:string, value), do: is_binary(value) and String.valid?(value)

  defp fields_map(value, operation, class) do
    cond do
      absent?(value) ->
        {:ok, nil}

      match?(%V{type: :map, value: entries} when is_list(entries), value) ->
        if Enum.all?(value.value, &symbol_entry?/1) and
             MapSet.size(MapSet.new(Enum.map(value.value, &elem(&1, 0)))) == length(value.value),
           do: {:ok, value},
           else: failure(operation, class, :invalid_fields_map)

      true ->
        failure(operation, class, :invalid_fields_map)
    end
  end

  defp symbol_entry?({%V{type: :symbol, value: key}, %V{}}),
    do: is_binary(key) and ascii?(key)

  defp symbol_entry?(_entry), do: false

  defp decode_error(value, operation, class) do
    if absent?(value) do
      {:ok, nil}
    else
      case value do
        %V{type: :described, value: %Described{descriptor: descriptor, value: body}}
        when descriptor == %V{type: :ulong, value: 0x1D} or
               descriptor == %V{type: :symbol, value: "amqp:error:list"} ->
          with {:ok, values} <- fields(body, 3, operation, class),
               {:ok, condition} <- scalar(Enum.at(values, 0), :symbol, true, operation, class),
               {:ok, description} <- scalar(Enum.at(values, 1), :string, false, operation, class),
               {:ok, info} <- fields_map(Enum.at(values, 2), operation, class) do
            {:ok, %ProtocolError{condition: condition, description: description, info: info}}
          end

        _ ->
          failure(operation, class, :invalid_nested_error)
      end
    end
  end

  defp state_fields(%Received{} = state, _operation, _class),
    do: {:ok, 0x23, [state.section_number, state.section_offset]}

  defp state_fields(%Outcome.Accepted{}, _operation, _class), do: {:ok, 0x24, []}
  defp state_fields(%Outcome.Released{}, _operation, _class), do: {:ok, 0x26, []}

  defp state_fields(%Outcome.Rejected{error: error}, operation, class) do
    with {:ok, value} <- encode_error(error, operation, class), do: {:ok, 0x25, [value]}
  end

  defp state_fields(%Outcome.Modified{} = state, operation, class) do
    if is_boolean(state.delivery_failed?) and is_boolean(state.undeliverable_here?) do
      {:ok, 0x27,
       [flag(state.delivery_failed?), flag(state.undeliverable_here?), state.message_annotations]}
    else
      failure(operation, class, :invalid_delivery_state_field)
    end
  end

  defp state_fields(_state, operation, class),
    do: failure(operation, class, :invalid_delivery_state)

  defp encode_error(nil, _operation, _class), do: {:ok, nil}

  defp encode_error(%ProtocolError{} = error, operation, class) do
    value = tagged_composite(0x1D, [error.condition, error.description, error.info])
    with {:ok, _} <- decode_error(value, operation, class), do: {:ok, value}
  end

  defp encode_error(_error, operation, class),
    do: failure(operation, class, :invalid_nested_error)

  defp tagged_composite(code, fields) do
    %V{
      type: :described,
      value: %Described{
        descriptor: V.ulong(code),
        value: %V{type: :list, value: trim_nulls(fields)}
      }
    }
  end

  defp ascii?(<<>>), do: true
  defp ascii?(<<octet, rest::binary>>) when octet <= 0x7F, do: ascii?(rest)
  defp ascii?(_value), do: false

  defp flag(false), do: nil
  defp flag(true), do: V.boolean(true)
  defp absent?(nil), do: true
  defp absent?(%V{type: :null, value: nil}), do: true
  defp absent?(_value), do: false

  defp trim_nulls(fields) do
    fields
    |> Enum.reverse()
    |> Enum.drop_while(&absent?/1)
    |> Enum.reverse()
    |> Enum.map(fn value -> if is_nil(value), do: V.null(), else: value end)
  end

  defp retain_original(state, _value, nil, _limits, _operation, _class), do: {:ok, state}

  defp retain_original(state, value, original, limits, operation, class)
       when is_binary(original) do
    with {:ok, ^value, <<>>} <- WireValue.decode(original, limits) do
      case state do
        %Outcome.Rejected{error: %ProtocolError{}} ->
          with {:ok, raw} <- RawField.composite_field(original, 0, limits),
               do: {:ok, %{state | encoded_error: raw}}

        %Outcome.Modified{message_annotations: %V{}} ->
          with {:ok, raw} <- RawField.composite_field(original, 2, limits),
               do: {:ok, %{state | encoded_message_annotations: raw}}

        _ ->
          {:ok, state}
      end
    else
      {:error, %Error{} = error} -> {:error, %{error | operation: operation}}
      _ -> failure(operation, class, :invalid_delivery_state_provenance)
    end
  end

  defp retain_original(_state, _value, _original, _limits, operation, class),
    do: failure(operation, class, :invalid_delivery_state_provenance)

  defp failure(operation, class, reason), do: {:error, Error.new(operation, class, reason)}
end
