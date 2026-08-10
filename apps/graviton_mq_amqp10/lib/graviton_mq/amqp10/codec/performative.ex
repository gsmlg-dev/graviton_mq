defmodule GravitonMQ.AMQP10.Codec.Performative do
  @moduledoc """
  Pure schema codec for the bounded AMQP 1.0 Open and Begin performatives.

  Binary constructor work is delegated to `GravitonMQ.AMQP10.Codec.Value`.
  This module recognizes the two standard descriptors, validates their ordered
  fields, and returns immutable protocol data without performing negotiation or
  changing connection or session state.
  """

  alias GravitonMQ.AMQP10.Codec
  alias GravitonMQ.AMQP10.Codec.Error
  alias GravitonMQ.AMQP10.Codec.Limits
  alias GravitonMQ.AMQP10.Codec.Value
  alias GravitonMQ.AMQP10.Performative.Begin
  alias GravitonMQ.AMQP10.Performative.Open
  alias GravitonMQ.AMQP10.Value, as: AMQPValue
  alias GravitonMQ.AMQP10.Value.Array
  alias GravitonMQ.AMQP10.Value.Described

  @open_descriptor 0x10
  @open_symbol "amqp:open:list"
  @open_field_count 10

  @begin_descriptor 0x11
  @begin_symbol "amqp:begin:list"
  @begin_field_count 8

  @uint_max 4_294_967_295
  @ushort_max 65_535

  defguardp absent?(value)
            when is_nil(value) or
                   (is_struct(value, AMQPValue) and value.type == :null and is_nil(value.value))

  @spec decode(binary()) :: Codec.decode_result(Open.t() | Begin.t())
  @spec decode(binary(), Limits.t()) :: Codec.decode_result(Open.t() | Begin.t())
  def decode(input, limits \\ Limits.default())

  def decode(input, limits) when is_binary(input) do
    case Value.decode(input, limits) do
      {:ok, value, rest} ->
        case decode_value(value) do
          {:ok, performative} -> {:ok, performative, rest}
          {:error, %Error{}} = error -> error
        end

      {:more, _needed} = more ->
        more

      {:error, %Error{}} = error ->
        error
    end
  end

  def decode(_input, _limits),
    do: schema_error(:performative_decode, :invalid_value, :invalid_performative)

  @spec encode(Open.t() | Begin.t()) :: Codec.encode_result()
  @spec encode(Open.t() | Begin.t(), Limits.t()) :: Codec.encode_result()
  def encode(performative, limits \\ Limits.default())

  def encode(%Open{} = open, limits) do
    with {:ok, normalized} <-
           validate_open(open_fields(open), :performative_encode, :invalid_value) do
      normalized
      |> canonical_open_fields()
      |> encode_fields(@open_descriptor, limits)
    end
  end

  def encode(%Begin{} = begin_performative, limits) do
    with {:ok, normalized} <-
           validate_begin(
             begin_fields(begin_performative),
             :performative_encode,
             :invalid_value
           ) do
      normalized
      |> canonical_begin_fields()
      |> encode_fields(@begin_descriptor, limits)
    end
  end

  def encode(_performative, _limits),
    do: schema_error(:performative_encode, :invalid_value, :invalid_performative)

  defp decode_value(%AMQPValue{
         type: :described,
         value: %Described{descriptor: descriptor, value: body}
       }) do
    case descriptor_name(descriptor) do
      :open -> decode_body(:open, body)
      :begin -> decode_body(:begin, body)
      :unknown -> unknown_descriptor(descriptor)
    end
  end

  defp decode_value(_value),
    do: schema_error(:performative_decode, :malformed, :invalid_performative)

  defp decode_body(:open, %AMQPValue{type: :list, value: fields}) when is_list(fields),
    do: validate_open(fields, :performative_decode, :malformed)

  defp decode_body(:begin, %AMQPValue{type: :list, value: fields}) when is_list(fields),
    do: validate_begin(fields, :performative_decode, :malformed)

  defp decode_body(_name, _body),
    do: schema_error(:performative_decode, :malformed, :invalid_performative_body)

  defp descriptor_name(%AMQPValue{type: :ulong, value: @open_descriptor}), do: :open
  defp descriptor_name(%AMQPValue{type: :symbol, value: @open_symbol}), do: :open
  defp descriptor_name(%AMQPValue{type: :ulong, value: @begin_descriptor}), do: :begin
  defp descriptor_name(%AMQPValue{type: :symbol, value: @begin_symbol}), do: :begin
  defp descriptor_name(_descriptor), do: :unknown

  defp validate_open(fields, operation, class) do
    with :ok <- validate_field_count(fields, @open_field_count, operation, class),
         {:ok, container_id} <-
           required_field(fields, 0, :container_id, :string, operation, class),
         {:ok, hostname} <- optional_field(fields, 1, :hostname, :string, operation, class),
         {:ok, max_frame_size} <-
           default_field(
             fields,
             2,
             :max_frame_size,
             :uint,
             AMQPValue.uint(@uint_max),
             operation,
             class
           ),
         :ok <- validate_max_frame_size(max_frame_size, operation, class),
         {:ok, channel_max} <-
           default_field(
             fields,
             3,
             :channel_max,
             :ushort,
             AMQPValue.ushort(@ushort_max),
             operation,
             class
           ),
         {:ok, idle_time_out} <-
           optional_field(fields, 4, :idle_time_out, :uint, operation, class),
         {:ok, outgoing_locales} <-
           multiple_symbol_field(fields, 5, :outgoing_locales, operation, class),
         {:ok, incoming_locales} <-
           multiple_symbol_field(fields, 6, :incoming_locales, operation, class),
         {:ok, offered_capabilities} <-
           multiple_symbol_field(fields, 7, :offered_capabilities, operation, class),
         {:ok, desired_capabilities} <-
           multiple_symbol_field(fields, 8, :desired_capabilities, operation, class),
         {:ok, properties} <- properties_field(fields, 9, operation, class) do
      {:ok,
       %Open{
         container_id: container_id,
         hostname: hostname,
         max_frame_size: max_frame_size,
         channel_max: channel_max,
         idle_time_out: idle_time_out,
         outgoing_locales: outgoing_locales,
         incoming_locales: incoming_locales,
         offered_capabilities: offered_capabilities,
         desired_capabilities: desired_capabilities,
         properties: properties
       }}
    end
  end

  defp validate_begin(fields, operation, class) do
    with :ok <- validate_field_count(fields, @begin_field_count, operation, class),
         {:ok, remote_channel} <-
           optional_field(fields, 0, :remote_channel, :ushort, operation, class),
         {:ok, next_outgoing_id} <-
           required_field(fields, 1, :next_outgoing_id, :uint, operation, class),
         {:ok, incoming_window} <-
           required_field(fields, 2, :incoming_window, :uint, operation, class),
         {:ok, outgoing_window} <-
           required_field(fields, 3, :outgoing_window, :uint, operation, class),
         {:ok, handle_max} <-
           default_field(
             fields,
             4,
             :handle_max,
             :uint,
             AMQPValue.uint(@uint_max),
             operation,
             class
           ),
         {:ok, offered_capabilities} <-
           multiple_symbol_field(fields, 5, :offered_capabilities, operation, class),
         {:ok, desired_capabilities} <-
           multiple_symbol_field(fields, 6, :desired_capabilities, operation, class),
         {:ok, properties} <- properties_field(fields, 7, operation, class) do
      {:ok,
       %Begin{
         remote_channel: remote_channel,
         next_outgoing_id: next_outgoing_id,
         incoming_window: incoming_window,
         outgoing_window: outgoing_window,
         handle_max: handle_max,
         offered_capabilities: offered_capabilities,
         desired_capabilities: desired_capabilities,
         properties: properties
       }}
    end
  end

  defp validate_field_count(fields, maximum, _operation, _class)
       when is_list(fields) and length(fields) <= maximum,
       do: :ok

  defp validate_field_count(fields, maximum, operation, class) when is_list(fields) do
    schema_error(operation, class, :too_many_fields,
      actual: length(fields),
      maximum: maximum
    )
  end

  defp required_field(fields, index, name, expected_type, operation, class) do
    case Enum.at(fields, index) do
      value when absent?(value) ->
        schema_error(operation, class, :mandatory_field_missing,
          field: name,
          index: index
        )

      %AMQPValue{type: ^expected_type} = value ->
        {:ok, value}

      value ->
        type_mismatch(operation, class, name, index, expected_type, value)
    end
  end

  defp optional_field(fields, index, name, expected_type, operation, class) do
    case Enum.at(fields, index) do
      value when absent?(value) ->
        {:ok, nil}

      %AMQPValue{type: ^expected_type} = value ->
        {:ok, value}

      value ->
        type_mismatch(operation, class, name, index, expected_type, value)
    end
  end

  defp default_field(fields, index, name, expected_type, default, operation, class) do
    case optional_field(fields, index, name, expected_type, operation, class) do
      {:ok, nil} -> {:ok, default}
      {:ok, value} -> {:ok, value}
      {:error, %Error{}} = error -> error
    end
  end

  defp multiple_symbol_field(fields, index, name, operation, class) do
    case Enum.at(fields, index) do
      value when absent?(value) ->
        {:ok, nil}

      %AMQPValue{type: :symbol, value: symbol} = value when is_binary(symbol) ->
        {:ok, value}

      %AMQPValue{
        type: :array,
        value: %Array{element_type: :symbol, values: values}
      } = value ->
        if is_list(values) and Enum.all?(values, &tagged_symbol?/1) do
          {:ok, value}
        else
          type_mismatch(operation, class, name, index, :symbol_or_symbol_array, value)
        end

      value ->
        type_mismatch(operation, class, name, index, :symbol_or_symbol_array, value)
    end
  end

  defp properties_field(fields, index, operation, class) do
    case Enum.at(fields, index) do
      value when absent?(value) ->
        {:ok, nil}

      %AMQPValue{type: :map, value: entries} = value when is_list(entries) ->
        if Enum.all?(entries, &symbol_property_entry?/1) do
          {:ok, value}
        else
          schema_error(operation, class, :invalid_property_key,
            field: :properties,
            index: index
          )
        end

      value ->
        type_mismatch(operation, class, :properties, index, :map, value)
    end
  end

  defp symbol_property_entry?({%AMQPValue{type: :symbol}, %AMQPValue{}}), do: true
  defp symbol_property_entry?(_entry), do: false

  defp tagged_symbol?(%AMQPValue{type: :symbol, value: value}) when is_binary(value), do: true
  defp tagged_symbol?(_value), do: false

  defp validate_max_frame_size(
         %AMQPValue{type: :uint, value: value},
         _operation,
         _class
       )
       when is_integer(value) and value >= 512,
       do: :ok

  defp validate_max_frame_size(_value, operation, class) do
    schema_error(operation, class, :field_value_out_of_range,
      field: :max_frame_size,
      index: 2,
      minimum: 512
    )
  end

  defp open_fields(%Open{} = open) do
    [
      open.container_id,
      open.hostname,
      open.max_frame_size,
      open.channel_max,
      open.idle_time_out,
      open.outgoing_locales,
      open.incoming_locales,
      open.offered_capabilities,
      open.desired_capabilities,
      open.properties
    ]
  end

  defp begin_fields(%Begin{} = begin_performative) do
    [
      begin_performative.remote_channel,
      begin_performative.next_outgoing_id,
      begin_performative.incoming_window,
      begin_performative.outgoing_window,
      begin_performative.handle_max,
      begin_performative.offered_capabilities,
      begin_performative.desired_capabilities,
      begin_performative.properties
    ]
  end

  defp canonical_open_fields(%Open{} = open) do
    [
      open.container_id,
      open.hostname,
      omit_default(open.max_frame_size, :uint, @uint_max),
      omit_default(open.channel_max, :ushort, @ushort_max),
      open.idle_time_out,
      open.outgoing_locales,
      open.incoming_locales,
      open.offered_capabilities,
      open.desired_capabilities,
      open.properties
    ]
  end

  defp canonical_begin_fields(%Begin{} = begin_performative) do
    [
      begin_performative.remote_channel,
      begin_performative.next_outgoing_id,
      begin_performative.incoming_window,
      begin_performative.outgoing_window,
      omit_default(begin_performative.handle_max, :uint, @uint_max),
      begin_performative.offered_capabilities,
      begin_performative.desired_capabilities,
      begin_performative.properties
    ]
  end

  defp omit_default(%AMQPValue{type: type, value: value}, type, value), do: nil
  defp omit_default(value, _type, _default), do: value

  defp encode_fields(fields, descriptor, limits) do
    semantic_fields =
      fields
      |> trim_trailing_absent()
      |> Enum.map(fn
        nil -> AMQPValue.null()
        value -> value
      end)

    descriptor
    |> AMQPValue.ulong()
    |> AMQPValue.described(AMQPValue.list(semantic_fields))
    |> Value.encode(limits)
  end

  defp trim_trailing_absent(fields) do
    fields
    |> Enum.reverse()
    |> Enum.drop_while(&is_nil/1)
    |> Enum.reverse()
  end

  defp type_mismatch(operation, class, field, index, expected, actual) do
    schema_error(operation, class, :field_type_mismatch,
      field: field,
      index: index,
      expected: expected,
      actual: semantic_type(actual)
    )
  end

  defp semantic_type(nil), do: :absent
  defp semantic_type(%AMQPValue{type: type}), do: type
  defp semantic_type(_value), do: :invalid

  defp unknown_descriptor(descriptor) do
    schema_error(:performative_decode, :unsupported, :unknown_descriptor, descriptor: descriptor)
  end

  defp schema_error(operation, class, reason, details \\ []) do
    {:error, Error.new(operation, class, reason, details: Map.new(details))}
  end
end
