defmodule GravitonMQ.AMQP10.Codec.Performative do
  @moduledoc """
  Pure schema codec for the bounded AMQP 1.0 performative surface.

  Binary constructor work is delegated to `GravitonMQ.AMQP10.Codec.Value`.
  Open, Begin, Detach, End, and Close are encoded and decoded as immutable protocol
  data with validated ordered fields, without performing protocol transitions.
  """

  alias GravitonMQ.AMQP10.Codec
  alias GravitonMQ.AMQP10.Codec.Error
  alias GravitonMQ.AMQP10.Codec.Limits
  alias GravitonMQ.AMQP10.Codec.Value
  alias GravitonMQ.AMQP10.Error, as: ProtocolError
  alias GravitonMQ.AMQP10.Performative.Begin
  alias GravitonMQ.AMQP10.Performative.Close
  alias GravitonMQ.AMQP10.Performative.Detach
  alias GravitonMQ.AMQP10.Performative.End
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

  @detach_descriptor 0x16
  @detach_symbol "amqp:detach:list"
  @detach_field_count 3

  @end_descriptor 0x17
  @end_symbol "amqp:end:list"
  @end_field_count 1

  @close_descriptor 0x18
  @close_symbol "amqp:close:list"
  @close_field_count 1

  @protocol_error_descriptor 0x1D
  @protocol_error_symbol "amqp:error:list"
  @protocol_error_field_count 3

  @uint_max 4_294_967_295
  @ushort_max 65_535

  defguardp absent?(value)
            when is_nil(value) or
                   (is_struct(value, AMQPValue) and value.type == :null and is_nil(value.value))

  @spec decode(binary()) ::
          Codec.decode_result(Open.t() | Begin.t() | Detach.t() | End.t() | Close.t())
  @spec decode(binary(), Limits.t()) ::
          Codec.decode_result(Open.t() | Begin.t() | Detach.t() | End.t() | Close.t())
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

  @spec encode(Open.t() | Begin.t() | Detach.t() | End.t() | Close.t()) :: Codec.encode_result()
  @spec encode(Open.t() | Begin.t() | Detach.t() | End.t() | Close.t(), Limits.t()) ::
          Codec.encode_result()
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

  def encode(%Detach{} = detach, limits) do
    with {:ok, normalized} <-
           validate_detach_fields(
             [detach.handle, detach.closed, detach.error],
             :performative_encode,
             :invalid_value
           ),
         {:ok, error_value} <-
           nested_error_value(detach.error, :detach, 2, :performative_encode, :invalid_value) do
      encode_fields(
        [normalized.handle, omit_default(normalized.closed, :boolean, false), error_value],
        @detach_descriptor,
        limits
      )
    end
  end

  def encode(%End{} = end_performative, limits) do
    with {:ok, error_value} <-
           nested_error_value(
             end_performative.error,
             :end,
             0,
             :performative_encode,
             :invalid_value
           ) do
      encode_fields([error_value], @end_descriptor, limits)
    end
  end

  def encode(%Close{} = close_performative, limits) do
    with {:ok, error_value} <-
           nested_error_value(
             close_performative.error,
             :close,
             0,
             :performative_encode,
             :invalid_value
           ) do
      encode_fields([error_value], @close_descriptor, limits)
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
      :detach -> decode_body(:detach, body)
      :end -> decode_body(:end, body)
      :close -> decode_body(:close, body)
      :unknown -> unknown_descriptor(descriptor)
    end
  end

  defp decode_value(_value),
    do: schema_error(:performative_decode, :malformed, :invalid_performative)

  defp decode_body(:open, %AMQPValue{type: :list, value: fields}) when is_list(fields),
    do: validate_open(fields, :performative_decode, :malformed)

  defp decode_body(:begin, %AMQPValue{type: :list, value: fields}) when is_list(fields),
    do: validate_begin(fields, :performative_decode, :malformed)

  defp decode_body(:detach, %AMQPValue{type: :list, value: fields}) when is_list(fields) do
    with {:ok, detach} <- validate_detach_fields(fields, :performative_decode, :malformed),
         {:ok, error} <- nested_error_field(fields, :detach, 2, :performative_decode, :malformed) do
      {:ok, %{detach | error: error}}
    end
  end

  defp decode_body(:end, %AMQPValue{type: :list, value: fields}) when is_list(fields),
    do: validate_end(fields, :performative_decode, :malformed)

  defp decode_body(:close, %AMQPValue{type: :list, value: fields}) when is_list(fields),
    do: validate_close(fields, :performative_decode, :malformed)

  defp decode_body(name, body) when name in [:detach, :end, :close] do
    outer_schema_error(name, :performative_decode, :malformed, :invalid_performative_body,
      expected: :list,
      actual: semantic_type(body)
    )
  end

  defp decode_body(_name, _body),
    do: schema_error(:performative_decode, :malformed, :invalid_performative_body)

  defp descriptor_name(%AMQPValue{type: :ulong, value: @open_descriptor}), do: :open
  defp descriptor_name(%AMQPValue{type: :symbol, value: @open_symbol}), do: :open
  defp descriptor_name(%AMQPValue{type: :ulong, value: @begin_descriptor}), do: :begin
  defp descriptor_name(%AMQPValue{type: :symbol, value: @begin_symbol}), do: :begin
  defp descriptor_name(%AMQPValue{type: :ulong, value: @detach_descriptor}), do: :detach
  defp descriptor_name(%AMQPValue{type: :symbol, value: @detach_symbol}), do: :detach
  defp descriptor_name(%AMQPValue{type: :ulong, value: @end_descriptor}), do: :end
  defp descriptor_name(%AMQPValue{type: :symbol, value: @end_symbol}), do: :end
  defp descriptor_name(%AMQPValue{type: :ulong, value: @close_descriptor}), do: :close
  defp descriptor_name(%AMQPValue{type: :symbol, value: @close_symbol}), do: :close
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

  defp validate_detach_fields(fields, operation, class) do
    result =
      with :ok <-
             validate_outer_field_count(fields, @detach_field_count, :detach, operation, class),
           {:ok, handle} <- required_field(fields, 0, :handle, :uint, operation, class),
           {:ok, closed} <-
             default_field(
               fields,
               1,
               :closed,
               :boolean,
               AMQPValue.boolean(false),
               operation,
               class
             ) do
        {:ok, %Detach{handle: handle, closed: closed}}
      end

    case result do
      {:ok, _detach} = success ->
        success

      {:error, %Error{} = error} ->
        {:error, %{error | details: Map.put(error.details, :performative, :detach)}}
    end
  end

  defp validate_end(fields, operation, class) do
    with :ok <-
           validate_outer_field_count(fields, @end_field_count, :end, operation, class),
         {:ok, error} <- nested_error_field(fields, :end, 0, operation, class) do
      {:ok, %End{error: error}}
    end
  end

  defp validate_close(fields, operation, class) do
    with :ok <-
           validate_outer_field_count(
             fields,
             @close_field_count,
             :close,
             operation,
             class
           ),
         {:ok, error} <- nested_error_field(fields, :close, 0, operation, class) do
      {:ok, %Close{error: error}}
    end
  end

  defp validate_outer_field_count(fields, maximum, _performative, _operation, _class)
       when length(fields) <= maximum,
       do: :ok

  defp validate_outer_field_count(fields, maximum, performative, operation, class) do
    outer_schema_error(performative, operation, class, :too_many_fields,
      actual: length(fields),
      maximum: maximum
    )
  end

  defp nested_error_field(fields, performative, index, operation, class) do
    case Enum.at(fields, index) do
      value when absent?(value) ->
        {:ok, nil}

      %AMQPValue{type: :described} = value ->
        decode_protocol_error(value, performative, index, operation, class)

      value ->
        outer_schema_error(performative, operation, class, :field_type_mismatch,
          field: :error,
          index: index,
          expected: :error,
          actual: semantic_type(value)
        )
    end
  end

  defp nested_error_value(nil, _performative, _index, _operation, _class), do: {:ok, nil}

  defp nested_error_value(%ProtocolError{} = error, performative, _index, operation, class) do
    fields = [error.condition, error.description, error.info]

    with {:ok, validated} <-
           validate_protocol_error(fields, performative, operation, class) do
      {:ok, protocol_error_value(validated)}
    end
  end

  defp nested_error_value(value, performative, index, operation, class) do
    outer_schema_error(performative, operation, class, :field_type_mismatch,
      field: :error,
      index: index,
      expected: :error,
      actual: semantic_type(value)
    )
  end

  defp protocol_error_value(%ProtocolError{} = error) do
    described_list_value(
      [error.condition, error.description, error.info],
      @protocol_error_descriptor
    )
  end

  defp decode_protocol_error(
         %AMQPValue{
           type: :described,
           value: %Described{descriptor: descriptor, value: body}
         },
         performative,
         index,
         operation,
         class
       ) do
    if protocol_error_descriptor?(descriptor) do
      case body do
        %AMQPValue{type: :list, value: fields} when is_list(fields) ->
          validate_protocol_error(fields, performative, operation, class)

        value ->
          nested_schema_error(performative, operation, class, :invalid_error_body,
            field: :error,
            index: index,
            expected: :list,
            actual: semantic_type(value)
          )
      end
    else
      outer_schema_error(performative, operation, class, :invalid_error_descriptor,
        field: :error,
        index: index,
        expected: :error,
        actual: :described,
        descriptor: descriptor
      )
    end
  end

  defp protocol_error_descriptor?(%AMQPValue{
         type: :ulong,
         value: @protocol_error_descriptor
       }),
       do: true

  defp protocol_error_descriptor?(%AMQPValue{
         type: :symbol,
         value: @protocol_error_symbol
       }),
       do: true

  defp protocol_error_descriptor?(_descriptor), do: false

  defp validate_protocol_error(fields, performative, operation, class) do
    with :ok <-
           validate_protocol_error_field_count(fields, performative, operation, class),
         {:ok, condition} <-
           required_protocol_error_field(
             fields,
             0,
             :condition,
             :symbol,
             performative,
             operation,
             class
           ),
         {:ok, description} <-
           optional_protocol_error_field(
             fields,
             1,
             :description,
             :string,
             performative,
             operation,
             class
           ),
         {:ok, info} <- protocol_error_info_field(fields, performative, operation, class) do
      {:ok, %ProtocolError{condition: condition, description: description, info: info}}
    end
  end

  defp validate_protocol_error_field_count(fields, _performative, _operation, _class)
       when length(fields) <= @protocol_error_field_count,
       do: :ok

  defp validate_protocol_error_field_count(fields, performative, operation, class) do
    nested_schema_error(performative, operation, class, :too_many_fields,
      actual: length(fields),
      maximum: @protocol_error_field_count
    )
  end

  defp required_protocol_error_field(
         fields,
         index,
         name,
         expected_type,
         performative,
         operation,
         class
       ) do
    case Enum.at(fields, index) do
      value when absent?(value) ->
        nested_schema_error(performative, operation, class, :mandatory_field_missing,
          field: name,
          index: index
        )

      %AMQPValue{type: ^expected_type, value: value} = tagged when is_binary(value) ->
        {:ok, tagged}

      value ->
        protocol_error_type_mismatch(
          performative,
          operation,
          class,
          name,
          index,
          expected_type,
          value
        )
    end
  end

  defp optional_protocol_error_field(
         fields,
         index,
         name,
         expected_type,
         performative,
         operation,
         class
       ) do
    case Enum.at(fields, index) do
      value when absent?(value) ->
        {:ok, nil}

      %AMQPValue{type: ^expected_type, value: value} = tagged when is_binary(value) ->
        {:ok, tagged}

      value ->
        protocol_error_type_mismatch(
          performative,
          operation,
          class,
          name,
          index,
          expected_type,
          value
        )
    end
  end

  defp protocol_error_info_field(fields, performative, operation, class) do
    case Enum.at(fields, 2) do
      value when absent?(value) ->
        {:ok, nil}

      %AMQPValue{type: :map, value: entries} = value when is_list(entries) ->
        if Enum.all?(entries, &protocol_error_info_entry?/1) do
          {:ok, value}
        else
          nested_schema_error(performative, operation, class, :invalid_property_key,
            field: :info,
            index: 2
          )
        end

      value ->
        protocol_error_type_mismatch(
          performative,
          operation,
          class,
          :info,
          2,
          :map,
          value
        )
    end
  end

  defp protocol_error_info_entry?({%AMQPValue{type: :symbol, value: key}, %AMQPValue{}})
       when is_binary(key),
       do: true

  defp protocol_error_info_entry?(_entry), do: false

  defp protocol_error_type_mismatch(
         performative,
         operation,
         class,
         field,
         index,
         expected,
         actual
       ) do
    nested_schema_error(performative, operation, class, :field_type_mismatch,
      field: field,
      index: index,
      expected: expected,
      actual: semantic_type(actual)
    )
  end

  defp nested_schema_error(performative, operation, class, reason, details) do
    details =
      details
      |> Keyword.put(:nested_schema, :error)
      |> Keyword.put(:performative, performative)

    schema_error(operation, class, reason, details)
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
    fields
    |> described_list_value(descriptor)
    |> Value.encode(limits)
  end

  defp described_list_value(fields, descriptor) do
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

  defp outer_schema_error(performative, operation, class, reason, details) do
    schema_error(operation, class, reason, Keyword.put(details, :performative, performative))
  end

  defp schema_error(operation, class, reason, details \\ []) do
    {:error, Error.new(operation, class, reason, details: Map.new(details))}
  end
end
