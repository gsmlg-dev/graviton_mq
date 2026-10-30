defmodule GravitonMQ.AMQP10.Codec.Message do
  @moduledoc """
  Bounded, pure message-format-0 sections and an optional parsed message view.

  `decode/2` takes the complete message content, whose boundary is supplied by
  Transfer assembly. A partial final section returns `:more`; the caller must
  not treat a section boundary as proof that a fragmented delivery is complete.
  The parsed view retains exact tagged field values, not an inferred body.

  `encode/2` validates and returns the authoritative original content unchanged.
  `encode_sections/2` constructs new content, choosing numeric section
  descriptors and canonical value constructors. It must not be used to forward
  an existing bare message, whose original encoding is immutable.
  Annotation understanding, expiry and durability acceptance are endpoint or
  broker policy; recognizing a section does not implement those semantics.
  """

  alias GravitonMQ.AMQP10.Codec.DeliveryState
  alias GravitonMQ.AMQP10.Codec.Error
  alias GravitonMQ.AMQP10.Codec.Limits
  alias GravitonMQ.AMQP10.Codec.Value, as: WireValue
  alias GravitonMQ.AMQP10.Message
  alias GravitonMQ.AMQP10.Value
  alias GravitonMQ.AMQP10.Value.Described

  @sections [
    {:header, 0x70, "amqp:header:list", :list, 0},
    {:delivery_annotations, 0x71, "amqp:delivery-annotations:map", :map, 1},
    {:message_annotations, 0x72, "amqp:message-annotations:map", :map, 2},
    {:properties, 0x73, "amqp:properties:list", :list, 3},
    {:application_properties, 0x74, "amqp:application-properties:map", :map, 4},
    {:data, 0x75, "amqp:data:binary", :binary, 5},
    {:amqp_sequence, 0x76, "amqp:amqp-sequence:list", :list, 5},
    {:amqp_value, 0x77, "amqp:amqp-value:*", :any, 5},
    {:footer, 0x78, "amqp:footer:map", :map, 6}
  ]
  @header_types [[:boolean], [:ubyte], [:uint], [:boolean], [:uint]]
  @properties_types [
    [:ulong, :uuid, :binary, :string],
    [:binary],
    [:string],
    [:string],
    [:string],
    [:ulong, :uuid, :binary, :string],
    [:symbol],
    [:symbol],
    [:timestamp],
    [:timestamp],
    [:string],
    [:uint],
    [:string]
  ]
  @simple_types Value.semantic_types() -- [:map, :list, :array, :described]

  @spec decode(binary(), Limits.t()) ::
          {:ok, Message.t(), binary()} | {:more, pos_integer()} | {:error, Error.t()}
  def decode(input, limits \\ Limits.default())

  def decode(input, limits) when is_binary(input) do
    with {:ok, limits} <- Limits.validate(limits, :value_decode),
         :ok <- total_bytes(byte_size(input), limits, :value_decode),
         {:ok, sections} <- read_sections(input, limits, nil, 0, []),
         :ok <- validate_content_encoding(sections, :value_decode, :malformed) do
      {:ok, %Message{message_format: Value.uint(0), encoded_content: input, sections: sections},
       <<>>}
    end
  end

  def decode(_input, _limits), do: failure(:value_decode, :invalid_value, :invalid_message)

  @spec encode(Message.t(), Limits.t()) :: {:ok, binary()} | {:error, Error.t()}
  def encode(message, limits \\ Limits.default())

  def encode(
        %Message{message_format: %Value{type: :uint, value: 0}, encoded_content: bytes},
        limits
      )
      when is_binary(bytes) do
    case decode(bytes, limits) do
      {:ok, _message, <<>>} ->
        {:ok, bytes}

      {:more, _} ->
        failure(:value_encode, :invalid_value, :incomplete_message)

      {:error, %Error{class: :malformed} = error} ->
        {:error, %{error | operation: :value_encode, class: :invalid_value}}

      {:error, %Error{} = error} ->
        {:error, %{error | operation: :value_encode}}
    end
  end

  def encode(%Message{message_format: %Value{type: :uint, value: format}}, _limits)
      when is_integer(format) and format > 0 and format <= 0xFFFF_FFFF,
      do: failure(:value_encode, :unsupported, :unsupported_message_format)

  def encode(_message, _limits), do: failure(:value_encode, :invalid_value, :invalid_message)

  @spec decode_section(binary(), Limits.t()) ::
          {:ok, Message.section(), binary()} | {:more, pos_integer()} | {:error, Error.t()}
  def decode_section(input, limits \\ Limits.default())

  def decode_section(input, limits) when is_binary(input) do
    with {:ok, value, rest} <- WireValue.decode(input, limits),
         {:ok, section} <- section_from_value(value, :value_decode, :malformed) do
      {:ok, section, rest}
    end
  end

  def decode_section(_input, _limits),
    do: failure(:value_decode, :invalid_value, :invalid_section)

  @spec encode_section(Message.section(), Limits.t()) :: {:ok, binary()} | {:error, Error.t()}
  def encode_section(section, limits \\ Limits.default()) do
    with {:ok, value} <- section_value(section, :value_encode, :invalid_value) do
      WireValue.encode(value, limits)
    end
  end

  @spec encode_sections([Message.section()], Limits.t()) :: {:ok, binary()} | {:error, Error.t()}
  def encode_sections(sections, limits \\ Limits.default())

  def encode_sections(sections, limits) when is_list(sections) do
    with {:ok, limits} <- Limits.validate(limits, :value_encode),
         :ok <- section_count(length(sections), limits, :value_encode),
         {:ok, values} <- preflight(sections, limits) do
      values
      |> Enum.reduce_while({:ok, []}, fn value, {:ok, acc} ->
        case WireValue.encode(value, limits) do
          {:ok, bytes} -> {:cont, {:ok, [bytes | acc]}}
          error -> {:halt, error}
        end
      end)
      |> case do
        {:ok, bytes} -> {:ok, bytes |> Enum.reverse() |> IO.iodata_to_binary()}
        error -> error
      end
    end
  end

  def encode_sections(_sections, _limits),
    do: failure(:value_encode, :invalid_value, :invalid_message)

  defp preflight(sections, limits) do
    sections
    |> Enum.reduce_while({:ok, nil, 0, []}, fn section, {:ok, previous, bytes, acc} ->
      with {:ok, value} <- section_value(section, :value_encode, :invalid_value),
           {:ok, order} <- order(elem(section, 0), previous, :value_encode, :invalid_value),
           {:ok, size} <- WireValue.encoded_size(value, limits),
           :ok <- total_bytes(bytes + size, limits, :value_encode) do
        {:cont, {:ok, order, bytes + size, [value | acc]}}
      else
        error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, _order, _size, values} ->
        with :ok <- validate_content_encoding(sections, :value_encode, :invalid_value) do
          {:ok, Enum.reverse(values)}
        end

      error ->
        error
    end
  end

  defp read_sections(<<>>, _limits, _previous, _count, acc), do: {:ok, Enum.reverse(acc)}

  defp read_sections(input, limits, previous, count, acc) do
    with :ok <- section_count(count + 1, limits, :value_decode),
         {:ok, {name, _body} = section, rest} <- decode_section(input, limits),
         {:ok, next} <- order(name, previous, :value_decode, :malformed) do
      read_sections(rest, limits, next, count + 1, [section | acc])
    end
  end

  defp section_from_value(
         %Value{type: :described, value: %Described{descriptor: descriptor, value: body}},
         operation,
         class
       ) do
    case Enum.find(@sections, fn {_name, code, symbol, _, _} ->
           descriptor == Value.ulong(code) or descriptor == Value.symbol(symbol)
         end) do
      {name, _code, _symbol, type, _rank} ->
        with :ok <- validate_body(name, body, type, operation, class), do: {:ok, {name, body}}

      nil ->
        failure(operation, :unsupported, :unsupported_message_section)
    end
  end

  defp section_from_value(_value, operation, class),
    do: failure(operation, class, :invalid_section)

  defp section_value({name, %Value{} = body}, operation, class) do
    case List.keyfind(@sections, name, 0) do
      {^name, code, _symbol, type, _rank} ->
        with :ok <- validate_body(name, body, type, operation, class),
             do: {:ok, Value.described(Value.ulong(code), body)}

      nil ->
        failure(operation, :unsupported, :unsupported_message_section)
    end
  end

  defp section_value(_value, operation, class), do: failure(operation, class, :invalid_section)

  defp validate_body(:header, %Value{type: :list, value: fields}, :list, operation, class)
       when is_list(fields),
       do: validate_fields(fields, @header_types, operation, class)

  defp validate_body(:properties, %Value{type: :list, value: fields}, :list, operation, class)
       when is_list(fields) do
    with :ok <- validate_fields(fields, @properties_types, operation, class) do
      if identity_encoding?(Enum.at(fields, 7)),
        do: failure(operation, class, :identity_content_encoding),
        else: :ok
    end
  end

  defp validate_body(name, %Value{type: :map, value: entries}, :map, operation, class)
       when name in [:delivery_annotations, :message_annotations, :footer] and is_list(entries) do
    if Enum.all?(entries, &annotation_entry?/1) do
      validate_rejected_annotation(name, entries, operation, class)
    else
      failure(operation, class, :invalid_annotation_key)
    end
  end

  defp validate_body(
         :application_properties,
         %Value{type: :map, value: entries},
         :map,
         operation,
         class
       )
       when is_list(entries) do
    if Enum.all?(entries, &application_entry?/1),
      do: :ok,
      else: failure(operation, class, :invalid_application_property)
  end

  defp validate_body(:amqp_value, %Value{}, :any, _operation, _class), do: :ok

  defp validate_body(:data, %Value{type: :binary, value: bytes}, :binary, _operation, _class)
       when is_binary(bytes),
       do: :ok

  defp validate_body(
         :amqp_sequence,
         %Value{type: :list, value: fields},
         :list,
         _operation,
         _class
       )
       when is_list(fields),
       do: :ok

  defp validate_body(_name, _body, _type, operation, class),
    do: failure(operation, class, :invalid_section_body)

  defp validate_fields(fields, types, operation, class) do
    cond do
      length(fields) > length(types) ->
        failure(operation, class, :too_many_section_fields)

      Enum.with_index(fields)
      |> Enum.all?(fn {value, index} ->
        match?(%Value{type: :null, value: nil}, value) or
            (is_struct(value, Value) and value.type in Enum.at(types, index))
      end) ->
        :ok

      true ->
        failure(operation, class, :invalid_section_field)
    end
  end

  defp annotation_entry?({%Value{type: type}, %Value{}}) when type in [:symbol, :ulong], do: true
  defp annotation_entry?(_entry), do: false

  defp application_entry?({%Value{type: :string}, %Value{type: type}}) when type in @simple_types,
    do: true

  defp application_entry?(_entry), do: false

  defp validate_rejected_annotation(:delivery_annotations, entries, operation, class) do
    case List.keyfind(entries, Value.symbol("rejected"), 0) do
      nil ->
        :ok

      {_key, value} ->
        case DeliveryState.decode_error_value(value, operation, class) do
          {:ok, %GravitonMQ.AMQP10.Error{}} -> :ok
          _ -> failure(operation, class, :invalid_rejected_annotation)
        end
    end
  end

  defp validate_rejected_annotation(_name, _entries, _operation, _class), do: :ok

  defp validate_content_encoding(sections, operation, class) do
    encoding =
      Enum.find_value(sections, fn
        {:properties, %Value{type: :list, value: fields}} -> Enum.at(fields, 7)
        _ -> nil
      end)

    non_data_body = Enum.any?(sections, fn {name, _} -> name in [:amqp_sequence, :amqp_value] end)

    if match?(%Value{type: :symbol}, encoding) and non_data_body,
      do: failure(operation, class, :content_encoding_requires_data),
      else: :ok
  end

  defp identity_encoding?(%Value{type: :symbol, value: bytes})
       when is_binary(bytes) and byte_size(bytes) == 8,
       do: String.downcase(bytes, :ascii) == "identity"

  defp identity_encoding?(_value), do: false

  defp order(name, previous, operation, class) do
    {^name, _code, _symbol, _type, rank} = List.keyfind(@sections, name, 0)

    cond do
      is_nil(previous) -> {:ok, {name, rank}}
      rank > elem(previous, 1) -> {:ok, {name, rank}}
      name in [:data, :amqp_sequence] and previous == {name, rank} -> {:ok, previous}
      true -> failure(operation, class, :invalid_section_order)
    end
  end

  defp total_bytes(bytes, limits, _operation) when bytes <= limits.max_value_bytes, do: :ok

  defp total_bytes(_bytes, _limits, operation),
    do: failure(operation, :limit_exceeded, :message_size_limit)

  defp section_count(count, limits, _operation) when count <= limits.max_compound_items, do: :ok

  defp section_count(_count, _limits, operation),
    do: failure(operation, :limit_exceeded, :message_section_limit)

  defp failure(operation, class, reason), do: {:error, Error.new(operation, class, reason)}
end
