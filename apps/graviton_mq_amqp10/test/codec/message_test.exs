defmodule GravitonMQ.AMQP10.Codec.MessageTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.AMQP10.Codec.{Error, Limits}
  alias GravitonMQ.AMQP10.Codec.Message, as: Codec
  alias GravitonMQ.AMQP10.Codec.Value, as: WireValue
  alias GravitonMQ.AMQP10.Message
  alias GravitonMQ.AMQP10.Value

  # OASIS AMQP 1.0 Messaging §3.2. Independent wire fixtures deliberately
  # include constructors the canonical encoder does not choose.
  @data <<0, 0x53, 0x75, 0xA0, 3, 0, 0xFF, 1>>
  @value <<0, 0x53, 0x77, 0xA1, 5, "hello">>
  @sequence <<0, 0x53, 0x76, 0xC0, 3, 2, 0x43, 0x42>>
  @header <<0, 0x53, 0x70, 0x45>>
  @properties <<0, 0x53, 0x73, 0xC0, 4, 1, 0xA1, 1, "m">>
  @noncanonical <<0, 0x80, 0x75::64, 0xB0, 3::32, 0, 0xFF, 1>>

  test "decodes all nine standard sections with exact tagged bodies" do
    fixtures = [
      {@header, {:header, Value.list([])}},
      {<<0, 0x53, 0x71, 0xC1, 1, 0>>, {:delivery_annotations, Value.map([])}},
      {<<0, 0x53, 0x72, 0xC1, 1, 0>>, {:message_annotations, Value.map([])}},
      {@properties, {:properties, Value.list([Value.string("m")])}},
      {<<0, 0x53, 0x74, 0xC1, 1, 0>>, {:application_properties, Value.map([])}},
      {@data, {:data, Value.binary(<<0, 0xFF, 1>>)}},
      {@sequence, {:amqp_sequence, Value.list([Value.uint(0), Value.boolean(false)])}},
      {@value, {:amqp_value, Value.string("hello")}},
      {<<0, 0x53, 0x78, 0xC1, 1, 0>>, {:footer, Value.map([])}}
    ]

    for {wire, section} <- fixtures do
      assert {:ok, ^section, <<0xFF>>} = Codec.decode_section(wire <> <<0xFF>>)
      assert {:ok, ^wire} = Codec.encode_section(section)
    end
  end

  test "symbolic section descriptors decode and encode numerically" do
    for {symbol, body, expected} <- [
          {"amqp:data:binary", <<0xA0, 3, 0, 0xFF, 1>>, @data},
          {"amqp:amqp-value:*", <<0xA1, 5, "hello">>, @value},
          {"amqp:header:list", <<0x45>>, @header}
        ] do
      wire = <<0, 0xA3, byte_size(symbol), symbol::binary, body::binary>>
      assert {:ok, section, <<>>} = Codec.decode_section(wire)
      assert {:ok, ^expected} = Codec.encode_section(section)
    end
  end

  test "whole-message view retains the original encoded content byte for byte" do
    bytes = @header <> @properties <> @noncanonical <> @data
    assert {:ok, %Message{} = message, <<>>} = Codec.decode(bytes)
    assert message.message_format == Value.uint(0)
    assert message.encoded_content == bytes
    assert Enum.map(message.sections, &elem(&1, 0)) == [:header, :properties, :data, :data]
    assert {:ok, ^bytes} = Codec.encode(message)
    assert {:ok, canonical} = Codec.encode_sections(message.sections)
    refute canonical == bytes
    assert {:ok, rebuilt, <<>>} = Codec.decode(canonical)
    assert rebuilt.sections == message.sections
  end

  test "original bytes are authoritative even when the optional parsed cache changes" do
    assert {:ok, message, <<>>} = Codec.decode(@data)
    assert {:ok, @data} = Codec.encode(%{message | sections: []})

    assert {:error, %Error{class: :unsupported}} =
             Codec.encode(%{message | message_format: Value.uint(1)})
  end

  test "valid message ordering permits repeated same-kind data or sequence body" do
    for body <- [@data <> @data, @sequence <> @sequence, @value] do
      bytes = @header <> @properties <> body <> <<0, 0x53, 0x78, 0xC1, 1, 0>>
      assert {:ok, message, <<>>} = Codec.decode(bytes)
      assert {:ok, _canonical} = Codec.encode_sections(message.sections)
    end
  end

  test "mixed body kinds, repeated singleton sections and reverse order fail" do
    for bytes <- [
          @data <> @sequence,
          @sequence <> @value,
          @value <> @value,
          @header <> @header,
          @properties <> @header,
          @data <> @properties,
          @data <> <<0, 0x53, 0x78, 0xC1, 1, 0>> <> @data
        ] do
      assert {:error, %Error{class: :malformed, reason: :invalid_section_order}} =
               Codec.decode(bytes)
    end

    assert {:error, %Error{class: :invalid_value, reason: :invalid_section_order}} =
             Codec.encode_sections([{:amqp_value, Value.null()}, {:data, Value.binary("x")}])
  end

  test "Header fields validate exact types, positional holes and full ranges" do
    fields = [
      Value.boolean(true),
      Value.ubyte(255),
      Value.uint(0xFFFF_FFFF),
      Value.boolean(true),
      Value.uint(0)
    ]

    assert {:ok, encoded} = Codec.encode_section({:header, Value.list(fields)})
    assert {:ok, {:header, Value.list(fields)}, <<>>} == Codec.decode_section(encoded)

    for {type, index} <- Enum.with_index([:boolean, :ubyte, :uint, :boolean, :uint]) do
      invalid = List.replace_at(fields, index, Value.string(Atom.to_string(type)))

      assert {:error, %Error{class: :invalid_value}} =
               Codec.encode_section({:header, Value.list(invalid)})

      assert {:error, %Error{class: :malformed}} =
               Codec.decode_section(raw(0x70, Value.list(invalid)))
    end

    assert {:error, %Error{reason: :too_many_section_fields}} =
             Codec.encode_section({:header, Value.list(List.duplicate(Value.null(), 6))})
  end

  test "Properties support every standard message ID and exact optional field types" do
    for id <- [Value.ulong(5), Value.uuid(<<0::128>>), Value.binary(<<0xFF>>), Value.string("id")] do
      fields = [
        id,
        Value.binary("u"),
        Value.string("to"),
        Value.string("s"),
        Value.string("reply"),
        id,
        Value.symbol("application/octet-stream"),
        Value.symbol("gzip"),
        Value.timestamp(-1),
        Value.timestamp(0),
        Value.string("g"),
        Value.uint(4),
        Value.string("rg")
      ]

      assert {:ok, encoded} = Codec.encode_section({:properties, Value.list(fields)})
      assert {:ok, {:properties, Value.list(fields)}, <<>>} == Codec.decode_section(encoded)
    end

    for index <- 0..12 do
      fields = List.duplicate(Value.null(), 13) |> List.replace_at(index, Value.boolean(true))

      assert {:error, %Error{class: :invalid_value}} =
               Codec.encode_section({:properties, Value.list(fields)})

      assert {:error, %Error{class: :malformed}} =
               Codec.decode_section(raw(0x73, Value.list(fields)))
    end
  end

  test "annotation keys are symbol or ulong while generic maps still accept null keys" do
    for name <- [:delivery_annotations, :message_annotations, :footer] do
      valid =
        Value.map([{Value.symbol("x-opt-k"), Value.null()}, {Value.ulong(1), Value.string("v")}])

      assert {:ok, wire} = Codec.encode_section({name, valid})
      assert {:ok, {^name, ^valid}, <<>>} = Codec.decode_section(wire)

      for key <- [Value.null(), Value.string("x"), Value.uint(0)] do
        invalid = Value.map([{key, Value.boolean(true)}])
        assert {:error, %Error{class: :invalid_value}} = Codec.encode_section({name, invalid})
      end
    end

    assert {:ok, _} = WireValue.encode(Value.map([{Value.null(), Value.uint(0)}]))
  end

  test "delivery annotation rejected contains an Error schema, not arbitrary data" do
    for value <- [
          Value.boolean(false),
          Value.described(Value.ulong(0x1D), Value.list([])),
          Value.described(Value.ulong(0x24), Value.list([]))
        ] do
      section = {:delivery_annotations, Value.map([{Value.symbol("rejected"), value}])}
      assert {:error, %Error{class: :invalid_value}} = Codec.encode_section(section)
    end

    error = Value.described(Value.ulong(0x1D), Value.list([Value.symbol("x-error")]))
    section = {:delivery_annotations, Value.map([{Value.symbol("rejected"), error}])}
    assert {:ok, wire} = Codec.encode_section(section)
    assert {:ok, ^section, <<>>} = Codec.decode_section(wire)
  end

  test "Application Properties have string keys and supported simple values only" do
    for value <- [
          Value.null(),
          Value.boolean(false),
          Value.int(-1),
          Value.binary(<<0xFF>>),
          Value.uuid(<<0::128>>)
        ] do
      section = {:application_properties, Value.map([{Value.string("k"), value}])}
      assert {:ok, encoded} = Codec.encode_section(section)
      assert {:ok, ^section, <<>>} = Codec.decode_section(encoded)
    end

    for value <- [
          Value.list([]),
          Value.map([]),
          Value.array(:symbol, []),
          Value.described(Value.ulong(100), Value.uint(0))
        ] do
      assert {:error, %Error{class: :invalid_value}} =
               Codec.encode_section(
                 {:application_properties, Value.map([{Value.string("k"), value}])}
               )
    end

    for key <- [Value.null(), Value.symbol("k")] do
      assert {:error, %Error{class: :invalid_value}} =
               Codec.encode_section({:application_properties, Value.map([{key, Value.uint(0)}])})
    end
  end

  test "body constructors retain type identity and AMQP value retains extension data" do
    for section <- [
          {:data, Value.string("wrong")},
          {:amqp_sequence, Value.binary("wrong")},
          {:footer, Value.list([])},
          {:header, Value.map([])}
        ] do
      assert {:error, %Error{class: :invalid_value}} = Codec.encode_section(section)
    end

    unknown = Value.described(Value.symbol("x:application:type"), Value.list([Value.binary("x")]))
    assert {:ok, encoded} = Codec.encode_section({:amqp_value, unknown})
    assert {:ok, {:amqp_value, ^unknown}, <<>>} = Codec.decode_section(encoded)
  end

  test "content-encoding is forbidden with sequence or AMQP-value bodies" do
    fields = List.duplicate(Value.null(), 7) ++ [Value.symbol("gzip")]
    properties = {:properties, Value.list(fields)}

    for {name, value, bytes} <- [
          {:amqp_sequence, Value.list([Value.uint(0), Value.boolean(false)]), @sequence},
          {:amqp_value, Value.string("hello"), @value}
        ] do
      assert {:error, %Error{class: :invalid_value, reason: :content_encoding_requires_data}} =
               Codec.encode_sections([properties, {name, value}])

      assert {:error, %Error{class: :malformed, reason: :content_encoding_requires_data}} =
               Codec.decode(raw(0x73, Value.list(fields)) <> bytes)
    end

    assert {:ok, _} =
             Codec.encode_sections([properties, {:data, Value.binary("compressed bytes")}])
  end

  test "identity content encoding is forbidden independently of body kind" do
    for name <- ["identity", "IDENTITY", "iDeNtItY"] do
      fields = List.duplicate(Value.null(), 7) ++ [Value.symbol(name)]
      properties = {:properties, Value.list(fields)}

      assert {:error, %Error{class: :invalid_value, reason: :identity_content_encoding}} =
               Codec.encode_section(properties)

      assert {:error, %Error{class: :malformed, reason: :identity_content_encoding}} =
               Codec.decode_section(raw(0x73, Value.list(fields)))
    end
  end

  test "section prefixes remain incomplete and malformed or unknown sections are explicit" do
    for wire <- [@data, @properties, @sequence, @noncanonical] do
      for size <- 0..(byte_size(wire) - 1) do
        assert {:more, needed} = Codec.decode_section(binary_part(wire, 0, size))
        assert needed > 0
      end
    end

    assert {:error, %Error{class: :unsupported}} = Codec.decode_section(<<0, 0x53, 0x79, 0x45>>)
    assert {:error, %Error{class: :malformed}} = Codec.decode_section(<<0x40>>)
    assert {:error, %Error{class: :unsupported}} = Codec.decode(@data <> <<0xFF>>)
  end

  test "aggregate message bytes and section count obey their explicit limits" do
    sections = [{:data, Value.binary(<<0, 0xFF, 1>>)}, {:data, Value.binary(<<0, 0xFF, 1>>)}]
    limits = %{Limits.default() | max_value_bytes: 2 * byte_size(@data)}
    assert {:ok, bytes} = Codec.encode_sections(sections, limits)
    assert {:ok, _, <<>>} = Codec.decode(bytes, limits)

    assert {:error, %Error{class: :limit_exceeded}} =
             Codec.encode_sections(sections, %{
               limits
               | max_value_bytes: limits.max_value_bytes - 1
             })

    assert {:error, %Error{class: :limit_exceeded}} =
             Codec.decode(bytes, %{limits | max_value_bytes: limits.max_value_bytes - 1})

    assert {:error, %Error{class: :limit_exceeded}} =
             Codec.encode_sections(sections, %{limits | max_compound_items: 1})

    assert {:error, %Error{class: :limit_exceeded}} =
             Codec.decode(bytes, %{limits | max_compound_items: 1})
  end

  test "aggregate preflight rejects before materializing repeated body sections" do
    sections = List.duplicate({:data, Value.binary(:binary.copy("x", 262_144))}, 64)
    parent = self()

    {pid, monitor} =
      :erlang.spawn_opt(
        fn ->
          send(
            parent,
            {self(),
             Codec.encode_sections(sections, %{Limits.default() | max_value_bytes: 262_160})}
          )
        end,
        [
          :monitor,
          {:max_heap_size,
           %{
             size: div(24_000_000, :erlang.system_info(:wordsize)),
             kill: true,
             error_logger: false,
             include_shared_binaries: true
           }}
        ]
      )

    on_exit(fn -> if Process.alive?(pid), do: Process.exit(pid, :kill) end)
    assert_receive {^pid, {:error, %Error{class: :limit_exceeded}}}, 5_000
    assert_receive {:DOWN, ^monitor, :process, ^pid, :normal}
  end

  defp raw(descriptor, body) do
    {:ok, wire} = WireValue.encode(Value.described(Value.ulong(descriptor), body))
    wire
  end
end
