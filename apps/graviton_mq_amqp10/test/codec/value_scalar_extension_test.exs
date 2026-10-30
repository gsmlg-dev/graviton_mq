defmodule GravitonMQ.AMQP10.Codec.ValueScalarExtensionTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.AMQP10.Codec.{Error, Limits, Performative, Value}
  alias GravitonMQ.AMQP10.Error, as: ProtocolError
  alias GravitonMQ.AMQP10.Performative.{Begin, End, Open}
  alias GravitonMQ.AMQP10.Value, as: AMQPValue

  # OASIS AMQP 1.0 Types §§1.6.3, 1.6.7–10, 1.6.17–19:
  # fixed integers are network-order two's complement; timestamp is signed
  # milliseconds since the Unix epoch, UUID is 16 octets, binary is opaque.
  @uuid <<0xF8, 0x1D, 0x4F, 0xAE, 0x7D, 0xEC, 0x11, 0xD0, 0xA7, 0x65, 0x00, 0xA0, 0xC9, 0x1E,
          0x6B, 0xF6>>
  @vectors [
    {<<0x50, 0>>, AMQPValue.ubyte(0), <<0x50, 0>>},
    {<<0x50, 0xFF>>, AMQPValue.ubyte(255), <<0x50, 0xFF>>},
    {<<0x51, 0x80>>, AMQPValue.byte(-128), <<0x51, 0x80>>},
    {<<0x51, 0xFF>>, AMQPValue.byte(-1), <<0x51, 0xFF>>},
    {<<0x51, 0>>, AMQPValue.byte(0), <<0x51, 0>>},
    {<<0x51, 0x7F>>, AMQPValue.byte(127), <<0x51, 0x7F>>},
    {<<0x61, 0x80, 0>>, AMQPValue.short(-32_768), <<0x61, 0x80, 0>>},
    {<<0x61, 0xFF, 0xFF>>, AMQPValue.short(-1), <<0x61, 0xFF, 0xFF>>},
    {<<0x61, 0, 0>>, AMQPValue.short(0), <<0x61, 0, 0>>},
    {<<0x61, 0x7F, 0xFF>>, AMQPValue.short(32_767), <<0x61, 0x7F, 0xFF>>},
    {<<0x54, 0x80>>, AMQPValue.int(-128), <<0x54, 0x80>>},
    {<<0x54, 0xFF>>, AMQPValue.int(-1), <<0x54, 0xFF>>},
    {<<0x54, 0>>, AMQPValue.int(0), <<0x54, 0>>},
    {<<0x54, 0x7F>>, AMQPValue.int(127), <<0x54, 0x7F>>},
    {<<0x71, 0x80, 0, 0, 0>>, AMQPValue.int(-2_147_483_648), <<0x71, 0x80, 0, 0, 0>>},
    {<<0x71, 0xFF, 0xFF, 0xFF, 0x7F>>, AMQPValue.int(-129), <<0x71, 0xFF, 0xFF, 0xFF, 0x7F>>},
    {<<0x71, 0xFF, 0xFF, 0xFF, 0x80>>, AMQPValue.int(-128), <<0x54, 0x80>>},
    {<<0x71, 0, 0, 0, 0>>, AMQPValue.int(0), <<0x54, 0>>},
    {<<0x71, 0, 0, 0, 0x7F>>, AMQPValue.int(127), <<0x54, 0x7F>>},
    {<<0x71, 0, 0, 0, 0x80>>, AMQPValue.int(128), <<0x71, 0, 0, 0, 0x80>>},
    {<<0x71, 0x7F, 0xFF, 0xFF, 0xFF>>, AMQPValue.int(2_147_483_647),
     <<0x71, 0x7F, 0xFF, 0xFF, 0xFF>>},
    {<<0x55, 0x80>>, AMQPValue.long(-128), <<0x55, 0x80>>},
    {<<0x55, 0xFF>>, AMQPValue.long(-1), <<0x55, 0xFF>>},
    {<<0x55, 0>>, AMQPValue.long(0), <<0x55, 0>>},
    {<<0x55, 0x7F>>, AMQPValue.long(127), <<0x55, 0x7F>>},
    {<<0x81, 0x80, 0, 0, 0, 0, 0, 0, 0>>, AMQPValue.long(-9_223_372_036_854_775_808),
     <<0x81, 0x80, 0, 0, 0, 0, 0, 0, 0>>},
    {<<0x81, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F>>, AMQPValue.long(-129),
     <<0x81, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F>>},
    {<<0x81, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x80>>, AMQPValue.long(-128),
     <<0x55, 0x80>>},
    {<<0x81, 0, 0, 0, 0, 0, 0, 0, 0>>, AMQPValue.long(0), <<0x55, 0>>},
    {<<0x81, 0, 0, 0, 0, 0, 0, 0, 0x7F>>, AMQPValue.long(127), <<0x55, 0x7F>>},
    {<<0x81, 0, 0, 0, 0, 0, 0, 0, 0x80>>, AMQPValue.long(128),
     <<0x81, 0, 0, 0, 0, 0, 0, 0, 0x80>>},
    {<<0x81, 0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF>>,
     AMQPValue.long(9_223_372_036_854_775_807),
     <<0x81, 0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF>>},
    {<<0x83, 0x80, 0, 0, 0, 0, 0, 0, 0>>, AMQPValue.timestamp(-9_223_372_036_854_775_808),
     <<0x83, 0x80, 0, 0, 0, 0, 0, 0, 0>>},
    {<<0x83, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF>>, AMQPValue.timestamp(-1),
     <<0x83, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF>>},
    {<<0x83, 0, 0, 0, 0, 0, 0, 0, 0>>, AMQPValue.timestamp(0), <<0x83, 0, 0, 0, 0, 0, 0, 0, 0>>},
    {<<0x83, 0, 0, 1, 0x31, 0x67, 0xAD, 0xB8, 0xA1>>, AMQPValue.timestamp(1_311_704_463_521),
     <<0x83, 0, 0, 1, 0x31, 0x67, 0xAD, 0xB8, 0xA1>>},
    {<<0x83, 0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF>>,
     AMQPValue.timestamp(9_223_372_036_854_775_807),
     <<0x83, 0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF>>},
    {<<0x98, @uuid::binary>>, AMQPValue.uuid(@uuid), <<0x98, @uuid::binary>>},
    {<<0x98, 0::128>>, AMQPValue.uuid(<<0::128>>), <<0x98, 0::128>>},
    {<<0x98, 0xFFFF_FFFF_FFFF_FFFF_FFFF_FFFF_FFFF_FFFF::128>>,
     AMQPValue.uuid(<<0xFFFF_FFFF_FFFF_FFFF_FFFF_FFFF_FFFF_FFFF::128>>),
     <<0x98, 0xFFFF_FFFF_FFFF_FFFF_FFFF_FFFF_FFFF_FFFF::128>>},
    {<<0xA0, 0>>, AMQPValue.binary(<<>>), <<0xA0, 0>>},
    {<<0xB0, 0::32>>, AMQPValue.binary(<<>>), <<0xA0, 0>>},
    {<<0xA0, 4, 0, 0xFF, 0xC0, 0x80>>, AMQPValue.binary(<<0, 0xFF, 0xC0, 0x80>>),
     <<0xA0, 4, 0, 0xFF, 0xC0, 0x80>>},
    {<<0xB0, 4::32, 0, 0xFF, 0xC0, 0x80>>, AMQPValue.binary(<<0, 0xFF, 0xC0, 0x80>>),
     <<0xA0, 4, 0, 0xFF, 0xC0, 0x80>>}
  ]

  @integer_ranges [
    ubyte: {0, 255},
    byte: {-128, 127},
    short: {-32_768, 32_767},
    int: {-2_147_483_648, 2_147_483_647},
    long: {-9_223_372_036_854_775_808, 9_223_372_036_854_775_807},
    timestamp: {-9_223_372_036_854_775_808, 9_223_372_036_854_775_807}
  ]

  test "independent scalar wire vectors preserve semantic identity and exact suffixes" do
    suffix = <<0xFF, 0x40, 0x98, 0x00>>

    for {wire, expected, _canonical} <- @vectors do
      assert {:ok, ^expected, <<>>} = Value.decode(wire)
      assert {:ok, ^expected, ^suffix} = Value.decode(wire <> suffix)
    end
  end

  test "every strict scalar prefix requests the exact minimum missing bytes" do
    for {wire, _value, _canonical} <- @vectors,
        prefix_size <- 0..(byte_size(wire) - 1) do
      prefix = binary_part(wire, 0, prefix_size)
      assert {:more, expected_more(wire, prefix_size)} == Value.decode(prefix)
    end
  end

  test "canonical encoding selects compact signed and binary constructors without retagging" do
    for {wire, expected, canonical} <- @vectors do
      assert {:ok, ^canonical} = Value.encode(expected)
      assert {:ok, decoded, <<>>} = Value.decode(wire)
      assert {:ok, ^canonical} = Value.encode(decoded)
      refute decoded.type in [:smallint, :smalllong, :vbin8, :vbin32]
    end
  end

  test "binary lengths at 255 and 256 use exact declared widths and retain arbitrary octets" do
    for size <- [255, 256] do
      bytes = :binary.copy(<<0xFF>>, size)

      canonical =
        if size == 255, do: <<0xA0, size, bytes::binary>>, else: <<0xB0, size::32, bytes::binary>>

      value = AMQPValue.binary(bytes)
      assert {:ok, ^canonical} = Value.encode(value)
      assert {:ok, ^value, <<0x40>>} = Value.decode(canonical <> <<0x40>>)
      assert {:ok, ^value, <<>>} = Value.decode(<<0xB0, size::32, bytes::binary>>)
    end
  end

  test "signed and unsigned families and empty binary/string/symbol/null remain distinct map keys" do
    keys = [
      AMQPValue.ubyte(0),
      AMQPValue.ushort(0),
      AMQPValue.uint(0),
      AMQPValue.ulong(0),
      AMQPValue.byte(0),
      AMQPValue.short(0),
      AMQPValue.int(0),
      AMQPValue.long(0),
      AMQPValue.timestamp(0),
      AMQPValue.uuid(<<0::128>>),
      AMQPValue.binary(<<>>),
      AMQPValue.string(""),
      AMQPValue.symbol(""),
      AMQPValue.null()
    ]

    body =
      <<0x50, 0, 0x40, 0x60, 0, 0, 0x40, 0x43, 0x40, 0x44, 0x40, 0x51, 0, 0x40, 0x61, 0, 0, 0x40,
        0x54, 0, 0x40, 0x55, 0, 0x40, 0x83, 0::64, 0x40, 0x98, 0::128, 0x40, 0xA0, 0, 0x40, 0xA1,
        0, 0x40, 0xA3, 0, 0x40, 0x40, 0x40>>

    wire = <<0xC1, byte_size(body) + 1, 28, body::binary>>
    expected = AMQPValue.map(Enum.map(keys, &{&1, AMQPValue.null()}))
    assert length(Enum.uniq(keys)) == 14
    assert {:ok, ^expected, <<>>} = Value.decode(wire)
    assert {:ok, ^wire} = Value.encode(expected)
  end

  test "alternate constructors normalize before duplicate-key validation" do
    for {key, first, second} <- [
          {AMQPValue.int(0), <<0x71, 0::32>>, <<0x54, 0>>},
          {AMQPValue.long(0), <<0x81, 0::64>>, <<0x55, 0>>},
          {AMQPValue.binary(<<>>), <<0xB0, 0::32>>, <<0xA0, 0>>}
        ] do
      body = <<first::binary, 0x40, second::binary, 0x40>>

      assert_error(
        Value.decode(<<0xC1, byte_size(body) + 1, 4, body::binary>>),
        :value_decode,
        :malformed,
        :duplicate_map_key
      )

      assert_error(
        Value.encode(AMQPValue.map([{key, AMQPValue.null()}, {key, AMQPValue.null()}])),
        :value_encode,
        :invalid_value,
        :duplicate_map_key
      )
    end
  end

  test "integer range and payload types are checked exactly without wrapping or coercion" do
    for {type, {minimum, maximum}} <- @integer_ranges do
      for value <- [minimum, maximum] do
        assert %AMQPValue{type: ^type, value: ^value} = apply(AMQPValue, type, [value])
        assert {:ok, wire} = Value.encode(%AMQPValue{type: type, value: value})
        assert {:ok, %AMQPValue{type: ^type, value: ^value}, <<>>} = Value.decode(wire)
      end

      for value <- [
            minimum - 1,
            maximum + 1,
            nil,
            false,
            0.0,
            <<0>>,
            [],
            %{},
            ~U[1970-01-01 00:00:00Z]
          ] do
        assert {:error,
                %Error{
                  operation: :value_encode,
                  class: :invalid_value,
                  reason: :invalid_semantic_value,
                  details: %{type: ^type}
                }} =
                 Value.encode(%AMQPValue{type: type, value: value})
      end
    end

    for {type, value} <- [
          binary: nil,
          binary: [0],
          binary: <<0::1>>,
          uuid: nil,
          uuid: "",
          uuid: <<0::120>>,
          uuid: <<0::136>>,
          uuid: "f81d4fae-7dec-11d0-a765-00a0c91e6bf6"
        ] do
      assert_error(
        Value.encode(%AMQPValue{type: type, value: value}),
        :value_encode,
        :invalid_value,
        :invalid_semantic_value
      )
    end
  end

  test "new scalars work recursively through list/map/described independent vectors" do
    for {wire, value, canonical} <- @vectors do
      expected =
        AMQPValue.described(
          AMQPValue.ulong(0xFF),
          AMQPValue.map([{AMQPValue.symbol("k"), AMQPValue.list([value])}])
        )

      nested = nested_wire(wire)
      canonical = nested_wire(canonical)
      assert {:ok, ^expected, <<0xFF>>} = Value.decode(nested <> <<0xFF>>)
      assert {:ok, ^canonical} = Value.encode(expected)

      for size <- 0..(byte_size(nested) - 1) do
        assert {:more, needed} = Value.decode(binary_part(nested, 0, size))
        assert needed > 0
      end
    end
  end

  test "binary declared lengths fail before waiting for or allocating an oversized payload" do
    limits = %{Limits.default() | max_value_bytes: 3}

    for header <- [<<0xA0, 4>>, <<0xB0, 4::32>>, <<0xB0, 0xFFFF_FFFF::32>>] do
      assert_error(
        Value.decode(header, limits),
        :value_decode,
        :limit_exceeded,
        :value_size_limit
      )
    end

    assert_error(
      Value.encode(AMQPValue.binary("four"), limits),
      :value_encode,
      :limit_exceeded,
      :value_size_limit
    )

    assert {:ok, <<0xA0, 3, 0xFF, 0, 0x80>>} =
             Value.encode(AMQPValue.binary(<<0xFF, 0, 0x80>>), limits)
  end

  test "new fixed scalars enforce their payload byte budgets including noncanonical widths" do
    for {wire, value, canonical} <- @vectors, value.type != :binary do
      width = byte_size(wire) - 1
      limits = %{Limits.default() | max_value_bytes: width}
      assert {:ok, ^value, <<>>} = Value.decode(wire, limits)

      if width > 1 do
        small = %{limits | max_value_bytes: width - 1}

        assert_error(
          Value.decode(binary_part(wire, 0, 1), small),
          :value_decode,
          :limit_exceeded,
          :value_size_limit
        )

        assert_error(Value.decode(wire, small), :value_decode, :limit_exceeded, :value_size_limit)
      end

      canonical_width = byte_size(canonical) - 1
      assert {:ok, ^canonical} = Value.encode(value, %{limits | max_value_bytes: canonical_width})

      if canonical_width > 1 do
        assert_error(
          Value.encode(value, %{limits | max_value_bytes: canonical_width - 1}),
          :value_encode,
          :limit_exceeded,
          :value_size_limit
        )
      end
    end
  end

  test "new scalars retain aggregate byte, item count and nesting limits" do
    for {_wire, value, canonical} <- @vectors do
      body = <<0xC0, byte_size(canonical) + 1, 1, canonical::binary>>
      list = AMQPValue.list([value])
      budget = byte_size(canonical) + 1
      assert {:ok, ^body} = Value.encode(list, %{Limits.default() | max_value_bytes: budget})

      for {wire, term, limits, reason} <- [
            {body, list, %{Limits.default() | max_value_bytes: budget - 1}, :value_size_limit},
            {<<0xC0, 1 + byte_size(canonical) * 2, 2, canonical::binary, canonical::binary>>,
             AMQPValue.list([value, value]), %{Limits.default() | max_compound_items: 1},
             :compound_item_limit},
            {<<0xC0, byte_size(body) + 1, 1, body::binary>>, AMQPValue.list([list]),
             %{Limits.default() | max_nesting_depth: 1}, :nesting_depth_limit}
          ] do
        assert_error(Value.decode(wire, limits), :value_decode, :limit_exceeded, reason)
        assert_error(Value.encode(term, limits), :value_encode, :limit_exceeded, reason)
      end
    end
  end

  test "binary aggregate preflight rejects over-budget children before later unsupported values" do
    unsupported = %AMQPValue{type: :extension, value: nil}
    values = [AMQPValue.binary("abc"), AMQPValue.binary("abc"), unsupported]

    assert_error(
      Value.encode(AMQPValue.list(values), %{Limits.default() | max_value_bytes: 8}),
      :value_encode,
      :limit_exceeded,
      :value_size_limit
    )

    assert_error(
      Value.encode(
        AMQPValue.map([
          {AMQPValue.null(), AMQPValue.binary("abc")},
          {AMQPValue.uint(0), AMQPValue.binary("abc")},
          {AMQPValue.uint(1), unsupported}
        ]),
        %{Limits.default() | max_value_bytes: 8}
      ),
      :value_encode,
      :limit_exceeded,
      :value_size_limit
    )
  end

  test "binary aggregate allocation is rejected within a finite process heap budget" do
    value = AMQPValue.list(List.duplicate(AMQPValue.binary(:binary.copy(<<0xFF>>, 262_144)), 64))
    parent = self()

    {pid, monitor} =
      :erlang.spawn_opt(
        fn ->
          send(
            parent,
            {self(), Value.encode(value, %{Limits.default() | max_value_bytes: 262_144})}
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

    assert_receive {^pid, {:error, %Error{class: :limit_exceeded, reason: :value_size_limit}}},
                   5_000

    assert_receive {:DOWN, ^monitor, :process, ^pid, :normal}
  end

  test "scalar arrays preserve shared types and other scalar descriptors remain unsupported" do
    for {type, value, constructor} <- [
          {:ubyte, AMQPValue.ubyte(0), 0x50},
          {:byte, AMQPValue.byte(0), 0x51},
          {:short, AMQPValue.short(0), 0x61},
          {:int, AMQPValue.int(0), 0x54},
          {:long, AMQPValue.long(0), 0x55},
          {:timestamp, AMQPValue.timestamp(0), 0x83},
          {:uuid, AMQPValue.uuid(@uuid), 0x98},
          {:binary, AMQPValue.binary(<<>>), 0xA0}
        ] do
      array = AMQPValue.array(type, [value])
      assert {:ok, encoded} = Value.encode(array)
      assert {:ok, ^array, <<>>} = Value.decode(encoded)
      empty = AMQPValue.array(type, [])
      assert {:ok, ^empty, <<>>} = Value.decode(<<0xE0, 2, 0, constructor>>)

      {_input, _value, canonical} =
        Enum.find(@vectors, fn {_wire, candidate, _} -> candidate == value end)

      assert_error(
        Value.decode(<<0x00, canonical::binary, 0x40>>),
        :value_decode,
        :unsupported,
        :descriptor_type
      )
    end

    for {constructor, type} <- [{0x71, :int}, {0x81, :long}, {0xB0, :binary}] do
      empty = AMQPValue.array(type, [])
      assert {:ok, ^empty, <<>>} = Value.decode(<<0xE0, 2, 0, constructor>>)
    end
  end

  test "Open Begin and Error schema maps accept new values but retain symbol-only keys" do
    for {_wire, value, _canonical} <- @vectors do
      properties = AMQPValue.map([{AMQPValue.symbol("x"), value}])

      packets = [
        %Open{container_id: AMQPValue.string("c"), properties: properties},
        %Begin{
          next_outgoing_id: AMQPValue.uint(0),
          incoming_window: AMQPValue.uint(1),
          outgoing_window: AMQPValue.uint(1),
          properties: properties
        },
        %End{error: %ProtocolError{condition: AMQPValue.symbol("x"), info: properties}}
      ]

      for packet <- packets do
        assert {:ok, wire} = Performative.encode(packet)
        assert {:ok, decoded, <<>>} = Performative.decode(wire)
        retained = if match?(%End{}, decoded), do: decoded.error.info, else: decoded.properties
        assert retained == properties
      end

      invalid = %Open{
        container_id: AMQPValue.string("c"),
        properties: AMQPValue.map([{value, AMQPValue.null()}])
      }

      assert_error(
        Performative.encode(invalid),
        :performative_encode,
        :invalid_value,
        :invalid_property_key
      )
    end

    assert_error(
      Value.decode(<<0xC0, 2, 1, 0x98, 0::128>>),
      :value_decode,
      :malformed,
      :compound_item_truncated
    )
  end

  defp nested_wire(wire) do
    list = <<0xC0, byte_size(wire) + 1, 1, wire::binary>>
    <<0x00, 0x53, 0xFF, 0xC1, byte_size(list) + 4, 2, 0xA3, 1, "k", list::binary>>
  end

  defp expected_more(_wire, 0), do: 1
  defp expected_more(<<0xA0, _::binary>>, size) when size < 2, do: 2 - size
  defp expected_more(<<0xB0, _::binary>>, size) when size < 5, do: 5 - size
  defp expected_more(wire, size), do: byte_size(wire) - size

  defp assert_error(result, operation, class, reason) do
    assert {:error, %Error{operation: ^operation, class: ^class, reason: ^reason}} = result
  end
end
