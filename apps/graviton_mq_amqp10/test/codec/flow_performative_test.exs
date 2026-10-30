defmodule GravitonMQ.AMQP10.Codec.FlowPerformativeTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.AMQP10.Codec.Error
  alias GravitonMQ.AMQP10.Codec.Limits
  alias GravitonMQ.AMQP10.Codec.Performative
  alias GravitonMQ.AMQP10.Codec.Value
  alias GravitonMQ.AMQP10.Performative.Flow
  alias GravitonMQ.AMQP10.Value, as: AMQPValue

  # OASIS AMQP 1.0 Transport, section 2.7.4. These vectors are independent
  # of the production encoder; schema-error cases below use the value codec.
  @minimal <<0x00, 0x53, 0x13, 0xC0, 8, 4, 0x40, 0x52, 100, 0x52, 1, 0x52, 200>>
  @symbolic <<0x00, 0xA3, 14, "amqp:flow:list", 0xC0, 8, 4, 0x40, 0x52, 100, 0x52, 1, 0x52, 200>>
  @full <<0x00, 0x53, 0x13, 0xC0, 28, 11, 0x43, 0x52, 100, 0x52, 1, 0x52, 200, 0x52, 7, 0x70,
          0xFF, 0xFF, 0xFF, 0xFF, 0x52, 8, 0x52, 9, 0x41, 0x42, 0xC1, 5, 2, 0xA3, 1, "k", 0x41>>
  @canonical_full <<0x00, 0x53, 0x13, 0xC0, 28, 11, 0x43, 0x52, 100, 0x52, 1, 0x52, 200, 0x52, 7,
                    0x70, 0xFF, 0xFF, 0xFF, 0xFF, 0x52, 8, 0x52, 9, 0x41, 0x40, 0xC1, 5, 2, 0xA3,
                    1, "k", 0x41>>
  @property_holes <<0x00, 0x53, 0x13, 0xC0, 20, 11, 0x40, 0x52, 100, 0x52, 1, 0x52, 200, 0x40,
                    0x40, 0x40, 0x40, 0x40, 0x40, 0xC1, 4, 2, 0xA3, 0, 0x42>>

  test "accepts numeric and symbolic descriptors with exact mandatory uint fields" do
    for wire <- [@minimal, @symbolic] do
      assert {:ok, flow, <<>>} = Performative.decode(wire)
      assert flow.__struct__ == Flow
      assert flow == minimal_flow()
    end
  end

  test "decodes all eleven ordered fields and retains semantic identities" do
    assert {:ok, flow, <<>>} = Performative.decode(@full)
    assert flow == full_flow()
  end

  test "canonical encoding uses the numeric descriptor and independent literal vectors" do
    assert {:ok, @minimal} = Performative.encode(minimal_flow())
    assert {:ok, @canonical_full} = Performative.encode(full_flow())
    assert {:ok, symbolic, <<>>} = Performative.decode(@symbolic)
    assert {:ok, @minimal} = Performative.encode(symbolic)
  end

  test "materializes absent or null Boolean defaults and trims trailing defaults" do
    fields = minimum_fields() ++ List.duplicate(AMQPValue.null(), 6)

    for wire <- [@minimal, encode_fields!(fields)] do
      assert {:ok, flow, <<>>} = Performative.decode(wire)
      assert flow.drain == AMQPValue.boolean(false)
      assert flow.echo == AMQPValue.boolean(false)
      assert {:ok, @minimal} = Performative.encode(flow)
    end

    for default <- [nil, AMQPValue.null(), AMQPValue.boolean(false)] do
      flow = %{minimal_flow() | drain: default, echo: default}
      assert {:ok, @minimal} = Performative.encode(flow)
    end
  end

  test "preserves interior holes before properties and an independently present echo" do
    properties = AMQPValue.map([{AMQPValue.symbol(""), AMQPValue.boolean(false)}])
    flow = %{minimal_flow() | properties: properties}
    assert {:ok, @property_holes} = Performative.encode(flow)
    assert {:ok, ^flow, <<>>} = Performative.decode(@property_holes)

    echo = %{minimal_flow() | echo: AMQPValue.boolean(true)}

    echo_wire =
      <<0x00, 0x53, 0x13, 0xC0, 14, 10, 0x40, 0x52, 100, 0x52, 1, 0x52, 200, 0x40, 0x40, 0x40,
        0x40, 0x40, 0x41>>

    assert {:ok, ^echo_wire} = Performative.encode(echo)
    assert {:ok, ^echo, <<>>} = Performative.decode(echo_wire)
  end

  test "accepts all Boolean constructors and canonically reencodes drain and echo" do
    for {wire, value, canonical} <- [
          {<<0x41>>, true, <<0x41>>},
          {<<0x42>>, false, <<0x40>>},
          {<<0x56, 1>>, true, <<0x41>>},
          {<<0x56, 0>>, false, <<0x40>>}
        ] do
      prefix = <<0x40, 0x52, 100, 0x52, 1, 0x52, 200, 0x40, 0x40, 0x40, 0x40>>
      body = <<prefix::binary, wire::binary, wire::binary, 0xC1, 1, 0>>
      encoded = <<0x00, 0x53, 0x13, 0xC0, byte_size(body) + 1, 11, body::binary>>
      assert {:ok, flow, <<>>} = Performative.decode(encoded)
      assert flow.drain == AMQPValue.boolean(value)
      assert flow.echo == AMQPValue.boolean(value)

      canonical_body = <<prefix::binary, canonical::binary, canonical::binary, 0xC1, 1, 0>>

      expected =
        <<0x00, 0x53, 0x13, 0xC0, byte_size(canonical_body) + 1, 11, canonical_body::binary>>

      assert {:ok, ^expected} = Performative.encode(flow)
    end
  end

  test "every strict vector prefix requests more input and complete values preserve suffixes" do
    suffix = <<0xDE, 0xAD, 0x00, 0x53, 0x13>>

    for wire <- [@minimal, @symbolic, @full, @property_holes] do
      for size <- 0..(byte_size(wire) - 1) do
        assert {:more, needed} = Performative.decode(binary_part(wire, 0, size))
        assert needed > 0
      end

      assert {:ok, expected, <<>>} = Performative.decode(wire)
      assert {:ok, ^expected, ^suffix} = Performative.decode(wire <> suffix)
    end
  end

  test "rejects each mandatory field when missing or explicitly null" do
    for {name, index} <- [incoming_window: 1, next_outgoing_id: 2, outgoing_window: 3],
        fields <- [
          Enum.take(minimum_fields(), index),
          List.replace_at(minimum_fields(), index, AMQPValue.null())
        ] do
      error =
        schema_error(
          Performative.decode(encode_fields!(fields)),
          :performative_decode,
          :malformed,
          :mandatory_field_missing
        )

      assert error.details == %{field: name, index: index}

      flow = Map.put(minimal_flow(), name, nil)

      error =
        schema_error(
          Performative.encode(flow),
          :performative_encode,
          :invalid_value,
          :mandatory_field_missing
        )

      assert error.details == %{field: name, index: index}
    end
  end

  test "checks the exact semantic type in every field on decode and encode" do
    names = [
      :next_incoming_id,
      :incoming_window,
      :next_outgoing_id,
      :outgoing_window,
      :handle,
      :delivery_count,
      :link_credit,
      :available,
      :drain,
      :echo,
      :properties
    ]

    wrong =
      List.duplicate(AMQPValue.ulong(1), 8) ++
        [AMQPValue.uint(0), AMQPValue.uint(1), AMQPValue.list([])]

    for {{name, wrong_value}, index} <- Enum.with_index(Enum.zip(names, wrong)) do
      fields = List.replace_at(full_fields(), index, wrong_value)

      error =
        schema_error(
          Performative.decode(encode_fields!(fields)),
          :performative_decode,
          :malformed,
          :field_type_mismatch
        )

      assert error.details.field == name
      assert error.details.index == index

      error =
        schema_error(
          Performative.encode(Map.put(full_flow(), name, wrong_value)),
          :performative_encode,
          :invalid_value,
          :field_type_mismatch
        )

      assert error.details.field == name
      assert error.details.index == index
    end
  end

  test "rejects additional fields even when they are null" do
    error =
      schema_error(
        Performative.decode(encode_fields!(full_fields() ++ [AMQPValue.null()])),
        :performative_decode,
        :malformed,
        :too_many_fields
      )

    assert error.details == %{actual: 12, maximum: 11}
  end

  test "requires a list body and exact descriptor semantic identity" do
    schema_error(
      Performative.decode(<<0x00, 0x53, 0x13, 0x40>>),
      :performative_decode,
      :malformed,
      :invalid_performative_body
    )

    for wire <- [<<0x00, 0x52, 0x13, 0x45>>, <<0x00, 0xA1, 14, "amqp:flow:list", 0x45>>] do
      schema_error(Performative.decode(wire), :value_decode, :unsupported, :descriptor_type)
    end
  end

  test "properties require symbol keys while preserving entry order and nested Booleans" do
    for key <- [AMQPValue.null(), AMQPValue.string("k"), AMQPValue.uint(0)] do
      properties = AMQPValue.map([{key, AMQPValue.boolean(true)}])

      error =
        schema_error(
          Performative.decode(encode_fields!(List.replace_at(full_fields(), 10, properties))),
          :performative_decode,
          :malformed,
          :invalid_property_key
        )

      assert error.details == %{field: :properties, index: 10}

      schema_error(
        Performative.encode(%{minimal_flow() | properties: properties}),
        :performative_encode,
        :invalid_value,
        :invalid_property_key
      )
    end

    properties =
      AMQPValue.map([
        {AMQPValue.symbol("z"), AMQPValue.boolean(true)},
        {AMQPValue.symbol("k"),
         AMQPValue.list([
           AMQPValue.boolean(false),
           AMQPValue.map([{AMQPValue.null(), AMQPValue.boolean(true)}])
         ])}
      ])

    flow = %{minimal_flow() | properties: properties}
    assert {:ok, wire} = Performative.encode(flow)
    assert {:ok, ^flow, <<>>} = Performative.decode(wire)
  end

  test "duplicate property keys retain shared map rejection on decode and encode" do
    duplicate_properties =
      AMQPValue.map([
        {AMQPValue.symbol("k"), AMQPValue.boolean(true)},
        {AMQPValue.symbol("k"), AMQPValue.boolean(false)}
      ])

    wire =
      <<0x00, 0x53, 0x13, 0xC0, 25, 11, 0x40, 0x52, 100, 0x52, 1, 0x52, 200, 0x40, 0x40, 0x40,
        0x40, 0x40, 0x40, 0xC1, 9, 4, 0xA3, 1, "k", 0x41, 0xA3, 1, "k", 0x42>>

    schema_error(Performative.decode(wire), :value_decode, :malformed, :duplicate_map_key)

    schema_error(
      Performative.encode(%{minimal_flow() | properties: duplicate_properties}),
      :value_encode,
      :invalid_value,
      :duplicate_map_key
    )
  end

  test "uint fields accept zero and their full range without session or link context" do
    for value <- [0, 4_294_967_295] do
      fields = List.duplicate(AMQPValue.uint(value), 8)
      assert {:ok, flow, <<>>} = Performative.decode(encode_fields!(fields))
      assert flow.handle == AMQPValue.uint(value)
      assert flow.delivery_count == AMQPValue.uint(value)
      assert {:ok, wire} = Performative.encode(flow)
      assert {:ok, ^flow, <<>>} = Performative.decode(wire)
    end
  end

  test "propagates configured byte, count, and depth limits from the shared value layer" do
    for limits <- [
          %{Limits.default() | max_value_bytes: 3},
          %{Limits.default() | max_compound_items: 3},
          %{Limits.default() | max_nesting_depth: 1}
        ] do
      assert {:error, %Error{operation: :value_decode, class: :limit_exceeded}} =
               Performative.decode(@full, limits)

      assert {:error, %Error{operation: :value_encode, class: :limit_exceeded}} =
               Performative.encode(full_flow(), limits)
    end
  end

  test "propagates invalid scalar payloads and unsupported nested types without success" do
    for {field, value} <- [
          incoming_window: %AMQPValue{type: :uint, value: -1},
          handle: %AMQPValue{type: :uint, value: 4_294_967_296},
          drain: %AMQPValue{type: :boolean, value: :invalid}
        ] do
      assert {:error, %Error{operation: :value_encode, class: :invalid_value}} =
               Performative.encode(Map.put(minimal_flow(), field, value))
    end

    properties =
      AMQPValue.map([{AMQPValue.symbol("k"), %AMQPValue{type: :extension, value: nil}}])

    assert {:error, %Error{operation: :value_encode, class: :unsupported}} =
             Performative.encode(%{minimal_flow() | properties: properties})

    bad_boolean =
      <<0x00, 0x53, 0x13, 0xC0, 14, 9, 0x40, 0x52, 100, 0x52, 1, 0x52, 200, 0x40, 0x40, 0x40,
        0x40, 0x56, 2>>

    assert {:error, %Error{operation: :value_decode, class: :malformed, reason: :invalid_boolean}} =
             Performative.decode(bad_boolean)
  end

  defp minimum_fields do
    [AMQPValue.null(), AMQPValue.uint(100), AMQPValue.uint(1), AMQPValue.uint(200)]
  end

  defp full_fields do
    [
      AMQPValue.uint(0),
      AMQPValue.uint(100),
      AMQPValue.uint(1),
      AMQPValue.uint(200),
      AMQPValue.uint(7),
      AMQPValue.uint(4_294_967_295),
      AMQPValue.uint(8),
      AMQPValue.uint(9),
      AMQPValue.boolean(true),
      AMQPValue.boolean(false),
      AMQPValue.map([{AMQPValue.symbol("k"), AMQPValue.boolean(true)}])
    ]
  end

  defp minimal_flow do
    assert Code.ensure_loaded?(Flow)

    struct(Flow,
      incoming_window: AMQPValue.uint(100),
      next_outgoing_id: AMQPValue.uint(1),
      outgoing_window: AMQPValue.uint(200),
      drain: AMQPValue.boolean(false),
      echo: AMQPValue.boolean(false)
    )
  end

  defp full_flow do
    struct(Flow,
      next_incoming_id: AMQPValue.uint(0),
      incoming_window: AMQPValue.uint(100),
      next_outgoing_id: AMQPValue.uint(1),
      outgoing_window: AMQPValue.uint(200),
      handle: AMQPValue.uint(7),
      delivery_count: AMQPValue.uint(4_294_967_295),
      link_credit: AMQPValue.uint(8),
      available: AMQPValue.uint(9),
      drain: AMQPValue.boolean(true),
      echo: AMQPValue.boolean(false),
      properties: AMQPValue.map([{AMQPValue.symbol("k"), AMQPValue.boolean(true)}])
    )
  end

  defp encode_fields!(fields) do
    {:ok, wire} = Value.encode(AMQPValue.described(AMQPValue.ulong(0x13), AMQPValue.list(fields)))
    wire
  end

  defp schema_error(result, operation, class, reason) do
    assert {:error, %Error{operation: ^operation, class: ^class, reason: ^reason} = error} =
             result

    error
  end
end
