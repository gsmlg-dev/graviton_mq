defmodule GravitonMQ.AMQP10.Codec.AttachTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.AMQP10.Codec.{Attach, Error, Limits, Value}
  alias GravitonMQ.AMQP10.Performative.Attach, as: AttachData
  alias GravitonMQ.AMQP10.Terminus.{Source, Target}
  alias GravitonMQ.AMQP10.Value, as: V

  @receiver <<0, 0x53, 0x12, 0xC0, 6, 3, 0xA1, 1, "L", 0x43, 0x41>>
  @sender <<0, 0x53, 0x12, 0xC0, 13, 10, 0xA1, 1, "L", 0x43, 0x42, 0x40, 0x40, 0x40, 0x40, 0x40,
            0x40, 0x43>>

  setup do
    assert Code.ensure_loaded?(Attach)
    :ok
  end

  test "literal receiver and sender Attach vectors materialize exact defaults" do
    for {wire, role} <- [{@receiver, true}, {@sender, false}] do
      assert {:ok, attach, <<>>} = Attach.decode(wire)
      assert attach.__struct__ == AttachData
      assert attach.name == V.string("L")
      assert attach.handle == V.uint(0)
      assert attach.role == V.boolean(role)
      assert attach.snd_settle_mode == V.ubyte(2)
      assert attach.rcv_settle_mode == V.ubyte(0)
      assert attach.incomplete_unsettled == V.boolean(false)
      assert {:ok, ^wire} = Attach.encode(attach)
    end
  end

  test "symbolic descriptor normalizes and exact suffix/prefix contracts hold" do
    symbolic = <<0, 0xA3, 16, "amqp:attach:list", 0xC0, 6, 3, 0xA1, 1, "L", 0x43, 0x41>>

    for wire <- [@receiver, @sender, symbolic] do
      for size <- 0..(byte_size(wire) - 1) do
        assert {:more, needed} = Attach.decode(binary_part(wire, 0, size))
        assert needed > 0
      end

      assert {:ok, attach, <<0xDE, 0xAD>>} = Attach.decode(wire <> <<0xDE, 0xAD>>)
      assert {:ok, canonical} = Attach.encode(attach)
      assert canonical == if(attach.role.value, do: @receiver, else: @sender)
    end
  end

  test "validates all mandatory fields and sender conditional initial delivery count" do
    for {field, index} <- [name: 0, handle: 1, role: 2] do
      for fields <- [
            Enum.take(minimal_fields(), index),
            List.replace_at(minimal_fields(), index, V.null())
          ] do
        assert_error(Attach.decode(wire(fields)), :mandatory_field_missing, field)
      end

      for value <- [nil, V.null()] do
        assert_error(
          Attach.encode(Map.put(receiver(), field, value)),
          :mandatory_field_missing,
          field
        )
      end
    end

    assert_error(
      Attach.encode(%{receiver() | role: V.boolean(false)}),
      :mandatory_field_missing,
      :initial_delivery_count
    )

    assert_error(
      Attach.decode(wire([V.string("L"), V.uint(0), V.boolean(false)])),
      :mandatory_field_missing,
      :initial_delivery_count
    )
  end

  test "retains all ordered fields, extension data and nested Source/Target structs" do
    state = V.described(V.symbol("example:state"), V.list([V.boolean(true)]))
    unsettled = V.map([{V.binary(<<0, 255>>), state}, {V.binary("other"), V.null()}])

    attach =
      struct(AttachData,
        name: V.string("link"),
        handle: V.uint(8),
        role: V.boolean(false),
        snd_settle_mode: V.ubyte(1),
        rcv_settle_mode: V.ubyte(1),
        source: struct(Source, address: V.string("source")),
        target: struct(Target, address: V.string("target")),
        unsettled: unsettled,
        incomplete_unsettled: V.boolean(true),
        initial_delivery_count: V.uint(4_294_967_295),
        max_message_size: V.ulong(18_446_744_073_709_551_615),
        offered_capabilities: V.array(:symbol, [V.symbol("a"), V.symbol("b")]),
        desired_capabilities: V.symbol("cap"),
        properties: V.map([{V.symbol("x"), V.map([{V.null(), V.boolean(true)}])}])
      )

    assert {:ok, tagged} = Attach.encode_value(attach, :performative_encode, :invalid_value)
    assert {:ok, decoded} = Attach.decode_value(tagged, :performative_decode, :malformed)
    assert decoded.unsettled == unsettled
    assert decoded.source.address == V.string("source")
    assert decoded.target.address == V.string("target")
    assert decoded.initial_delivery_count == attach.initial_delivery_count
    assert decoded.max_message_size == attach.max_message_size
    assert decoded.offered_capabilities == attach.offered_capabilities
    assert decoded.properties == attach.properties
    assert {:ok, encoded} = Attach.encode(attach)
    assert {:ok, ^decoded, <<>>} = Attach.decode(encoded)
  end

  test "independent full wire vector covers nested termini, unsettled state and symbol array" do
    full =
      <<0, 0x53, 0x12, 0xC0, 61, 14, 0xA1, 1, "L", 0x43, 0x41, 0x50, 0, 0x50, 1, 0, 0x53, 0x28,
        0xC0, 4, 1, 0xA1, 1, "q", 0, 0x53, 0x29, 0xC0, 4, 1, 0xA1, 1, "q", 0xC1, 8, 2, 0xA0, 1,
        "t", 0, 0x53, 0x24, 0x45, 0x41, 0x52, 5, 0x53, 42, 0xA3, 1, "x", 0xE0, 6, 2, 0xA3, 1, "y",
        1, "z", 0xC1, 5, 2, 0xA3, 1, "k", 0x42>>

    assert {:ok, attach, <<>>} = Attach.decode(full)
    assert attach.source.address == V.string("q")
    assert attach.target.address == V.string("q")
    assert attach.snd_settle_mode == V.ubyte(0)
    assert attach.rcv_settle_mode == V.ubyte(1)
    assert attach.unsettled == V.map([{V.binary("t"), V.described(V.ulong(0x24), V.list([]))}])
    assert attach.incomplete_unsettled == V.boolean(true)
    assert attach.initial_delivery_count == V.uint(5)
    assert attach.max_message_size == V.ulong(42)
    assert attach.offered_capabilities == V.symbol("x")
    assert attach.desired_capabilities == V.array(:symbol, [V.symbol("y"), V.symbol("z")])
    assert attach.properties == V.map([{V.symbol("k"), V.boolean(false)}])
    assert {:ok, ^full} = Attach.encode(attach)
  end

  test "described terminus extensions are retained while known wrong schemas are rejected" do
    extension = V.described(V.symbol("example:source"), V.map([{V.null(), V.boolean(true)}]))
    attach = %{receiver() | source: extension, target: extension}
    assert {:ok, wire} = Attach.encode(attach)
    assert {:ok, decoded, <<>>} = Attach.decode(wire)
    assert decoded.source == extension
    assert decoded.target == extension
    known_wrong = V.described(V.ulong(0x17), V.list([]))
    assert_error(Attach.encode(%{receiver() | source: known_wrong}), :wrong_terminus)
    assert_error(Attach.encode(%{receiver() | target: known_wrong}), :wrong_terminus)
  end

  test "canonical trailing defaults and explicit null fields retain positional holes" do
    fields = minimal_fields() ++ List.duplicate(V.null(), 11)
    assert {:ok, attach, <<>>} = Attach.decode(wire(fields))
    assert {:ok, @receiver} = Attach.encode(attach)
    attach = %{receiver() | max_message_size: V.ulong(0)}

    expected =
      <<0, 0x53, 0x12, 0xC0, 14, 11, 0xA1, 1, "L", 0x43, 0x41, 0x40, 0x40, 0x40, 0x40, 0x40, 0x40,
        0x40, 0x44>>

    assert {:ok, ^expected} = Attach.encode(attach)
  end

  test "every exact field type is validated on decode and encode" do
    names = [
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

    for {field, index} <- Enum.with_index(names) do
      fields =
        (minimal_fields() ++ List.duplicate(V.null(), 11))
        |> List.replace_at(index, V.int(1))

      assert_error(Attach.decode(wire(fields)), :field_type_mismatch, field)

      assert_error(
        Attach.encode(Map.put(receiver(), field, V.int(1))),
        :field_type_mismatch,
        field
      )
    end
  end

  test "settlement modes have exact ubyte choice ranges" do
    for value <- 0..2 do
      assert {:ok, _} = Attach.encode(%{receiver() | snd_settle_mode: V.ubyte(value)})
    end

    for value <- 0..1 do
      assert {:ok, _} = Attach.encode(%{receiver() | rcv_settle_mode: V.ubyte(value)})
    end

    for {field, invalid} <- [snd_settle_mode: V.ubyte(3), rcv_settle_mode: V.ubyte(2)] do
      assert_error(
        Attach.encode(Map.put(receiver(), field, invalid)),
        :field_value_out_of_range,
        field
      )
    end
  end

  test "unsettled map requires binary tags of at most 32 bytes and delivery states or null" do
    for key <- [V.null(), V.string("tag"), V.binary(:binary.copy("x", 33))] do
      assert_error(
        Attach.encode(%{receiver() | unsettled: V.map([{key, V.null()}])}),
        :invalid_unsettled_key,
        :unsettled
      )
    end

    assert {:ok, _} = Attach.encode(%{receiver() | unsettled: V.map([{V.binary(""), V.null()}])})

    assert_error(
      Attach.encode(%{receiver() | unsettled: V.map([{V.binary("t"), V.uint(1)}])}),
      :invalid_delivery_state
    )

    assert {:ok, _} =
             Attach.encode(%{
               receiver()
               | unsettled:
                   V.map([
                     {V.binary(:binary.copy("x", 32)), V.described(V.ulong(0x24), V.list([]))}
                   ])
             })
  end

  test "property keys are symbols and capabilities remain scalar or symbol arrays" do
    for key <- [V.null(), V.string("p")] do
      assert_error(
        Attach.encode(%{receiver() | properties: V.map([{key, V.boolean(true)}])}),
        :invalid_property_key,
        :properties
      )
    end

    for field <- [:offered_capabilities, :desired_capabilities] do
      for value <- [V.symbol(""), V.array(:symbol, []), V.array(:symbol, [V.symbol("x")])] do
        assert {:ok, _} = Attach.encode(Map.put(receiver(), field, value))
      end
    end
  end

  test "dynamic source/target addresses enforce role-dependent requirements" do
    for {field, module, requester_role} <- [{:source, Source, true}, {:target, Target, false}] do
      for role <- [true, false] do
        attach = %{receiver() | role: V.boolean(role), initial_delivery_count: V.uint(0)}
        terminus = struct(module, dynamic: V.boolean(true))
        with_address = Map.put(terminus, :address, V.string("dynamic"))

        if role == requester_role do
          assert {:ok, _} = Attach.encode(Map.put(attach, field, terminus))

          assert_error(
            Attach.encode(Map.put(attach, field, with_address)),
            :dynamic_address_forbidden
          )
        else
          assert_error(Attach.encode(Map.put(attach, field, terminus)), :dynamic_address_required)
          assert {:ok, _} = Attach.encode(Map.put(attach, field, with_address))
        end
      end
    end
  end

  test "rejects Coordinator transactions, wrong terminus roles and excess fields" do
    coordinator = V.described(V.ulong(0x30), V.list([]))

    assert_error(
      Attach.decode(wire(minimal_fields() ++ [V.null(), V.null(), V.null(), coordinator])),
      :transactions_unsupported
    )

    assert_error(Attach.encode(%{receiver() | target: coordinator}), :transactions_unsupported)
    assert_error(Attach.encode(%{receiver() | source: struct(Target)}), :wrong_terminus)
    assert_error(Attach.encode(%{receiver() | target: struct(Source)}), :wrong_terminus)

    assert_error(
      Attach.decode(wire(minimal_fields() ++ List.duplicate(V.null(), 12))),
      :too_many_fields
    )

    assert_error(Attach.decode(<<0, 0x53, 0x12, 0x40>>), :invalid_attach_body)
    assert_error(Attach.encode_value(%{}, :performative_encode, :invalid_value), :invalid_attach)
  end

  @tag :canonical_nested
  test "decoding retains symbolic unsettled states and encoding uses numeric descriptors" do
    symbolic = V.described(V.symbol("amqp:received:list"), V.list([V.uint(0), V.ulong(0)]))

    fields =
      minimal_fields() ++
        List.duplicate(V.null(), 4) ++
        [
          V.map([{V.binary("t"), symbolic}])
        ]

    assert {:ok, attach, <<>>} = Attach.decode(wire(fields))
    assert attach.unsettled == V.map([{V.binary("t"), symbolic}])
    assert {:ok, encoded} = Attach.encode(attach)
    assert {:ok, decoded, <<>>} = Attach.decode(encoded)

    assert decoded.unsettled ==
             V.map([{V.binary("t"), V.described(V.ulong(0x23), V.list([V.uint(0), V.ulong(0)]))}])
  end

  test "shared value limits propagate and helper errors retain caller operation/class" do
    assert_error(
      Attach.decode_value(V.string("x"), :performative_decode, :malformed),
      :invalid_attach
    )

    assert {:error, %Error{operation: :performative_encode, class: :invalid_value}} =
             Attach.encode_value(struct(AttachData), :performative_encode, :invalid_value)

    for limits <- [
          %{Limits.default() | max_value_bytes: 3},
          %{Limits.default() | max_compound_items: 2},
          %{Limits.default() | max_nesting_depth: 1}
        ] do
      assert {:error, %Error{class: :limit_exceeded}} = Attach.encode(receiver(), limits)
      assert {:error, %Error{class: :limit_exceeded}} = Attach.decode(@receiver, limits)
    end
  end

  defp minimal_fields, do: [V.string("L"), V.uint(0), V.boolean(true)]

  defp receiver,
    do: struct(AttachData, name: V.string("L"), handle: V.uint(0), role: V.boolean(true))

  defp wire(fields) do
    {:ok, wire} = Value.encode(V.described(V.ulong(0x12), V.list(fields)))
    wire
  end

  defp assert_error(result, reason, field \\ nil) do
    assert {:error, %Error{reason: ^reason} = error} = result
    if field, do: assert(error.details.field == field)
    error
  end
end
