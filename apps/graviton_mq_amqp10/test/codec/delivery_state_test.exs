defmodule GravitonMQ.AMQP10.Codec.DeliveryStateTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.AMQP10.Codec.DeliveryState, as: Codec
  alias GravitonMQ.AMQP10.Codec.Error
  alias GravitonMQ.AMQP10.Codec.Limits
  alias GravitonMQ.AMQP10.Codec.Value, as: WireValue
  alias GravitonMQ.AMQP10.DeliveryState.Received
  alias GravitonMQ.AMQP10.Error, as: ProtocolError
  alias GravitonMQ.AMQP10.Outcome
  alias GravitonMQ.AMQP10.OutcomeMapper
  alias GravitonMQ.AMQP10.Value, as: V

  test "Received requires exact uint and ulong and canonically encodes numeric descriptor" do
    assert Code.ensure_loaded?(Received)
    state = struct!(Received, section_number: V.uint(0), section_offset: V.ulong(7))
    fixture = <<0, 0x53, 0x23, 0xC0, 4, 2, 0x43, 0x53, 7>>
    assert {:ok, ^state, <<>>} = Codec.decode(fixture)
    assert {:ok, ^fixture} = Codec.encode(state)
    assert {:ok, ^state, <<>>} = Codec.decode(symbolic("received", <<0x43, 0x53, 7>>, 2))
  end

  test "Accepted and Released are empty composites accepting numeric and symbolic descriptors" do
    for {code, name, state} <- [
          {0x24, "accepted", %Outcome.Accepted{}},
          {0x26, "released", %Outcome.Released{}}
        ] do
      wire = <<0, 0x53, code, 0x45>>
      assert {:ok, ^state, <<>>} = Codec.decode(wire)
      assert {:ok, ^state, <<>>} = Codec.decode(symbolic(name, <<>>, 0))
      assert {:ok, ^wire} = Codec.encode(state)
      assert {:error, %Error{class: :malformed}} = Codec.decode(composite(code, <<0x40>>, 1))
    end
  end

  test "Rejected preserves symbolic noncanonical nested Error bytes and maps them losslessly" do
    error_wire = symbolic_error(<<0xA3, 1, "x", 0x40, 0xC1, 6, 2, 0xA3, 1, "k", 0x56, 1>>, 3)
    wire = composite(0x25, error_wire, 1)
    assert {:ok, %Outcome.Rejected{} = state, <<>>} = Codec.decode(wire)

    assert state.error == %ProtocolError{
             condition: V.symbol("x"),
             info: V.map([{V.symbol("k"), V.boolean(true)}])
           }

    assert state.encoded_error == error_wire
    assert OutcomeMapper.to_core(state).details.encoded_data == error_wire
    assert {:ok, tagged} = Codec.encode_value(state, :performative_encode, :invalid_value)
    assert {:ok, without_wire} = Codec.decode_value(tagged, :performative_decode, :malformed)
    assert without_wire.encoded_error == nil
    assert {:ok, original_tagged, <<>>} = WireValue.decode(wire)

    assert {:ok, ^state} =
             Codec.decode_value(original_tagged, :performative_decode, :malformed, wire)
  end

  test "Modified defaults false and retains exact annotations bytes with positional holes" do
    annotations = <<0xD1, 0, 0, 0, 9, 0, 0, 0, 2, 0xA3, 1, "k", 0x56, 1>>
    wire = composite(0x27, <<0x40, 0x40, annotations::binary>>, 3)
    assert {:ok, %Outcome.Modified{} = state, <<>>} = Codec.decode(wire)
    assert state.delivery_failed? == false
    assert state.undeliverable_here? == false
    assert state.message_annotations == V.map([{V.symbol("k"), V.boolean(true)}])
    assert state.encoded_message_annotations == annotations
    assert OutcomeMapper.to_core(state).annotation_mutation.encoded_data == annotations
    assert {:ok, <<0, 0x53, 0x27, 0x45>>} = Codec.encode(%Outcome.Modified{})
    assert {:ok, %Outcome.Modified{}, <<>>} = Codec.decode(<<0, 0x53, 0x27, 0x45>>)
    assert {:ok, encoded} = Codec.encode(%Outcome.Modified{undeliverable_here?: true})
    assert encoded == composite(0x27, <<0x40, 0x41>>, 2)
  end

  test "strict prefixes stay incomplete and appended bytes remain opaque" do
    for fixture <- [
          <<0, 0x53, 0x24, 0x45>>,
          composite(0x23, <<0x43, 0x44>>, 2),
          composite(0x25, symbolic_error(<<0xA3, 1, "x">>, 1), 1)
        ] do
      for size <- 0..(byte_size(fixture) - 1) do
        assert {:more, needed} = Codec.decode(binary_part(fixture, 0, size))
        assert needed > 0
      end

      assert {:ok, _, <<0xFF, 0x00>>} = Codec.decode(fixture <> <<0xFF, 0x00>>)
    end
  end

  test "rejects missing mandatory values, wrong exact types, extra fields and nonlist bodies" do
    invalid = [
      composite(0x23, <<>>, 0),
      composite(0x23, <<0x43, 0x40>>, 2),
      composite(0x23, <<0x44, 0x44>>, 2),
      composite(0x23, <<0x43, 0x43>>, 2),
      composite(0x23, <<0x43, 0x44, 0x40>>, 3),
      <<0, 0x53, 0x24, 0x40>>,
      composite(0x27, <<0x43>>, 1),
      composite(0x27, <<0x41, 0x42, 0x40, 0x40>>, 4)
    ]

    for wire <- invalid, do: assert({:error, %Error{class: :malformed}} = Codec.decode(wire))
  end

  test "nested Error keeps extension conditions but validates its exact fields and symbol keys" do
    invalid_errors = [
      <<0x40>>,
      composite(0x24, <<>>, 0),
      composite(0x1D, <<>>, 0),
      composite(0x1D, <<0xA1, 1, "x">>, 1),
      composite(0x1D, <<0xA3, 1, "x", 0xA3, 1, "d">>, 2),
      composite(0x1D, <<0xA3, 1, "x", 0x40, 0xC1, 4, 2, 0x44, 0x41>>, 3)
    ]

    for error <- tl(invalid_errors) do
      assert {:error, %Error{class: :malformed}} = Codec.decode(composite(0x25, error, 1))
    end

    assert {:ok, %Outcome.Rejected{error: nil}, <<>>} = Codec.decode(composite(0x25, <<0x40>>, 1))
  end

  test "schema helpers retain operation and requested error class" do
    tagged = V.described(V.ulong(0x23), V.list([V.ulong(0), V.ulong(0)]))

    assert {:error, %Error{operation: :performative_decode, class: :malformed}} =
             Codec.decode_value(tagged, :performative_decode, :malformed)

    assert {:error, %Error{operation: :performative_encode, class: :invalid_value}} =
             Codec.encode_value(
               %Outcome.Modified{delivery_failed?: V.boolean(true)},
               :performative_encode,
               :invalid_value
             )

    assert {:ok, %Outcome.Modified{encoded_message_annotations: nil}} =
             Codec.decode_value(
               V.described(V.ulong(0x27), V.list([])),
               :performative_decode,
               :malformed,
               nil
             )
  end

  test "unknown and transactional descriptors are explicitly unsupported" do
    for descriptor <- [V.ulong(0x33), V.ulong(0x34), V.ulong(0x99), V.symbol("vendor:state:list")] do
      {:ok, wire} = WireValue.encode(V.described(descriptor, V.list([])))
      assert {:error, %Error{class: :unsupported}} = Codec.decode(wire)
    end

    assert {:error, %Error{class: :malformed}} = Codec.decode(<<0x45>>)
    assert {:error, %Error{class: :invalid_value}} = Codec.encode(:accepted)
  end

  test "all standalone paths enforce value byte, depth and compound budgets" do
    state = %Outcome.Modified{
      message_annotations: V.map([{V.symbol("k"), V.list([V.boolean(true)])}])
    }

    assert {:ok, wire} = Codec.encode(state)

    for limits <- [
          %Limits{max_value_bytes: 3},
          %Limits{max_nesting_depth: 1},
          %Limits{max_compound_items: 1}
        ] do
      assert {:error, %Error{class: :limit_exceeded}} = Codec.encode(state, limits)
      assert {:error, %Error{class: :limit_exceeded}} = Codec.decode(wire, limits)
    end

    assert {:error, %Error{class: :invalid_value, reason: :invalid_limits}} =
             Codec.encode(state, %Limits{max_value_bytes: 0})
  end

  test "Modified fields reject ulong keys while generic maps retain them" do
    annotations = V.map([{V.ulong(1), V.boolean(true)}])
    tagged = V.described(V.ulong(0x27), V.list([V.null(), V.null(), annotations]))
    assert {:ok, wire} = WireValue.encode(tagged)
    assert {:error, %Error{class: :malformed}} = Codec.decode(wire)

    assert {:error, %Error{class: :invalid_value}} =
             Codec.encode(%Outcome.Modified{message_annotations: annotations})

    assert {:ok, _} = WireValue.encode(annotations)
  end

  test "invalid manually constructed states return errors rather than raising" do
    assert Code.ensure_loaded?(Received)

    invalid = [
      struct!(Received, section_number: :zero, section_offset: V.ulong(0)),
      struct!(Received, section_number: V.uint(0), section_offset: %V{type: :ulong, value: -1}),
      %Outcome.Modified{message_annotations: :map},
      %Outcome.Rejected{error: %ProtocolError{condition: :condition}},
      %Outcome.Rejected{error: %ProtocolError{condition: V.symbol("é")}}
    ]

    for state <- invalid do
      assert {:error, %Error{class: :invalid_value}} =
               Codec.encode_value(state, :performative_encode, :invalid_value)

      assert {:error, %Error{class: :invalid_value}} = Codec.encode(state)
    end
  end

  test "all state descriptors accept symbolic form and canonicalize to numeric form" do
    vectors = [
      {0x23, "received", <<0x43, 0x44>>, 2},
      {0x24, "accepted", <<>>, 0},
      {0x25, "rejected", <<>>, 0},
      {0x26, "released", <<>>, 0},
      {0x27, "modified", <<0x56, 1, 0x56, 0>>, 2}
    ]

    for {code, name, fields, count} <- vectors do
      assert {:ok, state, <<>>} = Codec.decode(symbolic(name, fields, count))
      assert {:ok, <<0, 0x53, ^code, _::binary>> = wire} = Codec.encode(state)
      assert {:ok, ^state, <<>>} = Codec.decode(wire)
    end
  end

  test "Received retains full width unsigned boundaries without integer-tag coercion" do
    state =
      struct!(Received,
        section_number: V.uint(0xFFFF_FFFF),
        section_offset: V.ulong(0xFFFF_FFFF_FFFF_FFFF)
      )

    wire = composite(0x23, <<0x70, 0xFFFF_FFFF::32, 0x80, 0xFFFF_FFFF_FFFF_FFFF::64>>, 2)
    assert {:ok, ^state, <<>>} = Codec.decode(wire)
    assert {:ok, ^wire} = Codec.encode(state)
  end

  test "list32 outer fields retain nested constructor bytes without remainder capture" do
    annotations = <<0xC1, 5, 2, 0xA3, 1, "k", 0x41>>
    fields = <<0x42, 0x41, annotations::binary>>
    wire = <<0, 0x80, 0x27::64, 0xD0, byte_size(fields) + 4::32, 3::32, fields::binary>>
    assert {:ok, %Outcome.Modified{} = state, <<1, 2>>} = Codec.decode(wire <> <<1, 2>>)
    assert state.encoded_message_annotations == annotations
    assert state.delivery_failed? == false
    assert state.undeliverable_here? == true
  end

  test "nested Error canonical encoding preserves interior description null" do
    error = %ProtocolError{
      condition: V.symbol("vendor:condition"),
      info: V.map([{V.symbol("k"), V.boolean(false)}])
    }

    state = %Outcome.Rejected{error: error}
    assert {:ok, wire} = Codec.encode(state)
    assert {:ok, %Outcome.Rejected{error: ^error, encoded_error: raw}, <<>>} = Codec.decode(wire)
    assert {:ok, tagged, <<>>} = WireValue.decode(raw)
    assert tagged == V.described(V.ulong(0x1D), V.list([error.condition, V.null(), error.info]))
    assert {:ok, ^wire} = Codec.encode(%{state | encoded_error: <<0xFF>>})
  end

  test "caller limits survive standalone and five-argument schema provenance paths" do
    nested = Enum.reduce(1..35, V.boolean(true), fn _, value -> V.list([value]) end)
    annotations = V.map([{V.symbol("k"), nested}])
    state = %Outcome.Modified{message_annotations: annotations}
    limits = %Limits{max_nesting_depth: 50}
    assert {:ok, tagged} = Codec.encode_value(state, :performative_encode, :invalid_value)
    assert {:ok, wire} = Codec.encode(state, limits)
    assert {:ok, decoded, <<>>} = Codec.decode(wire, limits)
    assert decoded.message_annotations == annotations

    assert {:ok, ^decoded} =
             Codec.decode_value(tagged, :performative_decode, :malformed, wire, limits)

    assert {:error, %Error{class: :limit_exceeded}} =
             Codec.decode_value(tagged, :performative_decode, :malformed, wire)
  end

  test "annotation maps retain value identity and reject null, non-symbol and duplicate keys" do
    for entries <- [
          [{V.null(), V.uint(1)}],
          [{V.string("k"), V.uint(1)}],
          [{V.symbol("k"), V.uint(1)}, {V.symbol("k"), V.ulong(1)}]
        ] do
      assert {:error, %Error{class: :invalid_value}} =
               Codec.encode(%Outcome.Modified{message_annotations: V.map(entries)})
    end

    annotations =
      V.map([
        {V.symbol("a"), V.binary("x")},
        {V.symbol("b"), V.string("x")},
        {V.symbol("c"), V.symbol("x")}
      ])

    assert {:ok, wire} = Codec.encode(%Outcome.Modified{message_annotations: annotations})
    assert {:ok, %Outcome.Modified{message_annotations: ^annotations}, <<>>} = Codec.decode(wire)
  end

  test "raw helper rejects mismatched provenance rather than attaching unrelated bytes" do
    tagged = V.described(V.ulong(0x25), V.list([]))

    assert {:error, %Error{class: :malformed}} =
             Codec.decode_value(tagged, :performative_decode, :malformed, <<0, 0x53, 0x24, 0x45>>)
  end

  defp composite(code, fields, count),
    do: <<0, 0x53, code, 0xC0, byte_size(fields) + 1, count, fields::binary>>

  defp symbolic(name, fields, count) do
    descriptor = "amqp:" <> name <> ":list"

    <<0, 0xA3, byte_size(descriptor), descriptor::binary, 0xC0, byte_size(fields) + 1, count,
      fields::binary>>
  end

  defp symbolic_error(fields, count), do: symbolic("error", fields, count)
end
