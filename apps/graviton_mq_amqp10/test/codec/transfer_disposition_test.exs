defmodule GravitonMQ.AMQP10.Codec.TransferDispositionTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.AMQP10.Codec.Error
  alias GravitonMQ.AMQP10.Codec.Limits
  alias GravitonMQ.AMQP10.Codec.Performative
  alias GravitonMQ.AMQP10.Codec.Value, as: WireValue
  alias GravitonMQ.AMQP10.Outcome
  alias GravitonMQ.AMQP10.Performative.Disposition
  alias GravitonMQ.AMQP10.Performative.Transfer
  alias GravitonMQ.AMQP10.Value

  # OASIS AMQP 1.0 Transport §§2.7.5–6, 2.8.6, 2.8.14. These literal vectors
  # are independent of both production encoders. Payload is not a list field.
  @transfer <<0, 0x53, 0x14, 0xC0, 2, 1, 0x43>>
  @first <<0, 0x53, 0x14, 0xC0, 9, 4, 0x52, 7, 0x52, 42, 0xA0, 1, "t", 0x43>>
  @disposition <<0, 0x53, 0x15, 0xC0, 3, 2, 0x41, 0x43>>
  @accepted <<0, 0x53, 0x15, 0xC0, 10, 5, 0x41, 0x52, 42, 0x40, 0x41, 0, 0x53, 0x24, 0x45>>

  test "performative facade integrates both Attach descriptor forms" do
    numeric = <<0, 0x53, 0x12, 0xC0, 6, 3, 0xA1, 1, "l", 0x43, 0x41>>
    symbolic = <<0, 0xA3, 16, "amqp:attach:list", 0xC0, 6, 3, 0xA1, 1, "l", 0x43, 0x41>>

    for wire <- [numeric, symbolic] do
      assert {:ok, %GravitonMQ.AMQP10.Performative.Attach{} = attach, <<0xFF>>} =
               Performative.decode(wire <> <<0xFF>>)

      assert attach.role == Value.boolean(true)
      assert attach.name == Value.string("l")
      assert {:ok, ^numeric} = Performative.encode(attach)
    end
  end

  test "independent minimal numeric and symbolic Transfer descriptors" do
    symbolic = <<0, 0xA3, 18, "amqp:transfer:list", 0xC0, 2, 1, 0x43>>

    for wire <- [@transfer, symbolic] do
      assert {:ok, decoded, <<>>} = Performative.decode(wire)
      assert decoded == transfer()
      assert {:ok, @transfer} = Performative.encode(decoded)
    end
  end

  test "first Transfer values and original payload suffix remain separate" do
    payload = <<0, 0x53, 0x75, 0xA0, 2, 0xFF, 0>>
    assert {:ok, decoded, ^payload} = Performative.decode(@first <> payload)
    assert decoded.handle == Value.uint(7)
    assert decoded.delivery_id == Value.uint(42)
    assert decoded.delivery_tag == Value.binary("t")
    assert decoded.message_format == Value.uint(0)
    assert {:ok, @first} = Performative.encode(decoded)
  end

  test "Transfer settled omission differs from explicit false on continuations" do
    assert {:ok, %{settled: nil}, <<>>} = Performative.decode(@transfer)

    for flag <- [false, true] do
      candidate = %{transfer() | settled: Value.boolean(flag)}
      assert {:ok, encoded} = Performative.encode(candidate)
      assert {:ok, ^candidate, <<>>} = Performative.decode(encoded)
      assert {:ok, fields, <<>>} = WireValue.decode(encoded)
      assert length(fields.value.value.value) == 5
    end
  end

  test "the four Transfer false defaults canonicalize while interior holes remain" do
    for name <- [:more, :resume, :aborted, :batchable],
        default <- [nil, Value.null(), Value.boolean(false)] do
      assert {:ok, @transfer} = Performative.encode(Map.put(transfer(), name, default))
    end

    full = %{
      transfer()
      | delivery_id: Value.uint(0xFFFF_FFFF),
        delivery_tag: Value.binary(<<>>),
        message_format: Value.uint(0),
        settled: Value.boolean(false),
        more: Value.boolean(true),
        rcv_settle_mode: Value.ubyte(1),
        state: %Outcome.Released{},
        resume: Value.boolean(true),
        aborted: Value.boolean(true),
        batchable: Value.boolean(true)
    }

    # more+aborted is valid wire data; endpoint gives aborted precedence.
    assert {:ok, wire} = Performative.encode(full)
    assert {:ok, ^full, <<>>} = Performative.decode(wire)

    hole = %{transfer() | batchable: Value.boolean(true)}
    assert {:ok, wire} = Performative.encode(hole)
    assert {:ok, ^hole, <<>>} = Performative.decode(wire)
    assert {:ok, value, <<>>} = WireValue.decode(wire)
    assert Enum.slice(value.value.value.value, 1, 9) == List.duplicate(Value.null(), 9)
  end

  test "Disposition numeric and symbolic role, mandatory first and default flags" do
    symbolic = <<0, 0xA3, 21, "amqp:disposition:list", 0xC0, 3, 2, 0x41, 0x43>>

    for wire <- [@disposition, symbolic] do
      assert {:ok, decoded, <<>>} = Performative.decode(wire)
      assert decoded == disposition()
      assert {:ok, @disposition} = Performative.encode(decoded)
    end

    assert {:ok, accepted, <<>>} = Performative.decode(@accepted)
    assert accepted.state == %Outcome.Accepted{}
    assert accepted.first == Value.uint(42)
    assert accepted.last == nil
    assert accepted.settled == Value.boolean(true)
    assert {:ok, @accepted} = Performative.encode(accepted)
  end

  test "Disposition keeps wraparound ranges and explicit last without numeric ordering" do
    candidate = %{
      disposition()
      | first: Value.uint(0xFFFF_FFFE),
        last: Value.uint(1),
        settled: Value.boolean(true),
        batchable: Value.boolean(true)
    }

    assert {:ok, encoded} = Performative.encode(candidate)
    assert {:ok, ^candidate, <<>>} = Performative.decode(encoded)
  end

  test "each complete independent vector preserves suffix and every strict prefix is incomplete" do
    suffix = <<0xDE, 0xAD, 0, 0xFF>>

    for wire <- [@transfer, @first, @disposition, @accepted] do
      for size <- 0..(byte_size(wire) - 1) do
        assert {:more, needed} = Performative.decode(binary_part(wire, 0, size))
        assert needed > 0
      end

      assert {:ok, expected, <<>>} = Performative.decode(wire)
      assert {:ok, ^expected, ^suffix} = Performative.decode(wire <> suffix)
    end
  end

  test "mandatory fields reject omission or null on both boundaries" do
    for {candidate, name, descriptor, fields, index} <- [
          {transfer(), :handle, 0x14, [Value.uint(0)], 0},
          {disposition(), :role, 0x15, [Value.boolean(true), Value.uint(0)], 0},
          {disposition(), :first, 0x15, [Value.boolean(true), Value.uint(0)], 1}
        ] do
      for absent <- [nil, Value.null()] do
        assert {:error, %Error{class: :invalid_value, reason: :mandatory_field_missing}} =
                 Performative.encode(Map.put(candidate, name, absent))
      end

      for missing <- [Enum.take(fields, index), List.replace_at(fields, index, Value.null())] do
        assert {:error, %Error{class: :malformed, reason: :mandatory_field_missing}} =
                 Performative.decode(wire(descriptor, missing))
      end
    end
  end

  test "every Transfer scalar field requires its exact semantic type" do
    for {name, index} <-
          Enum.with_index([
            :handle,
            :delivery_id,
            :delivery_tag,
            :message_format,
            :settled,
            :more,
            :rcv_settle_mode,
            :state,
            :resume,
            :aborted,
            :batchable
          ]) do
      fields = List.duplicate(Value.null(), 11) |> List.replace_at(0, Value.uint(0))
      wrong = Value.string("wrong")

      assert {:error, %Error{class: :invalid_value}} =
               Performative.encode(Map.put(transfer(), name, wrong))

      assert {:error, %Error{class: :malformed}} =
               Performative.decode(wire(0x14, List.replace_at(fields, index, wrong)))
    end
  end

  test "Disposition scalar fields require exact semantic types" do
    for {name, index} <- Enum.with_index([:role, :first, :last, :settled, :state, :batchable]) do
      fields = [Value.boolean(true), Value.uint(0)] ++ List.duplicate(Value.null(), 4)
      wrong = Value.symbol("wrong")

      assert {:error, %Error{class: :invalid_value}} =
               Performative.encode(Map.put(disposition(), name, wrong))

      assert {:error, %Error{class: :malformed}} =
               Performative.decode(wire(0x15, List.replace_at(fields, index, wrong)))
    end
  end

  test "delivery tags cap at 32 raw bytes and receiver mode is zero or one" do
    for bytes <- [<<>>, :binary.copy(<<0xFF>>, 32)] do
      candidate = %{transfer() | delivery_tag: Value.binary(bytes)}
      assert {:ok, encoded} = Performative.encode(candidate)
      assert {:ok, ^candidate, <<>>} = Performative.decode(encoded)
    end

    assert {:error, %Error{reason: :field_value_out_of_range}} =
             Performative.encode(%{
               transfer()
               | delivery_tag: Value.binary(:binary.copy("x", 33))
             })

    assert {:error, %Error{class: :malformed, reason: :field_value_out_of_range}} =
             Performative.decode(
               wire(0x14, [Value.uint(0), Value.null(), Value.binary(:binary.copy("x", 33))])
             )

    for mode <- [0, 1] do
      candidate = %{transfer() | rcv_settle_mode: Value.ubyte(mode)}
      assert {:ok, encoded} = Performative.encode(candidate)
      assert {:ok, ^candidate, <<>>} = Performative.decode(encoded)
    end

    for mode <- [2, 255] do
      assert {:error, %Error{reason: :field_value_out_of_range}} =
               Performative.encode(%{transfer() | rcv_settle_mode: Value.ubyte(mode)})
    end
  end

  test "invalid direct scalar payloads produce errors without raising" do
    for {name, value} <- [
          handle: %Value{type: :uint, value: -1},
          delivery_id: %Value{type: :uint, value: 0x1_0000_0000},
          delivery_tag: %Value{type: :binary, value: []},
          settled: %Value{type: :boolean, value: :invalid},
          rcv_settle_mode: %Value{type: :ubyte, value: "0"}
        ] do
      assert {:error, %Error{class: :invalid_value}} =
               Performative.encode(Map.put(transfer(), name, value))
    end
  end

  test "field counts, non-list bodies and nested Error at top level are explicit failures" do
    for {descriptor, count} <- [{0x14, 12}, {0x15, 7}] do
      assert {:error, %Error{class: :malformed, reason: :too_many_fields}} =
               Performative.decode(wire(descriptor, List.duplicate(Value.null(), count)))

      assert {:ok, encoded} =
               WireValue.encode(Value.described(Value.ulong(descriptor), Value.uint(0)))

      assert {:error, %Error{class: :malformed}} = Performative.decode(encoded)
    end

    assert {:error, %Error{class: :unsupported}} = Performative.decode(<<0, 0x53, 0x24, 0x45>>)
  end

  test "compound and byte limits propagate before any endpoint side effect" do
    for wire <- [@first, @accepted] do
      assert {:error, %Error{class: :limit_exceeded}} =
               Performative.decode(wire, %{
                 Limits.default()
                 | max_value_bytes: byte_size(wire) - 1
               })
    end

    assert {:error, %Error{class: :limit_exceeded}} =
             Performative.encode(%{transfer() | delivery_tag: Value.binary("tag")}, %{
               Limits.default()
               | max_compound_items: 2
             })
  end

  test "nested Modified retains actual noncanonical annotation bytes for core mapping" do
    annotations = <<0xD1, 15::32, 2::32, 0xB3, 1::32, "k", 0x71, 7::32>>
    state_fields = <<0x56, 1, 0x40, annotations::binary>>

    state =
      <<0, 0x80, 0x27::64, 0xD0, byte_size(state_fields) + 4::32, 3::32, state_fields::binary>>

    fields = <<0x41, 0x43, 0x40, 0x40, state::binary>>
    wire = <<0, 0x53, 0x15, 0xD0, byte_size(fields) + 4::32, 5::32, fields::binary>>
    assert {:ok, decoded, <<>>} = Performative.decode(wire)

    assert %Outcome.Modified{delivery_failed?: true, encoded_message_annotations: ^annotations} =
             decoded.state

    core = GravitonMQ.AMQP10.OutcomeMapper.to_core(decoded.state)
    assert core.annotation_mutation.encoded_data == annotations
    assert {:ok, canonical} = Performative.encode(decoded)
    assert {:ok, %{state: canonical_state}, <<>>} = Performative.decode(canonical)
    assert canonical_state.message_annotations == decoded.state.message_annotations
    refute canonical_state.encoded_message_annotations == annotations
  end

  defp transfer do
    %Transfer{
      handle: Value.uint(0),
      more: Value.boolean(false),
      resume: Value.boolean(false),
      aborted: Value.boolean(false),
      batchable: Value.boolean(false)
    }
  end

  defp disposition do
    %Disposition{
      role: Value.boolean(true),
      first: Value.uint(0),
      settled: Value.boolean(false),
      batchable: Value.boolean(false)
    }
  end

  defp wire(descriptor, fields) do
    value = Value.described(Value.ulong(descriptor), Value.list(fields))
    {:ok, encoded} = WireValue.encode(value)
    encoded
  end
end
