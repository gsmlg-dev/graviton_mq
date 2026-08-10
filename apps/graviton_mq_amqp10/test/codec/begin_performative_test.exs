defmodule GravitonMQ.AMQP10.Codec.BeginPerformativeTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.AMQP10.Codec.Error
  alias GravitonMQ.AMQP10.Codec.Performative
  alias GravitonMQ.AMQP10.Performative.Begin
  alias GravitonMQ.AMQP10.Value, as: AMQPValue

  @minimal_begin <<
    0x00,
    0x53,
    0x11,
    0xC0,
    7,
    4,
    0x40,
    0x43,
    0x52,
    100,
    0x52,
    100
  >>
  @symbolic_begin <<
    0x00,
    0xA3,
    15,
    "amqp:begin:list",
    0xC0,
    7,
    4,
    0x40,
    0x43,
    0x52,
    100,
    0x52,
    100
  >>
  @begin_with_explicit_handle_max_null <<
    0x00,
    0x53,
    0x11,
    0xC0,
    8,
    5,
    0x40,
    0x43,
    0x52,
    100,
    0x52,
    100,
    0x40
  >>
  @begin_with_desired_capability <<
    0x00,
    0x53,
    0x11,
    0xC0,
    14,
    7,
    0x40,
    0x43,
    0x52,
    100,
    0x52,
    100,
    0x40,
    0x40,
    0xA3,
    3,
    "cap"
  >>
  @full_begin <<
    0x00,
    0x53,
    0x11,
    0xC0,
    36,
    8,
    0x60,
    7::16,
    0x52,
    1,
    0x52,
    100,
    0x52,
    200,
    0x70,
    1_000::32,
    0xA3,
    1,
    "o",
    0xE0,
    8,
    2,
    0xA3,
    2,
    "d1",
    2,
    "d2",
    0xC1,
    6,
    2,
    0xA3,
    1,
    "p",
    0x52,
    1
  >>

  test "numeric and symbolic Begin descriptors normalize to the same typed performative" do
    expected = minimal_begin()

    for fixture <- [@minimal_begin, @symbolic_begin] do
      assert {:ok, ^expected, <<>>} = Performative.decode(fixture)
    end
  end

  test "decodes all ordered Begin fields without losing exact AMQP value identities" do
    desired = AMQPValue.array(:symbol, [AMQPValue.symbol("d1"), AMQPValue.symbol("d2")])
    properties = AMQPValue.map([{AMQPValue.symbol("p"), AMQPValue.uint(1)}])

    assert {:ok,
            %Begin{
              remote_channel: %AMQPValue{type: :ushort, value: 7},
              next_outgoing_id: %AMQPValue{type: :uint, value: 1},
              incoming_window: %AMQPValue{type: :uint, value: 100},
              outgoing_window: %AMQPValue{type: :uint, value: 200},
              handle_max: %AMQPValue{type: :uint, value: 1_000},
              offered_capabilities: %AMQPValue{type: :symbol, value: "o"},
              desired_capabilities: ^desired,
              properties: ^properties
            }, <<>>} = Performative.decode(@full_begin)
  end

  test "materializes Begin's handle-max default and does not impose session context" do
    expected = minimal_begin()

    assert {:ok, ^expected, <<>>} = Performative.decode(@minimal_begin)
    assert {:ok, ^expected, <<>>} = Performative.decode(@begin_with_explicit_handle_max_null)

    with_remote_channel = %{expected | remote_channel: AMQPValue.ushort(9)}
    assert {:ok, encoded} = Performative.encode(with_remote_channel)
    assert {:ok, ^with_remote_channel, <<>>} = Performative.decode(encoded)
  end

  test "returns the exact suffix after one Begin value" do
    suffix = <<0xDE, 0xAD, 0xBE, 0xEF>>
    expected = minimal_begin()

    assert {:ok, ^expected, ^suffix} = Performative.decode(@minimal_begin <> suffix)
  end

  test "every strict numeric and symbolic Begin prefix requests more input" do
    for fixture <- [@minimal_begin, @symbolic_begin, @full_begin],
        prefix_size <- 0..(byte_size(fixture) - 1) do
      prefix = binary_part(fixture, 0, prefix_size)
      assert {:more, needed} = Performative.decode(prefix)
      assert needed > 0
    end
  end

  test "rejects every missing or null mandatory Begin field" do
    cases = [
      [],
      [AMQPValue.null(), AMQPValue.null(), AMQPValue.uint(100), AMQPValue.uint(100)],
      [AMQPValue.null(), AMQPValue.uint(0), AMQPValue.null(), AMQPValue.uint(100)],
      [AMQPValue.null(), AMQPValue.uint(0), AMQPValue.uint(100), AMQPValue.null()]
    ]

    for fields <- cases do
      assert_schema_error(
        Performative.decode(encode_begin_fields!(fields)),
        :performative_decode,
        :malformed,
        :mandatory_field_missing
      )
    end
  end

  test "rejects exact type mismatches in every Begin field" do
    valid_fields = [
      AMQPValue.ushort(7),
      AMQPValue.uint(1),
      AMQPValue.uint(100),
      AMQPValue.uint(200),
      AMQPValue.uint(1_000),
      AMQPValue.symbol("offered"),
      AMQPValue.array(:symbol, [AMQPValue.symbol("desired")]),
      AMQPValue.map([{AMQPValue.symbol("p"), AMQPValue.uint(1)}])
    ]

    wrong_values = [
      AMQPValue.uint(7),
      AMQPValue.ulong(1),
      AMQPValue.ulong(100),
      AMQPValue.ulong(200),
      AMQPValue.ulong(1_000),
      AMQPValue.string("not-a-symbol"),
      AMQPValue.string("not-a-symbol-array"),
      AMQPValue.list([])
    ]

    for {wrong_value, index} <- Enum.with_index(wrong_values) do
      encoded = valid_fields |> List.replace_at(index, wrong_value) |> encode_begin_fields!()

      assert_schema_error(
        Performative.decode(encoded),
        :performative_decode,
        :malformed,
        :field_type_mismatch
      )
    end
  end

  test "rejects invalid Begin properties and known malformed shapes" do
    invalid_properties =
      encode_begin_fields!([
        AMQPValue.null(),
        AMQPValue.uint(0),
        AMQPValue.uint(100),
        AMQPValue.uint(100),
        AMQPValue.null(),
        AMQPValue.null(),
        AMQPValue.null(),
        AMQPValue.map([{AMQPValue.string("p"), AMQPValue.uint(1)}])
      ])

    assert_schema_error(
      Performative.decode(invalid_properties),
      :performative_decode,
      :malformed,
      :invalid_property_key
    )

    extra_fields = [
      AMQPValue.null(),
      AMQPValue.uint(0),
      AMQPValue.uint(100),
      AMQPValue.uint(100)
      | nulls(5)
    ]

    assert_schema_error(
      Performative.decode(encode_begin_fields!(extra_fields)),
      :performative_decode,
      :malformed,
      :too_many_fields
    )

    assert_schema_error(
      Performative.decode(<<0x00, 0x53, 0x11, 0x40>>),
      :performative_decode,
      :malformed,
      :invalid_performative_body
    )
  end

  test "canonically encodes Begin with numeric descriptor, omitted default, and interior nulls" do
    assert {:ok, @minimal_begin} = Performative.encode(minimal_begin())
    assert {:ok, @minimal_begin} = Performative.encode(%{minimal_begin() | handle_max: nil})

    begin_performative = %{minimal_begin() | desired_capabilities: AMQPValue.symbol("cap")}
    assert {:ok, @begin_with_desired_capability} = Performative.encode(begin_performative)
    assert {:ok, @full_begin} = Performative.encode(full_begin())
  end

  test "invalid outbound Begin structs return schema errors instead of raising" do
    malformed_symbol_array = %AMQPValue{
      type: :array,
      value: %AMQPValue.Array{element_type: :symbol, values: :not_a_list}
    }

    cases = [
      {%Begin{}, :mandatory_field_missing},
      {%Begin{
         next_outgoing_id: AMQPValue.ulong(0),
         incoming_window: AMQPValue.uint(100),
         outgoing_window: AMQPValue.uint(100)
       }, :field_type_mismatch},
      {%Begin{
         next_outgoing_id: AMQPValue.uint(0),
         incoming_window: AMQPValue.uint(100),
         outgoing_window: AMQPValue.uint(100),
         offered_capabilities: AMQPValue.string("not-a-symbol")
       }, :field_type_mismatch},
      {%Begin{
         next_outgoing_id: AMQPValue.uint(0),
         incoming_window: AMQPValue.uint(100),
         outgoing_window: AMQPValue.uint(100),
         offered_capabilities: malformed_symbol_array
       }, :field_type_mismatch},
      {%Begin{
         next_outgoing_id: AMQPValue.uint(0),
         incoming_window: AMQPValue.uint(100),
         outgoing_window: AMQPValue.uint(100),
         properties: AMQPValue.map([{AMQPValue.string("p"), AMQPValue.uint(1)}])
       }, :invalid_property_key}
    ]

    for {begin_performative, reason} <- cases do
      assert_schema_error(
        Performative.encode(begin_performative),
        :performative_encode,
        :invalid_value,
        reason
      )
    end
  end

  defp minimal_begin do
    %Begin{
      next_outgoing_id: AMQPValue.uint(0),
      incoming_window: AMQPValue.uint(100),
      outgoing_window: AMQPValue.uint(100),
      handle_max: AMQPValue.uint(4_294_967_295)
    }
  end

  defp full_begin do
    %Begin{
      remote_channel: AMQPValue.ushort(7),
      next_outgoing_id: AMQPValue.uint(1),
      incoming_window: AMQPValue.uint(100),
      outgoing_window: AMQPValue.uint(200),
      handle_max: AMQPValue.uint(1_000),
      offered_capabilities: AMQPValue.symbol("o"),
      desired_capabilities:
        AMQPValue.array(:symbol, [AMQPValue.symbol("d1"), AMQPValue.symbol("d2")]),
      properties: AMQPValue.map([{AMQPValue.symbol("p"), AMQPValue.uint(1)}])
    }
  end

  defp encode_begin_fields!(fields) do
    value = AMQPValue.described(AMQPValue.ulong(0x11), AMQPValue.list(fields))
    {:ok, encoded} = GravitonMQ.AMQP10.Codec.Value.encode(value)
    encoded
  end

  defp nulls(count), do: List.duplicate(AMQPValue.null(), count)

  defp assert_schema_error(result, operation, class, reason) do
    assert {:error,
            %Error{
              operation: ^operation,
              class: ^class,
              reason: ^reason
            }} = result
  end
end
